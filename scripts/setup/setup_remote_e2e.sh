#!/usr/bin/env bash
set -euo pipefail

# Setup script for zk-Analytics on remote machines for distributed benchmarking.
#
# This script:
#   - Copies the project to remote machines
#   - Installs dependencies (Docker, Rust, FDB client, system packages)
#   - Configures Docker group for no-sudo access
#   - Builds the project on remote machines
#   - Configures FDB cluster file
#   - Sets up SSH keys for passwordless access
#
# Note: This script uses setup_local_e2e.sh for dependency installation
#       to ensure consistency between local and remote environments.
#
# Usage:
#   ./scripts/setup/setup_remote_e2e.sh
#
# Configuration via env vars:
#   REMOTE_MACHINES    Space-separated list of IPs/hostnames (required)
#   SSH_USER           SSH username (default: current user)
#   PROJECT_DIR        Local project directory (default: current dir)
#   REMOTE_PROJECT_DIR Remote project directory (default: /mydata/zk-Analytics)
#   FDB_CLUSTER_FILE   Path to FDB cluster file to copy (default: /etc/foundationdb/fdb.cluster)
#   KAFKA_BROKERS      Kafka broker addresses (default: localhost:9092)
#
# Example:
#   REMOTE_MACHINES="192.0.2.1 192.0.2.2 192.0.2.3" \
#   SSH_USER="ubuntu" \
#   KAFKA_BROKERS="192.0.2.100:9092" \
#   ./scripts/setup/setup_remote_e2e.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Configuration
REMOTE_MACHINES="${REMOTE_MACHINES:-}"
SSH_USER="${SSH_USER:-$USER}"
PROJECT_DIR="${PROJECT_DIR:-$ROOT_DIR}"
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/mydata/zk-Analytics}"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
# Auto-detect Kafka broker address for distributed mode:
# If KAFKA_BROKERS is not set, use the first remote machine's IP
_detect_kafka_brokers() {
    local default_brokers="localhost:9092"
    local first_machine
    read -ra _machines <<< "$REMOTE_MACHINES"
    first_machine="${_machines[0]:-localhost}"

    # If first machine is not localhost, use it as Kafka broker
    if [[ "$first_machine" != "localhost" && "$first_machine" != "127.0.0.1" && -n "$first_machine" ]]; then
        default_brokers="${first_machine}:9092"
    fi
    echo "$default_brokers"
}
KAFKA_BROKERS="${KAFKA_BROKERS:-$(_detect_kafka_brokers)}"
FDB_VERSION="${FDB_VERSION:-7.1.61}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# Validate configuration
validate_config() {
    if [[ -z "$REMOTE_MACHINES" ]]; then
        log_error "REMOTE_MACHINES environment variable is required"
        echo ""
        echo "Usage example:"
        echo "  REMOTE_MACHINES=\"192.0.2.1 192.0.2.2 192.0.2.3\" \\"
        echo "  SSH_USER=\"ubuntu\" \\"
        echo "  ./scripts/setup/setup_remote_e2e.sh"
        exit 1
    fi

    log_info "Configuration:"
    log_info "  Remote machines: $REMOTE_MACHINES"
    log_info "  SSH user: $SSH_USER"
    log_info "  Local project: $PROJECT_DIR"
    log_info "  Remote project: $REMOTE_PROJECT_DIR"
    log_info "  Kafka brokers: $KAFKA_BROKERS"
    echo ""
}

# Setup SSH keys for passwordless authentication
setup_ssh_keys() {
    log_step "Setting up SSH keys for passwordless authentication..."

    # Check if SSH key exists
    if [[ ! -f "$HOME/.ssh/id_ed25519" ]] && [[ ! -f "$HOME/.ssh/id_rsa" ]]; then
        log_info "No SSH key found, generating one..."
        ssh-keygen -t ed25519 -C "${USER}@$(hostname)" -f "$HOME/.ssh/id_ed25519" -N ""
        log_info "  ✓ SSH key generated at ~/.ssh/id_ed25519"
    else
        log_info "SSH key already exists"
    fi

    # Ask user if they want to copy keys to remote machines
    echo ""
    read -p "Do you want to copy SSH keys to remote machines now? [Y/n]: " -n 1 -r
    echo

    if [[ ! $REPLY =~ ^[Nn]$ ]]; then
        for machine in $REMOTE_MACHINES; do
            log_info "Copying SSH key to ${SSH_USER}@${machine}..."
            ssh-copy-id -o ConnectTimeout=10 "${SSH_USER}@${machine}" 2>/dev/null || {
                log_warn "  Failed to copy SSH key to $machine (you may need to do this manually)"
                log_info "  Run: ssh-copy-id ${SSH_USER}@${machine}"
            }
        done
    else
        log_warn "Skipping SSH key copy. You'll need to do this manually:"
        for machine in $REMOTE_MACHINES; do
            log_info "  ssh-copy-id ${SSH_USER}@${machine}"
        done
    fi

    echo ""
}

# Test SSH connectivity to all machines
test_connectivity() {
    log_step "Testing SSH connectivity to all machines..."

    local failed=0
    for machine in $REMOTE_MACHINES; do
        if ssh -o ConnectTimeout=5 -o BatchMode=yes "${SSH_USER}@${machine}" "echo 'SSH OK'" &>/dev/null; then
            log_info "  ✓ $machine - connected"
        else
            log_error "  ✗ $machine - connection failed"
            failed=1
        fi
    done

    if [[ $failed -eq 1 ]]; then
        log_error "Some machines are not accessible via SSH"
        log_info "Make sure SSH key-based authentication is configured:"
        log_info "  ssh-copy-id ${SSH_USER}@<machine>"
        exit 1
    fi

    log_info "All machines are accessible"
    echo ""
}

# Install dependencies on a remote machine using setup_local_e2e.sh
install_dependencies_on_machine() {
    local machine="$1"

    log_step "[$machine] Installing system dependencies (using setup_local_e2e.sh)..."

    # Copy setup_local_e2e.sh to remote machine
    scp -q "$PROJECT_DIR/scripts/setup/setup_local_e2e.sh" "${SSH_USER}@${machine}:/tmp/"

    # Run setup_local_e2e.sh --deps on remote machine
    ssh "${SSH_USER}@${machine}" bash <<'EOF'
        chmod +x /tmp/setup_local_e2e.sh
        sudo /tmp/setup_local_e2e.sh --deps 2>&1 | tail -3
        rm -f /tmp/setup_local_e2e.sh
EOF

    log_info "  ✓ System dependencies installed on $machine"
}

# Install Rust on a remote machine
install_rust_on_machine() {
    local machine="$1"

    log_step "[$machine] Installing Rust..."

    ssh "${SSH_USER}@${machine}" bash <<'EOF'
        if ! command -v rustc &>/dev/null; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y >/dev/null 2>&1
            source "$HOME/.cargo/env"
            echo "Rust installed: $(rustc --version)"
        else
            source "$HOME/.cargo/env"
            rustup update stable >/dev/null 2>&1
            echo "Rust already installed: $(rustc --version)"
        fi
EOF

    log_info "  ✓ Rust installed on $machine"
}

# Install Docker on a remote machine using setup_local_e2e.sh
install_docker_on_machine() {
    local machine="$1"

    log_step "[$machine] Installing Docker (using setup_local_e2e.sh)..."

    # Copy setup_local_e2e.sh to remote machine
    scp -q "$PROJECT_DIR/scripts/setup/setup_local_e2e.sh" "${SSH_USER}@${machine}:/tmp/"

    # Run setup_local_e2e.sh --docker on remote machine
    ssh "${SSH_USER}@${machine}" bash <<'EOF'
        chmod +x /tmp/setup_local_e2e.sh
        sudo /tmp/setup_local_e2e.sh --docker 2>&1 | tail -5
        rm -f /tmp/setup_local_e2e.sh
EOF

    log_info "  ✓ Docker installed on $machine"
}

# Install FoundationDB client on a remote machine using setup_local_e2e.sh
install_fdb_on_machine() {
    local machine="$1"
    local fdb_version="$2"

    log_step "[$machine] Installing FoundationDB client (using setup_local_e2e.sh)..."

    # Copy setup_local_e2e.sh to remote machine
    scp -q "$PROJECT_DIR/scripts/setup/setup_local_e2e.sh" "${SSH_USER}@${machine}:/tmp/"

    # Run setup_local_e2e.sh --fdb on remote machine
    ssh "${SSH_USER}@${machine}" bash <<EOF
        chmod +x /tmp/setup_local_e2e.sh
        export FDB_VERSION="${fdb_version}"
        sudo -E /tmp/setup_local_e2e.sh --fdb 2>&1 | tail -3
        rm -f /tmp/setup_local_e2e.sh
EOF

    log_info "  ✓ FoundationDB client installed on $machine"
}

# Copy FDB cluster file to remote machine
copy_fdb_cluster_file() {
    local machine="$1"

    log_step "[$machine] Copying FDB cluster file..."

    if [[ ! -f "$FDB_CLUSTER_FILE" ]]; then
        log_warn "  FDB cluster file not found at $FDB_CLUSTER_FILE, skipping"
        return
    fi

    # Copy cluster file
    scp -q "$FDB_CLUSTER_FILE" "${SSH_USER}@${machine}:/tmp/fdb.cluster"
    ssh "${SSH_USER}@${machine}" "sudo mv /tmp/fdb.cluster /etc/foundationdb/fdb.cluster && sudo chmod 644 /etc/foundationdb/fdb.cluster"

    log_info "  ✓ FDB cluster file copied to $machine"
}

# Create required directories on remote machine
create_directories_on_machine() {
    local machine="$1"

    log_step "[$machine] Creating required directories..."

    ssh "${SSH_USER}@${machine}" bash <<EOF
        # Create project directory
        sudo mkdir -p $REMOTE_PROJECT_DIR
        sudo chown -R ${SSH_USER}:${SSH_USER} $REMOTE_PROJECT_DIR

        # Create tmp directories for RISC0 builds
        mkdir -p $REMOTE_PROJECT_DIR/target/tmp
        mkdir -p $REMOTE_PROJECT_DIR/target/tmp

        # Create RocksDB directories
        sudo mkdir -p /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg
        sudo chown -R ${SSH_USER}:${SSH_USER} /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg

        echo "Directories created successfully"
EOF

    log_info "  ✓ Directories created on $machine"
}

# Sync project to remote machine
sync_project_to_machine() {
    local machine="$1"

    log_step "[$machine] Syncing project files to /mydata/..."

    # Create remote directory (may need sudo for /mydata/)
    ssh "${SSH_USER}@${machine}" "sudo mkdir -p $REMOTE_PROJECT_DIR && sudo chown -R ${SSH_USER}:${SSH_USER} $REMOTE_PROJECT_DIR"

    # Rsync project (excluding target directory and git)
    rsync -az --delete \
        --exclude 'target/' \
        --exclude '.git/' \
        --exclude 'bench_csv/' \
        --exclude 'bench_logs/' \
        --exclude '.env' \
        --exclude '*.log' \
        "$PROJECT_DIR/" \
        "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/" \
        --quiet

    log_info "  ✓ Project synced to $machine:$REMOTE_PROJECT_DIR"
}

# Sync code to all remote machines (quick sync without build)
sync_code_to_all_machines() {
    log_step "Syncing code to all remote machines..."

    local synced=0
    for machine in $REMOTE_MACHINES; do
        log_info "  Syncing to $machine:${REMOTE_PROJECT_DIR}..."
        rsync -az --delete \
            --exclude 'target' \
            --exclude '.git' \
            --exclude 'bench_logs' \
            --exclude 'bench_csv' \
            --exclude '*.log' \
            "${PROJECT_DIR}/" "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/" || \
            log_warn "  Failed to sync to $machine"
        synced=$((synced + 1))
    done

    log_info "  ✓ Code synced to $synced remote machine(s)"
}

# Build project on remote machine
build_project_on_machine() {
    local machine="$1"

    log_step "[$machine] Building project (this may take a while)..."

    ssh "${SSH_USER}@${machine}" bash <<EOF
        cd $REMOTE_PROJECT_DIR
        source "\$HOME/.cargo/env"

        # Create tmp directories for RISC0 builds to avoid compilation errors
        echo "Creating temporary build directories..."
        mkdir -p target/tmp target/tmp

        # Create RocksDB directories for runtime
        echo "Creating RocksDB directories..."
        sudo mkdir -p /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg
        sudo chown -R ${SSH_USER}:${SSH_USER} /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg

        # Build data source
        echo "Building data source..."
        cargo build --release -p data_source 2>&1 | tail -1

        # Build aggregator with kafka + fdb features
        echo "Building aggregator with kafka + fdb..."
        cargo build --release -p aggregator --features "kafka fdb" 2>&1 | tail -1

        # Build querier with fdb feature
        echo "Building querier with fdb..."
        cargo build --release -p querier --features fdb 2>&1 | tail -1

        echo "Build complete"
EOF

    log_info "  ✓ Project built on $machine"
}

# Test FDB connectivity from remote machine
test_fdb_connectivity() {
    local machine="$1"

    log_step "[$machine] Testing FDB connectivity..."

    local result=$(ssh "${SSH_USER}@${machine}" "fdbcli --exec 'status minimal' 2>&1" || echo "failed")

    if echo "$result" | grep -q "Healthy\|available"; then
        log_info "  ✓ FDB is accessible from $machine"
    else
        log_warn "  ⚠ FDB may not be accessible from $machine"
        log_warn "    Make sure the FDB cluster is running and accessible"
    fi
}

# Setup a single machine
setup_machine() {
    local machine="$1"

    echo ""
    echo "========================================"
    log_info "Setting up machine: $machine"
    echo "========================================"

    install_dependencies_on_machine "$machine"
    install_docker_on_machine "$machine"
    install_rust_on_machine "$machine"
    install_fdb_on_machine "$machine" "$FDB_VERSION"
    copy_fdb_cluster_file "$machine"
    create_directories_on_machine "$machine"
    sync_project_to_machine "$machine"
    build_project_on_machine "$machine"
    test_fdb_connectivity "$machine"

    log_info "✓ Machine $machine setup complete"
}

# Setup all machines in parallel
setup_all_machines_parallel() {
    log_step "Setting up all machines in parallel..."

    local pids=()
    for machine in $REMOTE_MACHINES; do
        setup_machine "$machine" > "/tmp/setup_${machine}.log" 2>&1 &
        pids+=($!)
    done

    # Wait for all setups to complete
    log_info "Waiting for all machines to complete setup..."
    for pid in "${pids[@]}"; do
        wait "$pid"
    done

    # Show logs
    for machine in $REMOTE_MACHINES; do
        echo ""
        echo "=== Log for $machine ==="
        cat "/tmp/setup_${machine}.log"
        rm -f "/tmp/setup_${machine}.log"
    done
}

# Setup all machines sequentially
setup_all_machines_sequential() {
    for machine in $REMOTE_MACHINES; do
        setup_machine "$machine"
    done
}

# Verify all machines are ready
verify_setup() {
    echo ""
    echo "========================================"
    log_step "Verifying setup on all machines..."
    echo "========================================"

    for machine in $REMOTE_MACHINES; do
        echo ""
        log_info "Checking $machine..."

        # Check Docker
        local docker_status=$(ssh "${SSH_USER}@${machine}" "docker info >/dev/null 2>&1 && echo 'ok' || echo 'not ready'")
        if [[ "$docker_status" == "ok" ]]; then
            log_info "  ✓ Docker installed and accessible"
        else
            log_warn "  ⚠ Docker not accessible (may need: newgrp docker or re-login)"
        fi

        # Check Rust
        local rust_version=$(ssh "${SSH_USER}@${machine}" "source ~/.cargo/env && rustc --version 2>/dev/null" || echo "not found")
        if [[ "$rust_version" != "not found" ]]; then
            log_info "  ✓ Rust: $rust_version"
        else
            log_error "  ✗ Rust not installed"
        fi

        # Check FDB
        if ssh "${SSH_USER}@${machine}" "command -v fdbcli >/dev/null 2>&1"; then
            log_info "  ✓ FDB client installed"
        else
            log_error "  ✗ FDB client not installed"
        fi

        # Check project
        if ssh "${SSH_USER}@${machine}" "test -d $REMOTE_PROJECT_DIR/target/release"; then
            log_info "  ✓ Project built"
        else
            log_error "  ✗ Project not built"
        fi
    done
}

# Print next steps
print_next_steps() {
    echo ""
    echo "========================================"
    log_info "Setup Complete!"
    echo "========================================"
    echo ""
    echo "Next steps:"
    echo ""
    echo "1. Run the distributed benchmark:"
    echo "   AGGREGATOR_MACHINES=\"$REMOTE_MACHINES\" \\"
    echo "   SSH_USER=\"$SSH_USER\" \\"
    echo "   REMOTE_PROJECT_PATH=\"$REMOTE_PROJECT_DIR\" \\"
    echo "   KAFKA_BROKERS=\"$KAFKA_BROKERS\" \\"
    echo "   ./scripts/distributed/bench_distributed_aggregators.sh"
    echo ""
    echo "2. To update code on remote machines, run this script again"
    echo ""
    echo "3. To manually SSH into a machine:"
    echo "   ssh ${SSH_USER}@<machine_ip>"
    echo ""
}

# Main
main() {
    # Check for --sync-only flag
    if [[ "${1:-}" == "--sync-only" || "${1:-}" == "--sync" ]]; then
        echo "========================================"
        echo "  zk-Analytics Code Sync Only"
        echo "========================================"
        echo ""
        validate_config
        test_connectivity
        sync_code_to_all_machines
        log_info "Done! Code synced to all remote machines."
        return
    fi

    # Check for --dirs-only flag
    if [[ "${1:-}" == "--dirs-only" || "${1:-}" == "--dirs" ]]; then
        echo "========================================"
        echo "  zk-Analytics Create Directories Only"
        echo "========================================"
        echo ""
        validate_config
        test_connectivity
        for machine in $REMOTE_MACHINES; do
            create_directories_on_machine "$machine"
        done
        log_info "Done! All required directories created on remote machines."
        return
    fi

    echo "========================================"
    echo "  zk-Analytics Remote Setup"
    echo "========================================"
    echo ""

    validate_config
    setup_ssh_keys
    test_connectivity

    # Ask user for parallel or sequential setup
    read -p "Setup machines in parallel? (faster but harder to debug) [Y/n]: " -n 1 -r
    echo

    if [[ $REPLY =~ ^[Nn]$ ]]; then
        setup_all_machines_sequential
    else
        setup_all_machines_parallel
    fi

    verify_setup
    print_next_steps
}

main "$@"
