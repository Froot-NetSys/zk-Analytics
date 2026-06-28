#!/usr/bin/env bash
set -euo pipefail

# Setup script for zk-Analytics local E2E testing environment.
#
# This script installs and configures:
#   - Docker (with no-sudo access)
#   - Docker Compose
#   - CMake (for rdkafka build)
#   - FoundationDB (client + server via Docker)
#   - RISC0 toolchain
#   - Kafka (via Docker)
#   - Rust build dependencies
#
# Usage:
#   ./scripts/setup/setup_local_e2e.sh [--all|--docker|--fdb|--risc0|--kafka|--deps]
#
# After setup, run:
#   ./scripts/eval/run_local_e2e.sh start

set -e

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Detect OS
detect_os() {
    if [[ -f /etc/os-release ]]; then
        . /etc/os-release
        OS=$ID
        VERSION=$VERSION_ID
    else
        log_error "Cannot detect OS. This script supports Ubuntu/Debian."
        exit 1
    fi
    log_info "Detected OS: $OS $VERSION"
}

# Install system dependencies
install_deps() {
    log_info "Installing system dependencies..."

    sudo apt-get update
    sudo apt-get install -y \
        build-essential \
        clang \
        libclang-dev \
        cmake \
        pkg-config \
        libssl-dev \
        libcurl4-openssl-dev \
        libsasl2-dev \
        libzstd-dev \
        liblz4-dev \
        zlib1g-dev \
        curl \
        wget \
        git \
        tmux \
        jq \
        lsof \
        net-tools \
        python3 \
        python3-matplotlib

    log_info "System dependencies installed"
}

# Install Docker
install_docker() {
    if command -v docker &>/dev/null; then
        log_info "Docker already installed: $(docker --version)"
    else
        log_info "Installing Docker..."

        # Remove old versions
        sudo apt-get remove -y docker docker-engine docker.io containerd runc 2>/dev/null || true

        # Install prerequisites
        sudo apt-get update
        sudo apt-get install -y \
            ca-certificates \
            curl \
            gnupg \
            lsb-release

        # Add Docker GPG key
        sudo install -m 0755 -d /etc/apt/keyrings
        curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
        sudo chmod a+r /etc/apt/keyrings/docker.gpg

        # Add Docker repository
        echo \
            "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
            $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
            sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

        # Install Docker
        sudo apt-get update
        sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

        log_info "Docker installed"
    fi

    # Add current user to docker group (no sudo required)
    if ! groups "$USER" | grep -q docker; then
        log_info "Adding $USER to docker group..."
        sudo usermod -aG docker "$USER"
        log_warn "You need to log out and log back in for docker group to take effect."
        log_warn "Or run: newgrp docker"
    else
        log_info "User $USER already in docker group"
    fi

    # Install docker-compose standalone (for older scripts)
    if ! command -v docker-compose &>/dev/null; then
        log_info "Installing docker-compose standalone..."
        sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" \
            -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose
    fi

    # Start Docker service
    sudo systemctl enable docker
    sudo systemctl start docker

    log_info "Docker setup complete"
}

# Install FoundationDB
install_fdb() {
    FDB_VERSION="${FDB_VERSION:-7.1.61}"

    if command -v fdbcli &>/dev/null; then
        log_info "FoundationDB CLI already installed"
    else
        log_info "Installing FoundationDB ${FDB_VERSION}..."

        cd /tmp

        # Download FDB packages
        wget -q "https://github.com/apple/foundationdb/releases/download/${FDB_VERSION}/foundationdb-clients_${FDB_VERSION}-1_amd64.deb" \
            -O foundationdb-clients.deb

        # Install client only (we'll use Docker for server)
        sudo dpkg -i foundationdb-clients.deb || sudo apt-get install -f -y

        rm -f foundationdb-clients.deb

        cd "$ROOT_DIR"
        log_info "FoundationDB client installed"
    fi

    # Create FDB config directory
    sudo mkdir -p /etc/foundationdb
    sudo chown "$USER:$USER" /etc/foundationdb 2>/dev/null || true

    # Start FDB via Docker
    log_info "Starting FoundationDB via Docker..."

    if docker ps -a --format '{{.Names}}' | grep -q '^fdb$'; then
        docker start fdb 2>/dev/null || true
    else
        docker run -d \
            --name fdb \
            --restart unless-stopped \
            -p 4500:4500 \
            foundationdb/foundationdb:7.1.25
    fi

    # Wait for FDB to start
    sleep 3

    # Copy cluster file from container
    log_info "Configuring FDB cluster file..."
    docker exec fdb cat /var/fdb/fdb.cluster | sudo tee /etc/foundationdb/fdb.cluster >/dev/null 2>&1 || {
        # If container IP changed, update cluster file
        FDB_IP=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' fdb)
        echo "docker:docker@${FDB_IP}:4500" | sudo tee /etc/foundationdb/fdb.cluster >/dev/null
    }

    # Initialize database if needed
    if ! fdbcli --exec "status minimal" 2>&1 | grep -q "Healthy"; then
        log_info "Initializing FDB database..."
        fdbcli --exec "configure new single ssd" 2>/dev/null || true
    fi

    # Verify connection
    if fdbcli --exec "status minimal" 2>&1 | grep -q "Healthy\|The database is available"; then
        log_info "FoundationDB is healthy"
    else
        log_warn "FoundationDB may need manual initialization. Run: fdbcli --exec 'configure new single ssd'"
    fi

    log_info "FoundationDB setup complete"
}

# Create required directories
create_directories() {
    log_info "Creating required directories..."

    cd "$ROOT_DIR"

    # Create tmp directories for RISC0 builds to avoid compilation errors
    log_info "Creating temporary build directories..."
    mkdir -p target/tmp target/tmp

    # Create RocksDB directories for runtime
    log_info "Creating RocksDB directories..."
    sudo mkdir -p /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg
    sudo chown -R "$USER:$USER" /mydata/rocksdb /mydata/rocksdb_secondary /mydata/rocksdb_agg

    log_info "All directories created successfully"
}

# Install Rust and RISC0
install_risc0() {
    # Install Rust if not present
    if ! command -v rustc &>/dev/null; then
        log_info "Installing Rust..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    else
        log_info "Rust already installed: $(rustc --version)"
    fi

    # Update Rust
    rustup update stable

    # Install RISC0 toolchain
    log_info "Installing RISC0 toolchain..."

    if ! command -v rzup &>/dev/null; then
        curl -L https://risczero.com/install | bash
        export PATH="$HOME/.risc0/bin:$PATH"
    fi

    # Install RISC0 components
    if command -v rzup &>/dev/null; then
        rzup install
        log_info "RISC0 toolchain installed"
    else
        log_warn "rzup not found in PATH. Add ~/.risc0/bin to PATH and run: rzup install"
    fi

    # Add to shell profile
    PROFILE_FILE="$HOME/.bashrc"
    if [[ -f "$HOME/.zshrc" ]]; then
        PROFILE_FILE="$HOME/.zshrc"
    fi

    if ! grep -q "risc0" "$PROFILE_FILE" 2>/dev/null; then
        echo 'export PATH="$HOME/.risc0/bin:$PATH"' >> "$PROFILE_FILE"
        log_info "Added RISC0 to $PROFILE_FILE"
    fi

    log_info "RISC0 setup complete"
}

# Setup Kafka via Docker
setup_kafka() {
    log_info "Setting up Kafka via Docker..."

    # Create docker-compose file if not exists
    COMPOSE_FILE="$ROOT_DIR/scripts/docker-compose-kafka.yml"

    if [[ ! -f "$COMPOSE_FILE" ]]; then
        log_info "Creating docker-compose-kafka.yml..."
        cat > "$COMPOSE_FILE" << 'EOF'
version: '3.8'

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    container_name: zookeeper
    restart: unless-stopped
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    container_name: kafka
    restart: unless-stopped
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
      - "29092:29092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    container_name: kafka-ui
    restart: unless-stopped
    depends_on:
      - kafka
    ports:
      - "8080:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:29092
      KAFKA_CLUSTERS_0_ZOOKEEPER: zookeeper:2181
EOF
    fi

    # Start Kafka
    cd "$ROOT_DIR/scripts"
    docker-compose -f docker-compose-kafka.yml up -d

    # Wait for Kafka to be ready
    log_info "Waiting for Kafka to be ready..."
    sleep 10

    # Create topic
    log_info "Creating Kafka topic: raw_events"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic raw_events \
        --partitions 16 --replication-factor 1 \
        --config retention.ms=604800000 2>/dev/null || true

    # Verify
    if docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep -q raw_events; then
        log_info "Kafka topic 'raw_events' created"
    else
        log_warn "Failed to create Kafka topic"
    fi

    cd "$ROOT_DIR"
    log_info "Kafka setup complete"
}

# Build zk-Analytics project
build_project() {
    log_info "Building zk-Analytics project..."

    cd "$ROOT_DIR"

    # Create required directories first
    create_directories

    # Build with required features
    log_info "Building data source..."
    cargo build --release -p data_source

    log_info "Building aggregator with kafka + fdb features..."
    cargo build --release -p aggregator --features "kafka fdb"

    log_info "Building querier with fdb feature..."
    cargo build --release -p querier --features fdb

    log_info "Build complete"
}

# Print status summary
print_status() {
    echo ""
    echo "======================================"
    echo "  zk-Analytics E2E Setup Status"
    echo "======================================"
    echo ""

    # Docker
    if command -v docker &>/dev/null && docker info &>/dev/null; then
        echo -e "Docker:        ${GREEN}OK${NC} ($(docker --version | cut -d' ' -f3 | tr -d ','))"
    else
        echo -e "Docker:        ${RED}NOT READY${NC} (may need: newgrp docker)"
    fi

    # Docker Compose
    if command -v docker-compose &>/dev/null; then
        echo -e "Docker Compose: ${GREEN}OK${NC}"
    else
        echo -e "Docker Compose: ${RED}MISSING${NC}"
    fi

    # CMake
    if command -v cmake &>/dev/null; then
        echo -e "CMake:         ${GREEN}OK${NC} ($(cmake --version | head -1 | cut -d' ' -f3))"
    else
        echo -e "CMake:         ${RED}MISSING${NC}"
    fi

    # Rust
    if command -v rustc &>/dev/null; then
        echo -e "Rust:          ${GREEN}OK${NC} ($(rustc --version | cut -d' ' -f2))"
    else
        echo -e "Rust:          ${RED}MISSING${NC}"
    fi

    # RISC0
    if command -v rzup &>/dev/null; then
        echo -e "RISC0:         ${GREEN}OK${NC}"
    else
        echo -e "RISC0:         ${YELLOW}NOT IN PATH${NC} (add ~/.risc0/bin to PATH)"
    fi

    # FoundationDB
    if command -v fdbcli &>/dev/null; then
        if fdbcli --exec "status minimal" 2>&1 | grep -q "Healthy\|available"; then
            echo -e "FoundationDB:  ${GREEN}OK${NC} (healthy)"
        else
            echo -e "FoundationDB:  ${YELLOW}NEEDS INIT${NC} (run: fdbcli --exec 'configure new single ssd')"
        fi
    else
        echo -e "FoundationDB:  ${RED}MISSING${NC}"
    fi

    # Kafka
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka$'; then
        echo -e "Kafka:         ${GREEN}RUNNING${NC}"
    else
        echo -e "Kafka:         ${RED}NOT RUNNING${NC}"
    fi

    # FDB Container
    if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^fdb$'; then
        echo -e "FDB Container: ${GREEN}RUNNING${NC}"
    else
        echo -e "FDB Container: ${RED}NOT RUNNING${NC}"
    fi

    echo ""
    echo "======================================"
    echo ""
    echo "Next steps:"
    echo "  1. If Docker group changed: log out and log back in, or run 'newgrp docker'"
    echo "  2. Run the E2E test: ./scripts/eval/run_local_e2e.sh start"
    echo "  3. Attach to tmux: tmux attach -t zktelemetry-e2e"
    echo ""
}

# Main
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  --all      Install everything (default)"
    echo "  --deps     Install system dependencies only"
    echo "  --docker   Install Docker only"
    echo "  --fdb      Install FoundationDB only"
    echo "  --risc0    Install RISC0 toolchain only"
    echo "  --kafka    Setup Kafka only"
    echo "  --dirs     Create required directories only (tmp, rocksdb)"
    echo "  --build    Build project only"
    echo "  --status   Show status only"
    echo "  --help     Show this help"
    echo ""
    exit 0
}

main() {
    detect_os

    case "${1:-all}" in
        --all|all)
            install_deps
            install_docker
            install_fdb
            install_risc0
            setup_kafka
            build_project
            print_status
            ;;
        --deps|deps)
            install_deps
            ;;
        --docker|docker)
            install_docker
            ;;
        --fdb|fdb)
            install_fdb
            ;;
        --risc0|risc0)
            install_risc0
            ;;
        --kafka|kafka)
            setup_kafka
            ;;
        --dirs|dirs)
            create_directories
            ;;
        --build|build)
            build_project
            ;;
        --status|status)
            print_status
            ;;
        --help|help|-h)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
}

main "$@"
