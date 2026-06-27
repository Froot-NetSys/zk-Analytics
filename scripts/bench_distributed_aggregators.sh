#!/usr/bin/env bash
set -euo pipefail

# Cleanup on exit
cleanup_on_exit() {
    # Stop background log sync if running
    if [[ -n "${LOG_SYNC_PID:-}" ]]; then
        kill "$LOG_SYNC_PID" 2>/dev/null || true
    fi
}
trap cleanup_on_exit EXIT

# Benchmark distributed aggregators with Kafka + FDB.
#
# This script measures aggregation throughput and latency across
# multiple aggregator instances consuming from Kafka partitions.
#
# Usage:
#   # Run locally on one machine:
#   ./scripts/bench_distributed_aggregators.sh
#
#   # Run across multiple machines:
#   AGGREGATOR_MACHINES="192.168.1.10 192.168.1.11 192.168.1.12" \
#   SSH_USER="ubuntu" \
#   REMOTE_PROJECT_PATH="/home/ubuntu/zk-Analytics" \
#   ./scripts/bench_distributed_aggregators.sh
#
# Options (env vars):
#   NUM_AGGREGATORS    Number of aggregator instances (default: 1, 2, 4, 8)
#   NUM_SOURCES        Number of data sources (default: 1000)
#   SERIES             Number of distinct keys per source (default: 64)
#   SAMPLES_PER_SERIES Samples per key per source (default: 32)
#   REPEATS            Repeats per configuration (default: 3)
#   WARMUP_EVENTS      Warmup events before measurement (default: 10000)
#   EPOCH_TYPE         Aggregation types to test: samples, histogram, cm (default: samples)
#                      Example: EPOCH_TYPE="samples cm histogram"
#   ENABLE_ZK_AGGREGATION Enable ZK proof generation (default: 1)
#   RISC0_DEV_MODE     Run RISC0 in dev mode (no actual proofs, fast) (default: 0)
#   CONSUMER_SETTLE_SEC Extra wait time after Kafka lag=0 for RocksDB writes (default: 5)
#   CSV_DIR            Output directory (default: bench_csv)
#   LOG_DIR            Log directory (default: bench_logs)
#   AGGREGATOR_MACHINES Space-separated list of machines (default: localhost)
#                      Example: AGGREGATOR_MACHINES="localhost 192.168.1.10 192.168.1.11"
#   SSH_USER           SSH username for remote machines (default: current user)
#   REMOTE_PROJECT_PATH Project path on remote machines (default: current path)
#
# Architecture:
#   Data Source(s) -> Kafka (N partitions) -> Aggregators (N instances) -> FDB
#   Deterministic mapping: key_id[last byte] % N → partition N → aggregator N
#
# Kafka consumer group ensures each partition is consumed by exactly one aggregator.
#
# Prerequisites for distributed mode:
#   - SSH key-based authentication configured for remote machines
#   - zk-Analytics project built on all machines (cargo build --release)
#   - All machines must have access to Kafka and FoundationDB
#   - FDB cluster file must be accessible on all machines

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Source centralized IP defaults (single source of truth for machine config)
# shellcheck source=ip_defaults.sh
source "${ROOT_DIR}/scripts/ip_defaults.sh"

# Configuration
NUM_AGGREGATORS_LIST="${NUM_AGGREGATORS:-2}"
# Note: For distributed testing, use larger values to ensure data distributes across partitions
# Small values (SERIES=16) may result in all data going to 1-2 partitions
# SERIES: Number of distinct keys PER SOURCE (each source generates SERIES unique keys)
SERIES="${SERIES:-64}"
# SAMPLES_PER_SERIES: Samples per key per source (each key receives this many events)
SAMPLES_PER_SERIES="${SAMPLES_PER_SERIES:-64}"
# NUM_SOURCES: Number of data sources to simulate (each source has its own hash chain)
# partition = source_id % num_aggregators, so sources are distributed across aggregators
NUM_SOURCES="${NUM_SOURCES:-32}"
REPEATS="${REPEATS:-1}"
WARMUP_EVENTS="${WARMUP_EVENTS:-0}"
EPOCH_TYPE_LIST="${EPOCH_TYPE:-histogram}"
# kafka_batch_size = total commit batches per send (across all keys)
# Total events per send = kafka_batch_size * commit_batch_size
KAFKA_BATCH_SIZE="${KAFKA_BATCH_SIZE:-100}"
COMMIT_BATCH_SIZE="${COMMIT_BATCH_SIZE:-8}"
ENABLE_ZK_AGGREGATION="${ENABLE_ZK_AGGREGATION:-1}"
# RISC0 dev mode: Set to 1 to disable actual proof generation (much faster for testing)
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"

# Consumer settle time: extra seconds to wait after Kafka lag=0 before starting ZK aggregation.
# This allows kafka-consumers to finish writing all batches to RocksDB.
CONSUMER_SETTLE_SEC="${CONSUMER_SETTLE_SEC:-5}"

# Epoch batching configuration
# EPOCH_BATCH_THRESHOLD: Max batches per epoch (default: 256)
#   Each epoch contains at most this many batches (limit always applies).
# EPOCH_TIMEOUT_MS: Timeout in ms to force epoch flush (default: 300000 = 5min)
#   Creates epoch with available batches (up to threshold) if timeout elapses.
# Total events per epoch = up to EPOCH_BATCH_THRESHOLD * COMMIT_BATCH_SIZE
EPOCH_BATCH_THRESHOLD="${EPOCH_BATCH_THRESHOLD:-2048}"
EPOCH_TIMEOUT_MS="${EPOCH_TIMEOUT_MS:-300000}"

# ZK aggregation timeout (seconds). Set to 0 for no timeout.
# Default: 0 (no timeout - ZK proof generation can take hours)
ZK_AGGREGATION_TIMEOUT="${ZK_AGGREGATION_TIMEOUT:-0}"

# Distributed machines configuration
# AGGREGATOR_MACHINES is sourced from ip_defaults.sh (single source of truth)
# Override by setting AGGREGATOR_MACHINES before running this script, e.g.:
#   AGGREGATOR_MACHINES="192.0.2.1 192.0.2.2" ./scripts/bench_distributed_aggregators.sh
# Or edit scripts/ip_defaults.sh directly for persistent changes.

# SSH user for remote machines (if different from current user)
SSH_USER="${SSH_USER:-$USER}"

# Remote project path (must be same on all machines)
REMOTE_PROJECT_PATH="${REMOTE_PROJECT_PATH:-$ROOT_DIR}"

# Kafka settings (KAFKA_BROKERS and KAFKA_EXTERNAL_IP are auto-detected from ip_defaults.sh)
# Override KAFKA_TOPIC from ip_defaults.sh - benchmark uses its own topic
KAFKA_TOPIC="bench_raw_events"
KAFKA_GROUP_ID="bench_aggregators"

# FDB settings
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
FDB_SUBSPACE="${FDB_SUBSPACE:-zktelemetry_bench}"

# Storage - Fixed RocksDB paths
RAW_ROCKSDB_BASE="/mydata/rocksdb"
RAW_ROCKSDB_SECONDARY_BASE="/mydata/rocksdb_secondary"

# Output
CSV_DIR="${CSV_DIR:-bench_csv/distributed}"
LOG_DIR="${LOG_DIR:-bench_logs/distributed}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
CSV_FILE="${CSV_DIR}/bench_distributed_aggregators_${TIMESTAMP}.csv"
MAIN_LOG_FILE="${LOG_DIR}/bench_main_${TIMESTAMP}.log"

mkdir -p "$CSV_DIR" "$LOG_DIR"

# Start logging to both console and file
exec > >(tee -a "$MAIN_LOG_FILE") 2>&1
echo "=== Benchmark started at $(date) ===" >> "$MAIN_LOG_FILE"
echo "Main log file: $MAIN_LOG_FILE"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Cleanup RocksDB directories on all machines
cleanup_rocksdb_all_machines() {
    log_info "Cleaning up all processes and databases on all machines..."

    # Parse machine list into array
    read -ra machines <<< "$AGGREGATOR_MACHINES"

    # Step 1: Force kill all benchmark-related processes in parallel
    log_info "Force killing benchmark processes (r0vm, kafka-consumer, aggregator)..."
    local kill_pids=()
    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            # Kill local processes
            pkill -9 -f 'r0vm' 2>/dev/null || true
            pkill -9 -f 'kafka-consumer' 2>/dev/null || true
            pkill -9 -f 'aggregator' 2>/dev/null || true
        else
            # Kill remote processes via SSH (in parallel)
            ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "${SSH_USER}@${machine}" \
                "pkill -9 -f 'r0vm' 2>/dev/null; pkill -9 -f 'kafka-consumer' 2>/dev/null; pkill -9 -f 'aggregator' 2>/dev/null; true" &
            kill_pids+=($!)
        fi
    done
    # Wait for all kill commands to complete
    for pid in "${kill_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Step 2: Wait for processes to fully terminate and release file locks
    log_info "Waiting for processes to terminate..."
    sleep 3

    # Step 3: Clean ALL state databases in parallel
    log_info "Cleaning all state databases..."
    local clean_pids=()
    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            # Clean local state databases
            log_info "  Cleaning state on localhost..."
            rm -rf /mydata/rocksdb /mydata/rocksdb_* 2>/dev/null || true
        else
            # Clean remote state databases via SSH (in parallel)
            log_info "  Cleaning state on $machine..."
            ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "${SSH_USER}@${machine}" \
                "rm -rf /mydata/rocksdb /mydata/rocksdb_* 2>/dev/null; true" &
            clean_pids+=($!)
        fi
    done
    # Wait for all cleanup commands to complete
    for pid in "${clean_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Step 4: Verify cleanup succeeded and retry if needed
    log_info "Verifying cleanup..."
    local max_retries=3
    local retry=0

    while [[ $retry -lt $max_retries ]]; do
        local cleanup_ok=true
        local failed_machines=()

        for machine in "${machines[@]}"; do
            if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
                if [[ -d /mydata/rocksdb ]]; then
                    cleanup_ok=false
                    failed_machines+=("localhost")
                fi
            else
                if ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "test -d /mydata/rocksdb" 2>/dev/null; then
                    cleanup_ok=false
                    failed_machines+=("$machine")
                fi
            fi
        done

        if [[ "$cleanup_ok" == "true" ]]; then
            log_info "Cleanup verified - all state databases cleaned"
            break
        fi

        retry=$((retry + 1))
        if [[ $retry -lt $max_retries ]]; then
            log_warn "Cleanup incomplete on: ${failed_machines[*]}, retrying ($retry/$max_retries)..."
            sleep 2
            # Retry cleanup on failed machines
            for machine in "${failed_machines[@]}"; do
                if [[ "$machine" == "localhost" ]]; then
                    pkill -9 -f 'kafka-consumer' 2>/dev/null || true
                    rm -rf /mydata/rocksdb /mydata/rocksdb_* 2>/dev/null || true
                else
                    ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                        "pkill -9 -f 'kafka-consumer' 2>/dev/null; rm -rf /mydata/rocksdb /mydata/rocksdb_* 2>/dev/null; true" || true
                fi
            done
            sleep 2
        else
            log_error "Cleanup failed after $max_retries retries on: ${failed_machines[*]}"
        fi
    done

    # Final sync barrier - ensure all cleanup operations are complete
    log_info "Sync barrier - waiting for all machines..."
    sleep 2
}

# Check all machines have clean state (for debugging)
check_clean_state() {
    log_info "Checking clean state on all machines..."
    read -ra machines <<< "$AGGREGATOR_MACHINES"

    local all_clean=true
    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            local result=$(ls -la /mydata/rocksdb* 2>/dev/null || echo "clean")
            if [[ "$result" == "clean" ]]; then
                log_info "  localhost: clean"
            else
                log_warn "  localhost: NOT CLEAN"
                echo "$result"
                all_clean=false
            fi
        else
            local result=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                "ls -la /mydata/rocksdb* 2>/dev/null || echo 'clean'" 2>/dev/null || echo "ssh_error")
            if [[ "$result" == "clean" ]]; then
                log_info "  $machine: clean"
            elif [[ "$result" == "ssh_error" ]]; then
                log_warn "  $machine: SSH connection failed"
            else
                log_warn "  $machine: NOT CLEAN"
                echo "$result"
                all_clean=false
            fi
        fi
    done

    if [[ "$all_clean" == "true" ]]; then
        log_info "All machines have clean state"
    else
        log_warn "Some machines have stale data - run cleanup first"
    fi
}

# Force cleanup all machines (kills processes and removes rocksdb)
force_cleanup_all_machines() {
    log_info "Force cleanup on all machines..."
    read -ra machines <<< "$AGGREGATOR_MACHINES"

    local pids=()
    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            pkill -9 -f 'kafka-consumer' 2>/dev/null || true
            pkill -9 -f 'r0vm' 2>/dev/null || true
            pkill -9 -f 'aggregator' 2>/dev/null || true
            rm -rf /mydata/rocksdb* 2>/dev/null || true
            log_info "  localhost: cleaned"
        else
            ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "${SSH_USER}@${machine}" \
                "pkill -9 -f 'kafka-consumer' 2>/dev/null; pkill -9 -f 'r0vm' 2>/dev/null; pkill -9 -f 'aggregator' 2>/dev/null; rm -rf /mydata/rocksdb* 2>/dev/null; true" &
            pids+=($!)
        fi
    done

    # Wait for all SSH commands to complete
    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    log_info "Force cleanup complete"
}

# Sync code to all remote machines (in parallel)
sync_code_to_machines() {
    log_info "Syncing code to remote machines (parallel)..."
    read -ra machines <<< "$AGGREGATOR_MACHINES"
    local pids=()
    local remote_machines=()

    for machine in "${machines[@]}"; do
        if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
            log_info "  Starting sync to $machine:${REMOTE_PROJECT_PATH}..."
            rsync -az --delete \
                --exclude 'target' \
                --exclude '.git' \
                --exclude 'bench_logs' \
                --exclude 'bench_csv' \
                --exclude 'testdata' \
                --exclude '*.log' \
                "${ROOT_DIR}/" "${SSH_USER}@${machine}:${REMOTE_PROJECT_PATH}/" &
            pids+=($!)
            remote_machines+=("$machine")
        fi
    done

    if [[ ${#pids[@]} -eq 0 ]]; then
        log_info "  No remote machines to sync (all local)"
        return
    fi

    # Wait for all syncs to complete
    local failed=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log_info "  ✓ Sync complete: ${remote_machines[$i]}"
        else
            log_warn "  ✗ Failed to sync to ${remote_machines[$i]}"
            failed=$((failed + 1))
        fi
    done

    log_info "  ✓ Code synced to ${#remote_machines[@]} remote machine(s) ($failed failed)"
}

# Sync compiled binaries to all remote machines (in parallel)
sync_binaries_to_machines() {
    log_info "Syncing binaries to remote machines (parallel)..."
    read -ra machines <<< "$AGGREGATOR_MACHINES"
    local pids=()
    local remote_machines=()

    local binaries=(
        "${ROOT_DIR}/target/release/kafka-consumer"
        "${ROOT_DIR}/target/release/aggregator"
        "${ROOT_DIR}/target/release/kafka-producer"
    )

    for machine in "${machines[@]}"; do
        if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
            log_info "  Starting binary sync to $machine..."
            (
                # Ensure target/release directory exists on remote
                ssh "${SSH_USER}@${machine}" "mkdir -p ${REMOTE_PROJECT_PATH}/target/release" && \
                # Sync binaries
                rsync -az "${binaries[@]}" "${SSH_USER}@${machine}:${REMOTE_PROJECT_PATH}/target/release/"
            ) &
            pids+=($!)
            remote_machines+=("$machine")
        fi
    done

    if [[ ${#pids[@]} -eq 0 ]]; then
        log_info "  No remote machines to sync (all local)"
        return
    fi

    # Wait for all syncs to complete
    local failed=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log_info "  ✓ Binaries synced: ${remote_machines[$i]}"
        else
            log_warn "  ✗ Failed to sync binaries to ${remote_machines[$i]}"
            failed=$((failed + 1))
        fi
    done

    log_info "  ✓ Binaries synced to ${#remote_machines[@]} remote machine(s) ($failed failed)"
}

# Background log sync - continuously copies logs from remote machines
LOG_SYNC_PID=""
start_log_sync() {
    local run_id="$1"
    local num_agg="$2"
    local sync_interval="${3:-5}"  # Default: sync every 5 seconds

    read -ra machines <<< "$AGGREGATOR_MACHINES"
    local num_machines=${#machines[@]}

    # Create local log directory
    mkdir -p "$LOG_DIR"

    # Start background sync process
    (
        while true; do
            for i in $(seq 0 $((num_agg - 1))); do
                local machine_idx=$((i % num_machines))
                local machine="${machines[$machine_idx]}"

                # Skip localhost - logs already local
                if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
                    local remote_log_dir="${REMOTE_PROJECT_PATH}/bench_logs/distributed"

                    # Sync aggregator logs
                    local agg_log="aggregator_${run_id}_${i}.log"
                    scp -q "${SSH_USER}@${machine}:${remote_log_dir}/${agg_log}" "${LOG_DIR}/${agg_log}" 2>/dev/null || true

                    # Sync zkvm logs
                    local zkvm_log="zkvm_${run_id}_${i}.log"
                    scp -q "${SSH_USER}@${machine}:${remote_log_dir}/${zkvm_log}" "${LOG_DIR}/${zkvm_log}" 2>/dev/null || true
                fi
            done
            sleep "$sync_interval"
        done
    ) &
    LOG_SYNC_PID=$!
    log_info "Started background log sync (PID: $LOG_SYNC_PID, interval: ${sync_interval}s)"
}

stop_log_sync() {
    if [[ -n "$LOG_SYNC_PID" ]]; then
        kill "$LOG_SYNC_PID" 2>/dev/null || true
        wait "$LOG_SYNC_PID" 2>/dev/null || true
        LOG_SYNC_PID=""
        log_info "Stopped background log sync"
    fi
}

# Calculate total events across all sources
# Total = NUM_SOURCES * SERIES (keys per source) * SAMPLES_PER_SERIES (samples per key)
TOTAL_EVENTS=$((NUM_SOURCES * SERIES * SAMPLES_PER_SERIES))

# CSV Header
CSV_HEADER="timestamp,epoch_type,num_aggregators,num_sources,series,samples_per_series,total_events,total_events_bytes,total_events_kb,kafka_batch_size,commit_batch_size,rep,warmup_ms,produce_ms,consume_ms,prove_ms_max,verify_ms_max,total_ms,events_per_sec,mb_per_sec,avg_latency_ms,p99_latency_ms,avg_proof_ms,p99_proof_ms,proof_bytes_sum,proof_kb_sum,journal_bytes_sum,journal_kb_sum,epochs_proved,batches_processed,proof_overhead_ratio,data_to_proof_ratio,events_per_proof,memory_mb_total,notes"

echo "$CSV_HEADER" > "$CSV_FILE"

log_info "=== Distributed Aggregator Benchmark ==="
log_info "Machines: $AGGREGATOR_MACHINES"
log_info "Kafka brokers: $KAFKA_BROKERS"
[[ -n "$KAFKA_EXTERNAL_IP" ]] && log_info "Kafka external IP: $KAFKA_EXTERNAL_IP"
log_info "Series: $SERIES"
log_info "Samples per series: $SAMPLES_PER_SERIES"
log_info "Total events: $TOTAL_EVENTS (= $NUM_SOURCES sources * $SERIES keys/source * $SAMPLES_PER_SERIES samples/key)"
log_info "Num sources: $NUM_SOURCES (keys per source: $SERIES, events per source: $((SERIES * SAMPLES_PER_SERIES)))"
log_info "Aggregator counts to test: $NUM_AGGREGATORS_LIST"
log_info "Repeats: $REPEATS"
log_info "Epoch types to test: $EPOCH_TYPE_LIST"
log_info "Epoch batch threshold: $EPOCH_BATCH_THRESHOLD (total batches to trigger epoch)"
log_info "Epoch timeout: ${EPOCH_TIMEOUT_MS}ms"
if [[ "$ZK_AGGREGATION_TIMEOUT" -gt 0 ]]; then
    log_info "ZK aggregation timeout: ${ZK_AGGREGATION_TIMEOUT}s"
else
    log_info "ZK aggregation timeout: none (unlimited)"
fi
if [[ "$RISC0_DEV_MODE" == "1" ]]; then
    log_info "RISC0 dev mode: ENABLED (no actual proofs, fast testing)"
else
    log_info "RISC0 dev mode: disabled (real proofs)"
fi
log_info "Output: $CSV_FILE"
echo ""

# Check dependencies
check_deps() {
    log_info "Checking dependencies..."

    # Ensure KAFKA_EXTERNAL_IP is set correctly for distributed mode
    # Kafka runs on the first machine in AGGREGATOR_MACHINES
    read -ra kafka_machines <<< "$AGGREGATOR_MACHINES"
    local kafka_broker_machine="${kafka_machines[0]}"
    if [[ "$kafka_broker_machine" != "localhost" && "$kafka_broker_machine" != "127.0.0.1" ]]; then
        export KAFKA_EXTERNAL_IP="$kafka_broker_machine"
        log_info "Kafka broker machine: $kafka_broker_machine (KAFKA_EXTERNAL_IP=$KAFKA_EXTERNAL_IP)"
    else
        unset KAFKA_EXTERNAL_IP
        log_info "Kafka broker machine: localhost (local mode)"
    fi

    # Check Kafka (runs on first AGGREGATOR_MACHINES machine)
    local kafka_running=false
    local kafka_cmd_prefix=""

    if [[ "$kafka_broker_machine" == "localhost" || "$kafka_broker_machine" == "127.0.0.1" ]]; then
        # Check local Kafka
        if docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
            kafka_running=true
        fi
        kafka_cmd_prefix=""
    else
        # Check remote Kafka via SSH
        if ssh -o ConnectTimeout=5 "${SSH_USER}@${kafka_broker_machine}" "docker ps --format '{{.Names}}' | grep -q '^kafka$'" 2>/dev/null; then
            kafka_running=true
        fi
        kafka_cmd_prefix="ssh -o ConnectTimeout=5 ${SSH_USER}@${kafka_broker_machine}"
    fi

    if ! $kafka_running; then
        log_error "Kafka container not running on $kafka_broker_machine."
        if [[ -n "$KAFKA_EXTERNAL_IP" ]]; then
            log_info "Starting Kafka on $kafka_broker_machine with external IP: $KAFKA_EXTERNAL_IP"
            if [[ "$kafka_broker_machine" == "localhost" || "$kafka_broker_machine" == "127.0.0.1" ]]; then
                cd "$ROOT_DIR/scripts"
                KAFKA_EXTERNAL_IP="$KAFKA_EXTERNAL_IP" docker-compose -f docker-compose-kafka.yml up -d
                cd "$ROOT_DIR"
            else
                ssh -o ConnectTimeout=5 "${SSH_USER}@${kafka_broker_machine}" \
                    "cd ${REMOTE_PROJECT_PATH}/scripts && KAFKA_EXTERNAL_IP=$KAFKA_EXTERNAL_IP docker-compose -f docker-compose-kafka.yml up -d"
            fi
            sleep 5
        else
            log_error "Start Kafka on $kafka_broker_machine with: docker-compose -f scripts/docker-compose-kafka.yml up -d"
            exit 1
        fi
    else
        # Kafka is running - check if it has correct advertised listeners for distributed mode
        if [[ -n "$KAFKA_EXTERNAL_IP" ]]; then
            # Check the actual KAFKA_ADVERTISED_LISTENERS environment variable in the container
            local current_advertised
            if [[ "$kafka_broker_machine" == "localhost" || "$kafka_broker_machine" == "127.0.0.1" ]]; then
                current_advertised=$(docker exec kafka env | grep KAFKA_ADVERTISED_LISTENERS || true)
            else
                current_advertised=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${kafka_broker_machine}" \
                    "docker exec kafka env | grep KAFKA_ADVERTISED_LISTENERS" 2>/dev/null || true)
            fi

            if [[ -z "$current_advertised" ]] || ! echo "$current_advertised" | grep -q "$KAFKA_EXTERNAL_IP"; then
                log_warn "Kafka is not configured with correct external IP"
                log_warn "Current: $current_advertised"
                log_warn "Expected: PLAINTEXT_HOST://$KAFKA_EXTERNAL_IP:9092"
                log_warn "Restarting Kafka with correct configuration..."
                if [[ "$kafka_broker_machine" == "localhost" || "$kafka_broker_machine" == "127.0.0.1" ]]; then
                    cd "$ROOT_DIR/scripts"
                    docker-compose -f docker-compose-kafka.yml down
                    KAFKA_EXTERNAL_IP="$KAFKA_EXTERNAL_IP" docker-compose -f docker-compose-kafka.yml up -d
                    cd "$ROOT_DIR"
                else
                    ssh -o ConnectTimeout=5 "${SSH_USER}@${kafka_broker_machine}" \
                        "cd ${REMOTE_PROJECT_PATH}/scripts && docker-compose -f docker-compose-kafka.yml down && KAFKA_EXTERNAL_IP=$KAFKA_EXTERNAL_IP docker-compose -f docker-compose-kafka.yml up -d"
                fi
                sleep 5
            else
                log_info "Kafka already configured with correct external IP: $KAFKA_EXTERNAL_IP"
            fi
        fi
    fi

    # Check FDB
    if ! fdbcli --exec "status minimal" 2>&1 | grep -q "available\|Healthy"; then
        log_warn "FDB may not be healthy. Check with: fdbcli --exec 'status'"
    fi

    # Build binaries
    log_info "Building binaries..."
    local build_output

    build_output=$(cargo build --release -p data_source --features "kafka" 2>&1) || {
        log_error "Failed to build data_source:"
        echo "$build_output"
        exit 1
    }

    build_output=$(cargo build --release -p aggregator --features "kafka fdb" 2>&1) || {
        log_error "Failed to build aggregator:"
        echo "$build_output"
        exit 1
    }

    log_info "Dependencies OK"
}

# Setup Kafka topic with correct partitions
setup_kafka_topic() {
    local partitions="$1"

    log_info "Setting up Kafka topic: $KAFKA_TOPIC with $partitions partitions..."

    # Delete existing topic (this also removes consumer group assignments)
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic "$KAFKA_TOPIC" 2>/dev/null || true

    # Delete consumer group to clear stale members
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$KAFKA_GROUP_ID" --delete 2>/dev/null || true

    # Wait for topic to be fully deleted
    log_info "Waiting for topic deletion to complete..."
    local wait_count=0
    while docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$"; do
        sleep 1
        wait_count=$((wait_count + 1))
        if [[ $wait_count -ge 30 ]]; then
            log_warn "Topic deletion taking too long, proceeding anyway"
            break
        fi
    done
    log_info "Topic deleted after ${wait_count}s"

    # Create fresh topic (offsets start at 0, no need to reset)
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$KAFKA_TOPIC" \
        --partitions "$partitions" \
        --replication-factor 1 \
        --config retention.ms=3600000

    # Verify topic was created with correct partitions
    local actual_partitions=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --describe --topic "$KAFKA_TOPIC" 2>/dev/null | grep -c "Partition:")
    if [[ "$actual_partitions" -ne "$partitions" ]]; then
        log_warn "Topic created with $actual_partitions partitions, expected $partitions"
    fi

    # Reset consumer group offsets to earliest (ensure clean start)
    # Note: Consumer group must not have active members for this to work
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$KAFKA_GROUP_ID" --reset-offsets --to-earliest \
        --topic "$KAFKA_TOPIC" --execute 2>/dev/null || true

    log_info "Kafka topic ready ($actual_partitions partitions, offsets reset)"
}

# Reset FDB subspace
reset_fdb() {
    log_info "Resetting FDB subspace: $FDB_SUBSPACE..."
    fdbcli --exec "clearrange \\x01${FDB_SUBSPACE} \\x01${FDB_SUBSPACE}\\xff" 2>/dev/null || true
}

# Execute command on a machine (local or remote)
exec_on_machine() {
    local machine="$1"
    shift
    local cmd="$@"

    if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
        bash -c "$cmd"
    else
        ssh "${SSH_USER}@${machine}" "$cmd"
    fi
}

# Start aggregator instances
start_aggregators() {
    local num="$1"
    local run_id="$2"
    local pids_and_hosts=()

    # Convert AGGREGATOR_MACHINES to array
    read -ra machines <<< "$AGGREGATOR_MACHINES"
    local num_machines=${#machines[@]}

    # First, ensure no stale kafka-consumer processes are running
    log_info "Checking for stale kafka-consumer processes..." >&2
    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            local stale_count=$(pgrep -c -f 'kafka-consumer' 2>/dev/null || echo 0)
            if [[ "$stale_count" -gt 0 ]]; then
                log_warn "  Found $stale_count stale kafka-consumer on localhost, killing..." >&2
                pkill -9 -f 'kafka-consumer' 2>/dev/null || true
                sleep 1
            fi
        else
            local stale_count=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "pgrep -c -f kafka-consumer 2>/dev/null || echo 0" 2>/dev/null || echo 0)
            if [[ "$stale_count" -gt 0 ]]; then
                log_warn "  Found $stale_count stale kafka-consumer on $machine, killing..." >&2
                ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "pkill -9 -f kafka-consumer" 2>/dev/null || true
            fi
        fi
    done
    sleep 1  # Brief pause after killing any stale processes

    log_info "Starting $num kafka-consumer instance(s) across $num_machines machine(s)..." >&2

    for i in $(seq 0 $((num - 1))); do
        # Round-robin assignment to machines
        local machine_idx=$((i % num_machines))
        local machine="${machines[$machine_idx]}"

        local log_file="${LOG_DIR}/aggregator_${run_id}_${i}.log"

        # Each machine uses the same local path (not indexed)
        # Multiple machines each use /mydata/rocksdb locally
        local raw_db="${RAW_ROCKSDB_BASE}"

        # Create log directory locally
        mkdir -p "$LOG_DIR"

        # Start aggregator on the assigned machine
        # For distributed testing: only "localhost" and "127.0.0.1" are treated as local
        # All IP addresses (even if they match local IPs) use SSH for proper distributed setup
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            # Local execution
            mkdir -p "$raw_db"

            # Run in subshell to ensure PATH is set for cargo
            (
                # Source cargo env if available
                [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"

                export KAFKA_BROKERS="$KAFKA_BROKERS"
                export KAFKA_TOPIC="$KAFKA_TOPIC"
                export KAFKA_GROUP_ID="$KAFKA_GROUP_ID"
                export KAFKA_PARTITION_ID="$i"  # Deterministic: aggregator N consumes partition N
                export AGGREGATOR_ID="$i"       # Same as partition for FDB key uniqueness
                export RAW_DB_PATH="$raw_db"
                export FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE"
                export FDB_SUBSPACE="$FDB_SUBSPACE"
                export RAYON_NUM_THREADS="${THREADS:-8}"
                export EPOCH_BATCH_THRESHOLD="$EPOCH_BATCH_THRESHOLD"
                export EPOCH_TIMEOUT_MS="$EPOCH_TIMEOUT_MS"

                "${ROOT_DIR}/target/release/kafka-consumer" \
                    > "$log_file" 2>&1
            ) &

            local pid=$!
            pids_and_hosts+=("localhost:$pid")
            log_info "  Kafka-consumer $i started on localhost (PID: $pid, log: $log_file)" >&2
        else
            # Remote execution via SSH
            local remote_log_dir="${REMOTE_PROJECT_PATH}/bench_logs/distributed"
            local remote_log="${remote_log_dir}/aggregator_${run_id}_${i}.log"
            local remote_cmd="cd $REMOTE_PROJECT_PATH && mkdir -p $raw_db $remote_log_dir && \
                ( [[ -f \$HOME/.cargo/env ]] && source \$HOME/.cargo/env; \
                export KAFKA_BROKERS='$KAFKA_BROKERS'; \
                export KAFKA_TOPIC='$KAFKA_TOPIC'; \
                export KAFKA_GROUP_ID='$KAFKA_GROUP_ID'; \
                export KAFKA_PARTITION_ID='$i'; \
                export AGGREGATOR_ID='$i'; \
                export RAW_DB_PATH='$raw_db'; \
                export FDB_CLUSTER_FILE='$FDB_CLUSTER_FILE'; \
                export FDB_SUBSPACE='$FDB_SUBSPACE'; \
                export RAYON_NUM_THREADS='${THREADS:-56}'; \
                export EPOCH_BATCH_THRESHOLD='$EPOCH_BATCH_THRESHOLD'; \
                export EPOCH_TIMEOUT_MS='$EPOCH_TIMEOUT_MS'; \
                nohup ${REMOTE_PROJECT_PATH}/target/release/kafka-consumer \
                    > ${remote_log} 2>&1 & echo \$! )"

            local pid=$(ssh "${SSH_USER}@${machine}" "$remote_cmd" 2>&1 | tail -1 | tr -d ' ')
            pids_and_hosts+=("$machine:$pid")
            log_info "  Kafka-consumer $i started on $machine (PID: $pid, remote log: $remote_log)" >&2
        fi
    done

    # Return space-separated list of "machine:pid"
    echo "${pids_and_hosts[@]}"
}

# Stop processes (kafka-consumers or aggregators)
stop_processes() {
    local pids_and_hosts="$1"
    local process_name="${2:-processes}"
    local force="${3:-true}"  # Default to force kill

    # Note: caller should log "Stopping X..." before calling this function

    # Track unique machines for cleanup
    declare -A machines_seen

    local kill_signal=""
    if [[ "$force" == "true" ]]; then
        kill_signal="-9"
    fi

    for entry in $pids_and_hosts; do
        IFS=':' read -r machine pid <<< "$entry"

        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            kill $kill_signal "$pid" 2>/dev/null || true
        else
            # Use timeout to prevent hanging on unresponsive machines
            timeout 10 ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "kill $kill_signal $pid 2>/dev/null || true" &
        fi

        machines_seen["$machine"]=1
    done

    # Wait for SSH commands to complete
    wait 2>/dev/null || true

    # Brief pause for cleanup
    sleep 1
}

# Backward compatibility alias
stop_aggregators() {
    stop_processes "$1" "aggregators"
}

# Signal kafka-consumers to flush all pending batches as epochs (SIGUSR1)
# This creates the final epoch with remaining data without stopping the consumers
signal_flush_epochs() {
    local pids_and_hosts="$1"

    log_info "Signaling kafka-consumers to flush pending epochs (SIGUSR1)..."

    local ssh_pids=()
    for entry in $pids_and_hosts; do
        IFS=':' read -r machine pid <<< "$entry"

        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            kill -USR1 "$pid" 2>/dev/null || log_warn "  Failed to signal local PID $pid"
        else
            # Send SIGUSR1 to remote process
            timeout 10 ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "kill -USR1 $pid 2>/dev/null || true" &
            ssh_pids+=($!)
        fi
    done

    # Wait only for the SSH commands we spawned (not other background jobs like log sync)
    for ssh_pid in "${ssh_pids[@]}"; do
        wait "$ssh_pid" 2>/dev/null || true
    done

    # Wait for flush to complete (epoch creation + RocksDB write)
    log_info "Waiting 3s for epoch flush to complete..."
    sleep 3
}

# Cleanup buffer directories on all machines
cleanup_buffer_directories() {
    local pids_and_hosts="$1"

    # Note: caller should log before calling this function

    # Track unique machines for cleanup
    declare -A machines_seen

    for entry in $pids_and_hosts; do
        IFS=':' read -r machine pid <<< "$entry"
        machines_seen["$machine"]=1
    done

    # Cleanup RocksDB directories on all machines
    for machine in "${!machines_seen[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            rm -rf "${RAW_ROCKSDB_BASE}_"* "${RAW_ROCKSDB_SECONDARY_BASE}_"* 2>/dev/null || true
        else
            # Use timeout to prevent hanging on unresponsive machines
            timeout 30 ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "rm -rf ${RAW_ROCKSDB_BASE} ${RAW_ROCKSDB_BASE}_* ${RAW_ROCKSDB_SECONDARY_BASE} ${RAW_ROCKSDB_SECONDARY_BASE}_* 2>/dev/null || true" &
        fi
    done

    # Wait with timeout for background SSH commands
    wait 2>/dev/null || true
}

# Produce events to Kafka
# Supports multiple data sources: each source has its own hash chain
# Events are split evenly across sources
produce_events() {
    local events="$1"
    local run_id="$2"
    local num_agg="${3:-1}"
    local num_sources="${NUM_SOURCES:-1}"

    log_info "Producing $events events to Kafka (num_aggregators=$num_agg, num_sources=$num_sources)..." >&2

    local start_ms=$(date +%s%3N)
    local events_per_source=$((events / num_sources))
    local pids=()

    # Use pre-built binary to avoid cargo lock contention when running in parallel
    local producer_bin="${ROOT_DIR}/target/release/kafka-producer"

    # Launch producers for each source (in parallel if multiple sources)
    for ((src_id=0; src_id<num_sources; src_id++)); do
        local log_file="${LOG_DIR}/producer_${run_id}_src${src_id}.log"

        (
            KAFKA_BROKERS="$KAFKA_BROKERS" \
            KAFKA_TOPIC="$KAFKA_TOPIC" \
            NUM_AGGREGATORS="$num_agg" \
            SOURCE_ID="$src_id" \
            "$producer_bin" \
                --events "$events_per_source" --kafka-batch-size "$KAFKA_BATCH_SIZE" \
                --commit-batch-size "$COMMIT_BATCH_SIZE" --series "$SERIES" \
                --source-id "$src_id" \
                > "$log_file" 2>&1
        ) &
        pids+=($!)
    done

    # Wait for all producers to finish
    for pid in "${pids[@]}"; do
        wait "$pid"
    done

    local end_ms=$(date +%s%3N)
    local duration_ms=$((end_ms - start_ms))

    echo "$duration_ms"
}

# Wait for consumers to be ready
# Note: With manual partition assignment (KAFKA_PARTITION_ID), consumers don't register
# with the consumer group coordinator, so we check process logs instead.
wait_for_consumers() {
    local expected_consumers="$1"
    local run_id="$2"
    local timeout_sec="${3:-60}"  # Increased default timeout
    local start_time=$(date +%s)

    log_info "Waiting for $expected_consumers consumer(s) to be ready..."

    read -ra machines <<< "$AGGREGATOR_MACHINES"
    local num_machines=${#machines[@]}

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout_sec ]]; then
            log_error "Timeout waiting for consumers after ${timeout_sec}s"
            # Show which consumers are not ready
            for i in $(seq 0 $((expected_consumers - 1))); do
                local machine_idx=$((i % num_machines))
                local machine="${machines[$machine_idx]}"
                local remote_log="${REMOTE_PROJECT_PATH}/bench_logs/distributed/aggregator_${run_id}_${i}.log"
                local status=$(ssh -o ConnectTimeout=2 "${SSH_USER}@${machine}" \
                    "grep -c 'kafka-consumer.*started' '$remote_log' 2>/dev/null || echo 0" 2>/dev/null || echo "SSH failed")
                log_info "  Consumer $i on $machine: started count = $status"
            done
            return 1
        fi

        # With manual partition assignment, check if consumers are running by looking for
        # "started" AND "assigned to partition" in their logs
        local ready_count=0
        local ready_list=""
        for i in $(seq 0 $((expected_consumers - 1))); do
            local machine_idx=$((i % num_machines))
            local machine="${machines[$machine_idx]}"

            if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
                # Check local log
                local log_file="${LOG_DIR}/aggregator_${run_id}_${i}.log"
                if [[ -f "$log_file" ]] && grep -q "kafka-consumer.*started" "$log_file" 2>/dev/null; then
                    ready_count=$((ready_count + 1))
                    ready_list="${ready_list}${i},"
                fi
            else
                # Check remote log via SSH - look for "started" which indicates partition is assigned
                local remote_log="${REMOTE_PROJECT_PATH}/bench_logs/distributed/aggregator_${run_id}_${i}.log"
                if ssh -o ConnectTimeout=2 "${SSH_USER}@${machine}" \
                    "grep -q 'kafka-consumer.*started' '$remote_log' 2>/dev/null" 2>/dev/null; then
                    ready_count=$((ready_count + 1))
                    ready_list="${ready_list}${i},"
                fi
            fi
        done

        if [[ $ready_count -ge $expected_consumers ]]; then
            log_info "All $ready_count consumer(s) ready and assigned to partitions"
            return 0
        fi

        # Log progress every 10 seconds
        if [[ $((elapsed % 10)) -eq 0 ]] && [[ $elapsed -gt 0 ]]; then
            log_info "  Progress: $ready_count/$expected_consumers consumers ready (${elapsed}s elapsed)..."
        fi

        sleep 1
    done
}

# Wait for all events to be consumed
wait_for_consumption() {
    local expected_events="$1"
    local timeout_sec="${2:-300}"
    local start_time=$(date +%s)

    log_info "Waiting for consumption of $expected_events events (timeout: ${timeout_sec}s)..."

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout_sec ]]; then
            log_warn "Timeout waiting for consumption"
            return 1
        fi

        # Check consumer lag
        # Determine where Kafka is running (first machine in AGGREGATOR_MACHINES)
        read -ra machines <<< "$AGGREGATOR_MACHINES"
        local kafka_machine="${machines[0]}"

        local lag
        if [[ "$kafka_machine" == "localhost" || "$kafka_machine" == "127.0.0.1" ]]; then
            # Local Kafka
            lag=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
                --group "$KAFKA_GROUP_ID" --describe 2>/dev/null | \
                awk 'NR>1 {sum += $6} END {print sum}' || echo "999999")
        else
            # Remote Kafka
            lag=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${kafka_machine}" \
                "docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
                --group '$KAFKA_GROUP_ID' --describe 2>/dev/null | \
                awk 'NR>1 {sum += \\\$6} END {print sum}'" 2>/dev/null || echo "999999")
        fi

        if [[ "$lag" == "0" ]] || [[ -z "$lag" ]]; then
            log_info "All events consumed"
            return 0
        fi

        sleep 1
    done
}

# Get memory usage of aggregator processes
get_memory_usage() {
    local pids_and_hosts="$1"
    local total_kb=0

    for entry in $pids_and_hosts; do
        IFS=':' read -r machine pid <<< "$entry"

        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            # Local process
            if [[ -f "/proc/$pid/status" ]]; then
                local rss=$(grep VmRSS /proc/$pid/status 2>/dev/null | awk '{print $2}' || echo 0)
                total_kb=$((total_kb + rss))
            fi
        else
            # Remote process
            local rss=$(ssh "${SSH_USER}@${machine}" \
                "if [[ -f /proc/$pid/status ]]; then grep VmRSS /proc/$pid/status 2>/dev/null | awk '{print \$2}'; else echo 0; fi" 2>/dev/null || echo 0)
            total_kb=$((total_kb + rss))
        fi
    done

    echo $((total_kb / 1024))
}

# Run single benchmark
run_benchmark() {
    local num_agg="$1"
    local rep="$2"
    local run_id="${EPOCH_TYPE}_${num_agg}agg_rep${rep}_${TIMESTAMP}"

    log_info ""
    log_info "=== Running: $num_agg aggregators, epoch_type=$EPOCH_TYPE, rep $rep ==="

    # Clean RocksDB on all machines before each run to ensure clean state
    cleanup_rocksdb_all_machines

    # Setup - use num_agg partitions so partition N → aggregator N (deterministic mapping)
    setup_kafka_topic "$num_agg"
    reset_fdb

    # Start aggregators
    local agg_pids=$(start_aggregators "$num_agg" "$run_id")

    # Start background log sync for real-time remote log access
    start_log_sync "$run_id" "$num_agg" 5

    # Wait for aggregators/consumers to join the consumer group
    wait_for_consumers "$num_agg" "$run_id" 60 || log_warn "Not all consumers ready"

    # Allow consumer group to stabilize (partition assignment can take a few seconds)
    log_info "Waiting for consumer group to stabilize..."
    sleep 5

    # Warmup
    if [[ $WARMUP_EVENTS -gt 0 ]]; then
        log_info "Warmup: producing $WARMUP_EVENTS events..."
        produce_events "$WARMUP_EVENTS" "${run_id}_warmup" "$num_agg" >/dev/null
        wait_for_consumption "$WARMUP_EVENTS" 60 || true
        sleep 2
    fi

    # Reset offsets for measurement
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$KAFKA_GROUP_ID" --reset-offsets --to-latest \
        --topic "$KAFKA_TOPIC" --execute 2>/dev/null || true

    # Benchmark run
    local total_start_ms=$(date +%s%3N)

    local produce_ms=$(produce_events "$TOTAL_EVENTS" "$run_id" "$num_agg")

    local consume_start_ms=$(date +%s%3N)
    wait_for_consumption "$TOTAL_EVENTS" 600 || log_warn "Consumption incomplete"
    local consume_end_ms=$(date +%s%3N)

    # Wait for kafka-consumers to finish writing all batches to RocksDB
    # Kafka lag=0 means all messages are read, but writes may still be pending
    if [[ "$CONSUMER_SETTLE_SEC" -gt 0 ]]; then
        log_info "Waiting ${CONSUMER_SETTLE_SEC}s for kafka-consumers to finish writing to RocksDB..."
        sleep "$CONSUMER_SETTLE_SEC"
    fi

    # DON'T stop kafka-consumers - keep them running while ZK aggregation happens
    # ZK aggregators will read from secondary RocksDB instance

    # Debug: Inspect RocksDB after consumption
    log_info "Debug: Checking RocksDB contents after consumption..."

    # Convert AGGREGATOR_MACHINES to array for distributed checks
    read -ra check_machines <<< "$AGGREGATOR_MACHINES"
    local check_num_machines=${#check_machines[@]}

    for i in $(seq 0 $((num_agg - 1))); do
        local machine_idx=$((i % check_num_machines))
        local machine="${check_machines[$machine_idx]}"
        local raw_db="${RAW_ROCKSDB_BASE}"  # Unindexed path used by kafka-consumers

        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            # Check local RocksDB
            if [[ -f "$raw_db/CURRENT" ]]; then
                local file_count=$(ls -1 "$raw_db" | wc -l)
                local db_size=$(du -sh "$raw_db" 2>/dev/null | cut -f1)
                log_info "  RocksDB $i (localhost): $file_count files, size: $db_size"
            else
                log_warn "  RocksDB $i (localhost): CURRENT file not found at $raw_db"
            fi
        else
            # Check remote RocksDB via SSH
            local remote_check=$(ssh "${SSH_USER}@${machine}" \
                "if [[ -f $raw_db/CURRENT ]]; then \
                    echo \"found:\$(ls -1 $raw_db 2>/dev/null | wc -l):\$(du -sh $raw_db 2>/dev/null | cut -f1)\"; \
                else \
                    echo 'notfound'; \
                fi" 2>/dev/null || echo "error")

            if [[ "$remote_check" == "error" ]]; then
                log_warn "  RocksDB $i ($machine): Failed to check via SSH"
            elif [[ "$remote_check" == "notfound" ]]; then
                log_warn "  RocksDB $i ($machine): CURRENT file not found at $raw_db"
            else
                IFS=':' read -r _ file_count db_size <<< "$remote_check"
                log_info "  RocksDB $i ($machine): $file_count files, size: $db_size"
            fi
        fi
    done

    # Signal kafka-consumers to flush all pending batches as final epochs
    # This ensures all data is written as epochs before ZK aggregators start
    signal_flush_epochs "$agg_pids"

    # Start ZK aggregation phase (optional, controlled by ENABLE_ZK_AGGREGATION)
    local prove_ms=0
    local verify_ms=0
    local avg_proof_ms=0
    local p99_proof_ms=0
    local proof_bytes_last=0
    local proof_bytes_max=0
    local journal_bytes_last=0
    local proof_kb_last="0"
    local proof_kb_max="0"
    local journal_kb_last="0"

    if [[ "${ENABLE_ZK_AGGREGATION:-1}" == "1" ]]; then
        # Calculate expected epoch count per aggregator for logging
        # Formula: total_batches_per_aggregator = (NUM_SOURCES / num_agg) * SERIES * (SAMPLES_PER_SERIES / COMMIT_BATCH_SIZE)
        #          expected_epochs = total_batches_per_aggregator / EPOCH_BATCH_THRESHOLD
        local batches_per_key=$(( SAMPLES_PER_SERIES / COMMIT_BATCH_SIZE ))
        local sources_per_agg=$(( NUM_SOURCES / num_agg ))
        [[ $sources_per_agg -lt 1 ]] && sources_per_agg=1
        local total_batches_per_agg=$(( sources_per_agg * SERIES * batches_per_key ))
        local expected_epochs=$(( total_batches_per_agg / EPOCH_BATCH_THRESHOLD ))
        [[ $expected_epochs -lt 1 ]] && expected_epochs=1
        log_info "Starting ZK aggregation (kafka-consumers still running)..."
        log_info "  Expected epochs per aggregator: $expected_epochs (batches_per_agg=$total_batches_per_agg / EPOCH_BATCH_THRESHOLD=$EPOCH_BATCH_THRESHOLD)"

        # Wait for RocksDB primary databases to be initialized
        log_info "Waiting for RocksDB primary databases to be initialized..."
        read -ra check_machines <<< "$AGGREGATOR_MACHINES"
        local check_num_machines=${#check_machines[@]}

        for i in $(seq 0 $((num_agg - 1))); do
            local machine_idx=$((i % check_num_machines))
            local machine="${check_machines[$machine_idx]}"
            local raw_db_primary="${RAW_ROCKSDB_BASE}"
            local timeout=30
            local elapsed=0

            if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
                # Local check
                while [[ ! -f "$raw_db_primary/CURRENT" ]] && [[ $elapsed -lt $timeout ]]; do
                    sleep 1
                    elapsed=$((elapsed + 1))
                done
                if [[ ! -f "$raw_db_primary/CURRENT" ]]; then
                    log_error "Timeout waiting for RocksDB primary at $raw_db_primary on localhost"
                    return 1
                fi
            else
                # Remote check via SSH
                while [[ $elapsed -lt $timeout ]]; do
                    if ssh "${SSH_USER}@${machine}" "test -f '$raw_db_primary/CURRENT'" 2>/dev/null; then
                        break
                    fi
                    sleep 1
                    elapsed=$((elapsed + 1))
                done
                if [[ $elapsed -ge $timeout ]]; then
                    log_error "Timeout waiting for RocksDB primary at $raw_db_primary on $machine"
                    return 1
                fi
            fi
            log_info "RocksDB primary $i initialized at $raw_db_primary on $machine"
        done

        local prove_start_ms=$(date +%s%3N)
        local zkvm_pids=()

        # Convert AGGREGATOR_MACHINES to array (same as kafka-consumer startup)
        read -ra machines <<< "$AGGREGATOR_MACHINES"
        local num_machines=${#machines[@]}

        # Start ZK aggregators to process events from RocksDB secondary
        # Each ZK aggregator runs on the same machine as its kafka-consumer
        for i in $(seq 0 $((num_agg - 1))); do
            # Determine which machine this aggregator runs on (same as kafka-consumer)
            local machine_idx=$((i % num_machines))
            local machine="${machines[$machine_idx]}"

            # Each machine uses the same local paths (not indexed)
            local raw_db_primary="${RAW_ROCKSDB_BASE}"
            local raw_db_secondary="${RAW_ROCKSDB_SECONDARY_BASE}"
            local agg_db="${RAW_ROCKSDB_BASE}_agg"
            local zkvm_log="${LOG_DIR}/zkvm_${run_id}_${i}.log"

            # For distributed testing: only "localhost" and "127.0.0.1" are treated as local
            # All IP addresses (even if they match local IPs) use SSH for proper distributed setup
            # This ensures ZK aggregators run on remote machines, not on the control machine
            #
            # Epoch discovery: Aggregator automatically discovers available epochs from RocksDB.
            # Each aggregator scans its local RocksDB for sample_shard_frames and processes
            # all found epochs. No START_SEQ/END_SEQ needed - aggregator reports epochs_proved at completion.
            if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
                # Local execution
                mkdir -p "$agg_db" "$raw_db_secondary"

                (
                    [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
                    export RAW_ROCKSDB_PATH="$raw_db_primary"
                    export RAW_ROCKSDB_SECONDARY_PATH="$raw_db_secondary"
                    export AGG_ROCKSDB_PATH="$agg_db"
                    export AGGREGATOR_ID="$i"  # Unique ID for FDB key namespacing
                    export AGGR_IDLE_TIMEOUT_SECS=300  # Exit quickly when no more epochs
                    [[ "$RISC0_DEV_MODE" == "1" ]] && export RISC0_DEV_MODE=1
                    unset FDB_CLUSTER_FILE  # Disable FDB writes
                    # Let aggregator determine epoch range from RocksDB
                    "${ROOT_DIR}/target/release/aggregator" \
                        --rocksdb --mode "$EPOCH_TYPE" \
                        > "$zkvm_log" 2>&1
                ) &
                local pid=$!
                zkvm_pids+=("localhost:$pid")
                log_info "  ZK aggregator $i started on localhost (PID: $pid, log: $zkvm_log)"
            else
                # Remote execution via SSH
                local remote_log_dir="${REMOTE_PROJECT_PATH}/bench_logs/distributed"
                local remote_log="${remote_log_dir}/zkvm_${run_id}_${i}.log"

                # Build remote command
                # Epoch discovery: aggregator auto-discovers epochs from RocksDB and reports epochs_proved
                local dev_mode_export=""
                [[ "$RISC0_DEV_MODE" == "1" ]] && dev_mode_export="export RISC0_DEV_MODE=1;"
                local remote_cmd="cd ${REMOTE_PROJECT_PATH} && mkdir -p $agg_db $raw_db_secondary $remote_log_dir && \
                    ( [[ -f \$HOME/.cargo/env ]] && source \$HOME/.cargo/env; \
                    export RAW_ROCKSDB_PATH='$raw_db_primary'; \
                    export RAW_ROCKSDB_SECONDARY_PATH='$raw_db_secondary'; \
                    export AGG_ROCKSDB_PATH='$agg_db'; \
                    export AGGREGATOR_ID='$i'; \
                    export AGGR_IDLE_TIMEOUT_SECS=300; \
                    ${dev_mode_export} \
                    unset FDB_CLUSTER_FILE; \
                    nohup ${REMOTE_PROJECT_PATH}/target/release/aggregator \
                        --rocksdb --mode '$EPOCH_TYPE' \
                        > ${remote_log} 2>&1 & echo \$! )"

                # Execute on remote machine and capture the remote PID
                local remote_pid=$(ssh "${SSH_USER}@${machine}" "$remote_cmd" 2>&1 | tail -1 | tr -d ' ')
                zkvm_pids+=("${machine}:${remote_pid}")
                log_info "  ZK aggregator $i started on $machine (PID: $remote_pid, remote log: $remote_log)"
            fi
        done

        # Wait for all ZK aggregators to complete
        if [[ "$ZK_AGGREGATION_TIMEOUT" -gt 0 ]]; then
            log_info "Waiting for ZK aggregators to complete (timeout: ${ZK_AGGREGATION_TIMEOUT}s)..."
        else
            log_info "Waiting for ZK aggregators to complete (no timeout)..."
        fi

        local check_interval=30  # Check every 30 seconds
        local status_interval=60  # Print full status every 60 seconds
        local last_status_time=0
        local wait_start_time=$(date +%s)

        # Track completion status for each aggregator
        declare -A completed_aggs

        while true; do
            # Check timeout
            if [[ "$ZK_AGGREGATION_TIMEOUT" -gt 0 ]]; then
                local elapsed=$(($(date +%s) - wait_start_time))
                if [[ $elapsed -ge $ZK_AGGREGATION_TIMEOUT ]]; then
                    log_warn "ZK aggregation timeout reached (${ZK_AGGREGATION_TIMEOUT}s elapsed)"
                    break
                fi
            fi
            local all_done=true
            local running_count=0
            local r0vm_count=0
            local status_lines=()

            # Check all aggregators
            for entry in "${zkvm_pids[@]}"; do
                IFS=':' read -r machine pid <<< "$entry"

                # Skip already completed
                if [[ "${completed_aggs[$entry]:-}" == "1" ]]; then
                    continue
                fi

                if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
                    # Check local process
                    if kill -0 "$pid" 2>/dev/null; then
                        all_done=false
                        running_count=$((running_count + 1))
                        # Use [r]0vm pattern to avoid matching the pgrep command itself
                        local has_r0vm=$(pgrep -f '[r]0vm' >/dev/null 2>&1 && echo "yes" || echo "no")
                        [[ "$has_r0vm" == "yes" ]] && r0vm_count=$((r0vm_count + 1))
                        status_lines+=("localhost: running (r0vm: $has_r0vm)")
                    else
                        completed_aggs[$entry]=1
                        log_info "  ZK aggregator completed on localhost (PID: $pid)"
                    fi
                else
                    # Check remote process
                    local agg_running=$(ssh "${SSH_USER}@${machine}" "kill -0 $pid 2>/dev/null && echo 1 || echo 0" 2>/dev/null || echo "0")
                    # Use [r]0vm pattern to avoid matching the pgrep/ssh command itself
                    local r0vm_running=$(ssh "${SSH_USER}@${machine}" "pgrep -f '[r]0vm' >/dev/null 2>&1 && echo 1 || echo 0" 2>/dev/null || echo "0")

                    if [[ "$agg_running" == "1" ]] || [[ "$r0vm_running" == "1" ]]; then
                        all_done=false
                        [[ "$agg_running" == "1" ]] && running_count=$((running_count + 1))
                        [[ "$r0vm_running" == "1" ]] && r0vm_count=$((r0vm_count + 1))
                        status_lines+=("$machine: agg=$([ "$agg_running" == "1" ] && echo "yes" || echo "no") r0vm=$([ "$r0vm_running" == "1" ] && echo "yes" || echo "no")")
                    else
                        completed_aggs[$entry]=1
                        log_info "  ZK aggregator completed on $machine (PID: $pid)"
                    fi
                fi
            done

            # Exit if all done
            if [[ "$all_done" == "true" ]]; then
                break
            fi

            # Print consolidated status periodically
            local now=$(date +%s)
            if [[ $((now - last_status_time)) -ge $status_interval ]]; then
                log_info "Status: $running_count aggregators running, $r0vm_count with r0vm active"
                for line in "${status_lines[@]}"; do
                    log_info "  $line"
                done
                last_status_time=$now
            fi

            sleep $check_interval
        done

        local prove_end_ms=$(date +%s%3N)
        prove_ms=$((prove_end_ms - prove_start_ms))
        log_info "ZK aggregation complete: ${prove_ms}ms"

        # Extract metrics from ZK aggregator logs
        # First, copy remote logs to local machine
        read -ra machines <<< "$AGGREGATOR_MACHINES"
        local num_machines=${#machines[@]}

        for i in $(seq 0 $((num_agg - 1))); do
            local machine_idx=$((i % num_machines))
            local machine="${machines[$machine_idx]}"
            local zkvm_log="${LOG_DIR}/zkvm_${run_id}_${i}.log"

            # Copy remote log to local machine if needed
            if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
                local remote_log_dir="${REMOTE_PROJECT_PATH}/bench_logs/distributed"
                local remote_log="${remote_log_dir}/zkvm_${run_id}_${i}.log"
                scp -q "${SSH_USER}@${machine}:${remote_log}" "$zkvm_log" 2>/dev/null || \
                    log_warn "Failed to copy log from $machine aggregator $i"
            fi
        done

        # Now extract metrics from local copies
        # Track per-aggregator metrics
        local total_epochs_proved=0
        local total_events_proved=0   # Sum of n_events across all aggregators
        local max_prove_ms=0          # Max among aggregators (parallel execution)
        local max_verify_ms=0         # Max among aggregators (parallel execution)
        local total_proof_bytes=0     # Sum among aggregators
        local total_journal_bytes=0   # Sum among aggregators
        local total_batches=0         # Sum among aggregators

        log_info "  Per-aggregator metrics:"
        for i in $(seq 0 $((num_agg - 1))); do
            local zkvm_log="${LOG_DIR}/zkvm_${run_id}_${i}.log"
            if [[ -f "$zkvm_log" ]]; then
                # Note: Use -a flag to handle binary characters (ANSI escape codes from warnings)
                # Extract epochs_proved from each aggregator
                local agg_epochs=$(grep -aoP 'epochs_proved=\K[0-9]+' "$zkvm_log" | tail -1 || echo "0")
                total_epochs_proved=$((total_epochs_proved + agg_epochs))

                # Sum prove_ms for this aggregator
                local agg_prove_ms=0
                while IFS= read -r ms; do
                    agg_prove_ms=$((agg_prove_ms + ms))
                done < <(grep -aoP 'prove_ms=\K[0-9]+' "$zkvm_log" || true)

                # Sum verify_ms for this aggregator
                local agg_verify_ms=0
                while IFS= read -r v; do
                    agg_verify_ms=$((agg_verify_ms + v))
                done < <(grep -aoP 'verify_ms=\K[0-9]+' "$zkvm_log" || true)

                # Sum proof_bytes for this aggregator
                local agg_proof_bytes=0
                while IFS= read -r pb; do
                    agg_proof_bytes=$((agg_proof_bytes + pb))
                done < <(grep -aoP 'proof_bytes=\K[0-9]+' "$zkvm_log" || true)

                # Sum journal_bytes for this aggregator
                local agg_journal_bytes=0
                while IFS= read -r jb; do
                    agg_journal_bytes=$((agg_journal_bytes + jb))
                done < <(grep -aoP 'journal_bytes=\K[0-9]+' "$zkvm_log" || true)

                # Count batches (n_events lines) for this aggregator
                # Note: grep -c outputs "0" and exits with 1 when no matches, so we can't use || echo
                local agg_batches
                agg_batches=$(grep -ac 'n_events=' "$zkvm_log" 2>/dev/null) || agg_batches=0

                # Sum n_events for this aggregator (total events proved)
                local agg_events=0
                while IFS= read -r ne; do
                    agg_events=$((agg_events + ne))
                done < <(grep -aoP 'n_events=\K[0-9]+' "$zkvm_log" || true)

                # Log per-aggregator metrics
                log_info "    Aggregator $i: epochs=$agg_epochs events=$agg_events prove_ms=$agg_prove_ms verify_ms=$agg_verify_ms proof_bytes=$agg_proof_bytes batches=$agg_batches"

                # Update totals
                [[ $agg_prove_ms -gt $max_prove_ms ]] && max_prove_ms=$agg_prove_ms
                [[ $agg_verify_ms -gt $max_verify_ms ]] && max_verify_ms=$agg_verify_ms
                total_proof_bytes=$((total_proof_bytes + agg_proof_bytes))
                total_journal_bytes=$((total_journal_bytes + agg_journal_bytes))
                total_batches=$((total_batches + agg_batches))
                total_events_proved=$((total_events_proved + agg_events))
            else
                log_warn "    Aggregator $i: log not found"
            fi
        done

        log_info "  Total epochs proved: $total_epochs_proved (across all aggregators)"
        log_info "  Total events proved: $total_events_proved (across all aggregators)"
        log_info "  Total batches processed: $total_batches (across all aggregators)"

        # Validate that all events were proved
        if [[ $total_events_proved -ne $TOTAL_EVENTS ]]; then
            local missing_events=$((TOTAL_EVENTS - total_events_proved))
            log_warn "  DATA LOSS DETECTED: proved $total_events_proved / $TOTAL_EVENTS events (missing: $missing_events)"
        else
            log_info "  Validation PASSED: all $TOTAL_EVENTS events proved"
        fi

        # Set final metrics
        # prove_ms and verify_ms = max among aggregators (they run in parallel)
        prove_ms=$max_prove_ms
        verify_ms=$max_verify_ms
        proof_bytes_last=$total_proof_bytes
        proof_bytes_max=$total_proof_bytes
        journal_bytes_last=$total_journal_bytes

        # Calculate averages
        if [[ $total_epochs_proved -gt 0 ]]; then
            avg_proof_ms=$((max_prove_ms / total_epochs_proved))
        fi
        p99_proof_ms=$max_prove_ms  # Simplified: use max as p99 for parallel execution

        # Calculate KB values
        proof_kb_last=$(echo "scale=3; $proof_bytes_last / 1024" | bc -l 2>/dev/null || echo "0")
        proof_kb_max=$(echo "scale=3; $proof_bytes_max / 1024" | bc -l 2>/dev/null || echo "0")
        journal_kb_last=$(echo "scale=3; $journal_bytes_last / 1024" | bc -l 2>/dev/null || echo "0")
    fi

    local total_end_ms=$(date +%s%3N)

    # Calculate metrics
    local consume_ms=$((consume_end_ms - consume_start_ms))
    local total_ms=$((total_end_ms - total_start_ms))
    local events_per_sec=0
    if [[ $total_ms -gt 0 ]]; then
        events_per_sec=$((TOTAL_EVENTS * 1000 / total_ms))
    fi

    # Calculate total events bytes (timestamp=8 + key_id=8 + value=4 = 20 bytes per event)
    local EVENT_SIZE_BYTES=20
    local total_events_bytes=$((TOTAL_EVENTS * EVENT_SIZE_BYTES))
    local total_events_kb=$(echo "scale=3; $total_events_bytes / 1024" | bc -l 2>/dev/null || echo "0")

    # Estimate MB/s
    local mb_per_sec=0
    if [[ $total_ms -gt 0 ]]; then
        mb_per_sec=$(echo "scale=2; $total_events_bytes / 1048576 * 1000 / $total_ms" | bc)
    fi

    # Calculate proof size ratios and events per proof
    local proof_overhead_ratio="0"
    local data_to_proof_ratio="0"
    local events_per_proof=0
    if [[ "${ENABLE_ZK_AGGREGATION:-1}" == "1" ]] && [[ $proof_bytes_last -gt 0 ]]; then
        # Proof overhead ratio: How much bigger the proof is than the data
        # >1 means proof is bigger (typical), <1 means proof is smaller
        proof_overhead_ratio=$(echo "scale=2; $proof_bytes_last / $total_events_bytes" | bc -l 2>/dev/null || echo "0")

        # Data to proof ratio: Inverse of above (for compatibility)
        data_to_proof_ratio=$(echo "scale=2; $total_events_bytes / $proof_bytes_last" | bc -l 2>/dev/null || echo "0")

        # Calculate events per proof (each epoch = one proof)
        local num_proofs=${total_epochs_proved:-0}
        if [[ $num_proofs -gt 0 ]]; then
            events_per_proof=$((TOTAL_EVENTS / num_proofs))
        fi
    fi

    # Memory usage
    local memory_mb=$(get_memory_usage "$agg_pids")



    # Calculate latencies (placeholder - would need per-event timing)
    # events_per_send = kafka_batch_size * commit_batch_size
    local events_per_send=$((KAFKA_BATCH_SIZE * COMMIT_BATCH_SIZE))
    local avg_latency_ms=$((total_ms / (TOTAL_EVENTS / events_per_send + 1)))
    local p99_latency_ms=$((avg_latency_ms * 2))

    # Log results
    log_info "Results:"
    log_info "  Total events: ${TOTAL_EVENTS}"
    log_info "  Total events size: ${total_events_kb} KB (${total_events_bytes} bytes)"
    log_info "  Produce time: ${produce_ms}ms"
    log_info "  Consume time: ${consume_ms}ms"
    if [[ "${ENABLE_ZK_AGGREGATION:-1}" == "1" ]]; then
        log_info "  Epochs proved: ${total_epochs_proved:-0} (sum across all aggregators)"
        log_info "  Batches processed: ${total_batches:-0} (sum across all aggregators)"
        log_info "  ZK Prove time: ${prove_ms}ms (max among aggregators - parallel execution)"
        log_info "  ZK Verify time: ${verify_ms}ms (max among aggregators - parallel execution)"
        log_info "  Avg proof time per epoch: ${avg_proof_ms}ms"
        log_info "  Proof bytes total: ${proof_kb_last} KB (${proof_bytes_last} bytes, sum across all aggregators)"
        log_info "  Journal bytes total: ${journal_kb_last} KB (${journal_bytes_last} bytes, sum across all aggregators)"
        log_info "  Proof overhead: ${proof_overhead_ratio}x (proof is ${proof_overhead_ratio}x the data size)"
        log_info "  Data size: ${total_events_kb} KB → Proof size: ${proof_kb_last} KB"
        log_info "  Events per proof: ${events_per_proof}"
    fi
    log_info "  Total time: ${total_ms}ms"
    log_info "  Throughput: ${events_per_sec} events/sec"
    log_info "  Throughput: ${mb_per_sec} MB/sec"
    log_info "  Memory: ${memory_mb} MB"

    # Write to CSV (prove_ms/verify_ms=max among agg, proof_bytes/journal_bytes=sum among agg)
    echo "${TIMESTAMP},${EPOCH_TYPE},${num_agg},${NUM_SOURCES},${SERIES},${SAMPLES_PER_SERIES},${TOTAL_EVENTS},${total_events_bytes},${total_events_kb},${KAFKA_BATCH_SIZE},${COMMIT_BATCH_SIZE},${rep},${WARMUP_EVENTS},${produce_ms},${consume_ms},${prove_ms},${verify_ms},${total_ms},${events_per_sec},${mb_per_sec},${avg_latency_ms},${p99_latency_ms},${avg_proof_ms},${p99_proof_ms},${proof_bytes_last},${proof_kb_last},${journal_bytes_last},${journal_kb_last},${total_epochs_proved:-0},${total_batches:-0},${proof_overhead_ratio},${data_to_proof_ratio},${events_per_proof},${memory_mb}," >> "$CSV_FILE"

    # Stop background log sync
    log_info "Stopping log sync..."
    stop_log_sync

}

# Collect logs from all machines
collect_remote_logs() {
    local run_id="$1"
    local num_agg="$2"

    log_info "Collecting logs from all machines..."

    local logs_collected_dir="${LOG_DIR}/collected_${run_id}"
    mkdir -p "$logs_collected_dir"

    # Read all machines from the array
    local machines_array
    IFS=' ' read -ra machines_array <<< "$AGGREGATOR_MACHINES"
    local num_machines=${#machines_array[@]}

    # Collect logs from each aggregator
    for i in $(seq 0 $((num_agg - 1))); do
        local machine_idx=$((i % num_machines))
        local machine="${machines_array[$machine_idx]}"
        local local_log_file="${LOG_DIR}/aggregator_${run_id}_${i}.log"
        local remote_log_file="${REMOTE_PROJECT_PATH}/bench_logs/distributed/aggregator_${run_id}_${i}.log"

        if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
            # Copy log from remote machine (use remote path)
            scp -q "${SSH_USER}@${machine}:${remote_log_file}" "$logs_collected_dir/" 2>/dev/null || \
                log_warn "  Failed to collect log from $machine aggregator $i"
        else
            # Copy local log
            cp "$local_log_file" "$logs_collected_dir/" 2>/dev/null || \
                log_warn "  Failed to copy local log for aggregator $i"
        fi
    done

    log_info "  ✓ Logs collected to: $logs_collected_dir"
}

# Extract detailed metrics from logs
extract_detailed_metrics() {
    local run_id="$1"
    local num_agg="$2"

    local logs_dir="${LOG_DIR}"
    local metrics_file="${CSV_DIR}/detailed_metrics_${run_id}.csv"

    echo "aggregator_id,proof_time_ms,verify_time_ms,events_processed,epochs_proved,proof_bytes,journal_bytes,memory_mb,errors" > "$metrics_file"

    for i in $(seq 0 $((num_agg - 1))); do
        # ZK proof metrics are in zkvm_*.log files, not aggregator_*.log
        local log_file="$logs_dir/zkvm_${run_id}_${i}.log"

        if [[ -f "$log_file" ]]; then
            # Note: Use -a flag to handle binary characters (ANSI escape codes from warnings)
            # Sum of all proof times for this aggregator (prove_ms=N pattern)
            local proof_time=0
            while IFS= read -r t; do
                proof_time=$((proof_time + t))
            done < <(grep -aoP 'prove_ms=\K[0-9]+' "$log_file" 2>/dev/null)

            # Sum of all verify times for this aggregator (verify_ms=N pattern)
            local verify_time=0
            while IFS= read -r t; do
                verify_time=$((verify_time + t))
            done < <(grep -aoP 'verify_ms=\K[0-9]+' "$log_file" 2>/dev/null)

            # Sum of events processed (n_events=N)
            local events=0
            while IFS= read -r n; do
                events=$((events + n))
            done < <(grep -aoP 'n_events=\K[0-9]+' "$log_file" 2>/dev/null)

            # Count epochs proved
            local epochs
            epochs=$(grep -ac 'Epoch seq=.*completed' "$log_file" 2>/dev/null) || epochs=0

            # Sum of proof_bytes for this aggregator
            local proof_bytes=0
            while IFS= read -r pb; do
                proof_bytes=$((proof_bytes + pb))
            done < <(grep -aoP 'proof_bytes=\K[0-9]+' "$log_file" 2>/dev/null)

            # Sum of journal_bytes for this aggregator
            local journal_bytes=0
            while IFS= read -r jb; do
                journal_bytes=$((journal_bytes + jb))
            done < <(grep -aoP 'journal_bytes=\K[0-9]+' "$log_file" 2>/dev/null)

            # Extract max memory usage (memory_mb=N pattern from ZK proof logs)
            local memory=$(grep -aoP 'memory_mb=\K[0-9]+' "$log_file" | sort -n | tail -1 || echo "0")
            [[ -z "$memory" ]] && memory=0

            # Count errors (exclude "dev mode" warnings which contain "invalid")
            # Use subshell with disabled pipefail to avoid exit on grep -v returning no matches
            local errors
            errors=$(set +o pipefail; grep -aiE "error|fail" "$log_file" 2>/dev/null | grep -v "dev mode" | wc -l)
            errors=${errors:-0}

            echo "$i,$proof_time,$verify_time,$events,$epochs,$proof_bytes,$journal_bytes,$memory,$errors" >> "$metrics_file"
        else
            # Log file not found, output zeros
            echo "$i,0,0,0,0,0,0,0,0" >> "$metrics_file"
        fi
    done

    log_info "  ✓ Detailed metrics saved to: $metrics_file"
}

# Generate comprehensive summary report
generate_summary_report() {
    local report_file="${CSV_DIR}/summary_report_${TIMESTAMP}.txt"

    {
        echo "============================================"
        echo "  Distributed Aggregator Benchmark Report"
        echo "============================================"
        echo ""
        echo "Timestamp: $(date)"
        echo "Configuration:"
        echo "  Machines: $AGGREGATOR_MACHINES"
        echo "  Aggregators tested: $NUM_AGGREGATORS_LIST"
        echo "  Series: $SERIES"
        echo "  Samples per series: $SAMPLES_PER_SERIES"
        echo "  Total events: $TOTAL_EVENTS"
        echo "  Kafka partitions: (equals num_aggregators per run)"
        echo "  Repeats: $REPEATS"
        echo "  Epoch types: $EPOCH_TYPE_LIST"
        echo ""
        echo "Results Summary:"
        echo "================"
        column -t -s',' "$CSV_FILE" | head -20
        echo ""
        echo "Detailed metrics files: ${CSV_DIR}/detailed_metrics_*.csv"
        echo "Collected logs: ${LOG_DIR}/collected_*/"
        echo ""
    } | tee "$report_file"

    log_info "  ✓ Summary report: $report_file"
}

# Main
main() {
    # Handle special command-line options
    case "${1:-}" in
        --check|--check-state)
            check_clean_state
            exit 0
            ;;
        --force-cleanup|--cleanup)
            force_cleanup_all_machines
            check_clean_state
            exit 0
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --check          Check clean state on all machines"
            echo "  --force-cleanup  Kill all processes and remove RocksDB on all machines"
            echo "  --help           Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  NUM_AGGREGATORS  Aggregator counts to test (default: 8 4 2 1)"
            echo "  EPOCH_TYPE       Epoch types to test (default: samples histogram cm)"
            echo "  REPEATS          Repeats per configuration (default: 1)"
            echo "  RISC0_DEV_MODE   Set to 1 for dev mode (no proofs, fast) (default: 0)"
            echo "  See script header for more options"
            exit 0
            ;;
    esac

    check_deps
    cleanup_rocksdb_all_machines
    check_clean_state  # Verify clean state before starting
    sync_code_to_machines
    sync_binaries_to_machines

    for num_agg in $NUM_AGGREGATORS_LIST; do
        log_info ""
        log_info "=== Testing num_aggregators: $num_agg ==="
        for EPOCH_TYPE in $EPOCH_TYPE_LIST; do
            export EPOCH_TYPE
            for rep in $(seq 1 "$REPEATS"); do
                local run_id="${EPOCH_TYPE}_${num_agg}agg_rep${rep}_${TIMESTAMP}"
                run_benchmark "$num_agg" "$rep"

                # Collect results after each run
                collect_remote_logs "$run_id" "$num_agg"
                extract_detailed_metrics "$run_id" "$num_agg"
            done
        done
    done

    log_info ""
    log_info "=== Benchmark Complete ==="
    log_info "Results saved to: $CSV_FILE"
    log_info ""

    # Generate comprehensive report
    generate_summary_report

    # Print summary
    echo ""
    echo "Summary:"
    echo "========"
    column -t -s',' "$CSV_FILE" | head -20
    echo ""
    echo "All results collected to local machine:"
    echo "  - CSV: $CSV_FILE"
    echo "  - Main log: $MAIN_LOG_FILE"
    echo "  - Detailed metrics: ${CSV_DIR}/detailed_metrics_*.csv"
    echo "  - Logs: ${LOG_DIR}/collected_*/"
    echo "  - Report: ${CSV_DIR}/summary_report_${TIMESTAMP}.txt"
    echo ""
    echo "=== Benchmark completed at $(date) ==="
}

main "$@"
