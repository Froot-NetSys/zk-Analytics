#!/usr/bin/env bash
set -euo pipefail

# Run distributed end-to-end evaluation for zk-Analytics.
#
# This script:
#   - Starts data source on dedicated remote machine (replaying real world dataset)
#   - Starts aggregators on X configurable remote machines
#   - Starts querier on dedicated remote machine for validation
#   - Monitors distributed system health
#   - Validates end-to-end data flow
#   - Collects metrics and generates report
#
# Usage:
#   ./scripts/distributed/run_distributed_e2e.sh [start|stop|status|validate|report]
#
# Configuration via env vars:
#   DATA_SOURCE_MACHINE  Machine for data source (required)
#   AGGREGATOR_MACHINES  Space-separated list of aggregator machines (required)
#   QUERIER_MACHINE      Machine for querier (required)
#   SSH_USER             SSH username for remote machines (default: current user)
#   REMOTE_PROJECT_DIR   Remote project path (default: ~/zk-Analytics)
#   KAFKA_BROKERS        Kafka broker addresses (default: auto-detected)
#   KAFKA_TOPIC          Kafka topic name (default: raw_events)
#   FDB_CLUSTER_FILE     FDB cluster file path (default: /etc/foundationdb/fdb.cluster)
#   FDB_SUBSPACE         FDB subspace (default: zktelemetry_dist_e2e)
#   NUM_AGGREGATORS      Total number of aggregators (default: 4)
#   EVENTS               Total events to produce (default: 1000000)
#   BATCH_SIZE           Events per batch (default: 100)
#   QUERIER_PORT         Querier HTTP port (default: 8082)
#   EPOCH_TYPE           Aggregation type: samples, histogram, cm (default: samples)
#   RISC0_DEV_MODE       Run RISC0 in dev mode (no actual proofs, fast) (default: 0)
#   QUERY_TYPES          Query types to evaluate (default: samples_sum,histogram_p90,cm_topk)
#   QUERY_WINDOW         Query time window, e.g., 5m, 1h, 24h (default: 1h)
#   QUERY_EPOCHS         Query latest N epochs (overrides QUERY_WINDOW if set)
#   NUM_QUERY_ITERATIONS Number of times to run each query for benchmarking (default: 10)
#   KEY_PREFIX           Key prefix for prefix-based queries like samples_sum_prefix (default: empty)
#
# Dataset Configuration (for replaying real-world datasets):
#   DATASET_TYPE         Dataset type: synthetic, google_cluster, caida, car_emission (default: synthetic)
#   GOOGLE_CLUSTER_DIR   Path to Google Cluster data on remote machine
#   TSV_MAX_FILES        Max machine files to load for Google Cluster (default: 64)
#   CAIDA_DIR            Path to CAIDA txt files on remote machine
#   CAIDA_MAX_FILES      Max CAIDA txt files to load (default: 64)
#   SERIES               Number of distinct keys per source (default: 64)
#   SAMPLES_PER_SERIES   Samples per key per source (default: 64)
#   KAFKA_BATCH_SIZE     Kafka batch size (default: 100)
#   COMMIT_BATCH_SIZE    Commit batch size (default: 8)
#   PARALLEL_PRODUCERS   Number of parallel producer tasks for dataset mode (default: NUM_AGGREGATORS)
#
# Example:
#   DATA_SOURCE_MACHINE="192.0.2.10" \
#   AGGREGATOR_MACHINES="192.0.2.1 192.0.2.2 192.0.2.3" \
#   QUERIER_MACHINE="192.0.2.20" \
#   SSH_USER="ubuntu" \
#   KAFKA_BROKERS="192.0.2.100:9092" \
#   EVENTS=5000000 \
#   ./scripts/distributed/run_distributed_e2e.sh start

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }
log_success() { echo -e "${CYAN}[SUCCESS]${NC} $1"; }
log_debug() { [[ "${DEBUG:-0}" == "1" ]] && echo -e "[DEBUG] $1" || true; }

# Source centralized IP defaults (contains E2E-specific machine config)
# shellcheck source=ip_defaults.sh
source "${ROOT_DIR}/scripts/ip_defaults.sh"

# Source config file if it exists
CONFIG_FILE="${ROOT_DIR}/scripts/distributed_e2e_config.example.sh"
if [[ -f "$CONFIG_FILE" ]]; then
    log_info "Sourcing configuration from $CONFIG_FILE"
    source "$CONFIG_FILE"
fi

# Configuration
# Backward compatibility: if REMOTE_MACHINES is set but new vars aren't, use it for aggregators
if [[ -n "${REMOTE_MACHINES:-}" && -z "${AGGREGATOR_MACHINES:-}" ]]; then
    log_warn "REMOTE_MACHINES is deprecated. Use DATA_SOURCE_MACHINE, AGGREGATOR_MACHINES, and QUERIER_MACHINE instead."
    AGGREGATOR_MACHINES="$REMOTE_MACHINES"
fi

# Use E2E-specific defaults from ip_defaults.sh if not overridden by environment
DATA_SOURCE_MACHINE="${DATA_SOURCE_MACHINE:-$E2E_DATA_SOURCE_MACHINE}"
AGGREGATOR_MACHINES="${AGGREGATOR_MACHINES:-$E2E_AGGREGATOR_MACHINES}"
QUERIER_MACHINE="${QUERIER_MACHINE:-$E2E_QUERIER_MACHINE}"
SSH_USER="${SSH_USER:-$USER}"
# Remote project directory - use /mydata/zk-Analytics for consistency across machines
REMOTE_PROJECT_DIR="${REMOTE_PROJECT_DIR:-/mydata/zk-Analytics}"

# Auto-detect Kafka broker address for distributed mode:
# Priority: KAFKA_BROKERS env > E2E_KAFKA_BROKERS from ip_defaults.sh > auto-detect from DATA_SOURCE_MACHINE
_detect_kafka_brokers() {
    # First check if E2E_KAFKA_BROKERS is set from ip_defaults.sh
    if [[ -n "${E2E_KAFKA_BROKERS:-}" && "${E2E_KAFKA_BROKERS}" != *"localhost"* ]]; then
        echo "$E2E_KAFKA_BROKERS"
        return
    fi

    local default_brokers="localhost:9092"

    # If data source machine is set and not localhost, use it as Kafka broker
    if [[ -n "$DATA_SOURCE_MACHINE" && "$DATA_SOURCE_MACHINE" != "localhost" && "$DATA_SOURCE_MACHINE" != "127.0.0.1" ]]; then
        default_brokers="${DATA_SOURCE_MACHINE}:9092"
    fi
    echo "$default_brokers"
}
KAFKA_BROKERS="${KAFKA_BROKERS:-$(_detect_kafka_brokers)}"
KAFKA_TOPIC="${KAFKA_TOPIC:-raw_events}"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
FDB_SUBSPACE="${FDB_SUBSPACE:-zktelemetry_dist_e2e}"
NUM_AGGREGATORS="${NUM_AGGREGATORS:-8}"
# Number of logical sources for source_id computation to for real world datasets and synthetic data
# source_id = hash(natural_key) % NUM_SOURCES
# Default: 64
NUM_SOURCES="${NUM_SOURCES:-64}"
# Number of parallel producer tasks for dataset mode (google_cluster, caida)
# Each task handles source_ids where source_id % PARALLEL_PRODUCERS == task_index
# Default: NUM_SOURCES (one producer task per source_id for maximum parallelism)
PARALLEL_PRODUCERS="${PARALLEL_PRODUCERS:-$NUM_SOURCES}"
EVENTS="${EVENTS:-16384}"
BATCH_SIZE="${BATCH_SIZE:-100}"
QUERIER_PORT="${QUERIER_PORT:-8082}"
EPOCH_TYPE="${EPOCH_TYPE:-samples}"
# RISC0 dev mode: Set to 1 to disable actual proof generation (much faster for testing)
# RISC0_DEV_MODE: 0 = real proofs (slow but shows actual metrics), 1 = dev mode (fast, no proofs)
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
# Verbose hash logging: Set to 1 to enable detailed [PRODUCER_HASH], [CONSUMER_HASH], [AGGREGATOR_ZK_INPUT] logs
VERBOSE_HASH_LOGGING="${VERBOSE_HASH_LOGGING:-0}"
QUERY_TYPES="${QUERY_TYPES:-samples_sum,histogram_p90,cm_topk}"
QUERY_WINDOW="${QUERY_WINDOW:-1h}"
QUERY_EPOCHS="${QUERY_EPOCHS:-}"  # If set, overrides QUERY_WINDOW to query latest N epochs
NUM_QUERY_ITERATIONS="${NUM_QUERY_ITERATIONS:-10}"
KEY_PREFIX="${KEY_PREFIX:-}"
# Query parameters for key-based queries
QUERY_KEY="${QUERY_KEY:-0}"
QUERY_MASK="${QUERY_MASK:-18446744073709551615}"  # 0xFFFFFFFFFFFFFFFF
QUERY_BUCKET="${QUERY_BUCKET:-0}"
QUERY_LIMIT="${QUERY_LIMIT:-10}"
QUERY_VALUE="${QUERY_VALUE:-0}"
QUERY_PATTERN="${QUERY_PATTERN:-0x00}"

# Performance tuning
# SKIP_FINAL_COLLECTION: Set to 1 to exit immediately after aggregators complete without collecting logs/metrics
# This speeds up test completion but skips final validation and report generation
SKIP_FINAL_COLLECTION="${SKIP_FINAL_COLLECTION:-0}"

# Dataset configuration for real-world data replay
# DATASET_TYPE: synthetic, google_cluster, caida
DATASET_TYPE="${DATASET_TYPE:-caida}"
# Google Cluster dataset settings
GOOGLE_CLUSTER_DIR="${GOOGLE_CLUSTER_DIR:-${REMOTE_PROJECT_DIR}/testdata/google_cluster_data/input}"
TSV_MAX_FILES="${TSV_MAX_FILES:-8192}"
CSV_VALUE_SCALE="${CSV_VALUE_SCALE:-1000000.0}"
CSV_METRIC_ID="${CSV_METRIC_ID:-2}"
# CAIDA dataset settings
CAIDA_DIR="${CAIDA_DIR:-${REMOTE_PROJECT_DIR}/testdata/caida_pcap/caida_txt}"
CAIDA_MAX_FILES="${CAIDA_MAX_FILES:-100000}"
CAIDA_SORT_BY_SIZE="${CAIDA_SORT_BY_SIZE:-1}"
# Car emission dataset settings
EMISSION_CSV="${EMISSION_CSV:-${REMOTE_PROJECT_DIR}/testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv}"
EMISSION_VALUE_SCALE="${EMISSION_VALUE_SCALE:-1.0}"
# Common dataset settings
TS_MODE="${TS_MODE:-default}"
TS_INTERVAL_MS="${TS_INTERVAL_MS:-100}"
SERIES="${SERIES:-64}"
SAMPLES_PER_SERIES="${SAMPLES_PER_SERIES:-128}"
COMMIT_BATCH_SIZE="${COMMIT_BATCH_SIZE:-8}"
KAFKA_BATCH_SIZE="${KAFKA_BATCH_SIZE:-100}"

# Separated architecture configuration (kafka-consumer + zk-aggregator)
# Consumer settle time: extra seconds to wait after Kafka lag=0 before starting ZK aggregation.
# This allows kafka-consumers to finish writing all batches to RocksDB.
CONSUMER_SETTLE_SEC="${CONSUMER_SETTLE_SEC:-5}"

# ZK aggregation timeout (seconds). Set to 0 for no timeout.
# Default: 0 (no timeout - ZK proof generation can take hours)
ZK_AGGREGATION_TIMEOUT="${ZK_AGGREGATION_TIMEOUT:-0}"

# Epoch batching configuration
# EPOCH_BATCH_THRESHOLD: Max batches per epoch (default: 2048)
#   Each epoch contains at most this many batches (limit always applies).
# EPOCH_TIMEOUT_MS: Timeout in ms to force epoch flush (default: 300000 = 5min)
#   Creates epoch with available batches (up to threshold) if timeout elapses.
# Total events per epoch = up to EPOCH_BATCH_THRESHOLD * COMMIT_BATCH_SIZE
EPOCH_BATCH_THRESHOLD="${EPOCH_BATCH_THRESHOLD:-2048}"
EPOCH_TIMEOUT_MS="${EPOCH_TIMEOUT_MS:-300000}"

# Minimum epochs required in FDB before starting querier
# This ensures the querier has data to query while ZK aggregation continues
MIN_EPOCHS_FOR_QUERIER="${MIN_EPOCHS_FOR_QUERIER:-8}"

# Timeout waiting for minimum epochs (seconds)
EPOCH_WAIT_TIMEOUT_SEC="${EPOCH_WAIT_TIMEOUT_SEC:-300}"

# RocksDB paths for separated architecture
RAW_ROCKSDB_BASE="/mydata/rocksdb"
RAW_ROCKSDB_SECONDARY_BASE="/mydata/rocksdb_secondary"

# Kafka consumer group ID
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-dist_e2e_consumers}"

SESSION_NAME="zktelemetry-dist-e2e"
# Log and results directories - use absolute paths under /mydata for consistency
LOG_DIR="/mydata/zk-Analytics/bench_logs/distributed_e2e"
RESULTS_DIR="/mydata/zk-Analytics/bench_csv/distributed_e2e"

# Main output log file (captures all script output)
MAIN_LOG_FILE="${LOG_DIR}/run_distributed_e2e_$(date +%Y%m%d).log"

# Background log sync PID
LOG_SYNC_PID=""

# Cleanup on exit
cleanup_on_exit() {
    # Stop background log sync if running
    if [[ -n "${LOG_SYNC_PID:-}" ]]; then
        kill "$LOG_SYNC_PID" 2>/dev/null || true
    fi
}
trap cleanup_on_exit EXIT

# Background log sync - continuously copies logs from remote machines
# Syncs: consumer logs, zkvm logs (separated architecture), querier log, datasource log
start_log_sync() {
    local sync_interval="${1:-5}"  # Default: sync every 5 seconds

    # Don't start if already running
    if [[ -n "$LOG_SYNC_PID" ]] && kill -0 "$LOG_SYNC_PID" 2>/dev/null; then
        log_debug "Log sync already running (PID: $LOG_SYNC_PID)"
        return 0
    fi

    # Create local log directory
    mkdir -p "$LOG_DIR"

    log_info "Starting background log sync (interval: ${sync_interval}s)..."

    # Start background sync process
    (
        # Calculate aggregator distribution
        local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
        local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
        local agg_remainder=$((NUM_AGGREGATORS % num_machines))

        while true; do
            # Sync consumer and zkvm logs from all aggregator machines (separated architecture)
            local machine_idx=0
            local global_agg_id=0
            for machine in $AGGREGATOR_MACHINES; do
                # Calculate how many aggregators are on this machine
                local this_machine_aggs=$agg_per_machine
                if [[ $machine_idx -lt $agg_remainder ]]; then
                    this_machine_aggs=$((agg_per_machine + 1))
                fi

                # Skip localhost - logs already local
                if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
                    local remote_log_dir="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e"
                    for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                        # Sync kafka-consumer log
                        local remote_consumer_log="${remote_log_dir}/consumer_${global_agg_id}.log"
                        local local_consumer_log="${LOG_DIR}/consumer_${machine}_${global_agg_id}.log"
                        scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${machine}:${remote_consumer_log}" "$local_consumer_log" 2>/dev/null || true

                        # Sync ZK aggregator (zkvm) log
                        local remote_zkvm_log="${remote_log_dir}/zkvm_${global_agg_id}.log"
                        local local_zkvm_log="${LOG_DIR}/zkvm_${machine}_${global_agg_id}.log"
                        scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${machine}:${remote_zkvm_log}" "$local_zkvm_log" 2>/dev/null || true

                        global_agg_id=$((global_agg_id + 1))
                    done
                else
                    global_agg_id=$((global_agg_id + this_machine_aggs))
                fi
                machine_idx=$((machine_idx + 1))
            done

            # Sync querier log
            scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "${LOG_DIR}/querier.log" 2>/dev/null || true

            # Sync data source log
            scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${DATA_SOURCE_MACHINE}:/tmp/zktelemetry_datasource.log" "${LOG_DIR}/datasource.log" 2>/dev/null || true

            sleep "$sync_interval"
        done
    ) &
    LOG_SYNC_PID=$!
    log_info "Background log sync started (PID: $LOG_SYNC_PID)"
}

stop_log_sync() {
    if [[ -n "$LOG_SYNC_PID" ]]; then
        kill "$LOG_SYNC_PID" 2>/dev/null || true

        # Wait with timeout (5 seconds)
        local wait_count=0
        local max_wait=10  # 10 * 0.5s = 5 seconds
        while kill -0 "$LOG_SYNC_PID" 2>/dev/null && [[ $wait_count -lt $max_wait ]]; do
            sleep 0.5
            wait_count=$((wait_count + 1))
        done

        # Force kill if still running
        if kill -0 "$LOG_SYNC_PID" 2>/dev/null; then
            log_warn "  Log sync process didn't stop gracefully, forcing kill..."
            kill -9 "$LOG_SYNC_PID" 2>/dev/null || true
            sleep 1
        fi

        wait "$LOG_SYNC_PID" 2>/dev/null || true
        LOG_SYNC_PID=""
        log_info "Background log sync stopped"
    fi
}

# Setup logging to both terminal and file
setup_logging() {
    mkdir -p "$LOG_DIR"
    # Use exec to redirect all output to tee with line buffering for real-time output
    # stdbuf ensures line-buffered output, tee -a appends to log file
    exec > >(stdbuf -oL tee -a "$MAIN_LOG_FILE") 2>&1
    log_info "Logging to: $MAIN_LOG_FILE"
}

# Setup FDB cluster file on all machines to point to the querier's FDB
# This ensures all aggregators write to the same shared FDB instance.
setup_fdb_cluster() {
    log_step "Setting up FDB cluster configuration for distributed mode..."

    local new_cluster_content="zktelemetry:zktelemetry@${QUERIER_MACHINE}:4500"

    # Get the cluster file content from querier machine
    local cluster_content
    cluster_content=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" "cat $FDB_CLUSTER_FILE 2>/dev/null || echo ''")

    log_info "Current FDB cluster: ${cluster_content:-<none>}"

    # Check if cluster file needs to be updated (uses Docker IP or doesn't exist)
    if [[ -z "$cluster_content" || "$cluster_content" == *"172.17."* || "$cluster_content" != *"${QUERIER_MACHINE}"* ]]; then
        log_info "Configuring FDB for distributed access on $QUERIER_MACHINE..."

        # First, check if FDB server needs to be restarted to use external IP
        # The Docker FDB server advertises on 172.17.0.2 by default
        log_info "  Checking if FDB server can be reached at ${QUERIER_MACHINE}:4500..."
        if ! ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" \
            "nc -z -w 2 ${QUERIER_MACHINE} 4500 2>/dev/null"; then
            log_warn "  FDB server not reachable at ${QUERIER_MACHINE}:4500"
            log_warn "  The FDB server may need to be restarted with host networking."
            log_warn ""
            log_warn "  To fix manually on $QUERIER_MACHINE:"
            log_warn "    # Stop current FDB and restart with host networking:"
            log_warn "    sudo pkill fdbserver"
            log_warn "    # Or if using Docker:"
            log_warn "    docker stop \$(docker ps -q --filter ancestor=foundationdb/foundationdb)"
            log_warn "    docker run -d --network host foundationdb/foundationdb:7.1.25"
            log_warn ""
            log_warn "  Continuing with local FDB (epochs will NOT be shared across machines)..."
            return 0
        fi

        # Update cluster file on querier machine to use external IP
        log_info "  Updating cluster file to: $new_cluster_content"
        ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" \
            "echo '$new_cluster_content' | sudo tee $FDB_CLUSTER_FILE > /dev/null" || {
            log_warn "Failed to update FDB cluster file on $QUERIER_MACHINE"
            return 1
        }

        # Initialize/reconfigure FDB database (with timeout to avoid hanging)
        # Note: 'configure new' will fail if already configured, which is fine
        log_info "  Initializing FDB database..."
        local configure_result
        configure_result=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" \
            "timeout 10 fdbcli --exec 'configure new single ssd' 2>&1 || echo 'timeout_or_configured'")

        if [[ "$configure_result" == *"timeout"* ]]; then
            log_warn "  FDB configure timed out - server may not be responding"
        elif [[ "$configure_result" == *"already_configured"* || "$configure_result" == *"ERROR"* ]]; then
            log_info "  FDB already configured (this is OK)"
        else
            log_info "  FDB initialized: $configure_result"
        fi

        cluster_content="$new_cluster_content"
    fi

    log_info "FDB cluster: $cluster_content"

    # Copy cluster file to all aggregator machines
    for machine in $AGGREGATOR_MACHINES; do
        if [[ "$machine" != "$QUERIER_MACHINE" ]]; then
            log_info "  Copying FDB cluster file to $machine..."
            ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                "echo '$cluster_content' | sudo tee $FDB_CLUSTER_FILE > /dev/null" || {
                log_warn "Failed to update FDB cluster file on $machine"
            }
        fi
    done

    # Also copy to data source machine if different
    if [[ "$DATA_SOURCE_MACHINE" != "$QUERIER_MACHINE" ]]; then
        log_info "  Copying FDB cluster file to $DATA_SOURCE_MACHINE..."
        ssh -o ConnectTimeout=5 "${SSH_USER}@${DATA_SOURCE_MACHINE}" \
            "echo '$cluster_content' | sudo tee $FDB_CLUSTER_FILE > /dev/null" || {
            log_warn "Failed to update FDB cluster file on $DATA_SOURCE_MACHINE"
        }
    fi

    # Wait a moment for FDB to stabilize after config change
    sleep 2

    # Verify FDB connectivity from querier
    local fdb_status
    fdb_status=$(ssh -o ConnectTimeout=10 "${SSH_USER}@${QUERIER_MACHINE}" \
        "fdbcli --exec 'status minimal' 2>&1 || echo 'unavailable'")

    if [[ "$fdb_status" == *"available"* ]]; then
        log_success "  ✓ FDB cluster is available and accessible from $QUERIER_MACHINE"
    else
        log_warn "FDB cluster status: $fdb_status"
        log_warn "FDB may not be accessible - check if fdbserver is running on $QUERIER_MACHINE"
    fi

    # Verify connectivity from one aggregator
    local first_agg
    read -ra agg_array <<< "$AGGREGATOR_MACHINES"
    first_agg="${agg_array[0]}"
    if [[ "$first_agg" != "$QUERIER_MACHINE" ]]; then
        local agg_fdb_status
        agg_fdb_status=$(ssh -o ConnectTimeout=10 "${SSH_USER}@${first_agg}" \
            "fdbcli --exec 'status minimal' 2>&1 || echo 'unavailable'")

        if [[ "$agg_fdb_status" == *"available"* ]]; then
            log_success "  ✓ FDB accessible from aggregator $first_agg"
        else
            log_warn "  FDB not accessible from $first_agg: $agg_fdb_status"
            log_warn "  Check network connectivity to $QUERIER_MACHINE:4500"
        fi
    fi

    return 0
}

# CSV Headers for comprehensive metrics collection (matching bench_distributed_aggregators.sh format)
AGGREGATOR_CSV_HEADER="timestamp,epoch_type,num_aggregators,total_events,total_events_bytes,total_events_kb,produce_ms,consume_ms,prove_ms_max,verify_ms_max,total_ms,events_per_sec,mb_per_sec,avg_proof_ms,p99_proof_ms,proof_bytes_sum,proof_kb_sum,journal_bytes_sum,journal_kb_sum,epochs_proved,batches_processed,memory_mb_total"

QUERY_CSV_HEADER="timestamp,query_type,iteration,latency_ms,success,prove_ms,verify_ms,proof_bytes,journal_bytes,fdb_fetch_ms,merge_ms,epochs_queried,error_message"

usage() {
    echo "Usage: $0 [start|stop|status|validate|evaluate|report|clean]"
    echo ""
    echo "Commands:"
    echo "  start    - Start distributed E2E test"
    echo "  stop     - Stop all components (local and remote)"
    echo "  status   - Show status of all components"
    echo "  validate - Validate end-to-end data consistency"
    echo "  evaluate - Evaluate querier with configured query types"
    echo "  report   - Generate evaluation report"
    echo "  clean    - Clean up all logs and temporary files"
    echo ""
    echo "Environment variables:"
    echo "  DATA_SOURCE_MACHINE  Data source machine (required)"
    echo "  AGGREGATOR_MACHINES  Aggregator machines (required)"
    echo "  QUERIER_MACHINE      Querier machine (required)"
    echo "  SSH_USER             SSH username"
    echo "  KAFKA_BROKERS        Kafka broker addresses"
    echo "  EVENTS               Total events to produce (default: 1000000)"
    echo "  NUM_AGGREGATORS      Total number of aggregators (default: 4)"
    echo "  RISC0_DEV_MODE       Set to 1 for dev mode (no proofs, fast) (default: 0)"
    echo ""
    echo "Dataset configuration:"
    echo "  DATASET_TYPE         Dataset: synthetic, google_cluster, caida (default: synthetic)"
    echo "  GOOGLE_CLUSTER_DIR   Path to Google Cluster data on remote machine"
    echo "  CAIDA_DIR            Path to CAIDA txt files on remote machine"
    echo ""
    echo "Examples:"
    echo "  # Synthetic data (default):"
    echo "  DATASET_TYPE=synthetic EVENTS=1000000 ./scripts/distributed/run_distributed_e2e.sh start"
    echo ""
    echo "  # Google Cluster data:"
    echo "  DATASET_TYPE=google_cluster \\"
    echo "  GOOGLE_CLUSTER_DIR=/path/to/google_cluster_data/input \\"
    echo "  ./scripts/distributed/run_distributed_e2e.sh start"
    echo ""
    echo "  # CAIDA network traffic data:"
    echo "  DATASET_TYPE=caida \\"
    echo "  CAIDA_DIR=/path/to/caida_pcap/caida_txt \\"
    echo "  ./scripts/distributed/run_distributed_e2e.sh start"
    echo ""
    exit 1
}

validate_config() {
    local errors=0

    if [[ -z "$DATA_SOURCE_MACHINE" ]]; then
        log_error "DATA_SOURCE_MACHINE environment variable is required"
        errors=1
    fi

    if [[ -z "$AGGREGATOR_MACHINES" ]]; then
        log_error "AGGREGATOR_MACHINES environment variable is required"
        errors=1
    fi

    if [[ -z "$QUERIER_MACHINE" ]]; then
        log_error "QUERIER_MACHINE environment variable is required"
        errors=1
    fi

    if [[ $errors -eq 1 ]]; then
        echo ""
        echo "Example:"
        echo "  DATA_SOURCE_MACHINE=\"192.0.2.10\" \\"
        echo "  AGGREGATOR_MACHINES=\"192.0.2.1 192.0.2.2 192.0.2.3\" \\"
        echo "  QUERIER_MACHINE=\"192.0.2.20\" \\"
        echo "  SSH_USER=\"ubuntu\" \\"
        echo "  ./scripts/distributed/run_distributed_e2e.sh start"
        exit 1
    fi

    local num_aggregator_machines=$(echo $AGGREGATOR_MACHINES | wc -w)

    log_info "Configuration:"
    log_info "  Data source machine: $DATA_SOURCE_MACHINE"
    log_info "  Aggregator machines: $AGGREGATOR_MACHINES ($num_aggregator_machines machines)"
    log_info "  Querier machine: $QUERIER_MACHINE"
    log_info "  SSH user: $SSH_USER"
    log_info "  Kafka brokers: $KAFKA_BROKERS"
    log_info "  FDB subspace: $FDB_SUBSPACE"
    local agg_per_machine=$((NUM_AGGREGATORS / num_aggregator_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_aggregator_machines))
    log_info "  Total aggregators: $NUM_AGGREGATORS"
    log_info "  Aggregators per machine: $agg_per_machine (remainder: $agg_remainder)"
    log_info "  Num sources: $NUM_SOURCES"
    log_info "  Total events: $EVENTS"
    log_info "  Epoch type: $EPOCH_TYPE"
    if [[ "$RISC0_DEV_MODE" == "1" ]]; then
        log_info "  RISC0 dev mode: ENABLED (no actual proofs, fast testing)"
    else
        log_info "  RISC0 dev mode: disabled (real proofs)"
    fi
    log_info "  Dataset type: $DATASET_TYPE"
    case "$DATASET_TYPE" in
        google_cluster|google|tsv)
            log_info "  Google Cluster dir: $GOOGLE_CLUSTER_DIR"
            log_info "  TSV max files: $TSV_MAX_FILES"
            log_info "  Source partitioning: source_id = hash(machine_id) %% $NUM_SOURCES, partition = source_id %% $NUM_AGGREGATORS"
            ;;
        caida)
            log_info "  CAIDA dir: $CAIDA_DIR"
            log_info "  CAIDA max files: $CAIDA_MAX_FILES"
            log_info "  Source partitioning: source_id = hash(ip_pair) %% $NUM_SOURCES, partition = source_id %% $NUM_AGGREGATORS"
            ;;
        synthetic)
            log_info "  Series: $SERIES"
            log_info "  Samples per series: $SAMPLES_PER_SERIES"
            log_info "  Source partitioning: single source_id per producer"
            ;;
    esac
    log_info "  Kafka batch size: $KAFKA_BATCH_SIZE"
    log_info "  Commit batch size: $COMMIT_BATCH_SIZE"
    log_info "  Query types: $QUERY_TYPES"
    if [[ -n "$QUERY_EPOCHS" ]]; then
        log_info "  Query mode: latest $QUERY_EPOCHS epochs (overriding time window)"
    else
        log_info "  Query window: $QUERY_WINDOW"
    fi
    log_info "  Query iterations: $NUM_QUERY_ITERATIONS"
    if [[ -n "$KEY_PREFIX" ]]; then
        log_info "  Key prefix: $KEY_PREFIX"
    fi
    log_info ""
    log_info "Separated Architecture Settings:"
    log_info "  Kafka consumer group: $KAFKA_GROUP_ID"
    log_info "  Consumer settle time: ${CONSUMER_SETTLE_SEC}s"
    log_info "  Epoch batch threshold: $EPOCH_BATCH_THRESHOLD"
    log_info "  Epoch timeout: ${EPOCH_TIMEOUT_MS}ms"
    log_info "  Min epochs for querier: $MIN_EPOCHS_FOR_QUERIER"
    log_info "  Epoch wait timeout: ${EPOCH_WAIT_TIMEOUT_SEC}s"
    if [[ "$ZK_AGGREGATION_TIMEOUT" -gt 0 ]]; then
        log_info "  ZK aggregation timeout: ${ZK_AGGREGATION_TIMEOUT}s"
    else
        log_info "  ZK aggregation timeout: none (unlimited)"
    fi
    log_info "  RocksDB base path: $RAW_ROCKSDB_BASE"
    echo ""
}

check_connectivity() {
    log_step "Testing connectivity to remote machines..."

    local failed=0

    # Check data source machine
    log_info "  Checking data source machine..."
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "${SSH_USER}@${DATA_SOURCE_MACHINE}" "echo 'OK'" &>/dev/null; then
        log_info "    ✓ $DATA_SOURCE_MACHINE - connected"
    else
        log_error "    ✗ $DATA_SOURCE_MACHINE - connection failed"
        failed=1
    fi

    # Check aggregator machines
    log_info "  Checking aggregator machines..."
    for machine in $AGGREGATOR_MACHINES; do
        if ssh -o ConnectTimeout=5 -o BatchMode=yes "${SSH_USER}@${machine}" "echo 'OK'" &>/dev/null; then
            log_info "    ✓ $machine - connected"
        else
            log_error "    ✗ $machine - connection failed"
            failed=1
        fi
    done

    # Check querier machine
    log_info "  Checking querier machine..."
    if ssh -o ConnectTimeout=5 -o BatchMode=yes "${SSH_USER}@${QUERIER_MACHINE}" "echo 'OK'" &>/dev/null; then
        log_info "    ✓ $QUERIER_MACHINE - connected"
    else
        log_error "    ✗ $QUERIER_MACHINE - connection failed"
        failed=1
    fi

    if [[ $failed -eq 1 ]]; then
        log_error "Some machines are not accessible"
        exit 1
    fi
}

check_fdb() {
    log_step "Checking FoundationDB connectivity..."

    # Check from the querier machine (the FDB host in distributed mode).
    local fdb_status
    fdb_status=$(ssh -o ConnectTimeout=10 "${SSH_USER}@${QUERIER_MACHINE}" \
        "fdbcli -C '$FDB_CLUSTER_FILE' --exec 'status minimal' 2>&1" || true)

    if echo "$fdb_status" | grep -q "Healthy\\|available"; then
        log_info "  ✓ FDB is healthy (via $QUERIER_MACHINE)"
        return 0
    fi

    log_error "  ✗ FDB is not accessible from $QUERIER_MACHINE"
    log_error "    $fdb_status"
    exit 1
}

check_dataset() {
    log_step "Checking dataset availability on data source machine..."

    local dataset_path=""
    local dataset_desc=""

    case "$DATASET_TYPE" in
        google_cluster|google|tsv)
            dataset_path="$GOOGLE_CLUSTER_DIR"
            dataset_desc="Google Cluster"
            ;;
        caida)
            dataset_path="$CAIDA_DIR"
            dataset_desc="CAIDA"
            ;;
        synthetic|*)
            log_info "  ✓ Using synthetic data (no dataset check needed)"
            return 0
            ;;
    esac

    log_info "  Checking $dataset_desc dataset at: $dataset_path"

    # Check if directory exists on remote machine
    if ssh -o ConnectTimeout=5 "${SSH_USER}@${DATA_SOURCE_MACHINE}" "test -d '$dataset_path'" 2>/dev/null; then
        # Count files in the directory
        local file_count=$(ssh "${SSH_USER}@${DATA_SOURCE_MACHINE}" "ls -1 '$dataset_path' 2>/dev/null | wc -l")
        if [[ "$file_count" -gt 0 ]]; then
            log_info "  ✓ $dataset_desc dataset found ($file_count files)"
        else
            log_error "  ✗ $dataset_desc dataset directory is empty: $dataset_path"
            exit 1
        fi
    else
        log_error "  ✗ $dataset_desc dataset not found at: $dataset_path"
        log_error "    Please ensure the dataset exists on $DATA_SOURCE_MACHINE"
        exit 1
    fi
}

reset_fdb_subspace() {
    log_step "Resetting FDB subspace: $FDB_SUBSPACE"

    # Clear the subspace for clean test.
    # Run on the querier machine to avoid hangs when the orchestrator can't reach FDB directly.
    local reset_timeout_sec="${FDB_RESET_TIMEOUT_SEC:-30}"

    log_info "  Clearing subspace on $QUERIER_MACHINE (timeout: ${reset_timeout_sec}s)..."
    if ssh -o ConnectTimeout=10 "${SSH_USER}@${QUERIER_MACHINE}" \
        "FDB_CLUSTER_FILE='$FDB_CLUSTER_FILE' FDB_RESET_TIMEOUT_SEC='${reset_timeout_sec}' '${REMOTE_PROJECT_DIR}/scripts/setup/reset_fdb.sh' '$FDB_SUBSPACE'"; then
        log_info "  ✓ FDB subspace cleared"
        return 0
    fi

    log_warn "  ✗ Failed to clear FDB subspace '${FDB_SUBSPACE}' on $QUERIER_MACHINE"
    log_warn "  Proceeding anyway (set DEBUG=1 for more context)."
    return 0
}

force_cleanup_all_machines() {
    log_step "Force cleanup: killing processes and cleaning state on all machines..."

    local all_machines="$DATA_SOURCE_MACHINE $AGGREGATOR_MACHINES $QUERIER_MACHINE"
    local unique_machines=$(echo "$all_machines" | tr ' ' '\n' | sort -u | tr '\n' ' ')

    # Step 1: Kill all benchmark-related processes in parallel
    log_info "  Killing processes (aggregator, querier, kafka-producer, r0vm)..."
    local kill_pids=()
    for machine in $unique_machines; do
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "${SSH_USER}@${machine}" \
            "pkill -9 -f 'aggregator' 2>/dev/null; \
             pkill -9 -f 'querier' 2>/dev/null; \
             pkill -9 -f 'kafka-producer' 2>/dev/null; \
             pkill -9 -f 'kafka-consumer' 2>/dev/null; \
             pkill -9 -f 'r0vm' 2>/dev/null; \
             true" &
        kill_pids+=($!)
    done
    # Wait for all kill commands
    for pid in "${kill_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Step 2: Wait for processes to fully terminate
    log_info "  Waiting for processes to terminate..."
    sleep 3

    # Step 3: Clean all state (RocksDB, logs, pid files) in parallel
    log_info "  Cleaning state directories (separated architecture)..."
    local clean_pids=()
    for machine in $unique_machines; do
        ssh -o ConnectTimeout=5 -o StrictHostKeyChecking=no "${SSH_USER}@${machine}" \
            "rm -rf /tmp/zktelemetry_agg_* 2>/dev/null; \
             rm -rf /tmp/zktelemetry_consumer_* 2>/dev/null; \
             rm -rf /tmp/zktelemetry_zkagg_* 2>/dev/null; \
             rm -rf /mydata/rocksdb* 2>/dev/null; \
             rm -f /tmp/zktelemetry_querier.* 2>/dev/null; \
             rm -f /tmp/zktelemetry_datasource.log 2>/dev/null; \
             rm -rf ${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e/*.log 2>/dev/null; \
             true" &
        clean_pids+=($!)
    done
    # Wait for all cleanup commands
    for pid in "${clean_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Final sync barrier - ensure all cleanup operations are complete
    log_info "Sync barrier - waiting for all machines..."
    sleep 2

    # Step 4: Verify cleanup succeeded and retry if needed
    log_info "  Verifying cleanup..."
    local max_retries=3
    local retry=0

    while [[ $retry -lt $max_retries ]]; do
        local cleanup_ok=true
        local failed_machines=()

        for machine in $unique_machines; do
            # Check if RocksDB directories still exist
            if ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "test -d /mydata/rocksdb" 2>/dev/null; then
                cleanup_ok=false
                failed_machines+=("$machine")
            fi
        done

        if [[ "$cleanup_ok" == "true" ]]; then
            log_info "  Cleanup verified - all state databases cleaned"
            break
        fi

        retry=$((retry + 1))
        if [[ $retry -lt $max_retries ]]; then
            log_warn "  Cleanup incomplete on: ${failed_machines[*]}, retrying ($retry/$max_retries)..."
            sleep 2
            # Retry cleanup on failed machines
            for machine in "${failed_machines[@]}"; do
                ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                    "pkill -9 -f 'aggregator' 2>/dev/null; \
                     pkill -9 -f 'kafka-consumer' 2>/dev/null; \
                     pkill -9 -f 'r0vm' 2>/dev/null; \
                     rm -rf /mydata/rocksdb /mydata/rocksdb_* 2>/dev/null; \
                     rm -rf /tmp/zktelemetry_agg_* 2>/dev/null; \
                     rm -rf /tmp/zktelemetry_consumer_* 2>/dev/null; \
                     rm -rf /tmp/zktelemetry_zkagg_* 2>/dev/null; \
                     true" || true
            done
            sleep 2
        else
            log_error "  Cleanup failed after $max_retries retries on: ${failed_machines[*]}"
        fi
    done

    log_success "  ✓ Force cleanup complete"
}

clean_rocksdb_buffers() {
    log_step "Cleaning RocksDB buffers on all machines..."

    local all_machines="$DATA_SOURCE_MACHINE $AGGREGATOR_MACHINES $QUERIER_MACHINE"
    local unique_machines=$(echo "$all_machines" | tr ' ' '\n' | sort -u | tr '\n' ' ')

    # Clean all machines in parallel
    local pids=()
    for machine in $unique_machines; do
        ssh "${SSH_USER}@${machine}" \
            "rm -rf /tmp/zktelemetry_agg_* /tmp/zktelemetry_consumer_* /tmp/zktelemetry_zkagg_* /mydata/rocksdb* 2>/dev/null || true" &
        pids+=($!)
    done

    for pid in "${pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    log_info "  ✓ RocksDB buffers cleaned"
}

# Sync code to all remote machines (in parallel)
sync_code_to_machines() {
    log_step "Syncing code to remote machines (parallel)..."

    local all_machines="$DATA_SOURCE_MACHINE $AGGREGATOR_MACHINES $QUERIER_MACHINE"
    # Deduplicate machines (in case same machine is used for multiple roles)
    local unique_machines=$(echo "$all_machines" | tr ' ' '\n' | sort -u | tr '\n' ' ')
    local pids=()
    local machines_array=()

    for machine in $unique_machines; do
        log_info "  Starting sync to $machine:${REMOTE_PROJECT_DIR}..."
        rsync -az --delete \
            -e "ssh -o ConnectTimeout=30 -o ServerAliveInterval=5 -o ServerAliveCountMax=3" \
            --exclude 'target' \
            --exclude '.git' \
            --exclude 'bench_logs' \
            --exclude 'bench_csv' \
            --exclude 'testdata' \
            --exclude '*.log' \
            "${ROOT_DIR}/" "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/" &
        pids+=($!)
        machines_array+=("$machine")
    done

    # Wait for all syncs to complete
    local failed=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log_info "  ✓ Sync complete: ${machines_array[$i]}"
        else
            log_warn "  ✗ Failed to sync to ${machines_array[$i]}"
            failed=$((failed + 1))
        fi
    done

    log_info "  ✓ Code synced to ${#machines_array[@]} machine(s) ($failed failed)"
}

# Build binaries locally
build_binaries() {
    log_step "Building binaries locally..."

    local build_output

    # Build kafka-producer (data source)
    log_info "  Building kafka-producer..."
    build_output=$(cargo build --release -p data_source --features "kafka" 2>&1) || {
        log_error "Failed to build data_source:"
        echo "$build_output"
        exit 1
    }

    # Build kafka-consumer (separated consumer for writing to RocksDB)
    log_info "  Building kafka-consumer..."
    build_output=$(cargo build --release -p aggregator --features "kafka" --bin kafka-consumer 2>&1) || {
        log_error "Failed to build kafka-consumer:"
        echo "$build_output"
        exit 1
    }

    # Build aggregator (for ZK proof generation from RocksDB)
    log_info "  Building aggregator..."
    build_output=$(cargo build --release -p aggregator --features "kafka fdb" 2>&1) || {
        log_error "Failed to build aggregator:"
        echo "$build_output"
        exit 1
    }

    # Build querier server
    log_info "  Building querier..."
    build_output=$(cargo build --release -p querier --features "fdb" 2>&1) || {
        log_error "Failed to build querier:"
        echo "$build_output"
        exit 1
    }

    log_success "  ✓ All binaries built successfully"
}

# Sync compiled binaries to all remote machines (in parallel)
sync_binaries_to_machines() {
    log_step "Syncing binaries to remote machines (parallel)..."

    local all_machines="$DATA_SOURCE_MACHINE $AGGREGATOR_MACHINES $QUERIER_MACHINE"
    # Deduplicate machines
    local unique_machines=$(echo "$all_machines" | tr ' ' '\n' | sort -u | tr '\n' ' ')
    local pids=()
    local machines_array=()

    local binaries=(
        "${ROOT_DIR}/target/release/kafka-producer"
        "${ROOT_DIR}/target/release/kafka-consumer"
        "${ROOT_DIR}/target/release/aggregator"
        "${ROOT_DIR}/target/release/querier"
    )

    for machine in $unique_machines; do
        log_info "  Starting binary sync to $machine..."
        (
            # Ensure target/release directory exists on remote
            ssh -o ConnectTimeout=30 -o ServerAliveInterval=5 -o ServerAliveCountMax=3 "${SSH_USER}@${machine}" "mkdir -p ${REMOTE_PROJECT_DIR}/target/release" && \
            # Sync binaries
            rsync -az -e "ssh -o ConnectTimeout=30 -o ServerAliveInterval=5 -o ServerAliveCountMax=3" "${binaries[@]}" "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/target/release/"
        ) &
        pids+=($!)
        machines_array+=("$machine")
    done

    # Wait for all syncs to complete
    local failed=0
    for i in "${!pids[@]}"; do
        if wait "${pids[$i]}"; then
            log_info "  ✓ Binaries synced: ${machines_array[$i]}"
        else
            log_warn "  ✗ Failed to sync binaries to ${machines_array[$i]}"
            failed=$((failed + 1))
        fi
    done

    log_info "  ✓ Binaries synced to ${#machines_array[@]} machine(s) ($failed failed)"
}

setup_kafka_topic() {
    log_step "Setting up Kafka topic: $KAFKA_TOPIC with $NUM_AGGREGATORS partitions..."

    if ! command -v docker &>/dev/null || ! docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
        log_warn "  ⚠ Cannot setup Kafka topic (docker not available or kafka not running)"
        return
    fi

    # Check if Kafka is configured for distributed mode (advertised listener should not be localhost)
    local advertised_listeners=$(docker exec kafka bash -c 'echo $KAFKA_ADVERTISED_LISTENERS' 2>/dev/null || echo "")
    if [[ "$advertised_listeners" == *"localhost:9092"* ]]; then
        log_warn "  ⚠ Kafka may be configured for local-only mode (advertised listener contains localhost:9092)"
        log_warn "    Remote consumers may fail to connect!"
        log_warn "    If consumers fail, restart Kafka with KAFKA_EXTERNAL_IP set:"
        log_warn "      cd scripts && docker-compose -f docker-compose-kafka.yml down"
        log_warn "      KAFKA_EXTERNAL_IP=${KAFKA_BROKERS%%:*} docker-compose -f docker-compose-kafka.yml up -d"
    else
        log_info "  Kafka advertised listeners: $advertised_listeners"
    fi

    # Delete existing topic (this also removes consumer group assignments)
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --delete --topic "$KAFKA_TOPIC" 2>/dev/null || true

    # Delete consumer group to clear stale members
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$KAFKA_GROUP_ID" --delete 2>/dev/null || true

    # Wait for topic to be fully deleted
    log_info "  Waiting for topic deletion to complete..."
    local wait_count=0
    while docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep -q "^${KAFKA_TOPIC}$"; do
        sleep 1
        wait_count=$((wait_count + 1))
        if [[ $wait_count -ge 30 ]]; then
            log_warn "  Topic deletion taking too long, proceeding anyway"
            break
        fi
    done
    log_info "  Topic deleted after ${wait_count}s"

    # Create fresh topic with partitions matching num_aggregators
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$KAFKA_TOPIC" \
        --partitions "$NUM_AGGREGATORS" \
        --replication-factor 1 \
        --config retention.ms=604800000

    # Verify topic was created with correct partitions
    local actual_partitions=$(docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --describe --topic "$KAFKA_TOPIC" 2>/dev/null | grep -c "Partition:")
    if [[ "$actual_partitions" -ne "$NUM_AGGREGATORS" ]]; then
        log_warn "  Topic created with $actual_partitions partitions, expected $NUM_AGGREGATORS"
    fi

    # Reset consumer group offsets to earliest (ensure clean start)
    docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
        --group "$KAFKA_GROUP_ID" --reset-offsets --to-earliest \
        --topic "$KAFKA_TOPIC" --execute 2>/dev/null || true

    log_info "  ✓ Kafka topic ready ($actual_partitions partitions, offsets reset)"
}

# =============================================================================
# Separated Architecture Functions
# =============================================================================
# The separated architecture uses:
# 1. kafka-consumer: Consumes from Kafka, writes batches to RocksDB
# 2. aggregator --rocksdb: Reads from RocksDB, generates proofs, writes to FDB
# This allows the querier to start serving once MIN_EPOCHS_FOR_QUERIER epochs are in FDB,
# while ZK aggregation continues in the background.

# Global variables for process tracking
CONSUMER_PIDS_AND_HOSTS=""
ZK_AGG_PIDS_AND_HOSTS=""

# Start kafka-consumer instances on aggregator machines
# These consume from Kafka and write batches to RocksDB
start_kafka_consumers() {
    log_step "Starting kafka-consumer instances on aggregator machines..."

    mkdir -p "$LOG_DIR"

    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))
    local pids_and_hosts=""

    # First, ensure no stale kafka-consumer processes are running
    log_info "  Checking for stale kafka-consumer processes..."
    for machine in $AGGREGATOR_MACHINES; do
        local stale_count=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
            "pgrep -c -f kafka-consumer 2>/dev/null || echo 0" 2>/dev/null || echo 0)
        if [[ "$stale_count" -gt 0 ]]; then
            log_warn "    Found $stale_count stale kafka-consumer on $machine, killing..."
            ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "pkill -9 -f kafka-consumer" 2>/dev/null || true
        fi
    done
    sleep 1

    local machine_idx=0
    local global_agg_id=0
    for machine in $AGGREGATOR_MACHINES; do
        # Distribute remainder: first N machines get 1 extra consumer
        local this_machine_aggs=$agg_per_machine
        if [[ $machine_idx -lt $agg_remainder ]]; then
            this_machine_aggs=$((agg_per_machine + 1))
        fi

        log_info "  Starting $this_machine_aggs kafka-consumer(s) on $machine..."

        for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
            # Remote log directory
            local remote_log_dir="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e"
            local remote_log="${remote_log_dir}/consumer_${global_agg_id}.log"

            # Each machine uses the same local RocksDB path (not indexed)
            local raw_db="${RAW_ROCKSDB_BASE}"

            # Start kafka-consumer on remote machine - run SSH synchronously to capture PID
            local pid=$(ssh "${SSH_USER}@${machine}" "
                cd $REMOTE_PROJECT_DIR || { echo 'ERROR: cd failed' >&2; exit 1; }
                mkdir -p $raw_db
                mkdir -p /tmp/zktelemetry_consumer_${global_agg_id}
                mkdir -p ${remote_log_dir}

                # Verify binary exists
                if [[ ! -x ./target/release/kafka-consumer ]]; then
                    echo 'ERROR: binary not found' >&2
                    exit 1
                fi

                nohup bash -c '
                    echo \"[kafka-consumer] Starting consumer ${global_agg_id} on \$(hostname)...\"
                    echo \"[kafka-consumer] RAW_DB_PATH=$raw_db\"
                    echo \"[kafka-consumer] KAFKA_BROKERS=$KAFKA_BROKERS\"
                    echo \"[kafka-consumer] KAFKA_PARTITION_ID=$global_agg_id\"
                    source ~/.cargo/env 2>/dev/null || true
                    KAFKA_BROKERS=$KAFKA_BROKERS \
                    KAFKA_TOPIC=$KAFKA_TOPIC \
                    KAFKA_GROUP_ID=$KAFKA_GROUP_ID \
                    KAFKA_PARTITION_ID=$global_agg_id \
                    AGGREGATOR_ID=$global_agg_id \
                    RAW_DB_PATH=$raw_db \
                    EPOCH_BATCH_THRESHOLD=$EPOCH_BATCH_THRESHOLD \
                    EPOCH_TIMEOUT_MS=$EPOCH_TIMEOUT_MS \
                    VERBOSE_HASH_LOGGING=$VERBOSE_HASH_LOGGING \
                    ./target/release/kafka-consumer
                ' > ${remote_log} 2>&1 </dev/null &
                consumer_pid=\$!
                disown \$consumer_pid
                echo \$consumer_pid > /tmp/zktelemetry_consumer_${global_agg_id}/pid
                echo \$consumer_pid
            " </dev/null 2>&1 | tail -1 | tr -d ' ')

            if [[ -n "$pid" && "$pid" =~ ^[0-9]+$ ]]; then
                pids_and_hosts="${pids_and_hosts} ${machine}:${pid}"
                log_info "    Consumer $global_agg_id started on $machine (PID: $pid)"
            else
                log_warn "    Failed to get PID for consumer_${global_agg_id} on ${machine}"
            fi

            global_agg_id=$((global_agg_id + 1))
        done

        machine_idx=$((machine_idx + 1))
    done

    # Start background log sync after consumers are started
    log_info "  Starting background log sync..."
    start_log_sync 3

    log_info "  PID collection complete: ${pids_and_hosts}"
    CONSUMER_PIDS_AND_HOSTS="$pids_and_hosts"
    log_success "  ✓ Started $NUM_AGGREGATORS kafka-consumer(s) across $num_machines machines"
}

# Wait for kafka-consumers to be ready (check logs for "started" pattern)
# This is a best-effort check - consumers run forever so we just need them started
wait_for_consumers_ready() {
    log_step "Waiting for kafka-consumers to be ready..."

    local timeout_sec="${1:-30}"  # Reduced from 60s - consumers should start quickly
    local start_time=$(date +%s)

    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout_sec ]]; then
            log_warn "  ⚠ Timeout waiting for consumers after ${timeout_sec}s - proceeding anyway"
            log_warn "    (Consumers may still be starting, check logs if issues occur)"
            return 0  # Return success - consumers run forever, they'll catch up
        fi

        local ready_count=0
        local machine_idx=0
        local global_agg_id=0

        for machine in $AGGREGATOR_MACHINES; do
            local this_machine_aggs=$agg_per_machine
            if [[ $machine_idx -lt $agg_remainder ]]; then
                this_machine_aggs=$((agg_per_machine + 1))
            fi

            for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                # Check remote log for "started" pattern with timeout to prevent hanging
                local remote_log="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e/consumer_${global_agg_id}.log"
                if timeout 5 ssh -o ConnectTimeout=2 -o ServerAliveInterval=1 -o ServerAliveCountMax=2 \
                    "${SSH_USER}@${machine}" "grep -q 'kafka-consumer.*started' '$remote_log' 2>/dev/null" 2>/dev/null; then
                    ready_count=$((ready_count + 1))
                fi
                global_agg_id=$((global_agg_id + 1))
            done
            machine_idx=$((machine_idx + 1))
        done

        log_info "  Progress: $ready_count/$NUM_AGGREGATORS consumers ready (${elapsed}s elapsed)"

        if [[ $ready_count -ge $NUM_AGGREGATORS ]]; then
            log_info "  ✓ All $NUM_AGGREGATORS kafka-consumer(s) ready and assigned to partitions"

            # Also check Kafka consumer group if docker available
            if command -v docker &>/dev/null && docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
                local group_members=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
                    --describe --group "$KAFKA_GROUP_ID" 2>/dev/null | grep -c "$KAFKA_TOPIC" || echo "0")
                log_info "  Consumer group has $group_members members"
            fi

            return 0
        fi

        # Log progress every 10 seconds
        if [[ $((elapsed % 10)) -eq 0 ]] && [[ $elapsed -gt 0 ]]; then
            log_info "  Progress: $ready_count/$NUM_AGGREGATORS consumers ready (${elapsed}s elapsed)..."
        fi

        sleep 2
    done
}

# Wait for all events to be consumed (Kafka lag = 0)
wait_for_consumption() {
    local expected_events="$1"
    local timeout_sec="${2:-300}"
    local start_time=$(date +%s)

    log_step "Waiting for consumption of $expected_events events (timeout: ${timeout_sec}s)..."

    # Check if we can use local docker to check Kafka lag
    local has_docker_kafka=false
    if command -v docker &>/dev/null && docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^kafka$'; then
        has_docker_kafka=true
        log_info "  Using local Kafka container to check consumer lag"
    else
        log_info "  No local Kafka container - will wait fixed time for consumers"
    fi

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout_sec ]]; then
            log_warn "  ⚠ Timeout waiting for consumption after ${timeout_sec}s"
            return 1
        fi

        if [[ "$has_docker_kafka" == "true" ]]; then
            # Check consumer lag via local docker
            local lag=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
                --group "$KAFKA_GROUP_ID" --describe 2>/dev/null | \
                awk 'NR>1 {sum += $6} END {print sum+0}' || echo "999999")

            if [[ "$lag" == "0" ]] || [[ -z "$lag" ]]; then
                log_info "  ✓ All events consumed (lag=0)"

                # Wait for kafka-consumers to finish writing all batches to RocksDB
                if [[ "$CONSUMER_SETTLE_SEC" -gt 0 ]]; then
                    log_info "  Waiting ${CONSUMER_SETTLE_SEC}s for kafka-consumers to finish writing to RocksDB..."
                    sleep "$CONSUMER_SETTLE_SEC"
                fi

                return 0
            fi

            # Log progress every 5 seconds
            if [[ $((elapsed % 5)) -eq 0 ]]; then
                log_info "  Consumer lag: $lag events (${elapsed}s elapsed)"
            fi
        else
            # No docker - use fixed wait time with progress
            local fixed_wait=60  # Wait 60 seconds for consumption
            if [[ $elapsed -ge $fixed_wait ]]; then
                log_info "  ✓ Fixed wait complete (${fixed_wait}s) - assuming consumption done"
                if [[ "$CONSUMER_SETTLE_SEC" -gt 0 ]]; then
                    log_info "  Waiting ${CONSUMER_SETTLE_SEC}s for kafka-consumers to finish writing to RocksDB..."
                    sleep "$CONSUMER_SETTLE_SEC"
                fi
                return 0
            fi
            # Log progress every 5 seconds
            if [[ $((elapsed % 5)) -eq 0 ]]; then
                log_info "  Waiting for consumption... (${elapsed}s / ${fixed_wait}s)"
            fi
        fi

        sleep 2
    done
}

# Signal kafka-consumers to flush all pending batches as epochs (SIGUSR1)
signal_flush_epochs() {
    log_step "Signaling kafka-consumers to flush pending epochs (SIGUSR1)..."

    local ssh_pids=()
    for entry in $CONSUMER_PIDS_AND_HOSTS; do
        IFS=':' read -r machine pid <<< "$entry"

        # Send SIGUSR1 to remote process
        ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" "kill -USR1 $pid 2>/dev/null || true" &
        ssh_pids+=($!)
    done

    # Wait for SSH commands to complete
    for ssh_pid in "${ssh_pids[@]}"; do
        wait "$ssh_pid" 2>/dev/null || true
    done

    # Wait for flush to complete (epoch creation + RocksDB write)
    log_info "  Waiting 3s for epoch flush to complete..."
    sleep 3

    log_success "  ✓ Flush signal sent to all kafka-consumers"
}

# Start ZK aggregators to process events from RocksDB and write proofs to FDB
start_zk_aggregators() {
    log_step "Starting ZK aggregators on aggregator machines..."

    mkdir -p "$LOG_DIR"

    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))
    local pids_and_hosts=""
    local ssh_pids=()

    # Build dev mode export if enabled
    local dev_mode_export=""
    [[ "$RISC0_DEV_MODE" == "1" ]] && dev_mode_export="export RISC0_DEV_MODE=1;"

    local machine_idx=0
    local global_agg_id=0
    for machine in $AGGREGATOR_MACHINES; do
        local this_machine_aggs=$agg_per_machine
        if [[ $machine_idx -lt $agg_remainder ]]; then
            this_machine_aggs=$((agg_per_machine + 1))
        fi

        log_info "  Starting $this_machine_aggs ZK aggregator(s) on $machine..."

        for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
            # Remote paths
            local remote_log_dir="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e"
            local remote_log="${remote_log_dir}/zkvm_${global_agg_id}.log"

            # RocksDB paths (same as kafka-consumer)
            local raw_db_primary="${RAW_ROCKSDB_BASE}"
            local raw_db_secondary="${RAW_ROCKSDB_SECONDARY_BASE}"
            local agg_db="${RAW_ROCKSDB_BASE}_agg"

            # Start ZK aggregator on remote machine
            # FDB writes are ENABLED for E2E (unlike benchmark which disables them)
            ssh "${SSH_USER}@${machine}" "
                cd $REMOTE_PROJECT_DIR
                mkdir -p $agg_db $raw_db_secondary
                mkdir -p /tmp/zktelemetry_zkagg_${global_agg_id}
                mkdir -p ${remote_log_dir}

                nohup bash -c '
                    source ~/.cargo/env 2>/dev/null || true
                    ${dev_mode_export}
                    RAW_ROCKSDB_PATH=$raw_db_primary \
                    RAW_ROCKSDB_SECONDARY_PATH=$raw_db_secondary \
                    AGG_ROCKSDB_PATH=$agg_db \
                    AGGREGATOR_ID=$global_agg_id \
                    FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE \
                    FDB_SUBSPACE=$FDB_SUBSPACE \
                    AGGR_IDLE_TIMEOUT_SECS=300 \
                    VERBOSE_HASH_LOGGING=$VERBOSE_HASH_LOGGING \
                    ./target/release/aggregator \
                        --rocksdb --mode $EPOCH_TYPE
                ' > ${remote_log} 2>&1 </dev/null &
                zk_pid=\$!
                disown \$zk_pid
                echo \$zk_pid > /tmp/zktelemetry_zkagg_${global_agg_id}/pid
            " </dev/null &
            ssh_pids+=($!)

            global_agg_id=$((global_agg_id + 1))
        done

        machine_idx=$((machine_idx + 1))
    done

    # Wait only for SSH commands to complete (not the data source)
    for pid in "${ssh_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Collect PIDs from pid files
    machine_idx=0
    global_agg_id=0
    for machine in $AGGREGATOR_MACHINES; do
        local this_machine_aggs=$agg_per_machine
        if [[ $machine_idx -lt $agg_remainder ]]; then
            this_machine_aggs=$((agg_per_machine + 1))
        fi

        for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
            local pid=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                "cat /tmp/zktelemetry_zkagg_${global_agg_id}/pid 2>/dev/null" || echo "")
            if [[ -n "$pid" ]]; then
                pids_and_hosts="${pids_and_hosts} ${machine}:${pid}"
            fi
            global_agg_id=$((global_agg_id + 1))
        done

        machine_idx=$((machine_idx + 1))
    done

    ZK_AGG_PIDS_AND_HOSTS="$pids_and_hosts"
    log_success "  ✓ Started $NUM_AGGREGATORS ZK aggregator(s) across $num_machines machines"
}

# Wait for minimum epochs to be written to FDB before starting querier
wait_for_min_epochs_in_fdb() {
    local min_epochs="${1:-$MIN_EPOCHS_FOR_QUERIER}"
    local timeout_sec="${2:-$EPOCH_WAIT_TIMEOUT_SEC}"
    local start_time=$(date +%s)

    log_step "Waiting for at least $min_epochs epoch(s) in FDB (timeout: ${timeout_sec}s)..."

    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))

    while true; do
        local current_time=$(date +%s)
        local elapsed=$((current_time - start_time))

        if [[ $elapsed -ge $timeout_sec ]]; then
            log_warn "  ⚠ Timeout waiting for epochs after ${timeout_sec}s"
            return 1
        fi

        # Count total epochs proved by checking ZK aggregator logs
        local total_epochs=0
        local machine_idx=0
        local global_agg_id=0

        for machine in $AGGREGATOR_MACHINES; do
            local this_machine_aggs=$agg_per_machine
            if [[ $machine_idx -lt $agg_remainder ]]; then
                this_machine_aggs=$((agg_per_machine + 1))
            fi

            for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                local remote_log="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e/zkvm_${global_agg_id}.log"
                # Count "DONE: epochs_proved=" or "Epoch seq=.*completed" patterns
                local agg_epochs=$(ssh -o ConnectTimeout=2 "${SSH_USER}@${machine}" \
                    "grep -c 'Epoch seq=.*completed' '$remote_log' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\n' || echo 0)
                # Ensure agg_epochs is a valid number
                [[ "$agg_epochs" =~ ^[0-9]+$ ]] || agg_epochs=0
                total_epochs=$((total_epochs + agg_epochs))
                global_agg_id=$((global_agg_id + 1))
            done
            machine_idx=$((machine_idx + 1))
        done

        if [[ $total_epochs -ge $min_epochs ]]; then
            log_success "  ✓ Found $total_epochs epoch(s) proved (>= $min_epochs required)"
            return 0
        fi

        # Log progress every iteration (every 5 seconds)
        log_info "  [epochs] $total_epochs/$min_epochs proved (${elapsed}s elapsed)"

        sleep 5
    done
}

# Wait for all ZK aggregators to complete
wait_for_zk_completion() {
    local timeout_sec="${1:-0}"  # 0 = no timeout

    # Calculate expected epochs based on events and batch configuration
    # This is an estimate: actual may vary due to batch distribution across aggregators
    local total_batches=$((EVENTS / BATCH_SIZE))
    local expected_epochs=$((total_batches / EPOCH_BATCH_THRESHOLD))
    # Add buffer for remainder and distribution across multiple aggregators
    [[ $((total_batches % EPOCH_BATCH_THRESHOLD)) -gt 0 ]] && expected_epochs=$((expected_epochs + 1))
    # Multiply by number of aggregators as each may create epochs independently
    expected_epochs=$((expected_epochs * NUM_AGGREGATORS))

    log_step "Waiting for ZK aggregators to complete all epochs..."
    log_info "  Expected epochs: ~$expected_epochs (EVENTS=$EVENTS, BATCH_SIZE=$BATCH_SIZE, EPOCH_BATCH_THRESHOLD=$EPOCH_BATCH_THRESHOLD)"
    if [[ "$timeout_sec" -gt 0 ]]; then
        log_info "  Timeout: ${timeout_sec}s"
    else
        log_info "  Timeout: none (unlimited)"
    fi

    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))

    local check_interval=10
    local status_interval=30
    local last_status_time=0
    local wait_start_time=$(date +%s)
    local last_epoch_count=0
    local stable_count=0

    # Track completion status for each aggregator
    declare -A completed_aggs

    while true; do
        local elapsed=$(($(date +%s) - wait_start_time))

        # Check timeout
        if [[ "$timeout_sec" -gt 0 ]] && [[ $elapsed -ge $timeout_sec ]]; then
            log_warn "  ⚠ ZK aggregation timeout reached (${timeout_sec}s elapsed)"
            log_info "  Killing remaining aggregators..."
            kill_all_aggregators
            return 1
        fi

        # Count total epochs proved by checking ZK aggregator logs
        local total_epochs=0
        local machine_idx=0
        local global_agg_id=0

        for machine in $AGGREGATOR_MACHINES; do
            local this_machine_aggs=$agg_per_machine
            if [[ $machine_idx -lt $agg_remainder ]]; then
                this_machine_aggs=$((agg_per_machine + 1))
            fi

            for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                local remote_log="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e/zkvm_${global_agg_id}.log"
                # Count "Epoch seq=.*completed" patterns in aggregator logs
                local agg_epochs=$(ssh -o ConnectTimeout=2 "${SSH_USER}@${machine}" \
                    "grep -c 'Epoch seq=.*completed' '$remote_log' 2>/dev/null || echo 0" 2>/dev/null | tr -d '\n' || echo 0)
                # Ensure agg_epochs is a valid number
                [[ "$agg_epochs" =~ ^[0-9]+$ ]] || agg_epochs=0
                total_epochs=$((total_epochs + agg_epochs))
                global_agg_id=$((global_agg_id + 1))
            done
            machine_idx=$((machine_idx + 1))
        done

        # Check if we've reached the expected number of epochs
        # Use 90% threshold to account for estimation variance
        local threshold_epochs=$((expected_epochs * 9 / 10))
        if [[ $total_epochs -ge $threshold_epochs ]]; then
            # Check if epoch count is stable (no new epochs for 2 checks)
            if [[ $total_epochs -eq $last_epoch_count ]]; then
                stable_count=$((stable_count + 1))
                if [[ $stable_count -ge 2 ]]; then
                    log_success "  ✓ All epochs completed: $total_epochs epochs proved (>= ${threshold_epochs} threshold)"
                    log_info "  Killing aggregators to exit cleanly..."
                    kill_all_aggregators
                    return 0
                fi
            else
                stable_count=0
            fi
        fi
        last_epoch_count=$total_epochs

        # Also check if all aggregator processes have naturally exited
        local all_done=true
        local running_count=0
        local r0vm_count=0
        local status_lines=()

        machine_idx=0
        global_agg_id=0
        for machine in $AGGREGATOR_MACHINES; do
            local this_machine_aggs=$agg_per_machine
            if [[ $machine_idx -lt $agg_remainder ]]; then
                this_machine_aggs=$((agg_per_machine + 1))
            fi

            for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                local entry="${machine}:${global_agg_id}"

                # Skip already completed
                if [[ "${completed_aggs[$entry]:-}" == "1" ]]; then
                    global_agg_id=$((global_agg_id + 1))
                    continue
                fi

                local pid=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                    "cat /tmp/zktelemetry_zkagg_${global_agg_id}/pid 2>/dev/null" || echo "")

                if [[ -n "$pid" ]]; then
                    # Check if aggregator process is running
                    local agg_running=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                        "kill -0 $pid 2>/dev/null && echo 1 || echo 0" 2>/dev/null || echo "0")
                    # Check if r0vm subprocess is running
                    local r0vm_running=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${machine}" \
                        "pgrep -f '[r]0vm' >/dev/null 2>&1 && echo 1 || echo 0" 2>/dev/null || echo "0")

                    if [[ "$agg_running" == "1" ]] || [[ "$r0vm_running" == "1" ]]; then
                        all_done=false
                        [[ "$agg_running" == "1" ]] && running_count=$((running_count + 1))
                        [[ "$r0vm_running" == "1" ]] && r0vm_count=$((r0vm_count + 1))
                        status_lines+=("$machine agg$global_agg_id: agg=$([ "$agg_running" == "1" ] && echo "yes" || echo "no") r0vm=$([ "$r0vm_running" == "1" ] && echo "yes" || echo "no")")
                    else
                        completed_aggs[$entry]=1
                        log_info "  ZK aggregator $global_agg_id naturally completed on $machine"
                    fi
                else
                    completed_aggs[$entry]=1
                fi

                global_agg_id=$((global_agg_id + 1))
            done
            machine_idx=$((machine_idx + 1))
        done

        # Exit if all aggregators naturally exited
        if [[ "$all_done" == "true" ]]; then
            log_success "  ✓ All ZK aggregators naturally completed ($total_epochs epochs proved)"
            return 0
        fi

        # Print consolidated status periodically
        local now=$(date +%s)
        if [[ $((now - last_status_time)) -ge $status_interval ]]; then
            log_info "  Status: $total_epochs/$expected_epochs epochs | $running_count aggregators running, $r0vm_count with r0vm active | ${elapsed}s elapsed"
            for line in "${status_lines[@]}"; do
                log_info "    $line"
            done
            last_status_time=$now
        fi

        sleep $check_interval
    done
}

# Kill all aggregators across all machines
kill_all_aggregators() {
    log_info "  Sending kill signals to all aggregators (max 10s)..."

    # Wrapper function that does the actual killing
    _do_kill_aggregators() {
        for machine in $AGGREGATOR_MACHINES; do
            # Fire and forget with timeout
            timeout 5 ssh -o ConnectTimeout=2 -o ServerAliveInterval=1 -o ServerAliveCountMax=1 \
                -o BatchMode=yes "${SSH_USER}@${machine}" \
                "pkill -TERM -f zktelemetry-risc0-aggr 2>/dev/null; sleep 1; pkill -KILL -f zktelemetry-risc0-aggr 2>/dev/null || true" \
                >/dev/null 2>&1 &
        done

        # Wait for background jobs with timeout
        local count=0
        while [[ $count -lt 8 ]]; do
            if ! jobs | grep -q Running; then
                break
            fi
            sleep 1
            count=$((count + 1))
        done

        # Force kill remaining jobs
        jobs -p | xargs -r kill -9 2>/dev/null || true
    }

    # Run with hard timeout
    timeout 10 bash -c "$(declare -f _do_kill_aggregators); _do_kill_aggregators" 2>/dev/null || true

    # Ensure we always continue
    log_info "  ✓ Kill operation completed"
}

start_remote_querier() {
    log_step "Starting querier on $QUERIER_MACHINE:$QUERIER_PORT..."
    log_info "  FDB_SUBSPACE=$FDB_SUBSPACE"
    log_info "  Querier log: /tmp/querier.log (remote) -> $LOG_DIR/querier.log (local)"

    mkdir -p "$LOG_DIR"

    # Kill any existing querier first
    if ! ssh -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}" \
        "pkill -9 -f 'querier-server' 2>/dev/null || true"; then
        log_warn "  ⚠ Could not SSH to querier host to kill existing process (will still try to start): ${SSH_USER}@${QUERIER_MACHINE}"
    fi
    sleep 1

    # Start querier on remote machine using pre-built binary
    # BENCH_PRINT=1 enables detailed metrics logging (prove_ms, verify_ms, proof_bytes, journal_bytes)
    # RISC0_DEV_MODE controls whether real proofs are generated
    log_info "  Starting querier with RISC0_DEV_MODE=$RISC0_DEV_MODE, BENCH_PRINT=1..."
    if ! ssh -o ConnectTimeout=10 "${SSH_USER}@${QUERIER_MACHINE}" "
        cd $REMOTE_PROJECT_DIR
        export BENCH_PRINT=1
        export RISC0_DEV_MODE=$RISC0_DEV_MODE
        export FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE
        export FDB_SUBSPACE=$FDB_SUBSPACE
        export HTTP_LISTEN=0.0.0.0:$QUERIER_PORT
        nohup ./target/release/querier > /tmp/querier.log 2>&1 &
        querier_pid=\$!
        disown \$querier_pid
        echo \$querier_pid > /tmp/zktelemetry_querier.pid
    "; then
        log_error "  ✗ Failed to start querier via SSH"
        return 1
    fi
    log_success "  ✓ Querier started on $QUERIER_MACHINE:$QUERIER_PORT"
    log_info "  Waiting 3s for querier to initialize..."
    sleep 3

    # Verify querier is running
    local querier_running=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" \
        "pgrep -f 'querier-server' >/dev/null && echo 'yes' || echo 'no'" 2>/dev/null || echo "unknown")
    if [[ "$querier_running" == "yes" ]]; then
        log_success "  ✓ Querier process verified running"
    else
        log_warn "  ⚠ Could not verify querier process (status: $querier_running)"
    fi

    # Copy querier log to local machine immediately
    log_info "  Copying initial querier log..."
    scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "${LOG_DIR}/querier.log" 2>/dev/null || true

    # Show first few lines of querier log
    if [[ -f "${LOG_DIR}/querier.log" ]]; then
        log_info "  Querier startup log:"
        head -10 "${LOG_DIR}/querier.log" | while read -r line; do
            log_info "    $line"
        done || true
    fi
}

start_remote_data_source() {
    log_step "Starting data source on data source machine: $DATA_SOURCE_MACHINE..."

    mkdir -p "$LOG_DIR"

    log_info "  Dataset type: $DATASET_TYPE"
    log_info "  Producing $EVENTS events..."

    # Build environment variables based on dataset type
    local bench_input_env=""
    local dataset_env=""

    case "$DATASET_TYPE" in
        google_cluster|google|tsv)
            log_info "  Replaying Google Cluster dataset from: $GOOGLE_CLUSTER_DIR"
            log_info "  source_id = hash(machine_id) %% $NUM_SOURCES, partition = source_id %% $NUM_AGGREGATORS"
            bench_input_env="BENCH_INPUT=tsv"
            dataset_env="TSV_DIR=$GOOGLE_CLUSTER_DIR \
                TSV_MAX_FILES=$TSV_MAX_FILES \
                CSV_VALUE_SCALE=$CSV_VALUE_SCALE \
                CSV_METRIC_ID=$CSV_METRIC_ID \
                TS_MODE=$TS_MODE \
                TS_INTERVAL_MS=$TS_INTERVAL_MS"
            ;;
        caida)
            log_info "  Replaying CAIDA dataset from: $CAIDA_DIR"
            log_info "  source_id = hash(src_ip||dst_ip) %% $NUM_SOURCES, partition = source_id %% $NUM_AGGREGATORS"
            bench_input_env="BENCH_INPUT=caida"
            dataset_env="CAIDA_DIR=$CAIDA_DIR \
                CAIDA_MAX_FILES=$CAIDA_MAX_FILES \
                CAIDA_SORT_BY_SIZE=$CAIDA_SORT_BY_SIZE \
                TS_MODE=$TS_MODE \
                TS_INTERVAL_MS=$TS_INTERVAL_MS"
            ;;
        car_emission|emission)
            log_info "  Replaying Car Emission dataset from: $EMISSION_CSV"
            log_info "  source_id = hash(encoded_key) %% $NUM_SOURCES, partition = source_id %% $NUM_AGGREGATORS"
            log_info "  Value: CO2 emissions (g/km) * $EMISSION_VALUE_SCALE, Timestamp: model year"
            bench_input_env="BENCH_INPUT=car_emission"
            dataset_env="EMISSION_CSV=$EMISSION_CSV \
                EMISSION_VALUE_SCALE=$EMISSION_VALUE_SCALE \
                TS_MODE=$TS_MODE \
                TS_INTERVAL_MS=$TS_INTERVAL_MS"
            ;;
        synthetic|*)
            log_info "  Using synthetic data (series=$SERIES, samples_per_series=$SAMPLES_PER_SERIES)"
            log_info "  Spawning $NUM_SOURCES parallel producers (one per source_id)"
            bench_input_env="BENCH_INPUT=synthetic"
            dataset_env="SERIES=$SERIES \
                SAMPLES_PER_SERIES=$SAMPLES_PER_SERIES"
            ;;
    esac

    # Run data source on remote machine using pre-built binary and capture timing
    local start_time=$(date +%s)

    if [[ "$DATASET_TYPE" == "synthetic" || "$DATASET_TYPE" == "" ]]; then
        # Synthetic mode: spawn NUM_SOURCES producers in parallel (one per source_id)
        # Each producer uses its source_id directly without hash computation
        local events_per_source=$((EVENTS / NUM_SOURCES))
        log_info "  Events per source: $events_per_source (total: $EVENTS, sources: $NUM_SOURCES)"

        ssh "${SSH_USER}@${DATA_SOURCE_MACHINE}" "
            cd $REMOTE_PROJECT_DIR

            # Launch NUM_SOURCES producers in parallel
            pids=()
            for src_id in \$(seq 0 $((NUM_SOURCES - 1))); do
                (
                    KAFKA_BROKERS=$KAFKA_BROKERS \
                    KAFKA_TOPIC=$KAFKA_TOPIC \
                    NUM_AGGREGATORS=$NUM_AGGREGATORS \
                    NUM_SOURCES=$NUM_SOURCES \
                    SOURCE_ID=\$src_id \
                    USE_CONFIGURED_SOURCE_ID=1 \
                    VERBOSE_HASH_LOGGING=$VERBOSE_HASH_LOGGING \
                    $bench_input_env \
                    $dataset_env \
                    ./target/release/kafka-producer \
                        --events $events_per_source \
                        --kafka-batch-size $KAFKA_BATCH_SIZE \
                        --commit-batch-size $COMMIT_BATCH_SIZE \
                        --series $SERIES \
                        --source-id \$src_id \
                    > /tmp/zktelemetry_producer_\${src_id}.log 2>&1
                ) &
                pids+=(\$!)
            done

            # Wait for all producers to finish
            for pid in \"\${pids[@]}\"; do
                wait \$pid
            done

            echo '[kafka-producer] All $NUM_SOURCES producers completed'
        " 2>&1 | tee "$LOG_DIR/datasource.log" | grep -Ev '\[PRODUCER_HASH\]|\[kafka-producer'
    else
        # Dataset mode (google_cluster, caida): parallel producer tasks with hash-based source_id
        log_info "  Using $PARALLEL_PRODUCERS parallel producer tasks"
        ssh "${SSH_USER}@${DATA_SOURCE_MACHINE}" "
            cd $REMOTE_PROJECT_DIR

            bash -c '
                KAFKA_BROKERS=$KAFKA_BROKERS \
                KAFKA_TOPIC=$KAFKA_TOPIC \
                NUM_AGGREGATORS=$NUM_AGGREGATORS \
                NUM_SOURCES=$NUM_SOURCES \
                VERBOSE_HASH_LOGGING=$VERBOSE_HASH_LOGGING \
                $bench_input_env \
                $dataset_env \
                ./target/release/kafka-producer \
                    --events $EVENTS \
                    --kafka-batch-size $KAFKA_BATCH_SIZE \
                    --commit-batch-size $COMMIT_BATCH_SIZE \
                    --series $SERIES \
                    --parallel-producers $PARALLEL_PRODUCERS
            ' 2>&1 | tee /tmp/zktelemetry_datasource.log
        " 2>&1 | tee "$LOG_DIR/datasource.log" | grep -Ev '\[PRODUCER_HASH\]|\[kafka-producer'
    fi

    local end_time=$(date +%s)
    local duration=$((end_time - start_time))

    echo "$duration" > "$LOG_DIR/produce_duration.txt"

    log_success "  ✓ Produced $EVENTS events in ${duration}s (dataset: $DATASET_TYPE)"
}

wait_for_processing() {
    log_step "Waiting for aggregators to process events..."

    local max_wait=300  # 5 minutes
    local waited=0
    local check_interval=5

    while [[ $waited -lt $max_wait ]]; do
        # Check Kafka consumer lag
        if command -v docker &>/dev/null && docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
            local lag=$(docker exec kafka kafka-consumer-groups \
                --bootstrap-server localhost:9092 \
                --describe --group "$KAFKA_GROUP_ID" 2>/dev/null | \
                awk 'NR>1 {sum+=$5} END {print sum+0}')

            log_info "  Consumer lag: $lag events (waited ${waited}s)"

            if [[ $lag -eq 0 ]]; then
                log_success "  ✓ All events processed"
                return 0
            fi
        fi

        sleep $check_interval
        waited=$((waited + check_interval))
    done

    log_warn "  ⚠ Timeout waiting for processing (${max_wait}s)"
    return 1
}

validate_data() {
    log_step "Validating end-to-end data consistency..."

    # Wait for querier to be ready
    log_info "  Waiting for querier to be ready..."
    sleep 5

    # Test query on remote querier with timeout
    log_info "  Running validation query on $QUERIER_MACHINE:$QUERIER_PORT..."
    local query_result=""
    local query_json="{\"type\":\"samples_sum\",\"window\":\"1h\"}"

    if timeout 10 curl -sS --max-time 5 "http://${QUERIER_MACHINE}:$QUERIER_PORT/" >/dev/null 2>&1; then
        query_result=$(curl -sS "http://${QUERIER_MACHINE}:$QUERIER_PORT/query" \
            -H "content-type: application/json" \
            -d "$query_json" 2>&1) || query_result=""
    else
        log_warn "  Direct HTTP to ${QUERIER_MACHINE}:$QUERIER_PORT not reachable from this machine; using SSH curl on the querier host"
        local query_b64
        query_b64=$(printf '%s' "$query_json" | base64 -w0 2>/dev/null || printf '%s' "$query_json" | base64)
        query_result=$(ssh -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 \
            "${SSH_USER}@${QUERIER_MACHINE}" "
            printf '%s' '$query_b64' | base64 -d | \
            curl -sS 'http://127.0.0.1:$QUERIER_PORT/query' \
                -H 'content-type: application/json' \
                --data-binary @- \
                2>/dev/null
        " 2>/dev/null) || query_result=""
    fi

    if [[ -z "$query_result" ]]; then
        log_warn "  ✗ Validation query failed: empty response (this may be normal if querier is still starting)"
        return 1
    elif echo "$query_result" | grep -qi "error\|failed"; then
        log_warn "  ✗ Query returned error: $(echo "$query_result" | head -c 200)"
        return 1
    else
        log_success "  ✓ Query successful"
        echo "$query_result" | jq '.' 2>/dev/null || echo "$query_result"
    fi

    # Check FDB data with timeout
    log_info "  Checking FDB data..."
    local fdb_result
    if fdb_result=$(timeout 10 fdbcli --exec "get \\x01$FDB_SUBSPACE" 2>&1); then
        if echo "$fdb_result" | grep -q "not found"; then
            log_warn "  ⚠ No data found in FDB (may still be processing)"
        else
            log_success "  ✓ Data found in FDB"
        fi
    else
        log_warn "  ⚠ Could not check FDB (timeout or unavailable)"
    fi

    return 0
}

evaluate_querier() {
    log_step "Evaluating querier with multiple query types..."

    mkdir -p "$RESULTS_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local query_metrics_csv="$RESULTS_DIR/query_metrics_${timestamp}.csv"

    # Some environments allow SSH to the querier machine but block direct HTTP access
    # from the orchestrator. Detect that and fall back to running curl via SSH.
    local use_ssh_curl=0
    if ! curl -sS --max-time 2 "http://${QUERIER_MACHINE}:$QUERIER_PORT/" >/dev/null 2>&1; then
        use_ssh_curl=1
        log_warn "  Direct HTTP to ${QUERIER_MACHINE}:$QUERIER_PORT not reachable from this machine; using SSH curl on the querier host"
    fi

    # Create CSV header with comprehensive metrics
    echo "$QUERY_CSV_HEADER" > "$query_metrics_csv"

    # Convert comma-separated query types to array
    IFS=',' read -ra query_types_array <<< "$QUERY_TYPES"

    local total_queries=0
    local successful_queries=0
    local failed_queries=0

    for query_type in "${query_types_array[@]}"; do
        query_type=$(echo "$query_type" | xargs)  # Trim whitespace

        # Warn if prefix query type is used without KEY_PREFIX set
        if [[ "$query_type" == *"_prefix" && -z "$KEY_PREFIX" ]]; then
            log_warn "  Query type '$query_type' requires KEY_PREFIX to be set. Skipping..."
            continue
        fi

        log_info "  Testing query type: $query_type"

        for iteration in $(seq 1 $NUM_QUERY_ITERATIONS); do
            local start_ms=$(date +%s%3N)

            # Build query JSON - use epochs if QUERY_EPOCHS is set, otherwise use window
            local query_json="{\"type\":\"$query_type\""
            if [[ -n "$QUERY_EPOCHS" ]]; then
                query_json="${query_json},\"epochs\":$QUERY_EPOCHS"
            else
                query_json="${query_json},\"window\":\"$QUERY_WINDOW\""
            fi

            # Add prefix for prefix-based query types
            if [[ "$query_type" == *"_prefix" && -n "$KEY_PREFIX" ]]; then
                query_json="${query_json},\"prefix\":\"$KEY_PREFIX\""
            fi

            # Add key field for query types that require it
            # Default to 0 if no QUERY_KEY environment variable is set
            if [[ "$query_type" == "cm_estimate" || \
                  "$query_type" == "samples_avg_key" || \
                  "$query_type" == "samples_sum_key" || \
                  "$query_type" == "samples_sum_exact_key" || \
                  "$query_type" == "samples_raw_max_key" || \
                  "$query_type" == "samples_raw_histogram_bucket_key" || \
                  "$query_type" == "samples_raw_cm_estimate_key" || \
                  "$query_type" == "samples_raw_stats_key" ]]; then
                local key_value="${QUERY_KEY:-0}"
                query_json="${query_json},\"key\":$key_value"
            fi

            # Add mask field for query types that require it (raw samples queries)
            # Default to all bits set (0xFFFFFFFFFFFFFFFF for exact match)
            if [[ "$query_type" == "samples_raw_max_key" || \
                  "$query_type" == "samples_raw_histogram_bucket_key" || \
                  "$query_type" == "samples_raw_cm_estimate_key" || \
                  "$query_type" == "samples_raw_stats_key" ]]; then
                local mask_value="${QUERY_MASK:-18446744073709551615}"  # 0xFFFFFFFFFFFFFFFF
                query_json="${query_json},\"mask\":$mask_value"
            fi

            # Add bucket field for histogram bucket queries
            if [[ "$query_type" == "histogram_bucket" || \
                  "$query_type" == "samples_raw_histogram_bucket_key" ]]; then
                local bucket_value="${QUERY_BUCKET:-0}"
                query_json="${query_json},\"bucket\":$bucket_value"
            fi

            # Add limit field for topk queries
            if [[ "$query_type" == "cm_topk" || \
                  "$query_type" == "samples_sum_topk" ]]; then
                local limit_value="${QUERY_LIMIT:-10}"
                query_json="${query_json},\"limit\":$limit_value"
            fi

            # Add value field for cm estimate queries
            if [[ "$query_type" == "samples_raw_cm_estimate_key" ]]; then
                local value_param="${QUERY_VALUE:-0}"
                query_json="${query_json},\"value\":$value_param"
            fi

            # Add pattern field for pattern-based queries
            if [[ "$query_type" == "samples_sum_key_pattern" || \
                  "$query_type" == "histogram_all_key" ]]; then
                local pattern_value="${QUERY_PATTERN:-0x00}"
                query_json="${query_json},\"pattern\":\"$pattern_value\""
            fi

            query_json="${query_json}}"

            # Temporary file to capture stderr (bench logs) separately
            local stderr_file=$(mktemp)

            # Execute query. In some networks, the querier HTTP port is not reachable
            # directly; in that case we run curl on the querier machine via SSH.
            # Always guard curl/ssh to avoid exiting the whole script under `set -e`.
            local query_result=""
            local curl_err=""
            if [[ "$use_ssh_curl" -eq 0 ]]; then
                if query_result=$(curl -sS "http://${QUERIER_MACHINE}:$QUERIER_PORT/query" \
                    -H "content-type: application/json" \
                    -d "$query_json" 2>"$stderr_file"); then
                    :
                else
                    curl_err="curl_failed"
                fi
            else
                # Send JSON payload safely via base64 to avoid shell escaping issues.
                local query_b64
                query_b64=$(printf '%s' "$query_json" | base64 -w0 2>/dev/null || printf '%s' "$query_json" | base64)
                if query_result=$(ssh -o ConnectTimeout=5 "${SSH_USER}@${QUERIER_MACHINE}" "
                    printf '%s' '$query_b64' | base64 -d | \
                    curl -sS 'http://127.0.0.1:$QUERIER_PORT/query' \
                        -H 'content-type: application/json' \
                        --data-binary @- \
                        2>/dev/null
                " 2>"$stderr_file"); then
                    :
                else
                    curl_err="ssh_curl_failed"
                fi
            fi

            local end_ms=$(date +%s%3N)
            local latency=$((end_ms - start_ms))

            total_queries=$((total_queries + 1))

            # Extract metrics from response JSON
            local prove_ms=$(echo "$query_result" | jq -r '.proof.prove_ms // 0' 2>/dev/null || echo "0")
            local proof_bytes=$(echo "$query_result" | jq -r '.proof.proof_bytes // 0' 2>/dev/null || echo "0")

            # Extract verify_ms and journal_bytes from stderr bench logs if available
            # Format: bench kind=risc0 steps=... prove_ms=... verify_ms=... proof_bytes=...
            local verify_ms=0
            local journal_bytes=0
            local fdb_fetch_ms=0
            local merge_ms=0
            local epochs_queried=0

            # Try to get metrics from querier log on remote machine
            local querier_stderr=$(ssh "${SSH_USER}@${QUERIER_MACHINE}" \
                "tail -20 /tmp/querier.log 2>/dev/null | grep -E 'bench (query|kind)' | tail -2" 2>/dev/null || echo "")

            if [[ -n "$querier_stderr" ]]; then
                # Extract from bench query=... line
                fdb_fetch_ms=$(echo "$querier_stderr" | grep -oP 'db_ms=\K[0-9]+' | tail -1 || echo "0")
                merge_ms=$(echo "$querier_stderr" | grep -oP 'merge_ms=\K[0-9]+' | tail -1 || echo "0")
                epochs_queried=$(echo "$querier_stderr" | grep -oP 'epochs=\K[0-9]+' | tail -1 || echo "0")

                # Extract from bench kind=risc0 line
                # Some querier builds do not include proof metrics in the HTTP response; prefer log parsing if present.
                local log_prove_ms=""
                local log_verify_ms=""
                local log_proof_bytes=""
                local log_journal_bytes=""
                log_prove_ms=$(echo "$querier_stderr" | grep -oP 'prove_ms=\K[0-9]+' | tail -1 || echo "")
                verify_ms=$(echo "$querier_stderr" | grep -oP 'verify_ms=\K[0-9]+' | tail -1 || echo "0")
                log_verify_ms="$verify_ms"
                log_proof_bytes=$(echo "$querier_stderr" | grep -oP 'proof_bytes=\K[0-9]+' | tail -1 || echo "")
                log_journal_bytes=$(echo "$querier_stderr" | grep -oP 'journal_bytes=\K[0-9]+' | tail -1 || echo "")

                [[ -n "$log_verify_ms" ]] && verify_ms="$log_verify_ms"
                [[ -n "$log_journal_bytes" ]] && journal_bytes="$log_journal_bytes"
                if [[ -n "$log_prove_ms" ]]; then
                    prove_ms="$log_prove_ms"
                fi
                if [[ -n "$log_proof_bytes" ]]; then
                    proof_bytes="$log_proof_bytes"
                fi
            fi

            rm -f "$stderr_file"

            # Check if query succeeded
            if [[ -n "$curl_err" || -z "$query_result" ]]; then
                failed_queries=$((failed_queries + 1))
                echo "${timestamp},${query_type},${iteration},${latency},false,0,0,0,0,0,0,0,\"${curl_err:-empty_response}\"" >> "$query_metrics_csv"
                if [[ $iteration -eq 1 ]]; then
                    log_warn "    ✗ Query failed: ${curl_err:-empty_response}"
                fi
            elif echo "$query_result" | grep -qiE "error|failed|invalid|out of range"; then
                failed_queries=$((failed_queries + 1))
                local error_msg=$(echo "$query_result" | tr ',' ' ' | tr '"' "'" | head -c 100)
                echo "${timestamp},${query_type},${iteration},${latency},false,${prove_ms},${verify_ms},${proof_bytes},${journal_bytes},${fdb_fetch_ms},${merge_ms},${epochs_queried},\"${error_msg}\"" >> "$query_metrics_csv"
                if [[ $iteration -eq 1 ]]; then
                    log_warn "    ✗ Query failed: $error_msg"
                fi
            else
                successful_queries=$((successful_queries + 1))
                echo "${timestamp},${query_type},${iteration},${latency},true,${prove_ms},${verify_ms},${proof_bytes},${journal_bytes},${fdb_fetch_ms},${merge_ms},${epochs_queried}," >> "$query_metrics_csv"
                if [[ $iteration -eq 1 ]]; then
                    log_success "    ✓ Query succeeded (${latency}ms, prove=${prove_ms}ms, verify=${verify_ms}ms, proof=${proof_bytes}B, fdb_fetch=${fdb_fetch_ms}ms, epochs=${epochs_queried})"
                fi
            fi
        done

        # Calculate statistics for this query type
        local avg_latency=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {sum+=$4; count++}
            END {print (count>0 ? int(sum/count) : 0)}
        ' "$query_metrics_csv")

        local min_latency=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {if(min=="" || $4<min) min=$4}
            END {print (min=="" ? 0 : min)}
        ' "$query_metrics_csv")

        local max_latency=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {if($4>max) max=$4}
            END {print max+0}
        ' "$query_metrics_csv")

        local avg_prove_ms=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {sum+=$6; count++}
            END {print (count>0 ? int(sum/count) : 0)}
        ' "$query_metrics_csv")

        local avg_verify_ms=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {sum+=$7; count++}
            END {print (count>0 ? int(sum/count) : 0)}
        ' "$query_metrics_csv")

        local total_proof_bytes=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {sum+=$8}
            END {print sum+0}
        ' "$query_metrics_csv")

        local avg_fdb_fetch_ms=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" {sum+=$10; count++}
            END {print (count>0 ? int(sum/count) : 0)}
        ' "$query_metrics_csv")

        local success_rate=$(awk -F',' -v qt="$query_type" '
            $2 == qt {total++; if($5=="true") success++}
            END {print (total>0 ? int(success*100/total) : 0)}
        ' "$query_metrics_csv")

        # Get epochs queried (should be consistent across iterations, take first value)
        local epochs_queried=$(awk -F',' -v qt="$query_type" '
            $2 == qt && $5 == "true" && $12 != "" {print $12; exit}
        ' "$query_metrics_csv")
        epochs_queried=${epochs_queried:-0}

        log_info "    Stats: avg_latency=${avg_latency}ms, min=${min_latency}ms, max=${max_latency}ms, epochs=${epochs_queried}"
        log_info "    Proof: avg_prove=${avg_prove_ms}ms, avg_verify=${avg_verify_ms}ms, total_bytes=${total_proof_bytes}"
        log_info "    FDB: avg_fetch=${avg_fdb_fetch_ms}ms, success_rate=${success_rate}%"
    done

    # Report final status based on failures
    if [[ $failed_queries -gt 0 ]]; then
        log_error "  ✗ Query evaluation FAILED"
        log_info "    Total queries: $total_queries"
        log_info "    Successful: $successful_queries"
        log_error "    Failed: $failed_queries"
        log_info "    Metrics saved to: $query_metrics_csv"
        echo "$query_metrics_csv"
        return 1
    else
        log_success "  ✓ Query evaluation complete"
        log_info "    Total queries: $total_queries"
        log_info "    Successful: $successful_queries"
        log_info "    Failed: $failed_queries"
        log_info "    Metrics saved to: $query_metrics_csv"
        echo "$query_metrics_csv"
    fi
}

collect_all_logs() {
    log_step "Collecting all logs from remote machines (separated architecture)..."

    local logs_collected_dir="$LOG_DIR/collected_e2e"
    mkdir -p "$logs_collected_dir"

    # Collect consumer and zkvm logs (separated architecture)
    local num_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_machines))

    local machine_idx=0
    local global_agg_id=0
    for machine in $AGGREGATOR_MACHINES; do
        local this_machine_aggs=$agg_per_machine
        if [[ $machine_idx -lt $agg_remainder ]]; then
            this_machine_aggs=$((agg_per_machine + 1))
        fi

        log_info "  Collecting $this_machine_aggs consumer/zkvm logs from $machine..."

        # Skip localhost - logs already local
        if [[ "$machine" != "localhost" && "$machine" != "127.0.0.1" ]]; then
            local remote_log_dir="${REMOTE_PROJECT_DIR}/bench_logs/distributed_e2e"

            for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
                # Collect kafka-consumer log
                local remote_consumer_log="${remote_log_dir}/consumer_${global_agg_id}.log"
                local local_consumer_log="$logs_collected_dir/consumer_${machine}_${global_agg_id}.log"
                scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${machine}:${remote_consumer_log}" "$local_consumer_log" 2>/dev/null || \
                    log_warn "    Failed to collect consumer log $global_agg_id from $machine"

                # Collect ZK aggregator (zkvm) log
                local remote_zkvm_log="${remote_log_dir}/zkvm_${global_agg_id}.log"
                local local_zkvm_log="$logs_collected_dir/zkvm_${machine}_${global_agg_id}.log"
                scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${machine}:${remote_zkvm_log}" "$local_zkvm_log" 2>/dev/null || \
                    log_warn "    Failed to collect zkvm log $global_agg_id from $machine"

                global_agg_id=$((global_agg_id + 1))
            done
        else
            # For localhost, just increment the counter
            global_agg_id=$((global_agg_id + this_machine_aggs))
        fi

        machine_idx=$((machine_idx + 1))
    done

    # Collect querier log from querier machine
    log_info "  Collecting querier log from $QUERIER_MACHINE..."
    scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "$logs_collected_dir/querier.log" 2>/dev/null || \
        log_warn "    Failed to collect querier log"

    # Collect data source log from data source machine
    log_info "  Collecting data source log from $DATA_SOURCE_MACHINE..."
    scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${DATA_SOURCE_MACHINE}:/tmp/zktelemetry_datasource.log" "$logs_collected_dir/datasource.log" 2>/dev/null || \
        log_warn "    Failed to collect data source log"

    # Copy local copy of data source log (if exists)
    cp "$LOG_DIR/datasource.log" "$logs_collected_dir/datasource_local.log" 2>/dev/null || true

    log_success "  ✓ All logs collected to: $logs_collected_dir"
}

extract_aggregator_metrics() {
    log_step "Extracting detailed metrics from logs (separated architecture, matching bench format)..."

    # Ensure results directory exists
    mkdir -p "$RESULTS_DIR"

    local logs_dir="$LOG_DIR/collected_e2e"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local metrics_csv="$RESULTS_DIR/aggregator_metrics_${timestamp}.csv"
    local detailed_csv="$RESULTS_DIR/aggregator_detailed_${timestamp}.csv"

    # Per-aggregator detailed metrics
    echo "machine,aggregator_id,total_events,epochs_proved,batches_processed,prove_ms_sum,verify_ms_sum,proof_bytes_sum,journal_bytes_sum,memory_mb,errors,warnings" > "$detailed_csv"

    # Summary metrics (matching bench_distributed_aggregators.sh CSV format)
    echo "$AGGREGATOR_CSV_HEADER" > "$metrics_csv"

    # Counters for summary metrics
    local total_events_all=0
    local prove_ms_max=0
    local verify_ms_max=0
    local proof_bytes_sum=0
    local journal_bytes_sum=0
    local epochs_proved_sum=0
    local batches_processed_sum=0
    local memory_mb_total=0

    local num_aggregator_machines=$(echo $AGGREGATOR_MACHINES | wc -w)
    local agg_per_machine=$((NUM_AGGREGATORS / num_aggregator_machines))
    local agg_remainder=$((NUM_AGGREGATORS % num_aggregator_machines))

    local machine_idx=0
    local global_agg_id=0

    for machine in $AGGREGATOR_MACHINES; do
        # Distribute remainder: first N machines get 1 extra aggregator
        local this_machine_aggs=$agg_per_machine
        if [[ $machine_idx -lt $agg_remainder ]]; then
            this_machine_aggs=$((agg_per_machine + 1))
        fi

        for agg_idx in $(seq 0 $((this_machine_aggs - 1))); do
            # Use zkvm log for ZK aggregator metrics (separated architecture)
            local log_file="$logs_dir/zkvm_${machine}_${global_agg_id}.log"

            if [[ -f "$log_file" ]]; then
                # Extract epochs_proved from summary line (epochs_proved=N)
                local epochs
                epochs=$(grep -oP 'epochs_proved=\K[0-9]+' "$log_file" 2>/dev/null | tail -1)
                epochs=${epochs:-0}

                # Count batches processed (n_events= lines)
                local batches
                batches=$(grep -c "n_events=" "$log_file" 2>/dev/null) || batches=0

                # Sum prove_ms for this aggregator
                local agg_prove_ms=0
                while IFS= read -r ms; do
                    agg_prove_ms=$((agg_prove_ms + ms))
                done < <(grep -oP 'prove_ms=\K[0-9]+' "$log_file" 2>/dev/null || true)

                # Sum verify_ms for this aggregator
                local agg_verify_ms=0
                while IFS= read -r v; do
                    agg_verify_ms=$((agg_verify_ms + v))
                done < <(grep -oP 'verify_ms=\K[0-9]+' "$log_file" 2>/dev/null || true)

                # Sum proof_bytes for this aggregator
                local agg_proof_bytes=0
                while IFS= read -r pb; do
                    agg_proof_bytes=$((agg_proof_bytes + pb))
                done < <(grep -oP 'proof_bytes=\K[0-9]+' "$log_file" 2>/dev/null || true)

                # Sum journal_bytes for this aggregator
                local agg_journal_bytes=0
                while IFS= read -r jb; do
                    agg_journal_bytes=$((agg_journal_bytes + jb))
                done < <(grep -oP 'journal_bytes=\K[0-9]+' "$log_file" 2>/dev/null || true)

                # Extract total events from n_events sum
                local total_events=0
                while IFS= read -r ev; do
                    total_events=$((total_events + ev))
                done < <(grep -oP 'n_events=\K[0-9]+' "$log_file" 2>/dev/null || true)

                # Extract memory usage (last value from /proc/pid/status if available)
                local memory
                memory=$(grep -oP 'VmRSS:\s*\K[0-9]+' "$log_file" 2>/dev/null | tail -1)
                memory=${memory:-0}
                if [[ "$memory" == "0" ]]; then
                    # Try extracting from memory_mb= pattern
                    memory=$(grep -oP 'memory_mb=\K[0-9]+' "$log_file" 2>/dev/null | tail -1)
                    memory=${memory:-0}
                    memory=$((memory * 1024))  # Convert MB to KB for consistency
                fi
                memory=$((memory / 1024))  # Convert KB to MB

                # Count errors and warnings (grep -c returns 1 on no match, so capture output separately)
                local errors
                errors=$(grep -ci "error" "$log_file" 2>/dev/null) || errors=0
                local warnings
                warnings=$(grep -ci "warn" "$log_file" 2>/dev/null) || warnings=0

                # Write per-aggregator detailed metrics
                echo "$machine,$global_agg_id,$total_events,$epochs,$batches,$agg_prove_ms,$agg_verify_ms,$agg_proof_bytes,$agg_journal_bytes,$memory,$errors,$warnings" >> "$detailed_csv"

                # Update summary totals
                total_events_all=$((total_events_all + total_events))
                [[ $agg_prove_ms -gt $prove_ms_max ]] && prove_ms_max=$agg_prove_ms
                [[ $agg_verify_ms -gt $verify_ms_max ]] && verify_ms_max=$agg_verify_ms
                proof_bytes_sum=$((proof_bytes_sum + agg_proof_bytes))
                journal_bytes_sum=$((journal_bytes_sum + agg_journal_bytes))
                epochs_proved_sum=$((epochs_proved_sum + epochs))
                batches_processed_sum=$((batches_processed_sum + batches))
                memory_mb_total=$((memory_mb_total + memory))
            else
                echo "$machine,$global_agg_id,0,0,0,0,0,0,0,0,0,0" >> "$detailed_csv"
            fi

            global_agg_id=$((global_agg_id + 1))
        done

        machine_idx=$((machine_idx + 1))
    done

    # Calculate derived metrics
    local total_events_bytes=$((total_events_all * 20))  # 20 bytes per event (timestamp=8 + key_id=8 + value=4)
    local total_events_kb=$(echo "scale=3; $total_events_bytes / 1024" | bc -l 2>/dev/null || echo "0")
    local proof_kb_sum=$(echo "scale=3; $proof_bytes_sum / 1024" | bc -l 2>/dev/null || echo "0")
    local journal_kb_sum=$(echo "scale=3; $journal_bytes_sum / 1024" | bc -l 2>/dev/null || echo "0")

    # Calculate avg and p99 proof times
    local avg_proof_ms=0
    local p99_proof_ms=$prove_ms_max
    if [[ $epochs_proved_sum -gt 0 ]]; then
        avg_proof_ms=$((prove_ms_max / epochs_proved_sum))
    fi

    # Get production and consumption times from log if available
    local produce_ms=0
    local consume_ms=0
    local total_ms=0
    if [[ -f "$LOG_DIR/produce_duration.txt" ]]; then
        produce_ms=$(cat "$LOG_DIR/produce_duration.txt")
        produce_ms=$((produce_ms * 1000))  # Convert seconds to ms
    fi

    # Calculate throughput
    local events_per_sec=0
    local mb_per_sec=0
    if [[ $total_ms -gt 0 ]]; then
        events_per_sec=$((total_events_all * 1000 / total_ms))
        mb_per_sec=$(echo "scale=2; $total_events_bytes / 1048576 * 1000 / $total_ms" | bc -l 2>/dev/null || echo "0")
    fi

    # Write summary row to CSV
    echo "${timestamp},${EPOCH_TYPE},${NUM_AGGREGATORS},${total_events_all},${total_events_bytes},${total_events_kb},${produce_ms},${consume_ms},${prove_ms_max},${verify_ms_max},${total_ms},${events_per_sec},${mb_per_sec},${avg_proof_ms},${p99_proof_ms},${proof_bytes_sum},${proof_kb_sum},${journal_bytes_sum},${journal_kb_sum},${epochs_proved_sum},${batches_processed_sum},${memory_mb_total}" >> "$metrics_csv"

    log_info "  Per-aggregator metrics:"
    log_info "    Total events processed: $total_events_all"
    log_info "    Epochs proved (sum): $epochs_proved_sum"
    log_info "    Batches processed (sum): $batches_processed_sum"
    log_info "    Prove time (max): ${prove_ms_max}ms"
    log_info "    Verify time (max): ${verify_ms_max}ms"
    log_info "    Proof bytes (sum): ${proof_kb_sum} KB"
    log_info "    Journal bytes (sum): ${journal_kb_sum} KB"
    log_info "    Memory (total): ${memory_mb_total} MB"

    log_success "  ✓ Detailed metrics saved to: $detailed_csv"
    log_success "  ✓ Summary metrics saved to: $metrics_csv"
    # Return both paths (space-separated)
    echo "$metrics_csv $detailed_csv"
}

collect_metrics() {
    log_step "Generating comprehensive evaluation report..."

    # Collect all logs first
    collect_all_logs

    # Extract detailed metrics (returns "metrics_csv detailed_csv")
    local metrics_paths=$(extract_aggregator_metrics)
    local metrics_csv=$(echo "$metrics_paths" | awk '{print $1}')
    local detailed_csv=$(echo "$metrics_paths" | awk '{print $2}')

    # Evaluate querier performance
    local query_metrics_csv=$(evaluate_querier)

    mkdir -p "$RESULTS_DIR"
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local report_file="$RESULTS_DIR/e2e_report_${timestamp}.txt"

    {
        echo "============================================"
        echo "  Distributed E2E Evaluation Report"
        echo "============================================"
        echo ""
        echo "Timestamp: $(date)"
        echo "Configuration:"
        echo "  Data source machine: $DATA_SOURCE_MACHINE"
        echo "  Aggregator machines: $AGGREGATOR_MACHINES"
        echo "  Querier machine: $QUERIER_MACHINE"
        echo "  Total aggregators: $NUM_AGGREGATORS"
        echo "  Events: $EVENTS"
        echo "  Batch size: $BATCH_SIZE"
        echo "  Epoch type: $EPOCH_TYPE"
        echo "  Dataset type: $DATASET_TYPE"
        case "$DATASET_TYPE" in
            google_cluster|google|tsv)
                echo "  Dataset path: $GOOGLE_CLUSTER_DIR"
                ;;
            caida)
                echo "  Dataset path: $CAIDA_DIR"
                ;;
        esac
        echo ""

        if [[ -f "$LOG_DIR/produce_duration.txt" ]]; then
            local duration=$(cat "$LOG_DIR/produce_duration.txt")
            local throughput=$((EVENTS / duration))
            echo "Production Performance:"
            echo "  Duration: ${duration}s"
            echo "  Throughput: $throughput events/s"
            echo "  MB/s: $((throughput * 100 / 1024 / 1024))"
            echo ""
        fi

        echo "Aggregator Metrics Summary (matching bench_distributed_aggregators.sh format):"
        echo "==========================================================================="
        column -t -s',' "$metrics_csv" 2>/dev/null || cat "$metrics_csv"
        echo ""

        # Parse the detailed CSV format if available
        if [[ -f "$detailed_csv" ]]; then
            echo "Per-Aggregator Detailed Metrics:"
            echo "================================="
            column -t -s',' "$detailed_csv" 2>/dev/null || cat "$detailed_csv"
            echo ""
        fi

        echo "Total Aggregator Metrics Summary:"
        echo "================================="
        # Parse the new summary CSV format
        # Header: timestamp,epoch_type,num_aggregators,total_events,total_events_bytes,total_events_kb,produce_ms,consume_ms,prove_ms_max,verify_ms_max,total_ms,events_per_sec,mb_per_sec,avg_proof_ms,p99_proof_ms,proof_bytes_sum,proof_kb_sum,journal_bytes_sum,journal_kb_sum,epochs_proved,batches_processed,memory_mb_total
        awk -F',' 'NR>1 {
            printf "  Total events processed: %s\n", $4
            printf "  Total events size: %s KB (%s bytes)\n", $6, $5
            printf "  Production time: %s ms\n", $7
            printf "  Consumption time: %s ms\n", $8
            printf "  Prove time (max across aggregators): %s ms\n", $9
            printf "  Verify time (max across aggregators): %s ms\n", $10
            printf "  Avg proof time per epoch: %s ms\n", $14
            printf "  P99 proof time: %s ms\n", $15
            printf "  Proof bytes (sum): %s KB (%s bytes)\n", $17, $16
            printf "  Journal bytes (sum): %s KB (%s bytes)\n", $19, $18
            printf "  Epochs proved (sum): %s\n", $20
            printf "  Batches processed (sum): %s\n", $21
            printf "  Memory usage (total): %s MB\n", $22
            printf "  Throughput: %s events/sec\n", $12
            printf "  Throughput: %s MB/sec\n", $13
        }' "$metrics_csv"
        echo ""

        echo "Querier Performance Evaluation:"
        echo "==============================="
        if [[ -f "$query_metrics_csv" ]]; then
            echo "Query types tested: $QUERY_TYPES"
            echo "Iterations per query: $NUM_QUERY_ITERATIONS"
            if [[ -n "$KEY_PREFIX" ]]; then
                echo "Key prefix filter: $KEY_PREFIX"
            fi
            echo ""

            # Overall statistics with new CSV format
            # Header: timestamp,query_type,iteration,latency_ms,success,prove_ms,verify_ms,proof_bytes,journal_bytes,fdb_fetch_ms,merge_ms,epochs_queried,error_message
            awk -F',' '
            NR>1 {
                total++
                if($5=="true") {
                    success++
                    sum_latency+=$4
                    sum_prove+=$6
                    sum_verify+=$7
                    sum_proof_bytes+=$8
                    sum_journal_bytes+=$9
                    sum_fdb_fetch+=$10
                    sum_merge+=$11
                    if(min_latency=="" || $4<min_latency) min_latency=$4
                    if($4>max_latency) max_latency=$4
                }
            }
            END {
                printf "  Overall Query Statistics:\n"
                printf "    Total queries: %d\n", total
                printf "    Successful: %d (%.1f%%)\n", success, (total>0 ? success*100.0/total : 0)
                printf "    Failed: %d\n", total-success
                printf "\n"
                printf "  Latency Metrics:\n"
                printf "    Avg latency: %d ms\n", (success>0 ? int(sum_latency/success) : 0)
                printf "    Min latency: %d ms\n", (min_latency=="" ? 0 : min_latency)
                printf "    Max latency: %d ms\n", max_latency+0
                printf "\n"
                printf "  Query Proof Metrics:\n"
                printf "    Avg prove time: %d ms\n", (success>0 ? int(sum_prove/success) : 0)
                printf "    Avg verify time: %d ms\n", (success>0 ? int(sum_verify/success) : 0)
                printf "    Total proof bytes: %d\n", sum_proof_bytes
                printf "    Total journal bytes: %d\n", sum_journal_bytes
                printf "\n"
                printf "  FDB Fetch Metrics:\n"
                printf "    Avg FDB fetch time: %d ms\n", (success>0 ? int(sum_fdb_fetch/success) : 0)
                printf "    Avg merge time: %d ms\n", (success>0 ? int(sum_merge/success) : 0)
                printf "\n"
            }
            ' "$query_metrics_csv"

            # Per-query-type statistics with new CSV format
            echo "  Per Query Type Detailed Metrics:"
            awk -F',' '
            NR>1 {
                qt=$2
                total[qt]++
                if($5=="true") {
                    success[qt]++
                    sum_latency[qt]+=$4
                    sum_prove[qt]+=$6
                    sum_verify[qt]+=$7
                    sum_proof_bytes[qt]+=$8
                    sum_journal_bytes[qt]+=$9
                    sum_fdb_fetch[qt]+=$10
                    if(min_latency[qt]=="" || $4<min_latency[qt]) min_latency[qt]=$4
                    if($4>max_latency[qt]) max_latency[qt]=$4
                }
            }
            END {
                for(qt in total) {
                    printf "    %s:\n", qt
                    printf "      Success rate: %.1f%% (%d/%d)\n",
                           (total[qt]>0 ? success[qt]*100.0/total[qt] : 0),
                           success[qt]+0, total[qt]
                    printf "      Latency: avg=%d ms, min=%d ms, max=%d ms\n",
                           (success[qt]>0 ? int(sum_latency[qt]/success[qt]) : 0),
                           (min_latency[qt]=="" ? 0 : min_latency[qt]), max_latency[qt]+0
                    printf "      Proof: avg_prove=%d ms, avg_verify=%d ms\n",
                           (success[qt]>0 ? int(sum_prove[qt]/success[qt]) : 0),
                           (success[qt]>0 ? int(sum_verify[qt]/success[qt]) : 0)
                    printf "      Proof bytes: %d, Journal bytes: %d\n",
                           sum_proof_bytes[qt]+0, sum_journal_bytes[qt]+0
                    printf "      FDB fetch: avg=%d ms\n",
                           (success[qt]>0 ? int(sum_fdb_fetch[qt]/success[qt]) : 0)
                }
            }
            ' "$query_metrics_csv"
        else
            echo "  No query metrics available"
        fi
        echo ""

        echo "Kafka Consumer Group Status:"
        if command -v docker &>/dev/null && docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
            docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
                --describe --group "$KAFKA_GROUP_ID" 2>/dev/null || echo "  Not available"
        else
            echo "  Docker not available"
        fi
        echo ""

        echo "Files Generated:"
        echo "================"
        echo "  Report: $report_file"
        echo "  Aggregator summary metrics (bench_distributed_aggregators.sh format): $metrics_csv"
        echo "  Aggregator detailed metrics: $detailed_csv"
        echo "  Query metrics: $query_metrics_csv"
        echo "  Collected logs: $LOG_DIR/collected_e2e/"
        echo ""
        echo "CSV Headers Reference:"
        echo "======================"
        echo "  Aggregator summary: $AGGREGATOR_CSV_HEADER"
        echo "  Query metrics: $QUERY_CSV_HEADER"
        echo ""

    } | tee "$report_file"

    log_success "  ✓ Complete report saved to: $report_file"
}

stop_all_components() {
    log_step "Stopping all components (separated architecture)..."

    # Stop all processes on aggregator machines (kafka-consumer, aggregator, r0vm)
    log_info "  Stopping kafka-consumers, ZK aggregators, and r0vm..."
    local stop_pids=()
    for machine in $AGGREGATOR_MACHINES; do
        ssh -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 "${SSH_USER}@${machine}" "
            # Stop r0vm first (child process of aggregator)
            pkill -9 -f r0vm 2>/dev/null || true
            # Stop ZK aggregators
            pkill -9 -f aggregator 2>/dev/null || true
            # Stop kafka-consumers
            pkill -9 -f kafka-consumer 2>/dev/null || true
            # Clean up state directories
            rm -rf /tmp/zktelemetry_consumer_* 2>/dev/null || true
            rm -rf /tmp/zktelemetry_zkagg_* 2>/dev/null || true
            rm -rf /tmp/zktelemetry_agg_* 2>/dev/null || true
        " &
        stop_pids+=($!)
    done
    for pid in "${stop_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    # Stop querier on querier machine
    log_info "  Stopping querier on $QUERIER_MACHINE..."
    ssh -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}" "
        if [[ -f /tmp/zktelemetry_querier.pid ]]; then
            kill \$(cat /tmp/zktelemetry_querier.pid) 2>/dev/null || true
            rm -f /tmp/zktelemetry_querier.pid
        fi
        pkill -f 'querier' 2>/dev/null || true
        pkill -f 'querier/server' 2>/dev/null || true
        rm -f /tmp/querier.log
    " &
    stop_pids+=($!)

    # Stop data source on data source machine (in case it's still running)
    log_info "  Stopping data source on $DATA_SOURCE_MACHINE..."
    ssh -o ConnectTimeout=10 -o ServerAliveInterval=5 -o ServerAliveCountMax=2 "${SSH_USER}@${DATA_SOURCE_MACHINE}" "
        pkill -f 'kafka-producer' 2>/dev/null || true
        rm -f /tmp/zktelemetry_datasource.log
    " &
    stop_pids+=($!)

    for pid in "${stop_pids[@]}"; do
        wait "$pid" 2>/dev/null || true
    done

    log_success "  ✓ All components stopped"
}

show_status() {
    echo ""
    echo "============================================"
    echo "  Distributed E2E Status (Separated Architecture)"
    echo "============================================"
    echo ""

    # Data source
    echo "Data Source ($DATA_SOURCE_MACHINE):"
    local ds_status=$(ssh "${SSH_USER}@${DATA_SOURCE_MACHINE}" "pgrep -c -f 'kafka-producer' 2>/dev/null || echo 0")
    if [[ $ds_status -gt 0 ]]; then
        echo "  Status: RUNNING ($ds_status processes)"
    else
        echo "  Status: NOT RUNNING"
    fi
    echo ""

    # Kafka Consumers (separated architecture)
    echo "Kafka Consumers (consume -> RocksDB):"
    for machine in $AGGREGATOR_MACHINES; do
        echo "  $machine:"
        local consumer_count=$(ssh "${SSH_USER}@${machine}" "pgrep -c -f 'kafka-consumer' 2>/dev/null || echo 0")
        echo "    kafka-consumer: $consumer_count processes"
    done
    echo ""

    # ZK Aggregators (separated architecture)
    echo "ZK Aggregators (RocksDB -> proofs -> FDB):"
    for machine in $AGGREGATOR_MACHINES; do
        echo "  $machine:"
        local agg_count=$(ssh "${SSH_USER}@${machine}" "pgrep -c -f 'aggregator' 2>/dev/null || echo 0")
        local r0vm_count=$(ssh "${SSH_USER}@${machine}" "pgrep -c -f 'r0vm' 2>/dev/null || echo 0")
        echo "    aggregator: $agg_count processes"
        echo "    r0vm (proof generation): $r0vm_count processes"
    done
    echo ""

    # Querier
    echo "Querier ($QUERIER_MACHINE):"
    local querier_pid=$(ssh "${SSH_USER}@${QUERIER_MACHINE}" "cat /tmp/zktelemetry_querier.pid 2>/dev/null" || echo "")
    if [[ -n "$querier_pid" ]]; then
        local is_running=$(ssh "${SSH_USER}@${QUERIER_MACHINE}" "kill -0 $querier_pid 2>/dev/null && echo '1' || echo '0'")
        if [[ "$is_running" == "1" ]]; then
            echo "  Status: RUNNING (PID: $querier_pid)"
            echo "  Port: $QUERIER_PORT"
            curl -sS "http://${QUERIER_MACHINE}:$QUERIER_PORT/health" 2>/dev/null && echo "" || echo "  Health check: FAILED"
        else
            echo "  Status: NOT RUNNING"
        fi
    else
        echo "  Status: NOT RUNNING"
    fi
    echo ""

    # Kafka Consumer Group
    echo "Kafka Consumer Group ($KAFKA_GROUP_ID):"
    if command -v docker &>/dev/null && docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
        docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
            --describe --group "$KAFKA_GROUP_ID" 2>/dev/null || echo "  Not active"
    else
        echo "  Docker not available"
    fi
    echo ""
}

clean_all() {
    log_step "Cleaning up logs and temporary files (separated architecture)..."

    # Clean local
    rm -rf "$LOG_DIR"
    log_info "  ✓ Local logs cleaned"

    # Clean aggregator machines (all state dirs for separated architecture)
    for machine in $AGGREGATOR_MACHINES; do
        ssh "${SSH_USER}@${machine}" \
            "rm -rf /tmp/zktelemetry_agg_* /tmp/zktelemetry_consumer_* /tmp/zktelemetry_zkagg_* 2>/dev/null" &
    done

    # Clean querier machine
    ssh "${SSH_USER}@${QUERIER_MACHINE}" "rm -f /tmp/zktelemetry_querier.* 2>/dev/null" &

    # Clean data source machine
    ssh "${SSH_USER}@${DATA_SOURCE_MACHINE}" "rm -f /tmp/zktelemetry_datasource.log 2>/dev/null" &

    wait
    log_info "  ✓ Remote files cleaned"

    log_success "  ✓ Cleanup complete"
}

start_e2e() {
    echo ""
    echo "============================================"
    echo "  Starting Distributed E2E Test"
    echo "  (Separated Consumer/Aggregator Architecture)"
    echo "============================================"
    echo ""

    validate_config
    check_connectivity
    check_fdb
    check_dataset
    force_cleanup_all_machines
    build_binaries
    sync_code_to_machines
    sync_binaries_to_machines
    setup_fdb_cluster
    reset_fdb_subspace
    setup_kafka_topic

    # =================================================================
    # Phase 1: Start Kafka Consumers (consume from Kafka, write to RocksDB)
    # =================================================================
    log_step "Phase 1: Starting Kafka consumers..."
    start_kafka_consumers

    # Note: start_log_sync is called inside start_kafka_consumers for earlier visibility

    wait_for_consumers_ready 60 || log_warn "Some consumers may not be ready"

    # Allow consumer group to stabilize (partition assignment can take a few seconds)
    log_info "Waiting for consumer group to stabilize..."
    sleep 5

    # =================================================================
    # Phase 2: Produce Events (kafka-producer sends to Kafka) - BACKGROUND
    # =================================================================
    log_step "Phase 2: Producing events to Kafka (background)..."
    start_remote_data_source &
    DATA_SOURCE_PID=$!
    log_info "  Data source started in background (PID: $DATA_SOURCE_PID)"

    # =================================================================
    # Phase 3: Start ZK Aggregators (read RocksDB, generate proofs, write to FDB)
    # =================================================================
    log_step "Phase 3: Starting ZK aggregators..."
    start_zk_aggregators

    # =================================================================
    # Phase 4: Wait for Consumption (Kafka lag = 0)
    # =================================================================
    log_step "Phase 4: Waiting for consumption..."
    wait_for_consumption "$EVENTS" 600 || log_warn "Consumption may be incomplete"
    log_success "  ✓ Consumption phase complete"

    # Wait for data source to complete before signaling flush
    log_info "Waiting for data source to complete..."
    wait $DATA_SOURCE_PID || log_warn "Data source process exited with non-zero status"
    log_success "  ✓ Data source finished"

    # Signal kafka-consumers to flush all pending batches as final epochs
    signal_flush_epochs
    log_success "============================================"
    log_success "  Production complete!"
    log_success "  Now waiting for ZK aggregators to prove epochs..."
    log_success "============================================"

    # =================================================================
    # Phase 5: Wait for Minimum Epochs, then Start Querier
    # =================================================================
    log_step "Phase 5: Waiting for at least $MIN_EPOCHS_FOR_QUERIER epoch(s) to be proved..."
    if wait_for_min_epochs_in_fdb "$MIN_EPOCHS_FOR_QUERIER" "$EPOCH_WAIT_TIMEOUT_SEC"; then
        start_remote_querier
        # Copy querier log locally
        scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "${LOG_DIR}/querier.log" 2>/dev/null || true

        # Run initial query evaluation with all QUERY_TYPES
        log_step "Phase 5b: Running initial query evaluation..."
        evaluate_querier
        # Copy updated querier log
        scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "${LOG_DIR}/querier.log" 2>/dev/null || true
    else
        log_warn "Could not wait for epochs - starting querier anyway"
        start_remote_querier
        scp -q -o ConnectTimeout=5 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 "${SSH_USER}@${QUERIER_MACHINE}:/tmp/querier.log" "${LOG_DIR}/querier.log" 2>/dev/null || true
    fi

    # =================================================================
    # Phase 6: Wait for ZK Aggregation to Complete
    # =================================================================
    log_step "Phase 6: Waiting for ZK aggregation to complete..."
    if wait_for_zk_completion "$ZK_AGGREGATION_TIMEOUT"; then
        log_success "  ✓ ZK aggregation completed successfully"
    else
        log_warn "ZK aggregation may be incomplete"
    fi

    # Stop background log sync before validation/collection
    stop_log_sync

    # Check if we should skip expensive collection (for faster exit)
    if [[ "${SKIP_FINAL_COLLECTION:-0}" == "1" ]]; then
        log_info "Skipping final validation and metrics collection (SKIP_FINAL_COLLECTION=1)"
        echo ""
        log_success "============================================"
        log_success "  Distributed E2E Test Complete (Fast Exit)"
        log_success "============================================"
        return 0
    fi

    # Final validation after all proofs are generated
    log_step "Phase 7: Final validation..."
    validate_data || log_warn "Validation failed but continuing..."

    # =================================================================
    # Phase 8: Collect Metrics and Generate Report
    # =================================================================
    log_step "Phase 8: Collecting metrics..."
    collect_metrics || log_warn "Metrics collection failed but test completed"

    echo ""
    log_success "============================================"
    log_success "  Distributed E2E Test Complete"
    log_success "============================================"
    echo ""
    echo "Architecture: Separated Consumer/Aggregator"
    echo "  1. kafka-consumer: Kafka -> RocksDB"
    echo "  2. aggregator --rocksdb: RocksDB -> FDB (proofs)"
    echo "  3. querier: FDB -> Query results"
    echo ""
    echo "Commands:"
    echo "  Status:   $0 status"
    echo "  Validate: $0 validate"
    echo "  Evaluate: $0 evaluate"
    echo "  Report:   $0 report"
    echo "  Stop:     $0 stop"
    echo ""
}

# Main
case "${1:-start}" in
    start)
        setup_logging
        # Auto-cleanup before starting to ensure clean state
        log_step "Auto-cleanup: Stopping existing processes and clearing data..."
        stop_all_components
        log_success "  ✓ Cleanup complete"
        echo ""
        start_e2e
        ;;
    stop)
        stop_all_components
        ;;
    status)
        show_status
        ;;
    validate)
        setup_logging
        validate_data
        ;;
    evaluate)
        setup_logging
        evaluate_querier || exit 1
        ;;
    report)
        setup_logging
        collect_metrics
        ;;
    clean)
        stop_all_components
        clean_all
        ;;
    *)
        usage
        ;;
esac
