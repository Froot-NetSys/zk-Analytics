#!/usr/bin/env bash
set -euo pipefail

# Run local end-to-end test with Kafka + FDB using tmux.
#
# Usage:
#   ./scripts/eval/run_local_e2e.sh [start|stop|status]
#
# Environment variables:
#   KAFKA_BROKERS       (default: localhost:9092)
#   KAFKA_TOPIC         (default: raw_events)
#   FDB_CLUSTER_FILE    (default: /etc/foundationdb/fdb.cluster)
#   FDB_SUBSPACE        (default: zktelemetry)
#   SOURCE_ID           (default: 1)
#   EVENTS              (default: 100000)
#   BATCH_SIZE          (default: 100)
#   QUERIER_PORT        (default: 8082)
#   RAW_BUFFER_PATH     (default: /tmp/zktelemetry_raw_buffer)
#
# Architecture:
#   Data Source -> Kafka -> Aggregator -> FDB -> Querier
#                            (local RocksDB buffer)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

SESSION_NAME="zktelemetry-e2e"

# Configuration
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-raw_events}"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
FDB_SUBSPACE="${FDB_SUBSPACE:-zktelemetry}"
SOURCE_ID="${SOURCE_ID:-1}"
EVENTS="${EVENTS:-100000}"
BATCH_SIZE="${BATCH_SIZE:-100}"
QUERIER_PORT="${QUERIER_PORT:-8082}"
RAW_BUFFER_PATH="${RAW_BUFFER_PATH:-/tmp/zktelemetry_raw_buffer}"

usage() {
    echo "Usage: $0 [start|stop|status|attach]"
    echo ""
    echo "Commands:"
    echo "  start   - Start all components in tmux session"
    echo "  stop    - Stop the tmux session and all components"
    echo "  status  - Show status of components"
    echo "  attach  - Attach to the tmux session"
    echo ""
    echo "After starting, attach with: tmux attach -t $SESSION_NAME"
    exit 1
}

check_dependencies() {
    echo "Checking dependencies..."

    # Check tmux
    if ! command -v tmux &>/dev/null; then
        echo "ERROR: tmux is required. Install with: sudo apt install tmux"
        exit 1
    fi

    # Check docker
    if ! command -v docker &>/dev/null; then
        echo "ERROR: docker is required"
        exit 1
    fi

    # Check Kafka container
    if ! docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
        echo "WARNING: Kafka container not running. Starting docker-compose..."
        docker-compose -f "$ROOT_DIR/scripts/docker-compose-kafka.yml" up -d
        sleep 5
    fi

    # Check FDB container or local service
    if ! docker ps --format '{{.Names}}' | grep -q '^fdb$'; then
        if ! systemctl is-active --quiet foundationdb 2>/dev/null; then
            echo "WARNING: FoundationDB not running. Checking docker..."
            if docker ps -a --format '{{.Names}}' | grep -q '^fdb$'; then
                docker start fdb
                sleep 3
            else
                echo "ERROR: FoundationDB not available. Please start FDB."
                exit 1
            fi
        fi
    fi

    # Ensure Kafka topic exists
    echo "Ensuring Kafka topic exists..."
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
        --create --topic "$KAFKA_TOPIC" \
        --partitions 16 --replication-factor 1 \
        --config retention.ms=604800000 2>/dev/null || true

    # Create buffer directory
    mkdir -p "$RAW_BUFFER_PATH"

    echo "Dependencies OK"
}

start_session() {
    check_dependencies

    # Kill existing session if any
    tmux kill-session -t "$SESSION_NAME" 2>/dev/null || true

    # Clean RocksDB buffer for fresh start
    echo "Cleaning RocksDB buffer: $RAW_BUFFER_PATH"
    rm -rf "$RAW_BUFFER_PATH" 2>/dev/null || true

    # Reset FDB subspace for clean test
    echo "Resetting FDB subspace: $FDB_SUBSPACE"
    "$ROOT_DIR/scripts/setup/reset_fdb.sh" "$FDB_SUBSPACE" 2>/dev/null || {
        echo "Note: FDB reset failed (may not be running yet)"
    }

    echo "Starting tmux session: $SESSION_NAME"

    # Create new session with first window (Aggregator)
    tmux new-session -d -s "$SESSION_NAME" -n "aggregator"

    # Window 0: Aggregator (Kafka consumer + FDB writer)
    tmux send-keys -t "$SESSION_NAME:0" "cd $ROOT_DIR/aggregator/host && \
KAFKA_BROKERS=$KAFKA_BROKERS \
KAFKA_TOPIC=$KAFKA_TOPIC \
KAFKA_GROUP_ID=aggregators \
RAW_DB_PATH=$RAW_BUFFER_PATH \
FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE \
FDB_SUBSPACE=$FDB_SUBSPACE \
cargo run --release --features 'kafka fdb'" Enter

    # Window 1: Data Source (Kafka producer) - wait for aggregator to build
    tmux new-window -t "$SESSION_NAME" -n "datasource"
    tmux send-keys -t "$SESSION_NAME:1" "echo 'Waiting 30s for aggregator to build...' && sleep 30 && \
cd $ROOT_DIR/data_source && \
KAFKA_BROKERS=$KAFKA_BROKERS \
KAFKA_TOPIC=$KAFKA_TOPIC \
SOURCE_ID=$SOURCE_ID \
cargo run --bin kafka-producer --release -- --events $EVENTS --batch-size $BATCH_SIZE" Enter

    # Window 2: Querier (FDB reader)
    tmux new-window -t "$SESSION_NAME" -n "querier"
    tmux send-keys -t "$SESSION_NAME:2" "echo 'Waiting 60s for builds...' && sleep 60 && \
cd $ROOT_DIR/querier/server && \
FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE \
FDB_SUBSPACE=$FDB_SUBSPACE \
HTTP_LISTEN=0.0.0.0:$QUERIER_PORT \
cargo run --release --features fdb" Enter

    # Window 3: Monitor / Test queries
    tmux new-window -t "$SESSION_NAME" -n "monitor"
    tmux send-keys -t "$SESSION_NAME:3" "echo '=== zk-Analytics E2E Test ===' && \
echo '' && \
echo 'Components:' && \
echo '  [0] aggregator - Kafka consumer + FDB writer' && \
echo '  [1] datasource - Kafka producer (starts after 30s)' && \
echo '  [2] querier    - FDB reader on port $QUERIER_PORT (starts after 60s)' && \
echo '  [3] monitor    - This window for testing' && \
echo '' && \
echo 'Test queries (after querier starts):' && \
echo '  curl -sS localhost:$QUERIER_PORT/query -H \"content-type: application/json\" -d \"{\\\"type\\\":\\\"samples_sum\\\",\\\"window\\\":\\\"1h\\\"}\"' && \
echo '' && \
echo 'Kafka status:' && \
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group aggregators 2>/dev/null || echo 'Consumer group not yet active' && \
echo '' && \
echo 'Switch windows: Ctrl+b, then 0/1/2/3' && \
echo 'Detach: Ctrl+b, then d' && \
echo 'Stop all: ./scripts/eval/run_local_e2e.sh stop'" Enter

    echo ""
    echo "=== zk-Analytics E2E Started ==="
    echo ""
    echo "Session: $SESSION_NAME"
    echo "Windows:"
    echo "  [0] aggregator - Kafka consumer + FDB writer"
    echo "  [1] datasource - Kafka producer (${EVENTS} events)"
    echo "  [2] querier    - HTTP API on port $QUERIER_PORT"
    echo "  [3] monitor    - Test commands"
    echo ""
    echo "Attach with: tmux attach -t $SESSION_NAME"
    echo "Stop with:   $0 stop"
    echo ""
}

stop_session() {
    echo "Stopping tmux session: $SESSION_NAME"
    tmux kill-session -t "$SESSION_NAME" 2>/dev/null || echo "Session not running"

    # Optionally clean up buffer
    if [[ -d "$RAW_BUFFER_PATH" ]]; then
        echo "Cleaning up buffer: $RAW_BUFFER_PATH"
        rm -rf "$RAW_BUFFER_PATH"
    fi

    echo "Stopped"
}

show_status() {
    echo "=== zk-Analytics E2E Status ==="
    echo ""

    # Check tmux session
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        echo "Tmux session: RUNNING"
        tmux list-windows -t "$SESSION_NAME"
    else
        echo "Tmux session: NOT RUNNING"
    fi
    echo ""

    # Check Docker containers
    echo "Docker containers:"
    docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" | grep -E "^NAMES|kafka|zookeeper|fdb" || echo "  (none)"
    echo ""

    # Check Kafka topic
    echo "Kafka topic:"
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic "$KAFKA_TOPIC" 2>/dev/null || echo "  (not available)"
    echo ""

    # Check Querier
    echo "Querier health:"
    curl -sS "http://localhost:$QUERIER_PORT/health" 2>/dev/null || echo "  (not responding)"
    echo ""
}

attach_session() {
    if tmux has-session -t "$SESSION_NAME" 2>/dev/null; then
        tmux attach -t "$SESSION_NAME"
    else
        echo "Session not running. Start with: $0 start"
        exit 1
    fi
}

# Main
case "${1:-start}" in
    start)
        start_session
        ;;
    stop)
        stop_session
        ;;
    status)
        show_status
        ;;
    attach)
        attach_session
        ;;
    *)
        usage
        ;;
esac
