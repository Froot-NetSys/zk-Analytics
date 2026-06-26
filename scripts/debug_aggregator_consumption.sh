#!/usr/bin/env bash
set -euo pipefail

# Debug script for distributed aggregator consumption issues
# This helps identify why aggregators may not be consuming Kafka events

KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
KAFKA_TOPIC="${KAFKA_TOPIC:-bench_raw_events}"
KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-bench_aggregators}"
LOG_DIR="${LOG_DIR:-bench_logs/distributed}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

echo "======================================="
echo "  Aggregator Consumption Debugger"
echo "======================================="
echo ""

# 1. Check if Kafka is running
log_info "1. Checking Kafka status..."
if docker ps --format '{{.Names}}' | grep -q '^kafka$'; then
    echo "   ✓ Kafka container is running"
else
    log_error "   ✗ Kafka container is NOT running"
    exit 1
fi
echo ""

# 2. Check topic details
log_info "2. Checking topic: $KAFKA_TOPIC"
docker exec kafka kafka-topics --bootstrap-server localhost:9092 \
    --describe --topic "$KAFKA_TOPIC" 2>/dev/null || {
    log_error "   ✗ Topic does not exist or cannot be accessed"
    exit 1
}
echo ""

# 3. Check message count in topic
log_info "3. Checking message count in topic..."
MSG_COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic "$KAFKA_TOPIC" \
    --time -1 2>/dev/null | awk -F':' '{sum += $3} END {print sum}')
echo "   Total messages in topic: ${MSG_COUNT:-0}"
echo ""

# 4. Check consumer group status
log_info "4. Checking consumer group: $KAFKA_GROUP_ID"
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group "$KAFKA_GROUP_ID" --describe 2>/dev/null || {
    log_warn "   Consumer group not found or has no active members"
    echo ""
}
echo ""

# 5. Check consumer group lag
log_info "5. Calculating consumer lag..."
LAG_OUTPUT=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group "$KAFKA_GROUP_ID" --describe 2>/dev/null)

if [ -z "$LAG_OUTPUT" ]; then
    log_warn "   No consumer group information available"
else
    TOTAL_LAG=$(echo "$LAG_OUTPUT" | awk 'NR>1 {sum += $6} END {print sum+0}')
    ACTIVE_CONSUMERS=$(echo "$LAG_OUTPUT" | grep -v "TOPIC" | grep -v "^$" | wc -l)

    echo "   Total lag: $TOTAL_LAG messages"
    echo "   Active partition assignments: $ACTIVE_CONSUMERS"

    if [ "$TOTAL_LAG" -eq 0 ]; then
        echo "   ✓ All messages consumed!"
    else
        log_warn "   Unconsumed messages detected"
    fi
fi
echo ""

# 6. Check for aggregator processes
log_info "6. Checking for running aggregator processes..."
AGGR_PROCS=$(pgrep -f "aggregator" || true)
if [ -z "$AGGR_PROCS" ]; then
    log_warn "   No local aggregator processes found"
else
    echo "   Found aggregator PIDs: $AGGR_PROCS"
    for pid in $AGGR_PROCS; do
        echo "   - PID $pid: $(ps -p $pid -o cmd= 2>/dev/null | head -c 100)..."
    done
fi
echo ""

# 7. Check recent aggregator logs
log_info "7. Checking most recent aggregator logs..."
if [ -d "$LOG_DIR" ]; then
    LATEST_LOG=$(find "$LOG_DIR" -name "aggregator_*.log" -type f -printf '%T@ %p\n' 2>/dev/null | sort -rn | head -1 | cut -d' ' -f2-)

    if [ -n "$LATEST_LOG" ]; then
        echo "   Latest log: $LATEST_LOG"
        echo ""
        echo "   Last 15 lines:"
        echo "   ─────────────────────────────────────────"
        tail -15 "$LATEST_LOG" | sed 's/^/   /'
        echo "   ─────────────────────────────────────────"
        echo ""

        # Look for specific indicators
        if grep -q "Connected to Kafka" "$LATEST_LOG" 2>/dev/null; then
            echo "   ✓ Aggregator connected to Kafka"
        else
            log_warn "   No 'Connected to Kafka' message found"
        fi

        if grep -q "partition" "$LATEST_LOG" 2>/dev/null; then
            echo "   ✓ Partition assignment messages found"
        else
            log_warn "   No partition assignment messages found"
        fi

        if grep -qi "error\|fail\|panic" "$LATEST_LOG" 2>/dev/null; then
            log_error "   Errors detected in log!"
            echo "   Error lines:"
            grep -i "error\|fail\|panic" "$LATEST_LOG" | tail -5 | sed 's/^/   /'
        fi

        if grep -q "n_events=" "$LATEST_LOG" 2>/dev/null; then
            echo "   ✓ Event processing messages found"
            EVENTS_PROCESSED=$(grep -o "n_events=[0-9]*" "$LATEST_LOG" | tail -1)
            echo "   Last event count: $EVENTS_PROCESSED"
        else
            log_warn "   No event processing messages found"
        fi
    else
        log_warn "   No aggregator logs found in $LOG_DIR"
    fi
else
    log_warn "   Log directory $LOG_DIR does not exist"
fi
echo ""

# 8. Test Kafka connectivity
log_info "8. Testing Kafka consumer connectivity..."
timeout 5s docker exec kafka kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic "$KAFKA_TOPIC" \
    --group "test-debug-group" \
    --max-messages 1 \
    --timeout-ms 3000 2>/dev/null && {
    echo "   ✓ Can successfully consume from topic"
} || {
    log_warn "   Could not consume test message (may be no messages in topic)"
}
echo ""

# 9. Summary and recommendations
log_info "9. Summary and Recommendations"
echo "   ─────────────────────────────────────────"

if [ -z "$AGGR_PROCS" ]; then
    log_error "   ISSUE: No aggregator processes running"
    echo "   → Start aggregators before producing events"
elif [ "${TOTAL_LAG:-999}" -gt 0 ]; then
    log_warn "   ISSUE: Events in Kafka but not consumed"
    echo "   → Check aggregator logs for connection/partition issues"
    echo "   → Verify KAFKA_BROKERS, KAFKA_TOPIC, KAFKA_GROUP_ID env vars"
    echo "   → Check if aggregators have correct permissions/access"
elif [ "${MSG_COUNT:-0}" -eq 0 ]; then
    log_warn "   ISSUE: No messages in topic"
    echo "   → Verify producer is writing to correct topic"
else
    echo "   ✓ Everything looks good!"
fi

echo ""
echo "To watch live consumption, run:"
echo "  watch -n 1 'docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group $KAFKA_GROUP_ID --describe'"
echo ""
echo "To view aggregator logs in real-time:"
echo "  tail -f $LOG_DIR/aggregator_*.log"
