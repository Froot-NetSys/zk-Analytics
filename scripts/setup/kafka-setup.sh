#!/bin/bash
#
# Kafka topic setup script for zk-Analytics
#
# Usage:
#   ./kafka-setup.sh                    # Use defaults
#   ./kafka-setup.sh --bootstrap-server kafka:9092
#   ./kafka-setup.sh --partitions 32 --replication-factor 3
#
# Environment Variables:
#   KAFKA_BOOTSTRAP_SERVER: Kafka broker address (default: localhost:9092)
#   KAFKA_TOPIC: Topic name (default: raw_events)
#   KAFKA_PARTITIONS: Number of partitions (default: 16)
#   KAFKA_REPLICATION_FACTOR: Replication factor (default: 1)
#   KAFKA_RETENTION_MS: Message retention in ms (default: 604800000 = 7 days)
#   KAFKA_RETENTION_BYTES: Max bytes per partition (default: -1 = unlimited)
#

set -e

# Defaults
BOOTSTRAP_SERVER="${KAFKA_BOOTSTRAP_SERVER:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-raw_events}"
PARTITIONS="${KAFKA_PARTITIONS:-16}"
REPLICATION_FACTOR="${KAFKA_REPLICATION_FACTOR:-1}"
RETENTION_MS="${KAFKA_RETENTION_MS:-604800000}"  # 7 days
RETENTION_BYTES="${KAFKA_RETENTION_BYTES:--1}"   # unlimited

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --bootstrap-server)
            BOOTSTRAP_SERVER="$2"
            shift 2
            ;;
        --topic)
            TOPIC="$2"
            shift 2
            ;;
        --partitions)
            PARTITIONS="$2"
            shift 2
            ;;
        --replication-factor)
            REPLICATION_FACTOR="$2"
            shift 2
            ;;
        --retention-ms)
            RETENTION_MS="$2"
            shift 2
            ;;
        --retention-bytes)
            RETENTION_BYTES="$2"
            shift 2
            ;;
        --delete)
            DELETE_TOPIC=1
            shift
            ;;
        --describe)
            DESCRIBE_ONLY=1
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --bootstrap-server HOST:PORT   Kafka broker (default: localhost:9092)"
            echo "  --topic NAME                   Topic name (default: raw_events)"
            echo "  --partitions N                 Number of partitions (default: 16)"
            echo "  --replication-factor N         Replication factor (default: 1)"
            echo "  --retention-ms MS              Message retention in ms (default: 604800000)"
            echo "  --retention-bytes BYTES        Max bytes per partition (default: -1)"
            echo "  --delete                       Delete the topic instead of creating"
            echo "  --describe                     Describe the topic only"
            echo "  --help                         Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

echo "=== zk-Analytics Kafka Setup ==="
echo "Bootstrap server: $BOOTSTRAP_SERVER"
echo "Topic: $TOPIC"
echo "Partitions: $PARTITIONS"
echo "Replication factor: $REPLICATION_FACTOR"
echo "Retention: ${RETENTION_MS}ms (~$((RETENTION_MS / 86400000)) days)"
echo ""

# Check if kafka-topics is available
if ! command -v kafka-topics.sh &> /dev/null && ! command -v kafka-topics &> /dev/null; then
    echo "ERROR: kafka-topics.sh or kafka-topics not found in PATH"
    echo "Please install Kafka CLI tools or use Docker:"
    echo ""
    echo "  docker run --rm -it confluentinc/cp-kafka:latest \\"
    echo "    kafka-topics --bootstrap-server $BOOTSTRAP_SERVER --list"
    exit 1
fi

# Use kafka-topics.sh or kafka-topics
KAFKA_TOPICS="kafka-topics.sh"
if ! command -v kafka-topics.sh &> /dev/null; then
    KAFKA_TOPICS="kafka-topics"
fi

# Describe only
if [[ -n "$DESCRIBE_ONLY" ]]; then
    echo "Describing topic: $TOPIC"
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe --topic "$TOPIC"
    exit 0
fi

# Delete topic
if [[ -n "$DELETE_TOPIC" ]]; then
    echo "Deleting topic: $TOPIC"
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
        --delete --topic "$TOPIC" || true
    echo "Topic deleted (or did not exist)"
    exit 0
fi

# Check if topic exists
if $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" --list 2>/dev/null | grep -q "^${TOPIC}$"; then
    echo "Topic '$TOPIC' already exists. Describing..."
    $KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
        --describe --topic "$TOPIC"
    echo ""
    echo "To recreate, run: $0 --delete && $0"
    exit 0
fi

# Create topic
echo "Creating topic: $TOPIC"
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --topic "$TOPIC" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR" \
    --config retention.ms="$RETENTION_MS" \
    --config retention.bytes="$RETENTION_BYTES" \
    --config cleanup.policy=delete \
    --config compression.type=lz4

echo ""
echo "Topic created successfully!"
echo ""

# Describe the created topic
$KAFKA_TOPICS --bootstrap-server "$BOOTSTRAP_SERVER" \
    --describe --topic "$TOPIC"

echo ""
echo "=== Producer Example ==="
echo "KAFKA_BROKERS=$BOOTSTRAP_SERVER KAFKA_TOPIC=$TOPIC \\"
echo "  cargo run --bin kafka-producer -- --events 10000 --batch-size 100"
echo ""
echo "=== Consumer Example ==="
echo "KAFKA_BROKERS=$BOOTSTRAP_SERVER KAFKA_TOPIC=$TOPIC \\"
echo "  RAW_DB_PATH=/tmp/raw_db \\"
echo "  cargo run --bin kafka-consumer"
