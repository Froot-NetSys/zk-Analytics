#!/usr/bin/env bash
# Default IP/port settings for zk-Analytics services.
# This file is sourced by benchmark scripts.

# =============================================================================
# BENCH_DISTRIBUTED_AGGREGATORS CONFIGURATION
# =============================================================================
# Aggregator machines - space-separated list of IPs
# First IP is used for Kafka broker and external access
# This is the SINGLE SOURCE OF TRUTH for bench_distributed_aggregators.sh.
# Override with environment variable or edit here for your cluster.
# For distributed setup, set to your actual machine IPs, e.g.:
#   export AGGREGATOR_MACHINES="10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4"
export AGGREGATOR_MACHINES="${AGGREGATOR_MACHINES:-10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8}"

# =============================================================================
# RUN_DISTRIBUTED_E2E CONFIGURATION
# =============================================================================
# Separate machine configuration for run_distributed_e2e.sh
# This allows running E2E tests on different machines than the aggregator benchmark.
#
# E2E Data source machine - where Kafka producer runs
export E2E_DATA_SOURCE_MACHINE="${E2E_DATA_SOURCE_MACHINE:-10.10.1.5}"

# E2E Aggregator machines - space-separated list of IPs for aggregators
export E2E_AGGREGATOR_MACHINES="${E2E_AGGREGATOR_MACHINES:-10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4}"

# E2E Querier machine - where querier server runs
export E2E_QUERIER_MACHINE="${E2E_QUERIER_MACHINE:-10.10.1.6}"

# E2E Kafka brokers (defaults to data source machine if not set)
export E2E_KAFKA_BROKERS="${E2E_KAFKA_BROKERS:-${E2E_DATA_SOURCE_MACHINE}:9092}"

# Auto-detect first aggregator IP
_get_first_aggregator_ip() {
    local first_machine
    read -ra _machines <<< "$AGGREGATOR_MACHINES"
    first_machine="${_machines[0]}"
    echo "$first_machine"
}

# Kafka broker - defaults to first aggregator IP
_detect_kafka_brokers() {
    local first_machine
    first_machine="$(_get_first_aggregator_ip)"
    if [[ "$first_machine" != "localhost" && "$first_machine" != "127.0.0.1" ]]; then
        echo "${first_machine}:9092"
    else
        echo "localhost:9092"
    fi
}
export KAFKA_BROKERS="${KAFKA_BROKERS:-$(_detect_kafka_brokers)}"

# Kafka external IP for remote consumers
_detect_kafka_external_ip() {
    local first_machine
    first_machine="$(_get_first_aggregator_ip)"
    if [[ "$first_machine" != "localhost" && "$first_machine" != "127.0.0.1" ]]; then
        echo "$first_machine"
    else
        echo ""
    fi
}
export KAFKA_EXTERNAL_IP="${KAFKA_EXTERNAL_IP:-$(_detect_kafka_external_ip)}"

# Kafka topic and consumer group
export KAFKA_TOPIC="${KAFKA_TOPIC:-raw_events}"
export KAFKA_GROUP_ID="${KAFKA_GROUP_ID:-aggregators}"

# Legacy HTTP settings (for backward compatibility)
export HTTP_LISTEN="${HTTP_LISTEN:-127.0.0.1:8080}"
export SINK_URL="${SINK_URL:-http://127.0.0.1:8080/ingest}"
