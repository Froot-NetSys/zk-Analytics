#!/usr/bin/env bash
set -euo pipefail

# Sweep samples and raw queries
# By default, runs only:
#   - samples/sum
#   - raw/max_key
# To enable other queries, set corresponding SKIP flags to 0
export SKIP_HISTOGRAM=1
export SKIP_CM=1
export SKIP_RAW="${SKIP_RAW:-0}"  # Enable raw queries (set to 1 to disable)

# Skip unwanted samples queries (enable only samples/sum by default)
export SKIP_SAMPLES_SUM="${SKIP_SAMPLES_SUM:-0}"         # Run samples/sum
export SKIP_SAMPLES_SUM_KEY="${SKIP_SAMPLES_SUM_KEY:-1}" # Skip samples/sum_key
export SKIP_SAMPLES_SUM_TOPK="${SKIP_SAMPLES_SUM_TOPK:-1}" # Skip samples/sum_topk

export OUTPUT_CSV="${OUTPUT_CSV:-./samples_sweep_results.csv}"

# Default parameter ranges for samples
export EPOCHS_LIST="${EPOCHS_LIST:-16}"

# Multi-source mode (default)
export NUM_SOURCES_LIST="${NUM_SOURCES_LIST:-128}"
export KEYS_PER_SOURCE_LIST="${KEYS_PER_SOURCE_LIST:-8}"
export SOURCES_PER_EPOCH_LIST="${SOURCES_PER_EPOCH_LIST:-8}"

# Multi-aggregator mode: distribute sources across aggregators for better key coverage
export NUM_AGGREGATORS_LIST="${NUM_AGGREGATORS_LIST:-16}"

# Legacy mode: uncomment to use --keys parameter instead
# export NUM_SOURCES_LIST=""
# export KEYS_LIST="${KEYS_LIST:-128 256 512 1024}"

# Samples per key per epoch
export SAMPLES_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST:-${EVENTS_PER_KEY_LIST:-128}}"
export EVENTS_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sweep_bench_queries.sh"
