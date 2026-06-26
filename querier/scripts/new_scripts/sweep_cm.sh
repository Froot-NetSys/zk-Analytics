#!/usr/bin/env bash
set -euo pipefail

# Sweep CM sketch queries only
export SKIP_HISTOGRAM=1
export SKIP_SAMPLES=1
export OUTPUT_CSV="${OUTPUT_CSV:-./cm_sweep_results.csv}"

# Default parameter ranges for CM (typically larger keys, fewer events per key)
export EPOCHS_LIST="${EPOCHS_LIST:-32}"

# Multi-source mode (default)
export NUM_SOURCES_LIST="${NUM_SOURCES_LIST:-128}"
export KEYS_PER_SOURCE_LIST="${KEYS_PER_SOURCE_LIST:-64}"
export SOURCES_PER_EPOCH_LIST="${SOURCES_PER_EPOCH_LIST:-32}"

# Multi-aggregator mode: distribute sources across aggregators for better key coverage
export NUM_AGGREGATORS_LIST="${NUM_AGGREGATORS_LIST:-1}"

# Legacy mode: uncomment to use --keys parameter instead
# export NUM_SOURCES_LIST=""
# export KEYS_LIST="${KEYS_LIST:-512 1024 2048}"

# Samples per key per epoch
export SAMPLES_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST:-${EVENTS_PER_KEY_LIST:-16}}"
export EVENTS_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sweep_bench_queries.sh"
