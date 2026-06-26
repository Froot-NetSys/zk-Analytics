#!/usr/bin/env bash
set -euo pipefail

# Sweep histogram p90 query only
export SKIP_CM=1
export SKIP_SAMPLES=1
export SKIP_HISTOGRAM_BUCKET=1
export SKIP_HISTOGRAM_ALL=1
export OUTPUT_CSV="${OUTPUT_CSV:-./histogram_sweep_results.csv}"

# Default parameter ranges for histogram
export EPOCHS_LIST="${EPOCHS_LIST:-1 2 4 8 16 32 64 128 256}"

# Multi-source mode (default)
export NUM_SOURCES_LIST="${NUM_SOURCES_LIST:-128}"
export KEYS_PER_SOURCE_LIST="${KEYS_PER_SOURCE_LIST:-8}"
export SOURCES_PER_EPOCH_LIST="${SOURCES_PER_EPOCH_LIST:-8}"

# Multi-aggregator mode: distribute sources across aggregators for better key coverage
# With 128 sources and 16 aggregators, each aggregator gets 8 sources
# Querying N epochs will interleave epochs from each aggregator
export NUM_AGGREGATORS_LIST="${NUM_AGGREGATORS_LIST:-16}"

# Legacy mode: uncomment to use --keys parameter instead
# export NUM_SOURCES_LIST=""
# export KEYS_LIST="${KEYS_LIST:-128 256 512 1024}"

# Samples per key per epoch (how many data points each key contributes in one epoch)
export SAMPLES_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST:-${EVENTS_PER_KEY_LIST:-128}}"
# For backward compatibility, also set EVENTS_PER_KEY_LIST
export EVENTS_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sweep_bench_queries.sh"
