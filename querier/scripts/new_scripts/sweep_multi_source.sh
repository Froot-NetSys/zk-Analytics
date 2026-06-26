#!/usr/bin/env bash
set -euo pipefail

# Sweep with multi-source configuration
# This demonstrates the new multi-source epoch generation feature

export OUTPUT_CSV="${OUTPUT_CSV:-./multi_source_sweep_results.csv}"

# Multi-source parameter ranges
export NUM_SOURCES_LIST="${NUM_SOURCES_LIST:-2 4 8}"
export KEYS_PER_SOURCE_LIST="${KEYS_PER_SOURCE_LIST:-25 50 100}"
export SOURCES_PER_EPOCH_LIST="${SOURCES_PER_EPOCH_LIST:-1 2 4}"

# Other parameters
export EPOCHS_LIST="${EPOCHS_LIST:-1 2 4 8}"
export EVENTS_PER_KEY_LIST="${EVENTS_PER_KEY_LIST:-8 16 32}"

# Optional: skip specific query types
# export SKIP_HISTOGRAM=1
# export SKIP_CM=1
# export SKIP_SAMPLES=1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/sweep_bench_queries.sh"
