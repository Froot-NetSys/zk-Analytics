#!/usr/bin/env bash
set -euo pipefail

# Sweep rayon threads for samples epoch type
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_aggr_samples_thread_sweep.sh
#
# Options (env vars):
#   THREADS_LIST   Space-separated thread counts (default: "56 32 16 8 4 2 1")
#   SERIES_LIST    Space-separated series counts (default: "256")
#   SPS_LIST       Space-separated samples-per-series (default: "8")
#   REPEATS        Repeats per config (default: "1")

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

THREADS_LIST="${THREADS_LIST:-56 32 16 8 4 2 1}"

# Use smaller parameter sweep for thread testing
NUM_SOURCES="${NUM_SOURCES:-128}"
SERIES_LIST="${SERIES_LIST:-32}"
SPS_LIST="${SPS_LIST:-8}"
KEY_ZIPF_S_LIST="${KEY_ZIPF_S_LIST:-1.2}"
VALUE_ZIPF_S_LIST="${VALUE_ZIPF_S_LIST:-1.2}"
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
REPEATS="${REPEATS:-1}"

for threads in $THREADS_LIST; do
  echo "=== Running mode=samples with threads=$threads ==="
  MODE="samples" \
    THREADS="$threads" \
    NUM_SOURCES="$NUM_SOURCES" \
    SERIES_LIST="$SERIES_LIST" \
    SPS_LIST="$SPS_LIST" \
    KEY_ZIPF_S_LIST="$KEY_ZIPF_S_LIST" \
    VALUE_ZIPF_S_LIST="$VALUE_ZIPF_S_LIST" \
    RISC0_DEV_MODE="$RISC0_DEV_MODE" \
    REPEATS="$REPEATS" \
    OUT_NAME="bench_risc0_aggregator_samples_threads${threads}.csv" \
    ./scripts/bench_aggregator_sweep.sh
done

echo ""
echo "Samples thread sweep complete!"
echo "CSV files written to: bench_csv/bench_risc0_aggregator_samples_threads*.csv"
