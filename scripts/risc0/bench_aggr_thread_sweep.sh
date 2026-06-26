#!/usr/bin/env bash
set -euo pipefail

# Sweep rayon threads for all 3 epoch types on a single machine
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_aggr_thread_sweep.sh
#
# Options (env vars):
#   THREADS_LIST   Space-separated thread counts (default: "1 2 4 8 16 32 56")
#   SERIES_LIST    Space-separated series counts (default: "32 64 128")
#   SPS_LIST       Space-separated samples-per-series (default: "64 128")
#   REPEATS        Repeats per config (default: "1")

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

THREADS_LIST="${THREADS_LIST:-56 32 16 8 4 2 1}"
MODES=("samples" "histogram" "cm")

# Use smaller parameter sweep for thread testing
NUM_SOURCES="${NUM_SOURCES:-8}"
SERIES_LIST="${SERIES_LIST:-32}"
SPS_LIST="${SPS_LIST:-16}"
KEY_ZIPF_S_LIST="${KEY_ZIPF_S_LIST:-0 1.2 1.5}"
VALUE_ZIPF_S_LIST="${VALUE_ZIPF_S_LIST:-1.2}"
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
REPEATS="${REPEATS:-1}"

for threads in $THREADS_LIST; do
  for mode in "${MODES[@]}"; do
    echo "=== Running mode=$mode with threads=$threads ==="
    MODE="$mode" \
      THREADS="$threads" \
      NUM_SOURCES="$NUM_SOURCES" \
      SERIES_LIST="$SERIES_LIST" \
      SPS_LIST="$SPS_LIST" \
      KEY_ZIPF_S_LIST="$KEY_ZIPF_S_LIST" \
      VALUE_ZIPF_S_LIST="$VALUE_ZIPF_S_LIST" \
      RISC0_DEV_MODE="$RISC0_DEV_MODE" \
      REPEATS="$REPEATS" \
      OUT_NAME="bench_risc0_aggregator_${mode}_threads${threads}.csv" \
      ./scripts/bench_aggregator_sweep.sh
  done
done

echo ""
echo "Thread sweep complete!"
echo "CSV files written to: bench_csv/bench_risc0_aggregator_*_threads*.csv"
