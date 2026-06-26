#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper: sweep only the "histogram" epoch type.
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_aggr_histogram_sweep.sh
#
# Supports the same env vars as `scripts/bench_aggregator_sweep.sh` (except MODE).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="histogram" \
  NUM_SOURCES="${NUM_SOURCES:-8}" \
  SERIES_LIST="${SERIES_LIST:-8 16 32}" \
  SPS_LIST="${SPS_LIST:-32 64 128}" \
  OUT_NAME="bench_risc0_aggregator_histogram.csv" \
  ./scripts/bench_aggregator_sweep.sh

