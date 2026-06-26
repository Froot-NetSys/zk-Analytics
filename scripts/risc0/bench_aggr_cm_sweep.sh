#!/usr/bin/env bash
set -euo pipefail

# Convenience wrapper: sweep only the "cm" epoch type.
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_aggr_cm_sweep.sh
#
# Supports the same env vars as `scripts/bench_aggregator_sweep.sh` (except MODE).

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="cm" \
  NUM_SOURCES="${NUM_SOURCES:-128}" \
  SERIES_LIST="${SERIES_LIST:-8 16 32 64}" \
  SPS_LIST="${SPS_LIST:-8}" \
  OUT_NAME="${OUT_NAME:-bench_risc0_aggregator_cm.csv}" \
  ./scripts/bench_aggregator_sweep.sh
