#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export MODE=histogram
export BENCH_INPUT=tsv
export DATA_TYPE=google_cluster
export TSV_DIR="${TSV_DIR:-../testdata/google_cluster_data/input}"
export TSV_MAX_FILES="${TSV_MAX_FILES:-128}"

exec "$ROOT_DIR/scripts/bench_risc0_aggregator_epoch_128.sh"

