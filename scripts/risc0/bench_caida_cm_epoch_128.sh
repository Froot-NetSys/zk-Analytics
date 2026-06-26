#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export MODE=cm
export BENCH_INPUT=caida
export DATA_TYPE=caida
export CAIDA_DIR="${CAIDA_DIR:-../testdata/caida_pcap/caida_txt}"
export CAIDA_MAX_FILES="${CAIDA_MAX_FILES:-10000}"

exec "$ROOT_DIR/scripts/bench_risc0_aggregator_epoch_128.sh"
