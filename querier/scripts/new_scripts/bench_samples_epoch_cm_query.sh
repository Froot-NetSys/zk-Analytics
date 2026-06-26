#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
export CARGO_HOME="${CARGO_HOME:-/mydata/cargo_home}"
mkdir -p /mydata/zk-Analytics/target/tmp

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building bench_queries binary (release)..."
  cargo build --manifest-path "${ROOT_DIR}/Cargo.toml" --bin bench_queries --release -q
fi

# Default benchmark parameters
EPOCHS="${EPOCHS:-256}"
KEYS="${KEYS:-128}"
EVENTS_PER_KEY="${EVENTS_PER_KEY:-128}"
SEED="${SEED:-0xBEEF}"

# RISC Zero dev mode (fast proofs for testing, insecure)
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
if [[ "$RISC0_DEV_MODE" == "1" ]]; then
  export RISC0_DEV_MODE=1
fi

echo "Running Samples Epoch Query Benchmark (All Samples Queries)"
echo "==========================================================="
echo "Note: This runs all samples query types including sum, sum_exact_key, and sum_topk"
echo ""
echo "Epochs: $EPOCHS"
echo "Keys per epoch: $KEYS"
echo "Events per key: $EVENTS_PER_KEY"
echo "Seed: $SEED"
echo "RISC0 Dev Mode: $RISC0_DEV_MODE"
echo ""

"${ROOT_DIR}/target/release/bench_queries" \
  --epochs "$EPOCHS" \
  --keys "$KEYS" \
  --events-per-key "$EVENTS_PER_KEY" \
  --seed "$SEED" \
  --skip-histogram \
  --skip-cm
