#!/usr/bin/env bash
set -uo pipefail
# Phase 2 of the non-ZK camera-ready evaluation, DEV-MODE flavour: run every
# planned zkVM experiment in RISC Zero dev mode (RISC0_DEV_MODE=1) so it finishes
# in minutes (guest executed, no STARK proof), validate the full pipeline on the
# real datasets, then re-merge. Real proof-generation TIMES still come from the
# existing measured data (bench_csv + paper Fig. 4); dev mode supplies the zkVM
# *execution* (witness-gen) component of the breakdown.
#
# Steps:
#   1. Dev-mode aggregation + query sweep  -> results/zkvm_dev_{aggregation,query}.csv
#   2. Prep CAIDA txt from the pcap         -> testdata/caida_pcap/caida_txt
#   3. Native + dev e2e on Google/CAIDA     -> results/non_zk_e2e_baseline.csv
#   4. Re-merge everything                  -> results/*.csv, plots/*, summary
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"
export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"

echo "===== [phase2] 1/4 dev-mode zkVM sweep ====="
./scripts/run_zkvm_dev_mode.sh || echo "[phase2] dev sweep had errors (continuing)"

echo "===== [phase2] 2/4 CAIDA prep ====="
if ! compgen -G "$ROOT_DIR/testdata/caida_pcap/caida_txt/*.txt" >/dev/null; then
  ./scripts/prep_caida.sh || echo "[phase2] caida prep failed (continuing)"
fi

echo "===== [phase2] 3/4 native + dev e2e on real datasets ====="
./scripts/run_e2e_native_baseline.sh || echo "[phase2] e2e failed (continuing)"

echo "===== [phase2] 4/4 re-merge results ====="
python3 scripts/build_non_zk_results.py

echo "[phase2] done."
