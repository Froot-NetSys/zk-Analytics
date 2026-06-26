#!/usr/bin/env bash
set -uo pipefail

# Drive the full non-ZK / zk-Analytics baseline matrix by calling
# scripts/run_baseline_e2e.sh for each (dataset, epoch_size, mode) cell, then
# build the camera-ready CSV tables.
#
# Figure-4 mapping: google=hash/8 aggregators, caida=cms/8, vehicle=histogram/4.
# Epoch sweep {8192,16384,32768} applies to google+caida (131,072 logs each);
# vehicle has only 10,058 records so it runs a single natural config.
#
# Env knobs:
#   DATASETS   space list (default "google caida vehicle")
#   EPOCHS     space list (default "8192 16384 32768") for google/caida
#   MODES      space list (default "native zk")
#   FRESH      1 = truncate results/_e2e_metrics.jsonl first (default 1)
#   MAIN_EPOCH main paper-table epoch (default 16384)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATASETS="${DATASETS:-google caida vehicle}"
EPOCHS="${EPOCHS:-8192 16384 32768}"
MODES="${MODES:-native zk}"
FRESH="${FRESH:-1}"
MAIN_EPOCH="${MAIN_EPOCH:-16384}"
METRICS="$ROOT_DIR/results/_e2e_metrics.jsonl"

mkdir -p "$ROOT_DIR/results"
[ "$FRESH" = "1" ] && : > "$METRICS"

cell() {  # dataset epoch mode
  local ds="$1" ep="$2" md="$3"
  echo "===================================================================="
  echo "[matrix] $ds epoch=$ep mode=$md  ($(date '+%H:%M:%S'))"
  echo "===================================================================="
  DATASET="$ds" EPOCH_LOGS="$ep" MODE="$md" \
    bash "$ROOT_DIR/scripts/run_baseline_e2e.sh" || echo "[matrix] cell FAILED: $ds/$ep/$md"
}

for ds in $DATASETS; do
  if [ "$ds" = "vehicle" ]; then
    eps="$MAIN_EPOCH"          # vehicle: single natural config at main epoch
  else
    eps="$EPOCHS"
  fi
  for ep in $eps; do
    for md in $MODES; do
      cell "$ds" "$ep" "$md"
    done
  done
done

echo "[matrix] building tables ..."
python3 "$ROOT_DIR/scripts/build_baseline_tables.py" \
  --metrics "$METRICS" --outdir "$ROOT_DIR/results" --main-epoch "$MAIN_EPOCH"
echo "[matrix] done. See results/baseline_main_table.csv"
