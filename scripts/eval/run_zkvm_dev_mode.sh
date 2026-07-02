#!/usr/bin/env bash
set -uo pipefail
# Run ALL planned zkVM experiments in RISC Zero DEV MODE (RISC0_DEV_MODE=1):
# the guest is EXECUTED (RISC-V emulation / witness generation) but NO STARK
# proof is generated, so every experiment finishes in seconds-to-minutes
# instead of hours. This validates the full pipeline end-to-end and measures
# the zkVM *execution* time (the witness-generation component of the cost
# breakdown). It does NOT measure real proof-generation time or proving memory
# (those come from the existing measured data: bench_csv + paper Fig. 4).
#
# Writes:
#   results/zkvm_dev_aggregation.csv   (synthetic, §7.2 shape)
#   results/zkvm_dev_query.csv         (Fig.7 query shapes)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
source "$ROOT_DIR/scripts/lib/common.sh"
export RISC0_DEV_MODE=1
THREADS="${THREADS:-56}"
SEED="${SEED:-0xA66A1E}"

echo "[dev] building host + querier ..."
cargo build -p aggregator --bin aggregator --release >/dev/null
cargo build -p querier-host --bin bench_queries --release >/dev/null
HOST=target/release/aggregator
BQ=target/release/bench_queries

# -------- Aggregation (dev mode): 3 modes x {8,4,2,1} epochs of 16,384 logs --
AGG_OUT="$ROOT_DIR/results/zkvm_dev_aggregation.csv"
echo "mode,num_aggregators,epochs,epoch_events,dev_exec_ms,verify_ms,dev_rss_kb" > "$AGG_OUT"
for mode in samples histogram cm; do
  for epochs in 8 4 2 1; do
    nagg=$(( 8 / epochs ))
    log="$ROOT_DIR/results/_dev_agg_${mode}_e${epochs}.log"
    echo "[dev] aggregation mode=$mode epochs=$epochs (RISC0_DEV_MODE=1) ..."
    /usr/bin/time -v env RISC0_DEV_MODE=1 RAYON_NUM_THREADS="$THREADS" \
      SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100 \
      "$HOST" --bench --mode "$mode" --epochs "$epochs" --series 128 \
        --samples-per-series 128 --seed "$SEED" --threads "$THREADS" \
      > "$log" 2>&1
    pm=$(grep -oE '^prove_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
    vm=$(grep -oE '^verify_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
    ee=$(grep -oE '^epoch_events=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
    rss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
    printf '%s,%s,%s,%s,%s,%s,%s\n' "$mode" "$nagg" "$epochs" "${ee:-16384}" "${pm:-}" "${vm:-}" "${rss:-}" >> "$AGG_OUT"
    echo "  dev_exec_ms=${pm:-?} verify_ms=${vm:-?} rss_kb=${rss:-?}"
  done
done

# -------- Query (dev mode): Fig.7 shapes, epochs 1..16 -----------------------
Q_OUT="$ROOT_DIR/results/zkvm_dev_query.csv"
echo "epoch_type,query,num_epochs,keys,dev_exec_ms,verify_ms,proof_bytes" > "$Q_OUT"
map_query() {
  case "$1" in
    samples/sum) echo "samples global_sum" ;;
    samples/sum_topk) echo "samples topk_hash" ;;
    samples/sum_key) echo "samples per_key_sum" ;;
    cm/topk) echo "cm cm_topk" ;;
    cm/estimate) echo "cm cm_estimate" ;;
    histogram/p90) echo "histogram hist_percentile" ;;
    *) echo "" ;;
  esac
}
run_q() {  # label keys events_per_key skips...
  local label="$1" kps="$2" epk="$3"; shift 3
  for ne in 1 2 4 8 16; do
    local log="$ROOT_DIR/results/_dev_q_${label}_e${ne}.log"
    echo "[dev] query $label epochs=$ne (RISC0_DEV_MODE=1) ..."
    env RISC0_DEV_MODE=1 RAYON_NUM_THREADS="$THREADS" \
      "$BQ" --epochs "$ne" --num-sources 1 --sources-per-epoch 1 \
      --keys-per-source "$kps" --events-per-key "$epk" --num-aggregators 1 \
      --dp-disabled "$@" > "$log" 2>&1
    grep '^CSVROW,' "$log" | while IFS=, read -r _ qt ep keys pms vms pbytes; do
      mapped=$(map_query "$qt"); [ -z "$mapped" ] && continue
      echo "${mapped% *},${mapped#* },$ep,$keys,$pms,$vms,$pbytes" >> "$Q_OUT"
    done
  done
}
run_q samples   1024 8 --skip-histogram --skip-cm --skip-raw --skip-samples-sum-key
run_q histogram 1024 8 --skip-samples --skip-cm --skip-raw --skip-histogram-bucket --skip-histogram-all
run_q cm        8192 1 --skip-samples --skip-histogram --skip-raw

echo "[dev] done. -> $AGG_OUT , $Q_OUT"
echo "=== aggregation ==="; column -t -s, "$AGG_OUT"
echo "=== query ==="; column -t -s, "$Q_OUT"
