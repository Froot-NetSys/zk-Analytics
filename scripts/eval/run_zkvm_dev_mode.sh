#!/usr/bin/env bash
set -euo pipefail
trap 'echo "[dev] ERROR at line $LINENO; benchmark incomplete (see results/_dev_*.log)" >&2' ERR
# Run local aggregation/query guest checks in RISC Zero DEV MODE
# (RISC0_DEV_MODE=1), using one aggregator:
# the guest is EXECUTED (RISC-V emulation / witness generation) but NO STARK
# proof is generated, so every experiment finishes in seconds-to-minutes
# instead of hours. This validates local guest execution and measures the zkVM
# *execution* time (the witness-generation component of the cost breakdown).
# It does NOT validate distributed scaling or measure real proof-generation
# time or proving memory
# (those come from the existing measured data: bench_csv + paper Fig. 4).
#
# The dev CSV schemas intentionally match their real-proof counterparts exactly.
# In dev mode, prove_ms/prove_ms_total measure guest execution. proof_bytes and
# verify_ms are written as zero because no cryptographic proof exists or is
# cryptographically verified; journal_bytes remains the real public output size.
# Writes:
#   results/zkvm_dev_aggregation.csv   (same schema as the Figure 6 ZK CSV)
#   results/zkvm_dev_query.csv         (same schema as the Figure 7 ZK CSV)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
mkdir -p "$ROOT_DIR/results"

export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
source "$ROOT_DIR/scripts/lib/common.sh"
export RISC0_DEV_MODE=1
THREADS="${THREADS:-56}"
SEED="${SEED:-0xA66A1E}"
RUN_AGGREGATION="${RUN_AGGREGATION:-1}"
RUN_QUERY="${RUN_QUERY:-1}"
FIG6_EPOCH_EVENTS="${FIG6_EPOCH_EVENTS:-16384}"
FIG6_KEYS="${FIG6_KEYS:-256 512 1024 2048 4096}"
FIG6_MODES="${FIG6_MODES:-samples histogram cm}"
EPOCH_LIST="${EPOCH_LIST:-1 2 4 8 16}"

if [ "$RUN_AGGREGATION" = 1 ]; then
  echo "[dev] building aggregator benchmark ..."
  cargo build -p aggregator --bin aggregator --release >/dev/null
  HOST=target/release/aggregator
fi
if [ "$RUN_QUERY" = 1 ]; then
  echo "[dev] building query benchmark ..."
  cargo build -p querier-host --bin bench_queries --release >/dev/null
  BQ=target/release/bench_queries
fi

# -------- Aggregation (dev mode): Figure 6, one 16,384-log epoch ------------
if [ "$RUN_AGGREGATION" = 1 ]; then
AGG_OUT="${FIG6_DEV_OUT:-$ROOT_DIR/results/zkvm_dev_aggregation.csv}"
echo "mode,unique_keys,events_per_key,threads,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,time_max_rss_kb,proof_bytes,journal_bytes" > "$AGG_OUT"
for mode in $FIG6_MODES; do
  for unique_keys in $FIG6_KEYS; do
  if (( FIG6_EPOCH_EVENTS % unique_keys != 0 )); then
    echo "[dev] FIG6_EPOCH_EVENTS=$FIG6_EPOCH_EVENTS must be divisible by unique_keys=$unique_keys" >&2
    exit 2
  fi
  epochs=1
  events_per_key=$((FIG6_EPOCH_EVENTS / unique_keys))
  log="$ROOT_DIR/results/_dev_agg_${mode}_k${unique_keys}.log"
  echo "[dev] aggregation mode=$mode unique_keys=$unique_keys events_per_key=$events_per_key (RISC0_DEV_MODE=1) ..."
  /usr/bin/time -v env RISC0_DEV_MODE=1 RAYON_NUM_THREADS="$THREADS" \
    SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100 \
    "$HOST" --bench --mode "$mode" --epochs "$epochs" --series "$unique_keys" \
      --samples-per-series "$events_per_key" --seed "$SEED" --threads "$THREADS" \
    > "$log" 2>&1
  pm=$(grep -oE '^prove_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  hwm=$(grep -oE '^proc_hwm_kb=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  ee=$(grep -oE '^epoch_events=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  trss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
  journal=$(grep -oE '^journal_bytes_last=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  for metric in pm hwm ee trss journal; do
    if [[ ! ${!metric} =~ ^[0-9]+$ ]]; then
      echo "[dev] missing $metric in $log; refusing incomplete CSV row" >&2
      exit 1
    fi
  done
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$mode" "$unique_keys" "$events_per_key" "$THREADS" \
    "${ee:-$FIG6_EPOCH_EVENTS}" "${pm:-}" 0 "${hwm:-}" \
    "${trss:-}" 0 "${journal:-}" >> "$AGG_OUT"
  echo "[dev] $mode/$unique_keys done: prove_ms=$pm verify_ms=0 hwm_kb=$hwm max_rss_kb=$trss"
  done
done
fi

# -------- Query (dev mode): Fig.7 shapes, epochs 1..16 -----------------------
if [ "$RUN_QUERY" = 1 ]; then
Q_OUT="$ROOT_DIR/results/zkvm_dev_query.csv"
echo "epoch_type,query,num_epochs,events_per_epoch,keys,prove_ms,verify_ms,max_rss_kb,proof_bytes" > "$Q_OUT"
map_query() {
  case "$1" in
    samples/sum) echo "samples samples_sum" ;;
    samples/sum_topk) echo "samples samples_sum_topk" ;;
    samples/sum_key) echo "samples per_key_sum" ;;
    cm/topk) echo "cm cm_topk" ;;
    cm/estimate) echo "cm cm_estimate" ;;
    histogram/p90) echo "histogram hist_percentile" ;;
    *) echo "" ;;
  esac
}
run_q() {  # label expected_rows keys events_per_key skips...
  local label="$1" expected_rows="$2" kps="$3" epk="$4"; shift 4
  for ne in $EPOCH_LIST; do
    local log="$ROOT_DIR/results/_dev_q_${label}_e${ne}.log"
    echo "[dev] query $label epochs=$ne (RISC0_DEV_MODE=1) ..."
    /usr/bin/time -v env RISC0_DEV_MODE=1 RAYON_NUM_THREADS="$THREADS" \
      "$BQ" --epochs "$ne" --num-sources 1 --sources-per-epoch 1 \
      --keys-per-source "$kps" --events-per-key "$epk" --num-aggregators 1 \
      --dp-disabled "$@" > "$log" 2>&1
    local rss
    rss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
    local count
    count=$(grep -c '^CSVROW,' "$log" || true)
    if [[ "$count" != "$expected_rows" || ! "$rss" =~ ^[0-9]+$ ]]; then
      echo "[dev] incomplete query metrics in $log (expected $expected_rows rows, got $count)" >&2
      exit 1
    fi
    grep '^CSVROW,' "$log" | while IFS=, read -r _ qt ep keys pms _vms _pbytes; do
      mapped=$(map_query "$qt")
      if [[ -z "$mapped" || "$ep" != "$ne" || ! "$keys" =~ ^[0-9]+$ || ! "$pms" =~ ^[0-9]+$ ]]; then
        echo "[dev] invalid query metrics in $log: $qt,$ep,$keys,$pms" >&2
        exit 1
      fi
      echo "${mapped% *},${mapped#* },$ep,$((keys * epk)),$keys,$pms,0,${rss:-},0" >> "$Q_OUT"
    done
  done
}
run_q samples   2 1024 8 --skip-histogram --skip-cm --skip-raw --skip-samples-sum-key
run_q histogram 1 1024 8 --skip-samples --skip-cm --skip-raw --skip-histogram-bucket --skip-histogram-all
run_q cm        2 8192 1 --skip-samples --skip-histogram --skip-raw
fi

echo "[dev] done."
if [ "$RUN_AGGREGATION" = 1 ]; then
  echo "=== aggregation ==="
  column -t -s, "$AGG_OUT" || cat "$AGG_OUT"
fi
if [ "$RUN_QUERY" = 1 ]; then
  echo "=== query ==="
  column -t -s, "$Q_OUT" || cat "$Q_OUT"
fi
