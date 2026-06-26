#!/usr/bin/env bash
set -euo pipefail

# Run REAL zkVM query proofs at small epoch counts (1/2/4) to anchor the query
# slowdown with measured data (the paper's Fig. 4 only gives the 16-epoch point).
# Uses the self-contained bench_queries binary (in-memory synthetic epochs ->
# real RISC Zero proof; no FDB/RocksDB needed).
#
# Figure-7 query shapes: 8,192 logs/epoch; CM epochs 8,192 keys, Histogram and
# Hash-table epochs 1,024 keys.
#
# Writes results/zkvm_query_proofs.csv consumed by build_non_zk_results.py.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
export RAYON_NUM_THREADS="${THREADS:-56}"
EPOCH_LIST="${EPOCH_LIST:-1 2 4}"

echo "[qproof] building bench_queries (querier guest ELFs)..."
cargo build -p querier-host --bin bench_queries --release >/dev/null
BIN=target/release/bench_queries

OUT="$ROOT_DIR/results/zkvm_query_proofs.csv"
echo "epoch_type,query,num_epochs,events_per_epoch,keys,prove_ms,verify_ms,max_rss_kb,proof_bytes" > "$OUT"

# Map bench_queries' "query_type" string -> (epoch_type, our query key)
map_query() {
  case "$1" in
    samples/sum)      echo "samples global_sum" ;;
    samples/sum_topk) echo "samples topk_hash" ;;
    samples/sum_key)  echo "samples per_key_sum" ;;
    cm/topk)          echo "cm cm_topk" ;;
    cm/estimate)      echo "cm cm_estimate" ;;
    histogram/p90)    echo "histogram hist_percentile" ;;
    *)                echo "" ;;
  esac
}

run_group() {  # label keys_per_source events_per_key extra_skips...
  local label="$1" kps="$2" epk="$3"; shift 3
  local skips=("$@")
  for ne in $EPOCH_LIST; do
    local log="$ROOT_DIR/results/_qproof_${label}_e${ne}.log"
    echo "[qproof] $label epochs=$ne keys=$kps events/key=$epk ..."
    /usr/bin/time -v "$BIN" --epochs "$ne" --num-sources 1 --sources-per-epoch 1 \
      --keys-per-source "$kps" --events-per-key "$epk" --num-aggregators 1 \
      --dp-disabled "${skips[@]}" > "$log" 2>&1 || { echo "  FAILED"; tail -4 "$log"; continue; }
    local rss; rss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1)
    # Parse CSVROW lines: CSVROW,query_type,epochs,keys,proof_ms,verify_ms,proof_bytes
    grep '^CSVROW,' "$log" | while IFS=, read -r _ qt ep keys pms vms pbytes; do
      mapped=$(map_query "$qt"); [[ -z "$mapped" ]] && continue
      et=${mapped% *}; qk=${mapped#* }
      epe=$(( keys * epk )); [[ "$et" == "cm" ]] && epe=$(( keys * epk ))
      echo "$et,$qk,$ep,$epe,$keys,$pms,$vms,${rss:-},$pbytes" >> "$OUT"
    done
  done
}

# Hash-table / Histogram epochs: 1024 keys x 8 events = 8192 logs/epoch.
run_group samples   1024 8 --skip-histogram --skip-cm --skip-raw \
  --skip-samples-sum-key
run_group histogram 1024 8 --skip-samples --skip-cm --skip-raw \
  --skip-histogram-bucket --skip-histogram-all
# CM epochs: 8192 keys x 1 event = 8192 logs/epoch.
run_group cm        8192 1 --skip-samples --skip-histogram --skip-raw

echo "[qproof] done -> $OUT"; column -t -s, "$OUT" || cat "$OUT"
