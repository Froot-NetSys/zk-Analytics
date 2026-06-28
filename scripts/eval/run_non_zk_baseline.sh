#!/usr/bin/env bash
set -euo pipefail

# Camera-ready non-ZK native baseline (SIGCOMM #573 zk-Analytics).
#
# Reruns the NATIVE (no zkVM, no proof) aggregation + query analytics on the
# same machine / input / epoch+batch sizes / aggregator counts / matched CPU
# cores as the zkVM experiments, then merges with the existing measured zkVM
# numbers to produce:
#   results/non_zk_aggregation_baseline.csv
#   results/non_zk_query_baseline.csv
#   results/zk_cost_breakdown.csv
#   results/non_zk_baseline_summary.md
#   plots/non_zk_vs_zk_{aggregation,query}.pdf  plots/zk_cost_breakdown.pdf
#
# Env knobs (defaults match the zkVM runs):
#   THREADS_MATCHED  rayon threads matched to the zkVM runs (default 32)
#   THREADS_MAX      all-core rayon threads (default: nproc)
#   REPS             timing repetitions, min reported (default 7 / 9)
#   SEED             base RNG seed (default 0xA66A1E)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

THREADS_MATCHED="${THREADS_MATCHED:-32}"
THREADS_MAX="${THREADS_MAX:-$(nproc)}"
REPS_AGG="${REPS:-7}"
REPS_Q="${REPS:-9}"
SEED="${SEED:-0xA66A1E}"

# Compile-time structural params of the aggregation cores — MUST match the
# values used for the zkVM benchmark CSVs (paper §6/§7.2).
export SAMPLES_HT_BUCKETS="${SAMPLES_HT_BUCKETS:-64}"
export SAMPLES_HT_BUCKET_CAP="${SAMPLES_HT_BUCKET_CAP:-4}"
export HISTOGRAM_SLOTS="${HISTOGRAM_SLOTS:-32}"
export CM_TOPK_SLOTS="${CM_TOPK_SLOTS:-100}"

mkdir -p results plots target/tmp

echo "[non-zk] building native-baseline (no zkVM guest build) ..."
( cargo build -p native-baseline --release )
BIN="target/release/native-baseline"

AGG_RAW="results/_native_aggregation_raw.txt"
Q_RAW="results/_native_query_raw.txt"
: > "$AGG_RAW"
: > "$Q_RAW"

echo "[non-zk] native AGGREGATION matrix (epoch=16384 logs, batch=8) ..."
# Aggregator counts 1/2/4/8 -> 8/4/2/1 epochs handled by the busiest aggregator.
for mode in samples histogram cm; do
  for epochs in 8 4 2 1; do
    for th in "$THREADS_MATCHED" "$THREADS_MAX"; do
      echo "### mode=$mode epochs=$epochs threads=$th" >> "$AGG_RAW"
      "$BIN" --task aggregation --mode "$mode" \
        --series 128 --samples-per-series 128 --batch 8 \
        --epochs "$epochs" --threads "$th" --reps "$REPS_AGG" --seed "$SEED" \
        >> "$AGG_RAW"
    done
  done
done

echo "[non-zk] native QUERY matrix (8192 logs/epoch, epochs 1..256) ..."
# epoch_type:query:series:samples_per_series   (series*sps == 8192 logs/epoch)
for cfg in \
  samples:global_sum:1024:8 samples:per_key_sum:1024:8 samples:topk_hash:1024:8 \
  cm:cm_topk:8192:1 cm:cm_estimate:8192:1 histogram:hist_percentile:1024:8 ; do
  IFS=: read -r et qk series sps <<< "$cfg"
  for ne in 1 2 4 8 16 32 64 128 256; do
    echo "### et=$et q=$qk num_epochs=$ne" >> "$Q_RAW"
    "$BIN" --task query --epoch-type "$et" --query "$qk" \
      --series "$series" --samples-per-series "$sps" \
      --num-epochs "$ne" --reps "$REPS_Q" --seed "$SEED" >> "$Q_RAW"
  done
done

echo "[non-zk] merging with measured zkVM numbers ..."
python3 scripts/lib/build_non_zk_results.py

echo "[non-zk] done. See results/non_zk_baseline_summary.md"
