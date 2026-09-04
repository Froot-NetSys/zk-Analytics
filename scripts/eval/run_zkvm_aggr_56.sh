#!/usr/bin/env bash
set -euo pipefail
# Run the Figure 6 zkVM aggregation proofs at 56 threads for the paper's
# epoch size of 16,384 logs while sweeping the paper's Unique Keys in Epoch
# x-axis. Writes one CSV row per (mode, unique_keys) point.
#
# Identical synthetic workload (seed, shape) to the native baseline so the
# native-vs-zkVM comparison is apples-to-apples.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

source "$ROOT_DIR/scripts/lib/common.sh"
THREADS="${THREADS:-56}"
FIG6_EPOCH_EVENTS="${FIG6_EPOCH_EVENTS:-16384}"
FIG6_KEYS="${FIG6_KEYS:-256 512 1024 2048 4096}"
FIG6_MODES="${FIG6_MODES:-histogram samples cm}"
export RAYON_NUM_THREADS="$THREADS"
# RocksDB/zstd bindings (pulled in by the aggregator host) need libclang.
export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
mkdir -p target/tmp
OUT="${FIG6_ZK_OUT:-$ROOT_DIR/results/zkvm_aggregation_56threads.csv}"
echo "mode,unique_keys,events_per_key,threads,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,time_max_rss_kb,proof_bytes,journal_bytes" > "$OUT"

echo "[zkvm56] building aggregator host (guest ELFs)..."
cargo build -p aggregator --bin aggregator --release

for mode in $FIG6_MODES; do
  for unique_keys in $FIG6_KEYS; do
  if (( FIG6_EPOCH_EVENTS % unique_keys != 0 )); then
    echo "[zkvm56] FIG6_EPOCH_EVENTS=$FIG6_EPOCH_EVENTS must be divisible by unique_keys=$unique_keys" >&2
    exit 2
  fi
  events_per_key=$((FIG6_EPOCH_EVENTS / unique_keys))
  echo "[zkvm56] proving mode=$mode unique_keys=$unique_keys events_per_key=$events_per_key ($THREADS threads)..."
  log="$ROOT_DIR/results/_zkvm56_${mode}_k${unique_keys}.log"
  /usr/bin/time -v env \
    SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100 \
    RAYON_NUM_THREADS="$THREADS" \
    cargo run -p aggregator --bin aggregator --release -- \
      --bench --mode "$mode" --epochs 1 --series "$unique_keys" --samples-per-series "$events_per_key" \
      --threads "$THREADS" --seed 0xA66A1E > "$log" 2>&1 || { echo "[zkvm56] $mode/$unique_keys FAILED"; tail -5 "$log"; continue; }

  prove=$(grep -oE '^prove_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  verify=$(grep -oE '^verify_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  hwm=$(grep -oE '^proc_hwm_kb=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  ee=$(grep -oE '^epoch_events=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  trss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
  proof=$(grep -oE '^proof_bytes_max=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  journal=$(grep -oE '^journal_bytes_last=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$mode" "$unique_keys" "$events_per_key" "$THREADS" "${ee:-$FIG6_EPOCH_EVENTS}" \
    "${prove:-}" "${verify:-}" "${hwm:-}" "${trss:-}" \
    "${proof:-}" "${journal:-}" >> "$OUT"
  echo "[zkvm56] $mode/$unique_keys done: prove_ms=$prove verify_ms=$verify hwm_kb=$hwm max_rss_kb=$trss"
  done
done
echo "[zkvm56] all done -> $OUT"
