#!/usr/bin/env bash
set -euo pipefail
# Re-run the zkVM aggregation proofs at 56 threads (all cores) for the §7.2
# epoch size (16,384 logs = series 128 x samples 128), 1 epoch per mode, to
# match the paper's 56-core setup. Writes one CSV row per mode.
#
# Identical synthetic workload (seed, shape) to the native baseline so the
# native-vs-zkVM comparison is apples-to-apples.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
export RAYON_NUM_THREADS=56
# RocksDB/zstd bindings (pulled in by the aggregator host) need libclang.
export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
mkdir -p target/tmp
OUT="$ROOT_DIR/results/zkvm_aggregation_56threads.csv"
echo "mode,threads,series,samples_per_series,epoch_events,prove_ms_total,verify_ms_total,proc_hwm_kb,time_max_rss_kb" > "$OUT"

echo "[zkvm56] building aggregator host (guest ELFs)..."
cargo build -p aggregator --bin aggregator --release

for mode in histogram samples cm; do
  echo "[zkvm56] proving mode=$mode (56 threads, 16384 logs)..."
  log="$ROOT_DIR/results/_zkvm56_${mode}.log"
  /usr/bin/time -v env \
    SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100 \
    RAYON_NUM_THREADS=56 \
    cargo run -p aggregator --bin aggregator --release -- \
      --bench --mode "$mode" --epochs 1 --series 128 --samples-per-series 128 \
      --threads 56 --seed 0xA66A1E > "$log" 2>&1 || { echo "[zkvm56] $mode FAILED"; tail -5 "$log"; continue; }

  prove=$(grep -oE '^prove_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  verify=$(grep -oE '^verify_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  hwm=$(grep -oE '^proc_hwm_kb=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  ee=$(grep -oE '^epoch_events=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  trss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
  printf '%s,56,128,128,%s,%s,%s,%s,%s\n' \
    "$mode" "${ee:-16384}" "${prove:-}" "${verify:-}" "${hwm:-}" "${trss:-}" >> "$OUT"
  echo "[zkvm56] $mode done: prove_ms=$prove verify_ms=$verify hwm_kb=$hwm max_rss_kb=$trss"
done
echo "[zkvm56] all done -> $OUT"
