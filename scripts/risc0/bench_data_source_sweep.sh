#!/usr/bin/env bash
set -euo pipefail

# Benchmark data_source per-key hash chains (SHA256) with serial/parallel comparison.
# Sweeps batch size per key, threads, and total unique keys.
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_data_source_sweep.sh
#
# Options (env vars):
#   BATCH_SIZE_LIST     Space-separated batch sizes per key (default: "10 100 1000")
#   KEY_MOD_LIST        Space-separated unique key counts (default: "100 1000 10000")
#   THREADS_LIST        Space-separated thread counts for parallel (default: "1 2 4 8")
#   REPEATS             Repeats per config (default: "1")
#   SEED                Base seed (default: "0x5EED")
#   CSV_DIR             Output CSV directory (default: "bench_csv")
#   LOG_DIR             Directory for raw logs (default: "bench_logs")
#
# Output metrics:
#   time_per_log_ns         Time per log in nanoseconds (parallel)
#   per_log_bytes           Bytes per log = 23 (key_id 15 + value 4 + ts 4)
#   per_log_amort_hash_bytes  Amortized hash commitment bytes = 64 / batch_size
#   overhead_ratio          Ratio = per_log_amort_hash_bytes / per_log_bytes

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Matches `risc0/.cargo/config.toml` (rustc temp dir). Avoids "couldn't create a temp dir".
mkdir -p target/tmp

BATCH_SIZE_LIST="${BATCH_SIZE_LIST:-1 2 4 8 16 32 64 128 256}"
KEY_MOD_LIST="${KEY_MOD_LIST:-100 1000 10000}"
THREADS_LIST="${THREADS_LIST:-1 2 4 8}"
REPEATS="${REPEATS:-1}"
SEED_BASE="${SEED:-0x5EED}"
CSV_DIR="${CSV_DIR:-bench_csv}"
OUT="${CSV_DIR}/bench_data_source_sha256.csv"
LOG_DIR="${LOG_DIR:-bench_logs}"

# Constants for overhead calculation
PER_LOG_BYTES=23        # key_id (15) + value (4) + ts (4)
BATCH_OVERHEAD_BYTES=32 # batch_hash only (sequential verification)

mkdir -p "$CSV_DIR" "$LOG_DIR"

# Build once before running benchmarks (avoids cargo overhead in memory measurements)
echo "Building release binary..."
cargo build -p data_source --bin data_source --release
BENCH_BIN="${ROOT_DIR}/target/release/data_source"

CSV_HEADER="ts,batch_size,n_unique_keys,n_events,threads,rep,seed,serial_ns,serial_ms,serial_ns_per_event,parallel_ns,parallel_ms,parallel_ns_per_event,speedup,per_log_bytes,per_log_amort_hash_bytes,overhead_ratio,rss_kb_baseline,rss_kb_after_grouping,rss_kb_before_hash,rss_kb_after_serial,rss_kb_after_parallel,hash_mem_serial_kb,hash_mem_parallel_kb,cpu_user_s,cpu_sys_s,cpu_pct,wall_clock_s,time_max_rss_kb,log_path"
if [[ -f "$OUT" ]]; then
  first_line="$(head -n 1 "$OUT" || true)"
  if [[ "$first_line" != "$CSV_HEADER" ]]; then
    ts_suffix="$(date +%Y%m%d_%H%M%S)"
    OUT="${CSV_DIR}/bench_data_source_sha256_${ts_suffix}.csv"
    echo "warning: existing CSV header mismatch; writing to new file: $OUT" >&2
    echo "$CSV_HEADER" >"$OUT"
  fi
else
  echo "$CSV_HEADER" >"$OUT"
fi

run_one() {
  local rep="$1"
  local seed="$2"
  local batch_size="$3"
  local key_mod="$4"
  local threads="$5"
  local n_events="$6"

  local ts log_path
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_path="${LOG_DIR}/data_source_sha256_b${batch_size}_k${key_mod}_t${threads}_r${rep}.log"

  # Calculate overhead metrics
  # per_log_amort_hash_bytes = 64 / batch_size (amortized hash bytes per log)
  # overhead_ratio = per_log_amort_hash_bytes / per_log_bytes
  local per_log_amort_hash_bytes overhead_ratio
  per_log_amort_hash_bytes=$(awk "BEGIN { printf \"%.4f\", $BATCH_OVERHEAD_BYTES / $batch_size }")
  overhead_ratio=$(awk "BEGIN { printf \"%.6f\", ($BATCH_OVERHEAD_BYTES / $batch_size) / $PER_LOG_BYTES }")

  cmd=(env
    RAYON_NUM_THREADS="$threads"
    "$BENCH_BIN" --bench --streaming
    --events "$n_events"
    --seed "$seed"
    --key-mod "$key_mod"
  )
  {
    printf 'cmd='
    printf '%q ' "${cmd[@]}"
    printf '\n'
    printf 'batch_size=%s\n' "$batch_size"
    printf 'n_unique_keys=%s\n' "$key_mod"
    printf 'per_log_bytes=%s\n' "$PER_LOG_BYTES"
    printf 'per_log_amort_hash_bytes=%s\n' "$per_log_amort_hash_bytes"
    printf 'overhead_ratio=%s\n' "$overhead_ratio"
  } >"$log_path"

  run_cmd() {
    if command -v /usr/bin/time >/dev/null 2>&1; then
      # Capture CPU time and memory: user_s, sys_s, cpu_pct, wall_clock_s, max_rss_kb
      /usr/bin/time -f 'time_user_s=%U
time_sys_s=%S
time_cpu_pct=%P
time_wall_s=%e
time_max_rss_kb=%M' "${cmd[@]}"
    else
      "${cmd[@]}"
    fi
  }

  run_cmd 2>&1 \
    | tee -a "$log_path" \
    | awk -v out="$OUT" -v ts="$ts" -v log_path="$log_path" \
        -v batch_size="$batch_size" -v key_mod="$key_mod" -v threads="$threads" \
        -v rep="$rep" -v seed="$seed" -v n_events="$n_events" \
        -v per_log_bytes="$PER_LOG_BYTES" \
        -v per_log_amort_hash_bytes="$per_log_amort_hash_bytes" \
        -v overhead_ratio="$overhead_ratio" \
        'BEGIN { }
         match($0, /^([A-Za-z0-9_]+)=(.*)$/, m) {
           k=m[1]; v=m[2];
           gsub(/^[ \t]+|[ \t]+$/, "", v);
           kv[k]=v;
         }
         END {
           n_chains=kv["n_chains"]+0;
           serial_ns=kv["serial_ns"]+0;
           serial_ms=kv["serial_ms"]+0;
           serial_ns_per_event=kv["serial_ns_per_event"]+0;
           parallel_ns=kv["parallel_ns"]+0;
           parallel_ms=kv["parallel_ms"]+0;
           parallel_ns_per_event=kv["parallel_ns_per_event"]+0;
           speedup=kv["speedup"];
           gsub(/x$/, "", speedup);

           # Memory breakdown
           rss_baseline=kv["rss_kb_baseline"]+0;
           rss_after_grouping=kv["rss_kb_after_grouping"]+0;
           rss_before_hash=kv["rss_kb_before_hash"]+0;
           rss_after_serial=kv["rss_kb_after_serial"]+0;
           rss_after_parallel=kv["rss_kb_after_parallel"]+0;
           hash_mem_serial=kv["hash_mem_serial_kb"]+0;
           hash_mem_parallel=kv["hash_mem_parallel_kb"]+0;

           cpu_user=kv["time_user_s"]+0;
           cpu_sys=kv["time_sys_s"]+0;
           cpu_pct=kv["time_cpu_pct"];
           gsub(/%/, "", cpu_pct);
           wall_s=kv["time_wall_s"]+0;
           time_kb=kv["time_max_rss_kb"]+0;

           printf "%s,%s,%s,%s,%s,%s,%s", ts, batch_size, key_mod, n_events, threads, rep, seed >> out;
           printf ",%s,%s,%s", serial_ns, serial_ms, serial_ns_per_event >> out;
           printf ",%s,%s,%s,%s", parallel_ns, parallel_ms, parallel_ns_per_event, speedup >> out;
           printf ",%s,%s,%s", per_log_bytes, per_log_amort_hash_bytes, overhead_ratio >> out;
           printf ",%s,%s,%s,%s,%s,%s,%s", rss_baseline, rss_after_grouping, rss_before_hash, rss_after_serial, rss_after_parallel, hash_mem_serial, hash_mem_parallel >> out;
           printf ",%s,%s,%s,%s,%s,%s", cpu_user, cpu_sys, cpu_pct, wall_s, time_kb, log_path >> out;
           printf "\n" >> out;

           printf "batch=%s keys=%s time_per_log=%.1fns amort_hash=%.2fB overhead=%.4f speedup=%sx\n", batch_size, key_mod, parallel_ns_per_event, per_log_amort_hash_bytes, overhead_ratio, speedup > "/dev/stderr";
         }'
}

echo "Sweeping: batch_size=[${BATCH_SIZE_LIST}] keys=[${KEY_MOD_LIST}] threads=[${THREADS_LIST}]"
echo "Per-log bytes: ${PER_LOG_BYTES} (key_id=15 + value=4 + ts=4)"
echo "Batch overhead: ${BATCH_OVERHEAD_BYTES} bytes (batch_hash only, sequential verification)"
echo ""

rep=0
while [[ "$rep" -lt "$REPEATS" ]]; do
  for batch_size in $BATCH_SIZE_LIST; do
    for key_mod in $KEY_MOD_LIST; do
      # Total events = batch_size * num_unique_keys
      n_events=$((batch_size * key_mod))
      for threads in $THREADS_LIST; do
        seed="$((SEED_BASE + rep))"
        run_one "$rep" "$seed" "$batch_size" "$key_mod" "$threads" "$n_events"
        echo "ok batch_size=${batch_size} keys=${key_mod} events=${n_events} threads=${threads} rep=${rep}"
      done
    done
  done
  rep="$((rep + 1))"
done

echo ""
echo "Wrote CSV: $OUT"
echo ""
echo "CSV columns:"
echo "  batch_size            - Events per key batch"
echo "  n_unique_keys         - Number of unique keys (key_mod)"
echo "  n_events              - Total events (batch_size * n_unique_keys)"
echo "  parallel_ns_per_event - Time per log in nanoseconds"
echo "  per_log_bytes         - Bytes per log = 23"
echo "  per_log_amort_hash_bytes - Amortized hash commitment bytes = 64/batch_size"
echo "  overhead_ratio        - Hash overhead ratio = amort_hash_bytes / per_log_bytes"
