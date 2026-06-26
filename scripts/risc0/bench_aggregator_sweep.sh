#!/usr/bin/env bash
set -euo pipefail

# Sweep RISC0 aggregator proving time / proof size / memory vs (series, samples_per_series).
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_aggregator_sweep.sh
#
# Per-epoch wrappers:
#   ./scripts/bench_aggr_samples_sweep.sh
#   ./scripts/bench_aggr_histogram_sweep.sh
#   ./scripts/bench_aggr_cm_sweep.sh
#
# Options (env vars):
#   MODE               Aggregator mode: "samples" (default), "histogram", or "cm"
#   NUM_SOURCES        Number of data sources (default: "8")
#   SERIES_LIST        Space-separated series counts per source (default: "4 16 32 64")
#   SPS_LIST           Space-separated samples-per-series (default: "32")
#   COMMIT_BATCH_SIZE  Events per batch (default: "16", matches Kafka production)
#   THREADS            Fixed Rayon threads (default: "56")
#   KEY_ZIPF_S_LIST    Space-separated Zipf s values for key distribution (default: "0 1.2 1.5"; 0=uniform, >0=skewed)
#   VALUE_ZIPF_S_LIST  Space-separated Zipf s values for value distribution (default: "1.2")
#   SAMPLES_HT_BUCKETS   Compile-time override for samples table buckets (default: "64")
#   SAMPLES_HT_BUCKET_CAP Compile-time override for samples table bucket cap (default: "4")
#   HISTOGRAM_SLOTS      Compile-time override for histogram slots (default: "32")
#   CM_TOPK_SLOTS        Compile-time override for cm top-k slots (default: "100")
#   RISC0_DEV_MODE     RISC0 dev mode: "0" (production, default) or "1" (dev mode - faster, less secure)
#   REPEATS            Repeats per config (default: "1")
#   SEED               Base seed (default: "0xA66A1E") - actual seed = SEED + rep
#   SKIP_VERIFY        "0" (default) or "1"
#   CSV_DIR            Output CSV directory (default: "bench_csv")
#   OUT_NAME           Output CSV filename (default: "bench_risc0_aggregator.csv")
#   LOG_DIR            Directory for raw logs (default: "bench_logs")

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Matches `risc0/.cargo/config.toml` (rustc temp dir). Avoids "couldn't create a temp dir".
mkdir -p target/tmp

MODE="${MODE:-samples}"
NUM_SOURCES="${NUM_SOURCES:-8}"
SERIES_LIST="${SERIES_LIST:-8 16 32 64}"
SPS_LIST="${SPS_LIST:-16}"
COMMIT_BATCH_SIZE="${COMMIT_BATCH_SIZE:-8}"
THREADS="${THREADS:-56}"
KEY_ZIPF_S_LIST="${KEY_ZIPF_S_LIST:-0 1.2 1.5}"
VALUE_ZIPF_S_LIST="${VALUE_ZIPF_S_LIST:-1.2}"
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
# Note: avoid `echo` + `tr '[:space:]'` (newline counts as space -> trailing "_").
ZIPF_SUFFIX="$(printf '%s' "$VALUE_ZIPF_S_LIST" | tr -s ' \t' '_' | tr '.' 'p' | sed -e 's/^_\\+//' -e 's/_\\+$//')"
SAMPLES_HT_BUCKETS="${SAMPLES_HT_BUCKETS:-64}"
SAMPLES_HT_BUCKET_CAP="${SAMPLES_HT_BUCKET_CAP:-4}"
HISTOGRAM_SLOTS="${HISTOGRAM_SLOTS:-32}"
CM_TOPK_SLOTS="${CM_TOPK_SLOTS:-100}"
REPEATS="${REPEATS:-1}"
SEED_BASE="${SEED:-0xA66A1E}"
SKIP_VERIFY="${SKIP_VERIFY:-0}"
CSV_DIR="${CSV_DIR:-bench_csv}"
OUT_NAME="${OUT_NAME:-bench_risc0_aggregator.csv}"
OUT="${CSV_DIR}/${OUT_NAME}"
LOG_DIR="${LOG_DIR:-bench_logs}"

mkdir -p "$CSV_DIR" "$LOG_DIR"

CSV_HEADER="ts,component,mode,num_sources,series,samples_per_series,commit_batch_size,epoch_events,input_bytes,input_kb,threads,rep,seed,key_zipf_s,value_zipf_s,skip_verify,risc0_dev_mode,prove_ms_total,verify_ms_total,proc_rss_kb_end,proc_hwm_kb,proc_hwm_mb,time_max_rss_kb,time_max_rss_mb,proof_bytes_last,proof_kb_last,proof_bytes_max,proof_kb_max,journal_bytes_last,journal_kb_last,events_commit_hex,out_commit_hex,log_path"
if [[ -f "$OUT" ]]; then
  first_line="$(head -n 1 "$OUT" || true)"
  if [[ "$first_line" != "$CSV_HEADER" ]]; then
    OUT="${CSV_DIR}/${OUT_NAME%.csv}_zipf${ZIPF_SUFFIX}.csv"
    echo "warning: existing CSV header mismatch; writing to new file: $OUT" >&2
    echo "$CSV_HEADER" >"$OUT"
  fi
else
  echo "$CSV_HEADER" >"$OUT"
fi

run_one() {
  local series="$1"
  local sps="$2"
  local threads="$3"
  local rep="$4"
  local seed="$5"
  local key_zipf_s="$6"
  local value_zipf_s="$7"
  local commit_batch_size="$8"
  local num_sources="$9"
  local risc0_dev_mode="${10}"

  local ts log_path dev_suffix
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  dev_suffix=""
  [[ "$risc0_dev_mode" == "1" ]] && dev_suffix="_dev"
  log_path="${LOG_DIR}/risc0_aggregator_${MODE}_ns${num_sources}_series${series}_sps${sps}_cbs${commit_batch_size}_t${threads}_k${key_zipf_s}_v${value_zipf_s}_r${rep}${dev_suffix}.log"

  cmd=(env
    CARGO_INCREMENTAL=0
    RISC0_DEV_MODE="$risc0_dev_mode"
    SAMPLES_HT_BUCKETS="$SAMPLES_HT_BUCKETS"
    SAMPLES_HT_BUCKET_CAP="$SAMPLES_HT_BUCKET_CAP"
    HISTOGRAM_SLOTS="$HISTOGRAM_SLOTS"
    CM_TOPK_SLOTS="$CM_TOPK_SLOTS"
    KEY_ZIPF_S="$key_zipf_s"
    VALUE_ZIPF_S="$value_zipf_s"
    cargo run -p aggregator --bin aggregator --release -- --bench
    --mode "$MODE"
    --num-sources "$num_sources"
    --series "$series"
    --samples-per-series "$sps"
    --commit-batch-size "$commit_batch_size"
    --seed "$seed"
    --threads "$threads"
  )
  if [[ "$SKIP_VERIFY" == "1" ]]; then
    cmd+=("--skip-verify")
  fi

  {
    printf 'cmd='
    printf '%q ' "${cmd[@]}"
    printf '\n'
  } >"$log_path"

  run_cmd() {
    if command -v /usr/bin/time >/dev/null 2>&1; then
      /usr/bin/time -f 'time_max_rss_kb=%M' "${cmd[@]}"
    else
      "${cmd[@]}"
    fi
  }

  run_cmd 2>&1 \
    | tee -a "$log_path" \
    | awk -v out="$OUT" -v ts="$ts" -v log_path="$log_path" \
        -v mode="$MODE" -v num_sources="$num_sources" -v series="$series" -v sps="$sps" -v commit_batch_size="$commit_batch_size" -v threads="$threads" \
        -v rep="$rep" -v seed="$seed" -v key_zipf_s="$key_zipf_s" -v value_zipf_s="$value_zipf_s" -v skip_verify="$SKIP_VERIFY" -v risc0_dev_mode="$risc0_dev_mode" \
        'BEGIN {
           component="risc0_aggregator";
         }
         match($0, /^([A-Za-z0-9_]+)=(.*)$/, m) {
           k=m[1]; v=m[2];
           gsub(/^[ \t]+|[ \t]+$/, "", v);
           kv[k]=v;
         }
         END {
           input_bytes=kv["input_bytes"]+0;
           input_kb_s=(input_bytes>0 ? sprintf("%.3f", input_bytes/1024.0) : "");
           proc_hwm_kb=kv["proc_hwm_kb"]+0;
           proc_hwm_mb_s=(proc_hwm_kb>0 ? sprintf("%.3f", proc_hwm_kb/1024.0) : "");
           time_max_rss_kb=kv["time_max_rss_kb"]+0;
           time_max_rss_mb_s=(time_max_rss_kb>0 ? sprintf("%.3f", time_max_rss_kb/1024.0) : "");
           proof_bytes_last=kv["proof_bytes_last"]+0;
           proof_kb_last_s=(proof_bytes_last>0 ? sprintf("%.3f", proof_bytes_last/1024.0) : "");
           proof_bytes_max=kv["proof_bytes_max"]+0;
           proof_kb_max_s=(proof_bytes_max>0 ? sprintf("%.3f", proof_bytes_max/1024.0) : "");
           journal_bytes_last=kv["journal_bytes_last"]+0;
           journal_kb_last_s=(journal_bytes_last>0 ? sprintf("%.3f", journal_bytes_last/1024.0) : "");

           printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", ts, component, mode, num_sources, series, sps, commit_batch_size, kv["epoch_events"], kv["input_bytes"], input_kb_s, threads, rep, seed, key_zipf_s, value_zipf_s, skip_verify, risc0_dev_mode >> out;
           printf ",%s,%s", kv["prove_ms_total"], kv["verify_ms_total"] >> out;
           printf ",%s,%s,%s,%s,%s", kv["proc_rss_kb_end"], kv["proc_hwm_kb"], proc_hwm_mb_s, kv["time_max_rss_kb"], time_max_rss_mb_s >> out;
           printf ",%s,%s,%s,%s,%s,%s,%s,%s,%s", kv["proof_bytes_last"], proof_kb_last_s, kv["proof_bytes_max"], proof_kb_max_s, kv["journal_bytes_last"], journal_kb_last_s, kv["events_commit_hex"], kv["out_commit_hex"], log_path >> out;
           printf "\n" >> out;
         }'
}

rep=0
while [[ "$rep" -lt "$REPEATS" ]]; do
  for key_zipf_s in $KEY_ZIPF_S_LIST; do
    for value_zipf_s in $VALUE_ZIPF_S_LIST; do
      for series in $SERIES_LIST; do
        for sps in $SPS_LIST; do
          seed="$((SEED_BASE + rep))"
          run_one "$series" "$sps" "$THREADS" "$rep" "$seed" "$key_zipf_s" "$value_zipf_s" "$COMMIT_BATCH_SIZE" "$NUM_SOURCES" "$RISC0_DEV_MODE"
          echo "ok mode=${MODE} num_sources=${NUM_SOURCES} series=${series} sps=${sps} commit_batch_size=${COMMIT_BATCH_SIZE} key_zipf_s=${key_zipf_s} value_zipf_s=${value_zipf_s} threads=${THREADS} risc0_dev_mode=${RISC0_DEV_MODE} rep=${rep}"
        done
      done
    done
  done
  rep="$((rep + 1))"
done

echo "Wrote CSV: $OUT"
