#!/usr/bin/env bash
set -euo pipefail

# Bench RISC0 aggregator on a fixed epoch shape (SERIES=128, SAMPLES_PER_SERIES=128 by default),
# optionally replaying Google cluster TSV/CSV or CAIDA txt inputs.
#
# Usage:
#   cd zk-Analytics
#   ./scripts/bench_google_cm_epoch_128.sh
#   ./scripts/bench_google_histogram_epoch_128.sh
#   ./scripts/bench_google_samples_epoch_128.sh
#   ./scripts/bench_caida_cm_epoch_128.sh
#   ./scripts/bench_caida_histogram_epoch_128.sh
#   ./scripts/bench_caida_samples_epoch_128.sh
#
# Options (env vars):
#   MODE               Aggregator mode: "samples" (default), "histogram", or "cm"
#   BENCH_INPUT         "synthetic" (default), "tsv" (google_cluster), or "caida"
#   DATA_TYPE           Label for CSV/log naming (default: "synthetic")
#   EPOCHS              Number of sequential epochs/proofs (default: "1")
#   SERIES              Key/modulus for synthetic, and default TSV_MAX_FILES (default: "128")
#   SAMPLES_PER_SERIES  Samples per series (default: "128")
#   THREADS             Rayon threads (default: all cores)
#   REPEATS             Repeats (default: "1")
#   SEED                Base seed (default: "0xA66A1E") - actual seed = SEED + rep
#   SKIP_VERIFY         "0" (default) or "1"
#   SAMPLES_HT_BUCKETS     Compile-time override (default: "64")
#   SAMPLES_HT_BUCKET_CAP  Compile-time override (default: "4")
#   HISTOGRAM_SLOTS        Compile-time override (default: "32")
#   CM_TOPK_SLOTS          Compile-time override (default: "100")
#   TSV_DIR             Google cluster input dir (default: "../testdata/google_cluster_data/input")
#   TSV_MAX_FILES       Max input files to open (default: SERIES)
#   TS_INTERVAL_MS      Synthetic interval per event for TSV replay (default: "100")
#   CSV_VALUE_SCALE     Scale first CSV column before rounding (default: "1000000")
#   CAIDA_DIR           CAIDA txt dir (default: "../testdata/caida_pcap/caida_txt")
#   CAIDA_MAX_FILES     Max CAIDA txt files to open (default: "64")
#   OUT_NAME            Output CSV filename (default: derived)
#   CSV_DIR             Directory for CSV outputs (default: "bench_csv")
#   LOG_DIR             Directory for raw logs (default: "bench_logs")

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Matches `risc0/.cargo/config.toml` (rustc temp dir). Avoids "couldn't create a temp dir".
mkdir -p target/tmp

MODE="${MODE:-samples}"
BENCH_INPUT="${BENCH_INPUT:-synthetic}"
DATA_TYPE="${DATA_TYPE:-synthetic}"
EPOCHS="${EPOCHS:-1}"
SERIES="${SERIES:-128}"
SAMPLES_PER_SERIES="${SAMPLES_PER_SERIES:-128}"
THREADS="${THREADS:-$(nproc)}"
REPEATS="${REPEATS:-1}"
SEED_BASE="${SEED:-0xA66A1E}"
SKIP_VERIFY="${SKIP_VERIFY:-0}"

SAMPLES_HT_BUCKETS="${SAMPLES_HT_BUCKETS:-64}"
SAMPLES_HT_BUCKET_CAP="${SAMPLES_HT_BUCKET_CAP:-4}"
HISTOGRAM_SLOTS="${HISTOGRAM_SLOTS:-32}"
CM_TOPK_SLOTS="${CM_TOPK_SLOTS:-100}"

TSV_DIR="${TSV_DIR:-../testdata/google_cluster_data/input}"
TSV_MAX_FILES="${TSV_MAX_FILES:-$SERIES}"
TS_INTERVAL_MS="${TS_INTERVAL_MS:-100}"
CSV_VALUE_SCALE="${CSV_VALUE_SCALE:-1000000}"

CAIDA_DIR="${CAIDA_DIR:-../testdata/caida_pcap/caida_txt}"
CAIDA_MAX_FILES="${CAIDA_MAX_FILES:-64}"

CSV_DIR="${CSV_DIR:-bench_csv}"
LOG_DIR="${LOG_DIR:-bench_logs}"
OUT_NAME="${OUT_NAME:-bench_risc0_aggregator_${MODE}_${DATA_TYPE}_e${EPOCHS}_s${SERIES}_n${SAMPLES_PER_SERIES}_t${THREADS}.csv}"
OUT="${CSV_DIR}/${OUT_NAME}"

mkdir -p "$CSV_DIR" "$LOG_DIR"

CSV_HEADER="ts,component,mode,bench_input,data_type,epochs,series,samples_per_series,epoch_events,threads,rep,seed,skip_verify,input_bytes,input_kb,total_input_bytes,prove_ms_total,prove_ms_per_epoch,verify_ms_total,verify_ms_per_epoch,proc_rss_kb_end,proc_hwm_kb,time_max_rss_kb,proof_bytes_last,proof_kb_last,proof_bytes_max,proof_kb_max,journal_bytes_last,journal_kb_last,events_commit_hex,out_commit_hex,log_path"
if [[ ! -s "$OUT" ]]; then
  echo "$CSV_HEADER" >"$OUT"
fi

run_one() {
  local rep="$1"
  local seed="$2"

  local ts log_path
  ts="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  log_path="${LOG_DIR}/risc0_aggregator_${MODE}_${DATA_TYPE}_e${EPOCHS}_s${SERIES}_n${SAMPLES_PER_SERIES}_t${THREADS}_r${rep}.log"

  cmd=(env
    CARGO_INCREMENTAL=0
    BENCH_INPUT="$BENCH_INPUT"
    TSV_DIR="$TSV_DIR"
    TSV_MAX_FILES="$TSV_MAX_FILES"
    TS_INTERVAL_MS="$TS_INTERVAL_MS"
    CSV_VALUE_SCALE="$CSV_VALUE_SCALE"
    CAIDA_DIR="$CAIDA_DIR"
    CAIDA_MAX_FILES="$CAIDA_MAX_FILES"
    RAYON_NUM_THREADS="$THREADS"
    SAMPLES_HT_BUCKETS="$SAMPLES_HT_BUCKETS"
    SAMPLES_HT_BUCKET_CAP="$SAMPLES_HT_BUCKET_CAP"
    HISTOGRAM_SLOTS="$HISTOGRAM_SLOTS"
    CM_TOPK_SLOTS="$CM_TOPK_SLOTS"
    cargo run -p aggregator --bin aggregator --release -- --bench
      --mode "$MODE"
      --epochs "$EPOCHS"
      --series "$SERIES"
      --samples-per-series "$SAMPLES_PER_SERIES"
      --seed "$seed"
      --threads "$THREADS"
  )
  if [[ "$SKIP_VERIFY" == "1" ]]; then
    cmd+=("--skip-verify")
  fi
  # Non-ZK native baseline: run the analytics natively (no zkVM proof).
  if [[ "${NATIVE:-0}" == "1" ]]; then
    cmd+=("--native")
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
        -v mode="$MODE" -v bench_input="$BENCH_INPUT" -v data_type="$DATA_TYPE" -v epochs="$EPOCHS" \
        -v series="$SERIES" -v sps="$SAMPLES_PER_SERIES" -v threads="$THREADS" \
        -v rep="$rep" -v seed="$seed" -v skip_verify="$SKIP_VERIFY" \
        'BEGIN { component="risc0_aggregator"; }
         match($0, /^([A-Za-z0-9_]+)=(.*)$/, m) {
           k=m[1]; v=m[2];
           gsub(/^[ \t]+|[ \t]+$/, "", v);
           kv[k]=v;
         }
         END {
           input_bytes=kv["input_bytes"]+0;
           input_kb_s=(input_bytes>0 ? sprintf("%.3f", input_bytes/1024.0) : "");
           total_input_bytes=kv["total_input_bytes"]+0;
           proof_bytes_last=kv["proof_bytes_last"]+0;
           proof_kb_last_s=(proof_bytes_last>0 ? sprintf("%.3f", proof_bytes_last/1024.0) : "");
           proof_bytes_max=kv["proof_bytes_max"]+0;
           proof_kb_max_s=(proof_bytes_max>0 ? sprintf("%.3f", proof_bytes_max/1024.0) : "");
           journal_bytes_last=kv["journal_bytes_last"]+0;
           journal_kb_last_s=(journal_bytes_last>0 ? sprintf("%.3f", journal_bytes_last/1024.0) : "");

           printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s", ts, component, mode, bench_input, data_type, epochs, series, sps, kv["epoch_events"], threads, rep, seed, skip_verify >> out;
           printf ",%s,%s,%s", kv["input_bytes"], input_kb_s, total_input_bytes >> out;
           printf ",%s,%s,%s,%s", kv["prove_ms_total"], kv["prove_ms_per_epoch"], kv["verify_ms_total"], kv["verify_ms_per_epoch"] >> out;
           printf ",%s,%s,%s", kv["proc_rss_kb_end"], kv["proc_hwm_kb"], kv["time_max_rss_kb"] >> out;
           printf ",%s,%s,%s,%s,%s,%s", kv["proof_bytes_last"], proof_kb_last_s, kv["proof_bytes_max"], proof_kb_max_s, kv["journal_bytes_last"], journal_kb_last_s >> out;
           printf ",%s,%s,%s", kv["events_commit_hex"], kv["out_commit_hex"], log_path >> out;
           printf "\n" >> out;
         }'

  echo "ok mode=${MODE} data_type=${DATA_TYPE} epochs=${EPOCHS} series=${SERIES} sps=${SAMPLES_PER_SERIES} threads=${THREADS} rep=${rep} log=${log_path}"
}

rep=0
while [[ "$rep" -lt "$REPEATS" ]]; do
  seed="$((SEED_BASE + rep))"
  run_one "$rep" "$seed"
  rep="$((rep + 1))"
done

echo "Wrote CSV: $OUT"
