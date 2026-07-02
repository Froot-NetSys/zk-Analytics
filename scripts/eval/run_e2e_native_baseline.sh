#!/usr/bin/env bash
set -euo pipefail

# Non-ZK NATIVE end-to-end baseline on the REAL Figure-4 datasets.
#
# Runs the SAME aggregation analytics as the zkVM guests, natively (no proof),
# over the real Google-cluster and CAIDA traces, with NO data-source hash
# commitment (the non-ZK baseline does not need the commitment chain). Reports
# native aggregation runtime + peak RSS, paired with the paper's measured zkVM
# end-to-end numbers (§7.1 / Fig. 4).
#
# Prereqs:
#   - Google: testdata/google_cluster_data/input/*.csv  (unzip input.zip)
#   - CAIDA:  testdata/caida_pcap/caida_txt/*.txt        (see scripts/setup/prep_caida.sh)
#
# Env:
#   THREADS (default 56)  TOTAL_LOGS (default 131072)  EPOCH_LOGS (default 16384)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

export LIBCLANG_PATH="${LIBCLANG_PATH:-/usr/lib/llvm-14/lib}"
source "$ROOT_DIR/scripts/lib/common.sh"
THREADS="${THREADS:-56}"
EPOCH_LOGS="${EPOCH_LOGS:-16384}"
TOTAL_LOGS="${TOTAL_LOGS:-131072}"
EPOCHS=$(( TOTAL_LOGS / EPOCH_LOGS ))
SERIES="${SERIES:-128}"
SPS="${SPS:-128}"   # SERIES*SPS == EPOCH_LOGS
SEED="${SEED:-0xA66A1E}"

OUT="$ROOT_DIR/results/non_zk_e2e_baseline.csv"
echo "dataset,mode,bench_input,epochs,epoch_logs,total_logs,native_ms_total,native_rss_mb,zkvm_dev_exec_ms,zk_agg_proofgen_s,zk_query_proofgen_s,slowdown_native_vs_proof,zk_provenance" > "$OUT"

echo "[e2e] building host..."
cargo build -p aggregator --bin aggregator --release >/dev/null

# Run the host --bench once; PASS1=native (--native, no zkVM), PASS2=dev mode
# (RISC0_DEV_MODE=1, guest executed, no proof). Returns parsed metrics via globals.
host_pass() {  # mode binput extra_env native|dev  -> echoes "value_ms rss_kb epoch_events"
  local mode="$1" binput="$2" extra="$3" kind="$4"
  local log="$ROOT_DIR/results/_e2e_${kind}_${mode}_${binput}.log"
  local devenv="" flag=""
  if [ "$kind" = "native" ]; then flag="--native"; else devenv="RISC0_DEV_MODE=1"; fi
  # shellcheck disable=SC2086
  /usr/bin/time -v env BENCH_INPUT="$binput" RAYON_NUM_THREADS="$THREADS" $devenv \
    SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100 \
    $extra \
    ./target/release/aggregator --bench $flag \
      --mode "$mode" --epochs "$EPOCHS" --series "$SERIES" \
      --samples-per-series "$SPS" --seed "$SEED" --threads "$THREADS" \
    > "$log" 2>&1 || { echo "FAILED FAILED FAILED"; return 0; }
  local val ee rss
  if [ "$kind" = "native" ]; then
    val=$(grep -oE '^native_ms_total=[0-9.]+' "$log" | head -1 | cut -d= -f2 || true)
  else
    val=$(grep -oE '^prove_ms_total=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  fi
  ee=$(grep -oE '^epoch_events=[0-9]+' "$log" | head -1 | cut -d= -f2 || true)
  rss=$(grep -oE 'Maximum resident set size \(kbytes\): [0-9]+' "$log" | grep -oE '[0-9]+$' | head -1 || true)
  echo "${val:-} ${rss:-} ${ee:-$EPOCH_LOGS}"
}

run_native() {  # dataset mode bench_input extra_env zk_agg_s zk_q_s prov
  local dataset="$1" mode="$2" binput="$3" extra="$4" zk_agg="$5" zk_q="$6" prov="$7"
  echo "[e2e] $dataset mode=$mode ($binput): native pass ..."
  read -r nat nrss ee <<< "$(host_pass "$mode" "$binput" "$extra" native)"
  echo "[e2e] $dataset mode=$mode ($binput): dev-mode pass ..."
  read -r dev drss _ <<< "$(host_pass "$mode" "$binput" "$extra" dev)"
  local rss_mb slow
  rss_mb=$(python3 -c "print(f'{${nrss:-0}/1024:.1f}')")
  slow=$(python3 -c "n=${nat:-0}/1000; print(f'{${zk_agg}/n:.0f}' if n>0 else '')")
  echo "$dataset,$mode,$binput,$EPOCHS,$EPOCH_LOGS,$((EPOCHS*${ee:-EPOCH_LOGS})),${nat:-},${rss_mb},${dev:-},${zk_agg},${zk_q},${slow},$prov" >> "$OUT"
  echo "[e2e] $dataset/$mode native_ms=$nat dev_exec_ms=$dev rss_mb=$rss_mb slowdown=${slow}x"
}

# Synthetic control (Hash Table / Histogram / CMS) — always available.
run_native synthetic samples   synthetic "" 0 0 "synthetic control (no zk anchor)"
run_native synthetic histogram synthetic "" 0 0 "synthetic control (no zk anchor)"
run_native synthetic cm        synthetic "" 0 0 "synthetic control (no zk anchor)"

# Google cluster (hash-table sum) — Fig.4(a): agg 90.8 min, query 524.6 s.
GOOGLE_DIR="$ROOT_DIR/testdata/google_cluster_data/input"
if [[ -d "$GOOGLE_DIR" ]] && compgen -G "$GOOGLE_DIR/*.csv" >/dev/null; then
  run_native google samples tsv \
    "TSV_DIR=$GOOGLE_DIR TSV_MAX_FILES=$SERIES CSV_VALUE_SCALE=1000000 TS_INTERVAL_MS=100" \
    5448 524.6 "paper Fig.4(a) Google cluster (90.8 min agg, 524.6 s query)"
else
  echo "[e2e] SKIP google — no CSVs in $GOOGLE_DIR"
fi

# CAIDA (Count-Min Top-10) — Fig.4(b): agg 227.5 min, query 80.5 s.
CAIDA_DIR="$ROOT_DIR/testdata/caida_pcap/caida_txt"
if [[ -d "$CAIDA_DIR" ]] && compgen -G "$CAIDA_DIR/*.txt" >/dev/null; then
  run_native caida cm caida \
    "CAIDA_DIR=$CAIDA_DIR CAIDA_MAX_FILES=100000" \
    13650 80.5 "paper Fig.4(b) CAIDA (227.5 min agg, 80.5 s query)"
else
  echo "[e2e] SKIP caida — no txt in $CAIDA_DIR (run scripts/setup/prep_caida.sh first)"
fi

echo "[e2e] done -> $OUT"
column -t -s, "$OUT" || cat "$OUT"
