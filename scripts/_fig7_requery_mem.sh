#!/usr/bin/env bash
set -uo pipefail
# Fig 7 native QUERY memory: re-query the already-aggregated FDB subspaces
# (zktel_fig7_{samples,histogram,cm}) and capture querier peak RSS per epoch
# count via mem_trace.py. Native (QUERY_NO_PROVE=1) -> no prover. Also re-emits
# the timing so the output CSV carries time + memory together.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT_DIR"
B="$ROOT_DIR/target/release"; FC="$HOME/zktel-dist/fdb.cluster"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
COUNTS="${COUNTS:-1 2 4 8 16 32 64 128 256}"; QPORT="${QUERIER_PORT:-8123}"
OUT="$ROOT_DIR/results/fig7_native.csv"
echo "epoch_type,query,queried_epochs,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s,query_rss_mb,query_prover_rss_mb" > "$OUT"
peak_rss(){ python3 - "$1" <<'PY'
import sys,json
try:
    s=json.load(open(sys.argv[1])); h=0.0
    for _,p in (s.get("per_process_peak_rss_mb") or {}).items():
        if "r0vm" not in (p.get("name") or "").lower(): h=max(h,p.get("peak_rss_mb",0.0))
    print(f"{h:.2f}")
except Exception: print("0.0")
PY
}
for cfg in samples:samples_sum cm:cm_topk histogram:histogram_p90; do
  IFS=: read -r et query <<< "$cfg"; SUB="zktel_fig7_${et}"
  for ec in $COUNTS; do
    pkill -9 -x querier 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null; sleep 1
    QLOG=/tmp/rqm_${et}_${ec}.log; MJSON=/tmp/rqm_${et}_${ec}.json
    QUERY_NO_PROVE=1 BENCH_PRINT=1 FDB_CLUSTER_FILE=$FC FDB_SUBSPACE=$SUB \
      HTTP_LISTEN=0.0.0.0:$QPORT "$B/querier" > "$QLOG" 2>&1 & QPID=$!
    python3 scripts/mem_trace.py --out /tmp/rqm_${et}_${ec}.csv --summary "$MJSON" \
      --match querier --match r0vm --interval 0.1 > /dev/null 2>&1 & MPID=$!
    for _ in $(seq 1 60); do curl -s -o /dev/null -m 2 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":1}" && break; sleep 1; done
    : > "$QLOG"
    for rep in 1 2 3 4 5; do curl -s -m 60 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":$ec}" >/dev/null 2>&1; done
    db=$(grep -aoE 'db_ms=[0-9]+' "$QLOG" | tail -1 | cut -d= -f2); mg=$(grep -aoE 'merge_ms=[0-9]+' "$QLOG" | tail -1 | cut -d= -f2)
    pr=$(grep -aoE 'prove_ms=[0-9]+' "$QLOG" | tail -1 | cut -d= -f2)
    db=${db:-0}; mg=${mg:-0}; pr=${pr:-0}
    kill -INT $MPID 2>/dev/null; sleep 1; kill -TERM $QPID 2>/dev/null; wait $QPID 2>/dev/null
    rss=$(peak_rss "$MJSON")
    tot=$(python3 -c "print(f'{($db+$mg+$pr)/1000:.6f}')")
    echo "$et,$query,$ec,$tot,$(python3 -c "print($db/1000)"),$(python3 -c "print($mg/1000)"),$(python3 -c "print($pr/1000)"),$rss,0.0" >> "$OUT"
    echo "[rqm] $et ec=$ec db=${db}ms merge=${mg}ms rss=${rss}MB"
  done
done
pkill -9 -x querier 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null
echo "[rqm] -> $OUT"
