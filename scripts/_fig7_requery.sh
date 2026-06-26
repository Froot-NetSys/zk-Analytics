#!/usr/bin/env bash
set -uo pipefail
# Re-run ONLY the Fig 7 query sweep against the already-aggregated FDB
# subspaces (zktel_fig7_{samples,histogram,cm}); no re-produce/re-aggregate.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT_DIR"
B="$ROOT_DIR/target/release"; FC="$HOME/zktel-dist/fdb.cluster"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
COUNTS="${COUNTS:-1 2 4 8 16 32 64 128 256}"; QPORT="${QUERIER_PORT:-8121}"
OUT="$ROOT_DIR/results/fig7_native.csv"
echo "epoch_type,query,queried_epochs,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s" > "$OUT"
for cfg in samples:samples_sum cm:cm_topk histogram:histogram_p90; do
  IFS=: read -r et query <<< "$cfg"
  SUB="zktel_fig7_${et}"
  pkill -9 -x querier 2>/dev/null; sleep 1
  QUERY_NO_PROVE=1 BENCH_PRINT=1 FDB_CLUSTER_FILE=$FC FDB_SUBSPACE=$SUB \
    HTTP_LISTEN=0.0.0.0:$QPORT "$B/querier" > /tmp/rq_${et}.log 2>&1 & QPID=$!
  for _ in $(seq 1 60); do curl -s -o /dev/null -m 2 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":1}" && break; sleep 1; done
  for ec in $COUNTS; do
    : > /tmp/rq_${et}.log
    for rep in 1 2 3 4 5; do curl -s -m 60 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":$ec}" >/dev/null 2>&1; done
    # median-ish: take the last (warm) db_ms
    db=$(grep -aoE 'db_ms=[0-9]+' /tmp/rq_${et}.log | tail -1 | cut -d= -f2)
    mg=$(grep -aoE 'merge_ms=[0-9]+' /tmp/rq_${et}.log | tail -1 | cut -d= -f2)
    pr=$(grep -aoE 'prove_ms=[0-9]+' /tmp/rq_${et}.log | tail -1 | cut -d= -f2)
    db=${db:-0}; mg=${mg:-0}; pr=${pr:-0}
    tot=$(python3 -c "print(f'{($db+$mg+$pr)/1000:.6f}')")
    echo "$et,$query,$ec,$tot,$(python3 -c "print($db/1000)"),$(python3 -c "print($mg/1000)"),$(python3 -c "print($pr/1000)")" >> "$OUT"
    echo "[rq] $et epochs=$ec db=${db}ms merge=${mg}ms prove=${pr}ms"
  done
  kill -TERM $QPID 2>/dev/null; wait $QPID 2>/dev/null
done
pkill -9 -x querier 2>/dev/null
echo "[rq] -> $OUT"
