#!/usr/bin/env bash
set -uo pipefail
# Measure query-ZK peak memory (host + r0vm prover) over the Fig 7 cm subspace.
# Confirms whether the paper's Fig 7 query-ZK memory omitted the prover.
ROOT=/mydata/zk-Analytics; cd "$ROOT"
B="$ROOT/target/release"; FC="$HOME/zktel-dist/fdb.cluster"; QPORT=8131
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
EC="${EC:-8}"; SUB="${SUB:-zktel_fig7_cm}"; QTYPE="${QTYPE:-cm_topk}"
pkill -9 -x querier 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null; pkill -9 -x r0vm 2>/dev/null; sleep 1
# querier WITH proving (no QUERY_NO_PROVE)
E2E_TIMING=1 BENCH_PRINT=1 FDB_CLUSTER_FILE=$FC FDB_SUBSPACE=$SUB \
  HTTP_LISTEN=0.0.0.0:$QPORT "$B/querier" > /tmp/qzk_${QTYPE}.log 2>&1 & QPID=$!
python3 scripts/mem_trace.py --out /tmp/qzk_${QTYPE}_mem.csv --summary /tmp/qzk_${QTYPE}_mem.json \
  --match querier --match r0vm --interval 0.2 > /dev/null 2>&1 & MPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null -m 2 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$QTYPE\",\"epochs\":1}" && break; sleep 1; done
echo "[qzk] querier up; proving query $QTYPE epochs=$EC ..."
RESP=$(curl -s -m 1800 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$QTYPE\",\"epochs\":$EC}")
echo "[qzk] response head: $(echo "$RESP" | head -c 200)"
sleep 1; kill -INT $MPID 2>/dev/null; sleep 2
echo "[qzk] === bench / proof ==="; grep -aE "bench kind=risc0|failed|prove_ms" /tmp/qzk_${QTYPE}.log | tail -3
echo "[qzk] === peak RSS (host + prover) ==="
python3 - /tmp/qzk_${QTYPE}_mem.json <<'PY'
import json,sys
s=json.load(open(sys.argv[1])); host=prover=0.0
for _,p in (s.get("per_process_peak_rss_mb") or {}).items():
    nm=(p.get("name") or "").lower(); v=p.get("peak_rss_mb",0.0)
    if "r0vm" in nm: prover=max(prover,v)
    else: host=max(host,v)
print(f"host={host:.1f}MB prover={prover:.1f}MB total={(host+prover)/1000:.2f}GB")
PY
kill -TERM $QPID 2>/dev/null; pkill -9 -x r0vm 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null
echo "[qzk] done"
