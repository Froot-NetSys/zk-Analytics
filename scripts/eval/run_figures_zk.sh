#!/usr/bin/env bash
set -euo pipefail
# Distributed ZK (zkVM) pipeline for paper Figure 5 and Table 3 through Kafka->RocksDB->
# aggregator->FDB->querier pipeline WITH real proving + verification. Reports,
# per cell: prove time, verify time, memory (host + r0vm prover), proof size,
# public output (journal bytes). The default is the full 12-cell matrix at an
# epoch size of 16,384 logs; proving each epoch can take hours.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh"
# Must match the driver's metrics path (run_distributed_baseline.sh writes here).
MET="$ROOT_DIR/results/_dist_metrics.jsonl"
nodes_for(){ local n="$1" out=""; for ((i=0;i<n;i++)); do out="$out node$i"; done; echo "${out# }"; }

emit(){ local csv="$1" var="$2" mode="$3"
  python3 - "$MET" "$var" "$mode" >> "$csv" <<'PY'
import sys,json
met,var,mode=sys.argv[1],sys.argv[2],sys.argv[3]
recs=[json.loads(l) for l in open(met) if l.strip()]
aggs=[r for r in recs if r.get('task')=='aggregation']
queries=[r for r in recs if r.get('task')=='query']
if len(aggs) != 1 or len(queries) != 1:
    raise SystemExit(f"expected one aggregation and one query metric, got {len(aggs)} and {len(queries)}")
agg, q = aggs[0], queries[0]
if (agg.get('epochs_processed', 0) < 1 or agg.get('total_time_s', 0) <= 0
        or agg.get('proof_bytes_per_epoch', 0) <= 0):
    raise SystemExit(f"invalid ZK aggregation metrics: {agg}")
c=agg.get('components_s',{}); qc=q.get('components_s',{})
def g(d,k): return round(d.get(k,0.0),6)
print(",".join(str(x) for x in [var,mode,
  # aggregation: time + prove/verify + storage
  round(agg['total_time_s'],3), g(c,'prove'), g(c,'verify'),
  g(c,'kafka_recv'), g(c,'rocksdb_raw_insert'), g(c,'fdb_write'),
  # aggregation memory (MB): per-node host, per-node r0vm prover, cluster total
  round(agg.get('per_node_host_rss_mb',0.0),1), round(agg.get('per_node_prover_rss_mb',0.0),1),
  round(agg.get('peak_rss_mb',0.0),1),
  # proof size & public output (bytes/epoch)
  agg.get('proof_bytes_per_epoch',0), agg.get('journal_bytes_per_epoch',0),
  # query: time + prove/verify + memory
  round(q['total_time_s'],3), g(qc,'query_compute'), g(qc,'verify'), g(qc,'fdb_lookup'),
  round(q.get('host_peak_rss_mb',0.0),1), round(q.get('prover_peak_rss_mb',0.0),1)]))
PY
}

run_cell(){
  local log="$ROOT_DIR/results/_dist_driver.log"
  if ! env "$@" MODE=zk bash "$DRV" 2>&1 | tee "$log"; then
    echo "  cell FAILED: $*" >&2
    tail -100 "$log" >&2
    return 1
  fi
}

HDR="var,mode,agg_total_s,prove_s,verify_s,kafka_recv_s,rocksdb_raw_insert_s,fdb_write_s,agg_host_rss_mb,agg_prover_rss_mb,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s,query_fdb_lookup_s,query_host_rss_mb,query_prover_rss_mb"

if [ "${FIG:-5}" = 5 ]; then
  echo "=== Figure 5 ZK: distributed aggregation (vary aggregators) ==="
  C="$ROOT_DIR/results/fig5_zk.csv"; echo "$HDR" > "$C"
  # Full measured scaling matrix. Override FIG5_SPECS for a reduced smoke test.
  for spec in ${FIG5_SPECS:-samples:1 samples:2 samples:4 samples:8 histogram:1 histogram:2 histogram:4 histogram:8 cm:1 cm:2 cm:4 cm:8}; do
    IFS=: read -r mode N <<< "$spec"
    echo "[fig5-zk] mode=$mode N=$N"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=4096 TOTAL_LOGS=131072 NODES="$(nodes_for $N)"
    emit "$C" "$N" "$mode"
    cat "$C" | tail -1
  done
  echo "[fig5-zk] -> $C"
fi
echo "[fig5-table3-zk] done"
