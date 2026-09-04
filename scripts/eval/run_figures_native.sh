#!/usr/bin/env bash
set -euo pipefail
# Non-ZK NATIVE baselines for paper Figures 5, 6, 7, with RocksDB/FDB
# insert+read breakdown. All synthetic, through the real Kafka->RocksDB->
# aggregator->FDB->querier pipeline (NO_ZKVM_PROOF=1), so storage times are real.
#   Fig 5: distributed aggregation, N=1/2/4/8 aggregators, 3 modes,
#          4096 keys / 131072 logs / epoch 16384 / batch 8.
#   Fig 6: single machine (node0), vary distinct keys/epoch, 3 modes,
#          1 epoch of 16384 logs.
#   Fig 7: query, vary #queried epochs 1..256 (separate script section).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT_DIR/results/_dist_metrics.jsonl"
N8="node0 node1 node2 node3 node4 node5 node6 node7"
nodes_for(){ local n="$1" out=""; for ((i=0;i<n;i++)); do out="$out node$i"; done; echo "${out# }"; }

# extract_row var mode -> append "var,mode,<agg breakdown>,<query breakdown>" to $2csv
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
if agg.get('epochs_processed', 0) < 1 or agg.get('total_time_s', 0) <= 0:
    raise SystemExit(f"invalid aggregation metrics: {agg}")
c=agg.get('components_s',{}); qc=q.get('components_s',{})
def g(d,k): return round(d.get(k,0.0),6)
print(",".join(str(x) for x in [var,mode,
  round(agg['total_time_s'],6), g(c,'kafka_recv'), g(c,'rocksdb_raw_insert'),
  g(c,'rocksdb_raw_read'), g(c,'aggr_compute'), g(c,'fdb_write'),
  round(q['total_time_s'],6), g(qc,'fdb_lookup'), g(qc,'deserialize'), g(qc,'query_compute'),
  # memory (MB): per-node aggregator host RSS, cluster host sum, prover sum, query RSS
  round(agg.get('per_node_host_rss_mb',0.0),2), round(agg.get('host_peak_rss_mb',0.0),2),
  round(agg.get('prover_peak_rss_mb',0.0),2), round(q.get('peak_rss_mb',0.0),2),
  round(q.get('prover_peak_rss_mb',0.0),2)]))
PY
}

run_cell(){ # dataset-args... ; runs driver, returns
  local log="$ROOT_DIR/results/_dist_driver.log"
  if ! env "$@" MODE=native bash "$DRV" >"$log" 2>&1; then
    echo "  cell FAILED: $*" >&2
    tail -100 "$log" >&2
    return 1
  fi
}

HDR="var,mode,agg_total_s,kafka_recv_s,rocksdb_raw_insert_s,rocksdb_raw_read_s,aggr_compute_s,fdb_write_s,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s,agg_per_node_host_rss_mb,agg_cluster_host_rss_mb,agg_prover_rss_mb,query_rss_mb,query_prover_rss_mb"

if [ "${FIG:-all}" = 5 ] || [ "${FIG:-all}" = all ]; then
  echo "=== Figure 5: distributed native aggregation (vary aggregators) ==="
  C="$ROOT_DIR/results/fig5_native.csv"; echo "$HDR" > "$C"
  for spec in ${FIG5_SPECS:-samples:1 samples:2 samples:4 samples:8 histogram:1 histogram:2 histogram:4 histogram:8 cm:1 cm:2 cm:4 cm:8}; do
    IFS=: read -r mode N <<< "$spec"
    echo "[fig5] mode=$mode N=$N"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=4096 TOTAL_LOGS=131072 NODES="$(nodes_for $N)"
    emit "$C" "$N" "$mode"
  done
  echo "[fig5] -> $C"
fi

if [ "${FIG:-all}" = 6 ] || [ "${FIG:-all}" = all ]; then
  echo "=== Figure 6: single-machine native aggregation (vary keys/epoch) ==="
  C="$ROOT_DIR/results/fig6_native.csv"; echo "$HDR" > "$C"
  for mode in ${FIG6_MODES:-samples histogram cm}; do
    for keys in ${FIG6_KEYS:-256 512 1024 2048 4096}; do
    echo "[fig6] mode=$mode keys=$keys"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=$keys TOTAL_LOGS=16384 NODES="node0"
    emit "$C" "$keys" "$mode"
  done; done
  echo "[fig6] -> $C"
fi
echo "[figs] done"
