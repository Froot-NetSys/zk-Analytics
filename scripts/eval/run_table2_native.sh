#!/usr/bin/env bash
set -euo pipefail
# Vanilla (non-ZK) Table 2 baseline with the real storage pipeline:
# Kafka -> RocksDB -> one native aggregator -> FoundationDB -> native query.
# The default sweep measures three aggregation modes and five exact key
# cardinalities over one 16,384-event epoch.  It is not Figure 6: Figure 6 is
# the standalone zkVM aggregator benchmark.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT_DIR/results/_dist_metrics.jsonl"

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
  # Keep orchestration progress visible.  Previously all output was hidden in
  # this file, which made Kafka/FDB waits look like a hung experiment.
  if ! env "$@" MODE=native bash "$DRV" 2>&1 | tee "$log"; then
    echo "  cell FAILED: $*" >&2
    tail -100 "$log" >&2
    return 1
  fi
}

HDR="var,mode,agg_total_s,kafka_recv_s,rocksdb_raw_insert_s,rocksdb_raw_read_s,aggr_compute_s,fdb_write_s,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s,agg_per_node_host_rss_mb,agg_cluster_host_rss_mb,agg_prover_rss_mb,query_rss_mb,query_prover_rss_mb"

echo "=== Table 2: single-machine vanilla pipeline (vary keys/epoch) ==="
C="$ROOT_DIR/results/table2_native.csv"; echo "$HDR" > "$C"
for mode in ${TABLE2_MODES:-samples histogram cm}; do
  for keys in ${TABLE2_KEYS:-256 512 1024 2048 4096}; do
    echo "[table2] mode=$mode keys=$keys"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=$keys TOTAL_LOGS=16384 \
      MEM_INTERVAL="${TABLE2_MEM_INTERVAL:-0.01}" NODES="node0"
    emit "$C" "$keys" "$mode"
  done
done
echo "[table2] -> $C"
