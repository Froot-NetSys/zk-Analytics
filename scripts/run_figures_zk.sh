#!/usr/bin/env bash
set -uo pipefail
# ZK (zkVM) baselines for paper Figures 5/6/7 through the real Kafka->RocksDB->
# aggregator->FDB->querier pipeline WITH real proving + verification. Reports,
# per cell: prove time, verify time, memory (host + r0vm prover), proof size,
# public output (journal bytes). Representative subset, full epoch size (16384
# logs/epoch) -- proving is ~28 min (histogram) / ~86 min (samples) / ~185 min
# (CM) per epoch, so cells are chosen deliberately (see FIG blocks).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/run_distributed_baseline.sh"
# Must match the driver's metrics path (run_distributed_baseline.sh writes here).
MET="$ROOT_DIR/results/_dist_metrics.jsonl"
nodes_for(){ local n="$1" out=""; for ((i=0;i<n;i++)); do out="$out node$i"; done; echo "${out# }"; }

emit(){ local csv="$1" var="$2" mode="$3"
  python3 - "$MET" "$var" "$mode" >> "$csv" <<'PY'
import sys,json
met,var,mode=sys.argv[1],sys.argv[2],sys.argv[3]
recs=[json.loads(l) for l in open(met) if l.strip()]
agg=[r for r in recs if r['task']=='aggregation'][-1]
q=[r for r in recs if r['task']=='query'][-1]
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

run_cell(){ env "$@" MODE=zk bash "$DRV" >/dev/null 2>&1 || echo "  cell FAILED: $*"; }

HDR="var,mode,agg_total_s,prove_s,verify_s,kafka_recv_s,rocksdb_raw_insert_s,fdb_write_s,agg_host_rss_mb,agg_prover_rss_mb,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s,query_fdb_lookup_s,query_host_rss_mb,query_prover_rss_mb"

KEYS="${SYNTH_KEYS:-1024}"

if [ "${FIG:-6}" = 6 ]; then
  echo "=== Figure 6 ZK: single-machine aggregation (1 epoch, 3 modes) ==="
  C="$ROOT_DIR/results/fig6_zk.csv"; echo "$HDR" > "$C"
  for mode in histogram samples cm; do
    echo "[fig6-zk] mode=$mode keys=$KEYS"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=$KEYS TOTAL_LOGS=16384 NODES="node0"
    emit "$C" "$KEYS" "$mode"
    cat "$C" | tail -1
  done
  echo "[fig6-zk] -> $C"
fi

if [ "${FIG:-6}" = 5 ]; then
  echo "=== Figure 5 ZK: distributed aggregation (vary aggregators) ==="
  C="$ROOT_DIR/results/fig5_zk.csv"; echo "$HDR" > "$C"
  # NS = space-separated N values; MODES = modes to run. Defaults chosen for
  # tractability: histogram (cheapest) across N=1,8; samples/cm at N=8 only.
  for spec in ${FIG5_SPECS:-"histogram:1" "histogram:8" "samples:8" "cm:8"}; do
    IFS=: read -r mode N <<< "$spec"
    echo "[fig5-zk] mode=$mode N=$N"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=4096 TOTAL_LOGS=131072 NODES="$(nodes_for $N)"
    emit "$C" "$N" "$mode"
    cat "$C" | tail -1
  done
  echo "[fig5-zk] -> $C"
fi
echo "[figs-zk] done"
