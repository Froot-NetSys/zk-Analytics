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
FIG5_MACHINES_VALUE="${FIG5_MACHINES:-node0 node1 node2 node3 node4 node5 node6 node7}"
read -r -a FIG5_MACHINE_ARRAY <<< "$FIG5_MACHINES_VALUE"
declare -A FIG5_MACHINE_SEEN=()
for machine in "${FIG5_MACHINE_ARRAY[@]}"; do
  if [[ -n "${FIG5_MACHINE_SEEN[$machine]:-}" ]]; then
    echo "[fig5-zk] duplicate machine in FIG5_MACHINES: $machine" >&2
    exit 2
  fi
  FIG5_MACHINE_SEEN[$machine]=1
done

# Figure 5 always uses exactly one aggregator process per selected machine.
# A point with N aggregators selects the first N entries from FIG5_MACHINES.
nodes_for() {
  local n="$1"
  if [[ ! "$n" =~ ^[1-9][0-9]*$ ]]; then
    echo "[fig5-zk] invalid aggregator count: $n" >&2
    return 2
  fi
  if (( n > ${#FIG5_MACHINE_ARRAY[@]} )); then
    echo "[fig5-zk] requested $n aggregators but FIG5_MACHINES contains only ${#FIG5_MACHINE_ARRAY[@]} machines" >&2
    return 2
  fi
  printf '%s' "${FIG5_MACHINE_ARRAY[0]}"
  for ((i=1; i<n; i++)); do printf ' %s' "${FIG5_MACHINE_ARRAY[i]}"; done
  printf '\n'
}

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
  # This target is exclusively for real proofs; never inherit dev mode from
  # the caller's shell.
  if ! env "$@" MODE=zk RISC0_DEV_MODE=0 bash "$DRV" 2>&1 | tee "$log"; then
    echo "  cell FAILED: $*" >&2
    tail -100 "$log" >&2
    return 1
  fi
}

HDR="var,mode,agg_total_s,prove_s,verify_s,kafka_recv_s,rocksdb_raw_insert_s,fdb_write_s,agg_host_rss_mb,agg_prover_rss_mb,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s,query_fdb_lookup_s,query_host_rss_mb,query_prover_rss_mb"

if [ "${FIG:-5}" = 5 ]; then
  echo "=== Figure 5 ZK: distributed aggregation (vary aggregators) ==="
  C="$ROOT_DIR/results/fig5_zk.csv"; echo "$HDR" > "$C"
  # Build the full cross product by default. FIG5_SPECS remains an explicit
  # point selector for reduced checks.
  if [[ -n "${FIG5_SPECS:-}" ]]; then
    specs="$FIG5_SPECS"
  else
    specs=""
    for mode in ${FIG5_MODES:-samples histogram cm}; do
      for n in ${FIG5_NUM_AGGREGATORS:-1 2 4 8}; do
        specs="${specs:+$specs }$mode:$n"
      done
    done
  fi
  for spec in $specs; do
    IFS=: read -r mode N <<< "$spec"
    case "$mode" in samples|histogram|cm) ;; *)
      echo "[fig5-zk] invalid mode '$mode'; expected samples, histogram, or cm" >&2
      exit 2
    esac
    selected_nodes="$(nodes_for "$N")"
    echo "[fig5-zk] mode=$mode aggregators=$N machines=[$selected_nodes]"; : > "$MET"
    run_cell DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=4096 TOTAL_LOGS=131072 NODES="$selected_nodes"
    emit "$C" "$N" "$mode"
    cat "$C" | tail -1
  done
  echo "[fig5-zk] -> $C"
fi
echo "[fig5-table3-zk] done"
