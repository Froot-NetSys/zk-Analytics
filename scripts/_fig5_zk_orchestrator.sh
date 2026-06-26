#!/usr/bin/env bash
set -uo pipefail
# Fig 5 ZK distributed: run one (mode,N) cell at a time, recover the row from
# the cell's dist_run dir (robust against emit/path bugs), rebuild the compare
# md, and commit+push after EACH cell so partial results land incrementally.
# Cheapest/most-distributed cells first; histogram N=1 (8 serial epochs ~13h)
# last. Kill-to-idle before every cell.
ROOT="/mydata/zk-Analytics"; cd "$ROOT"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
export AGG_MAX_WAIT=55000 AGGR_IDLE_TIMEOUT_SECS=30
DRV="$ROOT/scripts/run_distributed_baseline.sh"
LOG=/tmp/fig5zk_orch.log; : > "$LOG"
say(){ echo "[$(date +%m-%d_%H:%M:%S)] $*" | tee -a "$LOG"; }
KILL='for p in $(pgrep -x zktelemetry-ris); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x r0vm); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done; pkill -9 -x querier 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null; true'
nuke(){ bash -c "$KILL" || true; for n in 1 2 3 4 5 6 7; do timeout 8 ssh -o StrictHostKeyChecking=no node$n "$KILL" >/dev/null 2>&1 || true; done; sleep 2; }
nodes_for(){ local n="$1" o=""; for ((i=0;i<n;i++)); do o="$o node$i"; done; echo "${o# }"; }
atype_of(){ case "$1" in samples) echo hash_table;; histogram) echo histogram;; cm) echo cms;; esac; }

C="$ROOT/results/fig5_zk.csv"
HDR="var,mode,agg_total_s,prove_s,verify_s,kafka_recv_s,rocksdb_raw_insert_s,fdb_write_s,agg_host_rss_mb,agg_prover_rss_mb,agg_cluster_rss_mb,proof_bytes,journal_bytes,query_total_s,query_prove_s,query_verify_s,query_fdb_lookup_s,query_host_rss_mb,query_prover_rss_mb"
[ -f "$C" ] || echo "$HDR" > "$C"

emit_from_dir(){ # $1=dir $2=N $3=mode
  local tmp=/tmp/fig5zk_${3}_${2}.jsonl; : > "$tmp"
  python3 scripts/_parse_dist_cell.py --dataset synthetic --agg-type "$(atype_of $3)" --mode zk \
    --workdir "$1" --logdir "$1/logs" --metrics "$tmp" --epoch-size 16384 --num-aggregators "$2" >/dev/null 2>&1
  python3 - "$tmp" "$2" "$3" >> "$C" <<'PY'
import sys,json
met,var,mode=sys.argv[1],sys.argv[2],sys.argv[3]
recs=[json.loads(l) for l in open(met) if l.strip()]
agg=[r for r in recs if r['task']=='aggregation']; q=[r for r in recs if r['task']=='query']
if not agg: print(f"{var},{mode},PARSE_FAILED"); sys.exit(0)
agg=agg[-1]; q=q[-1] if q else {}
c=agg.get('components_s',{}); qc=q.get('components_s',{})
def g(d,k): return round(d.get(k,0.0),6)
print(",".join(str(x) for x in [var,mode,
  round(agg['total_time_s'],3), g(c,'prove'), g(c,'verify'),
  g(c,'kafka_recv'), g(c,'rocksdb_raw_insert'), g(c,'fdb_write'),
  round(agg.get('per_node_host_rss_mb',0.0),1), round(agg.get('per_node_prover_rss_mb',0.0),1),
  round(agg.get('peak_rss_mb',0.0),1),
  agg.get('proof_bytes_per_epoch',0), agg.get('journal_bytes_per_epoch',0),
  round(q.get('total_time_s',0.0),3), g(qc,'query_compute'), g(qc,'verify'), g(qc,'fdb_lookup'),
  round(q.get('host_peak_rss_mb',0.0),1), round(q.get('prover_peak_rss_mb',0.0),1)]))
PY
}
gitcp(){ git add -A results/ scripts/ >/dev/null 2>&1
  git -c user.name=zzylol -c user.email=zeyingz@umd.edu commit -q -m "$1" >/dev/null 2>&1 \
    && git push origin HEAD:camera-ready/non-zk-baseline >/dev/null 2>&1 && say "pushed: $1" || say "no-commit/push-fail: $1"; }

for spec in ${FIG5_SPECS:-histogram:8 samples:8 cm:8 histogram:1}; do
  IFS=: read -r mode N <<< "$spec"
  say "Fig5 ZK cell mode=$mode N=$N starting ..."
  nuke
  env DATASET=synthetic SYNTH_MODE=$mode SYNTH_KEYS=4096 TOTAL_LOGS=131072 \
    NODES="$(nodes_for $N)" MODE=zk bash "$DRV" >> "$LOG" 2>&1 || say "  driver error mode=$mode N=$N"
  emit_from_dir "/mydata/dist_run/synthetic_${mode}_n${N}_zk" "$N" "$mode"
  say "  row: $(tail -1 "$C")"
  python3 scripts/_build_zk_compare.py >/dev/null 2>&1 || true
  gitcp "Fig 5 ZK cell: $mode N=$N (prove/verify/memory/proof)"
done
nuke
say "Fig 5 ZK done -> $C"
