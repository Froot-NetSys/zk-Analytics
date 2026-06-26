#!/usr/bin/env bash
set -uo pipefail
# Distributed Table 2 sweep: aggregator proof size, public output, and CORRECTED
# peak memory (host + r0vm prover) vs number of aggregator machines.
#
# Synthetic 4,096 series / 131,072 logs / epoch 16,384 / batch 8; aggregators
# N in {1,2,4,8} x modes {samples,histogram,cm}. Each of the N nodes proves ONE
# epoch (16,384 logs, 4096/N keys) via the RocksDB pipeline + --fake-epochs (so
# proof_bytes/journal_bytes are logged AND host+prover memory is measured).
#
# PARALLELIZED: Table 2 cells are independent, so per mode the N={1,2,4} cells
# run CONCURRENTLY on disjoint nodes (node0 | node1-2 | node3-6), then N=8 uses
# all nodes. Modes run sequentially (each needs up to 8 nodes).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"; cd "$ROOT_DIR"
ALLNODES="node0 node1 node2 node3 node4 node5 node6 node7"
DIST="$HOME/zktel-dist"; RBIN="$DIST/bin"
REMOTE_ENV="LD_LIBRARY_PATH=$DIST/lib PATH=$HOME/.cargo/bin:\$PATH"
MEM_INTERVAL="${MEM_INTERVAL:-2.0}"; SEED="${SEED:-0xA66A1E}"
OUT="$ROOT_DIR/results/table2_distributed.csv"; JSONL="$ROOT_DIR/results/_table2_metrics.jsonl"
: > "$JSONL"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100

on_node(){ local n="$1"; shift; if [ "$n" = node0 ]; then bash -c "$*"; else ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$n" "$*"; fi; }
spawn(){ local n="$1"; shift; local c="$*"; if [ "$n" = node0 ]; then setsid bash -c "$c" </dev/null >/dev/null 2>&1 & else ssh -n -o BatchMode=yes "$n" "setsid bash -c '$c' </dev/null >/dev/null 2>&1 & echo ok" >/dev/null 2>&1; fi; }
KILL='for p in $(pgrep -x zktelemetry-ris); do kill -9 $p 2>/dev/null; done; pkill -9 -f mem_trace.py 2>/dev/null; sleep 1; for p in $(pgrep -x r0vm); do kill -9 $p 2>/dev/null; done'
nuke_idle(){
  for n in $ALLNODES; do on_node "$n" "$KILL" >/dev/null 2>&1 & done; wait
  for a in $(seq 1 10); do busy=0
    for n in $ALLNODES; do r=$(on_node "$n" "pgrep -xc r0vm 2>/dev/null; pgrep -xc zktelemetry-ris 2>/dev/null" 2>/dev/null|paste -sd+|bc 2>/dev/null)
      [ "${r:-0}" -gt 0 ] && { busy=1; on_node "$n" "$KILL" >/dev/null 2>&1; }; done
    [ "$busy" = 0 ] && break; sleep 2; done
}

# launch_cell mode N "node list" runid : start one aggregator per node (detached)
launch_cell(){
  local mode="$1" N="$2" nodes="$3" run="$4"
  local series=$((4096/N)) sps=$((4*N)) i=0
  for n in $nodes; do
    spawn "$n" "rm -f /tmp/${run}_$i.DONE; rm -rf /mydata/${run}_raw_$i /mydata/${run}_agg_$i; \
      nohup python3 $RBIN/mem_trace.py --out /tmp/${run}_$i.csv --summary /tmp/${run}_$i.json --match aggregator --match r0vm --interval $MEM_INTERVAL >/dev/null 2>&1 & \
      env $REMOTE_ENV E2E_TIMING=1 AGGR_PIPELINE=rocksdb FAKE_EPOCHS=1 BENCH_INPUT=synthetic AGGR_IDLE_TIMEOUT_SECS=10 RAYON_NUM_THREADS=56 \
      RAW_ROCKSDB_PATH=/mydata/${run}_raw_$i AGG_ROCKSDB_PATH=/mydata/${run}_agg_$i \
      /usr/bin/time -v $RBIN/aggregator --rocksdb --fake-epochs --mode $mode --series $series --samples-per-series $sps --epochs 1 --num-sources 1 --seed $SEED --threads 56 > /tmp/${run}_$i.log 2> /tmp/${run}_${i}_time.log; \
      pkill -INT -f mem_trace.py 2>/dev/null; sleep 3; touch /tmp/${run}_$i.DONE"
    i=$((i+1))
  done
}
# collect_parse mode N "node list" runid
collect_parse(){
  local mode="$1" N="$2" nodes="$3" run="$4"
  local WD="/mydata/dist_run/table2_${mode}_n${N}" LD; LD="$WD/logs"; mkdir -p "$LD"; local i=0
  for n in $nodes; do
    for try in 1 2 3 4 5; do on_node "$n" "cat /tmp/${run}_$i.log" > "$LD/agg_$i.log" 2>/dev/null; [ -s "$LD/agg_$i.log" ] && break; sleep 2; done
    on_node "$n" "cat /tmp/${run}_${i}_time.log" > "$LD/agg_${i}_time.log" 2>/dev/null
    on_node "$n" "cat /tmp/${run}_$i.json" > "$LD/mem_$i.json" 2>/dev/null
    i=$((i+1))
  done
  python3 "$ROOT_DIR/scripts/_parse_table2.py" --mode "$mode" --num-aggregators "$N" --logdir "$LD" --jsonl "$JSONL"
}
# wait for all DONE markers: args = "node:idx:runid ..."
wait_done(){ for spec in "$@"; do IFS=: read -r n i run <<< "$spec"
  for ((t=0;t<30000;t+=15)); do on_node "$n" "test -f /tmp/${run}_$i.DONE" 2>/dev/null && break; sleep 15; done; done; }

TS=$(date +%s)
for mode in ${MODES:-samples histogram cm}; do
  echo "=================================================================="; echo "[t2] mode=$mode  $(date '+%m-%d %H:%M:%S')"
  # ---- wave 1: N=1,2,4 concurrent on disjoint nodes ----
  echo "[t2] wave1 (N=1,2,4 concurrent) nuke+idle ..."; nuke_idle
  r1="t2_${mode}_1_$TS"; r2="t2_${mode}_2_$TS"; r4="t2_${mode}_4_$TS"
  launch_cell "$mode" 1 "node0" "$r1"
  launch_cell "$mode" 2 "node1 node2" "$r2"
  launch_cell "$mode" 4 "node3 node4 node5 node6" "$r4"
  echo "[t2] wave1 proving (N=1 on node0, N=2 on node1-2, N=4 on node3-6) ..."
  wait_done "node0:0:$r1" "node1:0:$r2" "node2:1:$r2" "node3:0:$r4" "node4:1:$r4" "node5:2:$r4" "node6:3:$r4"
  collect_parse "$mode" 1 "node0" "$r1"
  collect_parse "$mode" 2 "node1 node2" "$r2"
  collect_parse "$mode" 4 "node3 node4 node5 node6" "$r4"
  # ---- wave 2: N=8 on all nodes ----
  echo "[t2] wave2 (N=8) nuke+idle ..."; nuke_idle
  r8="t2_${mode}_8_$TS"; launch_cell "$mode" 8 "$ALLNODES" "$r8"
  echo "[t2] wave2 proving (N=8 all nodes) ..."
  specs=""; i=0; for n in $ALLNODES; do specs="$specs $n:$i:$r8"; i=$((i+1)); done
  wait_done $specs
  collect_parse "$mode" 8 "$ALLNODES" "$r8"
done
python3 "$ROOT_DIR/scripts/_build_table2.py" --jsonl "$JSONL" --out "$OUT"
echo "[t2] done -> $OUT"
