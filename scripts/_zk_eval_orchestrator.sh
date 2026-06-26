#!/usr/bin/env bash
set -uo pipefail
# Autonomous Fig 5/6/7 evaluation chain: wait for the in-flight native run,
# commit+push native memory results, run Fig 7 native memory, then the ZK runs
# (Fig 6, Fig 5), committing+pushing after every stage. Long-running (zk is
# multi-hour). Kill-to-idle before every cell is handled by the driver scripts;
# we also nuke the cluster between stages.
ROOT="/mydata/zk-Analytics"; cd "$ROOT"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
LOG=/tmp/zk_eval_orch.log; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

KILL='for p in $(pgrep -x zktelemetry-ris); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x r0vm); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done; pkill -9 -x querier 2>/dev/null; pkill -9 -f mem_trace.py 2>/dev/null; true'
nuke(){ bash -c "$KILL" || true; for n in 1 2 3 4 5 6 7; do timeout 8 ssh -o StrictHostKeyChecking=no node$n "$KILL" >/dev/null 2>&1 || true; done; sleep 2; }

gitcp(){ # $1 = commit message
  git add -A results/ scripts/ >/dev/null 2>&1
  git -c user.name=zzylol -c user.email=zeyingz@umd.edu commit -q -m "$1" >/dev/null 2>&1 \
    && git push origin HEAD:camera-ready/non-zk-baseline >/dev/null 2>&1 \
    && say "committed+pushed: $1" || say "nothing to commit / push failed: $1"
}

# ---- Stage 0: wait for the in-flight native Fig5/6 re-run ----
say "Stage 0: waiting for native Fig5/6 (memory) to finish ..."
for i in $(seq 1 400); do
  grep -q "\[figs\] done" /tmp/figs_native_mem.log 2>/dev/null && break
  sleep 15
done
say "native Fig5/6 done."
python3 scripts/_build_zk_compare.py >/dev/null 2>&1 || true
gitcp "Fig 5/6 native baselines: add peak-RSS (host/prover) memory columns"

# ---- Stage 1: Fig 7 native query memory ----
say "Stage 1: Fig 7 native query memory sweep ..."
nuke
bash scripts/_fig7_requery_mem.sh >> "$LOG" 2>&1 || say "fig7 mem sweep error"
gitcp "Fig 7 native: add querier peak-RSS memory per #queried epochs"

# ---- Stage 2: Fig 6 ZK (1 epoch x 3 modes) ----
say "Stage 2: Fig 6 ZK (real prove+verify, 3 modes) ~5h ..."
nuke
export AGG_MAX_WAIT=20000 AGGR_IDLE_TIMEOUT_SECS=30
FIG=6 SYNTH_KEYS=1024 bash scripts/run_figures_zk.sh >> "$LOG" 2>&1 || say "fig6 zk error"
python3 scripts/_build_zk_compare.py >/dev/null 2>&1 || true
gitcp "Fig 6 ZK: per-mode prove/verify time, host+prover memory, proof size, public output"

# ---- Stage 3: Fig 5 ZK (histogram N=1 & N=8; samples/cm N=8) ----
say "Stage 3: Fig 5 ZK (distributed scaling) ~9h ..."
nuke
FIG=5 bash scripts/run_figures_zk.sh >> "$LOG" 2>&1 || say "fig5 zk error"
python3 scripts/_build_zk_compare.py >/dev/null 2>&1 || true
gitcp "Fig 5 ZK: distributed prove/verify, host+prover memory, proof size vs aggregator count"

nuke
say "ALL ZK EVAL STAGES DONE (Fig 6, Fig 5). Fig 7 ZK pending design."
