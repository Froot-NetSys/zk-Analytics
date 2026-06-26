#!/usr/bin/env bash
# bench_resharding_perf.sh — corrected, *timed* online-resharding evaluation.
#
# This supersedes the existence-proof in `bench_resharding.sh`, which (a) is a
# pure smoke check and (b) — because it passes `--fake-series-hist` — makes the
# aggregator take the early `return Ok(())` path in main.rs and never actually
# reach the ownership filter. (Its only *hard* assertion is that the
# "[ownership] enabled" line is printed, which happens before that early
# return, so it passes without exercising resharding at all.)
#
# This harness instead:
#   1. Drives the REAL data path (`--fake-epochs` only; source_ids are 0-based
#      to match the generator) for scale-UP (1->2) and scale-DOWN (2->1).
#   2. Runs `chain-inspector` to render the per-source hash-chain CONTINUITY
#      verdict across each handoff.
#   3. Runs the in-process micro-benchmarks `reshard-bench` (install + owner
#      lookup latency) and `recovery-bench` (fault-recovery time).
#
# Build first:  cargo build --release -p aggregator \
#                 --bin aggregator --bin reshard-controller \
#                 --bin chain-inspector --bin reshard-bench --bin recovery-bench
#
# Exit 0 = harness ran; correctness verdicts are printed (NOT asserted PASS),
# because the current preview deliberately bootstraps chains from zero on a
# handoff (see EVALUATION_ONLINE_RESHARDING.md).

set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

W="${WORK_DIR:-/tmp/zkt_reshard_perf}"
BIN="$ROOT_DIR/target/release/aggregator"
RC="$ROOT_DIR/target/release/reshard-controller"
INSPECT="$ROOT_DIR/target/release/chain-inspector"
SYNC="$ROOT_DIR/target/release/handoff-sync"
RBENCH="$ROOT_DIR/target/release/reshard-bench"
RECBENCH="$ROOT_DIR/target/release/recovery-bench"

NUM_SOURCES="${NUM_SOURCES:-4}"        # generator emits source_ids 0..NUM_SOURCES-1
SERIES="${SERIES:-8}"
SPS="${SAMPLES_PER_SERIES:-4}"
CBS="${COMMIT_BATCH_SIZE:-4}"
IDLE="${AGGR_IDLE_TIMEOUT_SECS:-2}"

for b in "$BIN" "$RC" "$INSPECT" "$SYNC" "$RBENCH" "$RECBENCH"; do
    [[ -x "$b" ]] || { echo "[perf][FAIL] missing binary: $b — run the cargo build line in this script's header" >&2; exit 1; }
done

step() { echo; echo "=========== $* ==========="; }

run_agg() { # id db start end extra logfile
    AGGR_IDLE_TIMEOUT_SECS="$IDLE" timeout 120 "$BIN" --rocksdb --mode samples \
        --no-zkvm-proof --fake-epochs \
        --aggregator-id "$1" --agg-rocksdb-path "$2" --raw-rocksdb-path "$W/raw_$1" \
        --start-seq "$3" --end-seq "$4" \
        --source-ids "$(seq -s, 0 $((NUM_SOURCES-1)))" \
        --series "$SERIES" --samples-per-series "$SPS" --commit-batch-size "$CBS" \
        $5 >"$6" 2>&1
}

# even sources -> agg0, odd -> agg1
split_map() { local m=""; for s in $(seq 0 $((NUM_SOURCES-1))); do [[ -z "$m" ]] || m+=","; m+="$s:$((s%2))"; done; echo "$m"; }
all_to_0_map() { local m=""; for s in $(seq 0 $((NUM_SOURCES-1))); do [[ -z "$m" ]] || m+=","; m+="$s:0"; done; echo "$m"; }

rm -rf "$W"; mkdir -p "$W/agg0" "$W/agg1"

# Drive pattern (post handoff-inheritance fix): the LOSING owner must run
# continuously across a boundary so it has prev_owned state to detect the
# transition and publish a real-tip Handoff; handoff-sync replicates it; the
# GAINING owner inherits. Both maps are installed up front; agg1's single
# continuous 3..6 run is the loser on scale-up's mirror (gains 1,3 @3) and the
# loser on scale-down (releases 1,3 @6). See bench_resharding_handoff.sh for the
# focused single-direction demo.
step "RESHARD up (1->2 @epoch3) then down (2->1 @epoch6), sources=$NUM_SOURCES"
t0=$(date +%s.%N)
"$RC" --rocksdb-path "$W/agg0" --at-epoch 3 --map "$(split_map)"   >"$W/rc_up0.log" 2>&1
"$RC" --rocksdb-path "$W/agg1" --at-epoch 3 --map "$(split_map)"   >"$W/rc_up1.log" 2>&1
"$RC" --rocksdb-path "$W/agg0" --at-epoch 6 --map "$(all_to_0_map)" >"$W/rc_dn0.log" 2>&1
"$RC" --rocksdb-path "$W/agg1" --at-epoch 6 --map "$(all_to_0_map)" >"$W/rc_dn1.log" 2>&1
t1=$(date +%s.%N)
run_agg 0 "$W/agg0" 0 3 "--use-online-ownership" "$W/agg0_up.log"   # agg0 releases 1,3 @3
"$SYNC" --from "$W/agg0" --to "$W/agg1" >"$W/sync_up.log" 2>&1       # -> agg1
run_agg 1 "$W/agg1" 3 6 "--use-online-ownership" "$W/agg1.log"      # agg1 inherits 1,3 @3, releases @6
"$SYNC" --from "$W/agg1" --to "$W/agg0" >"$W/sync_dn.log" 2>&1       # -> agg0
run_agg 0 "$W/agg0" 6 8 "--use-online-ownership" "$W/agg0_dn.log"   # agg0 re-inherits 1,3 @6
echo "control-plane install (4 maps): $(awk "BEGIN{printf \"%.1f\", ($t1-$t0)*1000}") ms"
echo "published outgoing: agg0@3=$(grep -c 'outgoing' "$W/agg0_up.log") agg1@6=$(grep -c 'outgoing' "$W/agg1.log");  inherited: agg1=$(grep -c 'inherited' "$W/agg1.log") agg0=$(grep -c 'inherited' "$W/agg0_dn.log")"
"$INSPECT" --rocksdb-path "$W/agg0" --rocksdb-path "$W/agg1" 2>/dev/null | grep -E 'RESULT_SUMMARY|VERDICT'

step "MICROBENCH: reshard control-plane install + owner-lookup latency"
"$RBENCH" --sources-sweep 2,8,32,128,512,2048 --history-sweep 1,8,64,512,4096 --iters 20000 2>/dev/null

step "MICROBENCH: fault-recovery time vs completed-epoch count"
"$RECBENCH" --epochs-sweep 1,10,100,1000,10000,50000 --orphans 3 --repeat 5 2>/dev/null

echo
echo "[perf] done. logs + dbs under: $W"
