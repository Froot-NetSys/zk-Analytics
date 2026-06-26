#!/usr/bin/env bash
# bench_resharding_xy.sh — general X->Y online-resharding evaluation.
#
# Confirms that resharding from X aggregators to Y aggregators (scale up OR
# down, arbitrary X and Y) "still works": after the reshard
#   (1) ownership PARTITIONS the N sources across exactly Y aggregators
#       (coverage: every source owned exactly once), and
#   (2) every source that MOVED has its per-source SHA-256 chain stitched across
#       the boundary (continuity: chain-inspector --strict PASS),
# driven through the durable per-source tip (AggSourceTip) + handoff machinery.
#
# Protocol per (X,Y), N sources, boundary epoch B (each aggregator has its own
# RocksDB; coordination state is replicated by handoff-sync, the single-machine
# stand-in for a shared coordination store):
#   - install ownership map s->s%X @epoch 0 and s->s%Y @epoch B on all stores
#   - Phase A: run old owners 0..X-1 for epochs [0,B-1]; each persists per-source
#     chain tips for the sources it owns (no boundary processing yet)
#   - replicate all old owners' tips into each new owner's store
#   - Phase B: run new owners 0..Y-1 for epochs [B,B+1]; each loads the tip for
#     every source it owns (kept -> own tip; moved -> previous owner's tip, and
#     writes an auditable Handoff)
#   - verify coverage (--check-coverage) and continuity (--strict)
#
# Build:
#   cargo build --release -p aggregator \
#     --bin aggregator --bin reshard-controller \
#     --bin handoff-sync --bin chain-inspector

set -uo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

BIN="$ROOT_DIR/target/release/aggregator"
RC="$ROOT_DIR/target/release/reshard-controller"
SYNC="$ROOT_DIR/target/release/handoff-sync"
INSPECT="$ROOT_DIR/target/release/chain-inspector"
for b in "$BIN" "$RC" "$SYNC" "$INSPECT"; do
    [[ -x "$b" ]] || { echo "[xy][FAIL] missing binary: $b" >&2; exit 1; }
done

N="${SOURCES:-12}"          # number of sources
B="${BOUNDARY:-3}"          # reshard boundary epoch (>=2 so tips are built)
PASS=0; FAIL=0

run_agg() { # id db start end logfile
    AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-2}" timeout 120 "$BIN" --rocksdb \
        --mode samples --no-zkvm-proof --fake-epochs --use-online-ownership \
        --aggregator-id "$1" --agg-rocksdb-path "$2" --raw-rocksdb-path "${2}_raw" \
        --start-seq "$3" --end-seq "$4" --source-ids "$(seq -s, 0 $((N-1)))" \
        --series 8 --samples-per-series 4 --commit-batch-size 4 >"$5" 2>&1
}
map_mod() { local mod="$1" m=""; for s in $(seq 0 $((N-1))); do [[ -z "$m" ]] || m+=","; m+="$s:$((s%mod))"; done; echo "$m"; }

reshard_xy() { # X Y
    local X="$1" Y="$2"
    local M=$(( X>Y ? X : Y ))
    local W="/tmp/zkt_xy/x${X}_y${Y}"; rm -rf "$W"; mkdir -p "$W"
    local map_x map_y; map_x="$(map_mod "$X")"; map_y="$(map_mod "$Y")"

    # install both maps on all M stores
    local i j
    for i in $(seq 0 $((M-1))); do
        "$RC" --rocksdb-path "$W/agg_$i" --at-epoch 0 --map "$map_x" >"$W/rc_${i}_x.log" 2>&1
        "$RC" --rocksdb-path "$W/agg_$i" --at-epoch "$B" --map "$map_y" >"$W/rc_${i}_y.log" 2>&1
    done

    # Phase A: old owners build + persist tips for epochs [0, B-1]
    for i in $(seq 0 $((X-1))); do run_agg "$i" "$W/agg_$i" 0 $((B-1)) "$W/a_$i.log"; done

    # replicate every old owner's coordination state into each new owner's store
    local from_args=""
    for i in $(seq 0 $((X-1))); do from_args+=" --from $W/agg_$i"; done
    for j in $(seq 0 $((Y-1))); do
        "$SYNC" $from_args --to "$W/agg_$j" >"$W/sync_$j.log" 2>&1
    done

    # Phase B: new owners inherit + process [B, B+1]
    for j in $(seq 0 $((Y-1))); do run_agg "$j" "$W/agg_$j" "$B" $((B+1)) "$W/b_$j.log"; done

    # movers = sources whose owner changes from s%X to s%Y
    local movers=0
    for s in $(seq 0 $((N-1))); do [[ $((s%X)) -ne $((s%Y)) ]] && movers=$((movers+1)); done

    # coverage over any post-reshard store
    local cov; cov=$("$INSPECT" --check-coverage --rocksdb-path "$W/agg_0" \
        --sources "$N" --aggregators "$Y" --at-epoch "$B" --strict 2>/dev/null)
    local cov_rc=$?
    # continuity over all Y post-reshard stores
    local paths=""; for j in $(seq 0 $((Y-1))); do paths+=" --rocksdb-path $W/agg_$j"; done
    local cont; cont=$("$INSPECT" $paths --strict 2>/dev/null); local cont_rc=$?
    local handoffs; handoffs=$(echo "$cont" | sed -n 's/.*RESULT_SUMMARY.*[^_]handoffs=\([0-9]*\).*/\1/p')
    local cont_ok; cont_ok=$(echo "$cont" | grep -c 'all_continuous=true')

    local verdict="PASS"
    [[ $cov_rc -eq 0 ]] || verdict="FAIL(coverage)"
    [[ $cont_rc -eq 0 ]] || verdict="FAIL(continuity)"
    [[ "${handoffs:-0}" -eq "$movers" ]] || verdict="FAIL(handoffs=${handoffs:-0}!=movers=$movers)"
    if [[ "$verdict" == "PASS" ]]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

    printf "%-9s N=%-3s movers=%-3s handoffs=%-3s  %s | %s\n" \
        "${X}->${Y}" "$N" "$movers" "${handoffs:-0}" \
        "$(echo "$cov" | grep -o 'VERDICT:.*' | sed 's/VERDICT: //')" \
        "$verdict"
}

SWEEP="${SWEEP:-1->2 1->3 2->1 3->1 2->4 4->2 3->5 5->3 2->3 3->2 1->6 6->1}"
echo "=== General X->Y resharding sweep (N=$N sources, boundary epoch=$B) ==="
printf "%-9s %-6s %-10s %-12s %s | %s\n" "reshard" "N" "movers" "handoffs" "coverage" "verdict"
echo "------------------------------------------------------------------------------------"
for pair in $SWEEP; do
    X="${pair%%->*}"; Y="${pair##*->}"
    reshard_xy "$X" "$Y"
done
echo "------------------------------------------------------------------------------------"
echo "[xy] PASS=$PASS FAIL=$FAIL"
[[ $FAIL -eq 0 ]] && echo "[xy] ALL CONFIGS PASS" || echo "[xy] SOME CONFIGS FAILED"
exit $(( FAIL > 0 ? 1 : 0 ))
