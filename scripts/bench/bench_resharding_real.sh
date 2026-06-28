#!/usr/bin/env bash
# bench_resharding_real.sh — general X->Y online resharding on the REAL raw_db
# path (NOT --fake-epochs).
#
# Unlike the fake-epochs harnesses (which inherit tips at the coordination layer
# only), here the aggregators read real `epoch_batches` from a SHARED raw RocksDB
# and the core CRYPTOGRAPHICALLY verifies each batch's SHA-256 chain against the
# per-source tip. So a moved source whose new owner does NOT inherit the correct
# tip would PANIC with a chain/sequence mismatch. A clean run is therefore proof
# of genuine cross-aggregator continuity, not just metadata bookkeeping.
#
# Protocol per (X,Y), N sources, boundary epoch B:
#   - gen-raw-epochs: write chained epoch_batches for seqs [0, B+1] into ONE
#     shared raw_db (the durable source of truth, like the Kafka ingestor)
#   - install map s->s%X @0 and s->s%Y @B on every aggregator store
#   - Phase A: old owners 0..X-1 process [0, B-1] (--max-process-seq B-1),
#     persisting per-source tips
#   - replicate tips into each new owner's store (handoff-sync)
#   - Phase B: new owners 0..Y-1 process the remaining epochs [B, B+1]; each
#     loads the tip for every source it owns and the core verifies the real
#     boundary batch chains from it
#   - verify: coverage, continuity, no panic/gap, boundary epoch processed
#
# Build:
#   cargo build --release -p aggregator \
#     --bin aggregator --bin reshard-controller \
#     --bin handoff-sync --bin chain-inspector

set -uo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

BIN="$ROOT_DIR/target/release/aggregator"
RC="$ROOT_DIR/target/release/reshard-controller"
SYNC="$ROOT_DIR/target/release/handoff-sync"
INSPECT="$ROOT_DIR/target/release/chain-inspector"
for b in "$BIN" "$RC" "$SYNC" "$INSPECT"; do
    [[ -x "$b" ]] || { echo "[real][FAIL] missing binary: $b" >&2; exit 1; }
done

N="${SOURCES:-12}"
B="${BOUNDARY:-3}"
SRC_IDS="$(seq -s, 0 $((N-1)))"
PASS=0; FAIL=0

# real-mode aggregator (no --fake-epochs); reads the SHARED raw_db.
run_real() { # id db raw cap(or -1) logfile
    local cap_arg=""; [[ "$4" != "-1" ]] && cap_arg="--max-process-seq $4"
    AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-2}" timeout 180 "$BIN" --rocksdb \
        --mode samples --no-zkvm-proof --use-online-ownership --keep-raw-batches \
        --aggregator-id "$1" --agg-rocksdb-path "$2" --raw-rocksdb-path "$3" \
        --source-ids "$SRC_IDS" $cap_arg >"$5" 2>&1
}
map_mod() { local mod="$1" m=""; for s in $(seq 0 $((N-1))); do [[ -z "$m" ]] || m+=","; m+="$s:$((s%mod))"; done; echo "$m"; }

reshard_real() { # X Y
    local X="$1" Y="$2"
    local M=$(( X>Y ? X : Y ))
    local W="/tmp/zkt_real/x${X}_y${Y}"; rm -rf "$W"; mkdir -p "$W"
    local raw="$W/raw"

    # one shared raw_db with chained epoch_batches for [0, B+1]
    "$BIN" --rocksdb --gen-raw-epochs --mode samples --raw-rocksdb-path "$raw" \
        --start-seq 0 --end-seq $((B+1)) --source-ids "$SRC_IDS" \
        --series 8 --samples-per-series 4 --commit-batch-size 4 >"$W/gen.log" 2>&1

    local map_x map_y i j; map_x="$(map_mod "$X")"; map_y="$(map_mod "$Y")"
    for i in $(seq 0 $((M-1))); do
        "$RC" --rocksdb-path "$W/agg_$i" --at-epoch 0 --map "$map_x" >"$W/rc_${i}_x.log" 2>&1
        "$RC" --rocksdb-path "$W/agg_$i" --at-epoch "$B" --map "$map_y" >"$W/rc_${i}_y.log" 2>&1
    done

    # Phase A: old owners process [0, B-1]
    for i in $(seq 0 $((X-1))); do run_real "$i" "$W/agg_$i" "$raw" $((B-1)) "$W/a_$i.log"; done

    # replicate coordination state (tips) into each new owner
    local from_args=""; for i in $(seq 0 $((X-1))); do from_args+=" --from $W/agg_$i"; done
    for j in $(seq 0 $((Y-1))); do "$SYNC" $from_args --to "$W/agg_$j" >"$W/sync_$j.log" 2>&1; done

    # Phase B: new owners process the rest [B, B+1] with inherited tips
    for j in $(seq 0 $((Y-1))); do run_real "$j" "$W/agg_$j" "$raw" -1 "$W/b_$j.log"; done

    # ---- verify ----
    local movers=0; for s in $(seq 0 $((N-1))); do [[ $((s%X)) -ne $((s%Y)) ]] && movers=$((movers+1)); done

    # panic / chain-gap in any Phase B log? (the real-path correctness signal)
    local panics; panics=$(grep -lE 'panicked|sequence gap|GAP!' "$W"/b_*.log 2>/dev/null | wc -l)

    # every new owner must have processed the boundary epoch B
    local processed_B=0
    for j in $(seq 0 $((Y-1))); do grep -q "Epoch seq=$B " "$W/b_$j.log" && processed_B=$((processed_B+1)); done

    # coverage + continuity
    "$INSPECT" --check-coverage --rocksdb-path "$W/agg_0" --sources "$N" \
        --aggregators "$Y" --at-epoch "$B" --strict >"$W/cov.log" 2>/dev/null; local cov_rc=$?
    local paths=""; for j in $(seq 0 $((Y-1))); do paths+=" --rocksdb-path $W/agg_$j"; done
    "$INSPECT" $paths --strict >"$W/cont.log" 2>/dev/null; local cont_rc=$?
    local handoffs; handoffs=$(sed -n 's/.*RESULT_SUMMARY.*[^_]handoffs=\([0-9]*\).*/\1/p' "$W/cont.log")

    local verdict="PASS"
    [[ "$panics" -eq 0 ]]        || verdict="FAIL(panic/gap in $panics log)"
    [[ "$processed_B" -eq "$Y" ]] || verdict="FAIL(only $processed_B/$Y processed epoch $B)"
    [[ $cov_rc -eq 0 ]]          || verdict="FAIL(coverage)"
    [[ $cont_rc -eq 0 ]]         || verdict="FAIL(continuity)"
    [[ "${handoffs:-0}" -eq "$movers" ]] || verdict="FAIL(handoffs=${handoffs:-0}!=movers=$movers)"
    if [[ "$verdict" == "PASS" ]]; then PASS=$((PASS+1)); else FAIL=$((FAIL+1)); fi

    printf "%-9s N=%-3s movers=%-3s handoffs=%-3s procB=%s/%s panics=%s  %s\n" \
        "${X}->${Y}" "$N" "$movers" "${handoffs:-0}" "$processed_B" "$Y" "$panics" "$verdict"
}

SWEEP="${SWEEP:-1->2 2->1 2->4 4->2 3->5 5->3 2->3 3->2}"
echo "=== REAL raw_db path: X->Y resharding (N=$N sources, boundary epoch=$B, cryptographic chain verification) ==="
printf "%-9s %-6s %-10s %-12s %-8s %-9s %s\n" "reshard" "N" "movers" "handoffs" "procB" "panics" "verdict"
echo "----------------------------------------------------------------------------------------"
for pair in $SWEEP; do reshard_real "${pair%%->*}" "${pair##*->}"; done
echo "----------------------------------------------------------------------------------------"
echo "[real] PASS=$PASS FAIL=$FAIL"
[[ $FAIL -eq 0 ]] && echo "[real] ALL CONFIGS PASS (real raw_db path, chain verified)" || echo "[real] SOME CONFIGS FAILED"
exit $(( FAIL > 0 ? 1 : 0 ))
