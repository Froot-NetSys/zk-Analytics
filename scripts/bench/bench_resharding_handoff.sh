#!/usr/bin/env bash
# bench_resharding_handoff.sh — demonstrates the handoff-inheritance fix:
# after a reshard, the GAINING aggregator inherits the moved source's per-source
# chain tip from the authoritative Handoff row the LOSING owner published, so the
# SHA-256 chain stitches across the boundary instead of restarting from zero.
#
# It asserts `chain-inspector --strict` returns a PASS verdict (exit 0).
#
# Protocol (single machine; each aggregator has its own RocksDB, so handoffs are
# replicated between them by `handoff-sync`, standing in for the shared
# coordination store a real deployment would use):
#   1. install split ownership map at the boundary epoch on both stores
#   2. run the losing owner CONTINUOUSLY across the boundary so it detects the
#      outgoing transition and publishes Handoff{chain_tip=real, from=self}
#   3. handoff-sync the published handoffs into the gaining owner's store
#   4. run the gaining owner: its incoming path reads the handoff and inherits
#   5. chain-inspector --strict over both stores  ->  VERDICT: PASS
#
# Build first:
#   cargo build --release -p aggregator \
#     --bin aggregator --bin reshard-controller \
#     --bin handoff-sync --bin chain-inspector

set -uo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

W="${WORK_DIR:-/tmp/zkt_reshard_handoff}"
BIN="$ROOT_DIR/target/release/aggregator"
RC="$ROOT_DIR/target/release/reshard-controller"
SYNC="$ROOT_DIR/target/release/handoff-sync"
INSPECT="$ROOT_DIR/target/release/chain-inspector"
NUM_SOURCES="${NUM_SOURCES:-4}"

for b in "$BIN" "$RC" "$SYNC" "$INSPECT"; do
    [[ -x "$b" ]] || { echo "[handoff][FAIL] missing binary: $b" >&2; exit 1; }
done

run() { # id db start end logfile
    AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-2}" timeout 120 "$BIN" --rocksdb \
        --mode samples --no-zkvm-proof --fake-epochs --use-online-ownership \
        --aggregator-id "$1" --agg-rocksdb-path "$2" --raw-rocksdb-path "$W/raw_$1" \
        --start-seq "$3" --end-seq "$4" \
        --source-ids "$(seq -s, 0 $((NUM_SOURCES-1)))" \
        --series 8 --samples-per-series 4 --commit-batch-size 4 >"$5" 2>&1
}
split_map() { local m=""; for s in $(seq 0 $((NUM_SOURCES-1))); do [[ -z "$m" ]] || m+=","; m+="$s:$((s%2))"; done; echo "$m"; }

rm -rf "$W"; mkdir -p "$W/agg0" "$W/agg1"

echo "=== install split map @ epoch 3 on both coordination stores ==="
"$RC" --rocksdb-path "$W/agg0" --at-epoch 3 --map "$(split_map)" >"$W/rc0.log" 2>&1
"$RC" --rocksdb-path "$W/agg1" --at-epoch 3 --map "$(split_map)" >"$W/rc1.log" 2>&1

echo "=== losing owner (agg0) runs continuously across the boundary (epochs 0..3) ==="
run 0 "$W/agg0" 0 3 "$W/agg0.log"
grep -E 'outgoing|inherited' "$W/agg0.log" || true

echo "=== replicate published handoffs into the gaining owner's store ==="
"$SYNC" --from "$W/agg0" --to "$W/agg1" 2>/dev/null | grep RESULT

echo "=== gaining owner (agg1) runs (epochs 3..5) and inherits ==="
run 1 "$W/agg1" 3 5 "$W/agg1.log"
grep -E 'inherited|cold start' "$W/agg1.log" || true

echo "=== chain-inspector --strict over both stores ==="
"$INSPECT" --rocksdb-path "$W/agg0" --rocksdb-path "$W/agg1" --strict >"$W/inspect.log" 2>/dev/null
rc=$?
grep -E 'src=|RESULT_SUMMARY|VERDICT' "$W/inspect.log"
echo
if [[ $rc -eq 0 ]]; then
    echo "[handoff] PASS: per-source chain continuity preserved across the reshard."
else
    echo "[handoff] FAIL: chain-inspector reported broken continuity (exit $rc)."
fi
exit $rc
