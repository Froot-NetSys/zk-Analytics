#!/usr/bin/env bash
# prove_handoff_demo.sh — a single small epoch with REAL zkVM proving across a
# reshard handoff (1 -> 2 aggregators).
#
# Unlike the other harnesses (which run --no-zkvm-proof and verify the SHA-256
# chain in the core), this generates and VERIFIES actual RISC0 receipts. The
# point: the post-handoff epoch's proof, on the NEW owner, must verify with the
# moved source's chain continuing from the INHERITED tip. Because the guest
# verifies the per-source chain in-circuit (the host asserts out == expected and
# calls receipt.verify), a verified receipt is proof-level continuity across the
# handoff.
#
# WARNING: real proving is slow (no AVX-512 here: ~4 min per tiny epoch). This
# proves exactly two epochs (~8-10 min total). Requires r0vm on PATH and
# RISC0_DEV_MODE unset (real proving).
#
# Build: cargo build --release -p aggregator \
#   --bin aggregator --bin reshard-controller \
#   --bin handoff-sync --bin chain-inspector

set -uo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$ROOT/target/release/aggregator"
RC="$ROOT/target/release/reshard-controller"
SYNC="$ROOT/target/release/handoff-sync"
INSPECT="$ROOT/target/release/chain-inspector"
W="${WORK_DIR:-/tmp/zkt_prove_handoff}"; rm -rf "$W"; mkdir -p "$W"
IDS="0,1"   # 2 sources; minimal per-epoch work to keep proving fast

# 1) shared raw_db with chained epoch_batches for epochs 0..1 (1 event/source/epoch)
"$BIN" --rocksdb --gen-raw-epochs --mode samples --raw-rocksdb-path "$W/raw" \
    --start-seq 0 --end-seq 1 --source-ids "$IDS" \
    --series 1 --samples-per-series 1 --commit-batch-size 1 >"$W/gen.log" 2>&1
echo "[gen] $(grep total_batches "$W/gen.log")"

# 2) ownership maps: X=1 (both sources on agg0) @epoch 0 ; Y=2 (s%2) @epoch 1
for a in 0 1; do
    "$RC" --rocksdb-path "$W/agg_$a" --at-epoch 0 --map "0:0,1:0" >"$W/rc_${a}_x.log" 2>&1
    "$RC" --rocksdb-path "$W/agg_$a" --at-epoch 1 --map "0:0,1:1" >"$W/rc_${a}_y.log" 2>&1
done

# 3) Phase A: agg0 PROVES epoch 0 (owns both sources), persists per-source tips
echo "[phaseA] agg0 proving epoch 0 (real zkVM) ..."
AGGR_IDLE_TIMEOUT_SECS=2 timeout 1800 "$BIN" --rocksdb --mode samples \
    --use-online-ownership --keep-raw-batches --max-process-seq 0 \
    --aggregator-id 0 --agg-rocksdb-path "$W/agg_0" --raw-rocksdb-path "$W/raw" \
    --source-ids "$IDS" >"$W/a0.log" 2>&1
echo "[phaseA] exit=$?  $(grep -E '\[samples\] seq=0 prove_ms' "$W/a0.log")"

# 4) replicate the per-source tips into the new owner's coordination view
"$SYNC" --from "$W/agg_0" --to "$W/agg_1" >"$W/sync.log" 2>&1
echo "[sync] $(grep RESULT "$W/sync.log")"

# 5) Phase B: agg1 PROVES the boundary epoch 1 for the MOVED source, inheriting
#    its chain tip; the receipt must verify (per-source chain checked in-circuit)
echo "[phaseB] agg1 proving boundary epoch 1 for moved source (real zkVM) ..."
AGGR_IDLE_TIMEOUT_SECS=2 timeout 1800 "$BIN" --rocksdb --mode samples \
    --use-online-ownership --keep-raw-batches \
    --aggregator-id 1 --agg-rocksdb-path "$W/agg_1" --raw-rocksdb-path "$W/raw" \
    --source-ids "$IDS" >"$W/b1.log" 2>&1
echo "[phaseB] exit=$?"

echo "=== agg1: inheritance + proof + (no) panic ==="
grep -E 'inherited chain_tip|\[samples\] seq=1 prove_ms|panicked|sequence gap' "$W/b1.log"
echo "=== continuity verdict ==="
"$INSPECT" --rocksdb-path "$W/agg_0" --rocksdb-path "$W/agg_1" 2>/dev/null \
    | grep -E 'src=|RESULT_SUMMARY|VERDICT'

# PASS iff: agg1 inherited, produced a verified proof for epoch 1, no panic.
if grep -q 'incoming: inherited chain_tip' "$W/b1.log" \
   && grep -qE '\[samples\] seq=1 prove_ms=[0-9]+ verify_ms=[0-9]+' "$W/b1.log" \
   && ! grep -qE 'panicked|sequence gap' "$W/b1.log"; then
    echo "[prove-handoff] PASS: boundary-epoch zkVM receipt verified on the new owner with the inherited tip."
    exit 0
else
    echo "[prove-handoff] FAIL"
    exit 1
fi
