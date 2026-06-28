#!/usr/bin/env bash
set -uo pipefail
# Full distributed Figure-4 e2e set on the real cluster. Each dataset runs
# native + zk on its paper-faithful node/aggregator count (Google=8, CAIDA=8,
# Vehicle=4). Each cell nukes all nodes + waits for idle before starting, runs
# aggregators in PARALLEL across nodes (each on a full 56-core machine), and
# captures per-node host+prover memory. Cells run sequentially (cheapest first).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"
N8="node0 node1 node2 node3 node4 node5 node6 node7"
N4="node0 node1 node2 node3"
METRICS="$ROOT_DIR/results/_dist_metrics.jsonl"
: > "$METRICS"   # fresh

cell() { # dataset nodes mode
  echo "=================================================================="
  echo "[matrix] $1 ($3) nodes='$2'  $(date '+%m-%d %H:%M:%S')"
  echo "=================================================================="
  DATASET="$1" NODES="$2" MODE="$3" bash "$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh" \
    || echo "[matrix] FAILED: $1/$3"
}

# Order: cheapest dataset first; native (fast) before zk (long) within each.
cell vehicle "$N4" native
cell vehicle "$N4" zk
cell google  "$N8" native
cell google  "$N8" zk
cell caida   "$N8" native
cell caida   "$N8" zk

echo "[matrix] building distributed tables ..."
python3 "$ROOT_DIR/scripts/lib/build_dist_tables.py" --metrics "$METRICS" --outdir "$ROOT_DIR/results" || true
echo "[matrix] done."
