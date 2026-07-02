#!/usr/bin/env bash
set -uo pipefail
# Native (non-ZK) e2e for the three Figure-4 datasets at the SAME setup as the
# ZK runs: Google/hash-table/8 aggregators, CAIDA/CM/8, Vehicle/histogram/4.
# Full real pipeline (data source w/ hash-chain commitment -> Kafka -> RocksDB ->
# aggregator -> FoundationDB -> querier). Captures RocksDB/FDB read+write timing,
# native aggregation time, and native query time per dataset. ZK side comes from
# the preserved /mydata/dist_run/*_zk runs.
ROOT="/mydata/zk-Analytics"; cd "$ROOT"
DRV="$ROOT/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT/results/_dist_metrics.jsonl"
OUTDIR="$ROOT/results/e2e_native"; mkdir -p "$OUTDIR"
source "$ROOT/scripts/lib/common.sh"
export AGG_MAX_WAIT=900 AGGR_IDLE_TIMEOUT_SECS=20
nodes_for(){ local n="$1" o=""; for ((i=0;i<n;i++)); do o="$o node$i"; done; echo "${o# }"; }
LOG=/tmp/e2e_native.log; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# dataset:nodes
for spec in google:8 caida:8 vehicle:4; do
  IFS=: read -r ds n <<< "$spec"
  say "=== Native e2e: $ds ($n aggregators) ==="
  : > "$MET"
  env DATASET=$ds NODES="$(nodes_for $n)" MODE=native bash "$DRV" >> "$LOG" 2>&1 \
    || say "  driver error for $ds"
  cp "$MET" "$OUTDIR/${ds}_native.jsonl"
  say "  saved $OUTDIR/${ds}_native.jsonl"
done
say "=== Native e2e done ==="
