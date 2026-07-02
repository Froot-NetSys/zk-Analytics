#!/usr/bin/env bash
set -uo pipefail
# ZK dev-mode e2e for the three Figure-4 datasets at the paper-faithful setup
# (Google/hash-table/8 aggregators, CAIDA/CM/8, Vehicle/histogram/4). Runs the
# full distributed pipeline (data source -> Kafka -> RocksDB -> aggregator ->
# FoundationDB -> querier) with the zkVM guests EXECUTED but the STARK proof
# faked (RISC0_DEV_MODE=1) -> validates the end-to-end zk path in minutes rather
# than hours. Sibling of run_e2e_native_3datasets.sh (which uses MODE=native).
#
# Cluster config (override via environment):
#   KAFKA_HOST         coordinator IP reachable by all nodes (repo ships an
#                      RFC 5737 placeholder; set the real IP for your cluster)
#   FDB_CLUSTER_FILE   defaults to ~/zktel-dist/fdb.cluster
# Requires the aggregator binary deployed to each node's ~/zktel-dist/bin
# (see scripts/setup/setup_remote_e2e.sh).
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT_DIR/results/_dist_metrics.jsonl"
OUTDIR="$ROOT_DIR/results/e2e_dev_zk"; mkdir -p "$OUTDIR"
source "$ROOT_DIR/scripts/lib/common.sh"
export AGG_MAX_WAIT="${AGG_MAX_WAIT:-1800}" AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-20}"
nodes_for(){ local n="$1" o=""; for ((i=0;i<n;i++)); do o="$o node$i"; done; echo "${o# }"; }
LOG=/tmp/e2e_dev_zk.log; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# dataset:nodes (paper-faithful aggregator counts)
for spec in google:8 caida:8 vehicle:4; do
  IFS=: read -r ds n <<< "$spec"
  say "=== ZK dev-mode e2e: $ds ($n aggregators) ==="
  : > "$MET"
  env DATASET="$ds" NODES="$(nodes_for "$n")" MODE=zk RISC0_DEV_MODE=1 bash "$DRV" >> "$LOG" 2>&1 \
    || say "  driver error for $ds"
  cp "$MET" "$OUTDIR/${ds}_dev_zk.jsonl"
  say "  saved $OUTDIR/${ds}_dev_zk.jsonl"
done
say "=== ZK dev-mode e2e done ==="
