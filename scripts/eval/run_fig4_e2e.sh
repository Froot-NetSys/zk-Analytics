#!/usr/bin/env bash
set -uo pipefail
# Figure 4 end-to-end runner. The default is the bundled vehicle-emissions
# workload with four aggregators; override FIG4_SPECS to add other datasets.
# Runs the full distributed pipeline (data source -> Kafka -> RocksDB ->
# aggregator -> FoundationDB -> querier). RISC0_DEV_MODE=1 (the default)
# executes the guests without STARK proofs; RISC0_DEV_MODE=0 generates real
# proofs.
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
DEV_MODE="${RISC0_DEV_MODE:-1}"
if [ "$DEV_MODE" = 1 ]; then RUN_KIND=dev_zk; else RUN_KIND=real_zk; fi
OUTDIR="$ROOT_DIR/results/e2e_${RUN_KIND}"; mkdir -p "$OUTDIR"
source "$ROOT_DIR/scripts/lib/common.sh"
if [ "$DEV_MODE" = 1 ]; then
  export AGG_MAX_WAIT="${AGG_MAX_WAIT:-1800}"
else
  export AGG_MAX_WAIT="${AGG_MAX_WAIT:-20000}"
fi
export AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-20}"
nodes_for(){ local n="$1" o=""; for ((i=0;i<n;i++)); do o="$o node$i"; done; echo "${o# }"; }
LOG="/tmp/e2e_${RUN_KIND}.log"; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

# dataset:nodes (paper-faithful aggregator counts). Only vehicle is bundled.
for spec in ${FIG4_SPECS:-vehicle:4}; do
  IFS=: read -r ds n <<< "$spec"
  say "=== Figure 4 $RUN_KIND e2e: $ds ($n aggregators) ==="
  : > "$MET"
  if ! env DATASET="$ds" NODES="$(nodes_for "$n")" MODE=zk RISC0_DEV_MODE="$DEV_MODE" \
      bash "$DRV" 2>&1 | tee -a "$LOG"; then
    say "  driver error for $ds"
    exit 1
  fi
  cp "$MET" "$OUTDIR/${ds}_${RUN_KIND}.jsonl"
  say "  saved $OUTDIR/${ds}_${RUN_KIND}.jsonl"
done
say "=== Figure 4 $RUN_KIND e2e done ==="
