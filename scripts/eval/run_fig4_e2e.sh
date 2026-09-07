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
#   KAFKA_HOST         coordinator hostname/IP reachable by all nodes
#                      (default: node0, configurable via the shared config)
#   FDB_CLUSTER_FILE   defaults to ~/zktel-dist/fdb.cluster
# Requires the shared artifact cluster configuration and worker deployment
# created by scripts/setup/setup_artifact_cluster.sh.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
DRV="$ROOT_DIR/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT_DIR/results/_dist_metrics.jsonl"
DEV_MODE="${RISC0_DEV_MODE:-1}"
if [ "$DEV_MODE" = 1 ]; then RUN_KIND=dev_zk; else RUN_KIND=real_zk; fi
OUTDIR="$ROOT_DIR/results/e2e_${RUN_KIND}"; mkdir -p "$OUTDIR"
source "$ROOT_DIR/scripts/lib/common.sh"
source "$ROOT_DIR/scripts/lib/artifact_cluster.sh"
artifact_cluster_load "$ROOT_DIR"
if [ "$DEV_MODE" = 1 ]; then
  export AGG_MAX_WAIT="${AGG_MAX_WAIT:-1800}"
else
  export AGG_MAX_WAIT="${AGG_MAX_WAIT:-0}"
fi
export AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-20}"
LOG="/tmp/e2e_${RUN_KIND}.log"; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

print_metrics() {
  python3 - "$MET" <<'PY'
import json, sys

def flatten(value, prefix=""):
    for key, item in value.items():
        name = f"{prefix}.{key}" if prefix else key
        if isinstance(item, dict):
            yield from flatten(item, name)
        else:
            yield name, item

with open(sys.argv[1]) as f:
    records = [json.loads(line) for line in f if line.strip()]
if not records:
    raise SystemExit(f"no metrics found in {sys.argv[1]}")
for record in records:
    print(f"[fig4-result] task={record.get('task', 'unknown')}")
    for name, value in flatten(record):
        print(f"  {name}={value}")
PY
}

# dataset:nodes (paper-faithful aggregator counts). Only vehicle is bundled.
for spec in ${FIG4_SPECS:-vehicle:4}; do
  IFS=: read -r ds n <<< "$spec"
  selected_nodes="$(artifact_nodes_for "$n")" || exit $?
  say "=== Figure 4 $RUN_KIND e2e: $ds ($n aggregators on [$selected_nodes]) ==="
  : > "$MET"
  if ! env DATASET="$ds" NODES="$selected_nodes" MODE=zk RISC0_DEV_MODE="$DEV_MODE" \
      bash "$DRV" 2>&1 | tee -a "$LOG"; then
    say "  driver error for $ds"
    exit 1
  fi
  print_metrics | tee -a "$LOG"
  cp "$MET" "$OUTDIR/${ds}_${RUN_KIND}.jsonl"
  say "  saved $OUTDIR/${ds}_${RUN_KIND}.jsonl"
done
say "=== Figure 4 $RUN_KIND e2e done ==="
