#!/usr/bin/env bash
set -euo pipefail
# Table 2 Vanilla (non-ZK) runs at the SAME end-to-end setups as Figure 4:
# Google/hash-table/8 aggregators, CAIDA/CM/8, Vehicle/histogram/4.
# Full real pipeline (data source w/ hash-chain commitment -> Kafka -> RocksDB ->
# aggregator -> FoundationDB -> querier). Captures RocksDB/FDB read+write timing,
# native aggregation time, and native query time per dataset.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT"
DRV="$ROOT/scripts/distributed/run_distributed_baseline.sh"
MET="$ROOT/results/_dist_metrics.jsonl"
OUTDIR="$ROOT/results/table2_native"; mkdir -p "$OUTDIR"
OUT="$ROOT/results/table2_native.csv"
source "$ROOT/scripts/lib/common.sh"
source "$ROOT/scripts/lib/artifact_cluster.sh"
artifact_cluster_load "$ROOT"
export AGG_MAX_WAIT=900 AGGR_IDLE_TIMEOUT_SECS=20
LOG=/tmp/e2e_native.log; : > "$LOG"
say(){ echo "[$(date +%H:%M:%S)] $*" | tee -a "$LOG"; }

require_dataset() {
  case "$1" in
    google)
      if [[ ! -d "$ROOT/testdata/google_cluster_data/input" ]] ||
          ! compgen -G "$ROOT/testdata/google_cluster_data/input/*.csv" >/dev/null; then
        echo "[table2] missing Google Cluster CSVs under testdata/google_cluster_data/input/" >&2
        exit 2
      fi
      ;;
    caida)
      if [[ ! -d "$ROOT/testdata/caida_pcap/caida_txt" ]] ||
          ! compgen -G "$ROOT/testdata/caida_pcap/caida_txt/*.txt" >/dev/null; then
        echo "[table2] missing CAIDA text traces under testdata/caida_pcap/caida_txt/" >&2
        exit 2
      fi
      ;;
    vehicle)
      if [[ ! -f "$ROOT/testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv" ]]; then
        echo "[table2] missing bundled vehicle-emissions CSV" >&2
        exit 2
      fi
      ;;
  esac
}

TABLE2_SPECS_VALUE="${TABLE2_SPECS:-google:8 caida:8 vehicle:4}"
for spec in $TABLE2_SPECS_VALUE; do
  IFS=: read -r ds _ <<< "$spec"
  require_dataset "$ds"
done

echo "dataset,num_aggregators,epochs_on_critical_node,rocksdb_insert_ms_per_epoch,rocksdb_read_ms_per_epoch,fdb_write_ms_per_epoch,fdb_read_ms_per_query,aggregation_compute_ms_per_epoch,query_ms_per_query,agg_peak_rss_mb_per_node,query_peak_rss_mb" > "$OUT"

emit_row() {
  local dataset="$1"
  python3 - "$MET" "$dataset" >> "$OUT" <<'PY'
import json, sys
rows = [json.loads(line) for line in open(sys.argv[1]) if line.strip()]
agg = next(row for row in rows if row.get("task") == "aggregation")
query = next(row for row in rows if row.get("task") == "query")
n = int(agg["num_aggregators"])
epochs = int(agg.get("critical_path_epochs") or 0)
if epochs <= 0:
    raise SystemExit(f"invalid critical-path epoch count: {agg}")
ac = agg.get("components_s", {})
qc = query.get("components_s", {})
per_epoch_ms = lambda key: 1000.0 * float(ac.get(key, 0.0)) / epochs
values = [
    sys.argv[2], n, epochs,
    per_epoch_ms("rocksdb_raw_insert"),
    per_epoch_ms("rocksdb_raw_read"),
    per_epoch_ms("fdb_write"),
    1000.0 * float(qc.get("fdb_lookup", 0.0)),
    per_epoch_ms("aggr_compute"),
    1000.0 * float(query.get("total_time_s", 0.0)),
    float(agg.get("per_node_host_rss_mb", 0.0)),
    float(query.get("host_peak_rss_mb", 0.0)),
]
print(",".join(str(round(value, 3)) if isinstance(value, float) else str(value)
               for value in values))
PY
}

# dataset:nodes. These are measurements on real machines, not inferred rows.
for spec in $TABLE2_SPECS_VALUE; do
  IFS=: read -r ds n <<< "$spec"
  selected_nodes="$(artifact_nodes_for "$n")"
  say "=== Table 2 Vanilla: $ds ($n aggregators on [$selected_nodes]) ==="
  : > "$MET"
  if ! env DATASET="$ds" NODES="$selected_nodes" MODE=native \
      MEM_INTERVAL="${TABLE2_MEM_INTERVAL:-0.01}" bash "$DRV" 2>&1 | tee -a "$LOG"; then
    say "  driver error for $ds"
    exit 1
  fi
  cp "$MET" "$OUTDIR/${ds}_native.jsonl"
  emit_row "$ds"
  say "  saved $OUTDIR/${ds}_native.jsonl"
done
say "=== Table 2 Vanilla done -> $OUT ==="
