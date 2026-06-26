#!/usr/bin/env bash
set -euo pipefail

# Kill all zk-Analytics benchmark-related processes for the RISC0 querier benches.
#
# This is intentionally broad (kills by pattern). Use with care.
#
# Usage:
#   ./querier/scripts/kill_all.sh
#
# Options:
#   DRY_RUN=1                 Print matches, don't kill.
#   ROCKSDB_PATH=/mydata/...  Where to look for a RocksDB LOCK file to remove.
#   KILL_DB_LOCK=0            Do not remove RocksDB LOCK file (default: 1).

DRY_RUN="${DRY_RUN:-0}"
KILL_DB_LOCK="${KILL_DB_LOCK:-1}"
ROCKSDB_PATH="${ROCKSDB_PATH:-/mydata/rocksdb}"

patterns=(
  # RISC0 querier + helpers.
  "querier"
  "querier-host"
  "aggregator"
  "data_source"
  "/target/release/querier"
  "/target/debug/querier"
  "/target/release/querier-host"
  "/target/debug/querier-host"
  "/target/release/aggregator"
  "/target/debug/aggregator"
  "/target/release/data_source"
  "/target/debug/data_source"
  "cargo run -p querier"
  "cargo run -p querier-host"
  "cargo run -p aggregator"
  "cargo run -p aggregator --bin aggregator"
  "cargo run -p data_source"

  # Nova/non-RISC0 services used by some RISC0 bench scripts.
  "target/release/aggregator"
  "target/debug/aggregator"
  "cargo run -p aggregator"

  # Bench drivers.
  "bench_samples_epoch"
  "bench_cm_epoch"
  "bench_histogram_epoch"
  "bench env"
  "BENCH_REQUEST="
  "BENCH_PRINT="
)

kill_pids() {
  local pat="$1"
  local pids
  pids="$(pgrep -f "$pat" || true)"
  if [[ -z "$pids" ]]; then
    return
  fi

  if [[ "$DRY_RUN" == "1" ]]; then
    echo "match: $pat"
    ps -o pid=,command= -p $pids 2>/dev/null || true
    return
  fi

  echo "killed: $pat"
  kill -TERM $pids >/dev/null 2>&1 || true
  sleep 0.2
  kill -KILL $pids >/dev/null 2>&1 || true
}

killed_any=0
for pat in "${patterns[@]}"; do
  if pgrep -f "$pat" >/dev/null 2>&1; then
    killed_any=1
  fi
  kill_pids "$pat"
done

if [[ "$killed_any" -eq 0 ]]; then
  echo "no matching processes found"
fi

if [[ "$KILL_DB_LOCK" == "1" ]]; then
  lock_path="${ROCKSDB_PATH}/LOCK"
  if [[ -f "$lock_path" ]]; then
    if [[ "$DRY_RUN" == "1" ]]; then
      echo "would remove RocksDB lock: $lock_path"
    else
      rm -f "$lock_path"
      echo "removed RocksDB lock: $lock_path"
    fi
  fi
fi
