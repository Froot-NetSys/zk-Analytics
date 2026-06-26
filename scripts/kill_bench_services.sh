#!/usr/bin/env bash
set -euo pipefail

# Kill benchmark-related services (querier, aggregator).
#
# Usage:
#   ./scripts/kill_bench_services.sh
#

kill_pattern() {
  local pattern="$1"
  if pgrep -f "$pattern" >/dev/null 2>&1; then
    echo "Killing processes matching: $pattern"
    pkill -f "$pattern" || true
  fi
}

# Querier/aggregator (cargo run or direct binaries).
kill_pattern "cargo run -p querier"
kill_pattern "cargo run -p aggregator"
kill_pattern "querier"
kill_pattern "aggregator"
kill_pattern "/target/release/querier"
kill_pattern "/target/debug/querier"
kill_pattern "/target/release/aggregator"
kill_pattern "/target/debug/aggregator"

echo "Done."
