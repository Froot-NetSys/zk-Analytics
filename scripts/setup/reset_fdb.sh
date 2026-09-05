#!/usr/bin/env bash
set -euo pipefail

# Reset zk-Analytics FoundationDB storage.
#
# Usage:
#   ./scripts/setup/reset_fdb.sh [subspace]
#   FDB_CLUSTER_FILE=/etc/foundationdb/fdb.cluster ./scripts/setup/reset_fdb.sh
#
# Arguments:
#   subspace (optional): FDB subspace to clear (default: zktelemetry or $FDB_SUBSPACE)
#
# Env vars:
#   FDB_CLUSTER_FILE (default: /etc/foundationdb/fdb.cluster)
#   FDB_SUBSPACE (default: zktelemetry)

FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
FDB_SUBSPACE="${1:-${FDB_SUBSPACE:-zktelemetry}}"
FDB_RESET_TIMEOUT_SEC="${FDB_RESET_TIMEOUT_SEC:-30}"

if [[ ! -f "$FDB_CLUSTER_FILE" ]]; then
  echo "Error: FDB_CLUSTER_FILE not found: $FDB_CLUSTER_FILE" >&2
  exit 1
fi

echo "[i] Clearing FDB subspace '${FDB_SUBSPACE}' using cluster file ${FDB_CLUSTER_FILE}..."

# Use fdbcli to clear the subspace range
# The subspace key is the prefix, so we clear from "prefix" to "prefix\xff"
if command -v fdbcli >/dev/null 2>&1; then
  # fdbcli requires one \\xNN escape per byte. A single \\x followed by
  # xxd's hex dump both misencodes the key and wraps long prefixes into tokens.
  subspace_key=$(python3 - "$FDB_SUBSPACE" <<'PY'
import os, sys
print(''.join(r'\x%02x' % byte for byte in os.fsencode(sys.argv[1])))
PY
  )
  # End key is subspace + \xff (255)
  end_key="${subspace_key}\\xff"

  if command -v timeout >/dev/null 2>&1; then
    timeout "${FDB_RESET_TIMEOUT_SEC}" \
      fdbcli -C "$FDB_CLUSTER_FILE" --exec "writemode on; clearrange ${subspace_key} ${end_key}"
  else
    fdbcli -C "$FDB_CLUSTER_FILE" --exec "writemode on; clearrange ${subspace_key} ${end_key}"
  fi
  echo "[i] Done."
else
  echo "Error: fdbcli not found. Please install FoundationDB client tools." >&2
  echo "Alternatively, you can use the Rust FDB store's clear_all() method." >&2
  exit 1
fi
