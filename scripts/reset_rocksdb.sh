#!/usr/bin/env bash
set -euo pipefail

# Reset zk-Analytics RocksDB storage.
#
# Usage:
#   ROCKSDB_PATH=/mydata/rocksdb ./scripts/reset_rocksdb.sh
#
# Env vars:
#   ROCKSDB_PATH (default: /mydata/rocksdb)

ROCKSDB_PATH="${ROCKSDB_PATH:-/mydata/rocksdb}"
ROCKSDB_SECONDARY_PATH="${ROCKSDB_SECONDARY_PATH:-${ROCKSDB_PATH}_secondary}"

if [[ -z "$ROCKSDB_PATH" ]]; then
  echo "Error: ROCKSDB_PATH is empty." >&2
  exit 1
fi

echo "[i] Removing RocksDB directory at ${ROCKSDB_PATH}..."
rm -rf "$ROCKSDB_PATH"
mkdir -p "$ROCKSDB_PATH"

if [[ -n "$ROCKSDB_SECONDARY_PATH" && "$ROCKSDB_SECONDARY_PATH" != "$ROCKSDB_PATH" ]]; then
  echo "[i] Removing secondary RocksDB directory at ${ROCKSDB_SECONDARY_PATH}..."
  rm -rf "$ROCKSDB_SECONDARY_PATH"
  mkdir -p "$ROCKSDB_SECONDARY_PATH"
fi

echo "[i] Done."
