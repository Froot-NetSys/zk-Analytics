#!/usr/bin/env bash
# Smoke test: bring up the local Trillian stack, create a log (tree), append
# data-source checkpoints through the real gRPC path, and assert success.
#
#   scripts/util/trillian_smoke.sh          # up -> create tree -> smoke -> down
#   KEEP=1 scripts/util/trillian_smoke.sh   # leave the stack running afterwards
#
# Requires: docker (with the compose plugin), cargo, curl, and network access to
# pull the Trillian images + build createtree from the golang image.
# Verified round-trip: client QueueLeaf -> log server -> signer -> MySQL, with
# the stored LeafValue matching the canonical checkpoint encoding.
set -euo pipefail
cd "$(dirname "$0")/../.."
COMPOSE="docker compose -f deploy/trillian/docker-compose.yml"
ADMIN=127.0.0.1:8090
ADDR=http://127.0.0.1:8090
# Trillian >= v1.7.3 requires Go >= 1.25 to build createtree.
GOIMG=golang:1.25-alpine

cleanup() { [ "${KEEP:-0}" = "1" ] || { echo "[smoke] tearing down"; $COMPOSE down -v >/dev/null 2>&1 || true; }; }
trap cleanup EXIT

echo "[smoke] starting Trillian stack"
$COMPOSE up -d

echo "[smoke] waiting for log server health on :8091"
for i in $(seq 1 60); do
  [ "$(curl -fsS -m2 http://127.0.0.1:8091/healthz 2>/dev/null || true)" = "ok" ] && { echo "[smoke] log server ready"; break; }
  sleep 2
  [ "$i" = 60 ] && { echo "[smoke] log server did not become ready"; exit 1; }
done

echo "[smoke] creating a log (tree) via createtree"
TREE_ID=$(docker run --rm --network host "$GOIMG" sh -c "
  go install github.com/google/trillian/cmd/createtree@latest >/dev/null 2>&1
  createtree --admin_server=$ADMIN
" | grep -oE '^[0-9]+' | tail -1)
echo "[smoke] TREE_ID=$TREE_ID"
[ -n "$TREE_ID" ] || { echo "[smoke] failed to create tree"; exit 1; }

echo "[smoke] building + running the checkpoint smoke test"
TRILLIAN_ADDR="$ADDR" TRILLIAN_LOG_ID="$TREE_ID" CHECKPOINT_INTERVAL=1 \
  cargo run -q -p data_source --features trillian --bin trillian-smoke

echo "[smoke] PASS"
