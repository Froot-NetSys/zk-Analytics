#!/usr/bin/env bash
# Checks actual database availability, not just the coordinator's TCP socket.
set -euo pipefail
cluster_file="${1:?cluster file required}"
test -r "$cluster_file"
timeout 20 fdbcli -C "$cluster_file" --exec 'status json' |
  python3 -c 'import json,sys
s=json.load(sys.stdin)
if not s.get("client", {}).get("database_status", {}).get("available", False):
    sys.exit("FoundationDB unavailable from this machine; check advertised server addresses and routing")'
