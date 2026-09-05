#!/usr/bin/env bash
set -euo pipefail

# Configure the shared cluster used by Figure 4, Table 2, Figure 5, and Table 3.
# The first machine is the local coordinator; every later entry is one SSH
# worker. The generated .artifact-cluster.env is loaded automatically by all
# artifact distributed runners.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONFIG_FILE="${ARTIFACT_CONFIG_FILE:-$ROOT/.artifact-cluster.env}"
MACHINES="${ARTIFACT_MACHINES:-}"
SSH_USER_VALUE="${ARTIFACT_SSH_USER:-${USER:-}}"
KAFKA_VALUE="${ARTIFACT_KAFKA_HOST:-${KAFKA_HOST:-}}"
FDB_SOURCE="${ARTIFACT_FDB_SOURCE:-${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}}"
COPY_KEYS=0
INSTALL_DEPS=0
DEPLOY=0

usage() {
  cat <<'EOF'
Usage:
  setup_artifact_cluster.sh --machines "node0 host1 ... host7" \
    --ssh-user USER --kafka-host COORDINATOR_IP [--fdb-cluster-file FILE] \
    [--copy-keys] [--install-deps] [--deploy]

The first machine is the local coordinator and is never contacted over SSH.
Each remaining machine runs exactly one aggregator. --copy-keys may prompt for
the workers' passwords. --install-deps requires passwordless sudo and installs
system build packages, Rust, RISC Zero, and the FoundationDB client on every
worker without starting a worker-local Kafka/FDB server. --deploy builds locally
and installs the required binaries, memory tracer, and FDB cluster file on every
worker.
EOF
}

while (( $# )); do
  case "$1" in
    --machines) MACHINES="${2:?missing value}"; shift 2 ;;
    --ssh-user) SSH_USER_VALUE="${2:?missing value}"; shift 2 ;;
    --kafka-host) KAFKA_VALUE="${2:?missing value}"; shift 2 ;;
    --fdb-cluster-file) FDB_SOURCE="${2:?missing value}"; shift 2 ;;
    --copy-keys) COPY_KEYS=1; shift ;;
    --install-deps) INSTALL_DEPS=1; shift ;;
    --deploy) DEPLOY=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ -n "$MACHINES" ]] || { echo "--machines is required" >&2; exit 2; }
[[ -n "$SSH_USER_VALUE" ]] || { echo "--ssh-user is required" >&2; exit 2; }
[[ -n "$KAFKA_VALUE" ]] || { echo "--kafka-host is required" >&2; exit 2; }
read -r -a INPUT_MACHINES <<< "$MACHINES"
(( ${#INPUT_MACHINES[@]} >= 1 )) || { echo "machine list is empty" >&2; exit 2; }

ssh_target() {
  local host="$1"
  if [[ "$host" == *@* ]]; then printf '%s\n' "$host"; else printf '%s@%s\n' "$SSH_USER_VALUE" "$host"; fi
}

NORMALIZED=("${INPUT_MACHINES[0]}")
declare -A SEEN=(["${INPUT_MACHINES[0]}"]=1)
for host in "${INPUT_MACHINES[@]:1}"; do
  target="$(ssh_target "$host")"
  [[ -z "${SEEN[$target]:-}" ]] || { echo "duplicate worker: $target" >&2; exit 2; }
  SEEN[$target]=1
  NORMALIZED+=("$target")
done

if ! timeout 5 bash -c ">/dev/tcp/${KAFKA_VALUE}/9092" 2>/dev/null; then
  echo "Kafka ${KAFKA_VALUE}:9092 is not reachable from the coordinator" >&2
  exit 2
fi

if (( COPY_KEYS )); then
  if [[ ! -f "$HOME/.ssh/id_ed25519" && ! -f "$HOME/.ssh/id_rsa" ]]; then
    ssh-keygen -t ed25519 -f "$HOME/.ssh/id_ed25519" -N ""
  fi
  for target in "${NORMALIZED[@]:1}"; do
    echo "[artifact-setup] installing SSH key on $target"
    ssh-copy-id "$target"
  done
fi

if (( INSTALL_DEPS )); then
  for target in "${NORMALIZED[@]:1}"; do
    ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$target" true || {
      echo "passwordless SSH is not configured for $target; use --copy-keys first" >&2
      exit 2
    }
    ssh -n -o BatchMode=yes "$target" "sudo -n true" || {
      echo "passwordless sudo is required to install dependencies on $target" >&2
      exit 2
    }
    echo "[artifact-setup] installing worker dependencies on $target"
    scp -q -o BatchMode=yes "$ROOT/scripts/setup/setup_local_e2e.sh" "$target:/tmp/zk_analytics_setup_local_e2e.sh"
    ssh -n -o BatchMode=yes "$target" "chmod +x /tmp/zk_analytics_setup_local_e2e.sh && /tmp/zk_analytics_setup_local_e2e.sh --deps"
    ssh -n -o BatchMode=yes "$target" "/tmp/zk_analytics_setup_local_e2e.sh --fdb-client"
    ssh -n -o BatchMode=yes "$target" "bash -lc '/tmp/zk_analytics_setup_local_e2e.sh --risc0'"
  done
fi

REMOTE_HOME=""
for target in "${NORMALIZED[@]:1}"; do
  echo "[artifact-setup] checking passwordless SSH to $target"
  ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$target" true
  home="$(ssh -n -o BatchMode=yes "$target" 'printf %s "$HOME"')"
  if [[ -z "$REMOTE_HOME" ]]; then
    REMOTE_HOME="$home"
  elif [[ "$home" != "$REMOTE_HOME" ]]; then
    echo "workers have different home directories ($REMOTE_HOME and $home); use one SSH user on all workers" >&2
    exit 2
  fi
  ssh -n -o BatchMode=yes "$target" "timeout 5 bash -c '>/dev/tcp/${KAFKA_VALUE}/9092'" || {
    echo "Kafka ${KAFKA_VALUE}:9092 is not reachable from $target" >&2
    exit 2
  }
  ssh -n -o BatchMode=yes "$target" "test -x '$home/.cargo/bin/r0vm' || test -x '$home/.risc0/bin/r0vm'" || {
    echo "r0vm is not installed for $target; rerun setup with --install-deps" >&2
    exit 2
  }
done

REMOTE_DIST="${REMOTE_HOME:+$REMOTE_HOME/zktel-dist}"
if (( ${#NORMALIZED[@]} == 1 )); then
  REMOTE_DIST="$HOME/zktel-dist"
fi

if (( DEPLOY )); then
  [[ -f "$FDB_SOURCE" ]] || { echo "FDB cluster file not found: $FDB_SOURCE" >&2; exit 2; }
  echo "[artifact-setup] building coordinator binaries"
  (cd "$ROOT" && \
    cargo build --release -p data_source --features kafka --bin kafka-producer && \
    cargo build --release -p aggregator --features "kafka fdb" \
      --bin aggregator --bin kafka-consumer && \
    cargo build --release -p querier --features fdb --bin querier)
  for target in "${NORMALIZED[@]:1}"; do
    echo "[artifact-setup] deploying worker files to $target:$REMOTE_DIST"
    ssh -n "$target" "mkdir -p '$REMOTE_DIST/bin' '$REMOTE_DIST/lib'"
    scp -q "$ROOT/target/release/aggregator" "$ROOT/target/release/kafka-consumer" \
      "$ROOT/scripts/lib/mem_trace.py" "$target:$REMOTE_DIST/bin/"
    scp -q "$FDB_SOURCE" "$target:$REMOTE_DIST/fdb.cluster"
    ssh -n "$target" "chmod +x '$REMOTE_DIST/bin/aggregator' '$REMOTE_DIST/bin/kafka-consumer' '$REMOTE_DIST/bin/mem_trace.py'"
    ssh -n "$target" "test -x '$REMOTE_DIST/bin/aggregator' && test -x '$REMOTE_DIST/bin/kafka-consumer' && test -x '$REMOTE_DIST/bin/mem_trace.py' && test -r '$REMOTE_DIST/fdb.cluster'"
  done
fi

machine_string="${NORMALIZED[*]}"
mkdir -p "$(dirname "$CONFIG_FILE")"
{
  printf '# Generated by scripts/setup/setup_artifact_cluster.sh\n'
  printf 'ARTIFACT_MACHINES=%q\n' "$machine_string"
  printf 'ARTIFACT_KAFKA_HOST=%q\n' "$KAFKA_VALUE"
  printf 'ARTIFACT_FDB_CLUSTER_FILE=%q\n' "$FDB_SOURCE"
  printf 'ARTIFACT_REMOTE_DIST_DIR=%q\n' "$REMOTE_DIST"
} > "$CONFIG_FILE"
chmod 600 "$CONFIG_FILE"

echo "[artifact-setup] wrote $CONFIG_FILE"
echo "[artifact-setup] machine pool: $machine_string"
echo "[artifact-setup] one aggregator will run on each selected machine"
