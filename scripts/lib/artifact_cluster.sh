#!/usr/bin/env bash
# Shared machine-pool configuration for the artifact's distributed experiments.
# Source this file, call artifact_cluster_load ROOT, then artifact_nodes_for N.

artifact_cluster_load() {
  local root="$1"
  local config="${ARTIFACT_CONFIG_FILE:-$root/.artifact-cluster.env}"
  local machines_override="${ARTIFACT_MACHINES:-}"
  local kafka_override="${ARTIFACT_KAFKA_HOST:-}"
  local fdb_override="${ARTIFACT_FDB_CLUSTER_FILE:-}"
  local dist_override="${ARTIFACT_REMOTE_DIST_DIR:-}"
  if [[ -f "$config" ]]; then
    # shellcheck disable=SC1090
    source "$config"
  fi

  [[ -z "$machines_override" ]] || ARTIFACT_MACHINES="$machines_override"
  [[ -z "$kafka_override" ]] || ARTIFACT_KAFKA_HOST="$kafka_override"
  [[ -z "$fdb_override" ]] || ARTIFACT_FDB_CLUSTER_FILE="$fdb_override"
  [[ -z "$dist_override" ]] || ARTIFACT_REMOTE_DIST_DIR="$dist_override"

  ARTIFACT_MACHINES="${ARTIFACT_MACHINES:-node0 node1 node2 node3 node4 node5 node6 node7}"
  ARTIFACT_KAFKA_HOST="${ARTIFACT_KAFKA_HOST:-node0}"
  if [[ -n "${ARTIFACT_KAFKA_HOST:-}" && -z "${KAFKA_HOST:-}" ]]; then
    KAFKA_HOST="$ARTIFACT_KAFKA_HOST"
    export KAFKA_HOST
  fi
  if [[ -n "${ARTIFACT_FDB_CLUSTER_FILE:-}" && -z "${FDB_CLUSTER_FILE:-}" ]]; then
    FDB_CLUSTER_FILE="$ARTIFACT_FDB_CLUSTER_FILE"
    export FDB_CLUSTER_FILE
  fi
  export ARTIFACT_REMOTE_DIST_DIR="${ARTIFACT_REMOTE_DIST_DIR:-}"

  read -r -a ARTIFACT_MACHINE_ARRAY <<< "$ARTIFACT_MACHINES"
  if (( ${#ARTIFACT_MACHINE_ARRAY[@]} == 0 )); then
    echo "[artifact-cluster] ARTIFACT_MACHINES is empty" >&2
    return 2
  fi
  declare -gA ARTIFACT_MACHINE_SEEN=()
  local machine
  for machine in "${ARTIFACT_MACHINE_ARRAY[@]}"; do
    if [[ -n "${ARTIFACT_MACHINE_SEEN[$machine]:-}" ]]; then
      echo "[artifact-cluster] duplicate machine: $machine" >&2
      return 2
    fi
    ARTIFACT_MACHINE_SEEN[$machine]=1
  done
}

artifact_nodes_for() {
  local n="$1"
  if [[ ! "$n" =~ ^[1-9][0-9]*$ ]]; then
    echo "[artifact-cluster] invalid aggregator count: $n" >&2
    return 2
  fi
  if (( n > ${#ARTIFACT_MACHINE_ARRAY[@]} )); then
    echo "[artifact-cluster] requested $n aggregators but the shared pool contains only ${#ARTIFACT_MACHINE_ARRAY[@]} machines" >&2
    return 2
  fi
  printf '%s' "${ARTIFACT_MACHINE_ARRAY[0]}"
  local i
  for ((i=1; i<n; i++)); do printf ' %s' "${ARTIFACT_MACHINE_ARRAY[i]}"; done
  printf '\n'
}
