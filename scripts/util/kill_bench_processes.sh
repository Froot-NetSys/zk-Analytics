#!/usr/bin/env bash
# Kill all processes spawned by bench_distributed_aggregators.sh
#
# Processes killed:
#   - r0vm (RISC Zero VM for ZK proof generation)
#   - kafka-consumer (Kafka consumer aggregators)
#   - kafka-producer (Kafka event producer)
#   - aggregator (ZK aggregator host)
#
# Usage:
#   ./scripts/util/kill_bench_processes.sh                    # Kill on localhost only
#   ./scripts/util/kill_bench_processes.sh --all-machines     # Kill on all configured machines

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Source centralized IP defaults (single source of truth for machine config)
# shellcheck source=ip_defaults.sh
source "${SCRIPT_DIR}/../ip_defaults.sh"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }

# AGGREGATOR_MACHINES is sourced from ip_defaults.sh
SSH_USER="${SSH_USER:-$USER}"

# Processes to kill
PROCESS_PATTERNS=(
    "r0vm"
    "kafka-consumer"
    "kafka-producer"
    "aggregator"
)

kill_local_processes() {
    log_info "Killing benchmark processes on localhost..."

    for pattern in "${PROCESS_PATTERNS[@]}"; do
        local count=$(pgrep -f "$pattern" 2>/dev/null | wc -l)
        if [[ $count -gt 0 ]]; then
            log_info "  Killing $count process(es) matching '$pattern'"
            pkill -9 -f "$pattern" 2>/dev/null || true
        fi
    done

    log_info "Local cleanup complete"
}

kill_remote_processes() {
    log_info "Killing benchmark processes on all machines..."

    read -ra machines <<< "$AGGREGATOR_MACHINES"

    for machine in "${machines[@]}"; do
        if [[ "$machine" == "localhost" || "$machine" == "127.0.0.1" ]]; then
            kill_local_processes
        else
            log_info "  Killing processes on $machine..."
            # Build pattern for pkill -f with alternation
            local pattern_regex=$(IFS='|'; echo "${PROCESS_PATTERNS[*]}")
            ssh "${SSH_USER}@${machine}" "pkill -9 -f '$pattern_regex'" 2>/dev/null || true
        fi
    done

    log_info "Remote cleanup complete"
}

# Parse arguments
ALL_MACHINES=false
for arg in "$@"; do
    case $arg in
        --all-machines|-a)
            ALL_MACHINES=true
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Kill all processes spawned by bench_distributed_aggregators.sh"
            echo ""
            echo "Options:"
            echo "  --all-machines, -a  Kill on all configured machines (requires SSH)"
            echo "  --help, -h          Show this help message"
            echo ""
            echo "Environment variables:"
            echo "  AGGREGATOR_MACHINES  Space-separated list of machine IPs"
            echo "  SSH_USER             SSH username for remote machines"
            exit 0
            ;;
    esac
done

# Main
if [[ "$ALL_MACHINES" == "true" ]]; then
    kill_remote_processes
else
    kill_local_processes
fi

log_info "Done!"
