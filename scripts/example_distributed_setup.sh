#!/usr/bin/env bash

# Example configuration for distributed benchmarking setup
# Copy this file and customize for your environment

# List of remote machines (space-separated IPs or hostnames)
export REMOTE_MACHINES="10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12"

# SSH username for remote machines
export SSH_USER="zz_y"

# Remote project directory (will be created if doesn't exist)
export REMOTE_PROJECT_DIR="/mydata/zk-Analytics"

# Kafka broker address (auto-detected from first machine)
_first_machine=$(echo $REMOTE_MACHINES | awk '{print $1}')
export KAFKA_BROKERS="${_first_machine}:9092"

# FDB cluster file to copy to remote machines
export FDB_CLUSTER_FILE="/etc/foundationdb/fdb.cluster"

echo "=========================================="
echo "  Distributed Setup Configuration"
echo "=========================================="
echo ""
echo "Remote machines: $REMOTE_MACHINES"
echo "SSH user:        $SSH_USER"
echo "Remote path:     $REMOTE_PROJECT_DIR"
echo "Kafka brokers:   $KAFKA_BROKERS"
echo ""
echo "=========================================="
echo ""

# Uncomment the action you want to perform:

# 1. Setup remote machines (first time or after code changes)
# ./scripts/setup_remote_e2e.sh

# 2. Run distributed benchmark
# AGGREGATOR_MACHINES="$REMOTE_MACHINES" \
# SSH_USER="$SSH_USER" \
# REMOTE_PROJECT_PATH="$REMOTE_PROJECT_DIR" \
# KAFKA_BROKERS="$KAFKA_BROKERS" \
# NUM_AGGREGATORS="1 2 4 8" \
# REPEATS="3" \
# ./scripts/bench_distributed_aggregators.sh

# 3. Just sync code changes (faster than full setup)
# for machine in $REMOTE_MACHINES; do
#     echo "Syncing to $machine..."
#     rsync -az --delete \
#         --exclude 'target/' \
#         --exclude '.git/' \
#         --exclude 'bench_csv/' \
#         --exclude 'bench_logs/' \
#         . "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/"
# done

# 4. Build on all machines in parallel
# for machine in $REMOTE_MACHINES; do
#     echo "Building on $machine..."
#     ssh "${SSH_USER}@${machine}" "cd $REMOTE_PROJECT_DIR && \
#         source ~/.cargo/env && \
#         cargo build --release -p aggregator --features 'kafka fdb'" &
# done
# wait
# echo "All builds complete"
