# Distributed Benchmarking Setup Guide

This guide explains how to set up and run zk-Analytics benchmarks across multiple machines.

## Architecture

```
┌─────────────┐
│   Producer  │ (Local machine)
│   (Kafka)   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────┐
│   Kafka Cluster (Partitioned)   │
└─────────────┬───────────────────┘
              │
      ┌───────┴───────┬───────────┬───────────┐
      ▼               ▼           ▼           ▼
┌───────────┐   ┌───────────┐   ...   ┌───────────┐
│ Aggregator│   │ Aggregator│         │ Aggregator│
│ Machine 1 │   │ Machine 2 │         │ Machine N │
└─────┬─────┘   └─────┬─────┘         └─────┬─────┘
      │               │                     │
      └───────────────┴─────────────────────┘
                      │
                      ▼
              ┌───────────────┐
              │  FoundationDB │
              └───────────────┘
```

## Prerequisites

### On Your Local Machine

1. SSH access to all remote machines
2. FoundationDB cluster file configured
3. Kafka cluster running and accessible from all machines
4. zk-Analytics project cloned

### On Remote Machines

The setup script will install:
- Rust toolchain
- FoundationDB client
- System dependencies (build tools, cmake, etc.)
- zk-Analytics project and build it

## Step 1: Configure SSH Key-Based Authentication

First, set up SSH keys for passwordless access to remote machines:

```bash
# Generate SSH key if you don't have one
ssh-keygen -t ed25519 -C "your_email@example.com"

# Copy SSH key to each remote machine
ssh-copy-id ubuntu@192.0.2.1
ssh-copy-id ubuntu@192.0.2.2
# ... repeat for all machines

# Test SSH access
ssh ubuntu@192.0.2.1 "echo 'SSH OK'"
```

## Step 2: Configure Your Environment

Create a configuration file based on the example:

```bash
cp scripts/example_distributed_setup.sh scripts/my_distributed_setup.sh
```

Edit `scripts/my_distributed_setup.sh`:

```bash
# List your actual machine IPs
export REMOTE_MACHINES="192.0.2.1 192.0.2.2 192.0.2.3"

# Your SSH username
export SSH_USER="ubuntu"

# Where to install the project on remote machines
export REMOTE_PROJECT_DIR="/home/ubuntu/zk-Analytics"

# Your Kafka broker address (must be accessible from all machines)
export KAFKA_BROKERS="192.0.2.100:9092"

# Path to your FDB cluster file
export FDB_CLUSTER_FILE="/etc/foundationdb/fdb.cluster"
```

## Step 3: Run the Remote Setup Script

The setup script will:
- Test SSH connectivity to all machines
- Install dependencies (Rust, FDB client, build tools)
- Sync the zk-Analytics project
- Build the project on each machine
- Copy the FDB cluster file
- Verify everything is working

```bash
# Source your config
source scripts/my_distributed_setup.sh

# Run the setup (choose sequential for easier debugging)
./scripts/setup_remote_e2e.sh
```

**Note:** The first run will take 10-20 minutes per machine due to Rust compilation.

## Step 4: Run Distributed Benchmarks

Once setup is complete, run benchmarks:

```bash
# Source your config
source scripts/my_distributed_setup.sh

# Run benchmark with different aggregator counts
AGGREGATOR_MACHINES="$REMOTE_MACHINES" \
SSH_USER="$SSH_USER" \
REMOTE_PROJECT_PATH="$REMOTE_PROJECT_DIR" \
KAFKA_BROKERS="$KAFKA_BROKERS" \
NUM_AGGREGATORS="1 2 4 8" \
REPEATS="3" \
WARMUP_EVENTS="10000" \
./scripts/bench_distributed_aggregators.sh
```

## Understanding the Distribution

Aggregators are distributed using **round-robin assignment**:

- With 3 machines and 8 aggregators:
  - Machine 1: aggregators 0, 3, 6
  - Machine 2: aggregators 1, 4, 7
  - Machine 3: aggregators 2, 5

- Each aggregator joins the same Kafka consumer group
- Kafka automatically assigns partitions to aggregators
- All aggregators write to the same FDB cluster

## Configuration Options

### Benchmark Parameters

```bash
NUM_AGGREGATORS="1 2 4 8"      # Test with these aggregator counts
SERIES=1024                     # Number of distinct keys
SAMPLES_PER_SERIES=128          # Samples per key
KAFKA_PARTITIONS=16             # Should be >= max aggregators
REPEATS=3                       # Repetitions per config
WARMUP_EVENTS=10000             # Warmup events before measurement
EPOCH_TYPE="samples"            # samples, histogram, or cm
BATCH_SIZE=100                  # Events per batch
```

### Machine Configuration

```bash
AGGREGATOR_MACHINES="..."       # Space-separated machine list
SSH_USER="ubuntu"               # SSH username
REMOTE_PROJECT_PATH="/path"     # Remote project location
KAFKA_BROKERS="host:port"       # Kafka broker addresses
FDB_CLUSTER_FILE="/path"        # FDB cluster file path
```

## Common Tasks

### Update Code on All Machines

After making code changes, quickly sync without full rebuild:

```bash
# Quick sync (doesn't rebuild)
for machine in $REMOTE_MACHINES; do
    rsync -az --delete --exclude 'target/' . "${SSH_USER}@${machine}:${REMOTE_PROJECT_DIR}/"
done

# Then rebuild in parallel
for machine in $REMOTE_MACHINES; do
    ssh "${SSH_USER}@${machine}" "cd $REMOTE_PROJECT_DIR && \
        source ~/.cargo/env && \
        cargo build --release -p aggregator --features 'kafka fdb'" &
done
wait
```

### Check Status on All Machines

```bash
for machine in $REMOTE_MACHINES; do
    echo "=== $machine ==="
    ssh "${SSH_USER}@${machine}" "
        echo 'Rust: ' \$(rustc --version 2>/dev/null || echo 'not installed')
        echo 'FDB: ' \$(fdbcli --exec 'status minimal' 2>&1 | grep -o 'Healthy' || echo 'not connected')
        echo 'Disk: ' \$(df -h ~ | tail -1 | awk '{print \$4\" free\"}')"
done
```

### Monitor Running Benchmarks

```bash
# Watch logs on a specific machine
ssh ubuntu@192.0.2.1 "tail -f /tmp/zktelemetry_bench/*/aggregator*.log"

# Check resource usage
ssh ubuntu@192.0.2.1 "htop"

# Monitor all aggregator processes
for machine in $REMOTE_MACHINES; do
    echo "=== $machine ==="
    ssh "${SSH_USER}@${machine}" "ps aux | grep zktelemetry-risc0-aggr"
done
```

### Clean Up After Benchmarks

```bash
# Kill all aggregator processes
for machine in $REMOTE_MACHINES; do
    ssh "${SSH_USER}@${machine}" "pkill -f zktelemetry-risc0-aggr" &
done
wait

# Clean up temporary files
for machine in $REMOTE_MACHINES; do
    ssh "${SSH_USER}@${machine}" "rm -rf /tmp/zktelemetry_bench*" &
done
wait
```

## Troubleshooting

### SSH Connection Issues

```bash
# Test connectivity
ssh -v ubuntu@192.0.2.1

# Check SSH keys
ssh-add -l

# Re-copy SSH key
ssh-copy-id -i ~/.ssh/id_ed25519.pub ubuntu@192.0.2.1
```

### FDB Connection Issues

```bash
# Test FDB from remote machine
ssh ubuntu@192.0.2.1 "fdbcli --exec 'status'"

# Check cluster file
ssh ubuntu@192.0.2.1 "cat /etc/foundationdb/fdb.cluster"

# Verify network connectivity to FDB
ssh ubuntu@192.0.2.1 "nc -zv <fdb_host> 4500"
```

### Kafka Connection Issues

```bash
# Test Kafka connectivity
ssh ubuntu@192.0.2.1 "nc -zv <kafka_host> 9092"

# Check Kafka topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Check consumer group
docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
    --group bench_aggregators --describe
```

### Build Issues

```bash
# Clean and rebuild on a machine
ssh ubuntu@192.0.2.1 "cd ~/zk-Analytics && cargo clean && \
    cargo build --release -p aggregator --features 'kafka fdb'"

# Check disk space
ssh ubuntu@192.0.2.1 "df -h"
```

## Performance Tips

1. **Use Fast Networks**: Ensure machines have high-bandwidth, low-latency connections
2. **Separate Kafka and FDB**: Run Kafka and FDB on different machines from aggregators
3. **Use SSD Storage**: FDB performs best on SSD storage
4. **Monitor Resources**: Watch CPU, memory, and network during benchmarks
5. **Start Small**: Test with fewer machines/aggregators first

## Example: Full Workflow

```bash
# 1. Configure environment
cat > scripts/my_setup.sh <<'EOF'
export REMOTE_MACHINES="192.0.2.1 192.0.2.2 192.0.2.3"
export SSH_USER="ubuntu"
export REMOTE_PROJECT_DIR="/home/ubuntu/zk-Analytics"
export KAFKA_BROKERS="192.0.2.100:9092"
export FDB_CLUSTER_FILE="/etc/foundationdb/fdb.cluster"
EOF

# 2. Setup machines (first time only)
source scripts/my_setup.sh
./scripts/setup_remote_e2e.sh

# 3. Run benchmarks
source scripts/my_setup.sh
AGGREGATOR_MACHINES="$REMOTE_MACHINES" \
SSH_USER="$SSH_USER" \
REMOTE_PROJECT_PATH="$REMOTE_PROJECT_DIR" \
KAFKA_BROKERS="$KAFKA_BROKERS" \
NUM_AGGREGATORS="2 4 8" \
REPEATS="5" \
./scripts/bench_distributed_aggregators.sh

# 4. Results are saved to bench_csv/distributed/
ls -lh bench_csv/distributed/
```

## Security Considerations

- Use SSH keys, not passwords
- Consider using a VPN or private network for distributed testing
- Don't expose Kafka/FDB ports to the public internet
- Use firewall rules to restrict access between machines
- Keep FDB cluster file secure (contains connection credentials)

## Next Steps

After successful distributed benchmarking:
1. Analyze results in the CSV files
2. Experiment with different partition counts
3. Test with different workload characteristics
4. Monitor and optimize based on bottlenecks found
