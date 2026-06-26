# Distributed End-to-End Evaluation Guide

This guide explains how to run comprehensive end-to-end tests across multiple machines to validate zk-Analytics's distributed functionality.

## Overview

The distributed E2E test validates the complete data flow:

```
Data Source (local) → Kafka → Aggregators (distributed) → FDB → Querier (local)
                                     ↓
                          Multiple machines processing in parallel
```

## Quick Start

### 1. Prerequisites

Ensure you've completed the distributed setup:

```bash
# Configure your environment
source scripts/my_setup.sh

# Setup remote machines (if not already done)
./scripts/setup_remote_e2e.sh
```

### 2. Run E2E Test

```bash
# Basic test with default configuration
REMOTE_MACHINES="10.10.1.1 10.10.1.2 10.10.1.3" \
SSH_USER="ubuntu" \
KAFKA_BROKERS="10.10.1.100:9092" \
./scripts/run_distributed_e2e.sh start
```

### 3. Monitor and Validate

```bash
# Check status of all components
./scripts/run_distributed_e2e.sh status

# Validate data consistency
./scripts/run_distributed_e2e.sh validate

# Generate detailed report
./scripts/run_distributed_e2e.sh report
```

### 4. Stop and Clean Up

```bash
# Stop all components
./scripts/run_distributed_e2e.sh stop

# Clean up logs and temporary files
./scripts/run_distributed_e2e.sh clean
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `REMOTE_MACHINES` | (required) | Space-separated list of remote machine IPs |
| `SSH_USER` | `$USER` | SSH username for remote machines |
| `REMOTE_PROJECT_DIR` | `~/zk-Analytics` | Project directory on remote machines |
| `KAFKA_BROKERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_TOPIC` | `raw_events` | Kafka topic name |
| `FDB_CLUSTER_FILE` | `/etc/foundationdb/fdb.cluster` | FDB cluster file path |
| `FDB_SUBSPACE` | `zktelemetry_dist_e2e` | FDB subspace for isolation |
| `NUM_AGGREGATORS` | `2` | Number of aggregators per machine |
| `EVENTS` | `1000000` | Total events to produce |
| `BATCH_SIZE` | `100` | Events per batch |
| `QUERIER_PORT` | `8082` | Querier HTTP API port |
| `EPOCH_TYPE` | `samples` | Aggregation type (samples/histogram/cm) |

### Example Configurations

#### Small Test (Quick Validation)
```bash
REMOTE_MACHINES="10.10.1.1 10.10.1.2" \
SSH_USER="ubuntu" \
NUM_AGGREGATORS=1 \
EVENTS=100000 \
./scripts/run_distributed_e2e.sh start
```

#### Medium Test (Typical Workload)
```bash
REMOTE_MACHINES="10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4" \
SSH_USER="ubuntu" \
NUM_AGGREGATORS=2 \
EVENTS=5000000 \
EPOCH_TYPE=histogram \
./scripts/run_distributed_e2e.sh start
```

#### Large Test (Stress Test)
```bash
REMOTE_MACHINES="10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.5 10.10.1.6 10.10.1.7 10.10.1.8" \
SSH_USER="ubuntu" \
NUM_AGGREGATORS=4 \
EVENTS=50000000 \
BATCH_SIZE=1000 \
./scripts/run_distributed_e2e.sh start
```

## What the Script Does

### 1. Setup Phase
- Validates configuration and connectivity
- Checks FoundationDB health
- Resets FDB subspace for clean test
- Creates Kafka topic with appropriate partitions

### 2. Deployment Phase
- Starts aggregators on all remote machines
  - Each aggregator joins the same Kafka consumer group
  - Kafka automatically distributes partitions among aggregators
  - All aggregators write to the same FDB cluster
- Starts local querier for data validation

### 3. Execution Phase
- Runs data source (Kafka producer) locally
- Produces specified number of events
- Monitors aggregator processing via Kafka consumer lag

### 4. Validation Phase
- Waits for all events to be processed
- Validates data consistency via queries
- Checks FDB for expected data

### 5. Reporting Phase
- Collects logs from all machines
- Calculates performance metrics
- Generates comprehensive report

## Commands

### start
Runs the complete end-to-end test:
```bash
./scripts/run_distributed_e2e.sh start
```

### stop
Stops all components (local and remote):
```bash
./scripts/run_distributed_e2e.sh stop
```

### status
Shows current status of all components:
```bash
./scripts/run_distributed_e2e.sh status
```

### validate
Runs data validation queries:
```bash
./scripts/run_distributed_e2e.sh validate
```

### report
Generates detailed evaluation report:
```bash
./scripts/run_distributed_e2e.sh report
```

### clean
Removes all logs and temporary files:
```bash
./scripts/run_distributed_e2e.sh clean
```

## Monitoring

### During Execution

Check status while the test is running:
```bash
# Overall status
./scripts/run_distributed_e2e.sh status

# Kafka consumer lag
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --group dist_e2e_aggregators

# Watch logs on a specific machine
ssh ubuntu@10.10.1.1 "tail -f /tmp/zktelemetry_agg_*/aggregator.log"
```

### After Completion

Review the generated report:
```bash
# Latest report
ls -lt bench_csv/distributed_e2e/

# View report
cat bench_csv/distributed_e2e/e2e_report_*.txt
```

## Troubleshooting

### Aggregators Not Starting

**Check SSH connectivity:**
```bash
for machine in $REMOTE_MACHINES; do
    ssh "${SSH_USER}@${machine}" "echo OK"
done
```

**Check if binaries are built:**
```bash
ssh ubuntu@10.10.1.1 "test -f ~/zk-Analytics/target/release/aggregator && echo 'Built' || echo 'Not built'"
```

### Kafka Connection Issues

**Verify Kafka is accessible from remote machines:**
```bash
ssh ubuntu@10.10.1.1 "nc -zv $KAFKA_BROKERS"
```

**Check topic exists:**
```bash
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list
```

### FDB Connection Issues

**Test FDB from remote machine:**
```bash
ssh ubuntu@10.10.1.1 "fdbcli --exec 'status minimal'"
```

**Verify cluster file is copied:**
```bash
ssh ubuntu@10.10.1.1 "cat /etc/foundationdb/fdb.cluster"
```

### Processing Stalls

**Check aggregator logs for errors:**
```bash
ssh ubuntu@10.10.1.1 "tail -100 /tmp/zktelemetry_agg_0/aggregator.log"
```

**Check consumer group lag:**
```bash
docker exec kafka kafka-consumer-groups \
    --bootstrap-server localhost:9092 \
    --describe --group dist_e2e_aggregators
```

**Check FDB load:**
```bash
fdbcli --exec "status details"
```

## Performance Analysis

### Key Metrics

The report includes:
- **Production Duration**: Time to produce all events
- **Production Throughput**: Events per second produced
- **Processing Lag**: Current Kafka consumer lag
- **Aggregator Logs**: Recent processing activity per machine

### Interpreting Results

**Good Performance:**
- Production throughput > 10,000 events/s
- Consumer lag drops to 0 within expected time
- No errors in aggregator logs
- Successful data validation

**Potential Issues:**
- High consumer lag (check aggregator CPU/memory)
- Errors in logs (check configuration)
- Failed validation (check FDB consistency)

## Best Practices

1. **Start Small**: Begin with 2 machines and 100K events to verify setup
2. **Scale Gradually**: Increase machines and events incrementally
3. **Monitor Resources**: Watch CPU, memory, and network during tests
4. **Clean Between Tests**: Always run `clean` before a new test
5. **Separate Infrastructure**: Run Kafka and FDB on different machines from aggregators
6. **Use Fast Networks**: Ensure low-latency, high-bandwidth connections

## Integration with CI/CD

### Automated Testing

```bash
#!/bin/bash
# ci_distributed_e2e.sh

set -e

# Load configuration
source scripts/my_setup.sh

# Run small validation test
EVENTS=100000 \
NUM_AGGREGATORS=1 \
./scripts/run_distributed_e2e.sh start

# Validate results
./scripts/run_distributed_e2e.sh validate

# Generate report
./scripts/run_distributed_e2e.sh report

# Clean up
./scripts/run_distributed_e2e.sh clean

echo "CI E2E test passed!"
```

### Nightly Performance Tests

```bash
#!/bin/bash
# nightly_e2e.sh

# Array of test configurations
declare -a tests=(
    "EVENTS=1000000 EPOCH_TYPE=samples"
    "EVENTS=5000000 EPOCH_TYPE=histogram"
    "EVENTS=10000000 EPOCH_TYPE=cm"
)

for test in "${tests[@]}"; do
    echo "Running test: $test"
    eval "$test ./scripts/run_distributed_e2e.sh start"
    ./scripts/run_distributed_e2e.sh clean
    sleep 60
done
```

## Examples

### Complete Workflow

```bash
# 1. Configure environment
export REMOTE_MACHINES="10.10.1.1 10.10.1.2 10.10.1.3"
export SSH_USER="ubuntu"
export KAFKA_BROKERS="10.10.1.100:9092"

# 2. Ensure machines are set up
./scripts/setup_remote_e2e.sh

# 3. Run E2E test
./scripts/run_distributed_e2e.sh start

# 4. While running, monitor in another terminal
watch -n 5 './scripts/run_distributed_e2e.sh status'

# 5. After completion, review report
cat bench_csv/distributed_e2e/e2e_report_*.txt

# 6. Clean up
./scripts/run_distributed_e2e.sh stop
./scripts/run_distributed_e2e.sh clean
```

### Testing Different Aggregation Types

```bash
# Test samples aggregation
EPOCH_TYPE=samples EVENTS=1000000 ./scripts/run_distributed_e2e.sh start
./scripts/run_distributed_e2e.sh clean

# Test histogram aggregation
EPOCH_TYPE=histogram EVENTS=1000000 ./scripts/run_distributed_e2e.sh start
./scripts/run_distributed_e2e.sh clean

# Test count-min sketch aggregation
EPOCH_TYPE=cm EVENTS=1000000 ./scripts/run_distributed_e2e.sh start
./scripts/run_distributed_e2e.sh clean
```

## Next Steps

After successful E2E validation:
1. Run performance benchmarks with `bench_distributed_aggregators.sh`
2. Experiment with different partition counts and aggregator distributions
3. Test failure scenarios (machine failures, network partitions)
4. Monitor long-running workloads
5. Profile and optimize based on results
