# Results Collection Guide

This guide explains what results are collected from distributed tests and where they are stored.

## Overview

Both `bench_distributed_aggregators.sh` and `run_distributed_e2e.sh` now **automatically collect all results from remote machines back to the local machine** that initiates the test.

## Collected Data

### 1. Logs

**What's Collected:**
- Complete log files from all remote aggregators
- Local querier logs
- Local data source logs

**Location:**
- Benchmark: `bench_logs/distributed/collected_<run_id>/`
- E2E: `/tmp/zktelemetry_dist_e2e/collected/`

**Naming:**
- `aggregator_<machine>_<id>.log` - Aggregator logs from each machine
- `querier.log` - Querier logs
- `datasource.log` - Data source logs

### 2. CSV Metrics

#### Benchmark Script (`bench_distributed_aggregators.sh`)

**Main CSV:** `bench_csv/distributed/bench_distributed_aggregators_<timestamp>.csv`

Columns:
- `timestamp` - When the test ran
- `epoch_type` - Aggregation type (samples/histogram/cm)
- `num_aggregators` - Number of aggregators used
- `kafka_partitions` - Number of Kafka partitions
- `series` - Number of distinct keys
- `samples_per_series` - Samples per key
- `total_events` - Total events processed
- `batch_size` - Events per batch
- `rep` - Repetition number
- `warmup_ms` - Warmup duration
- `produce_ms` - Time to produce events
- `consume_ms` - Time to consume events
- `total_ms` - Total test duration
- `events_per_sec` - Throughput (events/s)
- `mb_per_sec` - Throughput (MB/s)
- `avg_latency_ms` - Average latency
- `p99_latency_ms` - P99 latency
- `memory_mb_total` - Total memory across all aggregators

**Detailed Metrics CSV:** `bench_csv/distributed/detailed_metrics_<run_id>.csv`

Columns per aggregator:
- `aggregator_id` - Aggregator index
- `proof_time_ms` - ZK proof generation time
- `verify_time_ms` - Proof verification time
- `events_processed` - Events handled by this aggregator
- `memory_mb` - Memory usage
- `errors` - Error count

#### E2E Script (`run_distributed_e2e.sh`)

**Metrics CSV:** `bench_csv/distributed_e2e/aggregator_metrics_<timestamp>.csv`

Columns per aggregator:
- `machine` - Machine hostname/IP
- `aggregator_id` - Aggregator index on that machine
- `total_events` - Total events processed
- `avg_proof_time_ms` - Average proof time
- `max_proof_time_ms` - Maximum proof time
- `total_memory_mb` - Memory usage
- `errors` - Error count
- `warnings` - Warning count

### 3. Summary Reports

#### Benchmark Report
**Location:** `bench_csv/distributed/summary_report_<timestamp>.txt`

**Contains:**
- Complete configuration details
- Results summary table
- File locations for detailed data

#### E2E Report
**Location:** `bench_csv/distributed_e2e/e2e_report_<timestamp>.txt`

**Contains:**
- Complete configuration
- Production performance metrics
- Per-aggregator metrics table
- Total metrics summary
- Kafka consumer group status
- File locations

### 4. Extracted Metrics

From log files, the scripts extract:

**Proof Times:**
- Pattern: `prove_ms=<number>`
- Used for: Average and max proof time calculations
- Found in: Aggregator logs

**Verification Times:**
- Pattern: `verify.*<number>ms`
- Used for: Verification performance analysis
- Found in: Aggregator logs

**Memory Usage:**
- Pattern: `VmRSS: <number> kB`
- Used for: Memory consumption tracking
- Found in: Process status in logs

**Event Counts:**
- Pattern: `n_events=<number>`
- Used for: Throughput validation
- Found in: Aggregator logs

**Errors and Warnings:**
- Pattern: Case-insensitive search for "error" and "warn"
- Used for: Health monitoring
- Found in: All logs

## Collection Process

### Benchmark Script

```
For each benchmark run:
  1. Run aggregators on remote machines
  2. Produce and consume events
  3. Stop aggregators
  4. ✓ Collect all logs via scp from remote machines
  5. ✓ Extract detailed metrics from logs
  6. ✓ Write results to CSVs
  7. ✓ Generate summary report

After all runs complete:
  - Generate comprehensive summary report
  - All data is on local machine
```

### E2E Script

```
1. Start aggregators on remote machines
2. Start local querier
3. Run data source
4. Wait for processing
5. Validate data
6. ✓ Collect all logs from all machines via scp
7. ✓ Extract metrics from each log
8. ✓ Generate per-aggregator CSV
9. ✓ Generate comprehensive report

All results saved to local machine
```

## File Organization

### Benchmark Results
```
bench_csv/distributed/
├── bench_distributed_aggregators_<timestamp>.csv    # Main results
├── detailed_metrics_<run_id>.csv                    # Per-aggregator metrics (multiple)
└── summary_report_<timestamp>.txt                   # Summary report

bench_logs/distributed/
└── collected_<run_id>/                              # Collected logs (multiple runs)
    ├── aggregator_<machine>_0.log
    ├── aggregator_<machine>_1.log
    └── ...
```

### E2E Results
```
bench_csv/distributed_e2e/
├── aggregator_metrics_<timestamp>.csv               # Per-aggregator metrics
└── e2e_report_<timestamp>.txt                       # Complete report

/tmp/zktelemetry_dist_e2e/
├── collected/                                       # All collected logs
│   ├── aggregator_<machine>_<id>.log
│   ├── querier.log
│   └── datasource.log
├── querier.log
├── datasource.log
└── produce_duration.txt
```

## Accessing Results

### View Latest Benchmark Results

```bash
# View main CSV
cat bench_csv/distributed/bench_distributed_aggregators_*.csv | column -t -s','

# View latest summary report
cat bench_csv/distributed/summary_report_*.txt | tail -100

# View detailed metrics for a specific run
cat bench_csv/distributed/detailed_metrics_<run_id>.csv | column -t -s','

# Check logs from a specific machine
less bench_logs/distributed/collected_<run_id>/aggregator_<machine>_0.log
```

### View E2E Results

```bash
# View latest report
cat bench_csv/distributed_e2e/e2e_report_*.txt

# View per-aggregator metrics
cat bench_csv/distributed_e2e/aggregator_metrics_*.csv | column -t -s','

# Check aggregator log from specific machine
less /tmp/zktelemetry_dist_e2e/collected/aggregator_<machine>_0.log

# Check for errors across all logs
grep -i error /tmp/zktelemetry_dist_e2e/collected/*.log
```

## Analyzing Results

### Performance Analysis

```bash
# Extract throughput over time
awk -F',' 'NR>1 {print $1,$14}' bench_csv/distributed/bench_distributed_aggregators_*.csv

# Compare different aggregator counts
awk -F',' 'NR>1 {print $3,$14}' bench_csv/distributed/bench_distributed_aggregators_*.csv | sort -n

# Find max proof time
awk -F',' 'NR>1 {print $3}' bench_csv/distributed/detailed_metrics_*.csv | sort -n | tail -1
```

### Error Analysis

```bash
# Find all errors from collected logs
grep -r "ERROR" bench_logs/distributed/collected_*/

# Count errors per aggregator
for log in bench_logs/distributed/collected_*/*.log; do
    echo "$log: $(grep -ci error $log)"
done

# Extract error messages
grep -h "ERROR" /tmp/zktelemetry_dist_e2e/collected/*.log | sort | uniq -c
```

### Memory Analysis

```bash
# Total memory usage from detailed metrics
awk -F',' 'NR>1 {sum+=$5} END {print "Total Memory: "sum" MB"}' \
    bench_csv/distributed/detailed_metrics_*.csv

# Memory per machine
awk -F',' 'NR>1 {mem[$1]+=$6; count[$1]++} END {
    for(m in mem) printf "%s: %d MB (avg: %d MB per agg)\n",
    m, mem[m], mem[m]/count[m]
}' bench_csv/distributed_e2e/aggregator_metrics_*.csv
```

## Automation and CI/CD

### Export for Analysis

```bash
# Create combined results file
{
    echo "=== Benchmark Results ==="
    cat bench_csv/distributed/bench_distributed_aggregators_*.csv
    echo ""
    echo "=== Detailed Metrics ==="
    cat bench_csv/distributed/detailed_metrics_*.csv
} > combined_results.txt
```

### Archive Results

```bash
# Archive all results for a test run
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
tar -czf results_archive_${TIMESTAMP}.tar.gz \
    bench_csv/distributed/ \
    bench_logs/distributed/ \
    bench_csv/distributed_e2e/

# Upload to storage
# scp results_archive_${TIMESTAMP}.tar.gz user@storage:/path/
```

### Parse for CI

```python
import csv
import sys

def check_performance_regression(csv_file, threshold_events_per_sec=10000):
    with open(csv_file) as f:
        reader = csv.DictReader(f)
        for row in reader:
            eps = float(row['events_per_sec'])
            if eps < threshold_events_per_sec:
                print(f"FAIL: Performance regression detected: {eps} events/s")
                sys.exit(1)
    print("PASS: Performance meets threshold")

check_performance_regression('bench_csv/distributed/bench_distributed_aggregators_*.csv')
```

## Remote Cleanup

After results are collected, you can clean up remote machines:

```bash
# Clean up remote aggregator logs and buffers
for machine in $REMOTE_MACHINES; do
    ssh "${SSH_USER}@${machine}" "
        rm -rf /tmp/zktelemetry_agg_*
        rm -rf /tmp/zktelemetry_bench_*
    "
done
```

## Best Practices

1. **Preserve Results**: Archive results after important test runs
2. **Monitor Disk Space**: Logs can grow large with many aggregators
3. **Clean Old Results**: Periodically remove old test results
4. **Version Control Config**: Track test configurations with results
5. **Automated Analysis**: Use scripts to parse and analyze results
6. **Error Tracking**: Always check collected logs for errors
7. **Baseline Comparison**: Keep baseline results for regression testing

## Troubleshooting

### Missing Logs

If logs aren't collected from a machine:

```bash
# Check SSH connectivity
ssh "${SSH_USER}@${machine}" "ls -la /tmp/zktelemetry_agg_*/aggregator.log"

# Check if logs exist
ssh "${SSH_USER}@${machine}" "find /tmp -name 'aggregator*.log'"

# Manual collection
scp "${SSH_USER}@${machine}:/tmp/zktelemetry_agg_0/aggregator.log" ./
```

### Incomplete Metrics

If metrics extraction fails:

```bash
# Check log format
less bench_logs/distributed/collected_*/aggregator_*.log

# Verify pattern matching
grep -P 'prove_ms=\K[0-9]+' bench_logs/distributed/collected_*/aggregator_*.log

# Manual extraction
grep "prove_ms" bench_logs/distributed/collected_*/aggregator_*.log
```

## Summary

✅ **All results are collected to the local machine**:
- Complete logs from all remote machines
- Detailed per-aggregator metrics (proof time, verification, memory)
- Comprehensive CSV files
- Summary reports

✅ **Nothing is left on remote machines** (after collection):
- Logs are copied, not moved
- Can be cleaned up after collection
- Originals remain for debugging if needed

✅ **Ready for analysis**:
- CSV format for spreadsheet/plotting tools
- Text logs for detailed debugging
- Structured metrics for automated analysis
