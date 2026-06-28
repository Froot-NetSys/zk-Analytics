# Query Types Reference

This document describes all available query types for the zk-Analytics distributed E2E evaluation script.

## Overview

The querier evaluation system supports multiple query types to test different aggregation and filtering scenarios. Query types are configured via the `QUERY_TYPES` environment variable as a comma-separated list.

## Configuration

```bash
# Basic configuration
export QUERY_TYPES="samples_sum,samples_avg,samples_p99"
export QUERY_WINDOW="1h"              # Time window for queries
export NUM_QUERY_ITERATIONS=10        # Number of times to run each query

# For prefix-based queries
export KEY_PREFIX="metric.cpu."       # Filter keys by this prefix
export QUERY_TYPES="samples_sum_prefix,samples_avg_prefix"
```

## Query Types by Epoch Type

### Samples Epoch (`EPOCH_TYPE=samples`)

#### Global Aggregations
These queries aggregate over all keys in the time window:

| Query Type | Description | Example Use Case |
|------------|-------------|------------------|
| `samples_sum` | Sum of all sample values | Total requests across all services |
| `samples_avg` | Average of sample values | Average response time |
| `samples_count` | Count of samples | Total number of events |
| `samples_min` | Minimum value | Lowest temperature reading |
| `samples_max` | Maximum value | Peak CPU usage |
| `samples_p50` | 50th percentile (median) | Median latency |
| `samples_p90` | 90th percentile | 90th percentile latency |
| `samples_p95` | 95th percentile | 95th percentile latency |
| `samples_p99` | 99th percentile | 99th percentile latency |

#### Prefix-Based Aggregations
These queries aggregate only over keys matching a specific prefix (requires `KEY_PREFIX` to be set):

| Query Type | Description | Example Use Case |
|------------|-------------|------------------|
| `samples_sum_prefix` | Sum for keys matching prefix | CPU metrics only: `metric.cpu.*` |
| `samples_avg_prefix` | Average for keys matching prefix | Average memory for app servers |
| `samples_count_prefix` | Count for keys matching prefix | Number of error events |
| `samples_min_prefix` | Minimum for keys matching prefix | Min disk usage for database servers |
| `samples_max_prefix` | Maximum for keys matching prefix | Max network throughput for region |
| `samples_p50_prefix` | 50th percentile for prefix | Median latency for API endpoints |
| `samples_p90_prefix` | 90th percentile for prefix | P90 latency for specific service |
| `samples_p95_prefix` | 95th percentile for prefix | P95 latency for specific service |
| `samples_p99_prefix` | 99th percentile for prefix | P99 latency for specific service |

### Histogram Epoch (`EPOCH_TYPE=histogram`)

| Query Type | Description |
|------------|-------------|
| `histogram_p50` | 50th percentile from histogram |
| `histogram_p90` | 90th percentile from histogram |
| `histogram_p95` | 95th percentile from histogram |
| `histogram_p99` | 99th percentile from histogram |
| `histogram_sum` | Sum from histogram |
| `histogram_count` | Count from histogram |
| `histogram_all` | All buckets with total count and sum |
| `histogram_all_key` | All buckets filtered by 15-byte key pattern (requires `QUERY_PATTERN`) |

### Count-Min Sketch Epoch (`EPOCH_TYPE=cm`)

| Query Type | Description |
|------------|-------------|
| `cm_estimate` | Frequency estimate for item |
| `cm_count` | Total count |
| `cm_top_k` | Top-K frequent items |

## Usage Examples

### Basic Query Evaluation

```bash
# Test common aggregations
export QUERY_TYPES="samples_sum,samples_avg,samples_p99,samples_count"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

### Prefix-Based Filtering

```bash
# Sum and average for CPU metrics only
export KEY_PREFIX="metric.cpu."
export QUERY_TYPES="samples_sum_prefix,samples_avg_prefix"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

### Comparing Global vs Filtered Results

```bash
# Compare total sum vs sum for specific prefix
export KEY_PREFIX="sensor.temperature."
export QUERY_TYPES="samples_sum,samples_sum_prefix"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

### Testing Multiple Prefixes

```bash
# Test different metric groups
for prefix in "metric.cpu." "metric.memory." "metric.disk."; do
  export KEY_PREFIX="$prefix"
  export QUERY_TYPES="samples_sum_prefix,samples_p99_prefix"
  ./scripts/distributed/run_distributed_e2e.sh evaluate
done
```

### Performance Benchmarking

```bash
# Run 100 iterations of each query for accurate performance data
export NUM_QUERY_ITERATIONS=100
export QUERY_TYPES="samples_sum,samples_p99"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

### Testing Different Time Windows

```bash
# Test query performance across different time ranges
for window in "5m" "1h" "6h" "24h"; do
  export QUERY_WINDOW="$window"
  export QUERY_TYPES="samples_sum,samples_avg"
  ./scripts/distributed/run_distributed_e2e.sh evaluate
done
```

## Output

Query evaluation produces:

1. **Console Output**: Real-time statistics for each query type
   - Success rate
   - Average, min, max latency
   - Number of successful/failed queries

2. **CSV Metrics File**: `bench_csv/distributed_e2e/query_metrics_TIMESTAMP.csv`
   - Per-iteration timing data
   - Success/failure status
   - Error messages for failed queries

3. **Comprehensive Report**: Included in E2E report with:
   - Overall query statistics
   - Per-query-type breakdown
   - Comparison across different query types

## Common Use Cases

### 1. Testing Global Aggregations

```bash
export QUERY_TYPES="samples_sum,samples_avg,samples_count,samples_p90,samples_p99"
export QUERY_WINDOW="1h"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

**Use for**: Validating basic aggregation correctness and performance

### 2. Testing Namespace/Metric Group Filtering

```bash
export KEY_PREFIX="prod.api."
export QUERY_TYPES="samples_sum_prefix,samples_avg_prefix,samples_p99_prefix"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

**Use for**: Verifying prefix-based filtering for multi-tenant or namespaced metrics

### 3. Latency Testing

```bash
export QUERY_TYPES="samples_p50,samples_p90,samples_p95,samples_p99"
export NUM_QUERY_ITERATIONS=50
./scripts/distributed/run_distributed_e2e.sh evaluate
```

**Use for**: Measuring query latency and performance characteristics

### 4. Correctness Validation

```bash
# Compare global vs prefix - prefix sum should be <= global sum
export KEY_PREFIX="test.metrics."
export QUERY_TYPES="samples_sum,samples_sum_prefix,samples_count,samples_count_prefix"
./scripts/distributed/run_distributed_e2e.sh evaluate
```

**Use for**: Verifying that filtered queries return subsets of global results

## Key Prefix Format

The `KEY_PREFIX` should match your metric naming convention:

- **Hierarchical**: `metric.cpu.usage.`, `sensor.temperature.room1.`
- **Namespaced**: `prod.api.`, `dev.backend.`, `staging.db.`
- **Service-based**: `service.auth.`, `service.payment.`, `service.notification.`
- **Region-based**: `us-east-1.`, `eu-west-1.`, `ap-southeast-1.`

## Tips

1. **Start with basic queries** (`samples_sum`, `samples_count`) to validate data ingestion
2. **Use prefix queries** to test multi-tenant isolation or namespace filtering
3. **Increase iterations** (`NUM_QUERY_ITERATIONS=100`) for stable latency measurements
4. **Test different time windows** to understand query performance scaling
5. **Monitor failed queries** in the CSV output to identify issues

## Troubleshooting

### Query Returns Error

Check if:
- Querier is running: `./scripts/distributed/run_distributed_e2e.sh status`
- Data has been ingested into FDB
- Query type is valid for your `EPOCH_TYPE`
- `KEY_PREFIX` is set when using `*_prefix` query types

### No Results Returned

Verify:
- Aggregators have processed events
- FDB contains data: Check "Kafka Consumer Group Status" in report
- Time window covers the data ingestion period

### Prefix Query Returns Empty

Ensure:
- `KEY_PREFIX` is set: `echo $KEY_PREFIX`
- Prefix matches actual keys in your data
- Keys in data source use the expected naming convention
