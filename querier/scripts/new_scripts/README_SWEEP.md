# Querier Benchmark Sweep Scripts

These scripts are used to run sweep tests across different parameters and output results to CSV files.

## Script Descriptions

### 1. Main Script

- **`sweep_bench_queries.sh`** - Main sweep script, supports all query types

### 2. Query-Type-Specific Scripts

- **`sweep_histogram.sh`** - Tests histogram queries only
- **`sweep_cm.sh`** - Tests Count-Min Sketch queries only
- **`sweep_samples.sh`** - Tests samples queries only

## Usage

### Basic Usage

```bash
# Test histogram queries (using default parameter ranges)
./sweep_histogram.sh

# Test CM queries
./sweep_cm.sh

# Test samples queries
./sweep_samples.sh
```

### Custom Parameter Ranges

You can customize sweep parameter ranges via environment variables:

```bash
# Custom epochs range
EPOCHS_LIST="1 2 4 8" ./sweep_histogram.sh

# Custom keys range
KEYS_LIST="64 128 256" ./sweep_histogram.sh

# Custom events-per-key range
EVENTS_PER_KEY_LIST="16 32 64" ./sweep_histogram.sh

# Combine multiple parameters
EPOCHS_LIST="1 2 4" \
KEYS_LIST="128 256" \
EVENTS_PER_KEY_LIST="32 64" \
OUTPUT_CSV="custom_results.csv" \
./sweep_histogram.sh
```

### Custom Output File

```bash
# Specify CSV output file
OUTPUT_CSV="./my_results.csv" ./sweep_histogram.sh
```

### Skip Build

```bash
# If the binary is already built, you can skip the build step
SKIP_BUILD=1 ./sweep_histogram.sh
```

## Environment Variable Reference

| Variable | Description | Default |
|----------|-------------|---------|
| `EPOCHS_LIST` | List of epoch counts (space-separated) | `"1 2 4 8 16 32 64 128 256"` |
| `KEYS_LIST` | List of keys per epoch | `"128 256 512 1024"` |
| `EVENTS_PER_KEY_LIST` | List of events per key | `"8 16 32 64 128"` |
| `OUTPUT_CSV` | Output CSV file path | `./bench_queries_results.csv` |
| `SEED` | Random seed | `0xBEEF` |
| `SKIP_BUILD` | Skip build (set to 1) | `0` |
| `SKIP_HISTOGRAM` | Skip histogram queries | `0` |
| `SKIP_CM` | Skip CM queries | `0` |
| `SKIP_SAMPLES` | Skip samples queries | `0` |
| `RISC0_DEV_MODE` | Enable RISC Zero dev mode (fast proofs, for testing only) | `0` |

## RISC Zero Dev Mode

### What is RISC0_DEV_MODE?

`RISC0_DEV_MODE` is a fast development mode provided by RISC Zero that significantly speeds up proof generation (typically 10-100x faster).

**Warning: Proofs generated in dev mode are not secure and should only be used for testing and debugging!**

### When to Use Dev Mode?

✅ **Appropriate use cases:**
- Quick testing of parameter ranges
- Debugging code logic
- Iterative testing during development
- Verifying functional correctness

❌ **Inappropriate use cases:**
- Production environments
- Security audits
- Performance benchmarking (proof size and prove time will be inaccurate)
- Any scenario requiring real ZK proofs

### How to Enable?

```bash
# Enable for a single run
RISC0_DEV_MODE=1 ./sweep_histogram.sh

# Quick test with small-scale parameters
RISC0_DEV_MODE=1 \
EPOCHS_LIST="1 2 4" \
KEYS_LIST="64 128" \
EVENTS_PER_KEY_LIST="8 16" \
./sweep_histogram.sh

# Dev mode + custom output
RISC0_DEV_MODE=1 \
OUTPUT_CSV="dev_test.csv" \
./sweep_cm.sh
```

### Performance Comparison Example

| Mode | Prove Time | Proof Size | Use Case |
|------|------------|------------|----------|
| Production (RISC0_DEV_MODE=0) | 30-300s | 200-400KB | Real benchmarking, production deployment |
| Dev Mode (RISC0_DEV_MODE=1) | 1-10s | Inaccurate | Quick iterative testing, functional verification |

## CSV Output Format

The output CSV file contains the following columns:

```
query_type,epochs,keys,events_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,seed,risc0_dev_mode
```

Example output:
```csv
query_type,epochs,keys,events_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,seed,risc0_dev_mode
histogram/bucket,1,128,8,1024,1234,56,789012,48879,0
histogram/all,1,128,8,1024,1456,67,801234,48879,0
histogram/bucket,2,128,8,2048,2345,89,945678,48879,0
...
```

## Example Scenarios

### Scenario 1: Quick Test with Small-Scale Parameters

```bash
EPOCHS_LIST="1 2" \
KEYS_LIST="64 128" \
EVENTS_PER_KEY_LIST="8 16" \
OUTPUT_CSV="quick_test.csv" \
./sweep_histogram.sh
```

### Scenario 2: Detailed Epoch Scalability Test

```bash
EPOCHS_LIST="1 2 4 8 16 32 64 128 256" \
KEYS_LIST="128" \
EVENTS_PER_KEY_LIST="32" \
OUTPUT_CSV="epoch_scaling.csv" \
./sweep_histogram.sh
```

### Scenario 3: Test Impact of Different Key Counts

```bash
EPOCHS_LIST="4" \
KEYS_LIST="64 128 256 512 1024 2048" \
EVENTS_PER_KEY_LIST="16" \
OUTPUT_CSV="key_scaling.csv" \
./sweep_histogram.sh
```

### Scenario 4: CM Sketch Specific Test (More Keys, Fewer Events)

```bash
EPOCHS_LIST="1 2 4 8 16 32" \
KEYS_LIST="512 1024 2048 4096" \
EVENTS_PER_KEY_LIST="4 8 16" \
OUTPUT_CSV="cm_scaling.csv" \
./sweep_cm.sh
```

## Analyzing Results

You can use Python, Excel, or other tools to analyze the CSV results:

```python
import pandas as pd
import matplotlib.pyplot as plt

# Read results
df = pd.read_csv('bench_queries_results.csv')

# Group by query_type and show statistics
print(df.groupby('query_type')[['prove_time_ms', 'proof_size_bytes']].describe())

# Plot epochs vs prove_time
for query_type in df['query_type'].unique():
    data = df[df['query_type'] == query_type]
    plt.plot(data['epochs'], data['prove_time_ms'], label=query_type, marker='o')

plt.xlabel('Epochs')
plt.ylabel('Prove Time (ms)')
plt.legend()
plt.title('Prove Time vs Epochs')
plt.show()
```

## Notes

1. **Runtime**: A full parameter sweep can take a very long time (hours to days), depending on the parameter ranges
2. **Resource Requirements**: ZK proof generation requires significant memory and CPU; ensure sufficient system resources
3. **Parallel Execution**: Scripts currently run sequentially; for parallel execution, consider splitting parameter ranges across multiple processes
4. **Result Overwriting**: Each run overwrites the output CSV file; manual handling is needed if you want to append results

## Troubleshooting

### Issue: Binary Not Found

```bash
# Solution: Make sure to build first
SKIP_BUILD=0 ./sweep_histogram.sh
```

### Issue: Out of Memory

```bash
# Reduce parameter ranges
EPOCHS_LIST="1 2 4" \
KEYS_LIST="128 256" \
./sweep_histogram.sh
```

### Issue: Process Killed

```bash
# Check system memory
free -h

# Use smaller parameters
KEYS_LIST="64 128" \
EVENTS_PER_KEY_LIST="8 16" \
./sweep_histogram.sh
```
