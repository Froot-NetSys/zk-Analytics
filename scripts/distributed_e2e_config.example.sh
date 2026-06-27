#!/usr/bin/env bash
# Example configuration for distributed E2E testing
# Copy this file and customize for your environment

# ============================================
# Machine Configuration
# ============================================

# Data source machine (replays real-world dataset)
export DATA_SOURCE_MACHINE="192.0.2.1"

# Aggregator machines (space-separated list)
# Each machine will run NUM_AGGREGATORS aggregator processes
export AGGREGATOR_MACHINES="192.0.2.2 192.0.2.3 192.0.2.4 192.0.2.5 192.0.2.6 192.0.2.7 192.0.2.8 192.0.2.9"

# Querier machine (handles queries and validation)
export QUERIER_MACHINE="192.0.2.10"

# SSH configuration
export SSH_USER="${SSH_USER:-$USER}"
export REMOTE_PROJECT_DIR="/mydata/zk-Analytics"

# ============================================
# Kafka Configuration
# ============================================

# Kafka broker addresses (auto-detected from DATA_SOURCE_MACHINE if not set)
export KAFKA_BROKERS="192.0.2.1:9092"
export KAFKA_TOPIC="raw_events"

# ============================================
# FoundationDB Configuration
# ============================================
#
# IMPORTANT: For distributed E2E testing, ALL machines must connect to
# the SAME FDB instance. By default, each machine may have its own local
# FDB Docker container - this won't work for distributed testing!
#
# FDB Architecture:
#   - FDB server runs on QUERIER_MACHINE (192.0.2.6)
#   - Aggregators connect remotely to FDB on querier
#   - All machines need the same cluster file pointing to the querier's FDB
#
# Setup FDB for distributed mode (REQUIRED):
#
# Option 1: Native FDB installation on querier machine
#   1. On 192.0.2.6: apt-get install foundationdb-server foundationdb-clients
#   2. Update cluster file to use external IP:
#      echo 'zktelemetry:zktelemetry@192.0.2.6:4500' | sudo tee /etc/foundationdb/fdb.cluster
#   3. Configure FDB: fdbcli --exec 'configure new single ssd'
#   4. Copy cluster file to all other machines:
#      for ip in 192.0.2.2 192.0.2.3 192.0.2.4 192.0.2.5; do
#        scp /etc/foundationdb/fdb.cluster $ip:/etc/foundationdb/
#      done
#
# Option 2: Docker FDB with host networking on querier machine
#   1. Stop existing FDB container
#   2. Run with --network host:
#      docker run -d --name fdb --network host foundationdb/foundationdb:7.1.25
#   3. Create cluster file: echo 'zktelemetry:zktelemetry@192.0.2.6:4500' | sudo tee /etc/foundationdb/fdb.cluster
#   4. Initialize: fdbcli --exec 'configure new single ssd'
#   5. Copy cluster file to all other machines
#
# The run_distributed_e2e.sh script will detect Docker internal IPs (172.17.x.x)
# and warn if FDB is not configured for distributed access.
#
export FDB_CLUSTER_FILE="/etc/foundationdb/fdb.cluster"
export FDB_SUBSPACE="zktelemetry_dist_e2e"

# ============================================
# Aggregation Configuration
# ============================================

# Number of aggregator processes per machine
export NUM_AGGREGATORS=8

# Epoch/Aggregation type: samples, histogram, cm
# Change this to test different aggregation types
# export EPOCH_TYPE="samples"
# export EPOCH_TYPE="histogram"  # Uncomment to test histogram epochs (supports percentiles!)
export EPOCH_TYPE="cm"         # Uncomment to test Count-Min sketch epochs

# ============================================
# Dataset Type Configuration
# ============================================
#
# DATASET_TYPE: synthetic, google_cluster, caida, car_emission
export DATASET_TYPE="caida"
#
# Car Emission dataset:
#   - CSV file: testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv
#   - Value: CO2 emissions (g/km), scaled by EMISSION_VALUE_SCALE (default: 1.0)
#   - Timestamp: Model year → Unix timestamp of Jan 1 of that year
#   - Key (15 bytes): Encodes Make, Model, Vehicle class, Engine size,
#     Cylinders, Transmission, Fuel type via FNV-1a hashes
#   - Recommended: EPOCH_TYPE=samples for sum/avg queries on CO2 data
#
# To use car emission dataset with histogram epoch:
#   export DATASET_TYPE="car_emission"
#   export EPOCH_TYPE="histogram"
#   export EMISSION_VALUE_SCALE="1.0"
#   export EVENTS=10058    # ~10K rows in the CSV
#   export QUERY_TYPES="histogram_all,histogram_all_key"
#
# Query a specific Model's emission histogram via QUERY_PATTERN (30-hex-char key pattern):
#   Key layout: [Make hash 4B][Model hash 4B][class 1B][engine 1B][cyl 1B][trans 2B][fuel 1B][0]
#   To query by Model hash (bytes 4-7): QUERY_PATTERN="????????<model_hash_hex>??????????????"
#
#   Pre-computed FNV-1a hashes for common models (fnv1a_hash(name.as_bytes()) as u32):
#     Make hashes:   Toyota=2138BA1F  Honda=4D4D9979  Ford=0D9DCD7C
#     Model hashes:  Corolla=2E7370BB Civic=0571B7B5  F-150=222BBF42
#
#   Example queries (filter by Model only, wildcard everything else):
#     All Corolla entries: QUERY_PATTERN="????????2E7370BB??????????????"
#     All Civic entries:   QUERY_PATTERN="????????0571B7B5??????????????"
#     All F-150 entries:   QUERY_PATTERN="????????222BBF42??????????????"
#
#   Filter by both Make and Model (exact Make+Model combination):
#     Toyota Corolla:      QUERY_PATTERN="2138BA1F2E7370BB??????????????"
#     Honda Civic:         QUERY_PATTERN="4D4D99790571B7B5??????????????"
#     Ford F-150:          QUERY_PATTERN="0D9DCD7C222BBF42??????????????"

# ============================================
# Data Source Configuration
# ============================================

# Total events to produce
export EVENTS=131072

# Events per batch
export BATCH_SIZE=100

# ============================================
# Querier Configuration
# ============================================

# Querier HTTP port
export QUERIER_PORT=8082

# ============================================
# Query Evaluation Configuration
# ============================================

# Query types to evaluate (comma-separated)
# Available query types depend on your EPOCH_TYPE:
#
# For EPOCH_TYPE=samples:
#   - samples_sum           : Sum of all sample values
#   - samples_avg           : Average of sample values
#   - samples_sum_key       : Sum for specific key/prefix (requires key parameter)
#   - samples_avg_key       : Average for specific key/prefix (requires key parameter)
#   - samples_sum_topk      : Top-K highest sum values (privacy-preserving, no keys)
#   - samples_sum_exact_key : Sum for exact key match
#   - samples_sum_key_pattern : Sum matching pattern (hex or binary with wildcards)
#
# NOTE: Percentile queries (p50, p90, p95, p99) are NOT available for samples epochs.
#       Samples epochs store aggregates (sum, count) not individual values.
#       Use EPOCH_TYPE=histogram for percentile queries.
#
# For EPOCH_TYPE=histogram:
#   - histogram_p50     : 50th percentile from histogram
#   - histogram_p90     : 90th percentile from histogram
#   - histogram_p95     : 95th percentile from histogram
#   - histogram_p99     : 99th percentile from histogram
#   - histogram_sum     : Sum from histogram
#   - histogram_count   : Count from histogram
#
# For EPOCH_TYPE=cm (Count-Min Sketch):
#   - cm_estimate       : Frequency estimate for item
#   - cm_count          : Total count
#   - cm_top_k          : Top-K frequent items
#
# Default: Test actually implemented query types
# export QUERY_TYPES="samples_sum, samples_raw_max_key"
# export QUERY_TYPES="histogram_p90"
export QUERY_TYPES="cm_topk"

# Query the latest N epochs (recommended for consistent benchmarking)
export QUERY_EPOCHS=8

# Alternative: Use time window for queries (e.g., 1h, 30m, 24h)
# Uncomment to use time-based filtering instead of epoch count:
# export QUERY_WINDOW="1h"

# Number of iterations to run each query (for performance benchmarking)
export NUM_QUERY_ITERATIONS=1

# Key prefix for prefix-based queries (used with samples_sum_prefix, etc.)
# Leave empty to test all keys, or set to filter by prefix
# Examples: "metric.cpu.", "sensor.temperature.", "event.user."
export KEY_PREFIX=""

# Query parameters for key-based queries (samples_sum_key, samples_avg_key, cm_estimate, etc.)
# These are used when QUERY_TYPES includes queries that require specific parameters:
#
# QUERY_KEY: Numeric key ID (default: 0)
# - Used by: samples_sum_key, samples_avg_key, cm_estimate, samples_sum_exact_key, etc.
# export QUERY_KEY=12345
#
# QUERY_MASK: Bitmask for key matching (default: 0xFFFFFFFFFFFFFFFF = exact match)
# - Used by: samples_raw_max_key, samples_raw_histogram_bucket_key, etc.
# - For prefix matching, set appropriate mask bits
# export QUERY_MASK=18446744073709551615
#
# QUERY_BUCKET: Histogram bucket index (default: 0)
# - Used by: histogram_bucket, samples_raw_histogram_bucket_key
# export QUERY_BUCKET=5
#
# QUERY_LIMIT: Maximum number of results for topk queries (default: 10)
# - Used by: cm_topk, samples_sum_topk
# export QUERY_LIMIT=20
#
# QUERY_VALUE: Value parameter for count-min estimate (default: 0)
# - Used by: samples_raw_cm_estimate_key
# export QUERY_VALUE=100
#
# QUERY_PATTERN: Pattern string for pattern-based queries (default: "0x00")
# - Used by: samples_sum_key_pattern, histogram_all_key
# - For samples_sum_key_pattern: hex (up to 16 nibbles) or binary with wildcards
#   e.g., "0x12??34" or "b1010****"
# - For histogram_all_key: hex (up to 30 nibbles) matching full 15-byte key
#   e.g., "????????aabbccdd??????????????" matches bytes 4-7 (Model hash)
# export QUERY_PATTERN="0x00"
# Toyota Corolla's CO2 emission distribution across all histogram buckets
export QUERY_PATTERN="2138BA1F2E7370BB??????????????"

# ============================================
# Performance Tuning
# ============================================

# Skip expensive final log collection and metrics generation for faster exit
# Set to 1 to exit immediately after aggregators complete proofs
# Useful for CI/CD pipelines or when you don't need detailed metrics
# Default: 0 (collect all metrics and generate comprehensive report)
export SKIP_FINAL_COLLECTION=0

# ============================================
# Example: Comprehensive Query Evaluation
# ============================================
# To test all samples query types:
# export QUERY_TYPES="samples_sum,samples_avg,samples_sum_topk,samples_sum_key,samples_avg_key"

# To test with more iterations for accurate benchmarking:
# export NUM_QUERY_ITERATIONS=100

# To test different epoch counts, run multiple evaluations:
# export QUERY_EPOCHS=5   && ./scripts/run_distributed_e2e.sh evaluate
# export QUERY_EPOCHS=10  && ./scripts/run_distributed_e2e.sh evaluate
# export QUERY_EPOCHS=50  && ./scripts/run_distributed_e2e.sh evaluate

# To test prefix-based queries:
# export QUERY_TYPES="samples_sum_prefix"
# export KEY_PREFIX="metric.cpu."
# ./scripts/run_distributed_e2e.sh evaluate

# To compare global sum vs prefix sum:
# export QUERY_TYPES="samples_sum,samples_sum_prefix"
# export KEY_PREFIX="sensor.temperature."
# ./scripts/run_distributed_e2e.sh evaluate

# ============================================
# Usage Examples
# ============================================

# Run full E2E test:
#   source distributed_e2e_config.example.sh
#   ./scripts/run_distributed_e2e.sh start

# Just evaluate querier (after data is loaded):
#   source distributed_e2e_config.example.sh
#   ./scripts/run_distributed_e2e.sh evaluate

# Evaluate with prefix filtering:
#   export QUERY_TYPES="samples_sum_prefix,samples_avg"
#   export KEY_PREFIX="metric.cpu."
#   ./scripts/run_distributed_e2e.sh evaluate

# Test multiple prefixes sequentially:
#   for prefix in "metric.cpu." "metric.memory." "metric.disk."; do
#     export KEY_PREFIX="$prefix"
#     export QUERY_TYPES="samples_sum_prefix"
#     ./scripts/run_distributed_e2e.sh evaluate
#   done

# Compare global vs filtered results:
#   export QUERY_TYPES="samples_sum,samples_sum_prefix"
#   export KEY_PREFIX="sensor."
#   ./scripts/run_distributed_e2e.sh evaluate

# Run with car emission dataset (histogram epoch):
#   export DATASET_TYPE="car_emission"
#   export EPOCH_TYPE="histogram"
#   export EVENTS=10058
#   export QUERY_TYPES="histogram_all,histogram_all_key"
#   source distributed_e2e_config.example.sh
#   ./scripts/run_distributed_e2e.sh start
#
# Query Honda Civic's CO2 emission distribution (all histogram buckets):
#   export QUERY_PATTERN="????????0571B7B5??????????????"
#   ./scripts/run_distributed_e2e.sh evaluate
#
# Query Toyota Corolla's CO2 emission distribution:
#   export QUERY_PATTERN="2138BA1F2E7370BB??????????????"
#   ./scripts/run_distributed_e2e.sh evaluate
#
# Query Ford F-150's CO2 emission distribution:
#   export QUERY_PATTERN="0D9DCD7C222BBF42??????????????"
#   ./scripts/run_distributed_e2e.sh evaluate
#
# Compare all models vs specific model:
#   export QUERY_TYPES="histogram_all"
#   ./scripts/run_distributed_e2e.sh evaluate    # global histogram
#   export QUERY_TYPES="histogram_all_key"
#   export QUERY_PATTERN="????????0571B7B5??????????????"
#   ./scripts/run_distributed_e2e.sh evaluate    # Honda Civic only

# Check status:
#   ./scripts/run_distributed_e2e.sh status

# Generate report:
#   ./scripts/run_distributed_e2e.sh report

# Stop all components:
#   ./scripts/run_distributed_e2e.sh stop
