#!/usr/bin/env bash
# Quick test script to demonstrate RISC0_DEV_MODE functionality

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=========================================="
echo "Testing RISC0_DEV_MODE with bench_queries"
echo "=========================================="
echo ""

# Test 1: Single benchmark with dev mode
echo "Test 1: Running single histogram benchmark with RISC0_DEV_MODE=1"
echo "This should complete very quickly (1-10 seconds instead of 30-300 seconds)"
echo ""

RISC0_DEV_MODE=1 \
SKIP_BUILD=1 \
EPOCHS=2 \
KEYS=64 \
EVENTS_PER_KEY=8 \
"${SCRIPT_DIR}/bench_histogram_epoch_query.sh"

echo ""
echo "=========================================="
echo "Test 1 Complete!"
echo "=========================================="
echo ""
echo "Test 2: Running small parameter sweep with RISC0_DEV_MODE=1"
echo "This will test multiple configurations quickly"
echo ""

RISC0_DEV_MODE=1 \
SKIP_BUILD=1 \
EPOCHS_LIST="1 2" \
KEYS_LIST="32 64" \
EVENTS_PER_KEY_LIST="8" \
SKIP_CM=1 \
SKIP_SAMPLES=1 \
OUTPUT_CSV="./test_dev_mode_results.csv" \
"${SCRIPT_DIR}/sweep_bench_queries.sh"

echo ""
echo "=========================================="
echo "All Tests Complete!"
echo "=========================================="
echo ""
echo "Results saved to: ./test_dev_mode_results.csv"
echo ""
echo "To see the results:"
echo "  cat ./test_dev_mode_results.csv"
echo ""
echo "Compare with production mode by running:"
echo "  RISC0_DEV_MODE=0 EPOCHS=2 KEYS=64 EVENTS_PER_KEY=8 ./bench_histogram_epoch_query.sh"
echo ""
