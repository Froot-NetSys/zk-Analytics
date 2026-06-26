#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../../.." && pwd)"
export CARGO_HOME="${CARGO_HOME:-/mydata/cargo_home}"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  echo "Building bench_queries binary (release)..."
  cargo build --manifest-path "${ROOT_DIR}/Cargo.toml" --bin bench_queries --release -q
fi

# Output CSV file
OUTPUT_CSV="${OUTPUT_CSV:-./bench_queries_results.csv}"
BENCH_BIN="${ROOT_DIR}/target/release/bench_queries"

# Parameter sweep configurations
EPOCHS_LIST="${EPOCHS_LIST:-1 2 4 8 16 32 64 128 256}"

# Legacy mode: use --keys parameter
KEYS_LIST="${KEYS_LIST:-128 256 512 1024}"

# Multi-source mode: use --num-sources, --keys-per-source, --sources-per-epoch
# If NUM_SOURCES_LIST is set, use multi-source mode
NUM_SOURCES_LIST="${NUM_SOURCES_LIST:-}"
KEYS_PER_SOURCE_LIST="${KEYS_PER_SOURCE_LIST:-}"
SOURCES_PER_EPOCH_LIST="${SOURCES_PER_EPOCH_LIST:-}"

# Multi-aggregator mode: distribute sources across aggregators for key coverage
# If not set, defaults to 1 (single aggregator, original behavior)
NUM_AGGREGATORS_LIST="${NUM_AGGREGATORS_LIST:-1}"

# Samples per key per epoch (alias: EVENTS_PER_KEY_LIST for backward compatibility)
SAMPLES_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST:-${EVENTS_PER_KEY_LIST:-8 16 32 64 128}}"
EVENTS_PER_KEY_LIST="${SAMPLES_PER_KEY_LIST}"
SEED="${SEED:-0xBEEF}"

# Query type flags
SKIP_HISTOGRAM="${SKIP_HISTOGRAM:-0}"
SKIP_CM="${SKIP_CM:-0}"
SKIP_SAMPLES="${SKIP_SAMPLES:-0}"
SKIP_RAW="${SKIP_RAW:-0}"

# Fine-grained histogram query flags
SKIP_HISTOGRAM_BUCKET="${SKIP_HISTOGRAM_BUCKET:-0}"
SKIP_HISTOGRAM_ALL="${SKIP_HISTOGRAM_ALL:-0}"
SKIP_HISTOGRAM_P90="${SKIP_HISTOGRAM_P90:-0}"

# Fine-grained samples query flags
SKIP_SAMPLES_SUM="${SKIP_SAMPLES_SUM:-0}"
SKIP_SAMPLES_SUM_KEY="${SKIP_SAMPLES_SUM_KEY:-0}"
SKIP_SAMPLES_SUM_TOPK="${SKIP_SAMPLES_SUM_TOPK:-0}"

# RISC Zero dev mode (fast proofs for testing, insecure)
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"
if [[ "$RISC0_DEV_MODE" == "1" ]]; then
  export RISC0_DEV_MODE=1
  echo "RISC0_DEV_MODE=1 (Fast insecure proofs for testing)"
fi

# Differential Privacy mode (enabled by default)
DP_ENABLED="${DP_ENABLED:-1}"
DP_FLAG=""
if [[ "$DP_ENABLED" == "0" || "$DP_ENABLED" == "false" ]]; then
  DP_FLAG="--dp-disabled"
  DP_ENABLED="0"
else
  DP_ENABLED="1"
fi

# Determine sweep mode
if [[ -n "$NUM_SOURCES_LIST" ]]; then
  SWEEP_MODE="multi-source"
  # Initialize CSV with header for multi-source mode (includes num_aggregators)
  echo "query_type,epochs,num_sources,num_aggregators,keys_per_source,sources_per_epoch,total_unique_keys,samples_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,journal_size_bytes,dp_offset,memory_mb,seed,dp_enabled,risc0_dev_mode" > "$OUTPUT_CSV"
else
  SWEEP_MODE="legacy"
  # Initialize CSV with header for legacy mode
  echo "query_type,epochs,keys,events_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,journal_size_bytes,dp_offset,memory_mb,seed,dp_enabled,risc0_dev_mode" > "$OUTPUT_CSV"
fi

echo "Starting parameter sweep (mode: $SWEEP_MODE)..."
echo "Output will be written to: $OUTPUT_CSV"
echo "RISC0 Dev Mode: $RISC0_DEV_MODE"
echo "DP Enabled: $DP_ENABLED"
echo ""

total_runs=0

if [[ "$SWEEP_MODE" == "multi-source" ]]; then
  # Multi-source mode with multi-aggregator support
  for epochs in $EPOCHS_LIST; do
    for num_sources in $NUM_SOURCES_LIST; do
      for num_aggregators in $NUM_AGGREGATORS_LIST; do
        for keys_per_source in $KEYS_PER_SOURCE_LIST; do
          for sources_per_epoch in $SOURCES_PER_EPOCH_LIST; do
            for samples_per_key in $SAMPLES_PER_KEY_LIST; do
              total_unique_keys=$((num_sources * keys_per_source))
              total_events=$((epochs * sources_per_epoch * keys_per_source * samples_per_key))

              echo "==================================================="
              echo "Running: epochs=$epochs num_sources=$num_sources num_aggregators=$num_aggregators keys_per_source=$keys_per_source sources_per_epoch=$sources_per_epoch samples_per_key=$samples_per_key"
              echo "Total unique keys: $total_unique_keys"
              echo "Total events: $total_events"
              echo "==================================================="

              # Build arguments (--samples-per-key is alias for --events-per-key)
              args="--epochs $epochs --num-sources $num_sources --num-aggregators $num_aggregators --keys-per-source $keys_per_source --sources-per-epoch $sources_per_epoch --samples-per-key $samples_per_key --seed $SEED $DP_FLAG"
              if [[ "$SKIP_HISTOGRAM" == "1" ]]; then
                args="$args --skip-histogram"
              fi
              if [[ "$SKIP_HISTOGRAM_BUCKET" == "1" ]]; then
                args="$args --skip-histogram-bucket"
              fi
              if [[ "$SKIP_HISTOGRAM_ALL" == "1" ]]; then
                args="$args --skip-histogram-all"
              fi
              if [[ "$SKIP_HISTOGRAM_P90" == "1" ]]; then
                args="$args --skip-histogram-p90"
              fi
              if [[ "$SKIP_CM" == "1" ]]; then
                args="$args --skip-cm"
              fi
              if [[ "$SKIP_SAMPLES" == "1" ]]; then
                args="$args --skip-samples"
              fi
              if [[ "$SKIP_SAMPLES_SUM" == "1" ]]; then
                args="$args --skip-samples-sum"
              fi
              if [[ "$SKIP_SAMPLES_SUM_KEY" == "1" ]]; then
                args="$args --skip-samples-sum-key"
              fi
              if [[ "$SKIP_SAMPLES_SUM_TOPK" == "1" ]]; then
                args="$args --skip-samples-sum-topk"
              fi
              if [[ "$SKIP_RAW" == "1" ]]; then
                args="$args --skip-raw"
              fi

              # Run benchmark with /usr/bin/time to capture memory usage
              time_output_file=$(mktemp)
              output=$(/usr/bin/time -v "$BENCH_BIN" $args 2> "$time_output_file")

              # Extract max RSS (in KB) from time output and convert to MB
              max_rss_kb=$(grep "Maximum resident set size" "$time_output_file" | awk '{print $NF}')
              memory_mb=$(echo "scale=2; $max_rss_kb / 1024" | bc)
              rm -f "$time_output_file"

              # Parse output and extract results for multi-source mode
              # Format: query_type,epochs,num_sources,num_aggregators,keys_per_source,sources_per_epoch,total_unique_keys,samples_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,journal_size_bytes,dp_offset,memory_mb,seed,dp_enabled,risc0_dev_mode
              echo "$output" | awk -v epochs="$epochs" -v num_sources="$num_sources" -v num_agg="$num_aggregators" -v kps="$keys_per_source" -v spe="$sources_per_epoch" -v total_keys="$total_unique_keys" -v spk="$samples_per_key" -v total="$total_events" -v mem_mb="$memory_mb" -v seed="$SEED" -v dp_enabled="$DP_ENABLED" -v dev_mode="$RISC0_DEV_MODE" '
                BEGIN {
                  in_results = 0
                }
                /=== Benchmark Results ===/ {
                  in_results = 1
                  next
                }
                /^-+$/ {
                  if (in_results) {
                    next
                  }
                }
                in_results && NF >= 8 && $1 != "Query" {
                  # Table format: query_type epochs keys proof_time verify_time proof_size journal_size dp_offset
                  # DP offset is always the last field (numeric)
                  query_type = $1
                  prove_time = $4
                  verify_time = $5
                  dp_offset = $NF  # Last field is always dp_offset

                  # Parse proof_size and journal_size (may have units)
                  # Find where journal_size starts by working backwards from dp_offset
                  if ($(NF-1) ~ /^[0-9]/) {
                    # journal_size is single field (no unit)
                    journal_size = $(NF-1)
                    if ($(NF-2) ~ /^[0-9]/) {
                      # proof_size is also single field
                      proof_size = $(NF-2)
                    } else {
                      # proof_size has unit
                      proof_size = $(NF-3) " " $(NF-2)
                    }
                  } else {
                    # journal_size has unit
                    journal_size = $(NF-2) " " $(NF-1)
                    if ($(NF-3) ~ /^[0-9]/) {
                      # proof_size is single field
                      proof_size = $(NF-3)
                    } else {
                      # proof_size has unit
                      proof_size = $(NF-4) " " $(NF-3)
                    }
                  }

                  # Output CSV row
                  printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", query_type, epochs, num_sources, num_agg, kps, spe, total_keys, spk, total, prove_time, verify_time, proof_size, journal_size, dp_offset, mem_mb, seed, dp_enabled, dev_mode
                }
              ' >> "$OUTPUT_CSV"

              total_runs=$((total_runs + 1))
              echo "Progress: $total_runs configurations completed"
              echo ""
            done
          done
        done
      done
    done
  done
else
  # Legacy mode
  for epochs in $EPOCHS_LIST; do
    for keys in $KEYS_LIST; do
      for events_per_key in $EVENTS_PER_KEY_LIST; do
        total_events=$((epochs * keys * events_per_key))

        echo "==================================================="
        echo "Running: epochs=$epochs keys=$keys events_per_key=$events_per_key"
        echo "Total events: $total_events"
        echo "==================================================="

        # Build arguments
        args="--epochs $epochs --keys $keys --events-per-key $events_per_key --seed $SEED $DP_FLAG"
        if [[ "$SKIP_HISTOGRAM" == "1" ]]; then
          args="$args --skip-histogram"
        fi
        if [[ "$SKIP_HISTOGRAM_BUCKET" == "1" ]]; then
          args="$args --skip-histogram-bucket"
        fi
        if [[ "$SKIP_HISTOGRAM_ALL" == "1" ]]; then
          args="$args --skip-histogram-all"
        fi
        if [[ "$SKIP_HISTOGRAM_P90" == "1" ]]; then
          args="$args --skip-histogram-p90"
        fi
        if [[ "$SKIP_CM" == "1" ]]; then
          args="$args --skip-cm"
        fi
        if [[ "$SKIP_SAMPLES" == "1" ]]; then
          args="$args --skip-samples"
        fi
        if [[ "$SKIP_SAMPLES_SUM" == "1" ]]; then
          args="$args --skip-samples-sum"
        fi
        if [[ "$SKIP_SAMPLES_SUM_KEY" == "1" ]]; then
          args="$args --skip-samples-sum-key"
        fi
        if [[ "$SKIP_SAMPLES_SUM_TOPK" == "1" ]]; then
          args="$args --skip-samples-sum-topk"
        fi
        if [[ "$SKIP_RAW" == "1" ]]; then
          args="$args --skip-raw"
        fi

        # Run benchmark with /usr/bin/time to capture memory usage
        time_output_file=$(mktemp)
        output=$(/usr/bin/time -v "$BENCH_BIN" $args 2> "$time_output_file")

        # Extract max RSS (in KB) from time output and convert to MB
        max_rss_kb=$(grep "Maximum resident set size" "$time_output_file" | awk '{print $NF}')
        memory_mb=$(echo "scale=2; $max_rss_kb / 1024" | bc)
        rm -f "$time_output_file"

        # Parse output and extract results for legacy mode
        # Format: query_type,epochs,keys,events_per_key,total_events,prove_time_ms,verify_time_ms,proof_size_bytes,journal_size_bytes,dp_offset,memory_mb,seed,dp_enabled,risc0_dev_mode
        echo "$output" | awk -v epochs="$epochs" -v keys="$keys" -v epk="$events_per_key" -v total="$total_events" -v mem_mb="$memory_mb" -v seed="$SEED" -v dp_enabled="$DP_ENABLED" -v dev_mode="$RISC0_DEV_MODE" '
          BEGIN {
            in_results = 0
          }
          /=== Benchmark Results ===/ {
            in_results = 1
            next
          }
          /^-+$/ {
            if (in_results) {
              next
            }
          }
          in_results && NF >= 8 && $1 != "Query" {
            # Table format: query_type epochs keys proof_time verify_time proof_size journal_size dp_offset
            # DP offset is always the last field (numeric)
            query_type = $1
            prove_time = $4
            verify_time = $5
            dp_offset = $NF  # Last field is always dp_offset

            # Parse proof_size and journal_size (may have units)
            if ($(NF-1) ~ /^[0-9]/) {
              journal_size = $(NF-1)
              if ($(NF-2) ~ /^[0-9]/) {
                proof_size = $(NF-2)
              } else {
                proof_size = $(NF-3) " " $(NF-2)
              }
            } else {
              journal_size = $(NF-2) " " $(NF-1)
              if ($(NF-3) ~ /^[0-9]/) {
                proof_size = $(NF-3)
              } else {
                proof_size = $(NF-4) " " $(NF-3)
              }
            }

            # Output CSV row
            printf "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n", query_type, epochs, keys, epk, total, prove_time, verify_time, proof_size, journal_size, dp_offset, mem_mb, seed, dp_enabled, dev_mode
          }
        ' >> "$OUTPUT_CSV"

        total_runs=$((total_runs + 1))
        echo "Progress: $total_runs configurations completed"
        echo ""
      done
    done
  done
fi

echo "==================================================="
echo "Sweep completed! Results written to: $OUTPUT_CSV"
echo "Total configurations tested: $total_runs"
echo "==================================================="

# Display summary
echo ""
echo "Summary of results:"
cat "$OUTPUT_CSV"
