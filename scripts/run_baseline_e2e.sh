#!/usr/bin/env bash
set -uo pipefail

# Single-cell non-ZK / zk-Analytics end-to-end baseline driver.
#
# Runs ONE (dataset, epoch_size, mode) measurement through the REAL pipeline:
#   data_source(kafka-producer) -> Kafka -> kafka-consumer -> RocksDB(raw)
#     -> aggregator (native or zk, reads RocksDB, writes FDB) -> FDB
#     -> querier (reads FDB, native or zk query)
#
# Captures per-component timing ([e2e-timing] lines, E2E_TIMING=1) and peak RSS
# (scripts/mem_trace.py, summed across aggregator workers; single process for
# the querier), and appends two JSON records (aggregation + query) to
# results/_e2e_metrics.jsonl for scripts/build_baseline_tables.py.
#
# Required env:
#   DATASET    google | caida | vehicle
#   MODE       native | zk
#   EPOCH_LOGS 8192 | 16384 | 32768   (logs per epoch; threshold = EPOCH_LOGS/8)
# Optional env (sane per-dataset defaults below):
#   NUM_AGGREGATORS, TOTAL_LOGS, COMMIT_BATCH_SIZE, AGGR_IDLE_TIMEOUT_SECS,
#   KAFKA_BROKERS, FDB_CLUSTER_FILE, WORK_BASE, QUERIER_PORT
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

DATASET="${DATASET:?set DATASET=google|caida|vehicle}"
MODE="${MODE:?set MODE=native|zk}"
EPOCH_LOGS="${EPOCH_LOGS:?set EPOCH_LOGS=8192|16384|32768}"

COMMIT_BATCH_SIZE="${COMMIT_BATCH_SIZE:-8}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9092}"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-/etc/foundationdb/fdb.cluster}"
AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-15}"
QUERIER_PORT="${QUERIER_PORT:-8090}"
WORK_BASE="${WORK_BASE:-/mydata/baseline_run}"
MEM_INTERVAL="${MEM_INTERVAL:-0.5}"

# Structural core params: MUST match the zkVM benchmark configuration (paper).
export SAMPLES_HT_BUCKETS="${SAMPLES_HT_BUCKETS:-64}"
export SAMPLES_HT_BUCKET_CAP="${SAMPLES_HT_BUCKET_CAP:-4}"
export HISTOGRAM_SLOTS="${HISTOGRAM_SLOTS:-32}"
export CM_TOPK_SLOTS="${CM_TOPK_SLOTS:-100}"

# ---- Per-dataset configuration (Figure 4 mapping) -------------------------
case "$DATASET" in
  google)
    AGG_MODE=samples; AGG_TYPE=hash_table
    NUM_AGGREGATORS="${NUM_AGGREGATORS:-8}"; TOTAL_LOGS="${TOTAL_LOGS:-131072}"
    BENCH_INPUT=google
    DATASET_ENV=( "TSV_DIR=$ROOT_DIR/testdata/google_cluster_data/input" "TSV_MAX_FILES=8192" "CSV_VALUE_SCALE=1000000" "TS_INTERVAL_MS=100" )
    QUERY_JSON='{"type":"samples_sum","epochs":100000}'
    ;;
  caida)
    AGG_MODE=cm; AGG_TYPE=cms
    NUM_AGGREGATORS="${NUM_AGGREGATORS:-8}"; TOTAL_LOGS="${TOTAL_LOGS:-131072}"
    BENCH_INPUT=caida
    DATASET_ENV=( "CAIDA_DIR=$ROOT_DIR/testdata/caida_pcap/caida_txt" "CAIDA_MAX_FILES=100000" )
    QUERY_JSON='{"type":"cm_topk","epochs":100000,"limit":10}'
    ;;
  vehicle)
    AGG_MODE=histogram; AGG_TYPE=histogram
    NUM_AGGREGATORS="${NUM_AGGREGATORS:-4}"; TOTAL_LOGS="${TOTAL_LOGS:-10058}"
    BENCH_INPUT=car_emission
    DATASET_ENV=( "EMISSION_CSV=$ROOT_DIR/testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv" )
    QUERY_JSON='{"type":"histogram_p90","epochs":100000}'
    ;;
  *) echo "unknown DATASET=$DATASET"; exit 2;;
esac

EPOCH_BATCH_THRESHOLD=$(( EPOCH_LOGS / COMMIT_BATCH_SIZE ))
TAG="${DATASET}_${AGG_MODE}_e${EPOCH_LOGS}_${MODE}"
WORK="$WORK_BASE/$TAG"
LOGDIR="$WORK/logs"
RESULTS_DIR="$ROOT_DIR/results"
METRICS="$RESULTS_DIR/_e2e_metrics.jsonl"
FDB_SUBSPACE="zktel_base_${TAG}"
KAFKA_TOPIC="raw_${TAG}"
mkdir -p "$LOGDIR" "$RESULTS_DIR"

if [ "$MODE" = "native" ]; then NO_ZKVM_PROOF=1; QUERY_NO_PROVE=1; else NO_ZKVM_PROOF=0; QUERY_NO_PROVE=0; fi

REL="target/release"
AGGR_BIN="$ROOT_DIR/$REL/aggregator"
CONSUMER_BIN="$ROOT_DIR/$REL/kafka-consumer"
PRODUCER_BIN="$ROOT_DIR/$REL/kafka-producer"
QUERIER_BIN="$ROOT_DIR/$REL/querier"
for b in "$AGGR_BIN" "$CONSUMER_BIN" "$PRODUCER_BIN" "$QUERIER_BIN"; do
  [ -x "$b" ] || { echo "missing binary: $b (build first)"; exit 3; }
done

echo "[cell] dataset=$DATASET mode=$MODE agg=$AGG_MODE epoch_logs=$EPOCH_LOGS threshold=$EPOCH_BATCH_THRESHOLD aggregators=$NUM_AGGREGATORS total_logs=$TOTAL_LOGS"

PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done
  pkill -f "mem_trace.py --out $WORK" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

# ---- Zero-contention: kill ANY leftover prover/aggregator/consumer/querier
# from a prior run so this experiment gets the full machine (56 cores). ----
killall -9 r0vm aggregator querier \
  kafka-consumer kafka-producer 2>/dev/null || true
pkill -9 -f mem_trace.py 2>/dev/null || true
sleep 2

# ---- Clean slate: RocksDB dirs, FDB subspace, Kafka topic -----------------
rm -rf "$WORK"/raw_* "$WORK"/agg_* 2>/dev/null || true
mkdir -p "$LOGDIR"
FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" FDB_SUBSPACE="$FDB_SUBSPACE" \
  bash "$ROOT_DIR/scripts/reset_fdb.sh" "$FDB_SUBSPACE" >/dev/null 2>&1 || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$KAFKA_TOPIC" >/dev/null 2>&1 || true
sleep 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$KAFKA_TOPIC" \
  --partitions "$NUM_AGGREGATORS" --replication-factor 1 >/dev/null 2>&1 || true

# ---- 1) Start N kafka-consumers (one per partition) -----------------------
echo "[cell] starting $NUM_AGGREGATORS kafka-consumers ..."
CONSUMER_PIDS=()
for ((i=0; i<NUM_AGGREGATORS; i++)); do
  E2E_TIMING=1 KAFKA_BROKERS="$KAFKA_BROKERS" KAFKA_TOPIC="$KAFKA_TOPIC" \
    KAFKA_GROUP_ID="cg_${TAG}_$i" KAFKA_PARTITION_ID="$i" \
    RAW_DB_PATH="$WORK/raw_$i" \
    EPOCH_BATCH_THRESHOLD="$EPOCH_BATCH_THRESHOLD" EPOCH_TIMEOUT_MS=600000 \
    "$CONSUMER_BIN" > "$LOGDIR/consumer_$i.log" 2>&1 &
  CONSUMER_PIDS+=($!); PIDS+=($!)
done
sleep 3

# ---- 2) Produce the dataset into Kafka ------------------------------------
echo "[cell] producing $TOTAL_LOGS logs ($BENCH_INPUT) ..."
env BENCH_INPUT="$BENCH_INPUT" "${DATASET_ENV[@]}" \
  KAFKA_BROKERS="$KAFKA_BROKERS" KAFKA_TOPIC="$KAFKA_TOPIC" \
  NUM_AGGREGATORS="$NUM_AGGREGATORS" PARALLEL_PRODUCERS="$NUM_AGGREGATORS" \
  DISTRIBUTE_EVENLY="${DISTRIBUTE_EVENLY:-0}" \
  "$PRODUCER_BIN" --events "$TOTAL_LOGS" --commit-batch-size "$COMMIT_BATCH_SIZE" \
  > "$LOGDIR/producer.log" 2>&1
echo "[cell] producer done; waiting for consumers to drain Kafka ..."

# Total messages produced = sum of partition end offsets.
total_end() {
  docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 --topic "$KAFKA_TOPIC" 2>/dev/null \
    | awk -F: '{s+=$3} END{print s+0}'
}
# Total consumed = sum of CURRENT-OFFSET across the per-consumer groups.
total_consumed() {
  local sum=0 g cur
  for ((i=0; i<NUM_AGGREGATORS; i++)); do
    cur=$(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
      --describe --group "cg_${TAG}_$i" 2>/dev/null \
      | awk 'NR>1 && $4 ~ /^[0-9]+$/ {s+=$4} END{print s+0}')
    sum=$(( sum + ${cur:-0} ))
  done
  echo "$sum"
}
PRODUCED=$(total_end)
echo "[cell] produced offsets total=$PRODUCED; polling consumption ..."
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-180}"
for ((t=0; t<DRAIN_TIMEOUT; t+=3)); do
  C=$(total_consumed)
  if [ "${C:-0}" -ge "${PRODUCED:-1}" ] && [ "${PRODUCED:-0}" -gt 0 ]; then
    echo "[cell] consumed=$C/$PRODUCED (drained)"; break
  fi
  sleep 3
done
sleep 2  # let the last in-flight batch land in RocksDB
for p in "${CONSUMER_PIDS[@]}"; do kill -TERM "$p" 2>/dev/null || true; done
for p in "${CONSUMER_PIDS[@]}"; do wait "$p" 2>/dev/null || true; done

RAW_EPOCHS=0
for ((i=0; i<NUM_AGGREGATORS; i++)); do
  n=$(grep -c 'created epoch seq=' "$LOGDIR/consumer_$i.log" 2>/dev/null); n=${n//[^0-9]/}
  RAW_EPOCHS=$(( RAW_EPOCHS + ${n:-0} ))
done
echo "[cell] consumers wrote $RAW_EPOCHS raw epochs total"

# ---- 3) Run aggregators (native or zk), with memory tracing ---------------
echo "[cell] running $NUM_AGGREGATORS aggregators (mode=$MODE) ..."
# Match the aggregator host AND its r0vm prover subprocess so the peak RSS
# includes the (dominant) proving working set, not just the host process.
python3 "$ROOT_DIR/scripts/mem_trace.py" \
  --out "$WORK/mem_aggregation.csv" --summary "$WORK/mem_agg_summary.json" \
  --match aggregator --match r0vm --interval "$MEM_INTERVAL" \
  > "$LOGDIR/mempoll_agg.log" 2>&1 &
MEMPID=$!; PIDS+=($MEMPID)

# Proving concurrency: by default run ONE aggregator at a time so each gets the
# full machine (56 cores) — faithfully reproducing the paper's per-machine cost
# (8 aggregators = 8 machines, each on a full 56-core box). The reported
# aggregation time is the critical path (busiest single aggregator), which is
# machine-count-independent. Override with AGG_CONCURRENCY if desired.
AGG_CONCURRENCY="${AGG_CONCURRENCY:-1}"
THREADS="${RAYON_NUM_THREADS:-56}"
launch_aggr() {  # i
  local i="$1"
  /usr/bin/time -v env E2E_TIMING=1 NO_ZKVM_PROOF="$NO_ZKVM_PROOF" \
    RAW_ROCKSDB_PATH="$WORK/raw_$i" AGG_ROCKSDB_PATH="$WORK/agg_$i" \
    AGGREGATOR_ID="$i" \
    FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" FDB_SUBSPACE="$FDB_SUBSPACE" \
    AGGR_IDLE_TIMEOUT_SECS="$AGGR_IDLE_TIMEOUT_SECS" \
    AGGR_PIPELINE=rocksdb \
    RAYON_NUM_THREADS="$THREADS" \
    "$AGGR_BIN" --rocksdb --mode "$AGG_MODE" --threads "$THREADS" \
    > "$LOGDIR/agg_$i.log" 2> "$LOGDIR/agg_${i}_time.log"
}
AGG_START=$(date +%s.%N)
# Run aggregators in batches of AGG_CONCURRENCY (default 1 = full cores each).
for ((start=0; start<NUM_AGGREGATORS; start+=AGG_CONCURRENCY)); do
  batch_pids=()
  for ((i=start; i<start+AGG_CONCURRENCY && i<NUM_AGGREGATORS; i++)); do
    launch_aggr "$i" & batch_pids+=($!); PIDS+=($!)
  done
  for p in "${batch_pids[@]}"; do wait "$p" 2>/dev/null || true; done
done
AGG_END=$(date +%s.%N)
kill -INT "$MEMPID" 2>/dev/null || true; wait "$MEMPID" 2>/dev/null || true

# ---- 4) Start querier, run the dataset query ------------------------------
echo "[cell] starting querier ..."
# Query engine + its r0vm prover subprocess (the querier delegates proving to
# r0vm, so its own RSS is tiny without this).
python3 "$ROOT_DIR/scripts/mem_trace.py" \
  --out "$WORK/mem_query.csv" --summary "$WORK/mem_query_summary.json" \
  --match querier --match r0vm --interval "$MEM_INTERVAL" \
  > "$LOGDIR/mempoll_query.log" 2>&1 &
QMEMPID=$!; PIDS+=($QMEMPID)

# Disable the access-control policy for the performance baseline: it must run
# all query types (incl. cm_topk, which the default policy rejects). Access
# control is ON by default in deployments; QUERY_POLICY_ENFORCE=0 opts out here.
/usr/bin/time -v env E2E_TIMING=1 BENCH_PRINT=1 QUERY_NO_PROVE="$QUERY_NO_PROVE" \
  QUERY_POLICY_ENFORCE=0 \
  FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" FDB_SUBSPACE="$FDB_SUBSPACE" \
  HTTP_LISTEN="0.0.0.0:$QUERIER_PORT" \
  "$QUERIER_BIN" > "$LOGDIR/querier.log" 2>&1 &
QPID=$!; PIDS+=($QPID)

# Wait for querier to accept connections.
for _ in $(seq 1 60); do
  if curl -s -o /dev/null -m 2 "localhost:$QUERIER_PORT/query" \
       -H 'content-type: application/json' -d "$QUERY_JSON"; then break; fi
  sleep 1
done
# Issue the query (a few reps; the last bench line is parsed).
for _ in 1 2 3; do
  curl -s -m 600 "localhost:$QUERIER_PORT/query" \
    -H 'content-type: application/json' -d "$QUERY_JSON" \
    >> "$LOGDIR/query_response.json" 2>/dev/null || true
done
sleep 1
kill -INT "$QMEMPID" 2>/dev/null || true; wait "$QMEMPID" 2>/dev/null || true
kill -TERM "$QPID" 2>/dev/null || true; wait "$QPID" 2>/dev/null || true

# ---- 5) Parse + emit metrics ----------------------------------------------
echo "[cell] parsing metrics -> $METRICS"
AGG_WALL=$(python3 -c "print(f'{$AGG_END-$AGG_START:.3f}')")
python3 "$ROOT_DIR/scripts/_parse_e2e_cell.py" \
  --dataset "$DATASET" --agg-type "$AGG_TYPE" --mode "$MODE" \
  --epoch-size "$EPOCH_LOGS" --num-aggregators "$NUM_AGGREGATORS" \
  --workdir "$WORK" --logdir "$LOGDIR" --agg-wall "$AGG_WALL" \
  --metrics "$METRICS"

echo "[cell] done: $TAG"
