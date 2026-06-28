#!/usr/bin/env bash
set -uo pipefail
# Clean CM compute re-measurement: all 5 key counts, 1 discarded warmup + 5
# measured reps each, report all values (median computed downstream). Confirms
# CM native aggregation compute is ~key-independent (fixed-size sketch).
cd /mydata/zk-Analytics
B=target/release; PROD=target/release/kafka-producer; FC=$HOME/zktel-dist/fdb.cluster; KB=192.0.2.1:9092
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
run_one(){ # keys tag -> echoes aggr_compute_ms
  local keys=$1 TAG=$2
  local TOPIC=raw_$TAG SUB=zktel_$TAG RAW=/mydata/${TAG}_raw AGG=/mydata/${TAG}_agg
  for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done
  rm -rf "$RAW" "$AGG"; mkdir -p "$RAW"
  FDB_CLUSTER_FILE=$FC bash scripts/setup/reset_fdb.sh "$SUB" >/dev/null 2>&1 || true
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 1
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  KAFKA_BROKERS=$KB KAFKA_TOPIC=$TOPIC KAFKA_GROUP_ID=cg_$TAG KAFKA_PARTITION_ID=0 RAW_DB_PATH=$RAW EPOCH_BATCH_THRESHOLD=2048 EPOCH_TIMEOUT_MS=600000 "$B/kafka-consumer" > /tmp/${TAG}_c.log 2>&1 & local CPID=$!
  sleep 2
  BENCH_INPUT=synthetic KAFKA_BROKERS=$KB KAFKA_TOPIC=$TOPIC NUM_AGGREGATORS=1 SOURCE_ID=0 "$PROD" --events 16384 --commit-batch-size 8 --key-mod "$keys" > /tmp/${TAG}_p.log 2>&1
  sleep 4; kill -TERM $CPID 2>/dev/null; wait $CPID 2>/dev/null
  E2E_TIMING=1 NO_ZKVM_PROOF=1 AGGR_PIPELINE=rocksdb RAW_ROCKSDB_PATH=$RAW AGG_ROCKSDB_PATH=$AGG AGGREGATOR_ID=0 FDB_CLUSTER_FILE=$FC FDB_SUBSPACE=$SUB AGGR_IDLE_TIMEOUT_SECS=12 RAYON_NUM_THREADS=56 "$B/aggregator" --rocksdb --mode cm --threads 56 > /tmp/${TAG}_a.log 2>&1
  grep -aoE "aggr_compute_ms=[0-9.]+" /tmp/${TAG}_a.log | tail -1 | cut -d= -f2
  rm -rf "$RAW" "$AGG"
}
for keys in 256 512 1024 2048 4096; do
  run_one "$keys" "cmw_${keys}_warm" >/dev/null 2>&1   # discard warmup
  vals=""
  for rep in 1 2 3 4 5; do
    v=$(run_one "$keys" "cmw_${keys}_${rep}")
    vals="$vals ${v:-NA}"
  done
  echo "RESULT keys=$keys vals=$vals"
done
echo "DONE"
