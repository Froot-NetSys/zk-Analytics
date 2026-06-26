#!/usr/bin/env bash
set -uo pipefail
# Re-measure CM native aggregation compute time across unique-key counts, 3 reps
# each, to characterize variance and confirm CM compute is ~key-independent.
cd /mydata/zk-Analytics
B=target/release; PROD=target/release/kafka-producer; FC=$HOME/zktel-dist/fdb.cluster; KB=10.10.1.1:9092
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
for keys in 256 1024 2048 4096; do
  for rep in 1 2 3; do
    TAG=cmre_${keys}_${rep}; TOPIC=raw_$TAG; SUB=zktel_$TAG; RAW=/mydata/${TAG}_raw; AGG=/mydata/${TAG}_agg
    for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done
    rm -rf "$RAW" "$AGG"; mkdir -p "$RAW"
    FDB_CLUSTER_FILE=$FC bash scripts/reset_fdb.sh "$SUB" >/dev/null 2>&1 || true
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 1
    docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1
    KAFKA_BROKERS=$KB KAFKA_TOPIC=$TOPIC KAFKA_GROUP_ID=cg_$TAG KAFKA_PARTITION_ID=0 RAW_DB_PATH=$RAW EPOCH_BATCH_THRESHOLD=2048 EPOCH_TIMEOUT_MS=600000 "$B/kafka-consumer" > /tmp/${TAG}_c.log 2>&1 & CPID=$!
    sleep 2
    BENCH_INPUT=synthetic KAFKA_BROKERS=$KB KAFKA_TOPIC=$TOPIC NUM_AGGREGATORS=1 SOURCE_ID=0 "$PROD" --events 16384 --commit-batch-size 8 --key-mod "$keys" > /tmp/${TAG}_p.log 2>&1
    sleep 4; kill -TERM $CPID 2>/dev/null; wait $CPID 2>/dev/null
    E2E_TIMING=1 NO_ZKVM_PROOF=1 AGGR_PIPELINE=rocksdb RAW_ROCKSDB_PATH=$RAW AGG_ROCKSDB_PATH=$AGG AGGREGATOR_ID=0 FDB_CLUSTER_FILE=$FC FDB_SUBSPACE=$SUB AGGR_IDLE_TIMEOUT_SECS=12 RAYON_NUM_THREADS=56 "$B/aggregator" --rocksdb --mode cm --threads 56 > /tmp/${TAG}_a.log 2>&1
    cm=$(grep -aoE "aggr_compute_ms=[0-9.]+" /tmp/${TAG}_a.log | tail -1 | cut -d= -f2)
    echo "RESULT keys=$keys rep=$rep aggr_compute_ms=${cm:-MISSING}"
    rm -rf "$RAW" "$AGG"
  done
done
echo "DONE"
