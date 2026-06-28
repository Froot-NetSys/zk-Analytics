#!/usr/bin/env bash
set -uo pipefail
# Fig 7 native: offline query, vary #queried epochs 1..256. CM 8192 keys,
# Hist/Hash 1024 keys, 8192 logs/epoch. Aggregate N_EPOCHS epochs once
# (native, via Kafka->RocksDB->aggregator->FDB), then run the native query
# (QUERY_NO_PROVE=1) over varying epoch counts; report fdb_lookup + deserialize
# + query_compute + total per epoch count.
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"; cd "$ROOT_DIR"
LBIN="$ROOT_DIR/target/release"; DIST="$HOME/zktel-dist"; RBIN="$DIST/bin"
REMOTE_ENV="LD_LIBRARY_PATH=$DIST/lib PATH=$HOME/.cargo/bin:\$PATH"
KAFKA_BROKERS="192.0.2.1:9092"; FDB_CLUSTER_FILE="$HOME/zktel-dist/fdb.cluster"
QPORT="${QUERIER_PORT:-8090}"; COMMIT=8; EPOCH_LOGS=8192
N_EPOCHS="${N_EPOCHS:-256}"; COUNTS="${COUNTS:-1 2 4 8 16 32 64 128 256}"
export SAMPLES_HT_BUCKETS=64 SAMPLES_HT_BUCKET_CAP=4 HISTOGRAM_SLOTS=32 CM_TOPK_SLOTS=100
OUT="$ROOT_DIR/results/fig7_native.csv"; echo "epoch_type,query,queried_epochs,query_total_s,fdb_lookup_s,deserialize_s,query_compute_s" > "$OUT"
KILL='for p in $(pgrep -x zktelemetry-ris); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done; pkill -9 -f mem_trace.py 2>/dev/null'

# epoch_type:mode:query:keys
for cfg in samples:samples:samples_sum:1024 histogram:histogram:histogram_p90:1024 cm:cm:cm_topk:8192; do
  IFS=: read -r et mode query keys <<< "$cfg"
  echo "=== Fig7 $et (keys=$keys, ${N_EPOCHS} epochs) ==="
  TAG="fig7_${et}"; SUB="zktel_$TAG"; TOPIC="raw_$TAG"; RAW=/mydata/${TAG}_raw; AGG=/mydata/${TAG}_agg
  eval "$KILL"; rm -rf "$RAW" "$AGG"; mkdir -p "$RAW"
  FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" bash scripts/setup/reset_fdb.sh "$SUB" >/dev/null 2>&1 || true
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 1
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null 2>&1
  # consumer (node0 local)
  E2E_TIMING=1 KAFKA_BROKERS=$KAFKA_BROKERS KAFKA_TOPIC=$TOPIC KAFKA_GROUP_ID=cg_$TAG KAFKA_PARTITION_ID=0 \
    RAW_DB_PATH=$RAW EPOCH_BATCH_THRESHOLD=$((EPOCH_LOGS/COMMIT)) EPOCH_TIMEOUT_MS=600000 \
    "$LBIN/kafka-consumer" > /tmp/${TAG}_cons.log 2>&1 & CPID=$!
  sleep 3
  # produce N_EPOCHS*EPOCH_LOGS synthetic logs
  total=$((N_EPOCHS*EPOCH_LOGS))
  BENCH_INPUT=synthetic KAFKA_BROKERS=$KAFKA_BROKERS KAFKA_TOPIC=$TOPIC NUM_AGGREGATORS=1 SOURCE_ID=0 \
    "$LBIN/kafka-producer" --events $total --commit-batch-size $COMMIT --key-mod $keys > /tmp/${TAG}_prod.log 2>&1
  sleep 5; kill -TERM $CPID 2>/dev/null; wait $CPID 2>/dev/null
  # aggregate ALL epochs natively (no proof) -> FDB
  echo "[fig7] aggregating $N_EPOCHS epochs ($et) ..."
  E2E_TIMING=1 NO_ZKVM_PROOF=1 AGGR_PIPELINE=rocksdb RAW_ROCKSDB_PATH=$RAW AGG_ROCKSDB_PATH=$AGG AGGREGATOR_ID=0 \
    FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE FDB_SUBSPACE=$SUB AGGR_IDLE_TIMEOUT_SECS=15 RAYON_NUM_THREADS=56 \
    "$LBIN/aggregator" --rocksdb --mode $mode --threads 56 > /tmp/${TAG}_agg.log 2>&1
  # start querier (native, no prove)
  E2E_TIMING=1 BENCH_PRINT=1 QUERY_NO_PROVE=1 FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE FDB_SUBSPACE=$SUB \
    HTTP_LISTEN=0.0.0.0:$QPORT "$LBIN/querier" > /tmp/${TAG}_q.log 2>&1 & QPID=$!
  for _ in $(seq 1 60); do curl -s -o /dev/null -m 2 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":1}" && break; sleep 1; done
  for ec in $COUNTS; do
    : > /tmp/${TAG}_q.log   # fresh bench line per query
    # re-set BENCH_PRINT marker by re-querying; capture last bench lines
    for rep in 1 2 3; do curl -s -m 60 localhost:$QPORT/query -H 'content-type: application/json' -d "{\"type\":\"$query\",\"epochs\":$ec}" >/dev/null 2>&1; done
    db=$(grep -aoE 'db_ms=[0-9]+' /tmp/${TAG}_q.log | tail -1 | cut -d= -f2); mg=$(grep -aoE 'merge_ms=[0-9]+' /tmp/${TAG}_q.log | tail -1 | cut -d= -f2)
    pr=$(grep -aoE 'prove_ms=[0-9]+' /tmp/${TAG}_q.log | tail -1 | cut -d= -f2)
    db=${db:-0}; mg=${mg:-0}; pr=${pr:-0}
    tot=$(python3 -c "print(f'{($db+$mg+$pr)/1000:.6f}')")
    echo "$et,$query,$ec,$tot,$(python3 -c "print($db/1000)"),$(python3 -c "print($mg/1000)"),$(python3 -c "print($pr/1000)")" >> "$OUT"
    echo "[fig7] $et epochs=$ec db=${db}ms merge=${mg}ms"
  done
  kill -TERM $QPID 2>/dev/null; wait $QPID 2>/dev/null
done
echo "[fig7] -> $OUT"
