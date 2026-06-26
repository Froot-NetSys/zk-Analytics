#!/usr/bin/env bash
set -uo pipefail
# Measure CAIDA consumer-insert leg only (RocksDB raw insert) at the 8-node
# Figure-4 setup: 8 detached consumers on node0-7 + the CAIDA producer, drain to
# lag=0, then read rocksdb_raw_insert_ms from the busiest consumer. This is
# ingestion-time (pre-aggregation) so it is identical for ZK and non-ZK; it just
# fills the cell that the old CAIDA ZK run failed to preserve. No proving.
cd /mydata/zk-Analytics
LBIN="target/release"; DIST="$HOME/zktel-dist"; RBIN="$DIST/bin"
REMOTE_ENV="LD_LIBRARY_PATH=$DIST/lib PATH=$HOME/.cargo/bin:\$PATH"
KB="10.10.1.1:9092"; N=8; TOPIC="raw_caida_insert"; RUNID="caidains$(date +%s)"
on_node(){ local node="$1"; shift; if [ "$node" = node0 ]; then bash -c "$*"; else ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$node" "$*"; fi; }
spawn_node(){ local node="$1"; shift; local cmd="$*"
  if [ "$node" = node0 ]; then setsid bash -c "$cmd" </dev/null >/dev/null 2>&1 &
  else ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$node" "setsid bash -c '$cmd' </dev/null >/dev/null 2>&1 & echo ok" >/dev/null 2>&1; fi; }

KILL='for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done; true'
bash -c "$KILL"; for i in 1 2 3 4 5 6 7; do on_node node$i "$KILL" >/dev/null 2>&1 || true; done
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$TOPIC" >/dev/null 2>&1; sleep 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$TOPIC" --partitions "$N" --replication-factor 1 >/dev/null 2>&1

echo "[caida-insert] starting $N consumers (node0-7) ..."
for ((i=0;i<N;i++)); do
  on_node node$i "rm -rf /mydata/${RUNID}_raw_$i; mkdir -p /mydata/${RUNID}_raw_$i" >/dev/null 2>&1
  spawn_node node$i "cd $RBIN && env $REMOTE_ENV E2E_TIMING=1 KAFKA_BROKERS=$KB KAFKA_TOPIC=$TOPIC KAFKA_GROUP_ID=cg_${RUNID}_$i KAFKA_PARTITION_ID=$i RAW_DB_PATH=/mydata/${RUNID}_raw_$i EPOCH_BATCH_THRESHOLD=2048 EPOCH_TIMEOUT_MS=600000 ./kafka-consumer > /tmp/${RUNID}_cons_$i.log 2>&1"
done
sleep 5
echo "[caida-insert] producing 131072 CAIDA logs (8 partitions) ..."
env BENCH_INPUT=caida CAIDA_DIR="$(pwd)/testdata/caida_pcap/caida_txt" CAIDA_MAX_FILES=100000 \
  KAFKA_BROKERS="$KB" KAFKA_TOPIC="$TOPIC" NUM_AGGREGATORS="$N" PARALLEL_PRODUCERS="$N" DISTRIBUTE_EVENLY=0 \
  "$LBIN/kafka-producer" --events 131072 --commit-batch-size 8 > /tmp/${RUNID}_prod.log 2>&1
echo "[caida-insert] drain (lag->0) ..."
for ((t=0;t<300;t+=3)); do
  LAG=0; CUR=0
  for ((i=0;i<N;i++)); do
    read l c < <(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "cg_${RUNID}_$i" 2>/dev/null \
      | awk 'NR>1 && $6 ~ /^[0-9]+$/ {lag+=$6} NR>1 && $4 ~ /^[0-9]+$/ {cur+=$4} END{print lag+0, cur+0}')
    LAG=$((LAG+${l:-0})); CUR=$((CUR+${c:-0}))
  done
  [ "${CUR:-0}" -gt 0 ] && [ "${LAG:-0}" -eq 0 ] && { echo "[caida-insert] drained lag=0 consumed_msgs=$CUR"; break; }
  sleep 3
done
sleep 2
for ((i=0;i<N;i++)); do on_node node$i "pkill -TERM -f kafka-consumer" >/dev/null 2>&1 || true; done
sleep 4
echo "[caida-insert] per-node rocksdb_raw_insert_ms (and kafka_recv_ms):"
best=0
for ((i=0;i<N;i++)); do
  line=$(on_node node$i "grep -a 'e2e-timing.*kafka-consumer' /tmp/${RUNID}_cons_$i.log | tail -1" 2>/dev/null)
  ins=$(echo "$line" | grep -aoE "rocksdb_raw_insert_ms=[0-9.]+" | cut -d= -f2)
  kr=$(echo "$line" | grep -aoE "kafka_recv_ms=[0-9.]+" | cut -d= -f2)
  echo "  node$i: rocksdb_raw_insert_ms=${ins:-NA} kafka_recv_ms=${kr:-NA}"
  awk "BEGIN{exit !(${ins:-0} > $best)}" && best=${ins:-0}
done
echo "[caida-insert] BUSIEST rocksdb_raw_insert = ${best} ms"
for ((i=0;i<N;i++)); do on_node node$i "rm -rf /mydata/${RUNID}_raw_$i" >/dev/null 2>&1 || true; done
