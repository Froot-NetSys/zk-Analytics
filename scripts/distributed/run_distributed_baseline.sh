#!/usr/bin/env bash
set -uo pipefail

# Distributed (multi-node) non-ZK/zk e2e baseline cell for the Figure-4 main
# table. Aggregators run on REAL nodes (node0..node{N-1}) in PARALLEL, each
# proving its partition on a full 56-core machine — faithful to the paper's
# distributed deployment. node0 hosts Kafka + FDB + producer + querier.
#
# Per-node peak memory = host(aggregator) + r0vm(prover), captured by mem_trace
# on each node (corrects the paper's host-only Table 2 memory).
#
# Required env:
#   DATASET   google | caida | vehicle
#   MODE      native | zk
#   NODES     space list of node hostnames (NODES[0]=node0=coordinator),
#             e.g. "node0 node1 node2 node3 node4 node5 node6 node7"
# Optional: EPOCH_LOGS(16384) COMMIT_BATCH_SIZE(8) KAFKA_HOST(192.0.2.1)
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

DATASET="${DATASET:?google|caida|vehicle}"
MODE="${MODE:?native|zk}"
NODES_STR="${NODES:?space list of nodes, NODES[0]=coordinator}"
read -r -a NODES <<< "$NODES_STR"
NUM_AGGREGATORS="${#NODES[@]}"
COORD="${NODES[0]}"

EPOCH_LOGS="${EPOCH_LOGS:-16384}"
COMMIT_BATCH_SIZE="${COMMIT_BATCH_SIZE:-8}"
KAFKA_HOST="${KAFKA_HOST:-192.0.2.1}"
KAFKA_BROKERS="${KAFKA_HOST}:9092"
FDB_CLUSTER_FILE="${FDB_CLUSTER_FILE:-$HOME/zktel-dist/fdb.cluster}"
QUERIER_PORT="${QUERIER_PORT:-8090}"
DIST="$HOME/zktel-dist"           # per-node deploy dir (bin/, lib/)
REMOTE_ENV="LD_LIBRARY_PATH=$DIST/lib PATH=$HOME/.cargo/bin:\$PATH"
AGGR_IDLE_TIMEOUT_SECS="${AGGR_IDLE_TIMEOUT_SECS:-20}"
MEM_INTERVAL="${MEM_INTERVAL:-1.0}"

source "$ROOT_DIR/scripts/lib/common.sh"

case "$DATASET" in
  google) AGG_MODE=samples; AGG_TYPE=hash_table; TOTAL_LOGS="${TOTAL_LOGS:-131072}"
    BENCH_INPUT=google
    DATASET_ENV=( "TSV_DIR=$ROOT_DIR/testdata/google_cluster_data/input" "TSV_MAX_FILES=8192" "CSV_VALUE_SCALE=1000000" "TS_INTERVAL_MS=100" )
    QUERY_JSON='{"type":"samples_sum","epochs":100000}';;
  caida)  AGG_MODE=cm; AGG_TYPE=cms; TOTAL_LOGS="${TOTAL_LOGS:-131072}"
    BENCH_INPUT=caida
    DATASET_ENV=( "CAIDA_DIR=$ROOT_DIR/testdata/caida_pcap/caida_txt" "CAIDA_MAX_FILES=100000" )
    QUERY_JSON='{"type":"cm_topk","epochs":100000,"limit":10}';;
  vehicle) AGG_MODE=histogram; AGG_TYPE=histogram; TOTAL_LOGS="${TOTAL_LOGS:-10058}"
    BENCH_INPUT=car_emission
    DATASET_ENV=( "EMISSION_CSV=$ROOT_DIR/testdata/car_emission/my2015-2024-fuel-consumption-ratings.csv" )
    QUERY_JSON='{"type":"histogram_p90","epochs":100000}';;
  synthetic) AGG_MODE="${SYNTH_MODE:-samples}"; TOTAL_LOGS="${TOTAL_LOGS:-131072}"
    SYNTH_KEYS="${SYNTH_KEYS:-4096}"; BENCH_INPUT=synthetic; DATASET_ENV=()
    case "$AGG_MODE" in samples) AGG_TYPE=hash_table; QUERY_JSON='{"type":"samples_sum","epochs":100000}';;
      cm) AGG_TYPE=cms; QUERY_JSON='{"type":"cm_topk","epochs":100000,"limit":10}';;
      histogram) AGG_TYPE=histogram; QUERY_JSON='{"type":"histogram_p90","epochs":100000}';; esac;;
  *) echo "unknown DATASET"; exit 2;;
esac

EPOCH_BATCH_THRESHOLD=$(( EPOCH_LOGS / COMMIT_BATCH_SIZE ))
# Unique per-run RocksDB dir prefix so a leftover consumer from a prior run can
# never collide with / corrupt this run's fresh DB.
RUNID="${RUNID:-r$(date +%s)}"
RAWP="/mydata/${RUNID}_raw"; AGGP="/mydata/${RUNID}_agg"
TAG="${DATASET}_${AGG_MODE}_n${NUM_AGGREGATORS}_${MODE}"
WORK="/mydata/dist_run/$TAG"; LOGDIR="$WORK/logs"
RESULTS_DIR="$ROOT_DIR/results"; METRICS="$RESULTS_DIR/_dist_metrics.jsonl"
FDB_SUBSPACE="zktel_dist_${TAG}"; KAFKA_TOPIC="raw_${TAG}"
mkdir -p "$LOGDIR" "$RESULTS_DIR"
if [ "$MODE" = native ]; then NO_ZKVM_PROOF=1; QUERY_NO_PROVE=1; else NO_ZKVM_PROOF=0; QUERY_NO_PROVE=0; fi
# RISC0_DEV_MODE=1 executes the zkVM guests but fakes the STARK proof (fast,
# end-to-end pipeline validation without hours of proving). Forwarded to the
# remote aggregators and the node0 querier below. Default 0 = real proofs.
RISC0_DEV_MODE="${RISC0_DEV_MODE:-0}"

LBIN="$ROOT_DIR/target/release"   # local (node0) binaries
RBIN="$DIST/bin"                        # remote node binaries

echo "[dist] $DATASET mode=$MODE nodes=$NUM_AGGREGATORS ($NODES_STR) epoch=$EPOCH_LOGS"

# run on a node (foreground, blocks): $1=node, rest=command.
on_node() { local node="$1"; shift; if [ "$node" = "$COORD" ] || [ "$node" = node0 ]; then bash -c "$*"; else ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$node" "$*"; fi; }

# launch a long-running command DETACHED on a node and return immediately
# (setsid so it survives the SSH channel close; cmd handles its own redirects).
spawn_node() { local node="$1"; shift; local cmd="$*";
  if [ "$node" = "$COORD" ] || [ "$node" = node0 ]; then
    setsid bash -c "$cmd" </dev/null >/dev/null 2>&1 &
  else
    ssh -n -o BatchMode=yes -o ConnectTimeout=8 "$node" "setsid bash -c '$cmd' </dev/null >/dev/null 2>&1 & echo ok" >/dev/null 2>&1
  fi
}

# Reliable kill: r0vm has an EMPTY /proc/cmdline so pkill -f never matches it ->
# must use comm (-x r0vm). The aggregator comm truncates to "zktelemetry-ris".
# Kill aggregator+consumer FIRST (the aggregator respawns r0vm), then r0vm.
ALLNODES="node0 node1 node2 node3 node4 node5 node6 node7"
# PID-based kill is reliable; pkill -x/-9 on r0vm races (it survives). r0vm
# must be killed AFTER the aggregator (which would respawn it).
KILLPATS='for p in $(pgrep -x zktelemetry-ris); do kill -9 $p 2>/dev/null; done; for p in $(pgrep -x kafka-consumer); do kill -9 $p 2>/dev/null; done; pkill -9 -f mem_trace.py 2>/dev/null; sleep 1; for p in $(pgrep -x r0vm); do kill -9 $p 2>/dev/null; done'
cleanup() { for n in $ALLNODES; do on_node "$n" "$KILLPATS" >/dev/null 2>&1 || true; done; }
trap cleanup EXIT INT TERM

# Hard pre-run nuke: kill everything on ALL nodes and WAIT until every node is
# idle (no r0vm / aggregator) so each experiment starts on a clean machine.
echo "[dist] nuking all nodes + waiting for idle ..."
for n in $ALLNODES; do on_node "$n" "$KILLPATS" >/dev/null 2>&1 & done; wait
for attempt in 1 2 3 4 5 6 7 8 9 10; do
  busy=0
  for n in $ALLNODES; do
    r=$(on_node "$n" "pgrep -xc r0vm 2>/dev/null; pgrep -xc zktelemetry-ris 2>/dev/null" 2>/dev/null | paste -sd+ | bc 2>/dev/null)
    [ "${r:-0}" -gt 0 ] && { busy=1; on_node "$n" "for p in \$(pgrep -x zktelemetry-ris); do kill -9 \$p; done; sleep 1; for p in \$(pgrep -x r0vm); do kill -9 \$p; done" >/dev/null 2>&1; }
  done
  [ "$busy" = 0 ] && { echo "[dist] all nodes idle"; break; }
  sleep 2
done

# ---- clean slate ----------------------------------------------------------
echo "[dist] reset FDB + topic + per-node raw dirs ..."
FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" bash "$ROOT_DIR/scripts/setup/reset_fdb.sh" "$FDB_SUBSPACE" >/dev/null 2>&1 || true
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic "$KAFKA_TOPIC" >/dev/null 2>&1 || true
sleep 1
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic "$KAFKA_TOPIC" --partitions "$NUM_AGGREGATORS" --replication-factor 1 >/dev/null 2>&1 || true
# Fresh unique RocksDB dirs (nodes already idle from the pre-run nuke; the
# RUNID prefix guarantees no collision with any leftover dir).
for ((i=0;i<NUM_AGGREGATORS;i++)); do
  on_node "${NODES[i]}" "rm -rf ${RAWP}_$i ${AGGP}_$i; mkdir -p ${RAWP}_$i" >/dev/null 2>&1
done
sleep 1

# ---- 1) consumers on each node -------------------------------------------
echo "[dist] starting $NUM_AGGREGATORS kafka-consumers (one per node) ..."
for ((i=0;i<NUM_AGGREGATORS;i++)); do
  spawn_node "${NODES[i]}" "cd $RBIN && env $REMOTE_ENV E2E_TIMING=1 KAFKA_BROKERS=$KAFKA_BROKERS KAFKA_TOPIC=$KAFKA_TOPIC KAFKA_GROUP_ID=cg_${TAG}_$i KAFKA_PARTITION_ID=$i RAW_DB_PATH=${RAWP}_$i EPOCH_BATCH_THRESHOLD=$EPOCH_BATCH_THRESHOLD EPOCH_TIMEOUT_MS=600000 ./kafka-consumer > /tmp/dist_consumer_$i.log 2>&1"
done
sleep 5

# ---- 2) produce on node0 --------------------------------------------------
echo "[dist] producing $TOTAL_LOGS logs ($BENCH_INPUT) on $COORD ..."
if [ "$BENCH_INPUT" = synthetic ]; then
  # Synthetic uses one source_id per producer -> one partition. Launch N
  # producers (SOURCE_ID=0..N-1) so each partition gets SYNTH_KEYS/N keys and
  # TOTAL_LOGS/N logs, evenly distributed across the N aggregators.
  per=$(( TOTAL_LOGS / NUM_AGGREGATORS )); kps=$(( SYNTH_KEYS / NUM_AGGREGATORS )); [ "$kps" -lt 1 ] && kps=1
  pp=()
  for ((s=0;s<NUM_AGGREGATORS;s++)); do
    env BENCH_INPUT=synthetic KAFKA_BROKERS="$KAFKA_BROKERS" KAFKA_TOPIC="$KAFKA_TOPIC" \
      NUM_AGGREGATORS="$NUM_AGGREGATORS" SOURCE_ID="$s" \
      "$LBIN/kafka-producer" --events "$per" --commit-batch-size "$COMMIT_BATCH_SIZE" --key-mod "$kps" \
      > "$LOGDIR/producer_$s.log" 2>&1 &
    pp+=($!)
  done
  for p in "${pp[@]}"; do wait "$p"; done
else
env BENCH_INPUT="$BENCH_INPUT" "${DATASET_ENV[@]}" KAFKA_BROKERS="$KAFKA_BROKERS" KAFKA_TOPIC="$KAFKA_TOPIC" \
  NUM_AGGREGATORS="$NUM_AGGREGATORS" PARALLEL_PRODUCERS="$NUM_AGGREGATORS" DISTRIBUTE_EVENLY=0 \
  "$LBIN/kafka-producer" --events "$TOTAL_LOGS" --commit-batch-size "$COMMIT_BATCH_SIZE" > "$LOGDIR/producer.log" 2>&1
fi

# Wait for Kafka drain by checking consumer LAG -> 0 across all groups. This is
# unit-correct: both LAG and current-offset are in Kafka MESSAGES (1 msg = 1
# commit-batch), so we don't mismatch event-count vs message-count (a prior
# TOTAL_LOGS-based check compared events to messages and never converged).
# Break when every group has consumed everything (lag==0) and >0 messages seen.
echo "[dist] waiting for Kafka drain (consumer lag -> 0) ..."
for ((t=0;t<300;t+=3)); do
  LAG=0; CUR=0
  for ((i=0;i<NUM_AGGREGATORS;i++)); do
    read l c < <(docker exec kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group "cg_${TAG}_$i" 2>/dev/null \
      | awk 'NR>1 && $6 ~ /^[0-9]+$/ {lag+=$6} NR>1 && $4 ~ /^[0-9]+$/ {cur+=$4} END{print lag+0, cur+0}')
    LAG=$((LAG+${l:-0})); CUR=$((CUR+${c:-0}))
  done
  [ "${CUR:-0}" -gt 0 ] && [ "${LAG:-0}" -eq 0 ] && { echo "[dist] drained: lag=0 consumed_msgs=$CUR"; break; }
  sleep 3
done
sleep 2
for ((i=0;i<NUM_AGGREGATORS;i++)); do on_node "${NODES[i]}" "pkill -TERM -f 'kafka-consumer' 2>/dev/null" >/dev/null 2>&1; done
sleep 4

# ---- 3) aggregators: launch DETACHED per node, poll a DONE marker, then
# collect. Detached+poll avoids holding a 90-min idle SSH channel (which drops
# and makes a foreground `wait` return early -> empty collection). Logs use
# RUNID-unique paths so no other cell can overwrite them.
echo "[dist] running $NUM_AGGREGATORS aggregators in parallel (mode=$MODE) ..."
ALOG="/tmp/${RUNID}_agg"; AMEM="/tmp/${RUNID}_mem"
AGG_START=$(date +%s.%N)
for ((i=0;i<NUM_AGGREGATORS;i++)); do
  spawn_node "${NODES[i]}" "rm -f ${ALOG}_$i.DONE; \
    nohup python3 $RBIN/mem_trace.py --out ${AMEM}_$i.csv --summary ${AMEM}_$i.json --match aggregator --match r0vm --interval $MEM_INTERVAL > /dev/null 2>&1 & \
    env $REMOTE_ENV E2E_TIMING=1 RISC0_DEV_MODE=$RISC0_DEV_MODE NO_ZKVM_PROOF=$NO_ZKVM_PROOF RAW_ROCKSDB_PATH=${RAWP}_$i AGG_ROCKSDB_PATH=${AGGP}_$i AGGREGATOR_ID=$i FDB_CLUSTER_FILE=$FDB_CLUSTER_FILE FDB_SUBSPACE=$FDB_SUBSPACE AGGR_IDLE_TIMEOUT_SECS=$AGGR_IDLE_TIMEOUT_SECS AGGR_PIPELINE=rocksdb RAYON_NUM_THREADS=56 \
    /usr/bin/time -v $RBIN/aggregator --rocksdb --mode $AGG_MODE --threads 56 > ${ALOG}_$i.log 2> ${ALOG}_${i}_time.log; \
    pkill -INT -f mem_trace.py 2>/dev/null; sleep 3; touch ${ALOG}_$i.DONE"
done
# Poll each node for its DONE marker (cheap test -f over SSH; no long-held channel).
AGG_MAX_WAIT="${AGG_MAX_WAIT:-20000}"
for ((i=0;i<NUM_AGGREGATORS;i++)); do
  for ((t=0;t<AGG_MAX_WAIT;t+=10)); do
    on_node "${NODES[i]}" "test -f ${ALOG}_$i.DONE" 2>/dev/null && break
    sleep 10
  done
done
AGG_END=$(date +%s.%N)
echo "[dist] all aggregators done; collecting ..."

# Robust collect: retry the stdout log until non-empty.
for ((i=0;i<NUM_AGGREGATORS;i++)); do
  for try in 1 2 3 4 5; do
    on_node "${NODES[i]}" "cat ${ALOG}_$i.log" > "$LOGDIR/agg_$i.log" 2>/dev/null
    [ -s "$LOGDIR/agg_$i.log" ] && break; sleep 2
  done
  on_node "${NODES[i]}" "cat ${ALOG}_${i}_time.log" > "$LOGDIR/agg_${i}_time.log" 2>/dev/null
  on_node "${NODES[i]}" "cat ${AMEM}_$i.json" > "$LOGDIR/mem_$i.json" 2>/dev/null
  on_node "${NODES[i]}" "cat /tmp/dist_consumer_$i.log" > "$LOGDIR/consumer_$i.log" 2>/dev/null
done

# ---- 4) querier on node0 --------------------------------------------------
echo "[dist] querier on $COORD ..."
nohup python3 "$ROOT_DIR/scripts/lib/mem_trace.py" --out "$WORK/mem_query.csv" --summary "$WORK/mem_query.json" \
  --match querier --match r0vm --interval "$MEM_INTERVAL" > "$LOGDIR/mempoll_q.log" 2>&1 &
QMEM=$!
/usr/bin/time -v env E2E_TIMING=1 RISC0_DEV_MODE=$RISC0_DEV_MODE BENCH_PRINT=1 QUERY_NO_PROVE="$QUERY_NO_PROVE" \
  FDB_CLUSTER_FILE="$FDB_CLUSTER_FILE" FDB_SUBSPACE="$FDB_SUBSPACE" HTTP_LISTEN="0.0.0.0:$QUERIER_PORT" \
  "$LBIN/querier" > "$LOGDIR/querier.log" 2>&1 &
QPID=$!
for _ in $(seq 1 60); do curl -s -o /dev/null -m 2 "localhost:$QUERIER_PORT/query" -H 'content-type: application/json' -d "$QUERY_JSON" && break; sleep 1; done
for _ in 1 2 3; do curl -s -m 1200 "localhost:$QUERIER_PORT/query" -H 'content-type: application/json' -d "$QUERY_JSON" >> "$LOGDIR/query_response.json" 2>/dev/null || true; done
sleep 1; kill -INT "$QMEM" 2>/dev/null; wait "$QMEM" 2>/dev/null; kill -TERM "$QPID" 2>/dev/null; wait "$QPID" 2>/dev/null

# ---- 5) parse -> metrics --------------------------------------------------
AGG_WALL=$(python3 -c "print(f'{$AGG_END-$AGG_START:.3f}')")
python3 "$ROOT_DIR/scripts/lib/_parse_dist_cell.py" --dataset "$DATASET" --agg-type "$AGG_TYPE" --mode "$MODE" \
  --epoch-size "$EPOCH_LOGS" --num-aggregators "$NUM_AGGREGATORS" --workdir "$WORK" --logdir "$LOGDIR" \
  --agg-wall "$AGG_WALL" --metrics "$METRICS"
echo "[dist] done: $TAG"
