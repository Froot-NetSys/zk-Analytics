#!/usr/bin/env python3
"""Parse one e2e baseline cell's logs into a metrics JSONL record.

Reads the aggregator/consumer/querier logs and mem_trace summaries produced by
scripts/run_baseline_e2e.sh and appends two JSON objects (aggregation + query)
to the metrics file consumed by scripts/build_baseline_tables.py.

Timing model:
  - Aggregation is distributed across NUM_AGGREGATORS workers that run in
    parallel; we report the *critical path* (busiest worker) for the offline
    processing components and the busiest consumer for the online ingest
    components, then total = sum of all components. peak_rss is summed across
    all aggregator worker processes (cluster total).
  - Query runs on a single engine; total = db + deserialize + compute + verify;
    peak_rss is that single process.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re

# [e2e-timing] seq=.. mode=.. rocksdb_raw_read_ms=.. aggr_compute_ms=.. prove_ms=..
# verify_ms=.. rocksdb_agg_write_ms=.. fdb_write_ms=.. epoch_total_ms=..
AGG_RE = re.compile(r"\[e2e-timing\]\s+(.*)$")
KV_RE = re.compile(r"(\w+)=([0-9.]+)")
CONS_RE = re.compile(r"\[e2e-timing\]\[kafka-consumer\]\s+(.*)$")
# querier: bench query=.. ... db_ms=.. merge_ms=..   /   bench kind=.. prove_ms=.. verify_ms=..
BENCHQ_RE = re.compile(r"^bench query=.*db_ms=(\d+)\s+merge_ms=(\d+)")
BENCHP_RE = re.compile(r"^bench kind=\S+.*prove_ms=(\d+)\s+verify_ms=(\d+)")


def kv(line):
    return {k: float(v) for k, v in KV_RE.findall(line)}


def parse_agg_logs(logdir):
    """Return (critical_components_s, num_epochs_total). Critical = busiest worker."""
    workers = []
    for path in sorted(glob.glob(os.path.join(logdir, "agg_*.log"))):
        comp = {"rocksdb_raw_read": 0.0, "aggr_compute": 0.0, "prove": 0.0,
                "verify": 0.0, "rocksdb_agg_write": 0.0, "fdb_write": 0.0}
        epoch_total = 0.0
        n = 0
        with open(path, errors="replace") as f:
            for line in f:
                m = AGG_RE.search(line)
                if not m:
                    continue
                d = kv(m.group(1))
                comp["rocksdb_raw_read"] += d.get("rocksdb_raw_read_ms", 0) / 1000.0
                comp["aggr_compute"] += d.get("aggr_compute_ms", 0) / 1000.0
                comp["prove"] += d.get("prove_ms", 0) / 1000.0
                comp["verify"] += d.get("verify_ms", 0) / 1000.0
                comp["rocksdb_agg_write"] += d.get("rocksdb_agg_write_ms", 0) / 1000.0
                comp["fdb_write"] += d.get("fdb_write_ms", 0) / 1000.0
                epoch_total += d.get("epoch_total_ms", 0) / 1000.0
                n += 1
        workers.append((epoch_total, comp, n))
    if not workers:
        return ({k: 0.0 for k in
                 ["rocksdb_raw_read", "aggr_compute", "prove", "verify",
                  "rocksdb_agg_write", "fdb_write"]}, 0)
    workers.sort(key=lambda w: w[0], reverse=True)  # busiest first
    total_epochs = sum(w[2] for w in workers)
    return workers[0][1], total_epochs


def parse_consumer_logs(logdir):
    """Return busiest-consumer ingest components (seconds)."""
    best = {"kafka_recv": 0.0, "rocksdb_raw_insert": 0.0}
    best_sum = -1.0
    for path in sorted(glob.glob(os.path.join(logdir, "consumer_*.log"))):
        with open(path, errors="replace") as f:
            for line in f:
                m = CONS_RE.search(line)
                if not m:
                    continue
                d = kv(m.group(1))
                kr = d.get("kafka_recv_ms", 0) / 1000.0
                ri = d.get("rocksdb_raw_insert_ms", 0) / 1000.0
                if kr + ri > best_sum:
                    best_sum = kr + ri
                    best = {"kafka_recv": kr, "rocksdb_raw_insert": ri}
    return best


def parse_querier_log(logdir):
    db_ms = merge_ms = prove_ms = verify_ms = None
    path = os.path.join(logdir, "querier.log")
    if os.path.exists(path):
        with open(path, errors="replace") as f:
            for line in f:
                m = BENCHQ_RE.match(line)
                if m:
                    db_ms, merge_ms = int(m.group(1)), int(m.group(2))
                m = BENCHP_RE.match(line)
                if m:
                    prove_ms, verify_ms = int(m.group(1)), int(m.group(2))
    return db_ms, merge_ms, prove_ms, verify_ms


def load_summary(path, key):
    try:
        with open(path) as f:
            return json.load(f).get(key)
    except (OSError, ValueError):
        return None


# /usr/bin/time -v line: "Maximum resident set size (kbytes): 9123456"
MAXRSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")


def max_rss_mb(path):
    """Peak RSS (MB) from a /usr/bin/time -v report, or None."""
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m = MAXRSS_RE.search(line)
                if m:
                    return int(m.group(1)) / 1024.0
    except OSError:
        return None
    return None


def summed_agg_max_rss_mb(logdir):
    """Sum of each aggregator worker's authoritative peak RSS (= cluster total
    across distributed workers). Concurrency-independent: each worker's Max RSS
    is its own peak whether workers ran concurrently or serialized."""
    total = 0.0
    found = False
    for path in sorted(glob.glob(os.path.join(logdir, "agg_*_time.log"))):
        v = max_rss_mb(path)
        if v is not None:
            total += v
            found = True
    return total if found else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True)
    ap.add_argument("--agg-type", required=True)
    ap.add_argument("--mode", required=True)
    ap.add_argument("--epoch-size", type=int, required=True)
    ap.add_argument("--num-aggregators", type=int, required=True)
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--logdir", required=True)
    ap.add_argument("--agg-wall", type=float, default=0.0)
    ap.add_argument("--metrics", required=True)
    args = ap.parse_args()

    base = dict(dataset=args.dataset, aggregation_type=args.agg_type,
                mode=args.mode, epoch_size=args.epoch_size,
                num_aggregators=args.num_aggregators)

    # --- Aggregation record ---
    agg_comp, n_epochs = parse_agg_logs(args.logdir)
    ingest = parse_consumer_logs(args.logdir)
    agg_components = {**ingest, **agg_comp}
    agg_total = sum(agg_components.values())
    # Peak memory across all aggregator workers + their r0vm provers (cluster
    # total). Take the MAX of two sources: (a) summed /usr/bin/time Max RSS of
    # the host processes — reliable for fast native runs the 0.5s poller may
    # miss, but blind to the r0vm subprocess; (b) the poller's summed peak,
    # which (matching r0vm) captures the dominant proving working set. For zk,
    # (b) wins; for native, (a) wins.
    agg_timev = summed_agg_max_rss_mb(args.logdir) or 0.0
    agg_poll = load_summary(os.path.join(args.workdir, "mem_agg_summary.json"),
                            "summed_peak_rss_mb") or 0.0
    agg_peak = max(agg_timev, agg_poll)
    agg_rec = {**base, "task": "aggregation",
               "total_time_s": round(agg_total, 6),
               "peak_rss_mb": round(agg_peak, 2),
               "components_s": {k: round(v, 6) for k, v in agg_components.items()},
               "agg_wall_clock_s": round(args.agg_wall, 3),
               "epochs_processed": n_epochs,
               "memory_model": "summed_across_workers"}

    # --- Query record ---
    db_ms, merge_ms, prove_ms, verify_ms = parse_querier_log(args.logdir)
    db_s = (db_ms or 0) / 1000.0
    des_s = (merge_ms or 0) / 1000.0
    comp_s = (prove_ms or 0) / 1000.0
    ver_s = (verify_ms or 0) / 1000.0
    q_total = db_s + des_s + comp_s + ver_s
    # Query engine + its r0vm prover. /usr/bin/time sees only the engine; the
    # poller (matching r0vm) captures the prover. Take the max.
    q_timev = max_rss_mb(os.path.join(args.logdir, "querier.log")) or 0.0
    q_poll = load_summary(os.path.join(args.workdir, "mem_query_summary.json"),
                          "summed_peak_rss_mb") or 0.0
    q_peak = max(q_timev, q_poll)
    q_rec = {**base, "task": "query",
             "total_time_s": round(q_total, 6),
             "peak_rss_mb": round(q_peak, 2),
             "components_s": {
                 "fdb_lookup": round(db_s, 6),
                 "deserialize": round(des_s, 6),
                 "query_compute": round(comp_s, 6),
                 "verify": round(ver_s, 6)},
             "memory_model": "single_process"}

    with open(args.metrics, "a") as f:
        f.write(json.dumps(agg_rec) + "\n")
        f.write(json.dumps(q_rec) + "\n")
    print(f"[parse] {args.dataset}/{args.mode}/e{args.epoch_size}: "
          f"agg_total={agg_total:.4f}s ({n_epochs} epochs, peak {agg_peak:.1f}MB) | "
          f"query_total={q_total:.4f}s (peak {q_peak:.1f}MB)")


if __name__ == "__main__":
    main()
