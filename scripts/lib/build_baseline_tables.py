#!/usr/bin/env python3
"""Build the camera-ready non-ZK vs zk-Analytics baseline tables.

Consumes a single intermediate file produced by scripts/eval/run_baseline_e2e.sh:

    results/_e2e_metrics.jsonl   (one JSON object per measured run)

Each line is one (dataset, aggregation_type, epoch_size, mode, task) measurement:

    {
      "dataset": "google" | "caida" | "vehicle",
      "aggregation_type": "hash_table" | "histogram" | "cms",
      "task": "aggregation" | "query",
      "mode": "native" | "zk",
      "epoch_size": 16384,
      "num_aggregators": 8,
      "total_time_s": 5448.0,            # end-to-end wall clock for this task
      "peak_rss_mb": 9092.0,            # summed across workers (agg) or single (query)
      "components_s": {                  # per-component seconds (end-to-end breakdown)
          # aggregation: kafka_recv, rocksdb_raw_insert, rocksdb_raw_read,
          #              aggr_compute, prove, verify, rocksdb_agg_write, fdb_write
          # query:       fdb_lookup, deserialize, query_compute, verify
          "aggr_compute": 0.031, "prove": 5448.0, ...
      },
      "memory_model": "summed_across_workers" | "single_process"
    }

Outputs (per the camera-ready spec):
  results/baseline_main_table.csv          (main epoch size, default 16384)
  results/baseline_epoch_sensitivity.csv   (all epoch sizes)
  results/aggregation_breakdown.csv        (per-component, native vs zk)
  results/query_breakdown.csv              (per-component, native vs zk)
  results/baseline_e2e_summary.md          (interpretation + memory model notes)
"""
from __future__ import annotations

import argparse
import csv
import json
import os
from collections import defaultdict

AGG_COMPONENTS = [
    "kafka_recv", "rocksdb_raw_insert", "rocksdb_raw_read",
    "aggr_compute", "prove", "verify", "rocksdb_agg_write", "fdb_write",
]
QUERY_COMPONENTS = ["fdb_lookup", "deserialize", "query_compute", "verify"]

AGG_TYPE_LABEL = {"hash_table": "Hash Table", "histogram": "Histogram", "cms": "Count-Min Sketch"}


def load(path):
    rows = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def index(rows):
    """key -> {native: row, zk: row}; key = (dataset, agg_type, task, epoch_size)."""
    out = defaultdict(dict)
    for r in rows:
        key = (r["dataset"], r["aggregation_type"], r["task"], int(r["epoch_size"]))
        out[key][r["mode"]] = r
    return out


def ratio(zk, native):
    if native and native > 0:
        return round(zk / native, 2)
    return ""


def write_main_like(path, idx, epoch_filter):
    """epoch_filter(epoch_size, dataset) -> bool selects which rows to emit."""
    fields = [
        "task", "aggregation_type", "dataset", "epoch_size", "num_aggregators",
        "native_time_s", "native_memory_mb", "zk_time_s", "zk_memory_mb",
        "time_slowdown", "memory_blowup", "native_peak_rss_mb", "zk_peak_rss_mb",
    ]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (dataset, agg, task, epoch), modes in sorted(idx.items()):
            if not epoch_filter(epoch, dataset):
                continue
            nat = modes.get("native", {})
            zk = modes.get("zk", {})
            nt = nat.get("total_time_s")
            zt = zk.get("total_time_s")
            nm = nat.get("peak_rss_mb")
            zm = zk.get("peak_rss_mb")
            w.writerow({
                "task": task,
                "aggregation_type": agg,
                "dataset": dataset,
                "epoch_size": epoch,
                "num_aggregators": nat.get("num_aggregators", zk.get("num_aggregators", "")),
                "native_time_s": "" if nt is None else f"{nt:.6f}",
                "native_memory_mb": "" if nm is None else f"{nm:.1f}",
                "zk_time_s": "" if zt is None else f"{zt:.3f}",
                "zk_memory_mb": "" if zm is None else f"{zm:.1f}",
                "time_slowdown": ratio(zt, nt) if (nt and zt) else "",
                "memory_blowup": ratio(zm, nm) if (nm and zm) else "",
                "native_peak_rss_mb": "" if nm is None else f"{nm:.1f}",
                "zk_peak_rss_mb": "" if zm is None else f"{zm:.1f}",
            })


def write_breakdown(path, idx, task, components):
    fields = ["dataset", "mode", "epoch_size", "component", "native_time_s", "zk_time_s"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (dataset, agg, t, epoch), modes in sorted(idx.items()):
            if t != task:
                continue
            nat = modes.get("native", {}).get("components_s", {}) or {}
            zk = modes.get("zk", {}).get("components_s", {}) or {}
            for comp in components:
                nv = nat.get(comp)
                zv = zk.get(comp)
                if nv is None and zv is None:
                    continue
                w.writerow({
                    "dataset": dataset,
                    "mode": agg,  # aggregation_type, e.g. hash_table/histogram/cms
                    "epoch_size": epoch,
                    "component": comp,
                    "native_time_s": "" if nv is None else f"{nv:.6f}",
                    "zk_time_s": "" if zv is None else f"{zv:.6f}",
                })
            # total row
            nt = modes.get("native", {}).get("total_time_s")
            zt = modes.get("zk", {}).get("total_time_s")
            w.writerow({
                "dataset": dataset, "mode": agg, "epoch_size": epoch, "component": "total",
                "native_time_s": "" if nt is None else f"{nt:.6f}",
                "zk_time_s": "" if zt is None else f"{zt:.6f}",
            })


def write_summary(path, idx, main_epoch):
    lines = []
    lines.append("# Non-ZK native vs zk-Analytics e2e baseline\n")
    lines.append("Generated by `scripts/lib/build_baseline_tables.py` from "
                 "`results/_e2e_metrics.jsonl` (measured end-to-end through the "
                 "real Kafka -> RocksDB -> aggregator -> FDB -> querier pipeline).\n")
    lines.append("## Memory model\n")
    lines.append("- **Aggregation rows**: `peak_rss_mb` is the **summed peak RSS "
                 "across all aggregator worker processes** (distributed cluster total).\n")
    lines.append("- **Query rows**: `peak_rss_mb` is the **peak RSS of the single "
                 "query-engine process**.\n")
    lines.append("- Peaks are VmHWM / max RSS from `/proc` polling + `/usr/bin/time -v`.\n")
    lines.append(f"\n## Main table (epoch = {main_epoch} logs; vehicle at its natural size)\n")
    for (dataset, agg, task, epoch), modes in sorted(idx.items()):
        if epoch != main_epoch and dataset != "vehicle":
            continue
        nat = modes.get("native", {})
        zk = modes.get("zk", {})
        nt, zt = nat.get("total_time_s"), zk.get("total_time_s")
        if nt and zt:
            lines.append(f"- {task}/{agg} ({dataset}): native {nt:.4f}s vs zk {zt:.1f}s "
                         f"=> {ratio(zt, nt)}x slower; "
                         f"mem {nat.get('peak_rss_mb','?')}MB -> {zk.get('peak_rss_mb','?')}MB.\n")
    lines.append("\n## Interpretation\n")
    lines.append("Total zk overhead is expected to be dominated by zkVM proving; "
                 "RocksDB/FoundationDB insert+lookup are reported per-component in "
                 "`aggregation_breakdown.csv` / `query_breakdown.csv` so storage "
                 "overhead can be compared directly against proving.\n")
    with open(path, "w") as f:
        f.writelines(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", default="results/_e2e_metrics.jsonl")
    ap.add_argument("--outdir", default="results")
    ap.add_argument("--main-epoch", type=int, default=16384)
    args = ap.parse_args()

    rows = load(args.metrics)
    idx = index(rows)
    os.makedirs(args.outdir, exist_ok=True)

    # Main table: main epoch for datasets with >=1 full epoch; vehicle always.
    write_main_like(
        os.path.join(args.outdir, "baseline_main_table.csv"), idx,
        lambda epoch, dataset: epoch == args.main_epoch or dataset == "vehicle",
    )
    # Epoch sensitivity: everything.
    write_main_like(
        os.path.join(args.outdir, "baseline_epoch_sensitivity.csv"), idx,
        lambda epoch, dataset: True,
    )
    write_breakdown(os.path.join(args.outdir, "aggregation_breakdown.csv"),
                    idx, "aggregation", AGG_COMPONENTS)
    write_breakdown(os.path.join(args.outdir, "query_breakdown.csv"),
                    idx, "query", QUERY_COMPONENTS)
    write_summary(os.path.join(args.outdir, "baseline_e2e_summary.md"),
                  idx, args.main_epoch)
    print(f"[build] wrote tables to {args.outdir}/ from {len(rows)} measurements")


if __name__ == "__main__":
    main()
