#!/usr/bin/env python3
"""Build the distributed Figure-4 main table from results/_dist_metrics.jsonl.

Pairs native vs zk per (dataset, task) and emits the corrected memory columns
(host data-path vs r0vm prover, summed across the real distributed nodes).
"""
from __future__ import annotations
import argparse, csv, json, os
from collections import defaultdict


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics", default="results/_dist_metrics.jsonl")
    ap.add_argument("--outdir", default="results")
    a = ap.parse_args()
    rows = [json.loads(l) for l in open(a.metrics) if l.strip()]
    idx = defaultdict(dict)  # (dataset,agg_type,task) -> {mode: rec}
    for r in rows:
        idx[(r["dataset"], r["aggregation_type"], r["task"])][r["mode"]] = r
    os.makedirs(a.outdir, exist_ok=True)

    fields = ["task", "aggregation_type", "dataset", "num_aggregators",
              "native_time_s", "native_peak_mb", "zk_time_s", "zk_peak_mb",
              "time_slowdown", "memory_blowup",
              "zk_host_peak_mb", "zk_prover_peak_mb",
              "zk_per_node_total_mb", "zk_per_node_host_mb", "zk_per_node_prover_mb",
              "zk_proof_kb_per_epoch", "zk_total_proof_kb", "zk_public_output_kb_per_epoch"]
    out = os.path.join(a.outdir, "dist_main_table.csv")
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for (ds, agg, task), m in sorted(idx.items()):
            n = m.get("native", {})
            z = m.get("zk", {})
            nt, zt = n.get("total_time_s"), z.get("total_time_s")
            nm, zm = n.get("peak_rss_mb"), z.get("peak_rss_mb")
            w.writerow({
                "task": task, "aggregation_type": agg, "dataset": ds,
                "num_aggregators": z.get("num_aggregators", n.get("num_aggregators", "")),
                "native_time_s": "" if nt is None else f"{nt:.4f}",
                "native_peak_mb": "" if nm is None else f"{nm:.1f}",
                "zk_time_s": "" if zt is None else f"{zt:.2f}",
                "zk_peak_mb": "" if zm is None else f"{zm:.1f}",
                "time_slowdown": f"{zt/nt:.0f}" if (nt and zt and nt > 0) else "",
                "memory_blowup": f"{zm/nm:.1f}" if (nm and zm and nm > 0) else "",
                "zk_host_peak_mb": f"{z.get('host_peak_rss_mb',0):.1f}",
                "zk_prover_peak_mb": f"{z.get('prover_peak_rss_mb',0):.1f}",
                "zk_per_node_total_mb": f"{z.get('per_node_total_rss_mb',0):.1f}",
                "zk_per_node_host_mb": f"{z.get('per_node_host_rss_mb',0):.1f}",
                "zk_per_node_prover_mb": f"{z.get('per_node_prover_rss_mb',0):.1f}",
                "zk_proof_kb_per_epoch": f"{z.get('proof_bytes_per_epoch',0)/1024:.1f}",
                "zk_total_proof_kb": f"{z.get('total_proof_bytes',0)/1024:.1f}",
                "zk_public_output_kb_per_epoch": f"{z.get('journal_bytes_per_epoch',0)/1024:.2f}",
            })
    print(f"[build-dist] wrote {out} from {len(rows)} records")


if __name__ == "__main__":
    main()
