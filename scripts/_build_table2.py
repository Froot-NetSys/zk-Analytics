#!/usr/bin/env python3
"""Build the corrected Table 2 CSV from the sweep's JSONL records."""
from __future__ import annotations
import argparse, csv, json

ap=argparse.ArgumentParser()
ap.add_argument("--jsonl",required=True); ap.add_argument("--out",required=True)
a=ap.parse_args()
rows=[json.loads(l) for l in open(a.jsonl) if l.strip()]
order={"samples":0,"histogram":1,"cm":2}
rows.sort(key=lambda r:(order.get(r["mode"],9), r["num_aggregators"]))
fields=["aggregation_type","mode","num_aggregators",
        "proof_size_per_epoch_kb","total_proof_size_kb","public_output_kb",
        "per_node_peak_mb","per_node_host_mb","per_node_prover_mb"]
with open(a.out,"w",newline="") as f:
    w=csv.DictWriter(f,fieldnames=fields); w.writeheader()
    for r in rows: w.writerow({k:r.get(k,"") for k in fields})
print(f"[t2-build] wrote {a.out} from {len(rows)} records")
