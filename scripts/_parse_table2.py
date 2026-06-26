#!/usr/bin/env python3
"""Parse one Table 2 cell (mode x N aggregators) -> one JSONL record.

Per node the bench prints proof_bytes_max / journal_bytes_last / proc_hwm_kb.
Memory split: host = max aggregator-process RSS, prover = max r0vm RSS (from the
mem_trace summary); per-node total = host + prover. /usr/bin/time Max RSS is a
fallback (it captures the r0vm child). Nodes are symmetric -> report the max
(representative) per-node values + the cluster total proof (8 epochs x per-epoch).
"""
from __future__ import annotations
import argparse, glob, json, os, re

PROOF_RE = re.compile(r"proof_bytes=(\d+)\s+journal_bytes=(\d+)")

def proof_journal(path):
    """proof_bytes, journal_bytes from the rocksdb-pipeline stderr line:
    [risc0-aggr][mode] seq=.. proof_bytes=.. journal_bytes=.. (last epoch)."""
    pb=jb=0
    try:
        for line in open(path, errors="replace"):
            m=PROOF_RE.search(line)
            if m: pb=int(m.group(1)); jb=int(m.group(2))
    except OSError: pass
    return pb, jb

def maxrss_kb(path):
    try:
        for line in open(path, errors="replace"):
            m=re.search(r"Maximum resident set size \(kbytes\):\s*(\d+)", line)
            if m: return int(m.group(1))
    except OSError: pass
    return None

def mem_split(path):
    try: d=json.load(open(path))
    except (OSError,ValueError): return 0.0,0.0
    host=prov=0.0
    for _p,v in (d.get("per_process_peak_rss_mb") or {}).items():
        nm=(v.get("name") or "").lower(); val=v.get("peak_rss_mb",0.0)
        if "r0vm" in nm: prov=max(prov,val)
        else: host=max(host,val)
    return host,prov

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--mode",required=True); ap.add_argument("--num-aggregators",type=int,required=True)
    ap.add_argument("--logdir",required=True); ap.add_argument("--jsonl",required=True)
    a=ap.parse_args()
    proof=jour=0; hosts=[]; provers=[]; totals=[]; n=0
    for p in sorted(glob.glob(os.path.join(a.logdir,"agg_*.log"))):
        m=re.search(r"agg_(\d+)\.log$",p)
        if not m or p.endswith("_time.log"): continue
        i=m.group(1); n+=1
        pb,jb=proof_journal(os.path.join(a.logdir,f"agg_{i}_time.log"))
        proof=max(proof,pb); jour=max(jour,jb)
        h,pr=mem_split(os.path.join(a.logdir,f"mem_{i}.json"))
        tv=maxrss_kb(os.path.join(a.logdir,f"agg_{i}_time.log"))
        tv_mb=(tv/1024.0) if tv else 0.0
        # prefer mem_trace split; if it missed the prover, use /usr/bin/time total
        if pr==0 and tv_mb>h: pr=tv_mb-h
        hosts.append(h); provers.append(pr); totals.append(h+pr)
    agg_type={"samples":"hash_table","histogram":"histogram","cm":"cms"}.get(a.mode,a.mode)
    rec=dict(mode=a.mode, aggregation_type=agg_type, num_aggregators=a.num_aggregators,
             nodes_measured=n,
             proof_size_per_epoch_kb=round(proof/1024.0,1),
             total_proof_size_kb=round(8*proof/1024.0,1),   # 8 epochs total in the cluster
             public_output_kb=round(jour/1024.0,2),
             per_node_host_mb=round(max(hosts,default=0),1),
             per_node_prover_mb=round(max(provers,default=0),1),
             per_node_peak_mb=round(max(totals,default=0),1))
    open(a.jsonl,"a").write(json.dumps(rec)+"\n")
    print(f"[t2-parse] {a.mode}/{a.num_aggregators}: proof/epoch={rec['proof_size_per_epoch_kb']}KB "
          f"pub={rec['public_output_kb']}KB peak/node={rec['per_node_peak_mb']}MB "
          f"(host {rec['per_node_host_mb']}+prover {rec['per_node_prover_mb']})")

if __name__=="__main__": main()
