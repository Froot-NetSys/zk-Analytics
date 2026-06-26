#!/usr/bin/env python3
"""Parse a distributed baseline cell into a metrics JSONL record.

Reads per-node aggregator logs + per-node mem_trace summaries (which match the
aggregator host AND its r0vm prover) and emits one aggregation + one query
record. Distributed semantics:
  - aggregators run in PARALLEL on separate nodes -> total_time_s = the busiest
    node's summed component time (critical path / max-aggregator, = paper).
  - peak memory = SUM over nodes of (host + prover) = cluster total; also split
    host_peak_total vs prover_peak_total for the corrected Table 2.
"""
from __future__ import annotations
import argparse, glob, json, os, re

AGG_RE = re.compile(r"\[e2e-timing\]\s+(.*)$")
KV_RE = re.compile(r"(\w+)=([0-9.]+)")
CONS_RE = re.compile(r"\[e2e-timing\]\[kafka-consumer\]\s+(.*)$")
BENCHQ_RE = re.compile(r"^bench query=.*db_ms=(\d+)\s+merge_ms=(\d+)")
BENCHP_RE = re.compile(r"^bench kind=\S+.*prove_ms=(\d+)\s+verify_ms=(\d+)")
MAXRSS_RE = re.compile(r"Maximum resident set size \(kbytes\):\s*(\d+)")

def kv(line): return {k: float(v) for k, v in KV_RE.findall(line)}

def node_components(path):
    comp = {"rocksdb_raw_read":0.0,"aggr_compute":0.0,"prove":0.0,"verify":0.0,
            "rocksdb_agg_write":0.0,"fdb_write":0.0}
    total=0.0; n=0
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m=AGG_RE.search(line)
                if not m: continue
                d=kv(m.group(1))
                comp["rocksdb_raw_read"]+=d.get("rocksdb_raw_read_ms",0)/1000
                comp["aggr_compute"]+=d.get("aggr_compute_ms",0)/1000
                comp["prove"]+=d.get("prove_ms",0)/1000
                comp["verify"]+=d.get("verify_ms",0)/1000
                comp["rocksdb_agg_write"]+=d.get("rocksdb_agg_write_ms",0)/1000
                comp["fdb_write"]+=d.get("fdb_write_ms",0)/1000
                total+=d.get("epoch_total_ms",0)/1000; n+=1
    except OSError: pass
    return comp, total, n

CONS_RE = re.compile(r"\[e2e-timing\]\[kafka-consumer\]\s+(.*)$")
def consumer_insert(logdir):
    """Busiest consumer's kafka_recv + rocksdb_raw_insert (seconds)."""
    best={"kafka_recv":0.0,"rocksdb_raw_insert":0.0}; best_sum=-1.0
    for path in glob.glob(os.path.join(logdir,"consumer_*.log")):
        try:
            for line in open(path, errors="replace"):
                m=CONS_RE.search(line)
                if not m: continue
                d={k:float(v) for k,v in re.findall(r"(\w+)=([0-9.]+)", m.group(1))}
                kr=d.get("kafka_recv_ms",0)/1000; ri=d.get("rocksdb_raw_insert_ms",0)/1000
                if kr+ri>best_sum: best_sum=kr+ri; best={"kafka_recv":kr,"rocksdb_raw_insert":ri}
        except OSError: pass
    return best

PROOF_RE = re.compile(r"proof_bytes=(\d+)\s+journal_bytes=(\d+)")

def node_proof(path):
    """Return (proof_bytes_per_epoch, journal_bytes_per_epoch, n_epochs) from the
    aggregator stderr log ([risc0-aggr][mode] seq=.. proof_bytes=.. journal_bytes=..)."""
    pb=jb=0; n=0
    try:
        for line in open(path, errors="replace"):
            m=PROOF_RE.search(line)
            if m:
                pb=int(m.group(1)); jb=int(m.group(2)); n+=1
    except OSError: pass
    return pb, jb, n

def node_mem(path):
    """Return (host_peak_mb, prover_peak_mb, node_total_mb) from a mem_trace summary."""
    try:
        with open(path) as f: s=json.load(f)
    except (OSError, ValueError): return 0.0,0.0,0.0
    host=prover=0.0
    for _pid, p in (s.get("per_process_peak_rss_mb") or {}).items():
        nm=(p.get("name") or "").lower(); v=p.get("peak_rss_mb",0.0)
        if "r0vm" in nm: prover=max(prover, v)
        else: host=max(host, v)
    total=s.get("summed_peak_rss_mb") or (host+prover)
    return host, prover, total

def max_rss_mb(path):
    try:
        with open(path, errors="replace") as f:
            for line in f:
                m=MAXRSS_RE.search(line)
                if m: return int(m.group(1))/1024.0
    except OSError: return None
    return None

def parse_querier(logdir):
    db=mg=pr=vf=None
    p=os.path.join(logdir,"querier.log")
    if os.path.exists(p):
        with open(p,errors="replace") as f:
            for line in f:
                m=BENCHQ_RE.match(line)
                if m: db,mg=int(m.group(1)),int(m.group(2))
                m=BENCHP_RE.match(line)
                if m: pr,vf=int(m.group(1)),int(m.group(2))
    return db,mg,pr,vf

def main():
    ap=argparse.ArgumentParser()
    for a in ["dataset","agg-type","mode","workdir","logdir","metrics"]: ap.add_argument("--"+a, required=True)
    ap.add_argument("--epoch-size", type=int, required=True)
    ap.add_argument("--num-aggregators", type=int, required=True)
    ap.add_argument("--agg-wall", type=float, default=0.0)
    args=ap.parse_args()
    base=dict(dataset=args.dataset, aggregation_type=args.agg_type, mode=args.mode,
              epoch_size=args.epoch_size, num_aggregators=args.num_aggregators)

    # per-node aggregation components + memory
    nodes=[]
    for path in sorted(glob.glob(os.path.join(args.logdir,"agg_*.log"))):
        m=re.search(r"agg_(\d+)\.log$", path)
        if not m or path.endswith("_time.log"):
            continue
        i=m.group(1)
        comp,total,n=node_components(path)
        host,prover,ntot=node_mem(os.path.join(args.logdir, f"mem_{i}.json"))
        if (host+prover)==0:  # fallback: /usr/bin/time max rss (captures prover)
            tv=max_rss_mb(os.path.join(args.logdir, f"agg_{i}_time.log"))
            if tv: ntot=tv; prover=tv
        pb,jb,pn=node_proof(os.path.join(args.logdir, f"agg_{i}_time.log"))
        nodes.append(dict(i=i, comp=comp, total=total, epochs=n,
                          host=host, prover=prover, node_total=ntot,
                          proof_bytes=pb, journal_bytes=jb, proof_epochs=pn))
    if not nodes:
        print("[parse-dist] no aggregator logs"); return
    # Native has no real prover; any r0vm the poller caught is a dying leftover
    # from a prior cell -> force prover=0 and use the clean host RSS.
    if args.mode == "native":
        for x in nodes:
            x["prover"] = 0.0
            x["node_total"] = x["host"]
    crit=max(nodes, key=lambda x:x["total"])   # busiest node = critical path
    host_sum=sum(x["host"] for x in nodes)
    prover_sum=sum(x["prover"] for x in nodes)
    # Clean cluster total = sum(host) + sum(prover) using per-node max values.
    # (Do NOT sum each node's summed_peak: that double-counts stray dying r0vm
    # from a prior cell that the poller may have caught on some nodes.)
    cluster_mem=host_sum+prover_sum
    for x in nodes:
        x["node_total"]=x["host"]+x["prover"]
    agg_rec={**base, "task":"aggregation",
        "total_time_s": round(crit["total"],6),
        "peak_rss_mb": round(cluster_mem,2),
        "host_peak_rss_mb": round(host_sum,2),
        "prover_peak_rss_mb": round(prover_sum,2),
        "per_node_total_rss_mb": round(crit["node_total"],2),
        "per_node_host_rss_mb": round(crit["host"],2),
        "per_node_prover_rss_mb": round(crit["prover"],2),
        "components_s": {**{k:round(v,6) for k,v in consumer_insert(args.logdir).items()},
                         **{k:round(v,6) for k,v in crit["comp"].items()}},
        "agg_wall_clock_s": round(args.agg_wall,3),
        "epochs_processed": sum(x["epochs"] for x in nodes),
        # Proof size & public output (zk): per-epoch (representative) + cluster
        # total summed over every epoch on every node.
        "proof_bytes_per_epoch": max((x["proof_bytes"] for x in nodes), default=0),
        "journal_bytes_per_epoch": max((x["journal_bytes"] for x in nodes), default=0),
        "total_proof_bytes": sum(x["proof_bytes"]*x["proof_epochs"] for x in nodes),
        "total_journal_bytes": sum(x["journal_bytes"]*x["proof_epochs"] for x in nodes),
        "memory_model":"sum_across_nodes(host+prover); time=max-node critical path"}

    db,mg,pr,vf=parse_querier(args.logdir)
    db_s=(db or 0)/1000; des=(mg or 0)/1000; comp_s=(pr or 0)/1000; ver=(vf or 0)/1000
    qh,qp,_=node_mem(os.path.join(args.workdir,"mem_query.json"))
    if args.mode == "native":
        qp = 0.0
    # Query runs ONE engine: peak = querier host + its single prover (max each),
    # NOT the summed_peak (which double-counts stray dying r0vm from aggregators).
    q_rec={**base, "task":"query","total_time_s":round(db_s+des+comp_s+ver,6),
        "peak_rss_mb": round(qh+qp,2),
        "host_peak_rss_mb": round(qh,2), "prover_peak_rss_mb": round(qp,2),
        "components_s":{"fdb_lookup":round(db_s,6),"deserialize":round(des,6),
                        "query_compute":round(comp_s,6),"verify":round(ver,6)},
        "memory_model":"single query engine host+prover"}

    with open(args.metrics,"a") as f:
        f.write(json.dumps(agg_rec)+"\n"); f.write(json.dumps(q_rec)+"\n")
    print(f"[parse-dist] {args.dataset}/{args.mode}/{args.num_aggregators}nodes: "
          f"agg crit={crit['total']:.1f}s cluster_mem={cluster_mem:.0f}MB "
          f"(host {host_sum:.0f}+prover {prover_sum:.0f}) | query={db_s+des+comp_s+ver:.3f}s")

if __name__=="__main__": main()
