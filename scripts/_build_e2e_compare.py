#!/usr/bin/env python3
"""Build the Figure-4 e2e non-ZK vs ZK comparison from native + zk metrics:
RocksDB/FDB read+write timing, aggregation time, query time, memory, proof.
Reads results/e2e_native/{ds}_native.jsonl and results/e2e_zk/{ds}_zk.jsonl."""
import json, os
R = os.path.join(os.path.dirname(__file__), "..", "results")
DS = [("google","Google","Hash table","8"),
      ("caida","CAIDA","Count-Min","8"),
      ("vehicle","Vehicle","Histogram","4")]

def load(p):
    if not os.path.exists(p): return None,None
    recs=[json.loads(l) for l in open(p) if l.strip()]
    agg=[r for r in recs if r['task']=='aggregation']
    q=[r for r in recs if r['task']=='query']
    return (agg[-1] if agg else None),(q[-1] if q else None)

rows=[]
for ds,name,prim,nagg in DS:
    na,nq = load(os.path.join(R,"e2e_native",f"{ds}_native.jsonl"))
    za,zq = load(os.path.join(R,"e2e_zk",f"{ds}_zk.jsonl"))
    if not na or not za:
        rows.append((name,prim,nagg,None)); continue
    nc=na['components_s']; zc=za['components_s']; nqc=(nq or {}).get('components_s',{}); zqc=(zq or {}).get('components_s',{})
    def ms(d,k): return d.get(k,0.0)*1000
    rows.append((name,prim,nagg,dict(
        rdb_insert=(ms(nc,'rocksdb_raw_insert'),ms(zc,'rocksdb_raw_insert')),
        rdb_read=(ms(nc,'rocksdb_raw_read'),ms(zc,'rocksdb_raw_read')),
        fdb_write=(ms(nc,'fdb_write'),ms(zc,'fdb_write')),
        fdb_read=(ms(nqc,'fdb_lookup'),ms(zqc,'fdb_lookup')),
        agg=(nc.get('aggr_compute',0.0), zc.get('prove',0.0)),         # native compute vs ZK prove (s)
        query=((nq or {}).get('total_time_s',0.0),(zq or {}).get('total_time_s',0.0)),  # s
        verify=zqc.get('verify',0.0)*1000,
        nmem=na.get('host_peak_rss_mb',0.0), zmem=za.get('prover_peak_rss_mb',0.0)+za.get('host_peak_rss_mb',0.0),
        proof=za.get('proof_bytes_per_epoch',0)/1024,
    )))

def fmt_t(s):
    if s>=3600: return f"{s/3600:.1f} h"
    if s>=60: return f"{s/60:.1f} min"
    if s>=1: return f"{s:.1f} s"
    return f"{s*1000:.0f} ms" if s>0 else "--"

out=["# Figure-4 e2e: non-ZK baseline vs zk-Analytics (real datasets)",""]
out.append("Same Figure-4 setup, full pipeline (data source w/ hash-chain commitment "
           "-> Kafka -> RocksDB -> aggregator -> FoundationDB -> querier). non-ZK = "
           "native (NO_ZKVM_PROOF); ZK = RISC Zero prove+verify. Storage timing is "
           "per-epoch on the critical-path aggregator; aggregation/query are the "
           "critical-path totals. Memory = peak RSS (ZK = host + r0vm prover, cluster).")
out.append("")
out.append("| Dataset (primitive, aggr) | Metric | non-ZK | ZK |")
out.append("|---|---|--:|--:|")
for name,prim,nagg,d in rows:
    if d is None:
        out.append(f"| {name} | (pending) | -- | -- |"); continue
    tag=f"{name} ({prim}, {nagg})"
    out.append(f"| {tag} | RocksDB raw insert | {d['rdb_insert'][0]:.0f} ms | {d['rdb_insert'][1]:.0f} ms |")
    out.append(f"| | RocksDB raw read | {d['rdb_read'][0]:.1f} ms | {d['rdb_read'][1]:.1f} ms |")
    out.append(f"| | FoundationDB write | {d['fdb_write'][0]:.1f} ms | {d['fdb_write'][1]:.1f} ms |")
    out.append(f"| | FoundationDB read (query) | {d['fdb_read'][0]:.0f} ms | {d['fdb_read'][1]:.0f} ms |")
    sa = d['agg'][1]/d['agg'][0] if d['agg'][0]>0 else 0
    sq = d['query'][1]/d['query'][0] if d['query'][0]>0 else 0
    out.append(f"| | **Aggregation** (compute→prove) | {fmt_t(d['agg'][0])} | {fmt_t(d['agg'][1])} (**{sa:.0e}×**) |")
    out.append(f"| | **Query** (total) | {fmt_t(d['query'][0])} | {fmt_t(d['query'][1])} (**{sq:.0e}×**) |")
    out.append(f"| | Verification | -- | {d['verify']:.0f} ms |")
    out.append(f"| | Peak memory (cluster) | {d['nmem']:.0f} MB | {d['zmem']/1000:.1f} GB |")
    out.append(f"| | Proof / epoch | -- | {d['proof']:.0f} KB |")
open(os.path.join(R,"e2e_nonzk_vs_zk.md"),"w").write("\n".join(out)+"\n")
print("wrote results/e2e_nonzk_vs_zk.md")
for name,prim,nagg,d in rows:
    if d: print(f"{name}: agg native={fmt_t(d['agg'][0])} ZK={fmt_t(d['agg'][1])} | query native={fmt_t(d['query'][0])} ZK={fmt_t(d['query'][1])} | rdb_ins {d['rdb_insert'][0]:.0f}/{d['rdb_insert'][1]:.0f}ms fdb_wr {d['fdb_write'][0]:.1f}/{d['fdb_write'][1]:.1f}ms")
