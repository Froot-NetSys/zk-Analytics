#!/usr/bin/env python3
"""Build the non-ZK vs ZK comparison markdown for Figures 5/6/7 from the
native + zk CSVs (whichever exist). Emits per-figure tables with prove/verify
time, memory (host + r0vm prover), proof size, public output, and the derived
slowdown + memory-blowup ratios. Missing CSVs are skipped gracefully.
"""
import csv, os, sys

R = os.path.join(os.path.dirname(__file__), "..", "results")

def load(name):
    p = os.path.join(R, name)
    if not os.path.exists(p):
        return None
    return list(csv.DictReader(open(p)))

def f(x, d=3):
    try: return round(float(x), d)
    except Exception: return x

def kb(x):
    try: return round(float(x) / 1024.0, 2)
    except Exception: return 0.0

out = []
out.append("# Non-ZK native vs ZK (zkVM) — Figures 5/6/7\n")
out.append("Same RocksDB→FDB→querier pipeline. Native = `NO_ZKVM_PROOF=1`; "
           "ZK = real RISC Zero proving + verification. Memory split into host "
           "process vs r0vm prover. Proof size / public output are per epoch.\n")

# ---- Figure 6: single machine, per mode (cleanest comparison) ----
nat6 = load("fig6_native.csv"); zk6 = load("fig6_zk.csv")
if zk6:
    out.append("## Figure 6 — single machine, per mode\n")
    out.append("| Mode | native agg (s) | zk prove (s) | zk verify (s) | "
               "**slowdown** | native host (MB) | zk host (MB) | zk prover (MB) | "
               "**mem blowup** | proof (KB) | public out (KB) |")
    out.append("|------|----:|----:|----:|----:|----:|----:|----:|----:|----:|----:|")
    # native fig6 per mode: take a representative key (1024) if present else last
    def nat6_for(mode):
        rows = [r for r in (nat6 or []) if r["mode"] == mode]
        if not rows: return None
        for r in rows:
            if r.get("var") == "1024": return r
        return rows[-1]
    for z in zk6:
        mode = z["mode"]; n = nat6_for(mode)
        nat_t = float(n["aggr_compute_s"]) if n else None   # pure compute
        nat_tot = float(n["agg_total_s"]) if n else None
        zk_prove = float(z["prove_s"]); zk_ver = float(z["query_verify_s"])
        nat_host = float(n["agg_per_node_host_rss_mb"]) if n else 0.0
        zk_host = float(z["agg_host_rss_mb"]); zk_prov = float(z["agg_prover_rss_mb"])
        slow = round(zk_prove / nat_tot, 0) if (nat_tot and nat_tot > 0) else "—"
        blow = round((zk_host + zk_prov) / nat_host, 0) if nat_host else "—"
        out.append(f"| {mode} | {f(nat_tot)} | {f(zk_prove,1)} | {f(zk_ver,3)} | "
                   f"**{slow}×** | {f(nat_host,1)} | {f(zk_host,1)} | {f(zk_prov,1)} | "
                   f"**{blow}×** | {kb(z['proof_bytes'])} | {kb(z['journal_bytes'])} |")
    out.append("")

# ---- Figure 5: distributed, vary aggregators ----
nat5 = load("fig5_native.csv"); zk5 = load("fig5_zk.csv")
if zk5:
    out.append("## Figure 5 — distributed, vary aggregator count\n")
    out.append("| Mode | N | native agg (s) | zk prove (s) | zk verify (s) | "
               "**slowdown** | native/node host (MB) | zk/node host (MB) | "
               "zk/node prover (MB) | cluster zk (GB) | proof (KB) |")
    out.append("|------|--:|----:|----:|----:|----:|----:|----:|----:|----:|----:|")
    def nat5_for(mode, N):
        for r in (nat5 or []):
            if r["mode"] == mode and r["var"] == str(N): return r
        return None
    for z in zk5:
        mode = z["mode"]; N = z["var"]; n = nat5_for(mode, N)
        nat_tot = float(n["agg_total_s"]) if n else None
        zk_prove = float(z["prove_s"]); zk_ver = float(z["query_verify_s"])
        nat_host = float(n["agg_per_node_host_rss_mb"]) if n else 0.0
        zk_host = float(z["agg_host_rss_mb"]); zk_prov = float(z["agg_prover_rss_mb"])
        cluster_gb = round(float(z["agg_cluster_rss_mb"]) / 1024.0, 2)
        slow = round(zk_prove / nat_tot, 0) if (nat_tot and nat_tot > 0) else "—"
        out.append(f"| {mode} | {N} | {f(nat_tot)} | {f(zk_prove,1)} | {f(zk_ver,3)} | "
                   f"**{slow}×** | {f(nat_host,1)} | {f(zk_host,1)} | {f(zk_prov,1)} | "
                   f"{cluster_gb} | {kb(z['proof_bytes'])} |")
    out.append("")

# ---- Figure 7: query, vary epochs ----
zk7 = load("fig7_zk.csv")
if zk7:
    out.append("## Figure 7 — query, vary #queried epochs\n")
    out.append("| Mode | epochs | zk query total (s) | prove (s) | verify (s) | "
               "host (MB) | prover (MB) |")
    out.append("|------|------:|----:|----:|----:|----:|----:|")
    for z in zk7:
        out.append(f"| {z.get('epoch_type',z.get('mode',''))} | {z.get('queried_epochs','')} | "
                   f"{f(z.get('query_total_s'))} | {f(z.get('query_prove_s'))} | "
                   f"{f(z.get('query_verify_s'))} | {f(z.get('query_host_rss_mb'),1)} | "
                   f"{f(z.get('query_prover_rss_mb'),1)} |")
    out.append("")

open(os.path.join(R, "zk_vs_native_results.md"), "w").write("\n".join(out) + "\n")
print("wrote results/zk_vs_native_results.md")
