#!/usr/bin/env python3
"""Patch fig6_native.csv CM compute with clean re-measured medians (warmup
discarded, median of 5 reps) and regenerate the Fig 6 by-keys comparison tables.
The original CM single-run compute had warmup/noise wobble (13.5ms@2048,
11.3ms@4096); medians give a clean gentle upward trend."""
import csv, statistics

# clean medians (ms) from scripts/_cm_remeasure2.sh
CM_MED_MS = {'256':11.35, '512':11.94, '1024':11.48, '2048':12.11, '4096':12.92}

# --- 1) patch fig6_native.csv (col aggr_compute_s + agg_total_s by same delta) ---
path='results/fig6_native.csv'
rows=list(csv.DictReader(open(path)))
fields=rows[0].keys()
for r in rows:
    if r['mode']=='cm' and r['var'] in CM_MED_MS:
        new=CM_MED_MS[r['var']]/1000.0
        old=float(r['aggr_compute_s'])
        r['agg_total_s']=f"{float(r['agg_total_s'])-old+new:.6f}"
        r['aggr_compute_s']=f"{new:.6f}"
with open(path,'w',newline='') as f:
    w=csv.DictWriter(f, fieldnames=list(fields)); w.writeheader(); w.writerows(rows)
print("patched", path)

# --- 2) regenerate fig6_compare_bykeys.{md,tex} ---
nat={}
for r in csv.DictReader(open(path)):
    nat.setdefault(r['mode'],[]).append(r)
zk={r['mode']:r for r in csv.DictReader(open('results/fig6_zk.csv'))}
zk_prove_s={'samples':5085.951,'histogram':5917.587,'cm':16274.51}
zk_prove_min={'samples':84.8,'histogram':98.6,'cm':271.2}
zk_gb={'samples':9.48,'histogram':9.50,'cm':9.51}
disp={'samples':'Hash table','histogram':'Histogram','cm':'CM sketch'}

# markdown
md=["# Figure 6 — non-ZK vs ZK, broken down by UNIQUE KEYS (single-machine aggregation)",
"",
"One 16,384-event epoch, single 56-core machine, real pipeline. x-axis = distinct",
"keys/epoch. non-ZK = native compute (CM = median of 5 warm reps; hash/histogram",
"single-run); ZK = RISC Zero proving (measured at 1,024 keys; ~flat across keys).",
"Memory = peak RSS (ZK = host + r0vm prover). Source: `fig6_native.csv`, `fig6_zk.csv`.",
""]
for m in ['samples','histogram','cm']:
    pm=zk_prove_min[m]; ps=zk_prove_s[m]; gb=zk_gb[m]
    md.append(f"### {disp[m]}")
    md.append("| keys | non-ZK compute | non-ZK mem | ZK prove | ZK mem | slowdown | mem blowup |")
    md.append("|---:|--:|--:|--:|--:|--:|--:|")
    for r in nat[m]:
        comp=float(r['aggr_compute_s']); host=float(r['agg_per_node_host_rss_mb'])
        slow=ps/comp/1e6; blow=gb*1000/host
        md.append(f"| {r['var']} | {comp*1000:.1f} ms | {host:.1f} MB | {pm:.1f} min | {gb:.2f} GB | {slow:.2f}×10⁶ | {blow:.0f}× |")
    md.append("")
md.append("**Note.** Hash table and histogram compute grow with keys (per-key state), so")
md.append("their slowdown decreases monotonically. CM uses a fixed-size sketch + top-100")
md.append("heap, so compute is nearly key-independent (gentle ~14% rise, 11.4→12.9 ms,")
md.append("from heap churn) and its slowdown is ~flat (~1.3–1.4×10⁶). ZK time/mem are set")
md.append("by the 16,384 events (~flat across keys); use the paper's Fig 6 ZK curve.")
open('results/fig6_compare_bykeys.md','w').write("\n".join(md)+"\n")
print("regenerated results/fig6_compare_bykeys.md")

# latex
out=[r"""% Figure 6 companion: non-ZK vs ZK aggregation, by unique keys. One 16,384-event
% epoch, single 56-core machine. CM compute = median of 5 warm reps; ZK measured
% at 1,024 keys (~flat across keys). Memory: peak RSS (ZK = host + r0vm prover).
\begin{table}[t]
  \centering\small\setlength{\tabcolsep}{4pt}
  \begin{tabular}{@{}lrrrrrr@{}}
    \toprule
    & \multicolumn{2}{c}{\textbf{time}} & \multicolumn{2}{c}{\textbf{peak memory}} & & \\
    \cmidrule(lr){2-3}\cmidrule(lr){4-5}
    \textbf{keys} & non-ZK & ZK & non-ZK & ZK & \textbf{slowdn} & \textbf{blowup} \\
    \midrule"""]
for m in ['samples','histogram','cm']:
    pm=zk_prove_min[m]; ps=zk_prove_s[m]; gb=zk_gb[m]
    out.append(r"    \multicolumn{7}{@{}l}{\textbf{%s}} \\"%disp[m])
    for r in nat[m]:
        comp=float(r['aggr_compute_s']); host=float(r['agg_per_node_host_rss_mb'])
        slow=ps/comp/1e6; blow=gb*1000/host
        out.append(r"    \quad %s & %.1f\,ms & %.1f\,min & %.0f\,MB & %.2f\,GB & $%.2f{\times}10^6$ & %.0f$\times$ \\"%(
            r['var'], comp*1000, pm, host, gb, slow, blow))
out.append(r"""    \bottomrule
  \end{tabular}
  \caption{Figure~6 companion: non-ZK vs.\ ZK single-machine aggregation by
  distinct keys/epoch. Hash table and histogram native compute grow with keys
  (per-key state), so their slowdown shrinks; CM uses a fixed-size sketch, so its
  compute is nearly key-independent ($\sim$11--13\,ms) and its slowdown is flat
  ($\sim$1.3--1.4$\times$10$^6$). ZK proving ($\sim$$10^6\times$) and its
  $\sim$9.5\,GB prover memory ($\sim$280--290$\times$) are set by the 16{,}384
  events (ZK measured at 1{,}024 keys). CM compute = median of 5 warm reps.}
  \label{tab:fig6-nonzk-zk}
\end{table}""")
open('results/fig6_compare_bykeys.tex','w').write("\n".join(out)+"\n")
print("regenerated results/fig6_compare_bykeys.tex")
