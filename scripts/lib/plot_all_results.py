#!/usr/bin/env python3
"""Generate every artifact plot whose measured input is currently available."""

from __future__ import annotations

import csv
import argparse
import glob
import json
import os
from collections import defaultdict

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:
    print("[plots] ERROR: install python3-matplotlib (or pip install matplotlib)")
    raise SystemExit(1)


ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
RESULTS = os.path.join(ROOT, "results")
PLOTS = os.path.join(ROOT, "plots")
STATUS = {}


def rows(path):
    if not os.path.isfile(path):
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def number(row, key):
    try:
        return float(row[key])
    except (KeyError, TypeError, ValueError):
        return None


def save(fig, name):
    path = os.path.join(PLOTS, name)
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)
    STATUS[name] = {"status": "generated", "path": path}
    print(f"[plots] wrote {path}")


def skipped(name, inputs):
    STATUS[name] = {"status": "skipped", "inputs": inputs}
    print(f"[plots] skipped {name}: missing measured input ({', '.join(inputs)})")
    if os.path.exists(os.path.join(PLOTS, name)):
        print(f"[plots] WARNING: existing {name} is stale; it was NOT regenerated")


def plot_fig4():
    records = []
    for path in sorted(glob.glob(os.path.join(RESULTS, "e2e_*", "*.jsonl"))):
        run = os.path.basename(os.path.dirname(path)).removeprefix("e2e_")
        with open(path) as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    record["run"] = run
                    records.append(record)
    if not records:
        return skipped("fig4_e2e.pdf", ["results/e2e_*/*.jsonl"])
    labels = sorted({f"{r['dataset']}\n{r['run']}" for r in records})
    fig, axes = plt.subplots(1, 2, figsize=(max(7, len(labels) * 1.3), 4))
    for task, marker in (("aggregation", "o"), ("query", "s")):
        selected = {f"{r['dataset']}\n{r['run']}": r for r in records if r.get("task") == task}
        axes[0].plot(labels, [number(selected.get(x, {}), "total_time_s") for x in labels],
                     marker=marker, label=task)
        axes[1].plot(labels, [number(selected.get(x, {}), "peak_rss_mb") for x in labels],
                     marker=marker, label=task)
    axes[0].set_ylabel("Measured time (s)")
    axes[0].set_title("Aggregation critical path / query time")
    axes[1].set_ylabel("Peak RSS (MB)")
    axes[1].set_title("Figure 4: memory")
    for ax in axes:
        ax.legend()
        ax.tick_params(axis="x", labelrotation=30)
    save(fig, "fig4_e2e.pdf")


def plot_fig5():
    datasets = [(kind, rows(os.path.join(RESULTS, f"fig5_{kind}.csv"))) for kind in ("dev", "zk")]
    datasets = [(kind, data) for kind, data in datasets if data]
    if not datasets:
        return skipped("fig5_scaling.pdf", ["results/fig5_dev.csv", "results/fig5_zk.csv"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for kind, data in datasets:
        grouped = defaultdict(list)
        for row in data:
            grouped[row["mode"]].append((number(row, "var"), number(row, "agg_total_s"),
                                          number(row, "agg_cluster_rss_mb")))
        for mode, points in grouped.items():
            points.sort()
            axes[0].plot([p[0] for p in points], [p[1] for p in points], marker="o", label=f"{mode} ({kind})")
            axes[1].plot([p[0] for p in points], [p[2] for p in points], marker="o", label=f"{mode} ({kind})")
    axes[0].set_title("Figure 5: aggregation scaling")
    axes[0].set_ylabel("Critical-path time (s)")
    axes[1].set_title("Figure 5: cluster memory")
    axes[1].set_ylabel("Peak RSS (MB)")
    for ax in axes:
        ax.set_xlabel("Number of aggregators")
        ax.set_xticks([1, 2, 4, 8])
        ax.legend(fontsize=7)
    save(fig, "fig5_scaling.pdf")


def plot_fig6():
    sources = [("dev", "zkvm_dev_aggregation.csv"), ("real ZK", "zkvm_aggregation_56threads.csv")]
    data = [(label, rows(os.path.join(RESULTS, name))) for label, name in sources]
    data = [(label, rs) for label, rs in data if rs]
    if not data:
        return skipped("fig6_aggregation.pdf", [f"results/{x[1]}" for x in sources])
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    for label, rs in data:
        grouped = defaultdict(list)
        for row in rs:
            grouped[row["mode"]].append((number(row, "unique_keys"), number(row, "prove_ms_total") / 1000))
        for mode, points in grouped.items():
            points.sort()
            ax.plot(*zip(*points), marker="o", label=f"{mode} ({label})")
    ax.set_xlabel("Unique keys")
    ax.set_ylabel("Prove / guest-execution time (s)")
    ax.set_yscale("log")
    ax.set_title("Figure 6: standalone aggregation")
    ax.legend(fontsize=7)
    save(fig, "fig6_aggregation.pdf")


def plot_fig7():
    sources = [("dev", "zkvm_dev_query.csv"), ("real ZK", "zkvm_query_proofs.csv")]
    data = [(label, rows(os.path.join(RESULTS, name))) for label, name in sources]
    data = [(label, rs) for label, rs in data if rs]
    if not data:
        return skipped("fig7_query.pdf", [f"results/{x[1]}" for x in sources])
    fig, ax = plt.subplots(figsize=(8, 5))
    for label, rs in data:
        grouped = defaultdict(list)
        for row in rs:
            key = row.get("query", row.get("query_type", "query"))
            grouped[key].append((number(row, "num_epochs"), number(row, "prove_ms") / 1000))
        for query, points in grouped.items():
            points.sort()
            ax.plot(*zip(*points), marker="o", label=f"{query} ({label})")
    ax.set_xlabel("Queried epochs")
    ax.set_ylabel("Prove / guest-execution time (s)")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.set_title("Figure 7: standalone queries")
    ax.legend(fontsize=6, ncol=2)
    save(fig, "fig7_query.pdf")


def plot_table2():
    data = rows(os.path.join(RESULTS, "table2_native.csv"))
    if not data:
        return skipped("table2_native.pdf", ["results/table2_native.csv"])
    labels = [f"{r['dataset']}\n({r['num_aggregators']} nodes)" for r in data]
    fields = [("rocksdb_insert_ms_per_epoch", "RocksDB insert"),
              ("rocksdb_read_ms_per_epoch", "RocksDB read"),
              ("aggregation_compute_ms_per_epoch", "Aggregation"),
              ("fdb_write_ms_per_epoch", "FDB write")]
    fig, ax = plt.subplots(figsize=(8, 4.5))
    bottom = [0.0] * len(data)
    for field, label in fields:
        values = [number(r, field) or 0 for r in data]
        ax.bar(labels, values, bottom=bottom, label=label)
        bottom = [a + b for a, b in zip(bottom, values)]
    ax.set_ylabel("Time (ms per epoch)")
    ax.set_title("Table 2: native storage and aggregation costs")
    ax.legend(fontsize=8)
    save(fig, "table2_native.pdf")


def plot_table3():
    kind = "real ZK"
    data = rows(os.path.join(RESULTS, "fig5_zk.csv"))
    if not data:
        kind = "DEV: guest execution, no cryptographic proof"
        data = rows(os.path.join(RESULTS, "fig5_dev.csv"))
    if not data:
        return skipped("table3_cost_components.pdf", ["results/fig5_zk.csv", "results/fig5_dev.csv"])
    fields = [("kafka_recv_s", "Kafka receive"), ("rocksdb_raw_insert_s", "RocksDB insert"),
              ("prove_s", "Prove / guest execution"), ("verify_s", "Verify"),
              ("fdb_write_s", "FDB write")]
    modes = sorted({r["mode"] for r in data})
    fig, axes = plt.subplots(1, len(modes), figsize=(5 * len(modes), 4), squeeze=False)
    for ax, mode in zip(axes[0], modes):
        selected = sorted((r for r in data if r["mode"] == mode), key=lambda r: number(r, "var"))
        x = [int(number(r, "var")) for r in selected]
        for field, label in fields:
            ax.plot(x, [number(r, field) or 0 for r in selected], marker="o", label=label)
        ax.set_title(mode)
        ax.set_xlabel("Number of aggregators")
        ax.set_ylabel("Time (s)")
        ax.legend(fontsize=7)
    fig.suptitle(f"Table 3: distributed cost components ({kind})")
    save(fig, "table3_cost_components.pdf")


def plot_non_zk_comparisons():
    agg = rows(os.path.join(RESULTS, "non_zk_aggregation_baseline.csv"))
    complete = [r for r in agg if number(r, "native_single_thread_s") is not None
                and number(r, "zkvm_prove_s") is not None]
    if complete:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        labels = [r["aggregation_type"] for r in complete]
        x = list(range(len(labels)))
        ax.bar([i - .2 for i in x], [number(r, "native_single_thread_s") for r in complete], .4,
               label="Native")
        ax.bar([i + .2 for i in x], [number(r, "zkvm_prove_s") for r in complete], .4,
               label="zkVM prove")
        ax.set_xticks(x, labels, rotation=20, ha="right")
        ax.set_yscale("log")
        ax.set_ylabel("Time (s)")
        ax.set_title("Native vs zkVM aggregation")
        ax.legend()
        save(fig, "non_zk_vs_zk_aggregation.pdf")
    else:
        skipped("non_zk_vs_zk_aggregation.pdf", ["complete measured ZK columns in results/non_zk_aggregation_baseline.csv"])

    query = rows(os.path.join(RESULTS, "non_zk_query_baseline.csv"))
    if query:
        fig, ax = plt.subplots(figsize=(8, 5))
        grouped = defaultdict(list)
        for row in query:
            grouped[row["query_type"]].append((number(row, "num_epochs"), number(row, "native_query_s")))
        for label, points in grouped.items():
            points = sorted(p for p in points if None not in p)
            if points:
                ax.plot(*zip(*points), marker="o", markersize=3, label=f"{label} (native)")
        anchors = [(number(r, "num_epochs"), number(r, "zkvm_prove_s"), r["query_type"])
                   for r in query if number(r, "zkvm_prove_s") is not None]
        for epochs, prove, label in anchors:
            ax.scatter(epochs, prove, marker="x", color="black")
        if anchors:
            ax.scatter([], [], marker="x", color="black", label="zkVM prove")
        ax.set_xscale("log", base=2)
        ax.set_yscale("log")
        ax.set_xlabel("Queried epochs")
        ax.set_ylabel("Time (s)")
        ax.set_title("Native vs zkVM queries")
        ax.legend(fontsize=6, ncol=2)
        save(fig, "non_zk_vs_zk_query.pdf")
    else:
        skipped("non_zk_vs_zk_query.pdf", ["results/non_zk_query_baseline.csv"])

    breakdown = rows(os.path.join(RESULTS, "zk_cost_breakdown.csv"))
    complete = [r for r in breakdown if number(r, "aggregation_time_s") is not None
                or number(r, "query_time_s") is not None]
    if complete:
        fig, ax = plt.subplots(figsize=(8, 4.5))
        labels = [r["component"].replace("_", "\n") for r in complete]
        x = list(range(len(labels)))
        ax.bar([i - .2 for i in x], [number(r, "aggregation_time_s") or 0 for r in complete], .4,
               label="Aggregation")
        ax.bar([i + .2 for i in x], [number(r, "query_time_s") or 0 for r in complete], .4,
               label="Query")
        ax.set_xticks(x, labels, fontsize=7)
        ax.set_yscale("log")
        ax.set_ylabel("Time (s)")
        ax.set_title("zk-Analytics cost breakdown")
        ax.legend()
        save(fig, "zk_cost_breakdown.pdf")
    else:
        skipped("zk_cost_breakdown.pdf", ["results/zk_cost_breakdown.csv"])


def main():
    global RESULTS, PLOTS
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--results-dir", default=RESULTS)
    parser.add_argument("--plots-dir", default=PLOTS)
    args = parser.parse_args()
    RESULTS = os.path.abspath(args.results_dir)
    PLOTS = os.path.abspath(args.plots_dir)
    STATUS.clear()
    os.makedirs(PLOTS, exist_ok=True)
    plot_fig4()
    plot_fig5()
    plot_fig6()
    plot_fig7()
    plot_table2()
    plot_table3()
    plot_non_zk_comparisons()
    manifest = os.path.join(PLOTS, "plot_manifest.json")
    with open(manifest, "w") as output:
        json.dump({"results_dir": RESULTS, "plots": STATUS}, output, indent=2)
        output.write("\n")
    print(f"[plots] wrote {manifest}; skipped plots are not validated results")


if __name__ == "__main__":
    main()
