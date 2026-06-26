#!/usr/bin/env python3
"""Peak-RSS memory tracer for the zk-Analytics camera-ready baseline.

Polls /proc/<pid>/status (+ /proc/<pid>/stat for CPU%) for a set of process
PIDs and/or processes matched by command-line substring, at a fixed interval,
and writes a per-sample trace CSV plus a per-process peak summary.

This is the authoritative source for the paper's runtime memory numbers:
  - per-process peak RSS,
  - summed peak RSS across all matched processes (cluster total) — used for the
    aggregation rows, where multiple aggregator processes run concurrently,
  - single-process peak RSS — used for the query row (one query engine).

Usage:
  scripts/mem_trace.py --out results/memory_trace_aggregation.csv \
      --match aggregator --match kafka-consumer \
      --interval 0.5 --summary results/_mem_summary_agg.json

  # stop by sending SIGINT/SIGTERM, or pass --max-seconds N.

Trace CSV columns: timestamp,process,pid,RSS_MB,VSZ_MB,CPU_percent
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

CLK_TCK = os.sysconf("SC_CLK_TCK") if hasattr(os, "sysconf") else 100


def list_pids():
    for name in os.listdir("/proc"):
        if name.isdigit():
            yield int(name)


def proc_cmdline(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/cmdline", "rb") as f:
            return f.read().replace(b"\x00", b" ").decode("utf-8", "replace").strip()
    except OSError:
        return ""


def proc_comm(pid: int) -> str:
    try:
        with open(f"/proc/{pid}/comm") as f:
            return f.read().strip()
    except OSError:
        return ""


def proc_mem_kb(pid: int):
    """Return (rss_kb, vsz_kb) from /proc/<pid>/status, or None if gone."""
    rss = vsz = 0
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    rss = int(line.split()[1])
                elif line.startswith("VmSize:"):
                    vsz = int(line.split()[1])
    except OSError:
        return None
    return rss, vsz


def proc_cpu_jiffies(pid: int):
    """utime+stime in clock ticks from /proc/<pid>/stat, or None."""
    try:
        with open(f"/proc/{pid}/stat") as f:
            data = f.read()
        # comm may contain spaces/parens; split on the last ')'.
        rparen = data.rfind(")")
        fields = data[rparen + 2 :].split()
        # After "(comm) ", field index 11 = utime, 12 = stime (0-based here:
        # state is fields[0]; utime is fields[11], stime fields[12]).
        utime = int(fields[11])
        stime = int(fields[12])
        return utime + stime
    except (OSError, IndexError, ValueError):
        return None


def matches(cmdline: str, comm: str, needles) -> bool:
    hay = cmdline + " " + comm
    return any(n in hay for n in needles)


def short_name(cmdline: str, comm: str) -> str:
    if comm:
        return comm
    return (cmdline.split() or ["?"])[0].rsplit("/", 1)[-1]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="per-sample trace CSV path")
    ap.add_argument("--summary", help="optional JSON peak-summary path")
    ap.add_argument("--match", action="append", default=[],
                    help="cmdline/comm substring to track (repeatable)")
    ap.add_argument("--pid", action="append", type=int, default=[],
                    help="explicit PID to track (repeatable)")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--max-seconds", type=float, default=0.0,
                    help="auto-stop after N seconds (0 = run until signalled)")
    args = ap.parse_args()

    if not args.match and not args.pid:
        print("mem_trace: need at least one --match or --pid", file=sys.stderr)
        sys.exit(2)

    stop = {"flag": False}

    def _sig(_signum, _frame):
        stop["flag"] = True

    signal.signal(signal.SIGINT, _sig)
    signal.signal(signal.SIGTERM, _sig)

    os.makedirs(os.path.dirname(os.path.abspath(args.out)) or ".", exist_ok=True)

    # peak[pid] = {"name", "rss_mb", "vsz_mb"}; cpu_prev[pid] = (jiffies, walltime)
    peak = {}
    cpu_prev = {}
    t0 = time.time()

    with open(args.out, "w") as out:
        out.write("timestamp,process,pid,RSS_MB,VSZ_MB,CPU_percent\n")
        while not stop["flag"]:
            now = time.time()
            # Resolve target PIDs each tick so newly-spawned workers are caught.
            targets = set(args.pid)
            if args.match:
                for pid in list_pids():
                    cl = proc_cmdline(pid)
                    cm = proc_comm(pid)
                    if matches(cl, cm, args.match):
                        targets.add(pid)
            for pid in sorted(targets):
                mem = proc_mem_kb(pid)
                if mem is None:
                    continue
                rss_kb, vsz_kb = mem
                rss_mb = rss_kb / 1024.0
                vsz_mb = vsz_kb / 1024.0
                name = short_name(proc_cmdline(pid), proc_comm(pid))
                # CPU%
                cpu_pct = 0.0
                j = proc_cpu_jiffies(pid)
                if j is not None:
                    prev = cpu_prev.get(pid)
                    if prev is not None:
                        dj = j - prev[0]
                        dt = now - prev[1]
                        if dt > 0:
                            cpu_pct = 100.0 * (dj / CLK_TCK) / dt
                    cpu_prev[pid] = (j, now)
                out.write(f"{now:.3f},{name},{pid},{rss_mb:.2f},{vsz_mb:.2f},{cpu_pct:.1f}\n")
                p = peak.get(pid)
                if p is None or rss_mb > p["rss_mb"]:
                    peak[pid] = {"name": name, "rss_mb": rss_mb, "vsz_mb": vsz_mb}
            out.flush()
            if args.max_seconds and (now - t0) >= args.max_seconds:
                break
            time.sleep(args.interval)

    # Summary: per-process peak + cluster-summed peak.
    summed = sum(p["rss_mb"] for p in peak.values())
    single_max = max((p["rss_mb"] for p in peak.values()), default=0.0)
    summary = {
        "per_process_peak_rss_mb": {
            str(pid): {"name": p["name"], "peak_rss_mb": round(p["rss_mb"], 2)}
            for pid, p in peak.items()
        },
        "summed_peak_rss_mb": round(summed, 2),
        "single_process_peak_rss_mb": round(single_max, 2),
        "num_processes": len(peak),
    }
    if args.summary:
        with open(args.summary, "w") as f:
            json.dump(summary, f, indent=2)
    print(json.dumps(summary))


if __name__ == "__main__":
    main()
