"""
Generate thesis plots from results/runs.csv
Output: results/amp_vs_streams.png, latency_vs_streams.png, cpu_vs_streams.png
"""

import os
import csv
import statistics
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "runs.csv")

# ── Load data ─────────────────────────────────────────────────────────────────

data = defaultdict(lambda: {"amp": [], "time": [], "cpu": [], "mem": []})

with open(CSV_PATH) as f:
    for row in csv.DictReader(f):
        n = int(row["n_streams"])
        data[n]["amp"].append(float(row["amp_factor"]))
        data[n]["time"].append(float(row["wall_time_s"]))
        data[n]["cpu"].append(float(row["server_cpu_avg"]))
        data[n]["mem"].append(float(row["server_mem_mb_peak"]))

ns = sorted(data.keys())

def mean(lst): return statistics.mean(lst)
def stdev(lst): return statistics.stdev(lst) if len(lst) > 1 else 0.0

amp_mean  = [mean(data[n]["amp"])  for n in ns]
amp_err   = [stdev(data[n]["amp"]) for n in ns]
time_mean = [mean(data[n]["time"]) for n in ns]
time_err  = [stdev(data[n]["time"])for n in ns]
cpu_mean  = [mean(data[n]["cpu"])  for n in ns]
cpu_err   = [stdev(data[n]["cpu"]) for n in ns]

STYLE = dict(marker="o", capsize=4, linewidth=1.8, color="#2563eb")

# ── Plot 1: Amplification vs Streams (log-y) ──────────────────────────────────

fig, ax = plt.subplots(figsize=(7, 4))
ax.errorbar(ns, amp_mean, yerr=amp_err, **STYLE)
ax.set_yscale("log")
ax.set_xlabel("Number of concurrent QUIC streams (N)")
ax.set_ylabel("Amplification factor (×)")
ax.set_title("Stream-Based Amplification Factor vs. N Concurrent Streams")
ax.set_xticks(ns)
ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.0f×"))
ax.grid(True, which="both", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "amp_vs_streams.png"), dpi=150)
plt.close(fig)
print("Saved amp_vs_streams.png")

# ── Plot 2: Wall time vs Streams ──────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(7, 4))
ax.errorbar(ns, time_mean, yerr=time_err, **STYLE)
ax.set_xlabel("Number of concurrent QUIC streams (N)")
ax.set_ylabel("Wall time (s)")
ax.set_title("Total Response Time vs. N Concurrent Streams")
ax.set_xticks(ns)
ax.grid(True, linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "latency_vs_streams.png"), dpi=150)
plt.close(fig)
print("Saved latency_vs_streams.png")

# ── Plot 3: Server CPU vs Streams ─────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(7, 4))
ax.errorbar(ns, cpu_mean, yerr=cpu_err, **STYLE)
ax.set_xlabel("Number of concurrent QUIC streams (N)")
ax.set_ylabel("Server CPU usage (%)")
ax.set_title("Server CPU Load vs. N Concurrent Streams")
ax.set_xticks(ns)
ax.set_ylim(bottom=0)
ax.grid(True, linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "cpu_vs_streams.png"), dpi=150)
plt.close(fig)
print("Saved cpu_vs_streams.png")
