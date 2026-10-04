"""
Attacker Cost Model — using Exp 5 wire-level data.

Computes the two metrics requested by the professor:

  Cost_attacker(N) = bytes_sent / bytes_received   (lower = cheaper attack)
  Efficiency(N)    = amp_wire(N) / cpu_server(N)   (higher = more damage per CPU%)

Also generates a comparison table vs known amplification attacks.
Writes: results/attacker_cost_model.csv
        results/exp5_cost_model.png
        results/amp_comparison_table.png
"""

import csv, os, math, statistics
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
EXP5_CSV    = os.path.join(RESULTS_DIR, "exp5_packet_bytes.csv")
OUT_CSV     = os.path.join(RESULTS_DIR, "attacker_cost_model.csv")

BLUE  = "#2563eb"
GREEN = "#16a34a"
RED   = "#ef4444"
LINE      = dict(marker="o", capsize=5, linewidth=1.8)
LINE_PLOT = dict(marker="o", linewidth=1.8)

def mean(lst): return statistics.mean(lst) if lst else 0
def ci95(lst):
    n = len(lst)
    if n < 2: return 0
    t = {2:12.71,3:4.30,4:3.18,5:2.78,6:2.57,7:2.45,8:2.36,9:2.31,10:2.26}.get(n,2.0)
    return t * statistics.stdev(lst) / math.sqrt(n)


# ── Load Exp 5 ────────────────────────────────────────────────────────────────

d = defaultdict(lambda: defaultdict(list))
with open(EXP5_CSV) as f:
    for row in csv.DictReader(f):
        try:
            n = int(row["n_streams"])
            d[n]["wire_sent"].append(float(row["req_bytes_wire"]))
            d[n]["wire_recv"].append(float(row["resp_bytes_wire"]))
            d[n]["amp_wire"].append(float(row["amp_factor_wire"]))
        except: pass

# Load CPU from main runs.csv
runs_csv = os.path.join(RESULTS_DIR, "runs.csv")
cpu_data = defaultdict(list)
with open(runs_csv) as f:
    for row in csv.DictReader(f):
        try:
            cpu_data[int(row["n_streams"])].append(float(row["server_cpu_avg"]))
        except: pass

ns = sorted(d.keys())


# ── Compute cost model ────────────────────────────────────────────────────────

rows = []
for n in ns:
    sent = mean(d[n]["wire_sent"])
    recv = mean(d[n]["wire_recv"])
    amp  = mean(d[n]["amp_wire"])
    cpu  = mean(cpu_data[n]) if cpu_data[n] else 1.0

    cost       = sent / recv if recv else 0          # attacker cost ratio
    efficiency = amp / cpu   if cpu  else 0          # amp per CPU%

    rows.append({
        "n_streams": n,
        "wire_sent_bytes": round(sent),
        "wire_recv_bytes": round(recv),
        "amp_wire": round(amp, 3),
        "cpu_avg_pct": round(cpu, 2),
        "cost_attacker": round(cost, 4),
        "efficiency": round(efficiency, 4),
    })
    print(f"N={n:3d}  cost={cost:.4f}  efficiency={efficiency:.3f}  "
          f"amp_wire={amp:.1f}x  cpu={cpu:.1f}%")

with open(OUT_CSV, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
    w.writeheader(); w.writerows(rows)
print(f"\nSaved → {OUT_CSV}")


# ── Plot 1: Cost model ────────────────────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(13, 5))

# Cost_attacker
ax = axes[0]
costs = [r["cost_attacker"] for r in rows]
ax.plot(ns, costs, color=RED, **LINE_PLOT)
ax.axhline(1.0, linestyle="--", color="gray", alpha=0.7,
           label="Break-even (cost=1.0)\nAttacker spends = receives")
ax.set_xlabel("N concurrent QUIC streams")
ax.set_ylabel("Cost_attacker = bytes_sent / bytes_received")
ax.set_title("Attacker Cost Ratio vs. N\n(lower = cheaper attack)")
ax.set_xticks(ns); ax.legend()
ax.grid(True, linestyle="--", alpha=0.4)

# Efficiency
ax = axes[1]
effs = [r["efficiency"] for r in rows]
ax.plot(ns, effs, color=GREEN, **LINE_PLOT)
ax.set_xlabel("N concurrent QUIC streams")
ax.set_ylabel("Efficiency = Amp_wire / CPU_server")
ax.set_title("Attack Efficiency vs. N\n(higher = more amplification per CPU% consumed)")
ax.set_xticks(ns)
ax.grid(True, linestyle="--", alpha=0.4)

fig.suptitle("Attacker Cost Model — Wire-Level Analysis (Experiment 5)",
             fontsize=13, fontweight="bold")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp5_cost_model.png"), dpi=150)
plt.close()
print("✓ exp5_cost_model.png")


# ── Plot 2: Comparison table vs known attacks ─────────────────────────────────

# Known amplification factors from literature
known_attacks = [
    ("SSDP",                    30),
    ("DNS",                     54),
    ("NTP",                    556),
    ("Memcached",            51000),
    ("HTTP/2 Rapid Reset",   ~1000),   # DoS via request flood
    ("This work\n(HTTP-level, N=100)",  133),
    ("This work\n(Wire-level, N=100)",   80),
]

labels  = [a[0] for a in known_attacks]
factors = [a[1] for a in known_attacks]
colors  = ["#94a3b8"]*5 + [BLUE, GREEN]

fig, ax = plt.subplots(figsize=(11, 5))
bars = ax.bar(labels, factors, color=colors, alpha=0.85, edgecolor="white")
ax.set_yscale("log")
ax.set_ylabel("Amplification factor (×, log scale)")
ax.set_title("Amplification Factor Comparison: This Work vs. Known Attacks\n"
             "(Sources: CERT, Cloudflare, academic literature)")
ax.grid(True, axis="y", linestyle="--", alpha=0.4)
ax.tick_params(axis="x", labelsize=9)

# Value labels on bars
for bar, val in zip(bars, factors):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() * 1.2,
            f"{val:,}×", ha="center", va="bottom", fontsize=9, fontweight="bold")

# Highlight this work
bars[5].set_edgecolor(BLUE); bars[5].set_linewidth(2)
bars[6].set_edgecolor(GREEN); bars[6].set_linewidth(2)

ax.set_ylim(top=ax.get_ylim()[1] * 5)  # extra headroom for label
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "amp_comparison_table.png"), dpi=150)
plt.close()
print("✓ amp_comparison_table.png")
