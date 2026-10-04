"""
Generate all thesis plots with 95% CI and boxplots.
Run: python plot_all.py
"""

import os, csv, math, statistics
from collections import defaultdict
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")

def load(filename, key_col, val_cols):
    path = os.path.join(RESULTS_DIR, filename)
    data = defaultdict(lambda: defaultdict(list))
    with open(path) as f:
        for row in csv.DictReader(f):
            k = row[key_col]
            for c in val_cols:
                try: data[k][c].append(float(row[c]))
                except: pass
    return data

def mean(lst):   return statistics.mean(lst) if lst else 0
def std(lst):    return statistics.stdev(lst) if len(lst) > 1 else 0
def ci95(lst):
    n = len(lst)
    if n < 2: return 0
    # t-value for 95% CI (two-tailed), approximate for n>=10
    t = {2:12.71,3:4.30,4:3.18,5:2.78,6:2.57,7:2.45,8:2.36,9:2.31,10:2.26}.get(n, 2.0)
    return t * std(lst) / math.sqrt(n)

BLUE  = "#2563eb"
GREEN = "#16a34a"
RED   = "#ef4444"
LINE  = dict(marker="o", capsize=5, linewidth=1.8)

# ════════════════════════════════════════════════════════════════════════════
# Main sweep (runs.csv)
# ════════════════════════════════════════════════════════════════════════════

d = load("runs.csv", "n_streams", ["amp_factor","wall_time_s","server_cpu_avg"])
ns = sorted(int(k) for k in d)

def vals(col): return [d[str(n)][col] for n in ns]

# ── Line plots with 95% CI ────────────────────────────────────────────────

for col, ylabel, title, fname in [
    ("amp_factor",    "Amplification factor (×)", "Amplification Factor vs. N Streams",    "amp_vs_streams.png"),
    ("wall_time_s",   "Wall time (s)",             "Total Response Time vs. N Streams",     "latency_vs_streams.png"),
    ("server_cpu_avg","Server CPU (%)",             "Server CPU Load vs. N Streams",         "cpu_vs_streams.png"),
]:
    fig, ax = plt.subplots(figsize=(7,4))
    means = [mean(v) for v in vals(col)]
    cis   = [ci95(v) for v in vals(col)]
    ax.errorbar(ns, means, yerr=cis, color=BLUE, **LINE)
    if col == "amp_factor": ax.set_yscale("log"); ax.yaxis.set_major_formatter(ticker.FormatStrFormatter("%.0f×"))
    if col == "server_cpu_avg": ax.set_ylim(bottom=0)
    ax.set_xlabel("N concurrent QUIC streams"); ax.set_ylabel(ylabel)
    ax.set_title(title); ax.set_xticks(ns)
    ax.grid(True, which="both", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, fname), dpi=150); plt.close()
    print(f"✓ {fname}")

# ── Boxplots ──────────────────────────────────────────────────────────────

for col, ylabel, title, fname in [
    ("wall_time_s",   "Wall time (s)",   "Response Time Distribution vs. N Streams (Boxplot)",  "box_latency.png"),
    ("server_cpu_avg","Server CPU (%)",  "Server CPU Distribution vs. N Streams (Boxplot)",      "box_cpu.png"),
]:
    fig, ax = plt.subplots(figsize=(8,4))
    ax.boxplot(vals(col), labels=ns, patch_artist=True,
               boxprops=dict(facecolor="#DBEAFE", color=BLUE),
               medianprops=dict(color=RED, linewidth=2),
               whiskerprops=dict(color=BLUE), capprops=dict(color=BLUE),
               flierprops=dict(marker="o", color=BLUE, alpha=0.5))
    ax.set_xlabel("N concurrent QUIC streams"); ax.set_ylabel(ylabel)
    ax.set_title(title); ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, fname), dpi=150); plt.close()
    print(f"✓ {fname}")

# ════════════════════════════════════════════════════════════════════════════
# Exp 1: Amp vs Range size
# ════════════════════════════════════════════════════════════════════════════

d1 = load("exp1_range_size.csv", "range_end_byte", ["amp_factor","wall_time_s"])
rs = sorted(int(k) for k in d1)

fig, ax = plt.subplots(figsize=(7,4))
means = [mean(d1[str(r)]["amp_factor"]) for r in rs]
cis   = [ci95(d1[str(r)]["amp_factor"]) for r in rs]
ax.errorbar(rs, means, yerr=cis, color=GREEN, **LINE)
ax.set_xlabel("Range end byte (response size ≈ value)")
ax.set_ylabel("Amplification factor (×)")
ax.set_title("Amplification Factor vs. Range Request Size (N=10 streams)")
ax.set_xticks(rs); ax.set_xticklabels([str(r) for r in rs], rotation=30)
ax.grid(True, linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp1_amp_vs_range.png"), dpi=150); plt.close()
print("✓ exp1_amp_vs_range.png")

# Boxplot for range size
fig, ax = plt.subplots(figsize=(8,4))
ax.boxplot([d1[str(r)]["amp_factor"] for r in rs], labels=rs, patch_artist=True,
           boxprops=dict(facecolor="#DCFCE7", color=GREEN),
           medianprops=dict(color=RED, linewidth=2),
           whiskerprops=dict(color=GREEN), capprops=dict(color=GREEN),
           flierprops=dict(marker="o", color=GREEN, alpha=0.5))
ax.set_xlabel("Range end byte"); ax.set_ylabel("Amplification factor (×)")
ax.set_title("Amplification Distribution vs. Range Size (Boxplot)")
ax.tick_params(axis='x', rotation=30)
ax.grid(True, axis="y", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp1_box_amp_vs_range.png"), dpi=150); plt.close()
print("✓ exp1_box_amp_vs_range.png")

# ════════════════════════════════════════════════════════════════════════════
# Exp 2: Protocol comparison
# ════════════════════════════════════════════════════════════════════════════

d2 = load("exp2_protocol_comparison.csv", "protocol",
          ["amp_factor","wall_time_s","server_cpu_avg"])
protos = ["HTTP/1.1","HTTP/2","HTTP/3"]
colors = ["#f59e0b","#3b82f6","#ef4444"]

fig, axes = plt.subplots(1, 3, figsize=(13,4))
for ax, (col, label) in zip(axes, [
    ("amp_factor",    "Amplification (×)"),
    ("wall_time_s",   "Wall time (s)"),
    ("server_cpu_avg","Server CPU (%)")
]):
    means = [mean(d2[p][col]) for p in protos]
    cis   = [ci95(d2[p][col]) for p in protos]
    ax.bar(protos, means, yerr=cis, capsize=5, color=colors, alpha=0.85)
    ax.set_ylabel(label); ax.set_title(label)
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)

fig.suptitle("Protocol Comparison: HTTP/1.1 vs HTTP/2 vs HTTP/3 (N=10, Range=0-9999)")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp2_protocol_comparison.png"), dpi=150); plt.close()
print("✓ exp2_protocol_comparison.png")

# Boxplot protocol comparison (wall time)
fig, axes = plt.subplots(1, 2, figsize=(12,4))
for ax, (col, label) in zip(axes, [
    ("wall_time_s",   "Wall time (s)"),
    ("server_cpu_avg","Server CPU (%)")
]):
    ax.boxplot([d2[p][col] for p in protos], labels=protos, patch_artist=True,
               boxprops=dict(facecolor="#FEF3C7"),
               medianprops=dict(color=RED, linewidth=2))
    ax.set_ylabel(label); ax.set_title(f"{label} Distribution by Protocol")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
fig.suptitle("Protocol Comparison Boxplots")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp2_box_protocol.png"), dpi=150); plt.close()
print("✓ exp2_box_protocol.png")

# ════════════════════════════════════════════════════════════════════════════
# Exp 3: Stream limit mitigation
# ════════════════════════════════════════════════════════════════════════════

unlimited = defaultdict(lambda: defaultdict(list))
limited   = defaultdict(lambda: defaultdict(list))

with open(os.path.join(RESULTS_DIR, "exp3_stream_limits.csv")) as f:
    for row in csv.DictReader(f):
        n = int(row["n_streams"])
        bucket = unlimited if row["server_mode"] == "unlimited" else limited
        for c in ["amp_factor","wall_time_s","server_cpu_avg"]:
            try: bucket[n][c].append(float(row[c]))
            except: pass

ns3 = sorted(unlimited.keys())

fig, axes = plt.subplots(1, 2, figsize=(13,4))

for ax, (col, label) in zip(axes, [
    ("server_cpu_avg", "Server CPU (%)"),
    ("wall_time_s",    "Wall time (s)"),
]):
    um = [mean(unlimited[n][col]) for n in ns3]
    ue = [ci95(unlimited[n][col]) for n in ns3]
    lm = [mean(limited[n][col])   for n in ns3]
    le = [ci95(limited[n][col])   for n in ns3]
    ax.errorbar(ns3, um, yerr=ue, label="Unlimited", color=RED,   **LINE)
    ax.errorbar(ns3, lm, yerr=le, label="Limited (MAX=10)", color=GREEN, **LINE)
    ax.set_xlabel("N streams requested"); ax.set_ylabel(label)
    ax.set_title(f"{label}: Unlimited vs. Limited")
    ax.set_ylim(bottom=0); ax.legend()
    ax.set_xticks(ns3); ax.grid(True, linestyle="--", alpha=0.4)

fig.suptitle("Experiment 3: Stream Limit Mitigation (95% CI)")
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp3_stream_limits.png"), dpi=150); plt.close()
print("✓ exp3_stream_limits.png")

# Boxplot mitigation CPU
fig, ax = plt.subplots(figsize=(10,4))
positions_u = [i*2.5 for i in range(len(ns3))]
positions_l = [i*2.5+1 for i in range(len(ns3))]
bp1 = ax.boxplot([unlimited[n]["server_cpu_avg"] for n in ns3],
                 positions=positions_u, patch_artist=True, widths=0.7,
                 boxprops=dict(facecolor="#FEE2E2"), medianprops=dict(color=RED, linewidth=2))
bp2 = ax.boxplot([limited[n]["server_cpu_avg"] for n in ns3],
                 positions=positions_l, patch_artist=True, widths=0.7,
                 boxprops=dict(facecolor="#DCFCE7"), medianprops=dict(color=GREEN, linewidth=2))
ax.set_xticks([i*2.5+0.5 for i in range(len(ns3))])
ax.set_xticklabels(ns3)
ax.set_xlabel("N streams requested"); ax.set_ylabel("Server CPU (%)")
ax.set_title("Server CPU Distribution: Unlimited vs. Limited (Boxplot)")
ax.legend([bp1["boxes"][0], bp2["boxes"][0]], ["Unlimited","Limited (MAX=10)"])
ax.grid(True, axis="y", linestyle="--", alpha=0.4)
fig.tight_layout()
fig.savefig(os.path.join(RESULTS_DIR, "exp3_box_cpu.png"), dpi=150); plt.close()
print("✓ exp3_box_cpu.png")

# ════════════════════════════════════════════════════════════════════════════
# Exp 4: Benign client latency under attack
# ════════════════════════════════════════════════════════════════════════════

exp4_path = os.path.join(RESULTS_DIR, "exp4_benign_client.csv")
if os.path.exists(exp4_path):
    d4 = defaultdict(list)
    with open(exp4_path) as f:
        for row in csv.DictReader(f):
            try: d4[int(row["n_attack_streams"])].append(float(row["benign_latency_s"]))
            except: pass

    ns4 = sorted(d4.keys())

    # Line plot with 95% CI
    fig, ax = plt.subplots(figsize=(8, 4))
    means4 = [mean(d4[n]) for n in ns4]
    cis4   = [ci95(d4[n]) for n in ns4]
    labels4 = ["Baseline\n(no attack)" if n == 0 else str(n) for n in ns4]
    ax.errorbar(range(len(ns4)), means4, yerr=cis4, color=RED, **LINE)
    ax.axhline(means4[0], linestyle="--", color="gray", alpha=0.6, label="Baseline latency")
    ax.set_xticks(range(len(ns4)))
    ax.set_xticklabels(labels4)
    ax.set_xlabel("N concurrent attack streams (separate connection)")
    ax.set_ylabel("Benign client latency (s)")
    ax.set_title("Benign Client Latency Under Attack (95% CI)")
    ax.legend(); ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp4_benign_latency.png"), dpi=150); plt.close()
    print("✓ exp4_benign_latency.png")

    # Boxplot
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.boxplot([d4[n] for n in ns4], labels=labels4, patch_artist=True,
               boxprops=dict(facecolor="#FEE2E2", color=RED),
               medianprops=dict(color="#991B1B", linewidth=2),
               whiskerprops=dict(color=RED), capprops=dict(color=RED),
               flierprops=dict(marker="o", color=RED, alpha=0.5))
    ax.axhline(mean(d4[0]), linestyle="--", color="gray", alpha=0.6, label="Baseline median")
    ax.set_xlabel("N concurrent attack streams")
    ax.set_ylabel("Benign client latency (s)")
    ax.set_title("Benign Client Latency Distribution Under Attack (Boxplot)")
    ax.legend(); ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp4_box_benign_latency.png"), dpi=150); plt.close()
    print("✓ exp4_box_benign_latency.png")
else:
    print("⚠ exp4_benign_client.csv not found — skipping Exp 4 plots")

# ════════════════════════════════════════════════════════════════════════════
# Exp 5: Approximate vs wire-level byte measurement
# ════════════════════════════════════════════════════════════════════════════

exp5_path = os.path.join(RESULTS_DIR, "exp5_packet_bytes.csv")
if os.path.exists(exp5_path):
    d5_approx  = defaultdict(list)
    d5_wire    = defaultdict(list)
    d5_overhead = defaultdict(list)
    with open(exp5_path) as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n_streams"])
                d5_approx[n].append(float(row["amp_factor_approx"]))
                d5_wire[n].append(float(row["amp_factor_wire"]))
                if "quic_overhead_pct" in row:
                    d5_overhead[n].append(float(row["quic_overhead_pct"]))
            except: pass

    ns5 = sorted(d5_approx.keys())

    # Plot 1: Approx vs Wire amplification
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.errorbar(ns5, [mean(d5_approx[n]) for n in ns5],
                yerr=[ci95(d5_approx[n]) for n in ns5],
                color=BLUE, label="HTTP-level (header sum)", **LINE)
    ax.errorbar(ns5, [mean(d5_wire[n]) for n in ns5],
                yerr=[ci95(d5_wire[n]) for n in ns5],
                color=RED, label="Wire-level (QUIC datagrams)", **LINE)
    ax.set_xlabel("N concurrent QUIC streams")
    ax.set_ylabel("Amplification factor (×)")
    ax.set_title("HTTP-Level vs. Wire-Level Amplification Factor")
    ax.set_xticks(ns5); ax.legend()
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp5_amp_approx_vs_wire.png"), dpi=150); plt.close()
    print("✓ exp5_amp_approx_vs_wire.png")

    # Plot 2: QUIC overhead % (attacker's extra cost)
    if d5_overhead:
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.errorbar(ns5, [mean(d5_overhead[n]) for n in ns5],
                    yerr=[ci95(d5_overhead[n]) for n in ns5],
                    color=GREEN, **LINE)
        ax.set_xlabel("N concurrent QUIC streams")
        ax.set_ylabel("QUIC overhead (%)")
        ax.set_title("QUIC Protocol Overhead vs. N Streams\n(extra bytes attacker must send beyond HTTP headers)")
        ax.set_xticks(ns5)
        ax.grid(True, linestyle="--", alpha=0.4)
        # Annotate the drop
        for n, y in zip(ns5, [mean(d5_overhead[n]) for n in ns5]):
            ax.annotate(f"{y:.0f}%", (n, y), textcoords="offset points",
                        xytext=(0, 8), ha="center", fontsize=8)
        fig.tight_layout()
        fig.savefig(os.path.join(RESULTS_DIR, "exp5_quic_overhead.png"), dpi=150); plt.close()
        print("✓ exp5_quic_overhead.png")
else:
    print("⚠ exp5_packet_bytes.csv not found — skipping Exp 5 plots")

# ════════════════════════════════════════════════════════════════════════════
# Exp 6: Latency sensitivity (wall time vs N at different RTTs)
# ════════════════════════════════════════════════════════════════════════════

exp6_path = os.path.join(RESULTS_DIR, "exp6_latency.csv")
if os.path.exists(exp6_path):
    from collections import OrderedDict
    d6 = {}   # rtt_ms → n → list of wall_time_s / amp_factor
    with open(exp6_path) as f:
        for row in csv.DictReader(f):
            try:
                rtt = int(row["rtt_target_ms"])
                n   = int(row["n_streams"])
                if rtt not in d6: d6[rtt] = {}
                if n   not in d6[rtt]: d6[rtt][n] = {"wall": [], "amp": []}
                d6[rtt][n]["wall"].append(float(row["wall_time_s"]))
                d6[rtt][n]["amp"].append(float(row["amp_factor"]))
            except: pass

    rtts   = sorted(d6.keys())
    ns6    = sorted(next(iter(d6.values())).keys())
    colors6 = ["#6b7280", "#3b82f6", "#f59e0b", "#ef4444"]
    labels6 = {0: "Baseline (0ms RTT)", 10: "10ms RTT", 50: "50ms RTT", 100: "100ms RTT"}

    # Wall time vs N for each RTT
    fig, ax = plt.subplots(figsize=(9, 5))
    for rtt, color in zip(rtts, colors6):
        means_w = [mean(d6[rtt][n]["wall"]) for n in ns6]
        cis_w   = [ci95(d6[rtt][n]["wall"]) for n in ns6]
        ax.errorbar(ns6, means_w, yerr=cis_w, color=color,
                    label=labels6.get(rtt, f"{rtt}ms RTT"), **LINE)
    ax.set_xlabel("N concurrent QUIC streams")
    ax.set_ylabel("Wall time (s)")
    ax.set_title("Response Time vs. N Streams at Different Simulated RTTs (95% CI)")
    ax.set_xticks(ns6); ax.legend(); ax.set_ylim(bottom=0)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp6_latency_wall.png"), dpi=150); plt.close()
    print("✓ exp6_latency_wall.png")

    # Boxplot: wall time at N=100 across RTTs
    fig, ax = plt.subplots(figsize=(8, 4))
    box_data   = [d6[rtt].get(100, {}).get("wall", [0]) for rtt in rtts]
    box_labels = [labels6.get(r, f"{r}ms") for r in rtts]
    ax.boxplot(box_data, labels=box_labels, patch_artist=True,
               boxprops=dict(facecolor="#DBEAFE", color=BLUE),
               medianprops=dict(color=RED, linewidth=2),
               whiskerprops=dict(color=BLUE), capprops=dict(color=BLUE),
               flierprops=dict(marker="o", color=BLUE, alpha=0.5))
    ax.set_xlabel("Simulated RTT"); ax.set_ylabel("Wall time (s)")
    ax.set_title("Response Time Distribution at N=100, Varying RTT (Boxplot)")
    ax.grid(True, axis="y", linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp6_box_latency.png"), dpi=150); plt.close()
    print("✓ exp6_box_latency.png")
else:
    print("⚠ exp6_latency.csv not found — skipping Exp 6 plots")

# ════════════════════════════════════════════════════════════════════════════
# Exp 7: Caddy vs aioquic comparison
# ════════════════════════════════════════════════════════════════════════════

exp7_path = os.path.join(RESULTS_DIR, "exp7_caddy.csv")
runs_path = os.path.join(RESULTS_DIR, "runs.csv")
if os.path.exists(exp7_path) and os.path.exists(runs_path):
    d7_caddy  = defaultdict(list)
    d7_aioquic = defaultdict(list)

    with open(exp7_path) as f:
        for row in csv.DictReader(f):
            try:
                d7_caddy[int(row["n_streams"])].append(float(row["amp_factor"]))
            except: pass

    with open(runs_path) as f:
        for row in csv.DictReader(f):
            try:
                d7_aioquic[int(row["n_streams"])].append(float(row["amp_factor"]))
            except: pass

    ns7 = sorted(d7_caddy.keys())

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.errorbar(ns7, [mean(d7_aioquic[n]) for n in ns7],
                yerr=[ci95(d7_aioquic[n]) for n in ns7],
                color=BLUE, label="aioquic (Python)", **LINE)
    ax.errorbar(ns7, [mean(d7_caddy[n]) for n in ns7],
                yerr=[ci95(d7_caddy[n]) for n in ns7],
                color=GREEN, label="Caddy (quic-go / Go)", **LINE)
    ax.set_xlabel("N concurrent QUIC streams")
    ax.set_ylabel("Amplification factor (×)")
    ax.set_title("Amplification Factor: aioquic vs. Caddy (quic-go)\n"
                 "Confirms finding is protocol-level, not implementation-specific")
    ax.set_xticks(ns7); ax.legend()
    ax.set_ylim(bottom=0)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp7_caddy_vs_aioquic.png"), dpi=150); plt.close()
    print("✓ exp7_caddy_vs_aioquic.png")
else:
    print("⚠ exp7_caddy.csv or runs.csv not found — skipping Exp 7 plot")

# ════════════════════════════════════════════════════════════════════════════
# Exp 8: Saturation study
# ════════════════════════════════════════════════════════════════════════════

exp8_path = os.path.join(RESULTS_DIR, "exp8_saturation.csv")
if os.path.exists(exp8_path):
    d8 = defaultdict(lambda: defaultdict(list))
    with open(exp8_path) as f:
        for row in csv.DictReader(f):
            try:
                n = int(row["n_streams"])
                d8[n]["cpu_peak"].append(float(row["server_cpu_peak"]))
                d8[n]["mem_peak"].append(float(row["server_mem_mb_peak"]))
                d8[n]["fail"].append(float(row["fail_rate_pct"]))
                d8[n]["wall"].append(float(row["wall_time_s"]))
            except: pass

    ns8 = sorted(d8.keys())

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))

    # Wall time
    ax = axes[0][0]
    ax.errorbar(ns8, [mean(d8[n]["wall"]) for n in ns8],
                yerr=[ci95(d8[n]["wall"]) for n in ns8],
                color=BLUE, **LINE)
    ax.set_xlabel("N streams"); ax.set_ylabel("Wall time (s)")
    ax.set_title("Wall Time vs. N (Saturation)"); ax.set_xticks(ns8)
    ax.grid(True, linestyle="--", alpha=0.4)

    # CPU peak
    ax = axes[0][1]
    ax.errorbar(ns8, [mean(d8[n]["cpu_peak"]) for n in ns8],
                yerr=[ci95(d8[n]["cpu_peak"]) for n in ns8],
                color=RED, **LINE)
    ax.set_xlabel("N streams"); ax.set_ylabel("CPU peak (%)")
    ax.set_title("CPU Peak vs. N (Saturation)"); ax.set_xticks(ns8)
    ax.grid(True, linestyle="--", alpha=0.4)

    # Memory peak
    ax = axes[1][0]
    ax.errorbar(ns8, [mean(d8[n]["mem_peak"]) for n in ns8],
                yerr=[ci95(d8[n]["mem_peak"]) for n in ns8],
                color=GREEN, **LINE)
    ax.set_xlabel("N streams"); ax.set_ylabel("Memory peak (MB)")
    ax.set_title("Memory Peak vs. N (Saturation)"); ax.set_xticks(ns8)
    ax.grid(True, linestyle="--", alpha=0.4)

    # Failure rate
    ax = axes[1][1]
    ax.bar(ns8, [mean(d8[n]["fail"]) for n in ns8],
           color=[RED if mean(d8[n]["fail"]) > 5 else BLUE for n in ns8],
           alpha=0.8, width=80)
    ax.axhline(10, linestyle="--", color="gray", alpha=0.6, label="10% failure threshold")
    ax.set_xlabel("N streams"); ax.set_ylabel("Stream failure rate (%)")
    ax.set_title("Stream Failure Rate vs. N"); ax.set_xticks(ns8)
    ax.legend(); ax.grid(True, axis="y", linestyle="--", alpha=0.4)

    fig.suptitle("Experiment 8: Saturation Study — Service Collapse at N=2000",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp8_saturation.png"), dpi=150); plt.close()
    print("✓ exp8_saturation.png")
else:
    print("⚠ exp8_saturation.csv not found — skipping Exp 8 plot")

# ════════════════════════════════════════════════════════════════════════════
# Exp 9: Mitigation tradeoff — benign latency under different stream limits
# ════════════════════════════════════════════════════════════════════════════

exp9_path = os.path.join(RESULTS_DIR, "exp9_mitigation_tradeoff.csv")
if os.path.exists(exp9_path):
    d9 = {}   # limit_label → n_attack → list of benign_latency_s
    with open(exp9_path) as f:
        for row in csv.DictReader(f):
            try:
                lim = row["max_streams_limit"]
                n   = int(row["n_attack"])
                lat = float(row["benign_latency_s"])
                if lim not in d9: d9[lim] = {}
                if n   not in d9[lim]: d9[lim][n] = []
                d9[lim][n].append(lat)
            except: pass

    limits     = ["unlimited", "MAX=10", "MAX=20", "MAX=50"]
    colors9    = ["#6b7280", "#ef4444", "#f59e0b", "#3b82f6"]
    ns9        = sorted(next(iter(d9.values())).keys())

    fig, ax = plt.subplots(figsize=(9, 5))
    for lim, color in zip(limits, colors9):
        if lim not in d9: continue
        means9 = [mean(d9[lim][n]) * 1000 for n in ns9]   # convert to ms
        cis9   = [ci95(d9[lim][n]) * 1000 for n in ns9]
        ax.errorbar(ns9, means9, yerr=cis9, color=color,
                    label=lim, **LINE)

    ax.set_xlabel("N concurrent attack streams")
    ax.set_ylabel("Benign client latency (ms)")
    ax.set_title("Experiment 9: Benign Client Latency Under Different Stream Limits\n"
                 "(95% CI — all configurations maintained 100% benign success rate)")
    ax.set_xticks(ns9); ax.legend()
    ax.set_ylim(bottom=0)
    ax.grid(True, linestyle="--", alpha=0.4)
    fig.tight_layout()
    fig.savefig(os.path.join(RESULTS_DIR, "exp9_mitigation_tradeoff.png"), dpi=150); plt.close()
    print("✓ exp9_mitigation_tradeoff.png")
else:
    print("⚠ exp9_mitigation_tradeoff.csv not found — skipping Exp 9 plot")

print(f"\nAll plots saved to {RESULTS_DIR}")
