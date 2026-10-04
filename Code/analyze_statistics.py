"""
Statistical significance analysis — run after all experiments complete.

Per professor feedback (18.pdf):
  - t-tests (Welch)
  - Mann-Whitney U tests
  - ANOVA / Kruskal-Wallis

Reads:  results/runs.csv, exp4_benign_client.csv, exp5_packet_bytes.csv,
        exp8_saturation.csv, exp9_mitigation_tradeoff.csv
Writes: results/stats_significance.csv
        results/stats_report.txt

Usage: python3 analyze_statistics.py
"""

import csv
import os
import warnings
from collections import defaultdict

import numpy as np
from scipy import stats

# Suppress all numpy RuntimeWarnings (empty-slice, invalid value, ddof<=0)
# that arise from groups with constant values or missing data.
warnings.filterwarnings("ignore", category=RuntimeWarning)
np.seterr(all="ignore")

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")


# ── Helpers ───────────────────────────────────────────────────────────────────

def load_csv(path: str) -> list:
    if not os.path.exists(path):
        print(f"  [skip] {os.path.basename(path)} not found")
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def group_by(rows: list, key: str, value_col: str) -> dict:
    groups = defaultdict(list)
    for r in rows:
        try:
            val = float(r[value_col])
            if np.isfinite(val):           # drop NaN and ±inf
                groups[r[key]].append(val)
        except (KeyError, ValueError):
            pass
    # Only keep groups with at least 2 valid observations
    return {k: v for k, v in groups.items() if len(v) >= 2}


def safe_float(x):
    try:
        return float(x)
    except (ValueError, TypeError):
        return x


# ── Statistical tests ──────────────────────────────────────────────────────────

def pairwise_ttests(groups: dict, label: str, out: list):
    """Welch t-test: baseline (smallest key) vs each other group."""
    keys = sorted(groups.keys(), key=safe_float)
    if len(keys) < 2:
        return
    baseline_key = keys[0]
    baseline = groups[baseline_key]
    for k in keys[1:]:
        other = groups[k]
        if len(baseline) < 2 or len(other) < 2:
            continue
        t_stat, p_val = stats.ttest_ind(baseline, other, equal_var=False)
        out.append({
            "test": "t-test (Welch)",
            "metric": label,
            "group_a": str(baseline_key),
            "group_b": str(k),
            "n_a": len(baseline),
            "n_b": len(other),
            "mean_a": round(float(np.mean(baseline)), 4),
            "mean_b": round(float(np.mean(other)), 4),
            "t_stat": round(t_stat, 4),
            "p_value": round(p_val, 6),
            "significant_p05": "YES" if p_val < 0.05 else "NO",
        })


def mann_whitney_tests(groups: dict, label: str, out: list):
    """Mann-Whitney U: baseline vs each other group (non-parametric)."""
    keys = sorted(groups.keys(), key=safe_float)
    if len(keys) < 2:
        return
    baseline_key = keys[0]
    baseline = groups[baseline_key]
    for k in keys[1:]:
        other = groups[k]
        if len(baseline) < 2 or len(other) < 2:
            continue
        u_stat, p_val = stats.mannwhitneyu(baseline, other, alternative='two-sided')
        out.append({
            "test": "Mann-Whitney U",
            "metric": label,
            "group_a": str(baseline_key),
            "group_b": str(k),
            "n_a": len(baseline),
            "n_b": len(other),
            "mean_a": round(float(np.mean(baseline)), 4),
            "mean_b": round(float(np.mean(other)), 4),
            "u_stat": round(u_stat, 4),
            "p_value": round(p_val, 6),
            "significant_p05": "YES" if p_val < 0.05 else "NO",
        })


def kruskal_wallis_test(groups: dict, label: str, out: list):
    """Kruskal-Wallis across all groups simultaneously."""
    arrays = [np.array(v) for v in groups.values() if len(v) >= 2]
    if len(arrays) < 2:
        return
    try:
        h_stat, p_val = stats.kruskal(*arrays)
    except ValueError:
        # All values identical — test undefined
        return
    out.append({
        "test": "Kruskal-Wallis",
        "metric": label,
        "group_a": "all",
        "group_b": "all",
        "n_a": sum(len(a) for a in arrays),
        "n_b": sum(len(a) for a in arrays),
        "mean_a": 0, "mean_b": 0,
        "h_stat": round(h_stat, 4),
        "p_value": round(p_val, 6),
        "significant_p05": "YES" if p_val < 0.05 else "NO",
    })


def anova_test(groups: dict, label: str, out: list):
    """One-way ANOVA across all groups."""
    arrays = [np.array(v) for v in groups.values() if len(v) >= 2]
    if len(arrays) < 2:
        return
    try:
        f_stat, p_val = stats.f_oneway(*arrays)
    except ValueError:
        return
    out.append({
        "test": "One-way ANOVA",
        "metric": label,
        "group_a": "all",
        "group_b": "all",
        "n_a": sum(len(a) for a in arrays),
        "n_b": sum(len(a) for a in arrays),
        "mean_a": 0, "mean_b": 0,
        "f_stat": round(f_stat, 4),
        "p_value": round(p_val, 6),
        "significant_p05": "YES" if p_val < 0.05 else "NO",
    })


def report_groups(groups: dict, report: list, label_fmt: str):
    for k in sorted(groups.keys(), key=safe_float):
        v = groups[k]
        report.append(label_fmt.format(k=k, mean=np.mean(v), std=np.std(v), n=len(v)))


# ── Per-experiment analysis ────────────────────────────────────────────────────

def analyze_runs(out_rows: list, report: list):
    rows = load_csv(os.path.join(RESULTS_DIR, "runs.csv"))
    if not rows:
        return
    report.append("=" * 60)
    report.append("MAIN SWEEP (runs.csv) — N ∈ {1,5,10,20,50,100}")
    report.append("=" * 60)
    for metric in ["wall_time_s", "server_cpu_avg", "throughput_rps", "server_mem_mb_peak"]:
        groups = group_by(rows, "n_streams", metric)
        if not groups:
            continue
        report.append(f"\n── {metric} vs N ──")
        report_groups(groups, report, "  N={k:>4}: mean={mean:.4f}  std={std:.4f}  n={n}")
        pairwise_ttests(groups, f"runs/{metric}", out_rows)
        mann_whitney_tests(groups, f"runs/{metric}", out_rows)
        kruskal_wallis_test(groups, f"runs/{metric}", out_rows)
        anova_test(groups, f"runs/{metric}", out_rows)
        kw = [r for r in out_rows if r.get("metric") == f"runs/{metric}" and r.get("test") == "Kruskal-Wallis"]
        if kw:
            report.append(f"  → Kruskal-Wallis p={kw[-1]['p_value']}  significant={kw[-1]['significant_p05']}")


def analyze_exp4(out_rows: list, report: list):
    rows = load_csv(os.path.join(RESULTS_DIR, "exp4_benign_client.csv"))
    if not rows:
        return
    report.append("\n" + "=" * 60)
    report.append("EXP 4 — Benign latency vs attack level")
    report.append("=" * 60)
    groups = group_by(rows, "n_attack_streams", "benign_latency_s")
    if not groups:
        return
    report_groups(groups, report, "  N_attack={k:>4}: mean={mean:.4f}s  std={std:.4f}  n={n}")
    pairwise_ttests(groups, "exp4/benign_latency_s", out_rows)
    mann_whitney_tests(groups, "exp4/benign_latency_s", out_rows)
    kruskal_wallis_test(groups, "exp4/benign_latency_s", out_rows)


def analyze_exp5(out_rows: list, report: list):
    rows = load_csv(os.path.join(RESULTS_DIR, "exp5_packet_bytes.csv"))
    if not rows:
        return
    report.append("\n" + "=" * 60)
    report.append("EXP 5 — Wire-level amp & Cost_attacker vs N")
    report.append("=" * 60)
    for metric in ["amp_factor_wire", "cost_attacker", "amp_convergence_gap"]:
        groups = group_by(rows, "n_streams", metric)
        if not groups:
            continue
        report.append(f"\n── {metric} vs N ──")
        report_groups(groups, report, "  N={k:>4}: mean={mean:.4f}  std={std:.4f}  n={n}")
        kruskal_wallis_test(groups, f"exp5/{metric}", out_rows)
        anova_test(groups, f"exp5/{metric}", out_rows)


def analyze_exp8(out_rows: list, report: list):
    rows = load_csv(os.path.join(RESULTS_DIR, "exp8_saturation.csv"))
    if not rows:
        return
    report.append("\n" + "=" * 60)
    report.append("EXP 8 — SATURATION STUDY (primary result)")
    report.append("=" * 60)
    for metric in ["fail_rate_pct", "wall_time_s", "server_cpu_avg"]:
        # Use min 1 for display (N=2000 has only 1 run due to early stop)
        all_groups = defaultdict(list)
        for r in rows:
            try:
                val = float(r[metric])
                if np.isfinite(val):
                    all_groups[r["n_streams"]].append(val)
            except (KeyError, ValueError):
                pass
        if not all_groups:
            continue
        report.append(f"\n── {metric} vs N ──")
        for k in sorted(all_groups.keys(), key=safe_float):
            v = all_groups[k]
            note = "  ← early stop" if len(v) < 5 else ""
            report.append(f"  N={k:>5}: mean={np.mean(v):.4f}  std={np.std(v):.4f}  n={len(v)}{note}")
        # Statistical tests only for groups with >= 2 observations
        test_groups = {k: v for k, v in all_groups.items() if len(v) >= 2}
        kruskal_wallis_test(test_groups, f"exp8/{metric}", out_rows)
        mann_whitney_tests(test_groups, f"exp8/{metric}", out_rows)


def analyze_exp9(out_rows: list, report: list):
    rows = load_csv(os.path.join(RESULTS_DIR, "exp9_mitigation_tradeoff.csv"))
    if not rows:
        return
    report.append("\n" + "=" * 60)
    report.append("EXP 9 — Mitigation tradeoff")
    report.append("=" * 60)
    for metric in ["benign_latency_s", "server_cpu_avg"]:
        groups = group_by(rows, "max_streams_limit", metric)
        if not groups:
            continue
        report.append(f"\n── {metric} vs MAX_STREAMS ──")
        for k in sorted(groups.keys(), key=lambda x: float('inf') if str(x) == '0' else safe_float(x)):
            v = groups[k]
            report.append(f"  MAX={k:>12}: mean={np.mean(v):.4f}  std={np.std(v):.4f}  n={len(v)}")
        pairwise_ttests(groups, f"exp9/{metric}", out_rows)
        mann_whitney_tests(groups, f"exp9/{metric}", out_rows)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    os.makedirs(RESULTS_DIR, exist_ok=True)

    out_rows: list = []
    report: list = [
        "STATISTICAL SIGNIFICANCE REPORT",
        "Generated by analyze_statistics.py",
        "",
    ]

    analyze_runs(out_rows, report)
    analyze_exp4(out_rows, report)
    analyze_exp5(out_rows, report)
    analyze_exp8(out_rows, report)
    analyze_exp9(out_rows, report)

    # Write CSV
    if out_rows:
        all_keys = set()
        for r in out_rows:
            all_keys.update(r.keys())
        base_cols = ["test", "metric", "group_a", "group_b", "n_a", "n_b",
                     "mean_a", "mean_b", "t_stat", "u_stat", "h_stat", "f_stat",
                     "p_value", "significant_p05"]
        cols = [c for c in base_cols if c in all_keys] + \
               [k for k in sorted(all_keys) if k not in base_cols]
        csv_out = os.path.join(RESULTS_DIR, "stats_significance.csv")
        with open(csv_out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(out_rows)
        print(f"Saved → {csv_out}")

    txt_out = os.path.join(RESULTS_DIR, "stats_report.txt")
    with open(txt_out, "w") as f:
        f.write("\n".join(report))
    print(f"Saved → {txt_out}")
    print()
    print("\n".join(report))


if __name__ == "__main__":
    main()
