# HTTP/3 Range Amplification

**Experimental testbed and results for stream-based HTTP Range amplification over HTTP/3 and QUIC.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Python](https://img.shields.io/badge/python-3.x-blue)
![Status](https://img.shields.io/badge/status-research-green)

This repository contains the full, reproducible testbed behind an MSc thesis
(*Experimental Analysis of Stream-Based HTTP Range Amplification in HTTP/3 over QUIC*,
University of Genova) that measures how a small, perfectly legitimate HTTP Range
request can be turned into a resource-amplification vector when it is multiplexed
across many QUIC streams on a single authenticated HTTP/3 connection.

---

## What this is about

An HTTP **Range request** lets a client ask for only part of a file (e.g. `bytes=0-9999`).
The request headers are tiny (~75 B) but the response is large (~10 kB), so the server
returns far more than it receives. On HTTP/3 a single connection can carry **many
concurrent streams**, so one client can issue many of these requests at once while
paying the connection-handshake cost only **once**.

The headline application-layer factor (**133.3× = 10,000 / 75**) is deterministic and is
*not* the contribution. The contribution is the **wire-level convergence effect**: when
the real bytes on the wire are measured, amplification **grows with stream concurrency**
— from **4.21× at N=1 to 80.01× at N=100** — approaching a transport-aware ceiling of
**≈ 98×** (below 133.3×, because each stream really costs ~102 B on the wire once QUIC
framing and acknowledgements are counted). A closed-form model and an attacker-cost
analysis accompany the measurements.

This extends prior work (RangeAmp, DSN 2020), which studied HTTP/1.1 and HTTP/2 in
CDN-chain topologies, to **HTTP/3 + QUIC stream multiplexing on a single connection** —
to the best of our knowledge, this combination had not been previously characterised.

---

## Key results

| Area | Finding |
|---|---|
| **HTTP-level factor** | **133.3×** (= 10,000 / 75), constant at every stream count — an application-layer upper reference |
| **Wire-level amplification (Exp 5)** | **4.21× (N=1) → 80.01× (N=100)**; grows toward a ceiling **R/r′ ≈ 98×** as handshake cost is amortised |
| **Attacker cost (Exp 5)** | `Cost = 1/Amp_wire` is **below 1 at every N** (0.238 → 0.013) — bandwidth-profitable even from a single stream |
| **Efficiency (Exp 5)** | `Amp_wire / CPU` peaks at **N ≈ 20** (≈ 3.0) — most cost-effective attack size |
| **Resource scaling (main sweep)** | server CPU 4.8% → 41.4% (N=1→100) from one client; throughput plateaus at N≈50–100 |
| **Saturation (Exp 8)** | 0% failures up to N=1000 (CPU ≈ 86%), sharp cliff to ≈ 96.9% at N=2000 — consistent with single-threaded event-loop saturation |
| **Cross-implementation (Exp 7)** | Caddy (quic-go / Go) reproduces the same 133.3× — the application-layer factor is protocol-level, not aioquic-specific |
| **Protocol comparison (Exp 2)** | HTTP/1.1 ≈ 138.9×, HTTP/3 = 133.3× (HTTP/2 via RangeAmp literature, not measured here) |
| **Mitigation (Exp 3 / 9)** | per-connection stream limit caps server CPU at ~3 ms added latency and 100% benign success |

> **Scope note.** The amplification factor and the wire-level convergence are *structural*
> (protocol-level). The resource-scaling and saturation numbers are *implementation-specific*
> — they characterise the tested single-threaded `aioquic` server on a loopback testbed and
> would shift on a multi-threaded production server (Go/Rust) or a real network path.

---

## Formal metrics

```
AF_HTTP        = resp_bytes / req_bytes = 10000 / 75 = 133.3×   (constant)
Amp_wire(N)    = wire_recv / wire_sent                           (grows 4.21× → 80.01×)
               ≈ N·R / (H + N·r')     [first-order model]        (fit: H≈2.3 kB, r'≈102 B, R=10 kB)
Cost_attacker  = wire_sent / wire_recv = 1 / Amp_wire            (< 1 ⇒ bandwidth-profitable)
Efficiency(N)  = Amp_wire(N) / CPU_server(N)                     (peaks at N≈20 ≈ 3.0)
ceiling        = R / r' ≈ 98×                                    (below AF_HTTP = 133.3×)
```

---

## Repository structure

```
Code/
  server.py                    aioquic HTTP/3 server (port 4433)
  run_experiment.py            main sweep: N ∈ {1,5,10,20,50,100}, 10 repeats
  exp1_range_size.py           amplification vs. requested Range size
  exp2_protocol_comparison.py  HTTP/1.1 vs HTTP/3 (HTTP/2 via literature)
  exp3_stream_limits.py        unlimited vs. per-connection MAX_STREAMS (HTTP 429)
  exp4_benign_client.py        latency of a benign client during an attack
  exp5_packet_bytes.py         wire-level QUIC datagram byte accounting (core result)
  exp6_latency.py              RTT sensitivity via a UDP delay proxy
  exp7_caddy.py                cross-implementation validation against Caddy/quic-go
  exp8_saturation.py           saturation sweep: N ∈ {100,200,500,1000,2000}
  exp9_mitigation_tradeoff.py  stream-limit vs. benign-latency tradeoff
  attacker_cost_model.py       closed-form Amp_wire(N) / cost model
  analyze_statistics.py        t-test, Mann-Whitney U, Kruskal-Wallis, ANOVA
  plot_all.py / plot_results.py  figures from the CSVs
  make_bigfile.py              generates the 10 kB static target file
  Caddyfile                    Caddy config for the Exp 7 validation server

results/                       raw result CSVs (tracked for reproducibility)
```

---

## Requirements

- Python 3.9+
- [`aioquic`](https://github.com/aiortc/aioquic), `httpx`, `psutil`, `numpy`, `scipy`, `matplotlib`
- [Caddy](https://caddyserver.com/) v2.11+ (only for Exp 7)
- `openssl` (to generate the self-signed test certificate)

```bash
pip install aioquic httpx psutil numpy scipy matplotlib
```

## Setup

```bash
cd Code

# 1. generate the 10 kB static target file
python3 make_bigfile.py

# 2. generate a self-signed TLS cert for the local test server (NOT committed)
openssl req -x509 -newkey rsa:2048 -nodes -keyout key.pem -out cert.pem \
        -days 365 -subj "/CN=localhost"
```

## Running the experiments

```bash
# Terminal 1 — start the server (keep running)
cd Code
python3 server.py --host 127.0.0.1 --port 4433 --cert cert.pem --key key.pem

# Terminal 2 — run the experiments
python3 run_experiment.py
python3 exp1_range_size.py
python3 exp2_protocol_comparison.py
python3 exp3_stream_limits.py
python3 exp4_benign_client.py
python3 exp5_packet_bytes.py
python3 exp6_latency.py
python3 exp8_saturation.py
python3 exp9_mitigation_tradeoff.py

# Exp 7 needs Caddy running in a separate terminal:
caddy run --config Code/Caddyfile
python3 exp7_caddy.py

# Analysis + figures
python3 analyze_statistics.py
python3 plot_all.py
```

The testbed runs entirely on the loopback interface (127.0.0.1) on a single machine — no
external hosts are involved. Raw outputs are written to `results/` (CSVs are tracked; PNG
figures are regenerated by `plot_all.py`).

---

## Responsible use

This is **defensive security research**. The technique relies only on two public,
standardised features — QUIC stream multiplexing and the HTTP Range mechanism — and no
software vulnerability, exploit, or zero-day. All experiments were run against a local,
self-hosted server on the loopback interface; **nothing here should be run against systems
you do not own or operate.** The repository is published together with an evaluated
mitigation (per-connection stream limiting) so operators can act on the findings.

**Operator takeaways:** on HTTP/3 services that serve byte-range resources, set a
per-connection stream limit, monitor the per-connection response-to-request byte ratio,
and treat stream concurrency as a security-relevant configuration parameter.

---

## Citation

If you use this testbed or its results, please cite the thesis:

```bibtex
@mastersthesis{rezaee2026http3rangeamp,
  author = {Ali Rezaee},
  title  = {Experimental Analysis of Stream-Based HTTP Range Amplification in HTTP/3 over QUIC},
  school = {University of Genova},
  year   = {2026}
}
```

## Acknowledgements

MSc thesis supervised by Prof. Alessandro Carrega, University of Genova
(MSc in Engineering Technology for Strategy and Security — STRATEGOS).

## License

[MIT](LICENSE) © Ali Rezaee
