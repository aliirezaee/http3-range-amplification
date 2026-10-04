"""
Experiment 5: Accurate wire-level byte measurement.

Intercepts the aioquic UDP transport to count actual bytes sent/received
at the QUIC datagram level — including QUIC framing, encryption, and
connection setup overhead. Provides the formal wire-level amplification.

Changes per professor feedback (18.pdf):
  - Added formal metrics per 18.pdf:
      Amp_wire(N)     = Bytes_response / Bytes_request  (wire level)
      Cost_attacker   = Bytes_sent / Bytes_received
      Efficiency(N)   = Amp_wire(N) / CPU_server(N)
  - Added: streams_ok, streams_failed, fail_rate_pct, throughput_rps
  - Added: per_stream_mem_mb

The professor notes: Amp_wire(N) → Amp_HTTP as N → ∞
This is computed and stored as amp_convergence_gap = Amp_HTTP - Amp_wire(N)

N values: {1, 5, 10, 20, 50, 100}, 10 repeats each.
Writes: results/exp5_packet_bytes.csv
"""

import asyncio, csv, os, ssl, time, uuid
from datetime import datetime
import psutil
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

HOST = "127.0.0.1"
PORT = 4433
STREAM_COUNTS = [1, 5, 10, 20, 50, 100]
REPEATS = 10
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "exp5_packet_bytes.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")

# HTTP-level amplification (constant) = 10000 / 75
AMP_HTTP = 10000 / 75  # ≈ 133.33

COLUMNS = [
    "run_id", "timestamp", "n_streams",
    # HTTP-level
    "req_bytes_approx", "resp_bytes_http", "amp_factor_http",
    # Wire-level (formal per professor)
    "req_bytes_wire", "resp_bytes_wire",
    "amp_factor_wire",       # Amp_wire(N) = resp_wire / req_wire
    "cost_attacker",         # Cost_attacker = req_wire / resp_wire  (< 1 = attacker loses)
    "amp_convergence_gap",   # Amp_HTTP - Amp_wire(N)  → 0 as N → ∞
    "quic_overhead_pct",
    # Efficiency
    "efficiency_n",          # Efficiency(N) = Amp_wire(N) / CPU_server(N)
    # Failure + throughput
    "streams_ok", "streams_failed", "fail_rate_pct",
    "throughput_rps",
    "wall_time_s", "server_cpu_avg", "server_mem_mb_peak",
    "per_stream_mem_mb",
]


# ── Counting UDP transport wrapper ────────────────────────────────────────────

class CountingTransport:
    """Wraps asyncio UDP transport to count bytes sent at datagram level."""
    def __init__(self, transport):
        self._transport = transport
        self.bytes_sent = 0

    def sendto(self, data, addr=None):
        self.bytes_sent += len(data)
        if addr:
            self._transport.sendto(data, addr)
        else:
            self._transport.sendto(data)

    def __getattr__(self, name):
        return getattr(self._transport, name)


# ── H3 Client with byte counting ──────────────────────────────────────────────

class H3Client(QuicConnectionProtocol):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._h3 = None
        self._done: dict[int, asyncio.Event] = {}
        self._data: dict[int, bytes] = {}
        self.bytes_received = 0

    def connection_made(self, transport):
        self._counting_transport = CountingTransport(transport)
        super().connection_made(self._counting_transport)

    def datagram_received(self, data, addr):
        self.bytes_received += len(data)
        super().datagram_received(data, addr)

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic)
        for e in self._h3.handle_event(event):
            if isinstance(e, DataReceived):
                self._data[e.stream_id] = self._data.get(e.stream_id, b"") + e.data
                if e.stream_ended and e.stream_id in self._done:
                    self._done[e.stream_id].set()

    def send_req(self) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"), (b":scheme", b"https"),
            (b":authority", HOST.encode()), (b":path", b"/bigfile.bin"),
            (b"range", b"bytes=0-9999"),
        ]
        rb = sum(len(k) + len(v) for k, v in headers)
        self._done[sid] = asyncio.Event()
        self._data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, headers, end_stream=True)
        self.transmit()
        return sid, rb

    async def wait(self, sid: int):
        await asyncio.wait_for(self._done[sid].wait(), timeout=15.0)
        return self._data.get(sid, b"")

    @property
    def wire_sent(self):
        try: return self._counting_transport.bytes_sent
        except: return 0


# ── psutil sampler ────────────────────────────────────────────────────────────

class Sampler:
    def __init__(self, pid):
        self._p = psutil.Process(pid)
        self._cpu = []; self._mem = 0.0; self._on = False
    async def start(self):
        self._cpu.clear(); self._mem = 0.0; self._on = True
        asyncio.create_task(self._run())
    async def _run(self):
        self._p.cpu_percent(interval=None)
        while self._on:
            self._cpu.append(self._p.cpu_percent(interval=None))
            m = self._p.memory_info().rss / 1_048_576
            if m > self._mem: self._mem = m
            await asyncio.sleep(0.1)
    async def stop(self):
        self._on = False; await asyncio.sleep(0.15)
        return (sum(self._cpu) / len(self._cpu) if self._cpu else 0.0), self._mem


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n: int, sampler: Sampler) -> dict:
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE

    await sampler.start()
    t0 = time.perf_counter()

    async with connect(HOST, PORT, configuration=cfg,
                       create_protocol=H3Client, wait_connected=True) as c:
        total_req_approx = 0
        sids = []
        for _ in range(n):
            sid, rb = c.send_req()
            sids.append(sid)
            total_req_approx += rb
        results = await asyncio.gather(*[c.wait(s) for s in sids],
                                       return_exceptions=True)
        wire_sent = c.wire_sent
        wire_recv = c.bytes_received

    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    streams_ok     = sum(1 for r in results if isinstance(r, bytes))
    streams_failed = sum(1 for r in results if not isinstance(r, bytes))
    fail_rate_pct  = (streams_failed / n * 100) if n else 0.0
    throughput_rps = streams_ok / wall if wall > 0 else 0.0

    resp_http    = sum(len(r) for r in results if isinstance(r, bytes))
    amp_http_obs = resp_http / total_req_approx if total_req_approx else 0.0

    # Formal wire-level metrics (per professor 18.pdf)
    amp_wire            = wire_recv / wire_sent if wire_sent else 0.0
    cost_attacker       = wire_sent / wire_recv if wire_recv else 0.0   # < 1 means attack is profitable
    amp_convergence_gap = AMP_HTTP - amp_wire                            # → 0 as N → ∞
    efficiency_n        = amp_wire / cpu if cpu > 0 else 0.0            # Amp_wire / CPU_server

    overhead_pct = ((wire_sent - total_req_approx) / total_req_approx * 100
                    if total_req_approx else 0.0)

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "n_streams": n,
        "req_bytes_approx": total_req_approx,
        "resp_bytes_http": resp_http,
        "amp_factor_http": round(amp_http_obs, 4),
        "req_bytes_wire": wire_sent,
        "resp_bytes_wire": wire_recv,
        "amp_factor_wire": round(amp_wire, 4),
        "cost_attacker": round(cost_attacker, 4),
        "amp_convergence_gap": round(amp_convergence_gap, 4),
        "quic_overhead_pct": round(overhead_pct, 2),
        "efficiency_n": round(efficiency_n, 6),
        "streams_ok": streams_ok,
        "streams_failed": streams_failed,
        "fail_rate_pct": round(fail_rate_pct, 2),
        "throughput_rps": round(throughput_rps, 4),
        "wall_time_s": round(wall, 6),
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
        "per_stream_mem_mb": round(mem / n, 4) if n else 0.0,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    with open(PID_FILE) as f:
        pid = int(f.read())
    sampler = Sampler(pid)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for n in STREAM_COUNTS:
            for rep in range(1, REPEATS + 1):
                print(f"  N={n:3d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await run_once(n, sampler)
                w.writerow(row); f.flush()
                print(f"wire={row['amp_factor_wire']:.2f}x  "
                      f"cost_atk={row['cost_attacker']:.3f}  "
                      f"gap={row['amp_convergence_gap']:.1f}  "
                      f"eff={row['efficiency_n']:.4f}")

    print(f"\nSaved → {CSV_PATH}")
    print(f"\n{'N':>5}  {'Amp_wire':>10}  {'Cost_atk':>10}  {'Conv.gap':>10}  {'Eff(N)':>10}  {'QUIC overhead':>14}")
    import statistics as st
    d = {}
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            n = int(row['n_streams'])
            if n not in d: d[n] = {'w':[],'c':[],'g':[],'e':[],'o':[]}
            d[n]['w'].append(float(row['amp_factor_wire']))
            d[n]['c'].append(float(row['cost_attacker']))
            d[n]['g'].append(float(row['amp_convergence_gap']))
            d[n]['e'].append(float(row['efficiency_n']))
            d[n]['o'].append(float(row['quic_overhead_pct']))
    for n in sorted(d):
        print(f"{n:>5}  {st.mean(d[n]['w']):>10.2f}x  "
              f"{st.mean(d[n]['c']):>10.3f}  "
              f"{st.mean(d[n]['g']):>10.1f}  "
              f"{st.mean(d[n]['e']):>10.4f}  "
              f"{st.mean(d[n]['o']):>13.0f}%")

if __name__ == "__main__":
    asyncio.run(main())
