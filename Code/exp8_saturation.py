"""
Experiment 8: Saturation Study.

Extends the main sweep to high N values to find the server saturation point —
where CPU maxes out, memory spikes, or stream failures begin occurring.

N values: {100, 200, 500, 1000, 2000}, 5 repeats each.
Stops early if: CPU > 95%, memory > 500 MB, or >10% stream failures.
Writes: results/exp8_saturation.csv
"""

import asyncio, csv, os, ssl, time, uuid
from datetime import datetime
import psutil
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

HOST          = "127.0.0.1"
PORT          = 4433
STREAM_COUNTS = [100, 200, 500, 1000, 2000]
REPEATS       = 5
RESULTS_DIR   = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH      = os.path.join(RESULTS_DIR, "exp8_saturation.csv")
PID_FILE      = os.path.join(os.path.dirname(__file__), "server.pid")

# Saturation thresholds — stop if any exceeded
# On macOS, psutil reports per-core %, so 800% = all 8 cores fully used
# Stop only on real saturation: stream failures or extreme CPU
CPU_LIMIT_PCT = 700.0   # near-full utilization of all cores
MEM_LIMIT_MB  = 1000.0  # 1 GB memory spike
FAIL_RATE_PCT = 10.0    # 10% stream failures = real service degradation

COLUMNS = [
    "run_id", "timestamp", "n_streams",
    "streams_ok", "streams_failed", "fail_rate_pct",
    "amp_factor",
    "wall_time_s",
    "server_cpu_avg", "server_cpu_peak",
    "server_mem_mb_avg", "server_mem_mb_peak",
    "saturation_hit",
]


# ── H3 Client ─────────────────────────────────────────────────────────────────

class H3Client(QuicConnectionProtocol):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._h3 = None
        self._done: dict[int, asyncio.Event] = {}
        self._data: dict[int, bytes] = {}
        self._status: dict[int, str] = {}

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic)
        for e in self._h3.handle_event(event):
            if isinstance(e, HeadersReceived):
                self._status[e.stream_id] = dict(e.headers).get(b":status", b"?").decode()
                if e.stream_ended and e.stream_id in self._done:
                    self._done[e.stream_id].set()
            elif isinstance(e, DataReceived):
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
        try:
            await asyncio.wait_for(self._done[sid].wait(), timeout=60.0)
            return self._data.get(sid, b""), self._status.get(sid, "?")
        except asyncio.TimeoutError:
            return b"", "timeout"


# ── psutil sampler ────────────────────────────────────────────────────────────

class Sampler:
    def __init__(self, pid):
        self._p = psutil.Process(pid)
        self._cpu = []; self._mem = []; self._on = False

    async def start(self):
        self._cpu.clear(); self._mem.clear(); self._on = True
        asyncio.create_task(self._run())

    async def _run(self):
        self._p.cpu_percent(interval=None)
        while self._on:
            self._cpu.append(self._p.cpu_percent(interval=None))
            self._mem.append(self._p.memory_info().rss / 1_048_576)
            await asyncio.sleep(0.1)

    async def stop(self):
        self._on = False
        await asyncio.sleep(0.15)
        import statistics as st
        cpu_avg  = st.mean(self._cpu) if self._cpu else 0
        cpu_peak = max(self._cpu) if self._cpu else 0
        mem_avg  = st.mean(self._mem) if self._mem else 0
        mem_peak = max(self._mem) if self._mem else 0
        return round(cpu_avg, 2), round(cpu_peak, 2), round(mem_avg, 2), round(mem_peak, 2)


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n: int, sampler: Sampler) -> dict:
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE

    await sampler.start()
    t0 = time.perf_counter()

    try:
        async with connect(HOST, PORT, configuration=cfg,
                           create_protocol=H3Client, wait_connected=True) as c:
            sids = []
            total_req = 0
            for _ in range(n):
                sid, rb = c.send_req()
                sids.append(sid); total_req += rb
            results = await asyncio.gather(*[c.wait(s) for s in sids],
                                           return_exceptions=True)
    except Exception as e:
        cpu_avg, cpu_peak, mem_avg, mem_peak = await sampler.stop()
        return {
            "run_id": str(uuid.uuid4())[:8],
            "timestamp": datetime.utcnow().isoformat(),
            "n_streams": n,
            "streams_ok": 0, "streams_failed": n,
            "fail_rate_pct": 100.0,
            "amp_factor": 0,
            "wall_time_s": round(time.perf_counter() - t0, 6),
            "server_cpu_avg": cpu_avg, "server_cpu_peak": cpu_peak,
            "server_mem_mb_avg": mem_avg, "server_mem_mb_peak": mem_peak,
            "saturation_hit": f"connection_error: {e}",
        }

    wall = time.perf_counter() - t0
    cpu_avg, cpu_peak, mem_avg, mem_peak = await sampler.stop()

    ok      = sum(1 for r in results if isinstance(r, tuple) and r[1] == "206")
    failed  = sum(1 for r in results if not isinstance(r, tuple) or r[1] != "206")
    resp    = sum(len(r[0]) for r in results if isinstance(r, tuple) and r[1] == "206")
    amp     = resp / total_req if total_req else 0
    fail_rt = (failed / n * 100) if n else 0

    # Check saturation
    sat = []
    if cpu_peak >= CPU_LIMIT_PCT:  sat.append(f"cpu_peak={cpu_peak:.0f}%")
    if mem_peak >= MEM_LIMIT_MB:   sat.append(f"mem={mem_peak:.0f}MB")
    if fail_rt  >= FAIL_RATE_PCT:  sat.append(f"fail_rate={fail_rt:.1f}%")
    saturation_hit = ", ".join(sat) if sat else "none"

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "n_streams": n,
        "streams_ok": ok, "streams_failed": failed,
        "fail_rate_pct": round(fail_rt, 2),
        "amp_factor": round(amp, 4),
        "wall_time_s": round(wall, 6),
        "server_cpu_avg": cpu_avg, "server_cpu_peak": cpu_peak,
        "server_mem_mb_avg": mem_avg, "server_mem_mb_peak": mem_peak,
        "saturation_hit": saturation_hit,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    with open(PID_FILE) as f:
        pid = int(f.read())
    sampler = Sampler(pid)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    saturated = False

    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for n in STREAM_COUNTS:
            if saturated:
                print(f"  [skip] N={n} — saturation already reached")
                continue

            for rep in range(1, REPEATS + 1):
                print(f"  N={n:4d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await run_once(n, sampler)
                w.writerow(row); f.flush()
                print(f"ok={row['streams_ok']}/{n}  "
                      f"cpu_peak={row['server_cpu_peak']:.0f}%  "
                      f"mem={row['server_mem_mb_peak']:.0f}MB  "
                      f"wall={row['wall_time_s']:.2f}s  "
                      f"sat={row['saturation_hit']}")

                if row["saturation_hit"] != "none":
                    print(f"\n  *** SATURATION HIT at N={n}: {row['saturation_hit']} ***")
                    saturated = True
                    break

            await asyncio.sleep(1.0)  # cooldown between N levels

    print(f"\nSaved → {CSV_PATH}")

    # Summary
    import statistics as st
    from collections import defaultdict
    d = defaultdict(lambda: defaultdict(list))
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            n = int(row["n_streams"])
            d[n]["cpu_peak"].append(float(row["server_cpu_peak"]))
            d[n]["mem_peak"].append(float(row["server_mem_mb_peak"]))
            d[n]["fail"].append(float(row["fail_rate_pct"]))
            d[n]["wall"].append(float(row["wall_time_s"]))

    print(f"\n{'N':>6} {'CPU peak':>10} {'Mem peak':>10} {'Fail%':>8} {'Wall(s)':>10}")
    for n in STREAM_COUNTS:
        if d[n]["cpu_peak"]:
            print(f"{n:>6} {st.mean(d[n]['cpu_peak']):>9.1f}%"
                  f" {st.mean(d[n]['mem_peak']):>9.1f}MB"
                  f" {st.mean(d[n]['fail']):>7.1f}%"
                  f" {st.mean(d[n]['wall']):>10.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
