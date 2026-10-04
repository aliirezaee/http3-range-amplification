"""
Experiment 6: Latency sensitivity — amplification under simulated RTT.

Since macOS loopback bypasses pfctl/dummynet, we use a Python UDP delay proxy:
  Client → port 4435 (proxy) → port 4433 (server)
  Each direction adds delay_ms/2, giving ~RTT ms round-trip time.

RTT targets: 0 ms (baseline), 10 ms, 50 ms, 100 ms
N values: {1, 5, 10, 20, 50, 100}, 10 repeats each.

Changes per professor feedback (18.pdf):
  - Added: streams_ok, streams_failed, fail_rate_pct
  - Added: throughput_rps (streams/sec) — key RTT efficiency metric
  - Added: cpu_per_delivered_byte — "CPU per delivered byte" as professor specified
  - Added: per_stream_mem_mb

Writes: results/exp6_latency.csv
Run WITHOUT sudo: python3 exp6_latency.py
"""

import asyncio, csv, os, socket, ssl, time, uuid
from datetime import datetime
import psutil
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

HOST        = "127.0.0.1"
SERVER_PORT = 4433
PROXY_PORT  = 4435
RTT_TARGETS_MS = [0, 10, 50, 100]
STREAM_COUNTS  = [1, 5, 10, 20, 50, 100]
REPEATS        = 10
RESULTS_DIR    = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH       = os.path.join(RESULTS_DIR, "exp6_latency.csv")
PID_FILE       = os.path.join(os.path.dirname(__file__), "server.pid")

COLUMNS = [
    "run_id", "timestamp", "rtt_target_ms", "n_streams",
    "amp_factor",
    "streams_ok", "streams_failed", "fail_rate_pct",
    "throughput_rps",         # completed streams / second
    "cpu_per_delivered_byte", # CPU% / bytes_delivered — efficiency under RTT
    "wall_time_s", "server_cpu_avg", "server_mem_mb_peak",
    "per_stream_mem_mb",
]


class DelayProxy:
    def __init__(self, one_way_s: float):
        self.one_way_s = one_way_s
        self._sock = None
        self._client_addr = None
        self._running = False

    async def start(self):
        loop = asyncio.get_event_loop()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((HOST, PROXY_PORT))
        self._sock.setblocking(False)
        self._running = True
        loop.add_reader(self._sock.fileno(), self._on_recv)

    def _on_recv(self):
        try:
            data, addr = self._sock.recvfrom(65536)
        except BlockingIOError:
            return
        if addr[1] == SERVER_PORT:
            if self._client_addr:
                asyncio.ensure_future(self._send(data, self._client_addr))
        else:
            self._client_addr = addr
            asyncio.ensure_future(self._send(data, (HOST, SERVER_PORT)))

    async def _send(self, data: bytes, dest: tuple):
        if self.one_way_s > 0:
            await asyncio.sleep(self.one_way_s)
        if self._sock and self._running:
            try:
                self._sock.sendto(data, dest)
            except OSError:
                pass

    def stop(self):
        self._running = False
        if self._sock:
            loop = asyncio.get_event_loop()
            try: loop.remove_reader(self._sock.fileno())
            except Exception: pass
            self._sock.close()
            self._sock = None


class H3Client(QuicConnectionProtocol):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._h3 = None
        self._done: dict[int, asyncio.Event] = {}
        self._data: dict[int, bytes] = {}

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

    async def wait(self, sid: int, rtt_ms: int = 0):
        timeout = max(30.0, 5 + rtt_ms * 0.5)
        await asyncio.wait_for(self._done[sid].wait(), timeout=timeout)
        return self._data.get(sid, b"")


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


async def run_once(n: int, rtt_ms: int, port: int, sampler: Sampler) -> dict:
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE

    await sampler.start()
    t0 = time.perf_counter()

    async with connect(HOST, port, configuration=cfg,
                       create_protocol=H3Client, wait_connected=True) as c:
        sids = []; total_req = 0
        for _ in range(n):
            sid, rb = c.send_req(); sids.append(sid); total_req += rb
        results = await asyncio.gather(
            *[c.wait(s, rtt_ms) for s in sids], return_exceptions=True)

    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    streams_ok     = sum(1 for r in results if isinstance(r, bytes))
    streams_failed = sum(1 for r in results if not isinstance(r, bytes))
    fail_rate_pct  = (streams_failed / n * 100) if n else 0.0
    throughput_rps = streams_ok / wall if wall > 0 else 0.0

    resp     = sum(len(r) for r in results if isinstance(r, bytes))
    amp      = resp / total_req if total_req else 0.0

    # CPU per delivered byte — measures attack efficiency under RTT (professor request)
    cpu_per_byte = cpu / resp if resp > 0 else 0.0

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "rtt_target_ms": rtt_ms,
        "n_streams": n,
        "amp_factor": round(amp, 4),
        "streams_ok": streams_ok,
        "streams_failed": streams_failed,
        "fail_rate_pct": round(fail_rate_pct, 2),
        "throughput_rps": round(throughput_rps, 4),
        "cpu_per_delivered_byte": round(cpu_per_byte, 8),
        "wall_time_s": round(wall, 6),
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
        "per_stream_mem_mb": round(mem / n, 4) if n else 0.0,
    }


async def main():
    with open(PID_FILE) as f:
        pid = int(f.read())
    sampler = Sampler(pid)

    os.makedirs(RESULTS_DIR, exist_ok=True)

    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for rtt in RTT_TARGETS_MS:
            one_way_s = (rtt / 2) / 1000.0
            label = "baseline (0ms RTT)" if rtt == 0 else f"RTT={rtt}ms"
            print(f"\n── {label} ──")

            if rtt == 0:
                port = SERVER_PORT; proxy = None
            else:
                proxy = DelayProxy(one_way_s)
                await proxy.start()
                await asyncio.sleep(0.1)
                port = PROXY_PORT

            try:
                for n in STREAM_COUNTS:
                    for rep in range(1, REPEATS + 1):
                        print(f"  N={n:3d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                        row = await run_once(n, rtt, port, sampler)
                        w.writerow(row); f.flush()
                        print(f"amp={row['amp_factor']:.1f}x  "
                              f"tput={row['throughput_rps']:.1f}rps  "
                              f"cpu/byte={row['cpu_per_delivered_byte']:.2e}")
            finally:
                if proxy:
                    proxy.stop(); await asyncio.sleep(0.1)

    print(f"\nSaved → {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
