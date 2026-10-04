"""
Experiment 4: Benign client latency under attack.

Runs N attack streams on one QUIC connection while simultaneously
a benign client sends a single normal request on a SEPARATE connection.
Measures benign client latency with and without attack.

N_ATTACK values: 0 (baseline), 10, 20, 50, 100
10 repeats each.
Writes: results/exp4_benign_client.csv
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

HOST = "127.0.0.1"
PORT = 4433
ATTACK_LEVELS = [0, 10, 20, 50, 100]  # 0 = baseline (no attack)
REPEATS = 10
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "exp4_benign_client.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")
COLUMNS = ["run_id", "timestamp", "n_attack_streams", "benign_latency_s",
           "benign_success", "server_cpu_avg", "server_mem_mb_peak"]


# ── Shared H3 client ──────────────────────────────────────────────────────────

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

    def send_req(self, range_end: int = 9999) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"), (b":scheme", b"https"),
            (b":authority", HOST.encode()), (b":path", b"/bigfile.bin"),
            (b"range", f"bytes=0-{range_end}".encode()),
        ]
        rb = sum(len(k) + len(v) for k, v in headers)
        self._done[sid] = asyncio.Event()
        self._data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, headers, end_stream=True)
        self.transmit()
        return sid, rb

    async def wait(self, sid: int, timeout: float = 15.0):
        await asyncio.wait_for(self._done[sid].wait(), timeout=timeout)
        return self._data.get(sid, b""), self._status.get(sid, "?")


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


# ── Config ────────────────────────────────────────────────────────────────────

def make_config():
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE
    return cfg


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n_attack: int, sampler: Sampler) -> dict:
    await sampler.start()

    benign_latency = None
    benign_success = False

    if n_attack == 0:
        # Baseline: just benign client, no attack
        async with connect(HOST, PORT, configuration=make_config(),
                           create_protocol=H3Client, wait_connected=True) as benign:
            t0 = time.perf_counter()
            sid, _ = benign.send_req()
            try:
                _, status = await benign.wait(sid, timeout=10.0)
                benign_latency = time.perf_counter() - t0
                benign_success = (status == "206")
            except asyncio.TimeoutError:
                benign_latency = 10.0
                benign_success = False
    else:
        # Attack + benign run concurrently on separate connections
        async def attack_task():
            async with connect(HOST, PORT, configuration=make_config(),
                               create_protocol=H3Client, wait_connected=True) as attacker:
                sids = []
                for _ in range(n_attack):
                    sid, _ = attacker.send_req()
                    sids.append(sid)
                await asyncio.gather(*[attacker.wait(s) for s in sids],
                                     return_exceptions=True)

        async def benign_task():
            # Small delay to let attack streams start first
            await asyncio.sleep(0.02)
            async with connect(HOST, PORT, configuration=make_config(),
                               create_protocol=H3Client, wait_connected=True) as benign:
                t0 = time.perf_counter()
                sid, _ = benign.send_req()
                try:
                    _, status = await benign.wait(sid, timeout=10.0)
                    lat = time.perf_counter() - t0
                    return lat, (status == "206")
                except asyncio.TimeoutError:
                    return 10.0, False

        results = await asyncio.gather(attack_task(), benign_task(),
                                       return_exceptions=True)
        benign_result = results[1]
        if isinstance(benign_result, tuple):
            benign_latency, benign_success = benign_result
        else:
            benign_latency = 10.0
            benign_success = False

    cpu, mem = await sampler.stop()

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "n_attack_streams": n_attack,
        "benign_latency_s": round(benign_latency, 6),
        "benign_success": benign_success,
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
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

        for n in ATTACK_LEVELS:
            label = "baseline" if n == 0 else f"attack N={n}"
            for rep in range(1, REPEATS + 1):
                print(f"  {label:15s}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await run_once(n, sampler)
                w.writerow(row); f.flush()
                ok = "✓" if row["benign_success"] else "✗"
                print(f"{ok} latency={row['benign_latency_s']:.4f}s  cpu={row['server_cpu_avg']:.1f}%")

    print(f"\nSaved → {CSV_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
