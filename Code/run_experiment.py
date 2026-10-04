"""
Full experiment sweep — writes results/runs.csv
Usage: python run_experiment.py
Server must already be running.

Changes per professor feedback (18.pdf):
  - Added: streams_ok, streams_failed, fail_rate_pct  (FailureRate analysis)
  - Added: throughput_rps = completed streams / wall_time  (Throughput metric)
  - Added: per_stream_mem_mb = peak_rss / n_streams  (per-stream memory cost)
"""

import asyncio
import csv
import os
import ssl
import time
import uuid
from datetime import datetime

import psutil
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

# ── Config ────────────────────────────────────────────────────────────────────
HOST = "127.0.0.1"
PORT = 4433
STREAM_COUNTS = [1, 5, 10, 20, 50, 100]
REPEATS = 10
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "runs.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")
SAMPLE_INTERVAL = 0.1  # seconds

CSV_COLUMNS = [
    "run_id", "timestamp", "n_streams",
    "req_bytes_total", "resp_bytes_total", "amp_factor",
    "streams_ok", "streams_failed", "fail_rate_pct",
    "throughput_rps",
    "wall_time_s", "server_cpu_avg", "server_mem_mb_peak",
    "per_stream_mem_mb",
]


# ── H3 Client protocol ────────────────────────────────────────────────────────

class H3Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._h3 = None
        self._stream_done: dict[int, asyncio.Event] = {}
        self._stream_data: dict[int, bytes] = {}

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic)
        for h3_event in self._h3.handle_event(event):
            if isinstance(h3_event, DataReceived):
                sid = h3_event.stream_id
                self._stream_data[sid] = self._stream_data.get(sid, b"") + h3_event.data
                if h3_event.stream_ended and sid in self._stream_done:
                    self._stream_done[sid].set()

    def send_request(self, host: str) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"),
            (b":scheme", b"https"),
            (b":authority", host.encode()),
            (b":path", b"/bigfile.bin"),
            (b"range", b"bytes=0-9999"),
        ]
        req_bytes = sum(len(k) + len(v) for k, v in headers)
        self._stream_done[sid] = asyncio.Event()
        self._stream_data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, headers, end_stream=True)
        self.transmit()
        return sid, req_bytes

    async def wait_stream(self, stream_id: int, timeout: float = 15.0):
        await asyncio.wait_for(self._stream_done[stream_id].wait(), timeout=timeout)
        return self._stream_data.get(stream_id, b"")


# ── psutil sampler ────────────────────────────────────────────────────────────

class PsutilSampler:
    def __init__(self, pid: int):
        self._proc = psutil.Process(pid)
        self._cpu_samples = []
        self._mem_peak = 0.0
        self._running = False
        self._task = None

    async def start(self):
        self._cpu_samples.clear()
        self._mem_peak = 0.0
        self._running = True
        self._task = asyncio.create_task(self._sample())

    async def _sample(self):
        self._proc.cpu_percent(interval=None)  # prime
        while self._running:
            try:
                self._cpu_samples.append(self._proc.cpu_percent(interval=None))
                mem = self._proc.memory_info().rss / 1_048_576
                if mem > self._mem_peak:
                    self._mem_peak = mem
            except psutil.NoSuchProcess:
                break
            await asyncio.sleep(SAMPLE_INTERVAL)

    async def stop(self) -> tuple[float, float]:
        self._running = False
        if self._task:
            await self._task
        avg_cpu = sum(self._cpu_samples) / len(self._cpu_samples) if self._cpu_samples else 0.0
        return avg_cpu, self._mem_peak


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n_streams: int, sampler: PsutilSampler) -> dict:
    config = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    config.verify_mode = ssl.CERT_NONE

    await sampler.start()
    t_start = time.perf_counter()

    async with connect(HOST, PORT, configuration=config,
                       create_protocol=H3Client, wait_connected=True) as client:
        total_req = 0
        sids = []
        for _ in range(n_streams):
            sid, req_bytes = client.send_request(HOST)
            sids.append(sid)
            total_req += req_bytes

        results = await asyncio.gather(
            *[client.wait_stream(sid) for sid in sids],
            return_exceptions=True,
        )

    wall_time = time.perf_counter() - t_start
    cpu_avg, mem_peak = await sampler.stop()

    # ── Failure analysis (per professor feedback) ────────────────────────────
    streams_ok     = sum(1 for r in results if isinstance(r, bytes))
    streams_failed = sum(1 for r in results if not isinstance(r, bytes))
    fail_rate_pct  = (streams_failed / n_streams * 100) if n_streams else 0.0

    total_resp = sum(len(r) for r in results if isinstance(r, bytes))
    amp = total_resp / total_req if total_req else 0.0

    # ── Throughput (per professor feedback) ──────────────────────────────────
    throughput_rps = streams_ok / wall_time if wall_time > 0 else 0.0

    # ── Per-stream memory cost (per professor feedback) ───────────────────────
    per_stream_mem_mb = mem_peak / n_streams if n_streams else 0.0

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "n_streams": n_streams,
        "req_bytes_total": total_req,
        "resp_bytes_total": total_resp,
        "amp_factor": round(amp, 4),
        "streams_ok": streams_ok,
        "streams_failed": streams_failed,
        "fail_rate_pct": round(fail_rate_pct, 2),
        "throughput_rps": round(throughput_rps, 4),
        "wall_time_s": round(wall_time, 6),
        "server_cpu_avg": round(cpu_avg, 2),
        "server_mem_mb_peak": round(mem_peak, 2),
        "per_stream_mem_mb": round(per_stream_mem_mb, 4),
    }


# ── Main sweep ────────────────────────────────────────────────────────────────

async def main():
    if not os.path.exists(PID_FILE):
        print("ERROR: server.pid not found. Start the server first.")
        return
    with open(PID_FILE) as f:
        server_pid = int(f.read().strip())
    print(f"Monitoring server PID {server_pid}")

    sampler = PsutilSampler(server_pid)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    csv_file = open(CSV_PATH, "w", newline="")   # overwrite — fresh run each time
    writer = csv.DictWriter(csv_file, fieldnames=CSV_COLUMNS)
    writer.writeheader()

    total_runs = len(STREAM_COUNTS) * REPEATS
    done = 0

    for n in STREAM_COUNTS:
        for rep in range(1, REPEATS + 1):
            print(f"  N={n:3d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
            row = await run_once(n, sampler)
            writer.writerow(row)
            csv_file.flush()
            done += 1
            print(f"amp={row['amp_factor']:.1f}x  t={row['wall_time_s']:.4f}s  "
                  f"cpu={row['server_cpu_avg']:.1f}%  mem={row['server_mem_mb_peak']:.1f}MB  "
                  f"tput={row['throughput_rps']:.1f}rps  "
                  f"fail={row['fail_rate_pct']:.1f}%")

    csv_file.close()
    print(f"\nDone — {done} runs saved to {CSV_PATH}")


if __name__ == "__main__":
    asyncio.run(main())
