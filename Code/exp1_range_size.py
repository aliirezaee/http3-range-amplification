"""
Experiment 1: Vary Range size at fixed N=10, 10 repeats.
Range sizes: 100, 500, 1000, 2000, 5000, 9999 bytes
Writes: results/exp1_range_size.csv

Changes per professor feedback (18.pdf):
  - Added: streams_ok, streams_failed, fail_rate_pct
  - Added: throughput_rps
  - Added: per_stream_mem_mb
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
N_STREAMS = 10
RANGE_SIZES = [100, 500, 1000, 2000, 5000, 9999]
REPEATS = 10
RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "exp1_range_size.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")
COLUMNS = [
    "run_id", "timestamp", "range_end_byte", "n_streams",
    "req_bytes_total", "resp_bytes_total", "amp_factor",
    "streams_ok", "streams_failed", "fail_rate_pct",
    "throughput_rps",
    "wall_time_s", "server_cpu_avg", "server_mem_mb_peak",
    "per_stream_mem_mb",
]


class H3Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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

    def send_request(self, range_end: int) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"),
            (b":scheme", b"https"),
            (b":authority", HOST.encode()),
            (b":path", b"/bigfile.bin"),
            (b"range", f"bytes=0-{range_end}".encode()),
        ]
        req_bytes = sum(len(k)+len(v) for k,v in headers)
        self._done[sid] = asyncio.Event()
        self._data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, headers, end_stream=True)
        self.transmit()
        return sid, req_bytes

    async def wait_stream(self, sid: int):
        await asyncio.wait_for(self._done[sid].wait(), timeout=15.0)
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
            m = self._p.memory_info().rss/1_048_576
            if m > self._mem: self._mem = m
            await asyncio.sleep(0.1)
    async def stop(self):
        self._on = False; await asyncio.sleep(0.15)
        return (sum(self._cpu)/len(self._cpu) if self._cpu else 0.0), self._mem


async def run_once(range_end: int, sampler: Sampler) -> dict:
    config = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    config.verify_mode = ssl.CERT_NONE
    await sampler.start()
    t0 = time.perf_counter()
    async with connect(HOST, PORT, configuration=config,
                       create_protocol=H3Client, wait_connected=True) as c:
        total_req = 0; sids = []
        for _ in range(N_STREAMS):
            sid, rb = c.send_request(range_end)
            sids.append(sid); total_req += rb
        results = await asyncio.gather(*[c.wait_stream(s) for s in sids],
                                       return_exceptions=True)
    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    streams_ok     = sum(1 for r in results if isinstance(r, bytes))
    streams_failed = sum(1 for r in results if not isinstance(r, bytes))
    fail_rate_pct  = (streams_failed / N_STREAMS * 100) if N_STREAMS else 0.0
    total_resp     = sum(len(r) for r in results if isinstance(r, bytes))
    amp            = total_resp / total_req if total_req else 0.0
    throughput_rps = streams_ok / wall if wall > 0 else 0.0
    per_stream_mem = mem / N_STREAMS if N_STREAMS else 0.0

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "range_end_byte": range_end,
        "n_streams": N_STREAMS,
        "req_bytes_total": total_req,
        "resp_bytes_total": total_resp,
        "amp_factor": round(amp, 4),
        "streams_ok": streams_ok,
        "streams_failed": streams_failed,
        "fail_rate_pct": round(fail_rate_pct, 2),
        "throughput_rps": round(throughput_rps, 4),
        "wall_time_s": round(wall, 6),
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
        "per_stream_mem_mb": round(per_stream_mem, 4),
    }


async def main():
    with open(PID_FILE) as f: pid = int(f.read())
    sampler = Sampler(pid)
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for r in RANGE_SIZES:
            for rep in range(1, REPEATS+1):
                print(f"  range=0-{r:5d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await run_once(r, sampler)
                w.writerow(row); f.flush()
                print(f"amp={row['amp_factor']:.1f}x  "
                      f"tput={row['throughput_rps']:.1f}rps  "
                      f"fail={row['fail_rate_pct']:.1f}%")
    print(f"\nSaved → {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
