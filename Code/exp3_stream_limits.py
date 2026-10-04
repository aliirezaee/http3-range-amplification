"""
Experiment 3: Stream limit mitigation.
Compares unlimited server vs MAX_STREAMS=10 enforced server.
Fixed: Range bytes=0-9999, 3 repeats, N in {1,5,10,20,50,100}

How the limit works:
  - Server tracks open streams per connection
  - If N > limit: extra streams get HTTP 429, not served
  - Client counts both successful and rejected streams

Writes: results/exp3_stream_limits.csv
"""

import asyncio, csv, os, ssl, time, uuid
from datetime import datetime
import psutil
from aioquic.asyncio import connect, serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "exp3_stream_limits.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")
HOST = "127.0.0.1"
PORT_UNLIMITED = 4433   # existing server (no limit)
PORT_LIMITED   = 4434   # new server with MAX_STREAMS=10
MAX_STREAMS_LIMIT = 10
STREAM_COUNTS = [1, 5, 10, 20, 50, 100]
REPEATS = 10
COLUMNS = ["run_id","timestamp","server_mode","n_streams",
           "streams_completed","streams_rejected",
           "req_bytes_total","resp_bytes_total","amp_factor",
           "wall_time_s","server_cpu_avg","server_mem_mb_peak"]

BIGFILE_PATH = os.path.join(os.path.dirname(__file__), "bigfile.bin")
with open(BIGFILE_PATH, "rb") as _f:
    BIGFILE = _f.read()


# ── Limited server ────────────────────────────────────────────────────────────

class LimitedServerProtocol(QuicConnectionProtocol):
    def __init__(self, *a, **kw):
        super().__init__(*a, **kw)
        self._h3 = None
        self._open_streams = 0

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic, enable_webtransport=False)
        for e in self._h3.handle_event(event):
            if isinstance(e, HeadersReceived):
                self._handle_request(e.stream_id, e.headers)
                self.transmit()

    def _handle_request(self, stream_id, headers):
        if self._open_streams >= MAX_STREAMS_LIMIT:
            # Reject with 429
            self._h3.send_headers(stream_id,
                [(b":status", b"429"),
                 (b"x-reject-reason", b"stream-limit-exceeded")],
                end_stream=True)
            return
        self._open_streams += 1
        range_hdr = dict(headers).get(b"range", b"bytes=0-9999")
        try:
            parts = range_hdr.decode().replace("bytes=","").split("-")
            start, end = int(parts[0]), int(parts[1])
            end = min(end, len(BIGFILE)-1)
        except Exception:
            start, end = 0, len(BIGFILE)-1
        body = BIGFILE[start:end+1]
        self._h3.send_headers(stream_id,
            [(b":status", b"206"),
             (b"content-length", str(len(body)).encode()),
             (b"content-range", f"bytes {start}-{end}/{len(BIGFILE)}".encode())],
            end_stream=False)
        self._h3.send_data(stream_id, body, end_stream=True)
        self._open_streams -= 1


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

    def send_req(self, port_host=HOST) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        hdrs = [(b":method",b"GET"),(b":scheme",b"https"),
                (b":authority",HOST.encode()),(b":path",b"/bigfile.bin"),
                (b"range",b"bytes=0-9999")]
        rb = sum(len(k)+len(v) for k,v in hdrs)
        self._done[sid] = asyncio.Event()
        self._data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, hdrs, end_stream=True)
        self.transmit()
        return sid, rb

    async def wait(self, sid):
        await asyncio.wait_for(self._done[sid].wait(), timeout=15.0)
        return self._data.get(sid, b""), self._status.get(sid, "?")


# ── psutil sampler ────────────────────────────────────────────────────────────

class Sampler:
    def __init__(self, pid):
        self._p = psutil.Process(pid); self._cpu=[]; self._mem=0.0; self._on=False
    async def start(self):
        self._cpu.clear(); self._mem=0.0; self._on=True
        asyncio.create_task(self._run())
    async def _run(self):
        self._p.cpu_percent(interval=None)
        while self._on:
            self._cpu.append(self._p.cpu_percent(interval=None))
            m = self._p.memory_info().rss/1_048_576
            if m > self._mem: self._mem = m
            await asyncio.sleep(0.1)
    async def stop(self):
        self._on=False; await asyncio.sleep(0.15)
        return (sum(self._cpu)/len(self._cpu) if self._cpu else 0.0), self._mem


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n: int, port: int, mode: str, sampler: Sampler) -> dict:
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE
    await sampler.start()
    t0 = time.perf_counter()
    async with connect(HOST, port, configuration=cfg,
                       create_protocol=H3Client, wait_connected=True) as c:
        total_req = 0; sids = []
        for _ in range(n):
            sid, rb = c.send_req(); sids.append(sid); total_req += rb
        results = await asyncio.gather(*[c.wait(s) for s in sids], return_exceptions=True)
    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    completed = sum(1 for r in results if isinstance(r, tuple) and r[1] == "206")
    rejected  = sum(1 for r in results if isinstance(r, tuple) and r[1] == "429")
    total_resp = sum(len(r[0]) for r in results if isinstance(r, tuple) and r[1] == "206")
    amp = total_resp / total_req if total_req else 0

    return {"run_id": str(uuid.uuid4())[:8],
            "timestamp": datetime.utcnow().isoformat(),
            "server_mode": mode, "n_streams": n,
            "streams_completed": completed, "streams_rejected": rejected,
            "req_bytes_total": total_req, "resp_bytes_total": total_resp,
            "amp_factor": round(amp, 4), "wall_time_s": round(wall, 6),
            "server_cpu_avg": round(cpu, 2), "server_mem_mb_peak": round(mem, 2)}


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    with open(PID_FILE) as f: pid = int(f.read())
    sampler = Sampler(pid)

    # Start limited server on port 4434
    cert_path = os.path.join(os.path.dirname(__file__), "cert.pem")
    key_path  = os.path.join(os.path.dirname(__file__), "key.pem")
    cfg_srv = QuicConfiguration(alpn_protocols=H3_ALPN, is_client=False,
                                 max_datagram_frame_size=65536)
    cfg_srv.load_cert_chain(cert_path, key_path)
    server = await serve(HOST, PORT_LIMITED, configuration=cfg_srv,
                         create_protocol=LimitedServerProtocol)
    print(f"[limited server] Listening on {HOST}:{PORT_LIMITED} (MAX_STREAMS={MAX_STREAMS_LIMIT})")
    await asyncio.sleep(0.3)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for n in STREAM_COUNTS:
            for rep in range(1, REPEATS+1):
                # Unlimited
                print(f"  unlimited  N={n:3d}  rep={rep}/3 ...", end=" ", flush=True)
                row = await run_once(n, PORT_UNLIMITED, "unlimited", sampler)
                w.writerow(row); f.flush()
                print(f"done={row['streams_completed']}  rej={row['streams_rejected']}  cpu={row['server_cpu_avg']:.1f}%")

                # Limited
                print(f"  limited    N={n:3d}  rep={rep}/3 ...", end=" ", flush=True)
                row = await run_once(n, PORT_LIMITED, f"limit={MAX_STREAMS_LIMIT}", sampler)
                w.writerow(row); f.flush()
                print(f"done={row['streams_completed']}  rej={row['streams_rejected']}  cpu={row['server_cpu_avg']:.1f}%")

    server.close()
    print(f"\nSaved → {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
