"""
Experiment 2: Protocol comparison — HTTP/1.1 vs HTTP/2 vs HTTP/3
Fixed: N=10 streams/connections, Range: bytes=0-9999, 10 repeats each.

Note on "streams":
  HTTP/1.1 — N sequential requests on 1 TCP connection (no multiplexing)
  HTTP/2   — N concurrent streams on 1 TCP connection
  HTTP/3   — N concurrent streams on 1 QUIC connection  ← your thesis case

Changes per professor feedback (18.pdf):
  - Added: streams_ok, streams_failed, fail_rate_pct
  - Added: throughput_rps (key for "why does HTTP/3 incur different resource costs?")
  - Added: per_stream_mem_mb, per_stream_cpu (resource cost per stream by protocol)

Writes: results/exp2_protocol_comparison.csv
Requires: pip install httpx[http2] h2 aiohttp
"""

import asyncio, csv, os, ssl, time, uuid, subprocess
from datetime import datetime
import psutil

RESULTS_DIR = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH = os.path.join(RESULTS_DIR, "exp2_protocol_comparison.csv")
PID_FILE = os.path.join(os.path.dirname(__file__), "server.pid")
N = 10
REPEATS = 10
COLUMNS = [
    "run_id", "timestamp", "protocol", "n_requests",
    "req_bytes_total", "resp_bytes_total", "amp_factor",
    "streams_ok", "streams_failed", "fail_rate_pct",
    "throughput_rps",
    "wall_time_s", "server_cpu_avg", "server_mem_mb_peak",
    "per_stream_mem_mb", "per_stream_cpu",
]

HTTP_HOST = "127.0.0.1"
HTTP_PORT = 8080
QUIC_PORT = 4433


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
        return (sum(self._cpu)/len(self._cpu) if self._cpu else 0.0), self._mem


def _row(proto, n, req, resp, ok, failed, wall, cpu, mem):
    fail_rate = (failed / n * 100) if n else 0.0
    throughput = ok / wall if wall > 0 else 0.0
    amp = resp / req if req else 0.0
    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "protocol": proto, "n_requests": n,
        "req_bytes_total": req, "resp_bytes_total": resp,
        "amp_factor": round(amp, 4),
        "streams_ok": ok, "streams_failed": failed,
        "fail_rate_pct": round(fail_rate, 2),
        "throughput_rps": round(throughput, 4),
        "wall_time_s": round(wall, 6),
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
        "per_stream_mem_mb": round(mem / n, 4) if n else 0.0,
        "per_stream_cpu": round(cpu / n, 4) if n else 0.0,
    }


async def run_http1(sampler: Sampler) -> dict:
    import httpx
    headers = {"Range": "bytes=0-9999"}
    url = f"http://{HTTP_HOST}:{HTTP_PORT}/bigfile.bin"
    req_bytes = len(f"GET /bigfile.bin HTTP/1.1\r\nHost: {HTTP_HOST}:{HTTP_PORT}\r\nRange: bytes=0-9999\r\n\r\n") * N

    ok = 0; failed = 0; total_resp = 0
    def _sync_requests():
        nonlocal ok, failed, total_resp
        with httpx.Client(http1=True, http2=False, timeout=30.0) as client:
            for _ in range(N):
                try:
                    r = client.get(url, headers=headers)
                    total_resp += len(r.content); ok += 1
                except Exception:
                    failed += 1

    await sampler.start()
    t0 = time.perf_counter()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, _sync_requests)
    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()
    return _row("HTTP/1.1", N, req_bytes, total_resp, ok, failed, wall, cpu, mem)


async def run_http2(sampler: Sampler) -> dict:
    import httpx
    headers = {"Range": "bytes=0-9999"}
    url = f"http://{HTTP_HOST}:{HTTP_PORT}/bigfile.bin"
    req_bytes = len(f"GET /bigfile.bin HTTP/2\r\nhost: {HTTP_HOST}:{HTTP_PORT}\r\nrange: bytes=0-9999\r\n\r\n") * N

    await sampler.start()
    t0 = time.perf_counter()
    ok = 0; failed = 0; total_resp = 0
    async with httpx.AsyncClient(http2=True, verify=False) as client:
        tasks = [client.get(url, headers=headers) for _ in range(N)]
        responses = await asyncio.gather(*tasks, return_exceptions=True)
    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    for r in responses:
        if isinstance(r, Exception):
            failed += 1
        else:
            total_resp += len(r.content); ok += 1
    return _row("HTTP/2", N, req_bytes, total_resp, ok, failed, wall, cpu, mem)


async def run_http3(sampler: Sampler) -> dict:
    import ssl as _ssl
    from aioquic.asyncio import connect
    from aioquic.asyncio.protocol import QuicConnectionProtocol
    from aioquic.h3.connection import H3_ALPN, H3Connection
    from aioquic.h3.events import DataReceived
    from aioquic.quic.configuration import QuicConfiguration
    from aioquic.quic.events import QuicEvent

    class H3Client(QuicConnectionProtocol):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)
            self._h3 = None; self._done = {}; self._data = {}
        def quic_event_received(self, ev: QuicEvent):
            if self._h3 is None: self._h3 = H3Connection(self._quic)
            for e in self._h3.handle_event(ev):
                if isinstance(e, DataReceived):
                    self._data[e.stream_id] = self._data.get(e.stream_id, b"") + e.data
                    if e.stream_ended and e.stream_id in self._done:
                        self._done[e.stream_id].set()
        def send_req(self):
            sid = self._quic.get_next_available_stream_id()
            hdrs = [(b":method",b"GET"),(b":scheme",b"https"),
                    (b":authority",HTTP_HOST.encode()),(b":path",b"/bigfile.bin"),
                    (b"range",b"bytes=0-9999")]
            rb = sum(len(k)+len(v) for k,v in hdrs)
            self._done[sid] = asyncio.Event(); self._data[sid] = b""
            self._h3 = self._h3 or H3Connection(self._quic)
            self._h3.send_headers(sid, hdrs, end_stream=True)
            self.transmit()
            return sid, rb
        async def wait(self, sid):
            await asyncio.wait_for(self._done[sid].wait(), timeout=15.0)
            return self._data.get(sid, b"")

    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = _ssl.CERT_NONE
    await sampler.start()
    t0 = time.perf_counter()
    async with connect(HTTP_HOST, QUIC_PORT, configuration=cfg,
                       create_protocol=H3Client, wait_connected=True) as c:
        total_req = 0; sids = []
        for _ in range(N):
            sid, rb = c.send_req(); sids.append(sid); total_req += rb
        results = await asyncio.gather(*[c.wait(s) for s in sids], return_exceptions=True)
    wall = time.perf_counter() - t0
    cpu, mem = await sampler.stop()

    ok     = sum(1 for r in results if isinstance(r, bytes))
    failed = sum(1 for r in results if not isinstance(r, bytes))
    total_resp = sum(len(r) for r in results if isinstance(r, bytes))
    return _row("HTTP/3", N, total_req, total_resp, ok, failed, wall, cpu, mem)


async def start_plain_server():
    from aiohttp import web
    BIGFILE = os.path.join(os.path.dirname(__file__), "bigfile.bin")
    with open(BIGFILE, "rb") as f:
        data = f.read()
    async def handler(request):
        range_hdr = request.headers.get("Range", "")
        if range_hdr.startswith("bytes="):
            parts = range_hdr[6:].split("-")
            start, end = int(parts[0]), int(parts[1]) if parts[1] else len(data)-1
            end = min(end, len(data)-1)
            body = data[start:end+1]
            return web.Response(status=206, body=body,
                                headers={"Content-Range": f"bytes {start}-{end}/{len(data)}"})
        return web.Response(body=data)
    app = web.Application()
    app.router.add_get("/bigfile.bin", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HTTP_HOST, HTTP_PORT)
    await site.start()
    print(f"[plain server] HTTP/1.1+HTTP/2 on {HTTP_HOST}:{HTTP_PORT}")
    return runner


async def main():
    try:
        import httpx, aiohttp, h2  # noqa
    except ImportError:
        print("Installing dependencies...")
        subprocess.run(["pip", "install", "httpx[http2]", "aiohttp", "h2",
                        "--break-system-packages", "-q"], check=True)
        import httpx, aiohttp, h2  # noqa

    with open(PID_FILE) as f: quic_pid = int(f.read())
    self_pid = os.getpid()
    sampler_self = Sampler(self_pid)
    sampler_quic = Sampler(quic_pid)

    runner = await start_plain_server()
    await asyncio.sleep(0.5)

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        for rep in range(1, REPEATS+1):
            for proto, fn, sampler in [("HTTP/1.1", run_http1, sampler_self),
                                        ("HTTP/2",   run_http2, sampler_self),
                                        ("HTTP/3",   run_http3, sampler_quic)]:
                print(f"  {proto}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await fn(sampler)
                w.writerow(row); f.flush()
                print(f"amp={row['amp_factor']:.1f}x  "
                      f"tput={row['throughput_rps']:.1f}rps  "
                      f"cpu/stream={row['per_stream_cpu']:.2f}%  "
                      f"fail={row['fail_rate_pct']:.1f}%")

    await runner.cleanup()
    print(f"\nSaved → {CSV_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
