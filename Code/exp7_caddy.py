"""
Experiment 7: Validate amplification against Caddy (quic-go / Go HTTP/3).

Repeats the main sweep against Caddy instead of the aioquic server.
Demonstrates the finding is not aioquic-specific — it is a protocol-level issue.

Caddy must be running before this script:
  cd ~/Downloads/Thesis\ Carrega/Code
  caddy run --config Caddyfile

N values: {1, 5, 10, 20, 50, 100}, 10 repeats each.
Writes: results/exp7_caddy.csv
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

HOST         = "127.0.0.1"
PORT         = 4436          # Caddy listens here
STREAM_COUNTS = [1, 5, 10, 20, 50, 100]
REPEATS       = 10
RESULTS_DIR   = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH      = os.path.join(RESULTS_DIR, "exp7_caddy.csv")

COLUMNS = [
    "run_id", "timestamp", "server", "n_streams",
    "req_bytes_approx", "resp_bytes_total", "amp_factor",
    "streams_ok", "streams_failed",
    "wall_time_s", "server_cpu_pct",
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
            (b":authority", b"localhost"), (b":path", b"/bigfile.bin"),
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
        await asyncio.wait_for(self._done[sid].wait(), timeout=20.0)
        return self._data.get(sid, b""), self._status.get(sid, "?")


# ── Single run ────────────────────────────────────────────────────────────────

async def run_once(n: int) -> dict:
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE
    cfg.server_name = "localhost"

    t0 = time.perf_counter()
    cpu_before = psutil.cpu_percent(interval=None)

    async with connect(HOST, PORT, configuration=cfg,
                       create_protocol=H3Client, wait_connected=True) as c:
        sids = []
        total_req = 0
        for _ in range(n):
            sid, rb = c.send_req()
            sids.append(sid); total_req += rb
        results = await asyncio.gather(*[c.wait(s) for s in sids],
                                       return_exceptions=True)

    wall = time.perf_counter() - t0
    cpu  = psutil.cpu_percent(interval=None)

    ok     = sum(1 for r in results if isinstance(r, tuple) and r[1] in ("200", "206"))
    failed = sum(1 for r in results if not isinstance(r, tuple) or r[1] not in ("200", "206"))
    resp   = sum(len(r[0]) for r in results if isinstance(r, tuple) and r[1] in ("200", "206"))
    amp    = resp / total_req if total_req else 0

    return {
        "run_id": str(uuid.uuid4())[:8],
        "timestamp": datetime.utcnow().isoformat(),
        "server": "caddy-v2",
        "n_streams": n,
        "req_bytes_approx": total_req,
        "resp_bytes_total": resp,
        "amp_factor": round(amp, 4),
        "streams_ok": ok,
        "streams_failed": failed,
        "wall_time_s": round(wall, 6),
        "server_cpu_pct": round((cpu + cpu_before) / 2, 2),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    print("Connecting to Caddy on localhost:4436 ...")
    # Quick connectivity check
    try:
        cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
        cfg.verify_mode = ssl.CERT_NONE
        cfg.server_name = "localhost"
        async with connect(HOST, PORT, configuration=cfg,
                           create_protocol=H3Client, wait_connected=True) as c:
            sid, _ = c.send_req()
            data, status = await c.wait(sid)
            print(f"  Connectivity OK — status={status}  bytes={len(data)}")
            if status not in ("200", "206"):
                print(f"  WARNING: unexpected status {status}. Check Caddyfile and bigfile.bin path.")
    except Exception as e:
        print(f"  ERROR connecting to Caddy: {e}")
        print("  Make sure Caddy is running:  caddy run --config Caddyfile")
        return

    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for n in STREAM_COUNTS:
            for rep in range(1, REPEATS + 1):
                print(f"  N={n:3d}  rep={rep}/{REPEATS} ...", end=" ", flush=True)
                row = await run_once(n)
                w.writerow(row); f.flush()
                print(f"amp={row['amp_factor']:.1f}x  "
                      f"ok={row['streams_ok']}/{n}  "
                      f"wall={row['wall_time_s']:.3f}s")

    print(f"\nSaved → {CSV_PATH}")

    import statistics as st
    from collections import defaultdict
    d = defaultdict(list)
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            d[int(row["n_streams"])].append(float(row["amp_factor"]))

    print(f"\n{'N':>5}  {'Amp (Caddy)':>14}")
    for n in STREAM_COUNTS:
        if d[n]:
            print(f"{n:>5}  {st.mean(d[n]):>13.1f}x")


if __name__ == "__main__":
    asyncio.run(main())
