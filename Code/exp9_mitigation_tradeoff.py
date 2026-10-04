"""
Experiment 9: Mitigation Tradeoff Study.

Tests stream-limit mitigation at MAX_STREAMS ∈ {10, 20, 50} and unlimited,
while a benign client runs simultaneously. Measures the tradeoff between
attack suppression and legitimate user impact.

For each limit setting and each N_attack:
  - Attacker sends N_attack streams on connection A
  - Benign client sends 1 stream on connection B (20ms later)
  - Measures: benign latency, benign success, attacker streams completed/rejected

N_attack values: {10, 20, 50, 100}
MAX_STREAMS limits: {unlimited, 10, 20, 50}
Repeats: 10
Writes: results/exp9_mitigation_tradeoff.csv
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

HOST            = "127.0.0.1"
PORT_UNLIMITED  = 4433
PORT_LIMITED    = 4437   # internal limited server
ATTACK_LEVELS   = [10, 20, 50, 100]
STREAM_LIMITS   = [0, 10, 20, 50]   # 0 = unlimited
REPEATS         = 10
RESULTS_DIR     = os.path.join(os.path.dirname(__file__), "..", "results")
CSV_PATH        = os.path.join(RESULTS_DIR, "exp9_mitigation_tradeoff.csv")
PID_FILE        = os.path.join(os.path.dirname(__file__), "server.pid")

BIGFILE_PATH = os.path.join(os.path.dirname(__file__), "bigfile.bin")
with open(BIGFILE_PATH, "rb") as _f:
    BIGFILE = _f.read()

COLUMNS = [
    "run_id", "timestamp", "max_streams_limit", "n_attack",
    "attacker_ok", "attacker_rejected",
    "benign_latency_s", "benign_success",
    "server_cpu_avg", "server_mem_mb_peak",
]


# ── Limited server ────────────────────────────────────────────────────────────

class LimitedServerProtocol(QuicConnectionProtocol):
    MAX_STREAMS = 10  # overridden per instance via class var

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
        if self._open_streams >= self.__class__.MAX_STREAMS:
            self._h3.send_headers(stream_id,
                [(b":status", b"429"),
                 (b"x-reject-reason", b"stream-limit-exceeded")],
                end_stream=True)
            return
        self._open_streams += 1
        range_hdr = dict(headers).get(b"range", b"bytes=0-9999")
        try:
            parts = range_hdr.decode().replace("bytes=", "").split("-")
            start, end = int(parts[0]), min(int(parts[1]), len(BIGFILE)-1)
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

    def send_req(self, authority=HOST) -> tuple[int, int]:
        sid = self._quic.get_next_available_stream_id()
        headers = [
            (b":method", b"GET"), (b":scheme", b"https"),
            (b":authority", authority.encode()), (b":path", b"/bigfile.bin"),
            (b"range", b"bytes=0-9999"),
        ]
        rb = sum(len(k) + len(v) for k, v in headers)
        self._done[sid] = asyncio.Event()
        self._data[sid] = b""
        self._h3 = self._h3 or H3Connection(self._quic)
        self._h3.send_headers(sid, headers, end_stream=True)
        self.transmit()
        return sid, rb

    async def wait(self, sid: int, timeout: float = 15.0):
        try:
            await asyncio.wait_for(self._done[sid].wait(), timeout=timeout)
        except asyncio.TimeoutError:
            self._status[sid] = "timeout"
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
        return (sum(self._cpu)/len(self._cpu) if self._cpu else 0.0), self._mem


# ── Single run ────────────────────────────────────────────────────────────────

def make_cfg():
    cfg = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    cfg.verify_mode = ssl.CERT_NONE
    return cfg

async def run_once(n_attack: int, port: int, sampler: Sampler) -> dict:
    await sampler.start()

    async def attack_task():
        async with connect(HOST, port, configuration=make_cfg(),
                           create_protocol=H3Client, wait_connected=True) as c:
            sids = []
            for _ in range(n_attack):
                sid, _ = c.send_req()
                sids.append(sid)
            results = await asyncio.gather(*[c.wait(s) for s in sids],
                                           return_exceptions=True)
            ok  = sum(1 for r in results if isinstance(r, tuple) and r[1] in ("206","200"))
            rej = sum(1 for r in results if isinstance(r, tuple) and r[1] == "429")
            return ok, rej

    async def benign_task():
        await asyncio.sleep(0.02)
        async with connect(HOST, port, configuration=make_cfg(),
                           create_protocol=H3Client, wait_connected=True) as c:
            t0 = time.perf_counter()
            sid, _ = c.send_req()
            _, status = await c.wait(sid, timeout=10.0)
            return time.perf_counter() - t0, status in ("206", "200")

    results = await asyncio.gather(attack_task(), benign_task(),
                                   return_exceptions=True)
    cpu, mem = await sampler.stop()

    atk_ok, atk_rej = results[0] if isinstance(results[0], tuple) else (0, 0)
    b_lat, b_ok = results[1] if isinstance(results[1], tuple) else (10.0, False)

    return {
        "attacker_ok": atk_ok,
        "attacker_rejected": atk_rej,
        "benign_latency_s": round(b_lat, 6),
        "benign_success": b_ok,
        "server_cpu_avg": round(cpu, 2),
        "server_mem_mb_peak": round(mem, 2),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    # PID of the external unlimited server (server.py on port 4433)
    with open(PID_FILE) as f:
        external_pid = int(f.read())

    # TLS certs for limited server
    cert_path = os.path.join(os.path.dirname(__file__), "cert.pem")
    key_path  = os.path.join(os.path.dirname(__file__), "key.pem")

    os.makedirs(RESULTS_DIR, exist_ok=True)

    with open(CSV_PATH, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()

        for limit in STREAM_LIMITS:
            label = "unlimited" if limit == 0 else f"MAX={limit}"

            if limit == 0:
                port = PORT_UNLIMITED
                server = None
                # Monitor the external server.py process
                sampler = Sampler(external_pid)
            else:
                # Dynamically set the class-level limit
                LimitedServerProtocol.MAX_STREAMS = limit
                cfg_srv = QuicConfiguration(alpn_protocols=H3_ALPN, is_client=False,
                                             max_datagram_frame_size=65536)
                cfg_srv.load_cert_chain(cert_path, key_path)
                server = await serve(HOST, PORT_LIMITED, configuration=cfg_srv,
                                     create_protocol=LimitedServerProtocol)
                await asyncio.sleep(0.3)
                port = PORT_LIMITED
                # Limited server runs in-process (asyncio.serve inside this script),
                # so monitor this process's own PID to capture its CPU/memory.
                sampler = Sampler(os.getpid())

            print(f"\n── {label} ──")

            try:
                for n_attack in ATTACK_LEVELS:
                    for rep in range(1, REPEATS + 1):
                        print(f"  {label:12s}  N_atk={n_attack:3d}  rep={rep}/{REPEATS} ...",
                              end=" ", flush=True)
                        row = await run_once(n_attack, port, sampler)
                        row.update({
                            "run_id": str(uuid.uuid4())[:8],
                            "timestamp": datetime.utcnow().isoformat(),
                            "max_streams_limit": label,
                            "n_attack": n_attack,
                        })
                        w.writerow(row); f.flush()
                        ok = "✓" if row["benign_success"] else "✗"
                        print(f"{ok} benign={row['benign_latency_s']:.4f}s  "
                              f"atk_ok={row['attacker_ok']}  "
                              f"atk_rej={row['attacker_rejected']}  "
                              f"cpu={row['server_cpu_avg']:.1f}%")
            finally:
                if server:
                    server.close()
                    await asyncio.sleep(0.2)

    print(f"\nSaved → {CSV_PATH}")

    # Summary
    from collections import defaultdict
    import statistics as st
    d = defaultdict(lambda: defaultdict(list))
    with open(CSV_PATH) as f:
        for row in csv.DictReader(f):
            key = (row["max_streams_limit"], int(row["n_attack"]))
            d[key]["lat"].append(float(row["benign_latency_s"]))
            d[key]["ok"].append(1 if row["benign_success"] == "True" else 0)
            d[key]["atk_ok"].append(float(row["attacker_ok"]))

    print(f"\n{'Limit':>12} {'N_atk':>6} {'Benign lat':>12} {'Benign ok%':>12} {'Atk ok':>8}")
    for limit in STREAM_LIMITS:
        label = "unlimited" if limit == 0 else f"MAX={limit}"
        for n in ATTACK_LEVELS:
            key = (label, n)
            if d[key]["lat"]:
                print(f"{label:>12} {n:>6} "
                      f"{st.mean(d[key]['lat']):>11.4f}s "
                      f"{st.mean(d[key]['ok'])*100:>11.0f}% "
                      f"{st.mean(d[key]['atk_ok']):>8.1f}")
        print()


if __name__ == "__main__":
    asyncio.run(main())
