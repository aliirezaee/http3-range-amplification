"""
HTTP/3 server for thesis experiment: Stream-Based Amplification Abuse in QUIC/HTTP3
Serves bigfile.bin (10 KB of 0x41 bytes) and supports Range requests.

Usage:
    python server.py --host 0.0.0.0 --port 4433 \
                     --cert cert.pem --key key.pem

Generate a self-signed cert for local testing:
    openssl req -x509 -newkey rsa:2048 -keyout key.pem -out cert.pem \
                -days 365 -nodes -subj "/CN=localhost"
"""

import argparse
import asyncio
import os
import time
from typing import Dict, Optional

import psutil
from aioquic.asyncio import serve
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import HeadersReceived, H3Event
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent

# ── Static file loaded once at startup ──────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BIG_FILE_PATH = os.path.join(SCRIPT_DIR, "bigfile.bin")

with open(BIG_FILE_PATH, "rb") as _f:
    BIG_FILE_BYTES = _f.read()

BIG_FILE_LEN = len(BIG_FILE_BYTES)

# ── psutil sampler (shared state) ────────────────────────────────────────────
_proc = psutil.Process()
_cpu_samples: list[float] = []
_mem_peak_mb: float = 0.0
_sampling = False


async def _cpu_sampler(interval: float = 0.1):
    """Background task: sample CPU % and peak RSS every `interval` seconds."""
    global _mem_peak_mb, _sampling
    _cpu_samples.clear()
    _mem_peak_mb = 0.0
    while _sampling:
        _cpu_samples.append(_proc.cpu_percent(interval=None))
        mem_mb = _proc.memory_info().rss / 1_048_576
        if mem_mb > _mem_peak_mb:
            _mem_peak_mb = mem_mb
        await asyncio.sleep(interval)


def start_sampling():
    global _sampling
    _sampling = True
    _cpu_samples.clear()
    asyncio.get_event_loop().create_task(_cpu_sampler())


def stop_sampling():
    global _sampling
    _sampling = False


def get_stats():
    avg_cpu = sum(_cpu_samples) / len(_cpu_samples) if _cpu_samples else 0.0
    return avg_cpu, _mem_peak_mb


# ── HTTP/3 request handler ────────────────────────────────────────────────────

def _parse_range(header_value: bytes) -> tuple[int, int]:
    """Parse 'bytes=START-END' → (start, end). Returns (0, BIG_FILE_LEN-1) on error."""
    try:
        s = header_value.decode().strip()
        if s.startswith("bytes="):
            parts = s[6:].split("-")
            start = int(parts[0])
            end = int(parts[1]) if parts[1] else BIG_FILE_LEN - 1
            end = min(end, BIG_FILE_LEN - 1)
            return start, end
    except Exception:
        pass
    return 0, BIG_FILE_LEN - 1


class H3RequestHandler:
    def __init__(self, connection: H3Connection, stream_id: int):
        self.h3 = connection
        self.stream_id = stream_id

    def handle_headers(self, headers: list):
        range_header: Optional[bytes] = None
        path = b"/"
        for k, v in headers:
            if k == b":path":
                path = v
            elif k == b"range":
                range_header = v

        if path != b"/bigfile.bin":
            # 404
            self.h3.send_headers(
                self.stream_id,
                [(b":status", b"404"), (b"content-length", b"0")],
                end_stream=True,
            )
            return

        if range_header:
            start, end = _parse_range(range_header)
            body = BIG_FILE_BYTES[start : end + 1]
            status = b"206"
            content_range = f"bytes {start}-{end}/{BIG_FILE_LEN}".encode()
            resp_headers = [
                (b":status", status),
                (b"content-type", b"application/octet-stream"),
                (b"content-length", str(len(body)).encode()),
                (b"content-range", content_range),
            ]
        else:
            body = BIG_FILE_BYTES
            resp_headers = [
                (b":status", b"200"),
                (b"content-type", b"application/octet-stream"),
                (b"content-length", str(BIG_FILE_LEN).encode()),
            ]

        self.h3.send_headers(self.stream_id, resp_headers, end_stream=False)
        self.h3.send_data(self.stream_id, body, end_stream=True)


# ── QUIC / H3 connection protocol ────────────────────────────────────────────

class ServerProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._h3: Optional[H3Connection] = None
        self._handlers: Dict[int, H3RequestHandler] = {}

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic, enable_webtransport=False)

        for h3_event in self._h3.handle_event(event):
            self._h3_event_received(h3_event)

    def _h3_event_received(self, event: H3Event):
        if isinstance(event, HeadersReceived):
            handler = H3RequestHandler(self._h3, event.stream_id)
            self._handlers[event.stream_id] = handler
            handler.handle_headers(event.headers)
            self.transmit()


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="aioquic HTTP/3 server for thesis experiment")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=4433)
    parser.add_argument("--cert", default="cert.pem")
    parser.add_argument("--key", default="key.pem")
    args = parser.parse_args()

    config = QuicConfiguration(
        alpn_protocols=H3_ALPN,
        is_client=False,
        max_datagram_frame_size=65536,
    )
    config.load_cert_chain(args.cert, args.key)

    # Write PID file so client can monitor this process
    pid_path = os.path.join(SCRIPT_DIR, "server.pid")
    with open(pid_path, "w") as f:
        f.write(str(os.getpid()))
    print(f"[server] PID {os.getpid()} written to server.pid")
    print(f"[server] Loaded bigfile.bin — {BIG_FILE_LEN} bytes")
    print(f"[server] Listening on {args.host}:{args.port} (HTTP/3 over QUIC)")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(
        serve(
            args.host,
            args.port,
            configuration=config,
            create_protocol=ServerProtocol,
        )
    )
    loop.run_forever()


if __name__ == "__main__":
    main()
