import asyncio
import ssl
import argparse
import time
from aioquic.asyncio import connect
from aioquic.asyncio.protocol import QuicConnectionProtocol
from aioquic.h3.connection import H3_ALPN, H3Connection
from aioquic.h3.events import HeadersReceived, DataReceived
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent


class H3Client(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._h3: H3Connection | None = None
        self._stream_done: dict[int, asyncio.Event] = {}
        self._stream_data: dict[int, bytes] = {}
        self._stream_status: dict[int, str] = {}

    def quic_event_received(self, event: QuicEvent):
        if self._h3 is None:
            self._h3 = H3Connection(self._quic)
        for h3_event in self._h3.handle_event(event):
            if isinstance(h3_event, HeadersReceived):
                sid = h3_event.stream_id
                self._stream_status[sid] = dict(h3_event.headers).get(b":status", b"?").decode()
            elif isinstance(h3_event, DataReceived):
                sid = h3_event.stream_id
                self._stream_data[sid] = self._stream_data.get(sid, b"") + h3_event.data
                if h3_event.stream_ended and sid in self._stream_done:
                    self._stream_done[sid].set()

    def send_request(self, host: str) -> tuple[int, int]:
        """Open a new stream, send Range request. Returns (stream_id, req_bytes)."""
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

    async def wait_stream(self, stream_id: int, timeout: float = 10.0):
        await asyncio.wait_for(self._stream_done[stream_id].wait(), timeout=timeout)
        return self._stream_data.get(stream_id, b"")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=4433)
    parser.add_argument("--streams", type=int, default=1)
    args = parser.parse_args()

    config = QuicConfiguration(is_client=True, alpn_protocols=H3_ALPN)
    config.verify_mode = ssl.CERT_NONE

    print(f"Connecting to {args.host}:{args.port} with {args.streams} stream(s)...")

    async with connect(
        args.host, args.port,
        configuration=config,
        create_protocol=H3Client,
        wait_connected=True,
    ) as client:
        print("✅ QUIC connected")

        t_start = time.perf_counter()

        # Open all streams
        total_req_bytes = 0
        stream_ids = []
        for _ in range(args.streams):
            sid, req_bytes = client.send_request(args.host)
            stream_ids.append(sid)
            total_req_bytes += req_bytes

        # Wait for all streams concurrently
        results = await asyncio.gather(
            *[client.wait_stream(sid) for sid in stream_ids],
            return_exceptions=True,
        )

        wall_time = time.perf_counter() - t_start
        total_resp_bytes = sum(len(r) for r in results if isinstance(r, bytes))
        completed = sum(1 for r in results if isinstance(r, bytes))
        amp = total_resp_bytes / total_req_bytes if total_req_bytes else 0

        for i, (sid, r) in enumerate(zip(stream_ids, results)):
            status = client._stream_status.get(sid, "?")
            size = len(r) if isinstance(r, bytes) else "ERR"
            print(f"  stream {i+1} (id={sid}): HTTP {status}, {size} bytes")

        print(f"\n{'='*40}")
        print(f"Streams completed : {completed}/{args.streams}")
        print(f"Request bytes     : {total_req_bytes}")
        print(f"Response bytes    : {total_resp_bytes}")
        print(f"Amplification     : {amp:.1f}x")
        print(f"Wall time         : {wall_time:.3f}s")
        print(f"{'='*40}")


if __name__ == "__main__":
    asyncio.run(main())
