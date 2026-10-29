import base64
import hashlib
import json
import os
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from resume_controller.core import Controller, Ledger
from resume_controller.t3_control import T3Control
from resume_controller.websocket_transport import exchange
from tests.test_controller import FakeAdapter


def frame(payload, opcode=1, fin=True):
    size = len(payload)
    prefix = bytes([(128 if fin else 0) | opcode])
    length = bytes([size]) if size < 126 else b"\x7e" + struct.pack("!H", size)
    return prefix + length + payload


class WebSocketTransportTests(unittest.TestCase):
    def setUp(self):
        self.mode, self.authorizations, self.proxy_connections = "ok", [], []
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *_):
                pass

            def do_CONNECT(self):
                fixture.proxy_connections.append(self.path)
                self.send_error(403)

            def do_GET(self):
                fixture.authorizations.append(self.headers.get("Authorization"))
                try:
                    if fixture.mode == "handshake":
                        self.wfile.write(b"HTTP/1.1 101 Switching Protocols\r\nX-Trickle: ")
                        return self.trickle(b"a")
                    if fixture.mode == "redirect":
                        self.send_response(302)
                        self.send_header("Location", "ws://example.invalid/sink")
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    key = self.headers["Sec-WebSocket-Key"]
                    accept = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
                    self.send_response(101)
                    self.send_header("Upgrade", "websocket")
                    self.send_header("Connection", "Upgrade")
                    self.send_header("Sec-WebSocket-Accept", accept)
                    self.end_headers()
                    header = self.rfile.read(2)
                    length = header[1] & 127
                    if length == 126:
                        length = struct.unpack("!H", self.rfile.read(2))[0]
                    elif length == 127:
                        length = struct.unpack("!Q", self.rfile.read(8))[0]
                    mask = self.rfile.read(4)
                    payload = self.rfile.read(length)
                    request = json.loads(bytes(byte ^ mask[i % 4] for i, byte in enumerate(payload)))
                    if fixture.mode in ("trickle", "oversize"):
                        self.wfile.write(b"\x81\x7e" + struct.pack("!H", 10000))
                        self.wfile.flush()
                        return self.trickle(b"a")
                    if fixture.mode == "heartbeat":
                        return self.trickle(frame(b"", opcode=9))
                    if fixture.mode == "fragments":
                        self.wfile.write(frame(b"a" * 60, fin=False) + frame(b"b" * 60, opcode=0))
                    else:
                        answer = {"_tag": "Exit", "requestId": request["id"],
                                  "exit": {"_tag": "Success", "value": {"ok": True}}}
                        self.wfile.write(frame(json.dumps(answer).encode()))
                    self.wfile.flush()
                except (OSError, IndexError):
                    pass
                finally:
                    self.close_connection = True

            def trickle(self, data):
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    self.wfile.write(data)
                    self.wfile.flush()
                    time.sleep(.01)

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"ws://127.0.0.1:{self.server.server_port}/ws"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, **kwargs):
        return exchange(self.url, "AUDIT_DUMMY_ONLY", {"_tag": "Request", "id": "1",
                        "tag": "fixture", "payload": {}, "headers": []}, **kwargs)

    def test_real_control_connection_bypasses_environment_proxy(self):
        real_run = subprocess.run
        def runner(args, **kwargs):
            if args[0] == "fixture-t3":
                return SimpleNamespace(returncode=0, stdout=json.dumps({"token": "AUDIT_DUMMY_ONLY", "sessionId": "fixture"}))
            return real_run(args, **kwargs)
        proxy = f"http://127.0.0.1:{self.server.server_port}"
        with tempfile.TemporaryDirectory() as directory:
            runtime = Path(directory) / "userdata/server-runtime.json"
            runtime.parent.mkdir()
            runtime.write_text(json.dumps({"origin": self.url.replace("ws:", "http:").removesuffix("/ws")}))
            with patch.dict(os.environ, {"http_proxy": proxy, "https_proxy": proxy, "ALL_PROXY": proxy,
                                       "HTTP_PROXY": proxy, "HTTPS_PROXY": proxy, "NO_PROXY": "", "no_proxy": ""}), \
                 patch("resume_controller.t3_control.subprocess.run", side_effect=runner):
                control = T3Control({"command": ["fixture-t3"], "base_dir": directory})
                try:
                    self.assertTrue(control.call("fixture", {})["ok"])
                finally:
                    control.close()
        self.assertEqual(self.proxy_connections, [])
        self.assertEqual(self.authorizations, ["Bearer AUDIT_DUMMY_ONLY"])

    def test_partial_frames_control_frames_and_handshake_have_total_deadline(self):
        for mode in ("trickle", "heartbeat", "handshake"):
            with self.subTest(mode=mode):
                self.mode = mode
                start = time.monotonic()
                with self.assertRaises(TimeoutError):
                    self.request(timeout=.4)
                self.assertLess(time.monotonic() - start, 2)

    def test_size_limit_rejects_advertised_frames_and_fragment_accumulation(self):
        for mode in ("oversize", "fragments"):
            self.mode = mode
            with self.assertRaisesRegex(RuntimeError, "byte limit"):
                self.request(max_bytes=100)

    def test_redirect_is_rejected(self):
        self.mode = "redirect"
        with self.assertRaisesRegex(RuntimeError, "WebSocketException"):
            self.request()
        self.assertEqual(len(self.authorizations), 1)

    def test_trickled_frame_does_not_block_next_adapter(self):
        self.mode = "trickle"
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / "ledger.sqlite")
            try:
                broken, healthy = FakeAdapter(), FakeAdapter()
                broken.name = "trickled"
                broken.observe = lambda _: self.request(timeout=.4)
                results = Controller(ledger).scan([broken, healthy])
                self.assertTrue(any(row["action"] == "inspection_error" for row in results))
                self.assertTrue(any(row["action"] == "would_resume" for row in results))
            finally:
                ledger.close()
