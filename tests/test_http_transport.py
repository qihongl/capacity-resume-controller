import json
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from resume_controller.http_transport import exchange
from resume_controller.core import Controller, Ledger
from tests.test_controller import FakeAdapter
from pathlib import Path
import tempfile


class HttpTransportTests(unittest.TestCase):
    def setUp(self):
        self.received = []
        self.redirect = ""
        fixture = self
        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_GET(self):
                fixture.received.append(self.headers.get("Authorization"))
                self.send_response(200)
                self.end_headers()

            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                if self.path == "/redirect":
                    self.send_response(302)
                    self.send_header("Location", fixture.redirect)
                    self.end_headers()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream" if self.path != "/oversize" else "application/json")
                self.end_headers()
                try:
                    if self.path == "/oversize":
                        self.wfile.write(b"x" * 10000)
                    elif self.path == "/ok":
                        self.wfile.write(("data: " + json.dumps({"id": request["id"], "result": {"ok": True}}) + "\n\n").encode())
                    else:
                        deadline = time.monotonic() + 3
                        while time.monotonic() < deadline:
                            self.wfile.write(b": heartbeat\n\n" if self.path == "/heartbeat" else b"d")
                            self.wfile.flush()
                            time.sleep(.01)
                except (BrokenPipeError, ConnectionResetError):
                    pass
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)

    def request(self, path, **kwargs):
        return exchange(self.base + path, {"Authorization": "Bearer REVIEW_DUMMY", "Content-Type": "application/json"},
                        {"id": 1, "method": "fixture"}, **kwargs)

    def test_matching_sse_response(self):
        self.assertTrue(self.request("/ok")["answer"]["result"]["ok"])

    def test_authenticated_redirects_are_never_followed(self):
        for target in [self.base.replace("127.0.0.1", "localhost") + "/sink", "http://example.invalid/sink"]:
            self.redirect = target
            with self.assertRaisesRegex(RuntimeError, "302"):
                self.request("/redirect")
        self.assertFalse(self.received)

    def test_heartbeats_and_unterminated_lines_have_total_deadline(self):
        for path in ["/heartbeat", "/trickle"]:
            start = time.monotonic()
            with self.assertRaises(TimeoutError):
                self.request(path, timeout=.4)
            self.assertLess(time.monotonic() - start, 2)

    def test_response_size_is_bounded(self):
        with self.assertRaisesRegex(RuntimeError, "byte limit"):
            self.request("/oversize", max_bytes=100)

    def test_stalled_t3_does_not_prevent_the_next_adapter(self):
        with tempfile.TemporaryDirectory() as directory:
            ledger = Ledger(Path(directory) / "ledger.sqlite")
            try:
                broken = FakeAdapter()
                broken.name = "stalled"
                broken.observe = lambda _: self.request("/heartbeat", timeout=.4)
                healthy = FakeAdapter()
                results = Controller(ledger).scan([broken, healthy])
                self.assertTrue(any(r["action"] == "inspection_error" for r in results))
                self.assertTrue(any(r["action"] == "would_resume" for r in results))
            finally:
                ledger.close()
