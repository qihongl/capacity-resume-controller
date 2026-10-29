"""One bounded local T3 WebSocket RPC per killable worker.

Credentials pass through stdin. A parent deadline covers connection, handshake,
partial frames, control frames and cleanup, including peers that keep trickling.
"""
import json
import ipaddress
import socket
import ssl
import subprocess
import sys
from urllib.parse import urlparse


def exchange(url, token, envelope, *, timeout=30, max_bytes=8 * 1024 * 1024):
    request = {"url": url, "token": token, "envelope": envelope,
               "timeout": timeout, "max_bytes": max_bytes}
    try:
        worker = subprocess.run([sys.executable, "-m", "resume_controller.websocket_transport"],
                                input=json.dumps(request), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise TimeoutError("T3 WebSocket response exceeded its total deadline") from None
    if worker.returncode:
        raise RuntimeError("T3 WebSocket transport worker failed")
    result = json.loads(worker.stdout)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result.get("value")


def _perform(request):
    import websocket
    parsed = urlparse(request["url"])
    if parsed.scheme not in ("ws", "wss") or parsed.hostname not in ("127.0.0.1", "localhost", "::1"):
        raise ValueError("T3 WebSocket requires a loopback origin")

    class BoundedWebSocket(websocket.WebSocket):
        remaining_bytes = request["max_bytes"]

        def recv_frame(self):
            # websocket-client 1.9.2 is pinned. Inspect its buffered header/length
            # before it allocates or receives a payload. Budget spans fragments,
            # unsolicited events and protocol control frames for the whole RPC.
            buffer = self.frame_buffer
            with buffer.lock:
                if buffer.needs_header():
                    buffer.recv_header()
                if buffer.needs_length():
                    buffer.recv_length()
                if buffer.length > self.remaining_bytes:
                    raise ValueError("T3 WebSocket response exceeded its byte limit")
                self.remaining_bytes -= buffer.length
            return super().recv_frame()

    # A preconnected socket bypasses websocket-client's environment-proxy lookup
    # altogether. Verify the peer before any authenticated handshake.
    raw = socket.create_connection((parsed.hostname, parsed.port or (443 if parsed.scheme == "wss" else 80)),
                                   timeout=request["timeout"])
    try:
        if not ipaddress.ip_address(raw.getpeername()[0]).is_loopback:
            raise ValueError("T3 WebSocket peer is not loopback")
        if parsed.scheme == "wss":
            raw = ssl.create_default_context().wrap_socket(raw, server_hostname=parsed.hostname)
        ws = websocket.create_connection(request["url"], class_=BoundedWebSocket, socket=raw,
            header={"Authorization": "Bearer " + request["token"]}, suppress_origin=True,
            timeout=request["timeout"], redirect_limit=0, http_no_proxy=["*"])
    except BaseException:
        raw.close()
        raise
    try:
        ws.send(json.dumps(request["envelope"]))
        while True:
            message = json.loads(ws.recv())
            if message.get("_tag") == "Ping":
                ws.send(json.dumps({"_tag": "Pong"}))
            elif message.get("_tag") == "Exit" and message.get("requestId") == request["envelope"]["id"]:
                exit = message["exit"]
                if exit["_tag"] != "Success":
                    raise RuntimeError("T3 orchestration rejected the request")
                return {"value": exit.get("value")}
    finally:
        # No close-handshake wait; the completed RPC needs no persistent socket.
        ws.shutdown()


def main():
    try:
        result = _perform(json.load(sys.stdin))
    except ValueError as exc:
        result = {"error": str(exc) if "byte limit" in str(exc) else "Invalid T3 WebSocket response"}
    except Exception as exc:
        result = {"error": f"T3 WebSocket transport: {type(exc).__name__}"}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
