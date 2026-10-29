"""Bounded authenticated HTTP requests in a killable, short-lived worker.

The parent enforces a wall-clock deadline even if a peer trickles HTTP headers
or SSE bytes forever. Credentials travel only through an anonymous stdin pipe.
"""
import json
import subprocess
import sys
import urllib.error
import urllib.request


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, fp, code, message, headers, new_url):
        return None


def exchange(url, headers, envelope, *, timeout=30, max_bytes=8 * 1024 * 1024, notification=False):
    request = {"url": url, "headers": headers, "envelope": envelope,
               "timeout": timeout, "max_bytes": max_bytes, "notification": notification}
    try:
        worker = subprocess.run([sys.executable, "-m", "resume_controller.http_transport"],
                                input=json.dumps(request), capture_output=True, text=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        raise TimeoutError("T3 HTTP response exceeded its total deadline") from None
    if worker.returncode:
        raise RuntimeError("T3 HTTP transport worker failed")
    result = json.loads(worker.stdout)
    if "error" in result:
        raise RuntimeError(result["error"])
    return result


def _perform(request):
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    wire = urllib.request.Request(request["url"], data=json.dumps(request["envelope"]).encode(),
                                  headers=request["headers"])
    with opener.open(wire, timeout=request["timeout"]) as response:
        session = response.headers.get("Mcp-Session-Id")
        if request["notification"]:
            return {"session": session, "answer": {}}
        sse = "text/event-stream" in response.headers.get("Content-Type", "")
        total = 0
        buffer = b""
        payload = []
        body = bytearray()
        while True:
            chunk = response.read1(65536)
            if not chunk:
                break
            total += len(chunk)
            if total > request["max_bytes"]:
                raise ValueError("T3 HTTP response exceeded its byte limit")
            if not sse:
                body.extend(chunk)
                continue
            buffer += chunk
            if len(buffer) > min(request["max_bytes"], 1024 * 1024):
                raise ValueError("T3 SSE line exceeded its byte limit")
            while b"\n" in buffer:
                line, buffer = buffer.split(b"\n", 1)
                line = line.rstrip(b"\r")
                if line.startswith(b"data:"):
                    payload.append(line[5:].lstrip())
                elif not line and payload:
                    candidate = json.loads(b"\n".join(payload))
                    payload = []
                    if candidate.get("id") == request["envelope"].get("id"):
                        return {"session": session, "answer": candidate}
        if sse:
            raise ValueError("T3 MCP response had no matching request ID")
        return {"session": session, "answer": json.loads(body)}


def main():
    try:
        result = _perform(json.load(sys.stdin))
    except urllib.error.HTTPError as exc:
        result = {"error": f"T3 MCP HTTP status {exc.code}; redirects are not followed"}
    except Exception as exc:
        result = {"error": f"T3 HTTP transport: {type(exc).__name__}: {str(exc)[:300]}"}
    print(json.dumps(result))


if __name__ == "__main__":
    main()
