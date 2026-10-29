"""Use T3's authenticated, standard HTTP MCP endpoint (no database writes).

Credentials are minted through the installed CLI, held in memory, and revoked
on close. No tokens are put on command lines or written to controller logs.
"""
from __future__ import annotations

import json
import subprocess
import urllib.parse
from pathlib import Path
from .http_transport import exchange


class T3Mcp:
    def __init__(self, config, *, env=None):
        self.command = config.get("command", ["t3"])
        self.base = Path(config.get("base_dir", "~/.t3")).expanduser().resolve()
        self.env = env
        self.sequence, self.session, self.token, self.auth_id = 0, None, None, None
        runtime = json.loads((self.base / "userdata/server-runtime.json").read_text())
        origin = urllib.parse.urlparse(runtime["origin"])
        if origin.scheme not in ("http", "https") or origin.hostname not in ("127.0.0.1", "localhost", "::1"):
            raise RuntimeError("T3 local adapter requires a loopback server origin")
        self.url = runtime["origin"].rstrip("/") + "/mcp"
        issue = subprocess.run(
            [*self.command, "auth", "session", "issue", "--base-dir", str(self.base),
             "--scope", "orchestration:read", "--subject", "mcp-client",
             "--ttl", "5m", "--label", "resume-controller", "--json"],
            capture_output=True, text=True, timeout=30, env=env)
        if issue.returncode:
            raise RuntimeError("T3 could not issue a local scoped controller session")
        credentials = json.loads(issue.stdout)
        self.token, self.auth_id = credentials["token"], credentials["sessionId"]
        try:
            self.request("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                       "clientInfo": {"name": "resume-controller", "version": "0.1.0"}})
            self.request("notifications/initialized", {}, notification=True)
        except Exception:
            self.close()
            raise

    def request(self, method, params, notification=False):
        self.sequence += 1
        envelope = {"jsonrpc": "2.0", "method": method, "params": params}
        if not notification:
            envelope["id"] = self.sequence
        headers = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream",
                   "Authorization": f"Bearer {self.token}"}
        if self.session:
            headers["Mcp-Session-Id"] = self.session
        response = exchange(self.url, headers, envelope,
                            timeout=getattr(self, "response_timeout", 30), notification=notification)
        self.session = response.get("session") or self.session
        if notification:
            return {}
        answer = response["answer"]
        if "error" in answer:
            raise RuntimeError(f"T3 MCP {method} failed: {answer['error']}")
        return answer.get("result", {})

    def call(self, tool, arguments):
        result = self.request("tools/call", {"name": tool, "arguments": arguments})
        if result.get("isError"):
            raise RuntimeError(f"T3 tool {tool} failed")
        if isinstance(result.get("structuredContent"), dict):
            return result["structuredContent"]
        for block in result.get("content", []):
            if block.get("type") == "text":
                return json.loads(block["text"])
        raise RuntimeError(f"T3 tool {tool} returned no structured data")

    def close(self):
        if self.auth_id:
            try:
                subprocess.run([*self.command, "auth", "session", "revoke", "--base-dir",
                                str(self.base), self.auth_id], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=15, env=self.env)
            finally:
                self.auth_id, self.token = None, None
