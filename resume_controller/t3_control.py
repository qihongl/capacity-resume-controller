"""T3's authenticated orchestration RPC, with durable command IDs."""
import json
import subprocess
from urllib.parse import urlparse, urlunparse
from .websocket_transport import exchange


class T3Control:
    def __init__(self, config, *, env=None):
        from pathlib import Path
        self.command = config.get("command", ["t3"])
        self.base = Path(config.get("base_dir", "~/.t3")).expanduser().resolve()
        self.env, self.auth_id, self.token, self.sequence = env, None, None, 0
        origin = json.loads((self.base / "userdata/server-runtime.json").read_text())["origin"]
        parsed = urlparse(origin)
        if parsed.hostname not in ("127.0.0.1", "localhost", "::1") or parsed.scheme not in ("http", "https"):
            raise RuntimeError("T3 control requires a loopback owner server")
        p = subprocess.run([*self.command, "auth", "session", "issue", "--base-dir", str(self.base),
                            "--scope", "orchestration:read", "--scope", "orchestration:operate",
                            "--ttl", "5m", "--label", "resume-controller-dispatch", "--json"],
                           capture_output=True, text=True, timeout=30, env=env)
        if p.returncode:
            raise RuntimeError("T3 could not issue its controller transport credential")
        credentials = json.loads(p.stdout)
        self.auth_id = credentials["sessionId"]
        self.token = credentials["token"]
        self.url = urlunparse(("wss" if parsed.scheme == "https" else "ws", parsed.netloc,
                          "/ws", "", "orchestrationProtocol=2", ""))

    def call(self, method, payload):
        self.sequence += 1
        request_id = str(self.sequence)
        return exchange(self.url, self.token, {"_tag": "Request", "id": request_id, "tag": method,
                        "payload": payload, "headers": []}, timeout=getattr(self, "response_timeout", 30))

    def projection(self, thread_id):
        return self.call("orchestration.getThreadProjection", {"threadId": thread_id})

    def send(self, thread_id, request_id, text):
        self.call("orchestration.dispatchCommand", {
            "type": "message.dispatch", "commandId": f"resume-controller:{request_id}",
            "threadId": thread_id, "messageId": request_id, "text": text,
            "attachments": [], "createdBy": "system", "creationSource": "server",
            "dispatchMode": {"type": "queue_after_active"},
        })
        p = self.projection(thread_id)
        for message in p["messages"]:
            if message["id"] == request_id:
                return message["runId"]
        raise RuntimeError("T3 accepted the command; awaiting its projected message")

    def close(self):
        if self.auth_id:
            try:
                subprocess.run([*self.command, "auth", "session", "revoke", "--base-dir",
                                str(self.base), self.auth_id], stdout=subprocess.DEVNULL,
                               stderr=subprocess.DEVNULL, timeout=15, env=self.env)
            finally:
                self.auth_id, self.token = None, None
