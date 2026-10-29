from __future__ import annotations

import collections
import json
import queue
import socket
import subprocess
import tempfile
import threading
import time
from pathlib import Path

from .core import CONTINUE, Observation, DeliveryEvidence, continuation, is_continuation, eligible_error
from .t3_client import T3Mcp
from .t3_control import T3Control
from .ownership import socket_owner, previous_owner_exited, settings_snapshot, resume_parameters


def command_json(args, timeout=30):
    result = subprocess.run(args, capture_output=True, text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(f"{args[0]} exited {result.returncode}: {result.stderr.strip()[:500]}")
    return json.loads(result.stdout)


class T3Adapter:
    def __init__(self, config, runner=None, client=None):
        self.name = config.get("name", "t3")
        self.config, self.runner = config, runner or command_json
        self.base = str(Path(config.get("base_dir", "~/.t3")).expanduser().resolve())
        self.cli = config.get("command", ["t3"])
        self.scope = ["--base-dir", config["base_dir"]] if config.get("base_dir") else []
        # runner is a CLI-only fallback used by contract tests. Production uses
        # MCP activity: the CLI's message-only view omits Codex terminal errors.
        self.client = client if client is not None else (None if runner else T3Mcp(config))
        self.control = None

    def _control(self):
        if self.control is None:
            self.control = T3Control(self.config, env=getattr(self.client, "env", None))
        return self.control

    def _projects(self):
        selected = self.config.get("projects", [])
        if selected != ["*"]:
            return selected
        if not self.client:
            raise RuntimeError("all-project discovery requires MCP")
        projects, cursor = [], 0
        while cursor is not None:
            page = self.client.call("t3_project_list", {"cursor": cursor, "limit": 100})
            projects.extend(p["id"] for p in page["projects"])
            cursor = page.get("nextCursor")
        return projects

    def in_scope(self, thread_id):
        if not self.config.get("enabled", True) or thread_id in self.config.get("exclude_thread_ids", []):
            return False
        if thread_id in self.config.get("thread_ids", []):
            return True
        if not self.client:
            return False
        metadata = self.client.call("t3_thread_read", {"threadId": thread_id, "limit": 1})["thread"]
        return metadata["projectId"] in self._projects()

    def list_ids(self):
        ids = set(self.config.get("thread_ids", []))
        for project in self._projects():
            if self.client:
                cursor = 0
                while cursor is not None:
                    page = self.client.call("t3_thread_list", {"projectId": project, "cursor": cursor,
                                                               "limit": 100, "statuses": ["failed"],
                                                               "includeSubagents": False})
                    ids.update(r["threadId"] for r in page["threads"]
                               if not r.get("settled") and not r.get("snoozed"))
                    cursor = page.get("nextCursor")
            else:
                rows = self.runner(self.cli + ["thread", "list", *self.scope,
                                              "--project", project, "--json"])
                ids.update(r["threadId"] for r in rows if not r.get("settled") and not r.get("snoozed"))
        return sorted(ids)

    def observe(self, thread_id):
        if self.client:
            head = self.client.call("t3_thread_read", {"threadId": thread_id, "limit": 1, "view": "activity"})
            response = self.client.call("t3_thread_read", {
                "threadId": thread_id, "view": "activity", "limit": 100, "maxCharsPerItem": 3000,
                **({"afterPosition": head["thread"]["itemCount"] - 31}
                   if head["thread"]["itemCount"] > 30 else {})})
            # Only explicit terminal error items count; never match a quoted error
            # inside a tool output, user prompt, or assistant explanation.
            response["messages"] = [
                {"runId": i.get("runId"), "role": "user" if i["type"] == "user_message" else "assistant",
                 "text": i.get("text") or ""}
                for i in response["items"] if i["type"] in ("user_message", "error")]
        else:
            response = self.runner(self.cli + ["thread", "show", *self.scope,
                                             "--messages", "10", "--json", thread_id])
        t = response["thread"]
        run = t.get("latestRunId") or ""
        messages = [m for m in response.get("messages", []) if m.get("runId") == run]
        assistants = [m.get("text", "") for m in messages if m.get("role") == "assistant"]
        users = [m.get("text", "") for m in messages if m.get("role") == "user"]
        reason = ""
        if t.get("archived") or t.get("settled") or t.get("snoozed"):
            reason = "archived, settled, or snoozed"
        elif t.get("parentThreadId"):
            reason = "subagent is controlled by its parent"
        elif t.get("pendingRequestCount", 0) > 0:
            reason = "waiting for user input or approval"
        elif t.get("activeRunId") or t.get("status") in ("running", "starting", "preparing", "queued", "waiting"):
            reason = "busy or waiting"
        return Observation(
            key=f"t3:{self.base}:{thread_id}",
            thread_id=thread_id, generation=run, status=t.get("status", "unknown"),
            provider=t.get("providerInstanceId"), model=t.get("model"),
            error=assistants[-1] if assistants else "", blocked=reason,
            is_retry=bool(users and is_continuation(users[-1])),
        )

    def send(self, observation, request_id):
        # A stable server command ID makes a lost-response replay idempotent.
        return self._control().send(observation.thread_id, request_id, continuation(request_id))

    def reconcile(self, observation, request):
        p = self._control().projection(observation.thread_id)
        for message in p["messages"]:
            if message["id"] == request["request_id"]:
                return DeliveryEvidence("accepted", message["runId"])
        if observation.blocked:
            return DeliveryEvidence("unknown")
        if observation.generation != request["failure"] or observation.status != "failed":
            return DeliveryEvidence("superseded")
        return DeliveryEvidence("absent")

    def close(self):
        try:
            if self.client:
                self.client.close()
        finally:
            if self.control:
                self.control.close()


class RpcError(RuntimeError):
    pass


class JsonRpc:
    """Line-oriented Codex app-server transport, including multiplexed notifications."""
    def __init__(self, command, *, timeout=30, env=None):
        self.timeout = timeout
        self.stderr = tempfile.TemporaryFile(mode="w+")
        self.process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=self.stderr, text=True, bufsize=1, env=env)
        self._initialize()

    def _initialize(self):
        self.incoming = queue.Queue()
        self.notifications = collections.deque(maxlen=500)
        self.usage = {}
        self.live_turns = {}
        self.pending_requests = {}
        self.sequence = 0
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.reader.start()
        try:
            self.call("initialize", {"clientInfo": {"name": "resume_controller", "version": "0.1.0"}})
            self.notify("initialized")
        except Exception:
            self.close()
            raise

    def _read(self):
        try:
            for line in self.process.stdout:
                if line.strip():
                    self.incoming.put(json.loads(line))
        except Exception as exc:
            self.incoming.put(exc)
        finally:
            self.incoming.put(EOFError("Codex transport closed"))

    def _write(self, message):
        if self.process.poll() is not None:
            raise RpcError("Codex transport is not running")
        self.process.stdin.write(json.dumps(message) + "\n")
        self.process.stdin.flush()

    def notify(self, method, params=None):
        self._write({"method": method, **({"params": params} if params is not None else {})})

    def _event(self, message):
        self.notifications.append(message)
        params = message.get("params", {})
        if message.get("method") == "thread/tokenUsage/updated":
            self.usage[params["threadId"]] = params["tokenUsage"]
        if message.get("method") in ("turn/started", "turn/completed") and params.get("threadId"):
            self.live_turns[params["threadId"]] = params["turn"]
        if "id" in message and "method" in message:
            # Do not approve, deny, or answer a user's pending request.
            self.pending_requests[message["id"]] = params.get("threadId")
        if message.get("method") == "serverRequest/resolved":
            self.pending_requests.pop(params.get("requestId"), None)

    def call(self, method, params=None, timeout=None):
        self.sequence += 1
        request_id = self.sequence
        self._write({"id": request_id, "method": method, "params": params if params is not None else {}})
        deadline = time.monotonic() + (self.timeout if timeout is None else timeout)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f"Codex RPC timed out: {method}")
            try:
                message = self.incoming.get(timeout=remaining)
            except queue.Empty as exc:
                raise TimeoutError(f"Codex RPC timed out: {method}") from exc
            if isinstance(message, BaseException):
                raise RpcError(str(message))
            if message.get("id") == request_id and "method" not in message:
                if "error" in message:
                    raise RpcError(json.dumps(message["error"]))
                return message.get("result", {})
            self._event(message)

    def close(self):
        if self.process.poll() is None:
            self.process.terminate()
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=5)
        if self.process.stdin:
            self.process.stdin.close()
        if self.process.stdout:
            self.process.stdout.close()
        self.reader.join(timeout=1)
        self.stderr.close()


class UnixSocketRpc(JsonRpc):
    """Connect to an EXISTING owner via WebSocket over its local Unix socket."""
    def __init__(self, socket_path, *, timeout=30):
        import websocket  # websocket-client; required only for owner-socket access
        self.timeout = timeout
        socket_path = Path(socket_path).expanduser()
        if not socket_path.exists():
            raise RuntimeError(f"No Codex owner socket at {socket_path}; no replacement server was started")
        raw = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        raw.settimeout(10)
        try:
            raw.connect(str(socket_path))
            self.owner_identity = socket_owner(raw, socket_path)
            self.ws = websocket.create_connection("ws://localhost/", socket=raw,
                                                   suppress_origin=True, timeout=10)
            self.ws.settimeout(None)
        except Exception:
            raw.close()
            raise
        self._initialize()

    def _read(self):
        try:
            while True:
                message = self.ws.recv()
                if not message:
                    break
                self.incoming.put(json.loads(message))
        except Exception as exc:
            self.incoming.put(exc)
        finally:
            self.incoming.put(EOFError("Codex owner socket closed"))

    def _write(self, message):
        self.ws.send(json.dumps(message))

    def close(self):
        self.ws.close(timeout=1)
        if hasattr(self, "reader"):
            self.reader.join(timeout=2)


def user_text(items):
    texts = []
    for item in items:
        if item.get("type") == "userMessage":
            texts.append("".join(c.get("text", "") for c in item.get("content", [])
                                 if c.get("type") in ("text", "inputText", "input_text")))
    return texts[-1] if texts else ""


def latest_turn(rpc, thread_id):
    try:
        rows = rpc.call("thread/turns/list", {"threadId": thread_id, "limit": 1,
                                             "sortDirection": "desc", "itemsView": "full"})["data"]
        return rows[0] if rows else {}
    except RpcError as exc:
        # Installed 0.164.0 advertises pagination but legacy sessions return -32601.
        # Fall back only for unsupported methods, never hide connectivity/auth errors.
        if "-32601" not in str(exc):
            raise
        try:
            rows = rpc.call("thread/read", {"threadId": thread_id, "includeTurns": True})["thread"]["turns"]
            return rows[-1] if rows else {}
        except RpcError as fallback:
            if "-32601" not in str(fallback):
                raise
            # During a newly created session's initialization, both history
            # views can briefly be unavailable. Live events are authoritative
            # when this connection observed them; otherwise defer safely.
            return getattr(rpc, "live_turns", {}).get(thread_id, {})


class CodexAdapter:
    def __init__(self, config, rpc=None, registry=None):
        self.config = config
        self.name = config.get("name", "codex")
        if rpc is not None:
            self.rpc = rpc
        elif "command" in config:
            self.rpc = JsonRpc(config["command"])
        else:
            path = config.get("socket_path", "~/.codex/app-server-control/app-server-control.sock")
            try:
                self.rpc = UnixSocketRpc(path)
            except (OSError, RuntimeError):
                if not config.get("ensure_owner_daemon", False):
                    raise
                default = Path("~/.codex/app-server-control/app-server-control.sock").expanduser().resolve()
                if Path(path).expanduser().resolve() != default:
                    raise RuntimeError("automatic daemon recovery is restricted to Codex's default owner socket")
                started = subprocess.run(config.get("daemon_command", ["codex"]) +
                                         ["app-server", "daemon", "start"],
                                         capture_output=True, text=True, timeout=45)
                if started.returncode:
                    raise RuntimeError("Codex owner daemon is unavailable; it will be retried automatically")
                self.rpc = UnixSocketRpc(path)
        # Each endpoint needs an explicit stable namespace for ledger identity.
        self.namespace = config.get("namespace", "local-owner-server")
        self.allow_ids = set(config.get("thread_ids", []))
        self.exclude_ids = set(config.get("exclude_thread_ids", []))
        self.registry = registry
        self.restoration_warnings = []

    def in_scope(self, thread_id):
        return (self.config.get("enabled", True) and thread_id not in self.exclude_ids and
                (thread_id in self.allow_ids or self.config.get("enroll_loaded_threads", False)))

    def list_ids(self):
        loaded = self.rpc.call("thread/loaded/list")["data"]
        if self.registry is not None and getattr(self.rpc, "owner_identity", None):
            for key, record in self.registry.enrollments(self.namespace):
                thread_id = record["thread_id"]
                if (not record.get("active") or thread_id in loaded or not self.in_scope(thread_id) or
                        not previous_owner_exited(record.get("owner"), self.rpc.owner_identity)):
                    continue
                try:
                    self._restore_owned(key, record)
                except Exception as exc:
                    self.restoration_warnings.append({"thread_id": thread_id, "error": str(exc)})
            loaded = self.rpc.call("thread/loaded/list")["data"]
        # Never load/resume foreign threads just to inspect them.
        return sorted((set(loaded) | self.allow_ids) - self.exclude_ids)

    def _restore_owned(self, key, record):
        thread_id = record["thread_id"]
        current = self.rpc.call("thread/read", {"threadId": thread_id})["thread"]
        if current.get("status", {}).get("type") != "notLoaded" or "t3" in (current.get("originator") or "").lower():
            return
        turn = latest_turn(self.rpc, thread_id)
        if not turn or turn.get("completedAt") is None:
            return
        if turn.get("status") in ("completed", "interrupted", "cancelled"):
            record["active"] = False
            self.registry.enroll(key, record)
            return
        error = turn.get("error") or {}
        settings = record["settings"]
        if (turn.get("status") != "failed" or not eligible_error(error.get("message", ""), error.get("codexErrorInfo")) or
                (turn.get("id") != record["generation"] and not is_continuation(user_text(turn.get("items", [])))) or
                current.get("model") != settings["model"] or current.get("modelProvider") != settings["modelProvider"]):
            return
        restored = self.rpc.call("thread/resume", resume_parameters(thread_id, settings))
        if settings_snapshot(restored) != settings:
            # No continuation is sent when any effective permission/selection
            # differs. Do not subsequently enroll this mismatched load as valid.
            record["restoration_blocked_owner"] = self.rpc.owner_identity
            record["restoration_blocked_generation"] = turn["id"]
            self.registry.enroll(key, record)
            self.restoration_warnings.append({"thread_id": thread_id, "error": "restored settings differ from enrolled settings"})
            return
        record.update(owner=self.rpc.owner_identity)
        record.pop("restoration_blocked_owner", None)
        record.pop("restoration_blocked_generation", None)
        self.registry.enroll(key, record)

    def observe(self, thread_id):
        t = self.rpc.call("thread/read", {"threadId": thread_id})["thread"]
        runtime = t.get("status", {}).get("type", "unknown")
        blocked = ""
        if t.get("ephemeral"):
            blocked = "ephemeral preload has no durable recovery history"
        elif runtime == "notLoaded":
            blocked = "not loaded in connected owner server; ownership unverified"
        elif runtime == "active":
            blocked = "busy or waiting for input/approval"
        elif runtime not in ("idle", "systemError"):
            blocked = "unknown live status"
        elif thread_id not in self.allow_ids and not self.config.get("enroll_loaded_threads", False):
            blocked = "not enrolled; explicit thread ID required for mutation"
        elif t.get("parentThreadId"):
            blocked = "subagent is controlled by its parent"
        elif "t3" in (t.get("originator") or "").lower():
            blocked = "T3-owned session; use T3 adapter"
        elif thread_id in self.rpc.pending_requests.values():
            blocked = "pending user request"
        if blocked:
            return Observation(f"codex:{self.namespace}:{thread_id}", thread_id, "", runtime,
                               t.get("modelProvider"), t.get("model"), blocked=blocked)
        turn = latest_turn(self.rpc, thread_id)
        error = turn.get("error") or {}
        if turn.get("status") in ("failed", "completed", "interrupted") and turn.get("completedAt") is None:
            blocked = "turn terminal state has not been committed"
        key = f"codex:{self.namespace}:{thread_id}"
        enrolled = self.registry.enrollment(key) if self.registry is not None else None
        if (enrolled and enrolled.get("restoration_blocked_owner") is not None and
                enrolled.get("restoration_blocked_owner") == getattr(self.rpc, "owner_identity", None)):
            if turn.get("id") == enrolled.get("restoration_blocked_generation", enrolled["generation"]):
                actual = self.rpc.call("thread/resume", {"threadId": thread_id, "excludeTurns": True})
                if settings_snapshot(actual) != enrolled["settings"]:
                    blocked = "restored settings did not match the enrolled permissions"
                else:
                    enrolled.pop("restoration_blocked_owner", None)
                    enrolled.pop("restoration_blocked_generation", None)
                    enrolled["owner"] = self.rpc.owner_identity
                    self.registry.enroll(key, enrolled)
            else:
                # A new human turn supersedes a failed restoration. Never force
                # the old permission selection onto that new work.
                enrolled.pop("restoration_blocked_owner", None)
                enrolled.pop("restoration_blocked_generation", None)
                enrolled["active"] = False
                self.registry.enroll(key, enrolled)
        if (not blocked and self.registry is not None and getattr(self.rpc, "owner_identity", None) and
                turn.get("status") == "failed" and eligible_error(error.get("message", ""), error.get("codexErrorInfo"))):
            response = self.rpc.call("thread/resume", {"threadId": thread_id, "excludeTurns": True})
            self.registry.enroll(key, {
                "namespace": self.namespace, "thread_id": thread_id, "generation": turn["id"], "active": True,
                "owner": self.rpc.owner_identity, "settings": settings_snapshot(response)})
        return Observation(
            key=f"codex:{self.namespace}:{thread_id}", thread_id=thread_id,
            generation=turn.get("id", ""), status=turn.get("status", "idle"),
            provider=t.get("modelProvider"), model=t.get("model"),
            error=error.get("message", "") + " " + (error.get("additionalDetails") or ""),
            error_code=error.get("codexErrorInfo"),
            blocked=blocked,
            is_retry=is_continuation(user_text(turn.get("items", []))),
        )

    def send(self, observation, request_id):
        # A loaded thread already has its model, provider, cwd and permission policy.
        # Omitting overrides preserves all of those settings; no thread/resume needed.
        result = self.rpc.call("turn/start", {"threadId": observation.thread_id,
                                             "clientUserMessageId": request_id,
                                             "input": [{"type": "text", "text": continuation(request_id)}]})
        return result["turn"]["id"]

    def reconcile(self, observation, request):
        t = self.rpc.call("thread/read", {"threadId": observation.thread_id})["thread"]
        if t.get("status", {}).get("type") == "notLoaded":
            return DeliveryEvidence("unknown")
        turn = latest_turn(self.rpc, observation.thread_id)
        text = user_text(turn.get("items", []))
        marker = f"[resume-controller:{request['request_id']}]"
        if marker in text or any(item.get("type") == "userMessage" and
                                  item.get("clientId") == request["request_id"]
                                  for item in turn.get("items", [])):
            return DeliveryEvidence("accepted", turn["id"])
        # If a later user turn has superseded the failed work, do not inject a
        # stale continuation. No need to prove which earlier turn accepted it.
        if observation.blocked or turn.get("completedAt") is None:
            return DeliveryEvidence("unknown")
        if turn.get("id") != request["failure"] or turn.get("status") != "failed":
            if request.get("legacy_request") and is_continuation(text):
                return DeliveryEvidence("accepted", turn["id"])
            return DeliveryEvidence("superseded")
        return DeliveryEvidence("absent")

    def close(self):
        self.rpc.close()
