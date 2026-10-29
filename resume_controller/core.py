from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol


CONTINUE = (
    "Continue from your existing progress after the temporary capacity/overload "
    "failure. Preserve the current provider and model; do not restart the original job."
)


def continuation(request_id: str) -> str:
    return (CONTINUE + "\n\nContinue only work that remains unfinished. If a later turn has already "
            "completed it, do not repeat that work."
            f"\n\n[resume-controller:{request_id}]")


def is_continuation(text: str) -> bool:
    return text == CONTINUE or (text.startswith(CONTINUE + "\n\n") and "[resume-controller:" in text)


@dataclass(frozen=True)
class DeliveryEvidence:
    state: str  # accepted, absent, superseded, unknown
    receipt: str | None = None


@dataclass(frozen=True)
class Observation:
    key: str
    thread_id: str
    generation: str
    status: str
    provider: str | None
    model: str | None
    error: str = ""
    error_code: object = None
    blocked: str = ""
    is_retry: bool = False

    def stamp(self) -> tuple:
        """Everything that must remain unchanged before a dispatch."""
        return (self.key, self.generation, self.status, self.provider, self.model,
                self.error, json.dumps(self.error_code, sort_keys=True), self.blocked)


class Adapter(Protocol):
    name: str
    def list_ids(self) -> list[str]: ...
    def observe(self, thread_id: str) -> Observation: ...
    def in_scope(self, thread_id: str) -> bool: ...
    def send(self, observation: Observation, request_id: str) -> str | None: ...
    def reconcile(self, observation: Observation, request: dict) -> DeliveryEvidence: ...
    def close(self) -> None: ...


def eligible_error(text: str, code: object = None) -> str | None:
    """An allowlist, not a broad 'anything failed' heuristic."""
    text = text.lower()
    if any(s in text for s in (
        "context length", "context window", "quota", "insufficient_quota",
        "usage limit", "usage_limit", "billing", "budget exceeded",
        "authentication", "unauthorized", "invalid api key",
    )):
        return None
    if code in ("contextWindowExceeded", "sessionBudgetExceeded", "usageLimitExceeded",
                "unauthorized", "badRequest", "cyberPolicy", "misalignmentPolicyViolation"):
        return None
    if code == "serverOverloaded":
        return "overload"
    if isinstance(code, dict):
        for name in ("responseTooManyFailedAttempts", "httpConnectionFailed",
                     "responseStreamConnectionFailed", "responseStreamDisconnected"):
            detail = code.get(name)
            if isinstance(detail, dict) and detail.get("httpStatusCode") in (503, 529):
                return "overload"
        detail = code.get("responseTooManyFailedAttempts")
        if isinstance(detail, dict) and detail.get("httpStatusCode") == 429:
            return "rate-retry-limit"
    if re.search(r"selected model is at capacity|model.{0,40}(?:is at capacity|capacity exceeded)", text):
        return "capacity"
    if re.search(r"server (?:is )?overloaded|overloaded_error", text):
        return "overload"
    if re.search(r"(?:http|status|api error|error)[^\n]{0,35}\b(?:503|529)\b", text):
        return "overload"
    if "429" in text and re.search(r"(?:exceeded|reached).{0,25}retry limit|too many failed attempts", text):
        return "rate-retry-limit"
    return None


@contextlib.contextmanager
def exclusive_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("another controller pass holds this state lock") from exc
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


class Ledger:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.execute("CREATE TABLE IF NOT EXISTS episodes (key TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.db.execute("CREATE TABLE IF NOT EXISTS events (at REAL, key TEXT, action TEXT, body TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS enrollments (key TEXT PRIMARY KEY, body TEXT NOT NULL)")
        self.db.commit()

    def get(self, key):
        row = self.db.execute("SELECT body FROM episodes WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def put(self, key, body):
        self.db.execute("INSERT OR REPLACE INTO episodes VALUES (?,?)", (key, json.dumps(body)))
        self.db.commit()

    def event(self, key, action, detail, now):
        self.db.execute("INSERT INTO events VALUES (?,?,?,?)", (now, key, action, json.dumps(detail)))
        self.db.commit()

    def pending(self, adapter):
        return [(key, row) for key, body in self.db.execute("SELECT key, body FROM episodes")
                if (row := json.loads(body)).get("adapter") == adapter and
                row.get("state") in ("sent", "dispatching", "uncertain", "reconciling")]

    def enroll(self, key, body):
        self.db.execute("INSERT OR REPLACE INTO enrollments VALUES (?,?)", (key, json.dumps(body)))
        self.db.commit()

    def enrollment(self, key):
        row = self.db.execute("SELECT body FROM enrollments WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def enrollments(self, namespace):
        return [(key, row) for key, body in self.db.execute("SELECT key, body FROM enrollments")
                if (row := json.loads(body)).get("namespace") == namespace]

    def close(self):
        self.db.close()


class Controller:
    def __init__(self, ledger: Ledger, *, apply=False, max_attempts=None,
                 cooldown=1800, max_delay=21600, reconciliation_grace=60, clock=time.time):
        if max_attempts is not None and not 1 <= max_attempts <= 3:
            raise ValueError("max_attempts must be null or between 1 and 3")
        self.ledger, self.apply = ledger, apply
        self.max_attempts, self.cooldown, self.clock = max_attempts, cooldown, clock
        self.max_delay, self.reconciliation_grace = max_delay, reconciliation_grace

    def retry_delay(self, attempts):
        if self.max_attempts is not None:
            return self.cooldown
        return min(self.max_delay, self.cooldown * 2 ** min(max(attempts - 1, 0), 20))

    def process(self, adapter: Adapter, thread_id: str) -> dict:
        if not adapter.in_scope(thread_id):
            return {"adapter": adapter.name, "thread_id": thread_id, "action": "skip",
                    "reason": "outside current configured scope"}
        o = adapter.observe(thread_id)
        now = self.clock()
        row = self.ledger.get(o.key)

        def result(action, **details):
            item = {"adapter": adapter.name, "thread_id": thread_id, "action": action, **details}
            self.ledger.event(o.key, action, details, now)
            return item

        # An uncertain result is a machine-recoverable state, not a request for a
        # human. Reconcile the durable request ID before deciding to replay it.
        if row and row["state"] in ("dispatching", "uncertain", "reconciling"):
            if not self.apply:
                return result("would_reconcile")
            # Migrate a journal written by v0.1; existing retry text is checked too.
            if "request_id" not in row:
                row["request_id"] = str(uuid.uuid5(uuid.NAMESPACE_URL,
                    f"{o.key}:{row.get('episode')}:{row.get('attempts')}"))
                row["legacy_request"] = True
            evidence = adapter.reconcile(o, row)
            if evidence.state == "accepted":
                row.update(state="sent", receipt=evidence.receipt)
                self.ledger.put(o.key, row)
            elif evidence.state == "superseded":
                row["state"] = "stopped"
                self.ledger.put(o.key, row)
                return result("superseded", reason="later user activity replaced the failed work")
            elif evidence.state == "absent":
                if (o.provider != row["provider"] or o.model != row["model"] or
                        o.generation != row["failure"] or not eligible_error(o.error, o.error_code)):
                    row["state"] = "stopped"
                    self.ledger.put(o.key, row)
                    return result("superseded", reason="failure or selected model changed before replay")
                first_absent = row.setdefault("absent_since", now)
                row["state"] = "reconciling"
                self.ledger.put(o.key, row)
                if now - max(first_absent, row.get("transport_at", row["at"])) < self.reconciliation_grace:
                    return result("deferred", reason="confirming non-delivery automatically")
                fresh = adapter.observe(thread_id)
                if fresh.stamp() != o.stamp() or fresh.blocked or fresh.status != "failed":
                    return result("deferred", reason="thread changed during delivery reconciliation")
                # Same request and attempt, not a fresh continuation.
                return self._dispatch(adapter, o, row, result, replay=True)
            else:
                return result("deferred", reason="awaiting authoritative delivery evidence")

        if o.blocked:
            return result("skip", reason=o.blocked)
        if o.status != "failed":
            if self.apply and row and o.status in ("completed", "interrupted", "cancelled"):
                row["state"] = "resolved" if o.status == "completed" else "stopped"
                self.ledger.put(o.key, row)
            return result("skip", reason=o.status)
        kind = eligible_error(o.error, o.error_code)
        if not kind:
            return result("skip", reason="unrecognized or non-capacity failure")
        if not o.model or not o.provider or not o.generation:
            return result("skip", reason="missing model, provider, or failure identity")
        if row and row["state"] == "sent" and row["failure"] == o.generation:
            return result("skip", reason="already dispatched for this failure")

        same_chain = bool(row and row["state"] not in ("resolved", "stopped") and
                          row["provider"] == o.provider and row["model"] == o.model and
                          (row["failure"] == o.generation or
                           row.get("receipt") == o.generation or o.is_retry))
        if not same_chain:
            row = {"attempts": 0, "at": 0, "state": "new", "provider": o.provider,
                   "model": o.model, "episode": hashlib.sha256(
                       f"{o.key}:{o.generation}".encode()).hexdigest()[:24]}
        if self.max_attempts is not None and row["attempts"] >= self.max_attempts:
            if self.apply:
                row["state"] = "exhausted"
                self.ledger.put(o.key, row)
            return result("exhausted", reason="configured attempt limit reached", attempts=row["attempts"])
        delay = self.retry_delay(row["attempts"])
        if row["attempts"] and now - row["at"] < delay:
            return result("deferred", reason="retry backoff", retry_at=row["at"] + delay)
        if not self.apply:
            return result("would_resume", kind=kind, attempt=row["attempts"] + 1)

        fresh = adapter.observe(thread_id)
        if fresh.stamp() != o.stamp():
            return result("skip", reason="thread changed during recheck")
        row.update(at=now, failure=o.generation, state="dispatching", receipt=None,
                   attempts=row["attempts"] + 1, adapter=adapter.name, thread_id=thread_id,
                   request_id=str(uuid.uuid4()))
        row.pop("absent_since", None)
        return self._dispatch(adapter, o, row, result)

    def _dispatch(self, adapter, o, row, result, replay=False):
        if not adapter.in_scope(o.thread_id):
            return result("skip", reason="outside current configured scope")
        row["state"] = "dispatching"
        row["transport_at"] = self.clock()
        row.pop("absent_since", None)
        # Commit BEFORE sending: a crash must reconcile this request, not create
        # a fresh untracked continuation.
        self.ledger.put(o.key, row)
        try:
            receipt = adapter.send(o, row["request_id"])
        except Exception as exc:
            row["state"] = "uncertain"
            self.ledger.put(o.key, row)
            return result("deferred", reason="reconciling delivery automatically", error=str(exc))
        row.update(state="sent", receipt=receipt)
        self.ledger.put(o.key, row)
        return result("replayed" if replay else "resumed", attempt=row["attempts"], receipt=receipt)

    def scan(self, adapters: list[Adapter]) -> list[dict]:
        results = []
        seen = set()
        for adapter in adapters:
            try:
                ids = adapter.list_ids()
            except Exception as exc:
                results.append({"adapter": adapter.name, "action": "adapter_error", "error": str(exc)})
                continue
            results.extend({"adapter": adapter.name, "action": "restore_deferred", **warning}
                           for warning in getattr(adapter, "restoration_warnings", []))
            # Reconcile old dispatches even when the failed-only discovery filter
            # no longer returns a now-completed thread. Never dispatch from here.
            for key, row in self.ledger.pending(adapter.name):
                if row["thread_id"] in ids:
                    continue
                try:
                    if row["state"] in ("dispatching", "uncertain", "reconciling"):
                        results.append(self.process(adapter, row["thread_id"]))
                        continue
                    o = adapter.observe(row["thread_id"])
                    if not o.blocked and o.status in ("completed", "interrupted", "cancelled"):
                        state = "resolved" if o.status == "completed" else "stopped"
                        if self.apply:
                            row["state"] = state
                            self.ledger.put(key, row)
                        results.append({"adapter": adapter.name, "thread_id": o.thread_id,
                                        "action": state if self.apply else "would_" + state})
                except Exception as exc:
                    results.append({"adapter": adapter.name, "thread_id": row["thread_id"],
                                    "action": "inspection_error", "error": str(exc)})
            for thread_id in ids:
                if (adapter.name, thread_id) in seen:
                    continue
                seen.add((adapter.name, thread_id))
                try:
                    results.append(self.process(adapter, thread_id))
                except Exception as exc:
                    results.append({"adapter": adapter.name, "thread_id": thread_id,
                                    "action": "inspection_error", "error": str(exc)})
            results.append({"adapter": adapter.name, "action": "scan_complete", "discovered": len(set(ids))})
        return results
