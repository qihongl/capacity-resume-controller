import dataclasses
import json
import tempfile
import unittest
from unittest.mock import patch, Mock
from pathlib import Path

from resume_controller.adapters import CodexAdapter, T3Adapter, RpcError, latest_turn
from resume_controller.core import (CONTINUE, Controller, Ledger, Observation, DeliveryEvidence,
                                    eligible_error, exclusive_lock)


def failure(**kwargs):
    values = dict(key="test:one", thread_id="one", generation="turn-1", status="failed",
                  provider="openai", model="gpt-6.1-terra", error="server overloaded")
    values.update(kwargs)
    return Observation(**values)


class FakeAdapter:
    name = "fake"
    def __init__(self, observation=None):
        self.observation = observation or failure()
        self.sent = []
        self.on_recheck = None
        self.reads = 0
        self.send_error = None
        self.request_ids = []
        self.evidence = DeliveryEvidence("unknown")
        self.scoped = True

    def in_scope(self, _):
        return self.scoped

    def list_ids(self):
        return ["one", "one"]

    def observe(self, _):
        self.reads += 1
        if self.reads == 2 and self.on_recheck:
            return self.on_recheck
        return self.observation

    def send(self, observation, request_id):
        self.sent.append(observation)
        self.request_ids.append(request_id)
        if self.send_error:
            raise self.send_error
        return f"retry-{len(self.sent)}"

    def reconcile(self, observation, request):
        return self.evidence


class ControllerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "ledger.sqlite"
        self.ledger = Ledger(self.path)
        self.controller = Controller(self.ledger, apply=True, cooldown=0, max_attempts=3)
        self.adapter = FakeAdapter()

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def run_one(self):
        return self.controller.process(self.adapter, "one")

    def test_resume_once_and_preserve_selection(self):
        self.assertEqual(self.run_one()["action"], "resumed")
        self.assertEqual(self.run_one()["action"], "skip")
        self.assertEqual(len(self.adapter.sent), 1)
        self.assertEqual(self.adapter.sent[0].model, "gpt-6.1-terra")

    def test_dry_run_does_not_consume_attempt(self):
        c = Controller(self.ledger, apply=False)
        self.assertEqual(c.process(self.adapter, "one")["action"], "would_resume")
        self.assertIsNone(self.ledger.get("test:one"))
        self.assertFalse(self.adapter.sent)

    def test_three_attempts_one_episode_even_if_error_changes(self):
        for i in range(3):
            self.adapter.observation = failure(generation=f"failed-{i}", is_retry=i > 0,
                                               error="server overloaded" if i < 2 else "API Error: 503 unavailable")
            self.assertEqual(self.run_one()["attempt"], i + 1)
        self.adapter.observation = failure(generation="failed-4", is_retry=True)
        self.assertEqual(self.run_one()["action"], "exhausted")
        self.assertEqual(len(self.adapter.sent), 3)

    def test_receipt_links_chain_without_message(self):
        self.run_one()
        self.adapter.observation = failure(generation="retry-1")
        self.assertEqual(self.run_one()["attempt"], 2)

    def test_manual_new_task_creates_new_episode(self):
        self.run_one()
        self.adapter.observation = failure(generation="user-new-task", is_retry=False)
        self.assertEqual(self.run_one()["attempt"], 1)

    def test_changed_model_creates_new_episode(self):
        self.run_one()
        self.adapter.observation = failure(generation="new", model="another", is_retry=True)
        self.assertEqual(self.run_one()["attempt"], 1)

    def test_busy_changed_failure_selection_and_blocked_recheck(self):
        for changed in [failure(status="running"), failure(generation="new"),
                        failure(model="changed"), failure(blocked="approval"),
                        failure(error="context window exceeded")]:
            with self.subTest(changed=changed):
                a = FakeAdapter()
                a.on_recheck = changed
                self.assertEqual(self.controller.process(a, "one")["action"], "skip")
                self.assertFalse(a.sent)

    def test_skip_terminal_and_unmanaged(self):
        for status in ["completed", "cancelled", "interrupted", "waiting", "idle", "unknown"]:
            self.adapter.observation = failure(status=status)
            self.assertEqual(self.run_one()["action"], "skip")
        self.adapter.observation = failure(blocked="not loaded")
        self.assertEqual(self.run_one()["action"], "skip")
        self.assertFalse(self.adapter.sent)

    def test_unknown_and_noncapacity_failures_never_resume(self):
        for error in ["API Error: 400 context length exceeded", "permission denied", "429 quota exceeded",
                      "test failed", "some output mentions 503 experiments", "connection reset"]:
            self.adapter.observation = failure(error=error)
            self.assertEqual(self.run_one()["action"], "skip")

    def test_unknown_delivery_survives_restart(self):
        self.adapter.send_error = TimeoutError("lost reply")
        self.assertEqual(self.run_one()["action"], "deferred")
        self.ledger.close()
        self.ledger = Ledger(self.path)
        self.controller = Controller(self.ledger, apply=True, cooldown=0)
        self.adapter.send_error = None
        self.assertEqual(self.run_one()["action"], "deferred")
        self.assertEqual(len(self.adapter.sent), 1)

    def test_crash_after_claim_is_not_replayed(self):
        self.run_one()
        row = self.ledger.get("test:one")
        row["state"] = "dispatching"
        self.ledger.put("test:one", row)
        self.assertEqual(self.run_one()["action"], "deferred")

    def test_cooldown(self):
        self.controller = Controller(self.ledger, apply=True, cooldown=1800, clock=lambda: 100)
        self.run_one()
        self.adapter.observation = failure(generation="retry-1")
        self.assertEqual(self.run_one()["reason"], "retry backoff")

    def test_completion_closes_episode(self):
        self.run_one()
        self.adapter.observation = failure(generation="retry-1", status="completed")
        self.run_one()
        self.assertEqual(self.ledger.get("test:one")["state"], "resolved")

    def test_duplicate_discovery(self):
        self.controller.scan([self.adapter])
        self.assertEqual(len(self.adapter.sent), 1)

    def test_lock_excludes_second_process(self):
        lock = Path(self.tmp.name) / "lock"
        with exclusive_lock(lock):
            with self.assertRaises(RuntimeError):
                with exclusive_lock(lock):
                    self.fail("second owner")

    def test_error_allowlist(self):
        examples = [
            ("Selected model is at capacity. Please try a different model.", None, "capacity"),
            ("exceeded retry limit, last status: 429 Too Many Requests", None, "rate-retry-limit"),
            ("oops", "serverOverloaded", "overload"),
            ("oops", {"responseTooManyFailedAttempts": {"httpStatusCode": 503}}, "overload"),
            ("oops", {"responseTooManyFailedAttempts": {"httpStatusCode": 429}}, "rate-retry-limit"),
            ("API Error: 503 overloaded", "usageLimitExceeded", None),
            ("API Error: 429 Too Many Requests", None, None),
            ("server overloaded but quota exhausted", "serverOverloaded", None),
        ]
        for text, code, expected in examples:
            self.assertEqual(eligible_error(text, code), expected)

    def test_retry_count_survives_restart(self):
        self.run_one()
        self.ledger.close()
        self.ledger = Ledger(self.path)
        self.controller = Controller(self.ledger, apply=True, cooldown=0)
        self.adapter.observation = failure(generation="retry-1", is_retry=True)
        self.assertEqual(self.run_one()["attempt"], 2)

    def test_completion_reconciles_after_disappearing_from_failed_list(self):
        self.run_one()
        self.adapter.observation = failure(generation="retry-1", status="completed")
        self.adapter.list_ids = lambda: []
        results = self.controller.scan([self.adapter])
        self.assertEqual(results[0]["action"], "resolved")
        self.assertEqual(self.ledger.get("test:one")["state"], "resolved")
        self.assertEqual(len(self.adapter.sent), 1)

    def test_one_broken_adapter_does_not_block_another(self):
        broken = FakeAdapter()
        broken.name = "broken"
        def fail_list():
            raise ConnectionError("unavailable")
        broken.list_ids = fail_list
        results = self.controller.scan([broken, self.adapter])
        self.assertEqual(results[0]["action"], "adapter_error")
        self.assertTrue(any(r["action"] == "resumed" for r in results))

    def test_unattended_retries_beyond_three_without_human_step(self):
        now = [1000]
        c = Controller(self.ledger, apply=True, cooldown=10, max_delay=40, clock=lambda: now[0])
        for n in range(7):
            self.adapter.observation = failure(generation=f"failure-{n}", is_retry=n > 0)
            result = c.process(self.adapter, "one")
            self.assertEqual(result["action"], "resumed", result)
            self.assertEqual(result["attempt"], n + 1)
            now[0] += 40
        self.assertEqual(len(self.adapter.sent), 7)

    def test_unattended_backoff_is_capped_and_persistent(self):
        c = Controller(self.ledger, apply=True, cooldown=10, max_delay=40, clock=lambda: 1000)
        self.assertEqual([c.retry_delay(n) for n in range(1, 6)], [10, 20, 40, 40, 40])
        c.process(self.adapter, "one")
        self.adapter.observation = failure(generation="retry-1", is_retry=True)
        restarted = Controller(self.ledger, apply=True, cooldown=10, max_delay=40, clock=lambda: 1005)
        self.assertEqual(restarted.process(self.adapter, "one")["retry_at"], 1010)

    def test_lost_reply_accepted_is_reconciled_without_duplicate(self):
        self.adapter.send_error = TimeoutError("reply lost")
        self.run_one()
        self.adapter.evidence = DeliveryEvidence("accepted", "retry-1")
        self.adapter.observation = failure(generation="retry-1", status="completed")
        result = self.run_one()
        self.assertEqual(result["reason"], "completed")
        self.assertEqual(self.ledger.get("test:one")["state"], "resolved")
        self.assertEqual(len(self.adapter.sent), 1)

    def test_unsent_request_is_replayed_with_same_id_after_two_reads(self):
        now = [1000]
        c = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=60, clock=lambda: now[0])
        self.adapter.send_error = ConnectionError("not delivered")
        c.process(self.adapter, "one")
        request = self.adapter.request_ids[0]
        self.adapter.send_error = None
        self.adapter.evidence = DeliveryEvidence("absent")
        now[0] = 1060
        self.assertEqual(c.process(self.adapter, "one")["action"], "deferred")
        self.ledger.close()
        self.ledger = Ledger(self.path)
        restarted = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=60, clock=lambda: 1120)
        result = restarted.process(self.adapter, "one")
        self.assertEqual(result["action"], "replayed")
        self.assertEqual(result["attempt"], 1)
        self.assertEqual(self.adapter.request_ids, [request, request])

    def test_reconciliation_waits_for_busy_and_unavailable_threads(self):
        self.adapter.send_error = TimeoutError()
        self.run_one()
        self.adapter.observation = failure(status="active", blocked="busy")
        self.assertEqual(self.run_one()["action"], "deferred")
        self.assertEqual(len(self.adapter.sent), 1)

    def test_later_user_work_supersedes_uncertain_continuation(self):
        self.adapter.send_error = TimeoutError()
        self.run_one()
        self.adapter.evidence = DeliveryEvidence("superseded")
        self.adapter.observation = failure(generation="human-turn", status="completed")
        self.assertEqual(self.run_one()["action"], "superseded")
        self.assertEqual(len(self.adapter.sent), 1)

    def test_uncertain_replay_does_not_follow_model_or_error_changes(self):
        for changed in [failure(model="changed"), failure(provider="changed"),
                        failure(error="context window exceeded")]:
            with self.subTest(changed=changed):
                key = "test:" + str(changed)
                original = failure(key=key)
                a = FakeAdapter(original)
                a.send_error = TimeoutError()
                self.controller.process(a, "one")
                a.observation = dataclasses.replace(changed, key=key)
                a.evidence = DeliveryEvidence("absent")
                self.assertEqual(self.controller.process(a, "one")["action"], "superseded")
                self.assertEqual(len(a.sent), 1)

    def test_pending_request_cannot_bypass_current_exclusion(self):
        self.adapter.send_error = TimeoutError()
        self.run_one()
        self.adapter.scoped = False
        self.adapter.list_ids = lambda: []
        self.adapter.evidence = DeliveryEvidence("absent")
        self.adapter.send_error = None
        before = self.ledger.get("test:one")
        self.controller.scan([self.adapter])
        self.controller.scan([self.adapter])
        self.assertEqual(len(self.adapter.sent), 1)
        self.assertEqual(self.ledger.get("test:one"), before)

    def test_adapter_scope_changes_block_pending_replay_after_restart(self):
        for kind in ("t3", "codex"):
            with self.subTest(kind=kind):
                config = ({"projects": ["project"]} if kind == "t3" else
                          {"enroll_loaded_threads": True})
                client = Mock()
                client.call.return_value = {"thread": {"projectId": "project"}}
                def create():
                    return (T3Adapter(config, client=client) if kind == "t3" else
                            CodexAdapter(config, rpc=Mock()))
                adapter = create()
                observation = failure(key=f"scope:{kind}")
                adapter.observe = lambda _: observation
                adapter.send = Mock(side_effect=TimeoutError("delivery uncertain"))
                self.controller.process(adapter, "one")
                saved = self.ledger.get(observation.key)
                if kind == "t3":
                    config["projects"] = []
                else:
                    config["exclude_thread_ids"] = ["one"]
                self.ledger.close()
                self.ledger = Ledger(self.path)
                restarted = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=0)
                adapter = create()
                adapter.list_ids = lambda: []
                adapter.observe = Mock(side_effect=AssertionError("out-of-scope read"))
                adapter.send = Mock(side_effect=AssertionError("out-of-scope send"))
                for _ in range(2):
                    restarted.scan([adapter])
                    self.assertEqual(restarted.process(adapter, "one")["action"], "skip")
                adapter.observe.assert_not_called()
                adapter.send.assert_not_called()
                self.assertEqual(self.ledger.get(observation.key), saved)
                self.controller = restarted

    def test_scope_is_checked_again_immediately_before_dispatch(self):
        calls = iter((True, False))
        self.adapter.in_scope = lambda _: next(calls)
        self.assertEqual(self.run_one()["action"], "skip")
        self.assertFalse(self.adapter.sent)
        self.assertIsNone(self.ledger.get("test:one"))

    def test_every_uncertain_replay_requires_a_fresh_grace_window(self):
        now = [1000]
        c = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=60, clock=lambda: now[0])
        self.adapter.send_error = TimeoutError()
        self.adapter.evidence = DeliveryEvidence("absent")
        c.process(self.adapter, "one")
        for instant in [1060, 1120, 1121, 1122]:
            now[0] = instant
            c.process(self.adapter, "one")
        self.assertEqual(len(self.adapter.sent), 2)
        self.assertEqual(self.ledger.get("test:one")["transport_at"], 1120)
        self.ledger.close()
        self.ledger = Ledger(self.path)
        restarted = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=60, clock=lambda: 1180)
        restarted.process(self.adapter, "one")
        self.assertEqual(len(self.adapter.sent), 2)
        restarted.clock = lambda: 1181
        restarted.process(self.adapter, "one")
        self.assertEqual(len(self.adapter.sent), 3)


class AdapterTests(unittest.TestCase):
    def test_ephemeral_codex_preload_is_not_treated_as_failed_user_work(self):
        class Rpc:
            pending_requests = {}
            def call(self, method, *_):
                if method != "thread/read":
                    raise AssertionError("ephemeral history must not be queried")
                return {"thread": {"ephemeral": True, "status": {"type": "idle"}}}
        a = CodexAdapter({"enroll_loaded_threads": True}, rpc=Rpc())
        self.assertIn("ephemeral", a.observe("one").blocked)

    def test_codex_initializing_history_uses_live_state_without_guessing(self):
        class Rpc:
            live_turns = {"one": {"id": "new", "status": "inProgress"}}
            def call(self, *_):
                raise RpcError('{"code":-32601,"message":"not supported yet"}')
        rpc = Rpc()
        self.assertEqual(latest_turn(rpc, "one")["status"], "inProgress")
        self.assertEqual(latest_turn(rpc, "unseen"), {})

    def test_codex_owner_daemon_is_recovered_automatically(self):
        live = Mock()
        with patch("resume_controller.adapters.UnixSocketRpc", side_effect=[OSError("down"), live]), \
                patch("resume_controller.adapters.subprocess.run", return_value=Mock(returncode=0)) as run:
            adapter = CodexAdapter({"ensure_owner_daemon": True, "daemon_command": ["/absolute/codex"]})
            self.assertIs(adapter.rpc, live)
            self.assertEqual(run.call_args.args[0], ["/absolute/codex", "app-server", "daemon", "start"])

    def test_custom_socket_does_not_start_unrelated_daemon(self):
        with patch("resume_controller.adapters.UnixSocketRpc", side_effect=OSError("down")), \
                patch("resume_controller.adapters.subprocess.run") as run:
            with self.assertRaises(RuntimeError):
                CodexAdapter({"socket_path": "/missing/custom.sock", "ensure_owner_daemon": True})
            run.assert_not_called()

    def test_t3_uses_latest_run_only(self):
        response = {"thread": {"latestRunId": "r2", "status": "failed", "providerInstanceId": "claudeAgent",
                                "model": "claude-haiku-5-5"}, "messages": [
            {"runId": "r1", "role": "assistant", "text": "server overloaded"},
            {"runId": "r2", "role": "assistant", "text": "context window exceeded"}]}
        a = T3Adapter({}, runner=lambda _: response)
        o = a.observe("one")
        self.assertIsNone(eligible_error(o.error))

    def test_t3_skips_manual_settlement_approval_busy_and_subagent(self):
        for extra in [{"settled": True}, {"pendingRequestCount": 1}, {"activeRunId": "running"},
                      {"parentThreadId": "parent"}, {"snoozed": True}]:
            t = {"status": "failed", "latestRunId": "r", **extra}
            a = T3Adapter({}, runner=lambda _: {"thread": t})
            self.assertTrue(a.observe("one").blocked)

    def test_t3_send_inherits_model_no_override(self):
        calls = []
        class Control:
            def send(self, *args):
                calls.append(args)
                return "r"
        a = T3Adapter({}, runner=lambda _: {})
        a.control = Control()
        self.assertEqual(a.send(failure(), "request-1"), "r")
        self.assertEqual(calls[0][0:2], ("one", "request-1"))
        self.assertIn("[resume-controller:request-1]", calls[0][2])

    def test_codex_never_resumes_or_mutates_unloaded_and_unenrolled_threads(self):
        class Rpc:
            pending_requests = {}
            def call(self, method, params=None):
                if method != "thread/read":
                    raise AssertionError(method)
                return {"thread": {"id": "one", "status": {"type": self.status}, "model": "terra"}}
        rpc = Rpc()
        a = CodexAdapter({"thread_ids": ["one"]}, rpc=rpc)
        rpc.status = "notLoaded"
        self.assertIn("ownership", a.observe("one").blocked)
        rpc.status = "active"
        self.assertIn("busy", a.observe("one").blocked)
        a.allow_ids.clear()
        rpc.status = "idle"
        self.assertIn("not enrolled", a.observe("one").blocked)

    def test_t3_mcp_only_matches_terminal_errors_not_quoted_tool_text(self):
        class Client:
            def call(self, method, params):
                return {"thread": {"itemCount": 100, "latestRunId": "r", "status": "failed",
                                   "providerInstanceId": "codex", "model": "terra"}, "items": [
                    {"type": "dynamic_tool", "runId": "r", "text": "server overloaded"},
                    {"type": "assistant_message", "runId": "r", "text": "server overloaded"},
                    {"type": "error", "runId": "old", "text": "server overloaded"},
                    {"type": "error", "runId": "r", "text": "context window exceeded"}]}
        adapter = T3Adapter({}, client=Client())
        observation = adapter.observe("one")
        self.assertEqual(observation.error, "context window exceeded")
        self.assertIsNone(eligible_error(observation.error))

    def test_codex_skips_t3_originators_even_with_enrollment(self):
        class Rpc:
            pending_requests = {}
            def call(self, method, params):
                self.assert_method = method
                return {"thread": {"status": {"type": "idle"}, "originator": "t3-code",
                                   "model": "terra", "modelProvider": "openai"}}
        rpc = Rpc()
        adapter = CodexAdapter({"enroll_loaded_threads": True}, rpc=rpc)
        self.assertIn("T3-owned", adapter.observe("one").blocked)
        self.assertEqual(rpc.assert_method, "thread/read")


if __name__ == "__main__":
    unittest.main()
