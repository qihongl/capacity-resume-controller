"""Isolated real T3 server + real Codex + loopback API. No production threads.

RUN_LIVE_T3=1 python3 -m unittest tests.test_live_t3 -v
"""
import json
import os
import signal
import socket
import subprocess
import tempfile
import time
import unittest
from pathlib import Path

from resume_controller.adapters import T3Adapter
from resume_controller.core import Controller, Ledger
from resume_controller.t3_client import T3Mcp
from resume_controller.core import continuation
from tests.test_live_codex import FakeApi, fixture_env, cleanup_fixture


T3 = str(Path.home() / ".t3/bin/t3")


@unittest.skipUnless(os.environ.get("RUN_LIVE_T3") == "1" and Path(T3).exists(), "opt-in isolated T3 test")
class LiveT3Tests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.base = self.root / "t3"
        self.api = FakeApi()
        self.env = fixture_env(self.root, self.api)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        self.log = tempfile.TemporaryFile(mode="w+")
        self.server = subprocess.Popen(
            [T3, "start", "--mode", "web", "--no-browser", "--host", "127.0.0.1",
             "--port", str(port), "--base-dir", str(self.base),
             "--auto-bootstrap-project-from-cwd", str(self.workspace)],
            stdout=self.log, stderr=self.log, env=self.env, start_new_session=True)
        self.ledger = Ledger(self.root / "ledger.sqlite")
        try:
            deadline = time.monotonic() + 25
            while time.monotonic() < deadline:
                if self.server.poll() is not None:
                    raise AssertionError("isolated T3 server exited during startup")
                try:
                    self.run_cli(["thread", "list", "--project", str(self.workspace), "--json"])
                    break
                except RuntimeError:
                    time.sleep(.2)
            else:
                raise AssertionError("isolated T3 server was not ready")
        except Exception:
            self.tearDown()
            raise

    def tearDown(self):
        self.api.gate.set()
        if self.server.poll() is None:
            os.killpg(self.server.pid, signal.SIGTERM)
            try:
                self.server.wait(timeout=8)
            except subprocess.TimeoutExpired:
                os.killpg(self.server.pid, signal.SIGKILL)
                self.server.wait(timeout=5)
        self.log.close()
        self.ledger.close()
        self.api.close()
        cleanup_fixture(self.tmp)

    def run_cli(self, args):
        p = subprocess.run([T3, *args, "--base-dir", str(self.base)], env=self.env,
                           capture_output=True, text=True, timeout=15)
        if p.returncode:
            raise RuntimeError(p.stderr[:500])
        return json.loads(p.stdout)

    def adapter_runner(self, args):
        p = subprocess.run(args, env=self.env, capture_output=True, text=True, timeout=15)
        if p.returncode:
            raise RuntimeError(p.stderr[:500])
        return json.loads(p.stdout)

    def wait_terminal(self, thread_id, previous=None, timeout=25):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = self.run_cli(["thread", "show", thread_id, "--messages", "10", "--json"])
            t = r["thread"]
            if (t.get("latestRunId") != previous and not t.get("activeRunId") and
                    t["status"] in ("failed", "completed", "interrupted")):
                return r
            time.sleep(.1)
        raise AssertionError("isolated T3 test turn did not terminate")

    def test_real_t3_failure_and_recovery_preserve_thread_and_model(self):
        created = self.run_cli(["thread", "start", str(self.workspace), "Reply OK.",
                                "--provider", "codex", "--model", "controller-fixture",
                                "--title", "Disposable controller fixture", "--json"])
        thread_id = created["threadId"]
        first = self.wait_terminal(thread_id)
        self.assertEqual(first["thread"]["status"], "failed", first)
        self.assertTrue(self.api.requests, "T3 must have used the local API fixture")
        config = {"base_dir": str(self.base), "command": [T3], "projects": ["*"]}
        client = T3Mcp(config, env=self.env)
        adapter = T3Adapter(config, client=client, runner=self.adapter_runner)
        try:
            self.assertIn(thread_id, adapter.list_ids())
            self.api.mode = "success"
            c = Controller(self.ledger, apply=True, cooldown=0)
            result = c.process(adapter, thread_id)
            self.assertEqual(result["action"], "resumed", result)
            final = self.wait_terminal(thread_id, first["thread"]["latestRunId"])
            self.assertEqual(final["thread"]["status"], "completed", final)
            self.assertEqual(final["thread"]["threadId"], thread_id)
            self.assertEqual(final["thread"]["model"], "controller-fixture")
            self.assertEqual(final["thread"]["providerInstanceId"], "codex")
            self.assertEqual(c.process(adapter, thread_id)["action"], "skip")
        finally:
            adapter.close()

    def test_lost_ack_and_idempotent_replay_require_no_manual_inspection(self):
        created = self.run_cli(["thread", "start", str(self.workspace), "Reply OK.",
                                "--provider", "codex", "--model", "controller-fixture", "--json"])
        thread_id = created["threadId"]
        first = self.wait_terminal(thread_id)
        config = {"base_dir": str(self.base), "command": [T3], "projects": ["*"]}
        adapter = T3Adapter(config, client=T3Mcp(config, env=self.env), runner=self.adapter_runner)
        try:
            original = adapter.send
            def lose_ack(observation, request_id):
                original(observation, request_id)
                raise TimeoutError("injected lost T3 acknowledgement")
            adapter.send = lose_ack
            c = Controller(self.ledger, apply=True, cooldown=0)
            result = c.process(adapter, thread_id)
            self.assertEqual(result["action"], "deferred", result)
            self.wait_terminal(thread_id, first["thread"]["latestRunId"])
            row = self.ledger.get(f"t3:{self.base.resolve()}:{thread_id}")
            control = adapter._control()
            before = len(control.projection(thread_id)["runs"])
            # Restart the actual owner too: command deduplication must survive
            # more than the controller's Python process lifetime.
            adapter.close()
            arguments = self.server.args
            os.killpg(self.server.pid, signal.SIGKILL)
            self.server.wait(timeout=5)
            self.server = subprocess.Popen(arguments, stdout=self.log, stderr=self.log,
                                           env=self.env, start_new_session=True)
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                try:
                    self.run_cli(["thread", "list", "--project", str(self.workspace), "--json"])
                    break
                except RuntimeError:
                    time.sleep(.1)
            else:
                self.fail("T3 owner did not restart")
            adapter = T3Adapter(config, client=T3Mcp(config, env=self.env), runner=self.adapter_runner)
            control = adapter._control()
            # Repeating the exact same server command cannot create another run.
            control.send(thread_id, row["request_id"], continuation(row["request_id"]))
            self.assertEqual(len(control.projection(thread_id)["runs"]), before)
            self.ledger.close()
            self.ledger = Ledger(self.root / "ledger.sqlite")
            restarted = Controller(self.ledger, apply=True, cooldown=1800)
            adapter.send = original
            result = restarted.process(adapter, thread_id)
            self.assertEqual(result["action"], "deferred", result)
            self.assertEqual(self.ledger.get(f"t3:{self.base.resolve()}:{thread_id}")["state"], "sent")
            self.assertEqual(len(control.projection(thread_id)["runs"]), before)
        finally:
            adapter.close()


if __name__ == "__main__":
    unittest.main()
