"""Real Codex app-server + loopback-only fake model API. No paid requests.

RUN_LIVE_CODEX=1 python3 -m unittest tests.test_live_codex -v
"""
import json
import errno
import os
import pty
import select
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from resume_controller.adapters import CodexAdapter, JsonRpc, UnixSocketRpc, latest_turn
from resume_controller.core import Controller, Ledger
from resume_controller.ownership import settings_snapshot
from install_service import prepare_config, SERVICE_PATH


class FakeApi:
    def __init__(self):
        self.requests = []
        self.mode = "failure"
        self.gate = threading.Event()
        self.gate.set()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
                fixture.requests.append((self.path, body))
                fixture.gate.wait(10)
                if fixture.mode == "failure":
                    self.send_response(503)
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": {"message": "server overloaded",
                                                          "type": "server_error"}}).encode())
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                output = {"id": "msg_fixture", "type": "message", "role": "assistant",
                          "status": "completed", "content": [{"type": "output_text", "text": "OK", "annotations": []}]}
                events = [
                    {"type": "response.created", "response": {"id": "resp_fixture", "status": "in_progress"}},
                    {"type": "response.output_item.added", "output_index": 0, "item": {**output, "status": "in_progress", "content": []}},
                    {"type": "response.output_text.delta", "output_index": 0, "content_index": 0,
                     "item_id": "msg_fixture", "delta": "OK"},
                    {"type": "response.output_item.done", "output_index": 0, "item": output},
                    {"type": "response.completed", "response": {"id": "resp_fixture", "status": "completed",
                     "output": [output], "usage": {"input_tokens": 100, "output_tokens": 2,
                     "total_tokens": 102, "input_tokens_details": {"cached_tokens": 50},
                     "output_tokens_details": {"reasoning_tokens": 0}}}},
                ]
                try:
                    for event in events:
                        self.wfile.write(("event: " + event["type"] + "\ndata: " + json.dumps(event) + "\n\n").encode())
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_port}/v1"

    def close(self):
        self.gate.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


def fixture_env(root, api):
    home = root / "codex-home"
    home.mkdir(exist_ok=True)
    (home / "config.toml").write_text(
        'model = "controller-fixture"\nmodel_provider = "fixture"\n'
        '[model_providers.fixture]\nname = "Local controller fixture"\n'
        f'base_url = "{api.url}"\nwire_api = "responses"\nrequires_openai_auth = false\n'
        'request_max_retries = 0\nstream_max_retries = 0\n')
    # Do not inherit the parent T3/Codex session identity into test sessions.
    return {**{k: v for k, v in os.environ.items() if not k.startswith(("CODEX_", "T3_"))},
            "CODEX_HOME": str(home), "CODEX_INTERNAL_ORIGINATOR_OVERRIDE": "controller_fixture"}


def wait_terminal(rpc, thread_id, timeout=20, turn_id=None):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        turn = latest_turn(rpc, thread_id)
        if (turn and (turn_id is None or turn["id"] == turn_id) and
                turn["status"] in ("failed", "completed", "interrupted") and turn.get("completedAt") is not None):
            return turn
        time.sleep(0.05)
    raise AssertionError("test turn did not finish")


def cleanup_fixture(directory):
    # Native plugin setup can finish writing just after its owner exits. Retry
    # only that transient directory race, with a bound; persistent leaks fail.
    deadline = time.monotonic() + 3
    while True:
        try:
            directory.cleanup()
            return
        except OSError as exc:
            if exc.errno != errno.ENOTEMPTY or time.monotonic() >= deadline:
                raise
            time.sleep(.05)


@unittest.skipUnless(os.environ.get("RUN_LIVE_CODEX") == "1" and shutil.which("codex"), "opt-in live Codex test")
class LiveCodexTests(unittest.TestCase):
    def setUp(self):
        # macOS Unix sockets have a short pathname limit. Its default tempfile
        # directory plus Codex's control-socket suffix can exceed that limit.
        self.tmp = tempfile.TemporaryDirectory(prefix="resume-", dir="/tmp")
        self.root = Path(self.tmp.name).resolve()
        self.api = FakeApi()
        self.env = fixture_env(self.root, self.api)
        self.rpc = JsonRpc(["codex", "app-server", "--stdio"], env=self.env)
        self.extra_rpcs = []
        self.servers = []
        self.ledger = Ledger(self.root / "ledger.sqlite")

    def tearDown(self):
        self.api.gate.set()
        for rpc in self.extra_rpcs:
            rpc.close()
        self.rpc.close()
        for process, log in self.servers:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            log.close()
        self.ledger.close()
        self.api.close()
        cleanup_fixture(self.tmp)

    def start(self, text="fixture original job"):
        response = self.rpc.call("thread/start", {"model": "controller-fixture", "modelProvider": "fixture",
                                                  "cwd": str(self.root), "sandbox": "read-only",
                                                  "approvalPolicy": "never", "baseInstructions": "Reply OK."})
        thread_id = response["thread"]["id"]
        self.rpc.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": text}]})
        return thread_id

    def adapter(self, thread_id, rpc=None):
        return CodexAdapter({"namespace": "fixture", "thread_ids": [thread_id]}, rpc=rpc or self.rpc)

    def test_real_failure_retry_limit_model_and_thread_preserved(self):
        thread_id = self.start()
        self.assertEqual(wait_terminal(self.rpc, thread_id)["status"], "failed")
        adapter = self.adapter(thread_id)
        controller = Controller(self.ledger, apply=True, cooldown=0, max_attempts=3)
        first = adapter.observe(thread_id)
        self.assertEqual(first.status, "failed")
        self.assertEqual(first.model, "controller-fixture")
        self.assertEqual(first.provider, "fixture")
        for expected in range(1, 4):
            result = controller.process(adapter, thread_id)
            self.assertEqual(result["action"], "resumed", result)
            self.assertEqual(result["attempt"], expected)
            self.assertEqual(wait_terminal(self.rpc, thread_id)["status"], "failed")
        result = controller.process(adapter, thread_id)
        self.assertEqual(result["action"], "exhausted")
        after = self.rpc.call("thread/read", {"threadId": thread_id})["thread"]
        self.assertEqual(after["id"], thread_id)
        self.assertEqual(after["model"], first.model)
        self.assertEqual(after["modelProvider"], first.provider)
        self.assertTrue(all(body["model"] == "controller-fixture" for _, body in self.api.requests))

    def test_real_active_and_completed_threads_are_skipped(self):
        self.api.mode = "success"
        self.api.gate.clear()
        thread_id = self.start()
        deadline = time.monotonic() + 10
        while not self.api.requests and time.monotonic() < deadline:
            time.sleep(0.02)
        self.assertTrue(self.api.requests, "fixture API was not reached")
        adapter = self.adapter(thread_id)
        controller = Controller(self.ledger, apply=True, cooldown=0)
        before = len(self.api.requests)
        result = controller.process(adapter, thread_id)
        self.assertEqual(result["action"], "skip")
        self.assertIn("busy", result["reason"])
        self.api.gate.set()
        turn = wait_terminal(self.rpc, thread_id)
        self.assertEqual(turn["status"], "completed", turn)
        self.assertEqual(controller.process(adapter, thread_id)["reason"], "completed")
        self.assertEqual(len(self.api.requests), max(before, 1))

    def test_second_server_cannot_claim_live_ownership(self):
        self.api.gate.clear()
        thread_id = self.start()
        second = JsonRpc(["codex", "app-server", "--stdio"], env=self.env)
        self.extra_rpcs.append(second)
        observation = self.adapter(thread_id, second).observe(thread_id)
        self.assertIn("ownership unverified", observation.blocked)
        self.assertNotIn(thread_id, second.call("thread/loaded/list")["data"])
        self.assertIn(thread_id, self.rpc.call("thread/loaded/list")["data"])
        self.api.gate.set()
        wait_terminal(self.rpc, thread_id)

    def test_real_successful_recovery(self):
        thread_id = self.start()
        wait_terminal(self.rpc, thread_id)
        self.api.mode = "success"
        adapter = self.adapter(thread_id)
        controller = Controller(self.ledger, apply=True, cooldown=0)
        self.assertEqual(controller.process(adapter, thread_id)["action"], "resumed")
        self.assertEqual(wait_terminal(self.rpc, thread_id)["status"], "completed")
        controller.process(adapter, thread_id)
        self.assertEqual(self.ledger.get(f"codex:fixture:{thread_id}")["state"], "resolved")

    def test_lost_response_after_acceptance_recovers_after_restart_without_duplicate(self):
        thread_id = self.start()
        wait_terminal(self.rpc, thread_id)
        self.api.mode = "success"
        adapter = self.adapter(thread_id)
        original = adapter.send
        def lose_response(observation, request_id):
            original(observation, request_id)
            raise TimeoutError("injected lost delivery acknowledgement")
        adapter.send = lose_response
        c = Controller(self.ledger, apply=True, cooldown=0)
        self.assertEqual(c.process(adapter, thread_id)["action"], "deferred")
        self.assertEqual(wait_terminal(self.rpc, thread_id)["status"], "completed")
        count = len(self.api.requests)
        self.ledger.close()
        self.ledger = Ledger(self.root / "ledger.sqlite")
        adapter.send = original
        restarted = Controller(self.ledger, apply=True, cooldown=0)
        restarted.process(adapter, thread_id)
        self.assertEqual(self.ledger.get(f"codex:fixture:{thread_id}")["state"], "resolved")
        self.assertEqual(len(self.api.requests), count)

    def test_connection_failure_before_send_is_retried_automatically(self):
        thread_id = self.start()
        wait_terminal(self.rpc, thread_id)
        adapter = self.adapter(thread_id)
        original = adapter.send
        adapter.send = lambda *_: (_ for _ in ()).throw(ConnectionError("injected before send"))
        now = [1000]
        c = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=1, clock=lambda: now[0])
        c.process(adapter, thread_id)
        row = self.ledger.get(f"codex:fixture:{thread_id}")
        self.api.mode = "success"
        adapter.send = original
        now[0] += 1
        self.assertEqual(c.process(adapter, thread_id)["action"], "deferred")
        now[0] += 1
        result = c.process(adapter, thread_id)
        self.assertEqual(result["action"], "replayed", result)
        self.assertEqual(result["attempt"], 1)
        self.assertEqual(self.ledger.get(f"codex:fixture:{thread_id}")["request_id"], row["request_id"])
        self.assertEqual(wait_terminal(self.rpc, thread_id)["status"], "completed")

    def test_client_message_id_is_not_assumed_to_be_an_idempotency_key(self):
        thread_id = self.start()
        wait_terminal(self.rpc, thread_id)
        request = {"threadId": thread_id, "clientUserMessageId": str(uuid.uuid4()),
                   "input": [{"type": "text", "text": "Disposable duplicate-ID protocol check"}]}
        first = self.rpc.call("turn/start", request)["turn"]["id"]
        wait_terminal(self.rpc, thread_id, turn_id=first)
        second = self.rpc.call("turn/start", request)["turn"]["id"]
        wait_terminal(self.rpc, thread_id, turn_id=second)
        self.assertNotEqual(first, second)

    def test_separate_controller_client_can_use_same_owner_socket(self):
        sock = self.root / "owner.sock"
        log = tempfile.TemporaryFile(mode="w+")
        server = subprocess.Popen(["codex", "app-server", "--listen", f"unix://{sock}"],
                                  env=self.env, stdout=log, stderr=log)
        self.servers.append((server, log))
        deadline = time.monotonic() + 10
        while not sock.exists() and time.monotonic() < deadline and server.poll() is None:
            time.sleep(.05)
        self.assertTrue(sock.exists(), "owner-server socket did not start")
        owner = UnixSocketRpc(sock)
        controller_rpc = UnixSocketRpc(sock)
        self.extra_rpcs.extend([controller_rpc, owner])
        thread_id = owner.call("thread/start", {"model": "controller-fixture", "modelProvider": "fixture",
                                                "cwd": str(self.root), "sandbox": "read-only",
                                                "approvalPolicy": "never", "baseInstructions": "Reply OK."})["thread"]["id"]
        self.api.gate.clear()
        owner.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": "Reply OK."}]})
        deadline = time.monotonic() + 5
        while not self.api.requests and time.monotonic() < deadline:
            time.sleep(.02)
        adapter = self.adapter(thread_id, controller_rpc)
        self.assertIn(thread_id, adapter.list_ids())
        self.assertIn("busy", adapter.observe(thread_id).blocked)
        self.api.gate.set()
        self.assertEqual(wait_terminal(owner, thread_id)["status"], "failed")
        self.api.mode = "success"
        c = Controller(self.ledger, apply=True, cooldown=0)
        result = c.process(adapter, thread_id)
        self.assertEqual(result["action"], "resumed", result)
        self.assertEqual(wait_terminal(owner, thread_id)["status"], "completed")
        self.assertEqual(c.process(adapter, thread_id)["action"], "skip")

    def test_background_watch_recovers_when_owner_appears_later(self):
        sock = self.root / "late-owner.sock"
        state = self.root / "watch-state"
        cfg = self.root / "watch.json"
        cfg.write_text(json.dumps({"state_dir": str(state), "interval_seconds": 1,
            "retry_cooldown_seconds": 1, "max_retry_delay_seconds": 2,
            "adapters": [{"kind": "codex", "name": "late", "namespace": "late",
                           "socket_path": str(sock), "enroll_loaded_threads": True}]}))
        watcher = subprocess.Popen([sys.executable, "-m", "resume_controller", "--config", str(cfg),
                                    "--apply", "--watch"], env=self.env,
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            deadline = time.monotonic() + 5
            while not (state / "events.jsonl").exists() and time.monotonic() < deadline:
                time.sleep(.02)
            self.assertIn("adapter_error", (state / "events.jsonl").read_text())
            log = tempfile.TemporaryFile(mode="w+")
            server = subprocess.Popen(["codex", "app-server", "--listen", f"unix://{sock}"],
                                      env=self.env, stdout=log, stderr=log)
            self.servers.append((server, log))
            deadline = time.monotonic() + 10
            while not sock.exists() and time.monotonic() < deadline:
                if server.poll() is not None:
                    log.seek(0)
                    self.fail("restart fixture owner exited: " + log.read()[-2500:])
                time.sleep(.02)
            owner = UnixSocketRpc(sock)
            self.extra_rpcs.append(owner)
            thread_id = owner.call("thread/start", {"model": "controller-fixture", "modelProvider": "fixture",
                "cwd": str(self.root), "sandbox": "read-only", "approvalPolicy": "never"})["thread"]["id"]
            owner.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": "Reply OK."}]})
            wait_terminal(owner, thread_id)
            self.api.mode = "success"
            deadline = time.monotonic() + 10
            completed = False
            while time.monotonic() < deadline:
                turn = latest_turn(owner, thread_id)
                if turn.get("status") == "completed":
                    completed = True
                    break
                time.sleep(.1)
            self.assertTrue(completed, "background controller did not recover the failed turn")
            self.assertIsNone(watcher.poll(), "watcher must keep running after connection recovery")
            events = (state / "events.jsonl").read_text()
            self.assertIn('"action": "resumed"', events)
            self.assertNotIn("needs_attention", events)
        finally:
            watcher.terminate()
            watcher.communicate(timeout=5)

    def test_normal_interactive_cli_is_recovered_without_special_launch_flags(self):
        # Codex 0.164.0's normal TUI uses its shared daemon by default. Prove
        # recovery works without asking the user to start a special remote CLI.
        with (self.root / "codex-home/config.toml").open("a") as handle:
            handle.write(f'\n[projects."{self.root}"]\ntrust_level = "trusted"\n')
        master, slave = pty.openpty()
        tui = subprocess.Popen(["codex", "--no-alt-screen", "--ask-for-approval", "never",
                                "--sandbox", "read-only", "Reply OK."], cwd=self.root,
                               env={**self.env, "TERM": "xterm-256color"},
                               stdin=slave, stdout=slave, stderr=slave, start_new_session=True)
        os.close(slave)
        stopped = threading.Event()
        def terminal():
            while not stopped.is_set():
                ready, _, _ = select.select([master], [], [], .1)
                if ready:
                    try:
                        chunk = os.read(master, 65536)
                        if b'\x1b[6n' in chunk:
                            os.write(master, b'\x1b[1;1R')
                    except OSError:
                        return
        pump = threading.Thread(target=terminal, daemon=True)
        pump.start()
        try:
            sock = self.root / "codex-home/app-server-control/app-server-control.sock"
            deadline = time.monotonic() + 25
            while (not sock.exists() or not self.api.requests) and time.monotonic() < deadline:
                self.assertIsNone(tui.poll(), "normal Codex TUI exited during startup")
                time.sleep(.05)
            self.assertTrue(self.api.requests, "normal CLI did not reach the local model fixture")
            self.assertTrue(sock.exists(), "normal CLI did not expose its owner socket")
            rpc = UnixSocketRpc(sock)
            self.extra_rpcs.append(rpc)
            ids = rpc.call("thread/loaded/list")["data"]
            # The TUI may preload another empty conversation. Select its actual
            # failed model turn instead of assuming exactly one loaded thread.
            persistent = [thread_id for thread_id in ids
                          if not rpc.call("thread/read", {"threadId": thread_id})["thread"].get("ephemeral")]
            self.assertEqual(len(persistent), 1)
            thread_id = persistent[0]
            self.assertEqual(wait_terminal(rpc, thread_id)["status"], "failed")
            self.api.mode = "success"
            adapter = CodexAdapter({"namespace": "normal-cli", "enroll_loaded_threads": True}, rpc=rpc)
            result = Controller(self.ledger, apply=True, cooldown=0).process(adapter, thread_id)
            self.assertEqual(result["action"], "resumed", result)
            self.assertEqual(wait_terminal(rpc, thread_id)["status"], "completed")
            self.assertIsNone(tui.poll(), "the original TUI should remain open")
        finally:
            if tui.poll() is None:
                os.killpg(tui.pid, signal.SIGTERM)
                try:
                    tui.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(tui.pid, signal.SIGKILL)
                    tui.wait(timeout=3)
            stopped.set()
            pump.join(timeout=2)
            os.close(master)
            # The daemon is intentionally independent of the TUI process group.
            subprocess.run(["codex", "app-server", "daemon", "stop"], env=self.env,
                           capture_output=True, timeout=20)

    def test_resolved_daemon_entrypoint_recovers_work_under_service_path(self):
        config = prepare_config({"adapters": [{"kind": "codex", "ensure_owner_daemon": True}]}, Path.home())
        command = config["adapters"][0]["daemon_command"]
        environment = {**self.env, "PATH": SERVICE_PATH}
        owner = None
        try:
            started = subprocess.run([*command, "app-server", "daemon", "start"], env=environment,
                                     capture_output=True, text=True, timeout=20)
            self.assertEqual(started.returncode, 0, started.stderr)
            sock = self.root / "codex-home/app-server-control/app-server-control.sock"
            owner = UnixSocketRpc(sock)
            thread_id = owner.call("thread/start", {"model": "controller-fixture", "modelProvider": "fixture",
                "cwd": str(self.root), "sandbox": "read-only", "approvalPolicy": "never"})["thread"]["id"]
            owner.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": "Reply OK."}]})
            self.assertEqual(wait_terminal(owner, thread_id)["status"], "failed")
            self.api.mode = "success"
            adapter = CodexAdapter({"namespace": "service-path", "thread_ids": [thread_id]}, rpc=owner)
            self.assertEqual(Controller(self.ledger, apply=True, cooldown=0).process(adapter, thread_id)["action"], "resumed")
            self.assertEqual(wait_terminal(owner, thread_id)["status"], "completed")
        finally:
            if owner is not None:
                owner.close()
            stopped = subprocess.run([*command, "app-server", "daemon", "stop"], env=environment,
                                     capture_output=True, text=True, timeout=20)
            self.assertEqual(stopped.returncode, 0, stopped.stderr)

    def test_owner_restart_restores_enrolled_work_and_exact_permissions_only(self):
        foreign = self.start("Unrelated private-owner fixture")
        wait_terminal(self.rpc, foreign)
        sock = self.root / "restart-owner.sock"
        def server_start():
            log = tempfile.TemporaryFile(mode="w+")
            server = subprocess.Popen(["codex", "app-server", "--listen", f"unix://{sock}"],
                                      env=self.env, stdout=log, stderr=log)
            self.servers.append((server, log))
            deadline = time.monotonic() + 10
            while not sock.exists() and time.monotonic() < deadline:
                if server.poll() is not None:
                    log.seek(0)
                    self.fail("restart fixture owner exited: " + log.read()[-2500:])
                time.sleep(.02)
            rpc = UnixSocketRpc(sock)
            self.extra_rpcs.append(rpc)
            return server, rpc
        server, owner = server_start()
        created = owner.call("thread/start", {"model": "controller-fixture", "modelProvider": "fixture",
            "cwd": str(self.root), "sandbox": "read-only", "approvalPolicy": "on-request",
            "config": {"model_reasoning_effort": "high"}, "serviceTier": "priority"})
        thread_id = created["thread"]["id"]
        owner.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": "Reply OK."}]})
        wait_terminal(owner, thread_id)
        before = settings_snapshot(owner.call("thread/resume", {"threadId": thread_id, "excludeTurns": True}))
        config = {"namespace": "restart-owner", "thread_ids": [foreign], "enroll_loaded_threads": True}
        adapter = CodexAdapter(config, rpc=owner, registry=self.ledger)
        adapter.send = lambda *_: (_ for _ in ()).throw(ConnectionError("before delivery"))
        self.assertEqual(Controller(self.ledger, apply=True, cooldown=0).process(adapter, thread_id)["action"], "deferred")
        old_turn = latest_turn(self.rpc, foreign)["id"]
        self.ledger.close()
        self.ledger = Ledger(self.root / "ledger.sqlite")
        owner.close()
        server.terminate()
        server.wait(timeout=10)
        sock.unlink(missing_ok=True)
        home_config = self.root / "codex-home/config.toml"
        home_config.write_text('model_reasoning_effort = "low"\napproval_policy = "never"\nsandbox_mode = "danger-full-access"\n' + home_config.read_text())
        _, restarted = server_start()
        restored = CodexAdapter(config, rpc=restarted, registry=self.ledger)
        ids = restored.list_ids()
        self.assertIn(thread_id, ids, restored.restoration_warnings)
        loaded = restarted.call("thread/loaded/list")["data"]
        self.assertIn(thread_id, loaded)
        self.assertNotIn(foreign, loaded)
        after = settings_snapshot(restarted.call("thread/resume", {"threadId": thread_id, "excludeTurns": True}))
        self.assertEqual(after, before)
        self.assertIn("ownership", restored.observe(foreign).blocked)
        self.api.mode = "success"
        c = Controller(self.ledger, apply=True, cooldown=0, reconciliation_grace=0)
        result = c.process(restored, thread_id)
        self.assertEqual(result["action"], "replayed", result)
        self.assertEqual(wait_terminal(restarted, thread_id)["status"], "completed")
        self.assertEqual(latest_turn(self.rpc, foreign)["id"], old_turn)


if __name__ == "__main__":
    unittest.main()
