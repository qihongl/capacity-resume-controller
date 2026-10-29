import json
import os
import plistlib
import subprocess
import tempfile
import unittest
import itertools
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from install_service import install, LABEL, prepare_config, validate_commands, SERVICE_PATH
from make_launch_agent import generate
from resume_controller.core import exclusive_lock


class Builder:
    def create(self, path):
        (path / "bin").mkdir(parents=True)
        (path / "bin/python").touch()


class Runner:
    def __init__(self, fail_pip=False, fail_first_bootstrap=False):
        self.calls, self.loaded, self.bootstraps = [], True, 0
        self.fail_pip, self.fail_first_bootstrap = fail_pip, fail_first_bootstrap

    def __call__(self, args, **kwargs):
        self.calls.append(args)
        if "pip" in args and self.fail_pip:
            raise subprocess.CalledProcessError(1, args)
        if args[:2] == ["launchctl", "print"]:
            return SimpleNamespace(returncode=0 if self.loaded else 1,
                                   stdout="pid = 200\nstate = running\n" if self.loaded else "")
        if args[:2] == ["launchctl", "bootout"]:
            self.loaded = False
        if args[:2] == ["launchctl", "bootstrap"]:
            self.bootstraps += 1
            if self.fail_first_bootstrap and self.bootstraps == 1:
                raise subprocess.CalledProcessError(1, args)
            self.loaded = True
        return SimpleNamespace(returncode=0, stdout="")


class InstallerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.home = Path(self.tmp.name)
        self.root = self.home / "installed"
        self.root.mkdir()
        self.config = self.root / "config.local.json"
        self.original = json.dumps({"state_dir": str(self.home / "state"), "adapters": []}).encode()
        self.config.write_bytes(self.original)
        self.plist = self.home / "Library/LaunchAgents" / (LABEL + ".plist")
        self.plist.parent.mkdir(parents=True)
        self.old_plist = plistlib.dumps({"Label": LABEL, "ProgramArguments": ["old-service"]})
        self.plist.write_bytes(self.old_plist)
        self.source = Path(__file__).resolve().parents[1]

    def tearDown(self):
        self.tmp.cleanup()

    def perform(self, runner, **kwargs):
        return install(self.source, self.root, home=self.home, runner=runner, builder=Builder(), **kwargs)

    def test_dependency_failure_leaves_working_service_untouched(self):
        runner = Runner(fail_pip=True)
        with self.assertRaises(subprocess.CalledProcessError):
            self.perform(runner)
        self.assertTrue(runner.loaded)
        self.assertFalse(any(call[:2] == ["launchctl", "bootout"] for call in runner.calls))
        self.assertEqual(self.config.read_bytes(), self.original)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)

    def test_invalid_configuration_is_rejected_before_service_changes(self):
        candidate = self.home / "invalid.json"
        candidate.write_text('{"max_attempts":4}')
        runner = Runner()
        with self.assertRaises(ValueError):
            self.perform(runner, config_override=candidate)
        self.assertFalse(runner.calls)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)

    def test_bootstrap_failure_restores_registration_and_configuration(self):
        runner = Runner(fail_first_bootstrap=True)
        with self.assertRaises(subprocess.CalledProcessError):
            self.perform(runner)
        self.assertTrue(runner.loaded)
        self.assertEqual(runner.bootstraps, 2)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)
        self.assertEqual(self.config.read_bytes(), self.original)

    def test_transient_bootstrap_eio_is_retried_during_cutover_and_rollback(self):
        class TransientRunner(Runner):
            def __call__(self, args, **kwargs):
                if args[:2] == ["launchctl", "bootstrap"]:
                    self.bootstraps += 1
                    if self.bootstraps in (1, 3):
                        raise subprocess.CalledProcessError(5, args)
                    if self.bootstraps == 2:
                        raise subprocess.CalledProcessError(1, args)
                    self.loaded = True
                    return SimpleNamespace(returncode=0, stdout="")
                return super().__call__(args, **kwargs)
        runner = TransientRunner()
        with self.assertRaises(subprocess.CalledProcessError):
            self.perform(runner)
        self.assertEqual(runner.bootstraps, 4)
        self.assertTrue(runner.loaded)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)
        self.assertEqual(self.config.read_bytes(), self.original)

    def test_bootout_waits_for_label_and_old_process_exit_before_bootstrap(self):
        class DelayedRunner(Runner):
            remaining = 0
            def __call__(self, args, **kwargs):
                if args[:2] == ["launchctl", "bootout"]:
                    result = super().__call__(args, **kwargs)
                    self.remaining = 2
                    return result
                if args[:2] == ["launchctl", "print"] and self.remaining:
                    return SimpleNamespace(returncode=0, stdout="pid = 200\n")
                if args[:2] == ["/bin/ps", "-p"] and self.remaining:
                    self.remaining -= 1
                    return SimpleNamespace(returncode=0, stdout="S\n" if self.remaining else "")
                if args[:2] == ["launchctl", "bootstrap"] and self.remaining:
                    raise AssertionError("bootstrap occurred while the old service was unloading")
                return super().__call__(args, **kwargs)
        runner = DelayedRunner()
        self.perform(runner)
        self.assertTrue(runner.loaded)
        self.assertEqual(runner.remaining, 0)

    def test_rollback_retains_previous_pid_after_label_disappears_and_timeout(self):
        class StuckRunner(Runner):
            def __call__(self, args, **kwargs):
                if args[:2] == ["/bin/ps", "-p"]:
                    return SimpleNamespace(returncode=0, stdout="S\n")
                return super().__call__(args, **kwargs)
        runner = StuckRunner()
        ticks = itertools.count()
        with patch("install_service.time.monotonic", side_effect=lambda: next(ticks)), \
             patch("install_service.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "saved files restored.*confirm service shutdown"):
                self.perform(runner)
        self.assertEqual(runner.bootstraps, 0)
        self.assertEqual(self.config.read_bytes(), self.original)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)

    def test_rollback_cleanup_error_cannot_skip_saved_file_restoration(self):
        class CleanupFailureRunner(Runner):
            stops = 0
            def __call__(self, args, **kwargs):
                if args[:2] == ["launchctl", "bootout"]:
                    self.stops += 1
                    if self.stops == 2:
                        raise subprocess.CalledProcessError(5, args)
                if args[:2] == ["launchctl", "print"] and self.bootstraps:
                    return SimpleNamespace(returncode=0, stdout="state = waiting\n")
                return super().__call__(args, **kwargs)
        runner = CleanupFailureRunner()
        ticks = itertools.count()
        with patch("install_service.time.monotonic", side_effect=lambda: next(ticks)), \
             patch("install_service.time.sleep"):
            with self.assertRaisesRegex(RuntimeError, "saved files restored.*confirm service shutdown"):
                self.perform(runner)
        self.assertEqual(runner.bootstraps, 1)
        self.assertEqual(self.config.read_bytes(), self.original)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)

    def test_minimal_launchd_path_uses_absolute_cli_paths_and_stages_first(self):
        for name in [".t3/bin/t3", ".local/bin/codex"]:
            path = self.home / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("#!/bin/sh\nexit 0\n")
            path.chmod(0o700)
        self.config.write_text(json.dumps({"state_dir": str(self.home / "state"), "adapters": [
            {"kind": "t3", "command": ["t3"], "projects": ["*"]},
            {"kind": "codex", "ensure_owner_daemon": True}]}))
        runner = Runner()
        with patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}):
            result = self.perform(runner)
        config = json.loads(self.config.read_text())
        self.assertEqual(config["adapters"][0]["command"], [str(self.home / ".t3/bin/t3")])
        self.assertEqual(config["adapters"][1]["daemon_command"], [str(self.home / ".local/bin/codex")])
        self.assertEqual(self.config.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.root.stat().st_mode & 0o777, 0o700)
        pip_index = next(i for i, call in enumerate(runner.calls) if "pip" in call)
        stop_index = next(i for i, call in enumerate(runner.calls) if call[:2] == ["launchctl", "bootout"])
        self.assertLess(pip_index, stop_index)
        plist = plistlib.loads(self.plist.read_bytes())
        self.assertEqual(plist["WorkingDirectory"], result["release"])
        self.assertNotEqual(plist["WorkingDirectory"], str(self.root))

    def test_prepare_only_does_not_reconfigure_an_active_service(self):
        runner = Runner()
        result = self.perform(runner, bootstrap=False)
        self.assertTrue(result["prepared_only"])
        self.assertEqual(self.config.read_bytes(), self.original)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)
        self.assertTrue(runner.loaded)

    def test_concurrent_installer_is_rejected_without_service_changes(self):
        runner = Runner()
        with exclusive_lock(self.root / "install.lock"):
            with self.assertRaises(RuntimeError):
                self.perform(runner)
        self.assertFalse(runner.calls)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)

    def test_standalone_plist_resolves_t3_under_minimal_path(self):
        path = self.home / ".t3/bin/t3"
        path.parent.mkdir(parents=True)
        path.write_text("#!/bin/sh\nexit 0\n")
        path.chmod(0o700)
        self.config.write_text(json.dumps({"adapters": [{"kind": "t3", "command": ["t3"]}]}))
        with patch.dict(os.environ, {"PATH": "/usr/bin:/bin"}), patch("pathlib.Path.home", return_value=self.home):
            output = generate(self.config, self.root, apply=True)
        plist = plistlib.loads(output.read_bytes())
        config = json.loads(Path(plist["ProgramArguments"][4]).read_text())
        self.assertEqual(config["adapters"][0]["command"], [str(path)])

    def test_env_node_wrapper_resolves_interpreter_and_starts_on_service_path(self):
        wrapper = self.home / ".local/bin/codex"
        wrapper.parent.mkdir(parents=True)
        wrapper.write_text("#!/usr/bin/env node\nunused fixture script\n")
        wrapper.chmod(0o700)
        interpreter = wrapper.parent / "node"
        interpreter.write_text("#!/bin/sh\nexit 0\n")
        interpreter.chmod(0o700)
        config = {"adapters": [{"kind": "codex", "ensure_owner_daemon": True}]}
        with patch.dict(os.environ, {"PATH": SERVICE_PATH}):
            prepare_config(config, self.home)
        self.assertEqual(config["adapters"][0]["daemon_command"], [str(interpreter), str(wrapper)])
        validate_commands(config, self.home)

    def test_unstartable_cli_is_rejected_before_cutover(self):
        wrapper = self.home / "codex"
        wrapper.write_text("#!/bin/sh\nexit 127\n")
        wrapper.chmod(0o700)
        self.config.write_text(json.dumps({"adapters": [{"kind": "codex", "ensure_owner_daemon": True,
                                                       "daemon_command": [str(wrapper)]}]}))
        runner = Runner()
        base = runner.__call__
        def checked(args, **kwargs):
            if args[-1:] == ["--version"]:
                return subprocess.run(args, **kwargs)
            return base(args, **kwargs)
        with self.assertRaisesRegex(ValueError, "service environment"):
            self.perform(checked)
        self.assertTrue(runner.loaded)
        self.assertFalse(runner.calls)
        self.assertEqual(self.plist.read_bytes(), self.old_plist)


if __name__ == "__main__":
    unittest.main()
