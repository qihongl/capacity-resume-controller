import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


class CliTests(unittest.TestCase):
    def test_watch_lock_signal_and_invalid_policy(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            config = root / "config.json"
            config.write_text(json.dumps({"state_dir": str(root / "state"), "adapters": []}))
            command = [sys.executable, "-m", "resume_controller", "--config", str(config)]
            process = subprocess.Popen(command + ["--watch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            try:
                deadline = time.monotonic() + 5
                while not (root / "state/ledger.sqlite").exists() and time.monotonic() < deadline:
                    time.sleep(.02)
                second = subprocess.run(command, capture_output=True, text=True, timeout=5)
                self.assertNotEqual(second.returncode, 0)
                self.assertIn("another controller", second.stderr)
                process.send_signal(signal.SIGTERM)
                process.communicate(timeout=5)
                self.assertEqual(process.returncode, 0)
                # Lock is released after termination.
                self.assertEqual(subprocess.run(command, capture_output=True, timeout=5).returncode, 0)
                config.write_text(json.dumps({"state_dir": str(root / "state"), "max_attempts": 4}))
                invalid = subprocess.run(command, capture_output=True, text=True, timeout=5)
                self.assertEqual(invalid.returncode, 2)
                self.assertIn("between 1 and 3", invalid.stderr)
            finally:
                if process.poll() is None:
                    process.terminate()
                    process.communicate(timeout=5)


if __name__ == "__main__":
    unittest.main()
