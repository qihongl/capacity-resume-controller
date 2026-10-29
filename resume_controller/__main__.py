from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from pathlib import Path

from .adapters import CodexAdapter, T3Adapter
from .core import Controller, Ledger, exclusive_lock
from .configuration import validate_config


def main():
    parser = argparse.ArgumentParser(description="Resume known capacity failures without an LLM supervisor.")
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--apply", action="store_true", help="Send continuations (default: inspect only)")
    parser.add_argument("--watch", action="store_true", help="Repeat every configured interval")
    args = parser.parse_args()
    try:
        config = validate_config(json.loads(args.config.read_text()))
    except (OSError, ValueError) as exc:
        parser.error(str(exc))
    interval = config.get("interval_seconds", 1800)
    cap = config.get("max_attempts")
    state = Path(config.get("state_dir", "~/.local/state/resume-controller")).expanduser().resolve()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    os.chmod(state, 0o700)
    stop = threading.Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    # Hold one lock for the entire daemon lifetime, including sleeps.
    with exclusive_lock(state / "controller.lock"):
        ledger = Ledger(state / "ledger.sqlite")
        try:
            while not stop.is_set():
                try:
                    config = validate_config(json.loads(args.config.read_text()))
                    if Path(config.get("state_dir", "~/.local/state/resume-controller")).expanduser().resolve() != state:
                        raise ValueError("state_dir changes require a service restart")
                    interval = config.get("interval_seconds", 1800)
                    cap = config.get("max_attempts")
                except (OSError, ValueError) as exc:
                    print(json.dumps({"action": "configuration_error", "error": str(exc)}), flush=True)
                    if not args.watch:
                        return 2
                    stop.wait(min(interval, 30))
                    continue
                adapters, setup_errors = [], []
                for item in config.get("adapters", []):
                    if not item.get("enabled", True):
                        continue
                    try:
                        cls = {"t3": T3Adapter, "codex": CodexAdapter}[item["kind"]]
                        adapters.append(cls(item, registry=ledger if args.apply else None)
                                        if item["kind"] == "codex" else cls(item))
                    except Exception as exc:
                        setup_errors.append({"adapter": item.get("name", item["kind"]),
                                             "action": "adapter_error", "error": str(exc)})
                try:
                    controller = Controller(ledger, apply=args.apply,
                                            max_attempts=cap,
                                            cooldown=config.get("retry_cooldown_seconds", 1800),
                                            max_delay=config.get("max_retry_delay_seconds", 21600),
                                            reconciliation_grace=config.get("reconciliation_grace_seconds", 60))
                    results = setup_errors + controller.scan(adapters)
                    for result in results:
                        result["at"] = time.time()
                    for result in results:
                        print(json.dumps(result), flush=True)
                    with (state / "events.jsonl").open("a") as log:
                        for result in results:
                            log.write(json.dumps(result) + "\n")
                finally:
                    for adapter in adapters:
                        try:
                            adapter.close()
                        except Exception as exc:
                            print(json.dumps({"adapter": adapter.name, "action": "cleanup_deferred",
                                              "error": str(exc)}), flush=True)
                if not args.watch:
                    return 1 if any(r["action"] in ("adapter_error", "inspection_error", "needs_attention")
                                    for r in results) else 0
                stop.wait(interval)
        finally:
            ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
