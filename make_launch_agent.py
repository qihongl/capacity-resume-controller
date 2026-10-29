"""Generate a macOS LaunchAgent plist; installation is an explicit CLI action.

python3 make_launch_agent.py --config /absolute/config.local.json --apply
prints the path to a generated plist; it does not load or start the agent.
"""
import argparse
import plistlib
import sys
import json
from pathlib import Path
from install_service import prepare_config, atomic_write, validate_commands


def generate(config, root, apply=False):
    root = Path(root)
    logs = root / "runtime"
    logs.mkdir(exist_ok=True)
    resolved = prepare_config(json.loads(Path(config).expanduser().read_text()), Path.home())
    validate_commands(resolved, Path.home())
    checked = logs / "launch-agent-config.json"
    atomic_write(checked, json.dumps(resolved, indent=2).encode())
    arguments = [sys.executable, "-m", "resume_controller", "--config", str(checked), "--watch"]
    if apply:
        arguments.append("--apply")
    output = logs / "local.capacity-resume-controller.plist"
    with output.open("wb") as handle:
        plistlib.dump({"Label": "local.capacity-resume-controller", "ProgramArguments": arguments,
                      "WorkingDirectory": str(root), "RunAtLoad": True, "KeepAlive": True,
                      "ThrottleInterval": 30, "StandardOutPath": str(logs / "service.stdout.log"),
                      "StandardErrorPath": str(logs / "service.stderr.log")}, handle)
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(generate(args.config, Path(__file__).resolve().parent, args.apply))


if __name__ == "__main__":
    main()
