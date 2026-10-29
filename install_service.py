"""Stage, validate and atomically switch a per-user macOS service, with rollback."""
import argparse
import json
import os
import plistlib
import re
import shlex
import shutil
import subprocess
import tempfile
import time
import uuid
import venv
from pathlib import Path

from resume_controller.configuration import validate_config
from resume_controller.core import exclusive_lock

LABEL = "local.capacity-resume-controller"
SERVICE_PATH = "/usr/bin:/bin:/usr/sbin:/sbin"
FILES = ["pyproject.toml", "requirements.txt", "config.example.json", "README.md", "VALIDATION.md",
         "install_service.py", "make_launch_agent.py"]
DIRECTORIES = ["resume_controller", "tests", "docs"]


def atomic_write(path, data, mode=0o600):
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        try:
            os.fchmod(handle.fileno(), mode)
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def executable(command, fallbacks=()):
    command = list(command)
    found = shutil.which(os.path.expanduser(command[0]))
    if not found:
        found = next((str(p) for p in fallbacks if p.is_file() and os.access(p, os.X_OK)), None)
    if not found:
        raise ValueError(f"Required executable is unavailable: {command[0]}")
    command[0] = str(Path(found).absolute())
    with Path(command[0]).open("rb") as handle:
        line = handle.read(512).split(b"\n", 1)[0]
    if line.startswith(b"#!"):
        shebang = shlex.split(line[2:].decode())
        if shebang and shebang[0] == "/usr/bin/env":
            arguments = shebang[1:]
            if arguments[:1] == ["-S"]:
                arguments = arguments[1:]
            if not arguments or arguments[0].startswith("-") or "=" in arguments[0]:
                raise ValueError(f"Unsupported env shebang; configure its interpreter explicitly: {command[0]}")
            interpreter = shutil.which(arguments[0])
            sibling = Path(command[0]).parent / arguments[0]
            if interpreter is None and sibling.is_file() and os.access(sibling, os.X_OK):
                interpreter = str(sibling)
            if interpreter is None:
                raise ValueError(f"Required interpreter is unavailable: {arguments[0]}")
            command = [str(Path(interpreter).absolute()), *arguments[1:], *command]
    return command


def prepare_config(config, home):
    validate_config(config)
    for adapter in config.get("adapters", []):
        if not adapter.get("enabled", True):
            continue
        if adapter["kind"] == "t3":
            command = adapter.get("command", ["t3"])
            adapter["command"] = executable(command, [home / ".t3/bin/t3"] if command[0] == "t3" else [])
            adapter.setdefault("base_dir", str(home / ".t3"))
        elif "command" in adapter:
            adapter["command"] = executable(adapter["command"])
        elif adapter.get("ensure_owner_daemon", False):
            command = adapter.get("daemon_command", ["codex"])
            adapter["daemon_command"] = executable(command,
                [home / ".local/bin/codex", home / ".codex/bin/codex"] if command[0] == "codex" else [])
    return config


def validate_commands(config, home, runner=subprocess.run):
    """Check supported CLI entry points using the environment launchd will supply."""
    environment = {"PATH": SERVICE_PATH, "HOME": str(home), "LANG": "en_US.UTF-8"}
    for adapter in config.get("adapters", []):
        if not adapter.get("enabled", True):
            continue
        command = (adapter.get("command") if adapter["kind"] == "t3" else
                   adapter.get("daemon_command") if adapter.get("ensure_owner_daemon") and "command" not in adapter else None)
        if command:
            result = runner([*command, "--version"], env=environment, capture_output=True, text=True, timeout=20)
            if result.returncode:
                raise ValueError(f"{adapter['kind']} CLI cannot start in the service environment")


def copy_source(source, destination):
    for directory in DIRECTORIES:
        if (source / directory).exists() and source != destination:
            shutil.copytree(source / directory, destination / directory, dirs_exist_ok=True,
                            ignore=shutil.ignore_patterns("__pycache__"))
    for name in FILES:
        if (source / name).exists() and source != destination:
            shutil.copy2(source / name, destination / name)


def unload_service(target, runner, timeout=15, retiring_pids=None):
    retiring_pids = set() if retiring_pids is None else retiring_pids
    status = runner(["launchctl", "print", target], capture_output=True, text=True)
    if not status.returncode:
        match = re.search(r"^\s*pid = (\d+)", status.stdout, re.M)
        if match:
            retiring_pids.add(match.group(1))
        runner(["launchctl", "bootout", target], check=True, capture_output=True, text=True)
    deadline = time.monotonic() + timeout
    while True:
        status = runner(["launchctl", "print", target], capture_output=True, text=True)
        for pid in list(retiring_pids):
            process = runner(["/bin/ps", "-p", pid, "-o", "stat="], capture_output=True, text=True)
            alive = process.returncode == 0 and process.stdout.strip() and not process.stdout.lstrip().startswith("Z")
            if not alive:
                retiring_pids.remove(pid)
        if status.returncode and not retiring_pids:
            return
        if time.monotonic() >= deadline:
            raise RuntimeError("previous service did not finish unloading")
        time.sleep(.1)


def bootstrap_service(domain, plist_path, runner, timeout=15):
    # launchd can return EIO briefly after a successful bootout, even once print
    # reports the label absent. Apply the same bounded retry during rollback.
    deadline = time.monotonic() + timeout
    while True:
        try:
            runner(["launchctl", "bootstrap", domain, str(plist_path)], check=True,
                   capture_output=True, text=True)
            return
        except subprocess.CalledProcessError as exc:
            if exc.returncode != 5 or time.monotonic() >= deadline:
                raise
            time.sleep(.25)


def install(source, root, *, config_override=None, bootstrap=True, home=None,
            runner=subprocess.run, builder=None):
    root = Path(root).expanduser().resolve()
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid():
        raise PermissionError("installation directory is not owned by the current user")
    root.chmod(0o700)
    with exclusive_lock(root / "install.lock"):
        return _install(source, root, config_override=config_override, bootstrap=bootstrap,
                        home=home, runner=runner, builder=builder)


def _install(source, root, *, config_override=None, bootstrap=True, home=None,
             runner=subprocess.run, builder=None):
    source, root = Path(source).resolve(), Path(root).expanduser().resolve()
    home = Path.home() if home is None else Path(home)
    config_path = root / "config.local.json"
    config_source = Path(config_override).expanduser() if config_override else (
        config_path if config_path.exists() else source / "config.example.json")
    # Validate and resolve commands before changing any live files or services.
    config = prepare_config(json.loads(config_source.read_text()), home)
    validate_commands(config, home, runner)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    if root.stat().st_uid != os.getuid():
        raise PermissionError("installation directory is not owned by the current user")
    root.chmod(0o700)
    release = root / "releases" / uuid.uuid4().hex
    release.mkdir(mode=0o700, parents=True)
    copy_source(source, release)
    environment = release / ".venv"
    (builder or venv.EnvBuilder(with_pip=True)).create(environment)
    python = environment / "bin/python"
    runner([str(python), "-m", "pip", "install", "--disable-pip-version-check",
            "-r", str(release / "requirements.txt")], check=True)
    checked_config = release / "config.check.json"
    encoded_config = (json.dumps(config, indent=2) + "\n").encode()
    atomic_write(checked_config, encoded_config)
    runner([str(python), "-c", "import json,sys,websocket; from resume_controller.configuration import validate_config; "
            "validate_config(json.load(open(sys.argv[1])))", str(checked_config)], cwd=release, check=True)
    state = Path(config.get("state_dir", "~/.local/state/resume-controller")).expanduser().resolve()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)
    plist_path = home / "Library/LaunchAgents" / (LABEL + ".plist")
    domain = f"gui/{os.getuid()}"
    target = f"{domain}/{LABEL}"
    old_config = config_path.read_bytes() if config_path.exists() else None
    old_plist = plist_path.read_bytes() if plist_path.exists() else None
    was_loaded = runner(["launchctl", "print", target], capture_output=True, text=True).returncode == 0
    candidate = plistlib.dumps({
        "Label": LABEL, "ProgramArguments": [str(python), "-m", "resume_controller", "--config",
            str(config_path), "--apply", "--watch"], "WorkingDirectory": str(release),
        "RunAtLoad": True, "KeepAlive": True, "ThrottleInterval": 30,
        "StandardOutPath": str(state / "service.stdout.log"),
        "StandardErrorPath": str(state / "service.stderr.log"),
    })
    candidate_plist = release / (LABEL + ".plist")
    atomic_write(candidate_plist, candidate, 0o644)
    runner(["plutil", "-lint", str(candidate_plist)], check=True)
    if not bootstrap and was_loaded:
        return {"root": str(root), "release": str(release), "started": False, "prepared_only": True}
    retiring_pids = set()
    try:
        if was_loaded:
            unload_service(target, runner, retiring_pids=retiring_pids)
        atomic_write(config_path, encoded_config)
        atomic_write(plist_path, candidate, 0o644)
        if bootstrap:
            bootstrap_service(domain, plist_path, runner)
            deadline = time.monotonic() + 10
            while time.monotonic() < deadline:
                status = runner(["launchctl", "print", target], capture_output=True, text=True)
                if status.returncode == 0 and re.search(r"^\s*pid = \d+", status.stdout, re.M):
                    break
                time.sleep(.2)
            else:
                raise RuntimeError("replacement service did not start")
    except BaseException:
        # Restore the exact previous registration and configuration on failure.
        cleanup_error = None
        try:
            unload_service(target, runner, retiring_pids=retiring_pids)
        except BaseException as exc:
            cleanup_error = exc
        restoration_errors = []
        for path, data, mode in ((config_path, old_config, 0o600), (plist_path, old_plist, 0o644)):
            try:
                if data is None:
                    path.unlink(missing_ok=True)
                else:
                    atomic_write(path, data, mode)
            except BaseException as exc:
                restoration_errors.append(exc)
        if restoration_errors:
            raise RuntimeError("rollback could not restore all saved files; no previous service was bootstrapped") from restoration_errors[0]
        if cleanup_error is not None:
            raise RuntimeError("saved files restored, but rollback could not confirm service shutdown; no previous service was bootstrapped") from cleanup_error
        if was_loaded and old_plist is not None:
            bootstrap_service(domain, plist_path, runner)
        raise
    # The live process imports only from its immutable release. This convenience
    # copy can now be refreshed without overwriting a running Python package.
    copy_source(release, root)
    link = root / (".current-" + uuid.uuid4().hex)
    link.symlink_to(release.relative_to(root), target_is_directory=True)
    os.replace(link, root / "current")
    return {"root": str(root), "release": str(release), "config": str(config_path),
            "plist": str(plist_path), "started": bootstrap}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, default=Path("~/.local/share/capacity-resume-controller"))
    parser.add_argument("--config", type=Path)
    parser.add_argument("--no-bootstrap", action="store_true")
    args = parser.parse_args()
    print(json.dumps(install(Path(__file__).resolve().parent, args.root,
                             config_override=args.config, bootstrap=not args.no_bootstrap)))


if __name__ == "__main__":
    main()
