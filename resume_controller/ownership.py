"""Durable enrollment and OS-verified owner-exit checks for local Codex."""
import os
import socket
import struct
import subprocess
import sys


def process_identity(pid):
    result = subprocess.run(["/bin/ps", "-p", str(pid), "-o", "uid=,lstart="],
                            capture_output=True, text=True, timeout=3,
                            env={**os.environ, "LC_ALL": "C", "TZ": "UTC"})
    fields = result.stdout.strip().split(None, 1)
    if result.returncode or len(fields) != 2:
        return None
    return {"pid": pid, "uid": int(fields[0]), "birth": fields[1]}


def socket_owner(sock, path):
    try:
        if sys.platform == "darwin":
            # macOS SDK sys/un.h: SOL_LOCAL=0, LOCAL_PEERPID=0x002.
            pid = struct.unpack("i", sock.getsockopt(0, 0x002, 4))[0]
        elif hasattr(socket, "SO_PEERCRED"):
            pid, _, _ = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
        else:
            return None
        identity = process_identity(pid)
        if identity is None or identity["uid"] != os.getuid():
            return None
        return {**identity, "socket_path": str(path.resolve())}
    except (OSError, ValueError, subprocess.SubprocessError):
        return None


def previous_owner_exited(previous, current):
    if not previous or not current or previous.get("socket_path") != current.get("socket_path"):
        return False
    if previous.get("uid") != os.getuid() or current.get("uid") != os.getuid() or previous == current:
        return False
    try:
        os.kill(previous["pid"], 0)
    except ProcessLookupError:
        return True
    except PermissionError:
        return False
    # PIDs may be reused. Failure to inspect a live PID is not proof of exit.
    live = process_identity(previous["pid"])
    return live is not None and live.get("birth") != previous.get("birth")


SETTINGS = ("model", "modelProvider", "reasoningEffort", "serviceTier", "sandbox",
            "approvalPolicy", "approvalsReviewer", "cwd")


def settings_snapshot(response):
    snapshot = {key: response.get(key) for key in SETTINGS}
    snapshot["collaborationMode"] = (response.get("collaborationMode") or {}).get("mode")
    return snapshot


def resume_parameters(thread_id, settings):
    policy = settings["sandbox"] or {}
    mode = {"readOnly": "read-only", "workspaceWrite": "workspace-write", "dangerFullAccess": "danger-full-access"}.get(policy.get("type"))
    if mode is None:
        raise RuntimeError("stored sandbox cannot be restored without changing permissions")
    config = {}
    if settings.get("reasoningEffort") is not None:
        config["model_reasoning_effort"] = settings["reasoningEffort"]
    if policy["type"] == "workspaceWrite":
        config["sandbox_workspace_write"] = {
            "writable_roots": policy.get("writableRoots", []),
            "network_access": policy.get("networkAccess", False),
            "exclude_tmpdir_env_var": policy.get("excludeTmpdirEnvVar", False),
            "exclude_slash_tmp": policy.get("excludeSlashTmp", False),
        }
    elif policy["type"] == "readOnly":
        config["sandbox_read_only"] = {"network_access": policy.get("networkAccess", False)}
    result = {"threadId": thread_id, "excludeTurns": True, "model": settings["model"],
              "modelProvider": settings["modelProvider"], "cwd": settings["cwd"],
              "approvalPolicy": settings["approvalPolicy"], "approvalsReviewer": settings["approvalsReviewer"],
              "sandbox": mode, "config": config}
    if settings.get("serviceTier") is not None:
        result["serviceTier"] = settings["serviceTier"]
    return result
