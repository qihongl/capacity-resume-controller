"""Validate local configuration before touching services or credentials."""
import math


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("configuration must be an object")
    numeric = {"interval_seconds": (1800, 1), "retry_cooldown_seconds": (1800, 0),
               "max_retry_delay_seconds": (21600, 0), "reconciliation_grace_seconds": (60, 0)}
    for key, (default, minimum) in numeric.items():
        value = config.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum:
            raise ValueError(f"{key} must be a finite number >= {minimum}")
    if config.get("max_retry_delay_seconds", 21600) < config.get("retry_cooldown_seconds", 1800):
        raise ValueError("max_retry_delay_seconds cannot be less than retry_cooldown_seconds")
    cap = config.get("max_attempts")
    if cap is not None and (isinstance(cap, bool) or not isinstance(cap, int) or not 1 <= cap <= 3):
        raise ValueError("max_attempts must be null or between 1 and 3")
    if not isinstance(config.get("state_dir", "~/.local/state/resume-controller"), str):
        raise ValueError("state_dir must be a path string")
    adapters = config.get("adapters", [])
    if not isinstance(adapters, list):
        raise ValueError("adapters must be an array")
    names = []
    for adapter in adapters:
        if not isinstance(adapter, dict) or adapter.get("kind") not in ("t3", "codex"):
            raise ValueError("each adapter must select kind t3 or codex")
        name = adapter.get("name", adapter["kind"])
        if not isinstance(name, str) or not name:
            raise ValueError("adapter names must be nonempty strings")
        names.append(name)
        for key in ("enabled", "enroll_loaded_threads", "ensure_owner_daemon"):
            if key in adapter and not isinstance(adapter[key], bool):
                raise ValueError(f"{key} must be a boolean")
        for key in ("command", "daemon_command", "projects", "thread_ids", "exclude_thread_ids"):
            if key in adapter:
                values = adapter[key]
                if not isinstance(values, list) or any(not isinstance(v, str) or not v for v in values):
                    raise ValueError(f"{key} must be an array of nonempty strings")
                if key in ("command", "daemon_command") and not values:
                    raise ValueError(f"{key} cannot be empty")
    if len(names) != len(set(names)):
        raise ValueError("adapter names must be unique")
    return config
