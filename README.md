# Capacity Resume Controller

Automatically resumes T3 Code and Codex conversations stopped by temporary model
capacity or server-overload errors. Runs locally without an LLM supervisor.

**Requires:** macOS, Python 3.10+, T3 Code CLI, and Codex CLI.

```sh
python3 install_service.py
```

The installer enables a background service that checks every 30 minutes, starts
at login, and retries with backoff. Models and existing conversation progress are
preserved; deliberate stops and approval requests are respected.

- Configuration: `~/.local/share/capacity-resume-controller/config.local.json`
- Logs: `~/.local/state/resume-controller/`
- Stop: `launchctl bootout gui/$(id -u)/local.capacity-resume-controller`
- Start again: `launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/local.capacity-resume-controller.plist`

See [operations](docs/operations.md) for configuration and known harness limits,
and [validation](VALIDATION.md) for the tests performed.
