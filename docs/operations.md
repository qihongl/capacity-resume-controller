# Capacity resume controller

A local program checks T3 Code and Codex for **terminal capacity/overload
failures** and sends a short continuation in the existing conversation. It makes
**no supervisor model calls**. There is no ever-growing supervisor chat, so the
supervisor needs no context compaction. Resumed workers still use their normal
model allowances and native context management.

## Run

Requirements: Python 3.10+, T3's CLI, Codex, and `websocket-client` 1.9.2.

```sh
python3 -m venv .venv
.venv/bin/pip install -e .
cp config.example.json config.local.json

# Inspect only. No continuations are sent; retry counts are not consumed.
.venv/bin/python -m resume_controller --config config.local.json

# Run one pass with continuations enabled.
.venv/bin/python -m resume_controller --config config.local.json --apply

# Run continuously, every 30 minutes, until SIGINT/SIGTERM.
.venv/bin/python -m resume_controller --config config.local.json --apply --watch
```

For unattended operation on this Mac, `python3 install_service.py` installs a
private Python environment and a per-user LaunchAgent, then starts it with retries
enabled. It starts at login and restarts after process failure. Installed code is
under `~/.local/share/capacity-resume-controller`; its configuration is
`config.local.json` in that directory.

Updates use the same installer command. It resolves CLI paths and env-shebang
interpreters such as Codex's Node entry point, checks CLI startup with launchd's
minimal PATH, and builds and validates
a separate environment under `releases/`, then switches the LaunchAgent. Dependency
or configuration failures leave the previous service running; startup failures
restore its previous configuration and registration. The handoff waits for the
old launchd registration and process to exit; transient bootstrap EIO errors are
retried with a bound during both cutover and rollback. Concurrent installers are
blocked. `current/` points to the installed release. The old release is retained.
With an existing running service, `--no-bootstrap` prepares a candidate without
changing that service. The installer never edits a running release's Python files.

The example inspects all T3 projects and enrolled/loaded Codex sessions. With
`ensure_owner_daemon: true`, it starts or
recovers Codex's default shared daemon if its socket is unavailable. Custom owner
sockets are retried without starting an unrelated server.

The controller rereads configuration before each pass. Exclusions and project
removal apply to pending deliveries as well as new discovery. Invalid updates
stop scanning until corrected. A `state_dir` change requires restarting the service.

`state_dir` holds a SQLite retry ledger, an exclusive process lock, and JSONL
events. All instances controlling an environment must share this directory.
Use a local disk; keep one controller responsible for each owner server.
Exit codes: 0 for a successful pass, 1 for inspection/connection errors. Continuous
mode logs temporary problems and tries again next pass; no approval or inspection
question is sent to the user.

## Harness connections

**T3 Code:** keep the existing T3 server running. Set `command` to its CLI path;
optionally set `base_dir` for a nondefault data directory. `projects: ["*"]`
discovers all projects; an array of project IDs selects a subset.

The adapter uses the installed `auth session issue` command to mint a five-minute,
read-only MCP session (`subject: mcp-client`). It calls T3's `/mcp` endpoint to
read structured activity, then revokes that credential when the pass ends.
Tokens stay in memory. Sending uses a separate five-minute orchestration-scoped
credential and T3's `orchestration.dispatchCommand` API. Every continuation has a
durable command ID and message ID. Replaying the same command returns the original
receipt instead of creating another turn, including after a lost response or a
controller restart. No application bundle or database is modified directly.
Authenticated HTTP and WebSocket connections reject redirects and proxy routing.
WebSocket workers connect directly to a verified loopback peer before transmitting
credentials. Each response has a
30-second total deadline and an 8 MiB size limit, including SSE streams, so
keepalives cannot prevent subsequent adapter scans indefinitely.
The deadline also covers WebSocket handshakes, partial frames and control-frame
streams. WebSocket byte limits cover advertised payload sizes and accumulated
fragments, before receiving oversized payloads.

The CLI's `thread show` output alone is insufficient: it omits terminal Codex
error activity. The real-server test catches this distinction.

**Codex:** set `socket_path` to the **existing owner server's Unix socket**.
The default example path is `~/.codex/app-server-control/app-server-control.sock`.
On the tested Codex 0.164.0, an ordinary interactive `codex` launch uses that
shared daemon automatically. The live TUI test proves a failed normal CLI turn
can be resumed by the controller while its original terminal stays open; no
special `--remote` launch flag or manual terminal input is needed.
The controller connects using WebSocket-over-Unix-socket, initializes a separate
JSON-RPC client, and calls `thread/loaded/list`, `thread/read`, and `turn/start`.
For installations supporting paginated turns, it uses `thread/turns/list`;
the installed Codex 0.164.0 requires a legacy `thread/read(includeTurns=true)`
fallback for some sessions. That fallback can read more history, but consumes
no model tokens.

For an optional custom shared owner server, use two terminals:

```sh
# Terminal 1: start the owner, using your normal Codex configuration.
codex app-server --listen unix:///absolute/private/directory/codex.sock

# Terminal 2: use that SAME owner for interactive sessions.
codex --remote unix:///absolute/private/directory/codex.sock
```

Set the controller's `socket_path` to that socket. Use a private directory
(mode 700), since socket access permits controlling its sessions.
`enroll_loaded_threads: true` monitors top-level sessions already loaded there.
For an explicit allowlist, set it to `false` and populate `thread_ids`.
`exclude_thread_ids` excludes specific native Codex IDs.

Older/custom sessions launched with private stdio servers **are not automatically
controllable** through a new server. A stored thread appearing `notLoaded` means
ownership is unverified; the controller skips it unless it has previously enrolled
that session through the same Unix owner socket. In apply mode, eligible failed
sessions are enrolled with the owner's OS process identity and effective model,
reasoning, service tier, sandbox and approval settings. After verifying that the
previous owner exited, the controller may restore only these enrolled sessions.
It explicitly restores their saved settings and compares the effective result;
a mismatch blocks continuation. Other dormant/private sessions remain unloaded.
T3-owned originators and all subagents are also skipped
by the Codex adapter. Do not enroll the same imported native session in two
different owners/adapters; use `exclude_thread_ids` for any ambiguous overlap.

Codex documents its app-server/WebSocket interface as experimental:
<https://learn.chatgpt.com/docs/app-server>. Version changes should be checked
with the integration tests before updating an unattended installation.

## Recovery policy and limits

- Only explicit terminal model-capacity, server-overload, HTTP 503/529, or exhausted
  HTTP 429 retry-limit errors qualify. Plain 429, account quotas, context overflow,
  authentication, budgets, policy failures, and unknown errors are skipped.
- T3 reads terminal **error items**, never error strings quoted in user/tool text.
  Codex reads the latest committed turn's structured error.
- Completed, interrupted, cancelled, busy, approval-blocked, settled, snoozed,
  archived, subagent and unowned threads are skipped as applicable.
- The exact failure, provider and model are reread immediately before dispatch.
  Continuations omit model, provider, cwd, reasoning, service tier and permission
  overrides. Restoring a verified enrolled session after an owner restart uses
  explicit saved settings, checked before any continuation.
- Default unattended mode has no permanent attempt cap. Capacity errors back off
  for 30 minutes, then 1 hour, 2 hours, 4 hours, and at most 6 hours between later
  attempts. Set a numeric `max_attempts` only if you explicitly want a finite cap.
  Consecutive automatic failures stay in the same episode. A human's new task
  begins a new episode.
- The attempt is durably committed **before** sending. A crash or lost response
  becomes `dispatching`/`uncertain` and is reconciled automatically. T3 checks its
  durable message ID and can replay the idempotent server command. Codex checks
  the latest turn for the request's unique marker/client ID. If absent on two
  unchanged observations at least 60 seconds apart after the last delivery, it replays the same request
  without consuming another attempt. Busy or unavailable threads are deferred.
  Every ambiguous replay starts a fresh grace window, including after a crash.
  Later user work supersedes a stale continuation. No manual reset is required.
- Failed adapters do not prevent the other adapter from being checked.

**Delivery semantics:** Codex does not deduplicate repeated `clientUserMessageId`
values; the live test verifies this explicitly. Therefore the controller checks
history before replay and allows time for commits to become visible. This is
automatic reconciliation, not a server-side exactly-once guarantee. T3 has durable
command deduplication. Neither API supplies an atomic idle-failure precondition:
if a user starts a turn during the final send window, T3 queues the continuation
and Codex may steer the active turn. This needs no user inspection, but it can
produce an extra continuation. Continuations explicitly prohibit repeating work
that a later turn has already completed. Manual stops and approvals remain respected.

No production thread was resumed during validation. Disposable integration
servers use temporary data directories and a loopback-only fake model endpoint;
the real Codex and T3 executables process those requests. No paid API calls are
made by those tests.

## Optional macOS persistence

Generate a LaunchAgent using the same Python environment as your installation:

```sh
.venv/bin/python make_launch_agent.py --config /absolute/config.local.json --apply
```

This writes `runtime/local.capacity-resume-controller.plist` but does not load it.
After the configured owner connections work, copy it into `~/Library/LaunchAgents/`
and load it with `launchctl bootstrap gui/$(id -u) /absolute/path/to/the.plist`.
Stop it with `launchctl bootout gui/$(id -u)/local.capacity-resume-controller`.
The agent runs one long-lived controller with interruptible 30-minute waits;
it restarts after process failure and writes stdout/stderr logs under `runtime/`.

## Tests

```sh
python3 -m unittest discover -v
RUN_LIVE_CODEX=1 RUN_LIVE_T3=1 python3 -m unittest discover -v
```

The opt-in suite verifies real 503 failures, successful continuation, optional finite
retry limits, active/completed skipping, provider/model/thread preservation, and
two independent clients sharing one Codex owner socket. A separate test verifies
that a different server sees the active session as `notLoaded` and cannot claim
ownership. Unit tests cover classification, dry runs, changed-state rechecks,
backoff, automatic crash/timeout reconciliation, lost acknowledgements, delayed
owner availability, and exclusion locks.

See [validation](../VALIDATION.md) for the tested installation and results.
