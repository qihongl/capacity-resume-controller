# Validation — 2026-10-29

The deterministic controller recovered disposable T3 and Codex conversations from
terminal 503 errors using their existing thread, provider and model. The real
executables were used with a loopback fake model API; no paid model calls were
needed. This establishes the supported connection patterns, not universal access
to arbitrary private CLI processes.

## Tested installation

- macOS, Python 3.13.12
- T3 CLI v0.1.2
- Codex CLI 0.164.0
- websocket-client 1.9.2 (pinned dependency)

## Results

`RUN_LIVE_CODEX=1 RUN_LIVE_T3=1 python3 -m unittest discover -v`

The final installed v0.3.0 release passed **78 tests in 46.622 seconds** in its isolated
isolated Python environment, including real-executable tests for these cases:

1. Codex skips an active turn and a completed turn.
2. Codex retries a terminal 503 exactly three times, then reports exhaustion;
   the thread, provider and model remain the same.
3. Codex recovers successfully when the fixture stops returning 503.
4. A different app-server cannot claim a session loaded in the first server.
5. A separate controller client on the **same Unix owner socket** sees active
   work and later resumes its failed conversation to completion.
6. An isolated T3 server exposes terminal error activity and accepts a
   continuation in the same thread, which then completes successfully.
7. A lost Codex delivery response is reconciled after a controller restart without
   another model call. A connection failure before send is replayed automatically
   with the same request marker and attempt number.
8. A lost T3 acknowledgement is reconciled automatically; repeating the exact
   same server command creates no extra run, even after the T3 owner is killed
   and restarted.
9. A background controller keeps running through a missing owner socket, connects
   when the owner appears, then resumes a failed conversation without a person.
10. A protocol test confirms Codex's client message ID does not provide server-side
    deduplication; the adapter therefore checks history before replaying it.
11. A normal interactive `codex` launch creates its default shared owner socket.
    Its failed model turn is resumed to completion by the controller while the
    original TUI stays open, without special remote flags or manual input.
12. A previously enrolled failed session is restored after both the owner and
    controller restart. Its model, provider, reasoning effort, service tier,
    sandbox, approval policy and reviewer match the saved effective settings even
    when new owner defaults change to broader permissions. An unrelated private
    session remains unloaded. The pending request then completes automatically.
13. The installer-resolved Codex entry point starts an isolated daemon under
    launchd's minimal PATH. A failed conversation on that daemon is then resumed
    to completion using the loopback model fixture.

The remaining tests check classification, quota/context exclusion, pending/busy
states, changed-state rechecks, persisted retry limits, duplicate suppression,
exponential backoff without a permanent stop, interrupted/completed states,
automatic crash/timeout reconciliation, failure isolation,
cross-adapter origin exclusion, reconciliation, process locking, clean shutdown,
and invalid configuration. New regression tests verify scope changes across a
controller restart, repeated ambiguous sends with a fresh grace window each time,
HTTP heartbeat/trickle deadlines and byte limits, credential redirect rejection,
subsequent adapter progress, installer rollback/failure handling, concurrent
installer exclusion, and absolute executable/interpreter resolution under a
minimal PATH. WebSocket tests exercise dummy-token proxy bypass, rejected
redirects, partial-frame and handshake deadlines, endless protocol control frames,
advertised payload limits, fragment accumulation and subsequent adapter progress.

Independent audits and live deployment verification found thirteen actionable
issues, all fixed with regression coverage. The focused runtime review approved
the remaining three transport/interpreter fixes, with 64 passing tests and three additional socket probes verifying
cumulative complete-message limits, 64-bit frame limits before payload reception,
and partial-header timeout. The final rollback review approved its fixes with
13 installer tests and seven additional injected failure scenarios passing.
See [review details](docs/security-review.md).

Python compilation checks passed. The final LaunchAgent plist passed
`plutil -lint`; the live update completed and both production adapters scanned
successfully, with zero candidates and no stderr output. Installed Python source
checksums match the reviewed checkout. An earlier deployment's controller was
deliberately killed: launchd restarted it and both adapters completed fresh scans.
The installed service is enabled with `--apply --watch`, a 30-minute polling
interval, unlimited capacity retries with a six-hour maximum backoff, and
automatic delivery reconciliation.

## Existing installation dry run

The controller connected to the existing T3 server and scanned all projects.
It found **zero unsettled, unsnoozed failed candidates** at the time of the scan.
No production continuation was sent.

The default Codex owner daemon was started and its socket connection verified.
Both production adapters complete a dry scan successfully. No production
continuation was sent during these checks. Normal CLI sessions on the tested
version use that shared owner automatically; older/custom private stdio sessions
remain unmonitored.
The staged v0.3 controller also completed fresh read-only scans of both production
adapters, finding zero candidates, with exit code 0 and no stderr output.

## Material limits

- Neither harness offers an atomic idle-failure compare-and-send operation through
  these interfaces. Rechecking narrows the race but cannot exclude a human starting
  work immediately afterward. It no longer triggers manual inspection: accepted
  messages are tracked, busy threads are deferred, and later user work supersedes
  stale recovery requests. An extra continuation remains possible in that race.
- T3 commands have durable server-side deduplication. Codex uses two unchanged
  authoritative observations separated by a reconciliation grace period before
  replaying an absent marker. This is practical unattended recovery, not a
  server-side exactly-once guarantee.
- T3 and Codex must not independently control the same imported native session.
  Normal T3 originators are excluded; ambiguous imports need explicit exclusions.
- Arbitrary older releases, cloud-hosted Codex jobs, and servers on other machines
  were not tested. The adapters target the local installed interfaces.
- Codex's documented app-server transports are experimental. Live tests should be
  rerun after harness upgrades.
- This implementation deliberately has no LLM supervisor. It therefore has no
  supervisor context to measure or compact. Worker context-overflow failures are
  excluded from capacity retries.
