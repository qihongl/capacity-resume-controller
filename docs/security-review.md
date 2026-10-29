# Security and reliability review — 2026-10-29

An independent agent reviewed the full controller before publication. Its first
round requested changes for seven reproducible findings. All seven have fixes
and regression coverage in v0.3. A separate follow-up review confirmed those fixes
and found three additional issues, addressed below.
The final focused review approved these three fixes with no actionable findings
or nearby regressions. It ran 64 passing tests plus independent socket probes
for cumulative messages, oversized 64-bit frame lengths and partial-header timeout.
The rollback changes from live deployment received a separate focused approval:
all 13 installer tests and seven additional injected failure scenarios passed.
The final installed release then passed all 78 tests.

| Finding | Change and verification |
| --- | --- |
| Pending reconciliation bypassed exclusions or removed T3 projects. | Check current scope before processing and immediately before dispatch. Restart tests remove a project or exclude a Codex thread while delivery is uncertain. Neither can send again. |
| Repeated ambiguous sends reused the old reconciliation grace. | Persist every transport timestamp and clear prior absence evidence before every send. Repeated lost replies and ledger restarts require a fresh grace window without increasing the logical attempt. |
| MCP keepalives could stall both adapters indefinitely. | A killable HTTP worker enforces a total response deadline and byte limits. Heartbeats and trickled unterminated lines time out; a subsequent adapter still runs. |
| Restarting the Codex owner left enrolled work unloaded forever. | Persist eligible session enrollment with OS-verified owner identity and effective settings. Restore only after proving that owner exited. A real owner/ledger restart test changes default permissions, verifies exact saved settings, and leaves an unrelated private session unloaded. |
| An installation failure could unload the existing service. | Prepare a separate release and environment while the old service runs. Validate before switching; restore configuration and registration on startup failure. Dependency, configuration and bootstrap failures are injected in tests. |
| HTTP redirects could forward bearer credentials outside the loopback origin. | Reject authenticated redirects and disable HTTP proxy routing. Tests use dummy credentials and cross-origin/non-loopback redirect targets; none are followed. |
| launchd's minimal PATH could not locate T3. | Resolve executable paths before installation and standalone plist generation. Tests use the minimal PATH with home-directory CLI installations. |

| Follow-up finding | Change and verification |
| --- | --- |
| T3's WebSocket could forward credentials through an environment proxy. | Connect a raw socket directly, verify its peer is loopback, and supply it to the WebSocket handshake. The actual control class is tested with a dummy token, all proxy variables set and empty NO_PROXY; the proxy receives no CONNECT or credential. |
| A trickled partial WebSocket frame could outlast the deadline. | A killable worker bounds the entire RPC, including connection and handshake. Tests cover incomplete frames, endless control frames, trickled handshake headers and subsequent adapter progress. Advertised payload sizes and accumulated fragments share an 8 MiB byte budget. |
| An absolute Codex wrapper still needed Node on PATH. | Resolve env-shebang interpreters to absolute commands, then check CLI startup under launchd's minimal PATH before cutover. Tests cover successful interpreter resolution and a wrapper failure leaving the service untouched. The installed real Codex wrapper also passes this preflight. |
| Live launchd cutover returned transient EIO before the old job finished unloading. | Wait for both label removal and old process exit, then retry bootstrap EIO with a bound during cutover and rollback. Tests inject delayed unloading and transient errors on both paths. The corrected live update completed successfully and both adapters scanned without errors. |
| Rollback could lose a still-live PID after an unload timeout. | Retain captured process IDs across both cutover and rollback. Even when the launchd label disappears, a previous process must exit before any replacement is bootstrapped. A persistent-process timeout test verifies that no bootstrap occurs. |
| Rollback cleanup failure could skip saved-file restoration. | Attempt both saved-file restorations independently of cleanup success. Bootstrap remains blocked until shutdown and file restoration are confirmed. A startup-timeout/cleanup-EIO test verifies exact restoration and no duplicate bootstrap. |

Additional checks cover concurrent installer exclusion, configuration reloads,
private configuration permissions and pinned dependencies. The publication scan
excludes runtime files, local configuration, databases, logs and environments;
a bounded pattern scan found no common credential/private-key patterns in the
publishable source. This is not an exhaustive secret audit.

The native interfaces still lack atomic compare-and-send. Codex also lacks a
server-side delivery deduplication guarantee. An extra continuation remains
possible when acceptance is invisible beyond the grace period or a human starts
work during the final send window. These are documented in the
[operations guide](operations.md). Tests compare restored effective permissions;
they do not exercise every protected tool or platform sandbox behavior.
