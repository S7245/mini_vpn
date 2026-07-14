# Knife15 macOS M0 First-Run Failure And Repair Results

Date: 2026-07-14

Status: **ROOT CAUSE REPAIRED LOCALLY; FRESH USER-EXECUTED M0 PENDING**

## Evidence Reviewed

- direct baseline:
  `/tmp/mini_vpn_knife15_macos_baseline_20260714_103507`
- first M0 archive:
  `/tmp/mini_vpn_knife15_macos_20260714_103703.tar.gz`
- rearm archive:
  `/tmp/mini_vpn_knife15_macos_20260714_104628.tar.gz`

The rearm archive matched the reported SHA-256
`19354ff5ef7d2a34331f4ff96abac68dc4c6b75fa99062cd953ba3931afff632`.
The first archive no longer matched the earlier reported `32b7...` value; its
current archive and companion both matched `30cacada3e9bbe3bc7ac2b6e5cd441eed6a5ef1c036769a4627f0b531dfc6bff`.
Its event timeline showed repeated `stop`/`snapshot` commands after the first
bundle publication. The old runner regenerated the same archive path on every
stop, so this checksum drift is a runner evidence-finalization bug.

## User-Operation Decision

The primary M0 failure was not caused by incorrect credentials, route setup,
baseline selection, or the intended `start -> smoke -> m0` sequence. The
preflight route was physical before each start and the initial smoke passed.

Repeated post-stop commands changed the first archive, but the runner had
explicitly described stop as idempotent and did not protect finalized
evidence. That is a script contract defect, not a basis for blaming the user.
The second rearm was attempted before the target iperf server had recovered
from the first broken control session; the runner lacked a recovery gate.

## Exact Failure Chain

The direct baseline passed at `17.829/58.290 Mbit/s` receiver
forward/reverse. Initial TUN smoke passed at `45.429/51.842 Mbit/s`.

The first formal forward phase then carried its capped load for roughly 92
positive intervals and became all-zero from interval 97 onward. The iperf
client ended with a Broken pipe control error.

mini_vpn's control relay had only its small target exchange (`4B` downlink,
`186B` writer total) and was legitimately quiet while a separate data relay
carried traffic. At `90,001ms`, the old D16 coordinator emitted:

```text
direction=timer reason=idle_timeout state=Relaying
```

The main loop rearmed and aborted that still-Established control socket. The
target VPS recorded `client unexpectedly closed connection` at the matching
time, terminated the data session, and remained occupied long enough for the
immediate rearm smoke to fail. Endpoint pacing conservation remained at or
below `61,440B`; TUN interface errors and pump waits/read errors were zero.
The failure therefore selects relay lifecycle, not endpoint pacing, TUN
capacity, frozen constants, credentials, or user routing.

## Implemented Repair

- ADR-0014 supersedes only ADR-0011's full-open payload-idle rule.
- Full-duplex Established relays have no application-payload idle deadline.
- A concrete unfinished upstream write retains a 90-second no-progress guard.
- The writer reports start, each actual partial write, and completed flush;
  partial or remote progress resets the guard and completed flush disarms it.
- Local write-half completion retains the existing 10-second remote-drain
  guard and D16 queued/leased ownership protection.
- D16 reader-task and writer-signal failures now have explicit terminal causes
  rather than relying on the removed full-open timer.
- All D16, native chunk, native permit, and generic engines consume one
  `RelayCloseTimer` policy.
- The macOS runner publishes a bundle/checksum once, refuses mutations after
  finalization, and can recover only the checksum if interrupted after the
  immutable archive was already published.
- `start` now requires a positive direct one-second Target transaction before
  deleting stale runner state, launching mini_vpn, or changing routes. The
  readiness timestamp and receiver rate enter the manifest.
- Relay lifecycle implementation: commit `a4e4549`.
- Immutable evidence and Target rearm gate: commit `89cf1e9`.

## Local Gates

- deterministic full-open generic and exact D16 tests cross the former
  90-second boundary and close through the real command owner;
- pending-write, partial-progress, remote-progress, completed-write,
  half-close, D16 ownership, and injected reader-task failure tests pass;
- root library all-target suite: `635 passed`, `0 failed`, `3 ignored`; the
  main binary suite also passed `2/2`;
- shell syntax, runner internal self-test, external self-test, formatting, and
  diff checks pass;
- `cargo build --release` passes;
- review has no unresolved P0/P1 at this point.

No endpoint pacing, D16 capacity, MTU, pool, QUIC window, chunk, Cubic, GSO,
self-wake, or workload-duration constant changed.

## Next Acceptance

Using the repaired release binary, take a fresh direct baseline and run one
new M0 through the HITL runbook. Success must cross 90 seconds without
full-open `idle_timeout`, complete the two-hour timeline, finalize exactly one
archive, and then pass a fresh start/smoke/stop rearm. A new failure is
classified from its own terminal cause; it does not authorize parameter
tuning.
