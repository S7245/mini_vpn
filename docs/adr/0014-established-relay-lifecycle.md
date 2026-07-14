# ADR-0014: Established TCP Relays Are Lifecycle-Owned

**Date:** 2026-07-14
**Status:** Accepted
**Supersedes:** ADR-0011's L2 full-duplex payload-idle consequence only

## Context

ADR-0011 armed a 90-second timer whenever neither relay direction moved
payload. Knife15 M0 showed that this closes a healthy, quiet TUIC control
stream while a separate data stream is active. Transparent TCP protocols are
allowed to remain Established without payload for longer than any
application-independent duration.

A timer is still necessary when the relay has concrete unfinished work: an
upstream write can remain pending forever, and a locally half-closed relay
needs bounded remote drain.

## Decision

- Full-duplex Established idle has no payload-idle deadline.
- A started, incomplete upstream write has a 90-second no-progress deadline.
- Actual partial-write or remote-read progress resets that pending-write
  deadline.
- Successful completion of the whole write plus flush disarms it.
- Local write-half completion arms the existing 10-second remote-drain
  deadline; useful D16-owned payload continues to defer terminal close.
- One shared state machine supplies these semantics to every relay engine.

## Consequences

- Quiet control, long-poll, SSE, and other transparent TCP sessions remain
  open until a real lifecycle owner closes them.
- Wedged writes and half-closed drains remain bounded and observable.
- `idle_timeout` is no longer a valid full-open relay terminal cause;
  `stalled_write_timeout` identifies the concrete guarded condition.
- Channel closure, socket terminal state, remote EOF/error, and transport
  failure remain responsible for ordinary cleanup.
- A writer already blocked inside remote I/O may take up to the stalled-write
  bound to observe an owner disappearing. A future explicit supervisor-cancel
  seam can make that cleanup prompt without restoring payload-idle lifetime.
