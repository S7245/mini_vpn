# Knife15 M2 Connection-Local Path-State Recovery Local Results

Date: 2026-08-06

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; SHORT MAC QUALIFICATION REQUIRED; FORMAL M2 NOT RUN**

Implementation: `0e94e56`

Architecture:
`docs/tech/2026-08-06-knife15-m2-connection-local-path-state-recovery-architecture-spec.md`.

## Accepted Failure Evidence

The exact-source `bfaba9e906837c3bd0cc510644589468396d840f` Mac
artifact is:

```text
74300b2ea9343bad7551922ad166dfe47478a982295d4bf61a01611e5f560133  /tmp/mini_vpn_knife15_macos_20260806_083422.tar.gz
```

Its baseline passed at `32.551/58.899 Mbit/s`, and the 300-second direct
Target discriminator passed at `16.268 Mbit/s` without a sender or receiver
gap. Start, smoke, formal preflights, the first qualification transfer, and
cleanup also completed.

The first 300-second qualification forward transferred all `610,402,304B`,
but the Target recorded three complete one-second receiver-zero intervals
around phase seconds 101, 103, and 105. The exact TUIC data writer remained
Pending for up to `7,001,335us`. Its owning conn1 grew from zero to `144`
PLPMTUD black-hole detections, recorded `2,147,556B` QUIC loss and `537`
congestion events, and had fallen to MTU `1280`; the healthy peer conn0 had no
corresponding black-hole or loss growth.

The paired bounded Exit observer artifact is:

```text
e95880aed15077d5e5274034e56509bf0aa3892ee54c6b8f7adcfac56580dcc9  /tmp/mini_vpn_knife15_exit_target_observer_20260806_065748.tar.gz
```

It bound the exact Target socket, had zero capture/kernel drops, held Exit TCP
RTT at `1..9ms`, added no retransmitted bytes during the decisive window, and
showed the Target ACKing every byte supplied by the mature Exit. Application
supply from QUIC alone decayed to tens of KiB/s. D16, smoltcp/TUN, Endpoint
conservation, routes, process, gateway/Exit controls, and cleanup remained
healthy.

Therefore the selected defect is per-connection client-to-Exit QUIC sustained
service degradation. It is not operator error, a slow baseline, the
Exit-to-Target TCP path, TUN, D16, Endpoint pacing, cleanup, or permission to
tune a frozen constant.

## Implemented Contract

`EndpointRecoveryState` now owns a black-hole anchor and one-shot reset
authority for each live stable QUIC identity. While an exact business writer
is Pending, the anchor is preserved. A reset is eligible only when the
existing RTT-derived writer bound is reached and the same connection's
`black_holes_detected` has advanced. Idle increments, counter regression,
ordinary Pending without black-hole growth, and a healthy peer cannot
authorize the action.

The existing exact ACK-stall Endpoint rebind keeps precedence, and a path
reset cannot overlap an in-flight rebind. Issuing the action consumes the
authority before application. It is retained for that stable identity even
if the captured connection closes, preventing a repair loop; only a new
stable identity receives new authority.

The application samples the exact Quinn `Connection` handle, pool index, and
generation together with the pure-policy evidence. The hidden Quinn adapter
locks only that connection, invokes the existing quinn-proto
`path_changed(now)`, and wakes the driver. It restores configured congestion,
RTT, and MTU-discovery state while retaining the QUIC identity, TUIC stream,
Target TCP connection, shared UDP socket, and application bytes. There is no
stream replay, cross-connection migration, new queue, new timer, payload
duplication, or changed frozen value.

## TDD And Review

The policy RED failed solely because `TcpPathDegraded` and
`ResetConnectionPath` did not exist. Six focused GREEN cases now prove:

1. only the pressured connection with same-window black-hole growth resets;
2. no-growth, no-Pending, and counter-regression cases fail closed;
3. authority is one-shot per stable identity and renews only for replacement;
4. exact ACK stall keeps Endpoint-rebind precedence;
5. path reset cannot overlap rebind recovery.

The Quinn RED failed solely because the high-level adapter was absent. Its
GREEN test acknowledged `512 KiB`, reset the live connection to configured
`250ms` RTT and `1280` MTU, and delivered more bytes on the same stable
connection and same unidirectional stream. A closed connection returns
`LocallyClosed`.

Code review found and repaired two P1 issues before acceptance: possible path
reset/rebind overlap, and a close-to-reset race that could be logged as
applied. The final review has no unresolved P0/P1.

## Capacity And Regression Gates

The exact `32 MiB` Endpoint capacity gate reached `240.348 Mbit/s`, above the
unchanged `170 Mbit/s` stop rule and the frozen application-capacity baseline.
It delivered exact bytes/EOF with zero socket would-block and final
conservation:

```text
available_tokens/live_reservation_bytes/outstanding_bytes = 61,440/0/0B
```

The unchanged invariant remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

All local gates pass:

- root library `679 passed, 3 ignored`; main/bins `2 passed`;
- harness integration `10 passed, 4 ignored`;
- release build and established no-dependency Clippy lane;
- vendored Quinn `39 passed, 3 ignored` plus doc `1 passed`;
- vendored quinn-proto `310 passed` plus docs `3 passed`;
- root docs, Knife15/Knife14 shell syntax and self-tests;
- root fmt, focused vendor formatting/diff, patch manifest, and secret scan.

Two command outcomes were rejected as non-gates and rerun correctly: a
standalone Quinn command that omitted the absolute local quinn-proto patch,
and a concurrency command that omitted `--features harness` and ran zero
tests. Current rustfmt also rewrote unrelated vendored upstream formatting;
that mechanical noise was reverted and the reviewed vendor diff remains
narrow.

## Architecture Review Scores

- Clean Architecture: **10/10**. Detection policy remains in TUIC recovery;
  the Quinn seam is a mechanism-only adapter.
- DDIA fault tolerance: **9/10**. Authority, identity, queues, conservation,
  lifecycle, and fail-closed cases are bounded; real-WAN recovery liveness
  remains the explicit short discriminator.
- Deep-module seam: **10/10**. One existing proto operation is exposed without
  exporting application policy or replacing the transport.

## Next Discriminator

Pull the pushed descendant of `0e94e56`, rebuild release, keep Clash-TUN and
every other VPN off, and avoid deliberate heavy non-test traffic. Normal
Apple Push/iCloud may remain. After the bounded Exit observer is active on
`.33`, take exactly one fresh transaction:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop
```

This is about `13m10s` of scheduled qualification traffic plus bounded
preflight/cleanup time, not 25 hours. Do not run formal `m2`.

Qualification success is `PASS_NON_ACCEPTANCE`. If the exact path-reset action
is applied but a healthy-control Target receiver-zero interval recurs, reject
this architecture and do not tune or repeat unchanged. If no action becomes
eligible, classify the observer/predicate mismatch before changing code.
Formal M2 and M3 remain blocked.
