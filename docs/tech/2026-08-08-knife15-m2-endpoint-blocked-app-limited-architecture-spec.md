# Knife15 M2 Endpoint-Blocked App-Limited Ownership Architecture Spec

Date: 2026-08-08

Status: **LOCAL TDD AND REVIEW PASS; PAIRED QUALIFICATION PENDING; FORMAL M2 AND M3 REMAIN BLOCKED**

Failure evidence:
`docs/tech/2026-08-08-knife15-m2-endpoint-blocked-app-limited-qualification-failure-results.md`.

## Decision

Preserve the existing Quinn application-limited model, Cubic, Endpoint pacing
service, and successor service turn. Correct one ownership classification at
the integration seam: an empty transmit poll caused by an Endpoint **bulk**
reservation wait must retain non-application-limited state, just as an empty
poll caused by congestion or Quinn pacing already does. Control-only Endpoint
waits retain upstream behavior.

The Endpoint service is an egress transport scheduler below application data.
It cannot prove that the application has no bytes. Only an empty poll that is
neither congestion/pacing blocked nor Endpoint-bulk blocked may publish
application-limited state. A control-only wait does not claim application
ownership.

## Goals

1. Prevent Endpoint token waits from erasing ordinary business-packet
   slow-start authority while STREAM data remains sendable.
2. Preserve default-off and non-Endpoint Quinn behavior byte for byte.
3. Preserve Endpoint reservation, settlement, fairness, socket-outcome, and
   conservation behavior.
4. Prove the behavior through a real Quinn connection pair with endpoint
   pacing, a realistic RTT, continuous stream data, and observable cwnd
   growth.

## Non-Goals And Frozen Values

- Do not change D16, MTU/PLPMTUD, pool size, QUIC windows, chunk, Cubic, GSO
  default, Endpoint rate/burst/control reserve/quantum, self-wake, recovery,
  admission, startup priority, workload, SLI, or runner timeouts.
- Do not increase or repeat the successor service turn.
- Do not add a Target probe, business-payload replay, second Target socket,
  connection race, or new configuration knob.
- Do not change ACK handling for a genuinely application-idle connection.
- Do not claim formal-M2 or general WAN acceptance from local tests.

## Invariants

1. An Endpoint `Bulk` reservation wait cannot produce `app_limited=true` for
   that transmit poll.
2. `congestion_blocked=true` retains its existing non-application-limited
   behavior.
3. A control-only Endpoint wait, or an empty poll with neither cause, retains
   the existing application-limited behavior.
4. Tagged successor-turn ACK semantics and terminal loss/path/close/deadline
   rules remain unchanged.
5. Endpoint conservation remains:

   ```text
   available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
   ```

6. No new timer, counter owner, payload, connection, or unbounded state is
   introduced.

## Capacity And Reachability Gate

The failed business stream started with corrected cwnd `24,800B` and RTT about
`169ms`. Three ideal slow-start rounds provide:

```text
24.8 + 49.6 + 99.2 ~= 173.6KiB > 128KiB
```

The actual Exit socket crossed 128KiB only after about `1.21s`. The writer was
continuously Pending and ACK-progressing, while Endpoint reservations produced
empty waits. Restoring ACK growth only across those Endpoint-bulk-blocked polls
makes the existing sufficient capacity path reachable; it does not create new
capacity.

Hot path:

```text
D16 business writer has STREAM bytes
  -> Quinn poll_transmit
  -> Endpoint reservation temporarily blocked
  -> empty transmit poll retains non-app-limited ownership
  -> Endpoint wake/refill sends ordinary STREAM packet
  -> same-path ACK enters Cubic slow start
  -> cwnd grows by acknowledged bytes
  -> Exit supplies Target before the first 128KiB interval boundary
```

This is intended to be sufficient only for the exact cold-successor first
receiver-interval discriminator. The unchanged exact 32MiB Endpoint gate must
still exceed `170 Mbit/s`, finish with exact EOF, zero socket would-block, and
final ownership at or below `61,440/0/0B`.

## Old-Path Audit

Successor service-turn qualification, service-normalized admission, forward
qualification, replacement fallback, startup priority, writer ACK-stall
rebind, path-state reset, read-only recovery observer, UDP-demand recovery,
D16, Endpoint pacing, and predecessor drain all remain active and unchanged.

## TDD And Discriminators

One real Quinn-pair tracer test configures a one-datagram Endpoint burst, a
realistic RTT, and continuously queued unidirectional STREAM bytes. The
Endpoint service forces an empty reservation-blocked poll before each ACK.

- RED: the later ACK observes the incorrect application-limited state and the
  congestion window does not grow;
- GREEN: the same ACK grows the window without changing Endpoint accounting;
- regression: the existing successor-turn, Endpoint GSO/accounting, Cubic,
  and default-off suites remain green.

The next Mac qualification must show one of:

- corrected business slow-start and no successor receiver-zero interval:
  retain the architecture;
- the ownership tracer passes but the same receiver-zero recurs: reject this
  architecture and classify the next paired seam without tuning/repeat;
- any Endpoint conservation, TUN, D16, route, cleanup, or unpaced Quinn
  regression: reject the implementation.

## Stop Rule

Stop on a new knob, larger/multiple service turn, retry, Target probe, pool
expansion, frozen-value change, local exact Endpoint capacity at or below
`170 Mbit/s`, unexpected regression, or unresolved P0/P1. After local gates
and review pass, take exactly one fresh paired Mac qualification; formal M2
and M3 remain blocked.
