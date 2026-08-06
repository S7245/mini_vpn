# Knife15 Exit-to-Target Forwarding Observability Architecture Spec

Date: 2026-08-06

## Stage Goal

Turn the remaining post-QUIC forwarding seam into a bounded timeline that can
distinguish:

1. Exit kernel TCP loss/retransmission or Target ACK stall;
2. sing-box/TUIC application-copy service gaps before the Exit kernel socket;
3. healthy Exit-to-Target delivery despite a client/Target observer mismatch;
4. a client-side QUIC scheduling or flow-control stall before bytes reach the
   Exit application.

The corresponding macOS action must stop after one exact M2 mixed cycle, in
about 15 minutes, so an unresolved startup failure cannot consume another
25-hour attempt.

## Grounding

The accepted client artifact proves QUIC ACK progress while Target delivery
arrived in sparse `128 KiB` multiples. One exact-order direct control and sixty
fresh direct short controls all passed. Existing sing-box info logs show only
inbound routing and outbound creation; they do not expose first write or kernel
TCP service.

## Non-Goals

- Do not change mini_vpn data-plane behavior in this stage.
- Do not change sing-box routing, TUIC protocol, congestion control, or service
  configuration.
- Do not replay application bytes or require a nonstandard TUIC response.
- Do not tune D16, MTU, pool size, QUIC windows, chunk size, Cubic, GSO,
  Endpoint pacing constants, priority delta, or self-wake.
- Do not weaken the zero-receiver-interval or UDP-loss SLO.
- Do not let a qualification PASS count as formal M2 acceptance.

## Boundaries and Interfaces

The observer is an operations adapter around the mature server. It is not part
of the product data path.

- `MacQualification`: runs the existing full-tunnel/real-client preflight and
  exactly one steady mixed cycle.
- `ExitPacketObserver`: captures only packet headers matching
  `host <Target> and tcp port <iperf-port>`.
- `ExitSocketObserver`: samples only matching established Exit TCP sockets and
  kernel TCP_INFO rendered by `ss -tin`.
- `ObserverBundle`: owns metadata, bounded logs, capture files, command/version
  provenance, SHA-256 values, and cleanup evidence.

Architecture cleanliness score: **8/10**. The seam is explicit and the
observer is outside the data plane, but one human coordination boundary remains
between starting the Exit observer and running the Mac qualification. The
current one-turn startup mechanism scores **4/10 coverage** for this failure:
it is locally bounded and atomic, but it proved only one queued QUIC scheduling
turn and did not establish Target delivery service.

## Invariants

### Passive observation

- Capture filters must include only the exact Target and iperf port.
- Snap length must be header-oriented and must not intentionally retain full
  TCP payloads.
- Observation must not inject traffic into the measured TCP connection.
- The packet capture and socket sampler must have a hard lifetime.
- Storage must be a fixed-size rotating ring.

### Lifecycle

- `start` refuses a live prior observer rather than stacking another one.
- `status` reports live/dead process identity, elapsed time, files, sizes, and
  capture drops.
- `stop` sends an interrupt, waits boundedly, records final drop statistics,
  and kills only the exact recorded observer identities if graceful stop did
  not complete.
- `bundle` refuses live observers, scans metadata for secret-shaped material,
  creates an immutable archive, and emits its SHA-256.
- A watchdog timeout must clean observers even if the control SSH session
  disconnects.

### Qualification

- Uses the frozen M2-derived rates and durations from the bound baseline
  profile: 300-second forward, 300-second reverse, 180-second reverse UDP, and
  one 10-second short forward.
- Runs the same IPv6, route, DNS, real-client, quiescence, process, Endpoint,
  D16, network-control, and receiver-continuity gates that precede formal M2.
- Stops immediately on command, safety, receiver-zero, or UDP-loss failure.
- Records `PASS_NON_ACCEPTANCE` on success and can never write the formal M2
  pre-stop verdict.
- Requires `status` and `stop` after either result so route/DNS/process cleanup
  remains evidenced.

## Capacity and Boundedness

The observer does not service payload and therefore cannot increase product
throughput. Its sufficient capacity requirement is to retain the complete
roughly 13-minute mixed cycle at the existing 31 Mbit/s maximum packet rate.

With a 96-byte snapshot plus capture framing and roughly 3,000 packets/s, a
conservative 360 KiB/s estimate requires about 281 MiB for 13 minutes. Use a
fixed rotating ring of 17 files capped at 20,000,000 bytes each: 340,000,000
bytes, or about 324 MiB. The hard timeout is two hours. The socket sampler is
text-only, target-filtered, and similarly time-bounded.

This is a diagnostic-capacity claim only. It makes no `30 Mbit/s` or
`100+ Mbit/s` product claim and does not replace the frozen 32 MiB Endpoint
capacity gate.

## Failure Discriminators

| Exit packet/socket evidence | Target receiver | Client QUIC ACK/write evidence | Classification |
|---|---|---|---|
| retransmission/RTO, rising unacked, no Target ACK | zero | progressing | Exit-to-Target physical/kernel TCP stall |
| no/rare Exit data packets, socket writable/ACK healthy | zero | progressing | sing-box/TUIC application-copy service gap |
| continuous Exit data and Target ACKs | zero | progressing | Target/iperf observer inconsistency; inspect exact sequence accounting |
| no Exit data | zero | QUIC ACK stalled | client-to-Exit QUIC scheduling/flow-control stall |

If the evidence does not fit one row, stop with `inconclusive`; do not tune or
repeat unchanged.

## Acceptance

Local:

- deterministic shell self-tests cover start refusal, timeout ownership,
  bounded rotation arguments, exact filter rendering, PID identity, graceful
  and forced stop, secret scan, and bundle finalization;
- qualification schedule tests prove exactly 2 TCP long results, 1 UDP result,
  1 short TCP result, no idle/full formal schedule, and non-acceptance status;
- shell syntax, existing Knife15 self-test, docs, fmt/diff/secret, and review
  gates pass with no unresolved P0/P1;
- existing Rust and frozen Endpoint gates remain green because no Rust behavior
  changes.

VPS/Mac:

- observer starts on Exit and is live before the Mac qualification begins;
- one qualification either passes all four phases and cleanup, or preserves a
  failure with matching Exit packet/socket evidence;
- the observer bundle has bounded size, valid SHA-256, zero secret findings,
  and explicit cleanup.

## Stop Rules

- Any unexpected local regression stops implementation for root-cause repair;
  do not weaken tests.
- Any observer identity ambiguity, unbounded file growth, capture-drop
  ambiguity, or secret-scan failure blocks use on the VPS.
- Do not run formal 25-hour M2 merely because the 15-minute qualification
  passes; first classify the seam and complete the resulting architecture gate.
- Do not implement blind TCP replay, permanent priority, priority-value sweeps,
  GSO-only branches, or pool/window/MTU/pacing tuning.
