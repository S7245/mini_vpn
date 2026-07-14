# Knife14 H10d16 Credit-Rearm Gate A Results

Date: 2026-07-10

## Scope

This was the explicitly authorized replacement Gate A after the local fix for
the D16 Running read reservation cancelling its own credit. It was one 20s,
reverse-first, P1 mini_vpn run. Gate B was not started.

The run used the reviewed D16 composition only. It did not change VPS, iperf3,
MTU/PLPMTUD, QUIC windows, pool size, chunk size, or self-wake settings.

## Preflight And Evidence

- Exit sing-box: active.
- Target iperf3: active.
- Exit socket-buffer maxima: 16 MiB; defaults: 1 MiB.
- Direct reverse baseline: 280 Mbit/s.
- The bounded Exit-host management proxy completed Target evidence collection;
  cleanup and bundle creation completed normally.
- Remote bundle source:
  `/tmp/mini_vpn_knife14h10d16_credit_rearm_gatea`
- Local bundle:
  `/tmp/mini_vpn_knife14h10d16_credit_rearm_gatea_local/mvpn_knife14h10d16_credit_rearm_gatea_usclient_suite_20260710_144818.tar.gz`
- SHA-256:
  `8ada0a71c6f6000f44fcfeacd5372930460651fc7bde5e1c7c635113d935be1a`

## Result

The reader-credit repair worked, but Gate A failed after a local TUN drop:

- first data-flow TUIC byte: 3ms;
- actor-admitted bytes: 60,928,613;
- actor-bypass admitted bytes: 0;
- receiver: 58.0 MiB, 12.2 Mbit/s over the 39.99s timeout window;
- reverse intervals were bursty and then stopped rather than sustaining the
  required 20s window;
- TUN `tx_dropped_delta`: 2,029 in one event;
- send-slice zero/errors and TUN flush failures: 0;
- QUIC loss, congestion, and flow-control blocking deltas: 0;
- close accounting remained unobserved because both flows stayed active in
  DrainOnly until suite shutdown.

Gate A decision:

- receiver greater than 150 Mbit/s: fail;
- TUN drop delta zero: fail;
- tail/close values all zero: not established because the flows never reached
  a normal terminal close;
- actor exclusivity: pass;
- Gate B eligibility: fail.

## Causal Diagnosis

The run contains a closed feedback sequence:

1. Before the drop, both D16 flows were Running and the actor had admitted all
   60,928,613 bytes with no bypass.
2. The TUN sampler observed one `tx_dropped_delta=2029` event while feedback
   pressure was `max=557386`, `total=994674` bytes.
3. The event installed 122,727 bytes of global drop-credit debt and moved both
   flows to DrainOnly, correctly stopping Quinn reads and smoltcp admission
   while keeping poll/flush service active.
4. The next feedback sample observed `max_pressure=0`,
   `total_pressure=0` and emitted `pressure_low` resume. This proves that at
   least 994,674 bytes of aggregate local pressure drained across the sampling
   interval.
5. The existing debt can only be paid by a per-flow send-limit clock observing
   a send-queue decrease. D16 DrainOnly forbids admission, and the relevant
   pressure decrease occurred outside that clock, so the global 122,727-byte
   debt remained active.
6. The actor then executed thousands of successful poll/flush cycles with
   zero queues, but every transition still saw active drop debt. Final state
   was `d16_running_flows=0`, `d16_drain_only_flows=2`,
   `d16_recovery_flows=0`, `d16_drain_only_cycles=8283`, and
   `d16_drain_only_drain_bytes=0`.

This is a D16 feedback-composition deadlock, not a byte-owner, Quinn wake,
actor-exclusivity, QUIC-path, or raw capacity failure. The old drop-credit
accounting misses drain that is observable only across global feedback
samples; DrainOnly then prevents the admission path needed to create another
per-flow debt-payment observation.

The review also found that the production transition adapter currently passes
`completed_drain_bytes.is_some()` as `drain_progress`. A successful zero-byte
cycle is therefore labelled progress. That did not cause this freeze because
drop debt had priority, but it does not implement the spec's requirement that
a drain cycle actually make progress.

## Proposed Red-First Repair

1. Add a deterministic composition test for the exact episode: D16 flows enter
   DrainOnly on a drop with nonzero aggregate pressure, all pressure drains
   before any later send-limit call, the next feedback sample is below the low
   watermark, and no new admission is available to pay debt. The current code
   must remain DrainOnly, making the test red.
2. Preserve one bounded drop-episode pressure baseline. On the matching
   `pressure_low` edge, calculate the observed aggregate drain and use it once
   to pay the existing global drop debt. Do not forgive debt without measured
   pressure reduction and do not mint admission credit from the payment.
3. Feed that same one-shot clean-drain evidence to every active D16 flow held
   by the global drop episode. After debt is clear and pressure is below low,
   each eligible DrainOnly flow enters Recovery; terminal no-send flows remain
   DrainOnly.
4. Change ordinary actor-cycle transition evidence from
   `completed_drain_bytes.is_some()` to a strictly positive byte test. Keep the
   cycle counter and byte counter distinct so zero-byte drain service remains
   observable without being treated as recovery progress.
5. Prove Recovery reopens at a 128 KiB quantum, returns to Running only after
   four clean progress cycles, and falls back immediately on renewed pressure
   or drop evidence. Also assert exact debt conservation, no double payment,
   no actor bypass, and no queue/permit leak.
6. Rerun focused D16 tests, full normal and harness libraries, integrations,
   fmt/check/diff, suite self-test, and code review. Do not run another VPS test
   or Gate B without a new explicit authorization.

No production repair was made after this failed Gate A. Gate B remains frozen.
