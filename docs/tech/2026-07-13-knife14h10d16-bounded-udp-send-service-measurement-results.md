# Knife14h10d16 Bounded QUIC UDP Send-Service Measurement Results

Date: 2026-07-13

Source baseline: `a54fb17` with the existing uncommitted Knife14 worktree.
No commit was created. No VPS or macOS TUN test ran.

## Verdict

**FAIL / STOP before VPS.** The confirmed measurement-only follow-up passed
its deterministic tests and selected the timer/additive-cooldown branch, but
the real GSO-enabled `32 MiB` local capacity gate again failed:

```text
sender_mbps=98.311
required_mbps>170
exact bytes/pattern/clean EOF=PASS
```

The runner now evaluates the `16 MiB` QUIC-loss ceiling over the positive
window deltas of the whole pool rather than the largest single connection.
That repair passed a two-connection self-test where both individual deltas
were below the ceiling but their aggregate exceeded it.

## Measured Send Service

```text
accepted_bytes=34479447
accepted_datagrams=28734
accepted_bytes_per_sec=12607705
accepted_datagrams_per_sec=10506
mean_payload_bytes=1199.953
service_elapsed_ms=2734.792
batch_closes=598
cooldown_rearms=598
timer_blocked_polls=1867
mean_rearm_period_us=4573.230
mean_rearm_lateness_us=1340.215
max_rearm_lateness_us=2411.875
gate_would_block=0
wasted_datagrams=0
inner_would_block=0
inner_errors=0
invalid_transmits=0
```

Packetization is not the selected root. The observed mean wire payload was
essentially the expected `1200B`. Each closed batch carried about `48`
datagram equivalents, and `batch_closes == cooldown_rearms`, so missing batch
accounting is also excluded.

At `1199.953B`, `170 Mbit/s` requires about `17,709` datagrams/s. A 48-packet
batch must therefore complete every `2.710ms` or faster. The observed period
was `4.573ms`:

```text
intentional cooldown:       2.000ms
mean measured lateness:     1.340ms
remaining send/service:     1.233ms
observed total:             4.573ms
```

Even the optimistic zero-lateness period is therefore about `3.233ms`, whose
capacity is only about `142.5 Mbit/s` at the observed payload. The hard
`now + 2ms` cooldown is mathematically insufficient even with a perfect timer;
timer lateness makes the result worse but is not the sole cause.

## Deterministic Gates

Passed before the real gate:

- seven bounded-socket tests, including paused-time actual lateness/rate,
  shared two-poller budget, GSO accounting, no busy wake, conservation, and
  inner `WouldBlock`;
- send-service log-format test;
- low-RTT probe and outer-suite self-tests;
- `bash -n`, `cargo fmt -- --check`, and `git diff --check`.

The first `cargo fmt -- --check` reported formatting-only differences. The
standard formatter was applied and the check passed; this was not a product or
capacity failure.

## Post-Failure Code Review

1. **P0 — additive cooldown is not capacity-reachable.** `SendBatch` sets a
   fresh `now + 2ms` deadline only after the batch work has completed. Work,
   the complete cooldown, and timer/poll lateness are serialized. The measured
   `4.573ms` period and the zero-lateness `142.5 Mbit/s` upper estimate reject
   this mechanism before VPS.
2. **P0 — the wrapper double-paces Quinn.** Quinn-proto `0.11.16` already uses
   a private token-bucket pacer whose refill rate/debt is integrated with RTT,
   congestion window, and elapsed time. The public socket wrapper can only add
   another `WouldBlock`/timer edge after Quinn has already paced the
   connection. It cannot narrow Quinn's private burst capacity without also
   adding a second pacing delay.
3. **P1 — periodic service rates are cumulative, not interval deltas.** The
   snapshot rates use first-to-last accepted time, which was sufficient for
   this one-shot discriminator. Before any VPS use, the periodic logger must
   compute deltas between snapshots or label the values cumulative; otherwise
   later idle time and handshake/control traffic can make an interval appear
   healthier or worse than it was.
4. **P1 — a Quinn pacer cap is per connection unless a new aggregate contract
   is designed.** Quinn's `Pacer` is private and owned by each connection. A
   dependency-level capacity seam could preserve Quinn's rate/debt model, but
   it does not automatically prove the endpoint/pool aggregate burst bound.
5. The measurement and runner changes are structurally safe: bounded scalar
   state only, no payload queue/copy, no extra self-wake, default Quinn path
   unchanged, exact transfer/EOF preserved, and the aggregate-loss self-test
   closes the prior false-pass hole.

## Proposed Next Plan — Requires Confirmation

Do not change `48`, `2ms`, or any frozen product knob and do not rerun the
current wrapper.

1. Write a short architecture/capacity spec comparing two mechanisms:
   - preferred candidate: a narrow, pinned Quinn/quinn-proto pacing-cap seam
     that makes burst capacity configurable while preserving Quinn's existing
     RTT/cwnd token refill, debt, congestion checks, and production default;
   - fallback candidate: an endpoint-shared deadline/debt service, accepted
     only if its math proves it does not serialize batch work plus a full timer
     delay or repay missed deadlines as an unsafe same-millisecond burst.
2. Make endpoint/pool aggregation an explicit selection gate. A per-connection
   Quinn cap is rejected unless a deterministic two-connection test proves the
   required aggregate bound or the spec narrows and justifies the acceptance
   scope. Do not assume `pool=2` makes per-connection and endpoint bounds equal.
3. Before product implementation, require code-level math at the observed
   `1199.953B` payload: at least `17,709` datagrams/s and a mean 48-datagram
   service period no greater than `2.710ms`, with measured timer accuracy and
   control share included. The design must also explain how it bounds the
   prior `163-261 packets/ms` edge without reducing the mean below target.
4. TDD the selected seam in vertical slices:
   - default behavior remains byte-for-byte/config-equivalent to upstream
     Quinn;
   - lowering only burst capacity preserves Quinn's elapsed-time refill/debt;
   - two active connections satisfy the chosen aggregate contract;
   - no queue, payload copy, busy wake, loss-probe deadlock, or handshake
     regression is introduced;
   - the real GSO-enabled `32 MiB` upload again proves exact delivery, clean
     EOF, and `>170 Mbit/s` before any broader test.
5. Only after that local gate, full library/harness/concurrency/UDP checks, and
   a new code review may one same-window control plus one forward-only mini_vpn
   P1 be spent. Require zero TUN TX drops and aggregate window QUIC loss no
   greater than `16 MiB`. A failure stops again before P8.

D16, TUN/QUIC MTU, pool, QUIC windows, chunk, Cubic, and self-wake remain
frozen. P8, UDP/live-streaming, fake-IP DNS, and rearm remain unspent.
