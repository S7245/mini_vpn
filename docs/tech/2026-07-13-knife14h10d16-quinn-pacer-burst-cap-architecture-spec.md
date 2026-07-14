# Knife14h10d16 Quinn Pacer Burst-Cap Architecture And Capacity Spec

Date: 2026-07-13

Status: local implementation, full local regression, and code review PASS. No
VPS run occurred; the single VPS discriminator requires explicit authorization.

Source baseline: `a54fb171ad57dc48902a79f9d31bd08d2ac41802` plus the
existing uncommitted Knife14 worktree. Gate A and Gate B remain accepted. This
stage is still Task 12 step 4 after Gate B; it is not a Gate B rollback.

## Implementation Checkpoint

The confirmed four-P1 repair is complete. The already rejected bounded
`48 then 2ms` real replay is now an explicit ignored measurement; the optional
cap is derived from one upstream `optimal_capacity` result on both construction
and update; formal QUIC stats report `current_mtu` and `pacing_mtu`; and the
runner validates, mutually excludes, propagates, reports, and independently
fingerprints `MINI_VPN_TUIC_PACING_POLICY=quinn|pacer-cap64`.

The final pinned patch passed `275/275` upstream Quinn tests and `3/3` doc
tests. The GSO-enabled real loopback gate delivered exact `32 MiB` with the
fixed `64 KiB` chunk, zero pattern errors, clean EOF, and `621.573 Mbit/s`.
Its active snapshot reported `uncapped=307200B`,
`capacity=76800B=64*1200`, `tokens=70326B`, `current_mtu=1200`,
`pacing_mtu=1200`, `cap_active=true`, `delay_events=12`, and
`cwnd=34141529 <= u32::MAX`; the old bounded socket sender was not active.

The full local regression then passed: library `620 passed / 0 failed / 3
ignored`; normal harness `10 passed / 4 ignored`; explicit concurrency
`64/64`, `256/256`, and `1024/1024`; UDP `500/500` at every swept payload with
zero loss; default/harness checks; root fmt; all three shell syntax/self-tests;
and diff-check. Final code review found no open P0/P1. The existing unused
`SendBatch::try_reserve` warning remains confined to the frozen default-off
bounded diagnostic path.

Gate A and Gate B remain accepted. No VPS or macOS TUN ran and no frozen value
changed. The single same-window forward discriminator described below is now
locally eligible, but it was not authorized or run in this turn.

## Decision

Add one default-off, per-connection maximum-burst configuration seam to the
exact pinned `quinn-proto 0.11.16` pacer. The first candidate value is `64`
paced MTU-equivalents per QUIC connection. Keep Quinn's existing
RTT/cwnd-derived refill rate, elapsed-time debt, congestion checks,
control-packet semantics, loss timers, GSO, and default
`256`-MTU-equivalent maximum unchanged.

This is a deliberately narrow tracer architecture:

- it is intended to be sufficient for the real GSO-enabled local `32 MiB`
  upload and, only after all local gates pass, one single-flow forward P1;
- it is necessary-only for multi-flow/P8 and final product acceptance;
- it does not claim a strict endpoint sliding-window limit or cross-connection
  fairness;
- it does prove a static pool-2 stored-token ceiling of `128` paced
  MTU-equivalents and a single data connection ceiling of `64` paced
  MTU-equivalents.

If the local capacity gate fails, stop and review. Do not tune this cap, D16,
MTU, pool, QUIC windows, chunk, Cubic, or self-wake from that failure.

## Accepted Position And Evidence

Gate B passed from clean `a54fb17` with three reverse P1 receivers of
`192/188/191 Mbit/s`, median `191 Mbit/s`, followed by one exact fixed
`64 MiB` clean close at `179 Mbit/s`. That evidence remains accepted.

Task 12 step 4 later selected a forward-only failure:

- the same-window sing-box control reached `182.856 Mbit/s` receiver with no
  socket drops;
- default-GSO mini_vpn reached `193 Mbit/s` receiver but added `30` TUN TX
  drops and `57,632,755B` of formal-window QUIC loss;
- mini_vpn peaked at `261 packets/ms`, versus `93 packets/ms` for control;
- disabling GSO reduced the mini_vpn peak to `163 packets/ms` but did not
  remove the drops or loss;
- a public `AsyncUdpSocket` gate that sent 48 datagrams and then waited a full
  2ms delivered exact bytes and EOF but only `98.311 Mbit/s`.

The measured wrapper period was `4.573ms`: `2ms` intentional cooldown,
`1.340ms` mean timer lateness, and about `1.233ms` of send/service work. Even
with timer lateness removed, that serial design allowed only about
`142.5 Mbit/s`.

## Exact Code Reachability

The pacing owner is inside `quinn-proto`, before packet send accounting:

1. `Connection::poll_transmit` checks congestion and calls
   `path.pacing.delay(...)` before building the next datagram.
2. `PacketBuilder::finish_and_track` records `SentPacket.time_sent`, inserts
   bytes into in-flight/loss state, starts the loss timer, and calls
   `pacing.on_transmit`.
3. Only after `poll_transmit` returns does Quinn's connection driver call
   `AsyncUdpSocket::try_send`.

Therefore a socket adapter that intentionally returns `WouldBlock` delays an
already-accounted packet and necessarily becomes a second pacer. The public
socket seam is deep enough for ordinary I/O adaptation, but it is the wrong
seam for QUIC scheduling policy.

The selected deep Module is Quinn's existing `Pacer`. Its Interface already
contains the important invariants: RTT/cwnd refill, elapsed-time debt, token
capacity, and the next deadline. The patch adds only the missing maximum-burst
configuration at that seam. mini_vpn remains a thin configuration Adapter;
Quinn remains an outer framework detail and does not leak into D16, smoltcp,
TUN ownership, or TUIC stream lifecycle.

## Capacity Math

### Application and wire demand

The target application rate is:

```text
170 Mbit/s = 21,250,000 application bytes/s
```

The failed local measurement accepted `34,479,447B` of QUIC UDP traffic to
carry `33,554,432B` of application data:

```text
wire/application ratio = 1.027567595
measured mean wire payload = 1199.952913B/datagram
required wire bytes/s = 21,835,811.399
required wire datagrams/s = 18,197.224
```

The older `17,709 datagrams/s` number excluded measured control/QUIC overhead.
This spec uses the stricter `18,197 datagrams/s` figure for capacity demand.
The proposed cap itself remains byte-based: one paced MTU-equivalent is `mtu`
bytes of Quinn pacing capacity, not a promise to count every wire datagram.

### Quinn's existing rate and burst calculation

Quinn refills at:

```text
refill_rate_bytes = 1.25 * cwnd / smoothed_rtt
optimal_capacity  = cwnd * 2ms / smoothed_rtt
```

Thus an uncapped optimal bucket refills in `1.6ms`. The proposed patch changes
only:

```text
configured_max = min(selected_max, 256)
configured_min = min(10, configured_max)
capacity = clamp(
    optimal_capacity,
    configured_min * mtu,
    configured_max * mtu,
)
```

It does not change `refill_rate_bytes`, `prev`, elapsed-time debt, or the
deadline formula. Send/service work is included in elapsed time rather than
being followed by a fresh full cooldown.

### Why the first cap is 64, not 48

The external measurement observed `1.340215ms` mean rearm lateness. Treating
that as a conservative scheduler cost at Quinn's worst `1.6ms` ideal refill
edge gives:

| cap | pool-2 stored cap | mean-lateness application capacity |
| ---: | ---: | ---: |
| 48 | 96 MTU-equivalents | about `152.5 Mbit/s` |
| 64 | 128 MTU-equivalents | about `203.4 Mbit/s` |
| 80 | 160 MTU-equivalents | about `254.2 Mbit/s` |

`48` therefore fails the code-level mean-lateness capacity gate, while `80`
nearly recreates the observed disabled-GSO `163 packets/ms` edge across the
pool. `64` is the smallest simple power-of-two cap with measured mean-timer
headroom and a materially smaller single-connection burst than `163-261`.

The cap binds only when Quinn's calculated pacing rate is high enough to fill
64 MTU-equivalents in at most `1.6ms`, about `40,000` full-MTU equivalents/s
or `384 Mbit/s` at the measured payload. Below that point Quinn's smaller
RTT/cwnd-derived capacity remains authoritative, so the patch does not lower
the normal pacing rate to a fixed value.

This math is a plausible sufficient path, not a throughput result. Runtime
timer behavior still requires the real local gate.

## Pool Aggregate Contract And Deliberate Scope Narrowing

`TuicUpstream::connect` creates one endpoint and the frozen TCP pool creates
two QUIC connections from it. Each connection has one independent `PathData`
and `Pacer`.

With `max_pacing_burst_datagram_equivalents=64`:

```text
per-connection stored token capacity <= 64 * current_mtu bytes
pool=2 stored token capacity          <= 128 * current_mtu bytes
```

For the next single-flow forward P1, only one pool connection carries the
bulk TCP stream. Its stored paced-data burst is therefore at most 64
MTU-equivalents. The
other connection may send bounded authentication/heartbeat/ACK/control
traffic and contributes at most another 64 MTU-equivalents of stored paced
tokens in the formal worst case. Pure ACKs may remain unpaced under Quinn's
existing zero-sized pacing accounting, so this is intentionally not an
all-wire-packet count.

Quinn intentionally bypasses pacing when the congestion window exceeds
`u32::MAX`. The static stored-token contract therefore applies only while
`cwnd <= u32::MAX`; every formal local/VPS window must assert that condition.
Changing this upstream extreme-window behavior is outside the tracer scope.

This is not a sliding-window theorem. Independent pacers can refill at
different RTT/cwnd rates, so this design cannot promise
`N(t2)-N(t1) <= 128 + R*(t2-t1)` for a fixed endpoint-wide `R`, nor can it
promise fairness between two saturated connections. That limitation is why
the design is sufficient only for the next single-flow discriminator and
necessary-only for P8/product concurrency.

The measurement plan explicitly allowed a per-connection cap only when the
spec narrowed and justified the acceptance scope. This section is that
narrowing. If a later P8 run shows aggregate burst/fairness failure, the next
architecture must be a pre-accounting shared coordinator inside Quinn-proto;
it must not be another post-accounting socket gate.

## Dependency And Interface Contract

Pin and patch only `quinn-proto 0.11.16`, whose accepted crates.io checksum is
`2f4bfc015262b9df63c8845072ce59068853ff5872180c2ce2f13038b970e560`.
Do not fork `quinn 0.11.11` or `quinn-udp 0.5.15` for this stage.

The reproducible implementation form is a local
`third_party/quinn-proto-0.11.16` source plus a `[patch.crates-io]` entry. Keep
the upstream licenses, record the original checksum, and add a short patch
manifest naming every changed upstream file. Do not use a mutable Git branch
or an unpinned remote dependency.

The proposed public Interface is one fluent config method:

```rust
TransportConfig::max_pacing_burst_datagram_equivalents(
    Option<NonZeroU16>,
) -> &mut Self
```

Contract:

- `None` preserves the exact upstream `256 * mtu` maximum;
- `Some(n)` sets the upper capacity to `min(n, 256) * mtu` bytes;
- the lower capacity is `min(10, n, 256) * mtu`, so an explicit value below
  10 is honored without making the clamp internally inconsistent;
- the value is copied into every new path Pacer;
- path migration carries the previous Pacer's configured maximum;
- no runtime mutation is supported;
- Quinn continues to apply pacing checks and accounting to generated paced
  bulk/padded packets before GSO coalescing;
- pure ACK-only packets retain Quinn's existing zero-sized pacing accounting
  and are not reclassified as paced bulk data.

Expose read-only diagnostics through `PathStats`:

- current uncapped pacing capacity bytes;
- current pacing capacity bytes;
- current pacing token bytes;
- whether the configured pacing cap is currently active;
- cumulative pacing-delay events.

mini_vpn has two real Adapters at the seam:

- `QuinnDefault`: no setter call, effective max `256`;
- `PacerCap64`: call the setter once while building `TransportConfig`.

They are mutually exclusive. The rejected `AsyncUdpSocket` hard-cooldown
Adapter must not be stacked with `PacerCap64`.

## Default Compatibility

Until the complete Task 12 product gate passes, production/default remains
`QuinnDefault`. The patched dependency must pass all upstream and project
tests with no policy selected.

Default compatibility means:

- the same `optimal_capacity` matrix and pacing deadlines as unpatched
  `quinn-proto 0.11.16`;
- the same `256 * mtu` maximum;
- unchanged public `Debug` output when the option remains `None`;
- no new task, timer, queue, copy, mutex, wake, socket option, or log on the
  default hot path;
- unchanged GSO, congestion controller, loss probe, ACK-only, close, path
  validation, migration, and OS `WouldBlock` behavior;
- unchanged Cargo versions for Quinn and quinn-udp.

## Old-Path Audit

Paths that remain active and unchanged:

- Quinn's per-connection congestion controller and RTT/cwnd refill rate;
- Quinn's connection-driver limit of 20 datagrams per poll and immediate
  self-wake when work remains;
- GSO enabled by default, maximum 10 segments per `Transmit`;
- kernel UDP socket buffering and actual `WouldBlock` handling;
- D16 byte ledger, actor cadence, 24-packet TUN feedback admission, EOF and
  close lifecycle;
- TUN/QUIC MTU policy, pool=2, QUIC windows, 64KiB application chunk, Cubic,
  and D3 self-wake disabled.

Rejected paths that must not be active with the candidate:

- fixed 48-then-2ms `AsyncUdpSocket` cooldown;
- GSO-disabled as a product fix;
- driver `MAX_TRANSMIT_DATAGRAMS` tuning;
- Linux-only `SO_MAX_PACING_RATE`/`sch_fq` as the cross-platform product
  architecture.

## Safety, Liveness, And Boundedness

Safety properties:

- no packet is delayed by policy after Quinn records it as sent;
- no payload queue or copy is added;
- no token is minted beyond the configured per-connection capacity;
- ACK-only, loss probes, close, and path-control retain existing Quinn
  semantics;
- migration cannot silently restore the default 256 maximum;
- migration inherits the cap even though upstream path construction may start
  the new bucket full; formal acceptance therefore excludes path migration;
- default policy is behavior-equivalent to upstream.

The stored-token ceiling is conditional on Quinn's retained
`cwnd <= u32::MAX` pacing range; formal gates must reject a snapshot outside
that range rather than claiming the cap enforced it.

Liveness properties:

- a depleted Pacer retains Quinn's existing timer and wake path;
- elapsed send/service time contributes to refill debt;
- connection close/drop adds no waiter or permit cleanup responsibility;
- an actual UDP `WouldBlock` still uses Quinn's one existing buffered
  `Transmit` and poller lifecycle.

Memory remains constant per Pacer. There is no new user-space or kernel queue
and no cross-connection lock.

## TDD Implementation Slices

Each slice must start red, turn green, and keep the previous slice green.

1. **Pinned source/default equivalence**
   - vendor exact `quinn-proto 0.11.16`;
   - run its unmodified tests first;
   - add a matrix test proving default capacity/deadlines match the original
     10/256 formula.

2. **Config reaches a new Pacer**
   - red: max 64 cannot be expressed;
   - green: config field/method and `PathData::new` wiring;
   - assert `None` is exact upstream behavior and `Some(64)` caps at
     `64 * mtu`;
   - assert a nonzero explicit value below 10 uses a matching lower clamp;
   - assert a value above 256 cannot increase the upstream maximum.

3. **Rate/debt is preserved**
   - exhaust a cap-64 Pacer at one instant;
   - advance a fake clock by partial/full refill intervals;
   - assert tokens use `1.25*cwnd/rtt` and elapsed time, with no
     `wake_now + fixed cooldown` reset;
   - assert lowering only the maximum does not alter the refill slope.

4. **Migration and reset**
   - create a previous path with cap 64;
   - migrate/rebind path state;
   - assert the new Pacer remains cap 64 and cannot reopen to 256.

5. **Two-connection stored aggregate**
   - create two independently saturated cap-64 Pacers at one fake instant;
   - assert each exposes at most `64 * mtu` bytes of stored paced capacity and
     their sum at that instant is at most `128 * mtu`;
   - explicitly assert this test does not claim a shared refill-rate bound.

6. **Protocol semantics and observability**
   - run existing ACK, loss-probe, handshake, close, MTU, and migration tests;
   - assert `PathStats` reports uncapped/effective capacity, cap-active state,
     tokens, and delay count;
   - prove paced bulk bytes are charged before GSO coalescing and ACK-only
     zero-sized pacing remains unchanged.

7. **mini_vpn branch by abstraction**
   - keep `QuinnDefault` green;
   - add default-off `PacerCap64` composition and startup fingerprint;
   - reject combining it with the old socket cooldown;
   - ensure both TUIC pool slots log the effective cap.

8. **Real local capacity and lifecycle**
   - one GSO-enabled `32 MiB` upload, fixed 64KiB chunk;
   - exact bytes, zero pattern error, clean EOF;
   - application sender strictly `>170 Mbit/s`;
   - effective cap 64 and pacing-delay activity observed;
   - active snapshots report uncapped capacity greater than
     `64 * current_mtu`, effective capacity equal to `64 * current_mtu`, and
     cap-active true, proving that the selected cap actually bound;
   - every formal snapshot reports `cwnd <= u32::MAX`;
   - no queue, loss-probe, handshake, or close hang.

9. **Full local regression**
   - library and integration/harness gates;
   - concurrency 64/256/1024;
   - UDP payload sweep;
   - `cargo check --features harness`, fmt, shell syntax/self-tests, and
     diff-check;
   - code review before any VPS run.

## One Allowed VPS Discriminator After Local PASS

Only after every local slice and code review pass:

1. run one same-window Gate-aligned sing-box forward control;
2. run one fresh mini_vpn forward-only P1 with `PacerCap64`;
3. retain target-only routing, GSO enabled, Cubic, pool=2, and all frozen
   profile values;
4. capture both client egress and Exit ingress and snapshot both pool slots;
5. require the selected data connection to carry at least `95%` of QUIC TX
   bytes/datagrams, with the auxiliary connection limited to
   auth/keepalive/control, and require no path migration in the formal window;
6. require `20/20` nonzero intervals, zero TUN RX/TX drops, aggregate positive
   pool QUIC-loss delta no greater than `16 MiB`, zero flow-control blocking,
   exact clean/bounded lifecycle, and no handshake/reconnect regression.
7. require every formal pool snapshot to report `cwnd <= u32::MAX` and the
   selected data connection's configured cap active.

The pcap 1ms/10ms peak is a mechanism discriminator, not a substitute for the
drop/loss gate. It must materially fall from the `163-261 packets/ms` failed
edge; a peak that remains in that class proves the cap is not sufficient or
not active.

If this single forward discriminator fails, stop before P8 and perform another
code review. Do not retry cap values in the same architecture stage.

## Failure Discriminators

- Default-equivalence test fails: dependency patch is rejected.
- Cap/debt unit test fails: wrong pacer implementation; no real local run.
- Migration restores 256: lifecycle bug; no real local run.
- A formal snapshot has `cwnd > u32::MAX`: Quinn's retained pacing bypass makes
  this cap non-attributable; stop without changing that extreme-window path.
- Real local upload is exact but `<=170 Mbit/s`: timer/cap capacity is
  insufficient; stop, do not tune constants.
- Local handshake, loss probe, or close hangs: protocol liveness regression;
  reject the patch.
- VPS burst falls but loss/TUN drops remain: burst capacity is not the
  sufficient root; stop pacing work.
- VPS burst remains `163-261 packets/ms`: cap reachability or per-connection
  scope is insufficient.
- Single-flow VPS passes but later P8 fails with two saturated pool slots:
  select the deferred pre-accounting shared coordinator architecture; do not
  add a socket wrapper.

## Post-Discriminator Result

The one allowed VPS discriminator ran on 2026-07-13 and **failed**. The
same-window sing-box control passed at `191.928 Mbit/s` receiver with zero
socket drops. cap64 mini_vpn reached `185 Mbit/s` receiver and every interval
was nonzero, but added `29` TUN TX drops and `50,621,275B` formal QUIC loss.
Its client egress peak was `267 packets/1ms` and `1337/10ms`, versus
same-window control `100/605` and prior Quinn-default mini_vpn `261/1259`.

The result is attributable rather than a configuration miss. One data-conn
snapshot reported `327680B` uncapped and `81920B = 64*1280` effective with
`cap_active=true`; the data connection carried `99.9998%` of TX bytes, no
migration/blocking/reconnect occurred, and all formal `cwnd` values were below
`u32::MAX`. The cap nevertheless became inactive in four of five formal data
snapshots, and `delay_events` stopped at `22` while traffic continued.

Code review falsifies the candidate's sufficiency assumption: a stored-token
maximum does not bound the unchanged `1.25*cwnd/rtt` refill slope. At sub-ms
RTT the Pacer can refill and spend multiple buckets inside a 1ms measurement
window. `PacerCap64` is therefore rejected as the single-flow VPS solution;
do not retry a different cap value in this architecture stage.

The deferred endpoint pre-accounting coordinator is now evidence-justified as
a design subject, but not authorized for implementation. It requires a new
capacity/reachability spec and deterministic sub-ms refill/window tests before
any code or VPS request. The rejected public `AsyncUdpSocket` cooldown sender,
GSO-only branch, and frozen-parameter tuning remain closed. Detailed result:
`docs/tech/2026-07-13-knife14h10d16-pacer-cap64-forward-discriminator-results.md`.

## Deferred Full Endpoint Coordinator

A true endpoint sliding-window/fairness contract would require shared state
inside Quinn-proto before packet construction and send accounting. It would
need stable connection identities, a shared deadline combined with each
path's deadline, bounded waiter metadata, control reserve, idle borrowing,
cancel/migration cleanup, and fairness tests. That is materially larger than a
maximum-cap patch and is not justified before the narrow single-flow tracer
reports whether burst capacity is causal.

The tracer has now reported and rejected the stored-cap mechanism. The
confirmed design-preparation follow-up produced the deterministic
`259 datagrams/1ms` sub-ms refill characterization and the proposed successor
source of truth:

- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`
- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-implementation-plan.md`

The successor is not implemented or VPS-authorized by this amendment.

## Architecture Review Score

Current score: **8/10**.

Strengths: correct pre-accounting seam, preserved framework defaults, no new
queue/copy/lock/task, explicit capacity math, reversible parallel change, and
strict local/VPS stop rules.

The remaining two points require either:

- accepted P8 evidence that independent per-connection caps are sufficient in
  the real pool; or
- a reviewed endpoint-shared pre-accounting coordinator with formal aggregate
  rate, fairness, and lifecycle contracts.
