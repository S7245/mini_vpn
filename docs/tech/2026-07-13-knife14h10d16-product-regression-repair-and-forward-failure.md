# Knife14 H10d16 Product Regression Repair And Forward-First Failure

Date: 2026-07-13
Base source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`
Repair patch SHA-256: `59096ed2a9fd5fe890442b72631f5afdc236034199d378bda48a6a01c55f72ac`
Linux binary SHA-256: `9376627b42447c923e7b4bbd85f104683c935b9102dba037b04710cebf98598d`
Follow-up combined `client_tun.rs` patch SHA-256:
`64b580097e68364628d1934b565f2d157983b4547feb95b25d1665ecff32ee62`
Follow-up Linux binary SHA-256:
`9b43315d62746561ec130aafa69b850724e8c5c030a97cf5357e0504789639a7`

## Verdict

The confirmed O(active²) concurrency repair passed every local gate and the
first VPS product regression. The exact repaired build completed a sustained
`60s` reverse P1 at `187/186 Mbit/s`, with all `60/60` intervals nonzero,
zero TUN drops, no event-loop stall, and bounded/exact D16 accounting.

Task 12 step 4 nevertheless remains **STOPPED**. A fresh forward-first product
sequence failed before a valid P8 result: the P1 forward window added `54` TUN
TX drops and drove one QUIC pool connection through about `61.3 MB` of new lost
bytes and `20,983` new congestion events. The following P1 reverse inherited
that connection state, averaged only `136 Mbit/s`, and later reached
`half_closed_idle_timeout` while D16 still owned `524288B` and the local socket
remained active/send-capable with `27736B` queued for egress. The true P8
command had just started when the stop rule was applied; it was interrupted
and is not a concurrency sample. By the cleanup snapshot TUN TX drops had
reached `991`.

No UDP/live-streaming, Linux fake-IP DNS, or TUN stop/rearm acceptance followed
the real product failure. Gate A and Gate B remain accepted, but final stable
`170 Mbit/s` product acceptance remains unauthorized.

The approved follow-up has now completed. The D16 half-close ownership repair
passed its red/green and full local gates. A corrected same-instance forward
discriminator then selected the mini_vpn/Quinn send-shape branch: sing-box
completed at `184.020/182.856 Mbit/s` with zero socket drops, while mini_vpn
completed at `204/193 Mbit/s` but added `30` TUN TX drops, `57,632,755B` of
in-window QUIC loss, and `19,438` congestion events. Task 12 remains stopped;
P8, UDP/live-streaming, fake-IP DNS, and TUN rearm are still unspent.

## Local Concurrency Repair

The repair keeps the buffered-downlink aggregate semantics while removing the
per-handle full scan:

- D16/non-buffered credit now skips the unused aggregate entirely;
- buffered credit computes dirty pending once per pass and updates it from each
  handle's before/after pending delta;
- the remote-payload path scans pending only when buffered credit is enabled;
- AckDrainHint reuses one pressure snapshot rather than scanning twice.

A focused red test first failed because the conditional scan seam did not
exist. The green test proves disabled credit performs zero scans and enabled
credit performs one. The patch is identical in the user worktree, a clean
detached local clone, and the clean Linux build tree.

Clean local results:

```text
library: 610 passed, 0 failed, 2 ignored
normal concurrency harness: 10 passed, 0 failed, 4 ignored
focused scan test: 1 passed
cargo check --features harness: PASS
client_tun.rs rustfmt check: PASS
git diff --check: PASS
```

Explicit concurrency sweep:

| Connections | Completed | Wall | Relay segment |
|---:|---:|---:|---:|
| 64 | 64 | 104.9 ms | 32.7 ms |
| 256 | 256 | 1.134 s | 346.4 ms |
| 1024 | 1024 | 12.294 s | 4.272 s |

This replaces the failed `733/1024` at `120s` / `101.271s` relay result. No
flow reached the former `90s` idle-reap tail.

The existing UDP harness sweep also passed `500/500` with zero loss at payload
sizes `1000`, `1400`, `4000`, and `8000`; the fragmented `8000B` case reached
`33.41 Mbit/s`. Repository-wide `cargo fmt --check` still reports unrelated
pre-existing formatting changes outside this patch, so validation used a
file-level rustfmt check plus full diff whitespace checking without touching
the user's dirty files.

## Frozen VPS Profile

The Linux build came from a full Git bundle at exact `a54fb17` plus only the
reviewed `src/client_tun.rs` repair. The profile remained MTU `1200`, Cubic,
pool `2`, current QUIC windows, ordered/native chunk experiments disabled,
D16 enabled, and D3 self-wake disabled.

The Exit was the same official Shoes `v0.2.7` / Quinn `0.11.9` binary used by
Gate A/B:

```text
Shoes archive SHA-256=134974a4640807bb6767bf4b35f7a71637cdbbb96b8bef622ef86cf197220aac
Shoes binary SHA-256=160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147
alpn_protocols=["h3"]
workers=2
endpoints=1
zero_rtt_handshake=false
```

Config, certificate, and key were one-shot root-only FIFOs written
concurrently. The service was bounded by both `RuntimeMaxSec` and an
independent sysctl-restore watchdog. Credentials and TLS material were not
persisted in evidence.

## Sustained Reverse P1 — PASS

Same-window direct reverse baselines were `274 Mbit/s` from `.27` and
`281 Mbit/s` from `.111`. The target-only TUN route was asserted while the Exit
remained outside the TUN.

```text
sender/receiver=187/186 Mbit/s
intervals=60/60 nonzero
overall interval avg=186.255 Mbit/s
tail avg/min=189.333/183.000 Mbit/s
tail_collapse=0
TUN RX/TX drop delta=0/0
terminal pending reap=0
actor bypass=0
send_slice zero/errors=0/0
flush_tx_failures=0
QUIC blocked deltas=0
```

The event loop stayed mostly parked; observed relay share was approximately
`1.8-3.0%`, with no multi-second service stall. Timed close retained the
already accepted classification: one `local_socket_terminal` released the
bounded `524288B` reservoir and classified `27840B` as
`terminal_closed_no_send`. QUIC added only `31B` of lost bytes and one
congestion event during the full 60-second window.

## Forward-First P1 — FAIL

The suite's first attempt to select P8 through `reverse-first` was a
configuration-only mistake because that branch is hard-coded to P1. It
completed a clean extra P1 at `193/191 Mbit/s`, cleaned up, and is not a P8
sample.

The corrected full branch started with its required standard P1. Its forward
subrun completed at `195/183 Mbit/s`, but the window was not clean:

```text
TUN TX drop delta=54
new QUIC lost bytes=61260343
new QUIC congestion events=20983
start lost bytes/congestion events=2444151/1050
min cwnd=8079
attribution=quic_loss_congestion+inherited_quic_congestion+local_tun_egress_drop
```

The subsequent reverse subrun completed at only `137/136 Mbit/s`. Its first
seconds were severely suppressed, while the final six seconds recovered to a
`190.167 Mbit/s` average. It added no further QUIC loss, but inherited a pool
connection already carrying `63704494` lost bytes and `22033` congestion
events.

During the quiet barrier that followed, the same data flow was reaped as:

```text
terminal_reason=half_closed_idle_timeout
queue_queued=524288
queue_leased=0
queue_reserved=0
permit_terminal_drop_bytes=524288
close_egress_class=active_send_capable
close_egress_bytes=27736
close_egress_drain_candidate=true
tcp_state=CloseWait active=true can_send=true may_send=true
```

This violates the product-regression invariant that no relay is reaped while
useful owned/downlink bytes remain drainable. It also shows the timeout was not
an empty terminal cleanup.

The P8 process was interrupted immediately after its command began. It has no
valid aggregate or per-flow result. The manual stop snapshot showed `991` TUN
TX drops, strengthening the stop decision but not creating a P8 sample.

## Failure Discrimination

The evidence proves two distinct boundaries but does not yet prove one common
root:

1. The concurrency repair is not the failing mechanism: local 1024-flow
   scheduling passed, reverse-first `60s` passed at `186 Mbit/s`, and the patch
   never changed D16, MTU, pool, QUIC, chunk, or lifecycle policy.
2. The forward subrun is the first point where large QUIC loss/congestion and
   TUN drops appear. The following reverse subrun inherits that connection
   state rather than generating comparable new loss.
3. Independently of why the QUIC path became unhealthy, reaping a D16 flow at
   `half_closed_idle_timeout` with `524288B` owned and an active drain candidate
   is an unsafe lifecycle decision.

It is therefore premature to tune MTU, pool, windows, chunk size, D16, or
self-wake, and premature to assume the external path or mini_vpn alone caused
the forward loss.

## Confirmed Follow-up: Lifecycle Gate

`run_relay_d16` now snapshots the byte-owned queue when the half-closed timer
fires. It defers termination only while queued or leased payload remains and
there is no pre-existing terminal cause. Reserved-only read capacity does not
keep a dead flow alive. The new diagnostics are:

```text
tcp-d16-half-closed-idle-blocked
half_closed_idle_blocked_events
half_closed_idle_blocked_max_payload_bytes
```

The focused test `d16_half_closed_idle_waits_for_owned_payload_to_drain` first
reproduced the premature timeout, then proved both queued and leased/inflight
payload block it, and finally proved the timer may terminate after the permit
and payload ownership are released. The repair does not add a new self-wake or
change D16 capacity, actor cadence, MTU, pool, QUIC windows, or chunk behavior.

Clean-clone validation at exact `a54fb17` plus the reviewed patch passed:

```text
library single-threaded: 611 passed, 0 failed, 2 ignored
integration:             10 passed, 0 failed, 4 ignored
D16 focused filter:      64 passed
concurrency 64/256/1024: 64/64, 256/256, 1024/1024
UDP sweep:               500/500 at every payload size, zero loss
check/fmt/diff review:   PASS
```

The isolated 1024-flow result was `12.424s` wall / `4.292s` relay. One
parallel full-suite performance assertion fell to `157.5 Mbit/s` under test
contention; three isolated replays exceeded `170 Mbit/s`, and the complete
single-threaded suite passed. No product code was changed for that harness
contention result.

## Confirmed Follow-up: Same-Window Forward Discriminator

The valid control and mini_vpn sample used the same Shoes PID, corrected
CA-signed server leaf, `.111:8443` listener, `.77` target, Cubic, pool `2`,
TUN MTU `1200`, and unchanged product QUIC policy. `MINI_VPN_TUIC_MTU_POLICY`
remained `default`, exactly as in the accepted Gate A/B profile; TUN MTU 1200
must not be confused with the optional QUIC `safe1200` policy.

The valid sing-box `1.13.14` control reported:

```text
direct forward sender/receiver: 224.125 / 214.243 Mbit/s
tunnel forward sender/receiver: 184.020 / 182.856 Mbit/s
client socket rb/tb/drop:       16777216 / 16777216 / 0
Exit socket rb/tb/drop:         17250000 / 17250000 / 0
target-only route:              PASS
```

The fresh mini_vpn forward-only P1 reported:

```text
sender/receiver:                  204 / 193 Mbit/s
TUN RX/TX drop delta:             0 / 30
formal-window QUIC lost bytes:    57,632,755
formal-window congestion events: 19,438
final QUIC lost bytes/events:     62,280,889 / 21,177
QUIC flow-control blocked deltas: 0
PLPMTUD probes sent/lost:         4 / 0
half-close blocked events:        0
attribution: quic_loss_congestion+inherited_quic_congestion+local_tun_egress_drop
```

The final mini client socket had already closed before the manual `ss`
snapshot, so that file is empty. This does not hide the loss boundary: the
client pcap accounted for `558,558,962B` of outbound UDP, matching Quinn's
`558,549,351B` UDP TX counter, while the Exit capture received only
`496,353,577B`; both tcpdump processes reported zero kernel capture drops and
the live Exit socket reported `d0`. The `62,205,385B` bilateral gap closely
matches Quinn's final `62,280,889B` loss counter.

The mature control's corresponding client-out/Exit-in UDP payload was
`493,607,820B / 469,654,404B`, about `4.9%` path loss. The mini_vpn pair was
`558,558,962B / 496,353,577B`, about `11.1%`. Packet size is not the selected
root: control predominantly sent `1441B` UDP payloads while mini_vpn
predominantly sent `1280B` after loss recovery.

Capture timing selects burst shape as the first falsifiable seam:

| Client | Max per 1 ms | Max per 10 ms |
| --- | ---: | ---: |
| sing-box | 93 packets / 132,738B | 579 packets / 826,006B |
| mini_vpn | 261 packets / 334,080B | 1,259 packets / 1,611,520B |

Quinn-proto `0.11.16` uses a userspace token-bucket pacer whose public source
clamps one burst to at most `256 * MTU`; the observed 261-packet millisecond is
a close match. Quinn's `black_holes_detected` is a derived
suspicious-loss-burst counter even when MTU discovery is disabled; here all
four actual PLPMTUD probes succeeded. It is evidence of the loss pattern, not
proof of an MTU black hole.

## Authorized GSO Tracer Plan — Completed / Failed

1. Add a transport-policy seam and deterministic TDD around Quinn's public
   `TransportConfig::enable_segmentation_offload` reachability. Record the
   effective GSO policy in startup and result artifacts. Do not change D16,
   TUN/QUIC MTU, pool, QUIC windows, chunk, congestion control, or self-wake.
2. Use GSO-disabled as a reversible tracer-bullet discriminator, not an
   immediate product default. Extend the local Quinn probe/harness to prove
   full delivery, clean EOF, bounded memory, and no regression below the
   `170 Mbit/s` capacity math before any VPS run.
3. If that local gate passes, run exactly one same-window forward control plus
   one fresh mini_vpn forward-only P1 with socket snapshots taken by the runner
   during the active interval and the same bilateral burst/byte capture. Pass
   only if TUN drops are zero and mini_vpn no longer shows the excess loss and
   `256`-packet-class burst edge.
4. If disabling GSO does not change burst/loss shape, revert the tracer bullet
   and design a bounded custom Quinn UDP send-service/pacing seam or upstream
   patch with a deterministic transmit-burst test. Do not stack another knob.
5. Only after the forward discriminator is clean may Task 12 restart at a
   fresh reverse P8 gate, followed by UDP/live-streaming, Linux fake-IP DNS,
   and TUN stop/rearm. No macOS TUN test is required.

The user authorized this plan. Its policy and local-capacity gates passed, but
the single GSO-disabled VPS discriminator failed: the valid control reached
`177.303/175.102 Mbit/s`, while mini_vpn reached `206/193 Mbit/s` with the same
`30` TUN TX drops and worse formal QUIC loss/congestion (`67,826,613B` and
`40,887` events). Post-failure code review confirms that disabled GSO limits
one Quinn-proto transmit result, not the Quinn connection driver's repeated
send/self-wake service. The replacement plan and full evidence are recorded
in `2026-07-13-knife14h10d16-gso-disabled-forward-discriminator-results.md`.

No transport-policy implementation or further VPS sample is authorized until
this modification plan is confirmed.

## Artifacts And Cleanup

Sanitized bundles are retained at:

```text
/tmp/mini_vpn_h10d16_step4_repair_a54fb17/
/tmp/mini_vpn_h10d16_forward_discriminator_a54fb17/
```

```text
sustained60 SHA-256=d67ed709a8e0af8679f3f35014201983c42439c35246128b2769d52fac4ccabf
extra P1 SHA-256=d1c293b71cc24479fa6f97a87b5db9d47ee95158abebe07707c5020207dfd96e
forward/P8-stop SHA-256=65cb27a6e1f42b96a1c58970bb4e946b8fdcf1f7cad7207263d79e48eae2594a
forward control client pcap SHA-256=9e6bef757ec81d26c4a8bfc22c44f0ade5c259fcee6ca93c05f53093a41c4c6c
forward control Exit pcap SHA-256=f30910694ef304ad3ea189d6ea162929686deff0ee707326cd92f2ccba0714b6
forward mini client pcap SHA-256=9a0a3632ed4f72caad1560961954d4d3b44acc2481163ac3114ee6dc9613f5fd
forward mini Exit pcap SHA-256=ea45268f90834ad56deb879a95e7ae7cb2d2337bd65ef9aafec6255eb532eb8a
Exit evidence archive SHA-256=992a60262464ce05aa5f78afb9fb0f1565d9489b643ec71c7210ae8a2a22139a
```

Archive integrity and the no-secret scan passed. `.27` has no mini_vpn, TUN,
target route, or iperf client. `.111` has no Shoes listener or runtime
material, and all four temporary socket sysctls are restored to `212992`.
`.77` iperf3 remains active. No macOS TUN test ran.
