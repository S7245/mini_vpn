# Knife14 H10d16 Pacer-Cap64 Forward Discriminator Results

Date: 2026-07-13
Source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`
Verdict: **FAIL; Task 12 step 4 remains stopped before P8**

## Scope And Stop Rule

This run spent the architecture spec's one authorized same-window
Gate-aligned sing-box forward control and one fresh mini_vpn forward-only P1
with `PacerCap64`. The control passed, but mini_vpn failed the formal TUN-drop,
QUIC-loss, and pcap-burst gates. No retry, cap-value sweep, P8, UDP,
fake-IP DNS, rearm, macOS TUN, commit, or Slack notification followed.

The measured profile remained target-only, GSO enabled, Cubic, pool `2`, TUN
MTU `1200`, default QUIC MTU policy/windows, `1 MiB` TCP buffers, H10d16
byte-owned egress, zero ordered/native chunk experiments, and D3 self-wake
disabled. The rejected bounded socket sender was absent.

## Exact Build And Temporary Exit

The Linux release was built from the isolated reviewed source snapshot rather
than the dirty `.27` checkout:

```text
source_archive_sha256=d401dd31de1b85a7b7423e97070773707deda6c307e015aad9e8d720ca464ce2
binary_sha256=7446d8ee18da52c9cc9a05d313bf6ced05fed5aae7397026f2fecbd9136a4b48
suite_sha256=7f0a16bc04a630addff304486f19a3de6464c3ac543e74c5553afe5bda2c07ba
probe_sha256=9542188666709d76b7728d53be6bfaf8d930cab9d2a3cae19b70060aeddc6f11
control_sha256=b6bbee22ce20f421901e6dea60d5179cc86ff250362f43cd562b7e20cc5302e7
```

The temporary `.111:8443` Exit used official Shoes `v0.2.7`, binary SHA-256
`160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147`,
two worker threads, one endpoint, explicit `h3`, and zero RTT disabled. Shoes
`--dry-run` first exited `0` after loading two cert/key objects. The formal
service then logged `Starting 1 server(s)`, stayed at PID `440402` with
`NRestarts=0` across both samples, and consumed config/certificate/key through
concurrent root-only one-shot FIFOs. The client trusted the distinct CA:true
certificate rather than the CA:false server leaf.

## Same-Window Forward Control — PASS

The Gate-aligned sing-box `1.13.14` control retained MTU `1200`, Cubic, P1,
and target-only routing:

```text
direct sender/receiver:   224.360 / 215.312 Mbit/s
control sender/receiver:  192.567 / 191.928 Mbit/s
client UDP rb/tb/drop:    16777216 / 16777216 / 0
Exit UDP rb/tb/drop:      17250000 / 17250000 / 0
```

Both control captures reported zero kernel drops. Client and Exit pcaps held
`506802` and `471276` packets and had SHA-256 values
`dbe9ed90aad933eab8df6e373e599dadd3bb2502892577f99d98d48a4f81940f`
and `567845ae462f783e95701b0f934bb1e31cdc3dc0ac0c3c0dc0d888e48f933d32`.

## mini_vpn P1 — Formal Failure

The one P1 completed all 20 intervals at high aggregate throughput, but its
first two intervals exposed the same burst/loss edge:

```text
sender/receiver:       196 / 185 Mbit/s
intervals:             20/20 nonzero
first two intervals:   400 / 39.8 Mbit/s
tail avg/min:          190.833 / 183 Mbit/s
tail collapse:         0
TUN RX/TX drop delta:  0 / 29
QUIC lost-byte delta:  50621275B
loss limit:            16777216B
congestion delta:      15417
flow-control blocking: 0
```

The runner correctly propagated this discriminator failure and stopped before
the full sweep. The final data-connection totals were `52654396B` lost and
`16674` congestion events; the formal delta subtracts its already observed
`2033121B` / `1257` start state.

Pool selection and attribution were unambiguous. The two iperf TCP connections
opened on pool connections `0` and `1`, with no reconnect or connection-ID
change. From the last quiet snapshot to the final snapshot:

```text
data conn 1:  407331 datagrams / 525986598B
aux conn 0:        16 datagrams /      1019B
data share:    99.9961% datagrams / 99.9998% bytes
```

Every formal `cwnd` was below `u32::MAX`; data-connection values ranged from
`6231B` to `50858B`. No flow-control block or path migration was observed.
The data connection's five formal Pacer states were:

| Snapshot | Uncapped | Effective | Cap active | Delay events |
| --- | ---: | ---: | --- | ---: |
| 1 | 80517B | 80517B | false | 12 |
| 2 | 327680B | 81920B | true | 22 |
| 3 | 72465B | 72465B | false | 22 |
| 4 | 65830B | 65830B | false | 22 |
| 5 | 58758B | 58758B | false | 22 |

Thus the configured cap was reachable and did bind once at exactly
`64 * 1280B`; it was not bypassed by an extreme congestion window. It did not
remain the limiting mechanism as congestion and the sub-millisecond path
changed the upstream Pacer capacity.

The timed close had no H10d16 downlink pending, close-egress bytes, terminal
pending reap, send-slice error, or reconnect. The upload data handle recorded
the classified timed-boundary `remote_write_failed` with `1048560B` still in
the local receive queue, while the small iperf control handle closed by remote
EOF. This is retained as a lifecycle caveat; the drop/loss/burst gates already
make the sample a formal failure.

## Bilateral Pcap Discriminator

Both P1 captures reported zero kernel capture drops. Exit GRO coalesced
multiple datagrams, so client egress is the authoritative packet-cadence view;
bilateral bytes remain useful loss evidence.

| Window | Client-out bytes | Exit-in bytes | Gap | Gap ratio |
| --- | ---: | ---: | ---: | ---: |
| sing-box control | 542451937B | 491963330B | 50488607B | 9.31% |
| mini_vpn cap64 | 526004719B | 475126023B | 50878696B | 9.67% |

The shared raw-path gap means this window does not attribute all UDP loss
uniquely to mini_vpn; the mature control nevertheless sustained `191.928
Mbit/s` without socket drops. The mechanism gate is independently decisive:

| Client egress | Max per 1 ms | Max per 10 ms |
| --- | ---: | ---: |
| same-window sing-box | 100 packets / 142818B | 605 / 863481B |
| prior mini_vpn Quinn default | 261 / 334080B | 1259 / 1611520B |
| mini_vpn pacer-cap64 | 267 / 341760B | 1337 / 1711360B |

The cap64 peak occurred at `2026-07-13T13:12:25.672Z`, inside the formal P1.
It did not materially leave the old `163-261 packets/ms` failure class and was
slightly above the prior GSO-enabled Quinn-default peak.

P1 client and Exit captures contained `477836` and `207201` packets, reported
zero kernel drops, and had SHA-256 values
`eaca4ba3273636c7f57a2a95b8ccc6d9c7d159cf020e96e5b7f1f14a3848517e`
and `27c5abbed75fc60a94e0c8b8ed6f54bae0a75d84c6bbc17a0bb2e3b32947dcdc`.

## Post-Failure Code Review

No policy propagation, pool-selection, GSO, migration, `cwnd > u32::MAX`,
or stacked-sender defect explains the result. Startup fingerprinting passed,
the selected data connection carried virtually all QUIC output, formal stats
proved the cap active once, and the runner stopped on the exact failed gates.

The rejected assumption is architectural. Quinn's `Pacer::capacity` bounds
stored tokens, but `Pacer::delay` deliberately retains the upstream refill
slope `1.25 * cwnd / rtt`. At the observed `0-1ms` RTT, a single millisecond
can replenish and spend multiple token buckets. After delay event `22`, later
formal snapshots transmitted hundreds of thousands of datagrams without
another delay event. A maximum stored-token cap is therefore not a 1ms/10ms
sliding-window or endpoint service bound.

The active socket sampler also exposed a non-causal observability gap: its
peer-address match captured all 12 Exit samples at `drop=0`, but no client row
because Quinn's UDP socket is unconnected. Future evidence code must identify
the mini_vpn UDP FD/local port rather than assuming `ss` prints the peer.

No product code was changed during this review. Per the architecture stop
rule, do not try another cap constant, reopen GSO-only testing, revive the
public `AsyncUdpSocket` cooldown sender, or tune frozen transport parameters.

## Proposed Next Modification Plan — Awaiting Confirmation

1. Preserve `QuinnDefault` as production/default and treat `PacerCap64` as a
   rejected single-flow VPS candidate pending later cleanup.
2. RED a deterministic sub-millisecond-RTT fake-time replay from this formal
   trace. It must prove that a stored-token ceiling can refill multiple times
   inside 1ms and cannot satisfy the required 1ms/10ms envelope.
3. Write a new capacity/reachability spec for Quinn-proto pre-accounting state
   that owns a true endpoint time-window/service contract across both pool
   connections. It must cover control reserve, idle borrowing, fairness,
   deadline composition, cancel/migration cleanup, and `>170 Mbit/s` capacity
   before implementation.
4. If and only if that spec and plan are confirmed, TDD default equivalence,
   aggregate window bounds, no busy wake, ACK/handshake/loss/close semantics,
   two-connection fairness, exact local delivery/EOF, and the full local
   regression suite before requesting any new VPS run.
5. Repair the unconnected-Quinn socket sampler as an observability task, not as
   a throughput fix.

Task 12 step 4, P8, later product regressions, old-path cleanup, and any new VPS
sample remain stopped pending confirmation of that plan.

## Confirmed Design-Preparation Follow-Up

The user confirmed the preparation plan. A fake-time sub-ms replay reproduced
the rejected mechanism exactly: a cap64 Pacer at `RTT=200us`, `cwnd=40000B`,
and `MTU=1280` issued `259` datagrams inside `1ms`. The test now remains as a
default mechanism characterization.

The new endpoint pre-accounting architecture/capacity spec and implementation
plan are prepared at:

- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-architecture-spec.md`
- `docs/tech/2026-07-13-knife14h10d16-endpoint-pacing-service-implementation-plan.md`

They require a separate explicit confirmation before coordinator
implementation. No new VPS authorization follows from the design work.

## Artifacts And Cleanup

Sanitized evidence is retained locally at:

```text
/private/tmp/mini_vpn_h10d16_pacer_cap64_discriminator_a54fb17/
control-artifacts.tar.gz sha256=0e6099f3be36a5ac0d07753727ffadf93d1b888cf2d0eb9982a2310fdcf28a61
p1-artifacts.tar.gz      sha256=59ce8cee9695d70feeaba173625db7627370a0c420e9b4ba236286a62d676460
```

The archives contain no environment file, private key, service account,
unredacted UUID, or password. The full pcaps were analyzed in place, recorded
by exact hashes, and deleted from the VPSes.

The `.27` process, TUN, target route, build, logs, and captures are gone. Shoes,
FIFOs, watchdog, captures, and UDP `8443` are gone from `.111`; all four socket
sysctls are restored to `212992`. `.77` iperf3 remains active. No repository
commit was created.
