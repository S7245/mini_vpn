# Knife15 M1 Multi-Connection Rebind Retention Local Results

Date: 2026-07-21

Status: **LOCAL PASS — identified rebind lifecycle repaired; real M1 remains
failed and M2/M3 remain blocked**

## Outcome

The user-operated bundle
`/tmp/mini_vpn_knife15_macos_20260721_110225.tar.gz`, SHA-256
`62280ba0b638f73fee8647e2461f3a0880a08b6dd252f3e29624d09e5ae27715`,
matches exact source `1c587ba8bcc4c78c7fb77de72c1d40aba87f88f5` and its
release binary and runner hashes. Archive traversal, link, provenance, and
secret checks pass. The operation was correct; it was not stopped by the user,
sudo timing, Clash, stale artifacts, or route contamination.

Fresh physical baseline passed at `21.361983 Mbit/s` forward and
`65.381298 Mbit/s` reverse with no receiver-zero intervals. The fresh
300-second direct receiver passed at `10.675549 Mbit/s` with no stall. M1 then
ran for about 6 hours 43 minutes, completed four active windows and all three
idle/resume checkpoints, and failed cycle 28 `steady-c/short-reverse-6`.

This is a real M1 failure. The authoritative local receiver contains eight
complete approximately one-second zero-byte rows, not a partial command tail.
It received `12,794,880B` at `10.230903 Mbit/s`; the remote sender reported
`21,935,104B` at `17.260027 Mbit/s` with 11 retransmits.

## Selected Boundary

The failed conn1 business stream recorded an `8,301ms` read gap while conn0's
control stream simultaneously recorded a `10,733ms` gap. The existing
Endpoint monitor correctly detected endpoint-wide TX without RX and rebound
the shared UDP socket after the frozen two-second lower bound. Quinn then
reported current-socket recovery after `1,499ms`, but the conn1 business path
remained stalled.

The old Quinn lifecycle released the retained previous socket after the first
known connection packet arrived on the current socket. Vendored source even
contained `TODO: Account for multiple outgoing connections.` For a pool of two,
one connection could therefore prove the Endpoint-wide generation and release
the other connection's old receive path before that connection migrated.

The local data plane stayed healthy during the failure: Target remained on the
owned `utun4`, Exit remained on `en0`, Exit/gateway controls had zero loss,
physical and utun interface errors were zero, pump high-water was `113/500`
with zero full waits/read errors, and EndpointWindowV1 conservation stayed at
or below `61,440B` and ended `61,403/0/0B`. This rejects D16, TUN drain,
Endpoint pacing capacity, resource exhaustion, and local cleanup as the
selected boundary.

The same partial M1 independently failed the frozen reverse-UDP SLO in two of
24 completed windows: `4.421980%` and `4.127822%` versus the `3.0%` maximum.
The mean was `1.461368%`; 17 windows exceeded `1%`. Local UDP drop,
backpressure, route, ICMP, and interface evidence stayed clean. The TCP/rebind
repair does not explain away or waive this independent result.

## Repair

At each successful Quinn Endpoint rebind generation `g`, the Endpoint now
snapshots every live connection handle and retains exactly one previous socket
until every snapshot member either:

- authenticates a QUIC packet received on the current socket generation; or
- drains from the Endpoint.

Old-socket packets, routed-but-unauthenticated current-socket packets, stale
generation events, and connections created after the snapshot cannot complete
that pending set. A second explicit rebind replaces the one previous socket
and its snapshot rather than chaining sockets. Each connection reports at most
one authenticated recovery event per generation, so ordinary receive traffic
does not create an ongoing Endpoint event stream.

Quinn exposes a monotonic per-connection current-socket generation. mini_vpn's
pure recovery policy snapshots the stable IDs present at successful rebind and
emits `tuic-endpoint-rebind-recovered` only after the Endpoint generation and
every sampled connection generation reach `g`. A missing or replaced
unrecovered identity remains fail-closed. The existing runner-compatible log
fields remain unchanged and now append the expected connection count.

No D16, MTU `1200`, UDP payload `1160B`, pool `2`, QUIC window, chunk, Cubic,
GSO, self-wake, recovery bound/cadence, workload, duration, or acceptance SLO
changed.

## TDD And Code Review

Focused RED/GREEN coverage now proves:

- first-connection recovery retains the previous socket for the second;
- all recovered or drained members release it;
- old-socket, unauthenticated, and stale-generation packets cannot advance
  recovery;
- a post-rebind connection cannot extend old-socket ownership;
- a later rebind replaces the prior snapshot;
- Endpoint-level generation plus only one recovered pool connection is not a
  mini_vpn recovery;
- both established loopback connections survive rebind, reach generation 1,
  and preserve EndpointWindowV1 conservation.

Code review found one P1 in the first implementation: Endpoint routing occurs
before packet-protection authentication, so routing alone could accept a
spoofed or corrupt packet as migration proof. The final design compares Quinn
proto's authenticated-packet count around event handling and reports only a
real authentication advance. The affected and full gates passed afterward.
There are no unresolved P0/P1 findings.

## Final Local Gates

```text
root all-targets                 646 passed / 3 ignored; main 2/2
32 MiB EndpointWindowV1         232.164 Mbit/s; final 61,440/0/0B
vendored Quinn                  36 passed / 3 ignored; doc 1/1
Quinn integration              1 expected ignored
vendored quinn-proto            309/309; doc 3/3
cargo build --release           PASS
cargo clippy --all-targets      PASS (established warnings only)
Knife15 runner self-test        PASS
Knife15 wrapper self-test       PASS
Knife14 three shell self-tests  PASS
shell syntax / root fmt / diff  PASS
changed-content secret scan     PASS
```

The standalone Quinn gate must use the explicit absolute local
`patch.crates-io.quinn-proto.path`; invoking its manifest without that patch
selects the registry proto and is an invalid dependency graph for this fork.

## Accepted Stop Position

The identified premature old-socket-release defect is locally repaired, but
the six-hour bundle is not an M1 acceptance. The repair is sufficient for that
specific lifecycle defect and necessary-only for a complete M1: WAN migration
and the independent UDP SLO still require real evidence.

Next take one fresh user-operated HK M1 from the pushed repair source. Rebuild
release and create fresh baseline/direct artifacts because the binary changed.
Completely disable Clash-TUN and every other VPN/TUN before baseline and keep
them disabled through Knife15 `stop`. Use the reviewed
`baseline -> direct -> start -> smoke -> m1 -> status -> stop` sequence; on
failure preserve `status -> snapshot -> stop`. Do not tune frozen constants or
waive UDP/receiver SLOs. M2 and M3 remain blocked.
