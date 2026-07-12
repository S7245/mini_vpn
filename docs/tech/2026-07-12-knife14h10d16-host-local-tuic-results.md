# Knife14 H10d16 Host-Local TUIC Results

Date: 2026-07-12
Probe source: `0f07406c07a81a373a9b3dc426c4ad0a7128400a`

## Verdict

The host-local direct-TUIC discriminator passed. Running the exact direct
probe and the same sing-box `1.13.14` binary on `.111`, with the TUIC client
connected to `127.0.0.1:8443` and sing-box forwarding to `.77:5201`, delivered
`199.639 Mbit/s` receiver over 20 seconds. All `20/20` intervals carried data
and the minimum interval was `169.868 Mbit/s`.

This proves that the sing-box TUIC ingress, Connect handling, target-facing TCP
copy, and the mini_vpn generic OrderedJoin client path all have sufficient
single-flow capacity when the QUIC leg is loopback. It rejects an intrinsic
`2-5 Mbit/s` limit in the sing-box server/copy implementation or the direct
probe itself.

Together with the earlier `.111 -> .27` minimal-Quinn result of `192.597
Mbit/s`, the active boundary is now the interaction between sing-box/quic-go's
external TUIC sender and the `.111 -> .27` cross-host QUIC path. It is not the
D16 TCP/TUN egress architecture. Gate A and Gate B remain frozen until that
external/protocol boundary has a capable cross-host precondition.

## Fixed test shape

- exact release test binary from `0f07406`, SHA-256
  `11537685e7736e0ab22bdd602c9e37990eb14c7045e94cd91e0a6af63c724da8`;
- sing-box `1.13.14`, SHA-256
  `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`;
- client and server Cubic;
- client safe MTU 1200, `tcp_pool=1`, generic OrderedJoin;
- TUIC client and server on `.111`, connected over loopback;
- direct sing-box outbound to `.77:5201`;
- one 20-second reverse P1 with a strict receiver `>150 Mbit/s` floor and no
  zero-rate interval;
- temporary 16 MiB `.111` UDP socket maxima and 1 MiB defaults;
- FIFO-only service configuration and TLS material;
- credentials and client CA supplied only through ephemeral process/FIFO
  channels.

The pre-test `.111 -> .77` direct reverse baseline was `217.849 Mbit/s`
receiver.

## Result

Client iperf3 JSON:

```text
sender_mbps=200.607
receiver_mbps=199.639
duration_secs=20.001
sent_bytes=502530048
received_bytes=499122176
retransmits=44673
intervals=20
zero_intervals=0
min_interval_mbps=169.868
max_interval_mbps=305.873
```

The direct relay accepted and completed both iperf3 control/data connections:

```text
accepted_connections=2
completed_connections=2
failed_connections=0
local_to_remote_bytes=549
remote_to_local_bytes=499164917
```

The data Connect stream read `499164592B`, with a maximum read gap of `401ms`.
The client reported zero Quinn lost bytes, zero congestion events, and zero
connection/stream data-blocked frames. `.111` reported zero
`UdpInErrors`, `UdpRcvbufErrors`, and `UdpSndbufErrors`.

The `.77` sender independently showed all 20 one-second intervals carrying
data, with an aggregate `201 Mbit/s`. This is the opposite of the external
Cubic run, where the same target sender had `15/20` zero intervals and only
about `5.08 Mbit/s` aggregate.

## Evidence boundary

The combined experiment matrix is now:

| Path | Receiver | Delivery shape | Meaning |
|---|---:|---|---|
| raw UDP `.111 -> .27` | about `198 Mbit/s` | continuous | raw path has capacity |
| minimal Quinn `.111 -> .27` | `192.597 Mbit/s` | `21/21` nonzero | generic Quinn crosses the path |
| direct TUIC `.111 -> .27`, server Cubic | `2.674 Mbit/s` | target `15/20` zero | external TUIC sender/path interaction fails |
| direct TUIC host-local on `.111`, server Cubic | `199.639 Mbit/s` | `20/20` nonzero | TUIC server/copy and client probe have capacity |

Therefore do not reopen:

- D16 byte ownership, readiness queues, actor cadence, DrainOnly, or EOF
  ordering;
- TUN, smoltcp, native ordered readers, or auxiliary pool policy;
- client MTU, broad QUIC windows, chunk size, self-wake, or VPS socket buffers;
- sing-box target-facing TCP copy as an intrinsic capacity limit.

The host-local pass does not prove that every sing-box/quic-go version or every
external network is healthy. It specifically proves that the tested binary and
configuration can exceed the Gate A floor when the external QUIC leg is
removed.

## Review and next discriminator

No product-code correction follows from this result. The best next step is a
single cross-host server-implementation/version A/B on `.111:8443`, preserving
the exact `0f07406` client binary, Cubic/safe1200 profile, `.27` client, `.77`
target, strict floor, and target-side interval evidence.

Prefer an alternate mature TUIC server implementation over another sing-box
configuration or congestion-control tweak. If it exceeds `150 Mbit/s` with no
zero interval, the active defect is specific to the current sing-box/quic-go
external sender path and that capable server can qualify one composite Gate A.
If it reproduces `2-5 Mbit/s`, collect bilateral packet timing and server-side
QUIC transport evidence before considering any product change. Gate B remains
blocked until composite Gate A passes.

## Setup correction

The first TLS preflight used the server leaf certificate as the client trust
anchor and failed with `UnknownIssuer` before any TUIC Connect or iperf traffic.
The official run supplied the configured client CA through its own FIFO and
then passed. This was a setup correction, not a failed capacity attempt.

Reusable rule: keep server leaf/key FIFOs and client CA FIFOs distinct, and
require a successful TLS/TUIC preflight before starting measured traffic.

## Artifacts

- host-local client/server bundle:
  `/tmp/mini-vpn-tuic-hostlocal.tar.gz`
  (`57cea2889738b39dc0d4888924bef4c4dcbc202d139b3fedacdaa163725228ea`);
- target journal:
  `/tmp/mini-vpn-tuic-hostlocal-target.log`
  (`4ed0bd9dd7561223f8e388a80a664b622c7aaef1b936e37e09a3141f6676e852`).

The bundle contains only the whitelisted client log, iperf JSON, preflight
error, profile, service journal, socket/kernel snapshots, service state, and
binary hashes. It does not contain service configuration, environment files,
credentials, certificates, or private keys.

## Cleanup

- the temporary `.111` service and restore watchdog are inactive;
- `.111:8443` has no remaining loopback listener;
- all four temporary `.111` socket sysctls were restored to `212992`;
- runtime FIFOs, transient binaries, remote artifacts, and clean-clone build
  material were removed;
- `.77` iperf3 remained active and unchanged;
- no credential, private key, environment value, or sudo password was written
  to the repository or retained artifact.
