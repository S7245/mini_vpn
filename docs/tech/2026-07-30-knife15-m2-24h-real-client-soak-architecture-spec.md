# Knife15 M2 24-Hour Real-Client macOS Soak Architecture Specification

Date: 2026-07-30

Status: **LOCAL PASS — contract implemented and reviewed; user-run HK macOS
M2 pending**

## Stage Goal

M2 must prove that the accepted Knife14 H10d16 data plane and the formally
accepted Knife15 M1 runner remain bounded, useful, and leak-free for 24 hours
under controlled IPv4 full-tunnel client activity.

M2 adds a release-readiness surface that M1 intentionally did not cover:

- system-resolver DNS through the TUN;
- split-default IPv4 routing through the TUN while the TUIC Exit stays
  physical;
- real HTTPS browsing and public egress-IP evidence in addition to the
  deterministic Target workload;
- six long-run idle/final checkpoints;
- bounded evidence, disk, resources, relay state, fake-IP state, and cleanup;
- explicit HK/Shenzhen physical-path attribution.

M2 is sufficient only for the 24-hour controlled IPv4 macOS client gate. It is
not Network Extension, App sandbox/background, IPv6 tunnelling, mobile,
Windows, failure-injection M3, or final product release acceptance.

## Accepted Input Position

The exact-source HK formal M1 bundle is accepted:

```text
bundle=/tmp/mini_vpn_knife15_macos_20260729_105127.tar.gz
sha256=c263507c52d0146a39a74516b90a51b36fe43d082ea1b897d8f6c9c7dffc1b91
source_commit=ee1a42383097090e36146c33dc33fececa135b44
formal_m1_acceptance=PASS
```

The observer repair and acceptance record are committed at
`5e7a97c867beffd29096e1b2fa06da32340e36d9`. M2 must run from that commit or a
descendant. M0, M1, and M1 diagnostic must not be repeated for this gate.

## Frozen Product Boundary

M2 preserves:

- H10d16 `524,288B` per-flow and `67,108,864B` global byte ownership;
- EndpointWindowV1 at `30,720,000 wire B/s`, `61,440B` burst,
  `92,160B/1ms`, and `368,640B/10ms`;
- MTU `1200`, UDP application payload `1160B`, pool `2`, `1MiB` TCP socket
  storage, `368,640B` receive credit, Cubic, GSO, and self-wake;
- one-second direction-aware receiver evidence and the `1KiB` reverse TCP
  observer;
- the M1 `16MiB` TCP gap, `3.0%` reverse-UDP loss, `7,000ms` recovered-rebind,
  Endpoint conservation, D16 ownership, and pump/TUN failure rules;
- `30s` process/interface/network sampling;
- `256MiB` mini_vpn log hard limit and `128MiB` retained-log setting.

M2 changes runner-owned routes and DNS only. It adds no data-plane code or
throughput mechanism. HK/Shenzhen Mbps remains path evidence, not an
architecture discriminator.

## Controlled IPv4 Full-Tunnel Contract

`start` remains target-only and unchanged. Only the public `m2` action may
expand the run after baseline, direct discriminator, start, and smoke pass.

Before mutation, M2 records:

- current Exit interface and gateway;
- current route for two public IPv4 discriminator addresses;
- the macOS network service that owns the physical Exit interface;
- the exact DNS-server list for that service;
- the absence of a routable non-loopback IPv6 path.

M2 then owns these mutations, in order:

1. an explicit `/32` Exit route through the recorded physical gateway;
2. `0.0.0.0/1` and `128.0.0.0/1` through the owned utun;
3. `198.18.0.0/15` through the owned utun;
4. the physical service DNS list, temporarily set to the frozen
   `DNS_TARGET`;
5. a resolver-cache flush.

The already owned Target and `DNS_TARGET` `/32` routes remain on the utun.

The active invariant is:

```text
Target route == utun
DNS_TARGET route == utun
198.18.0.0/15 route == utun
low-half public probe route == utun
high-half public probe route == utun
Exit route == recorded physical interface/gateway
system DNS service == DNS_TARGET
no routable physical IPv6 default
```

Every cleanup path compares current state with the state M2 owns. It removes
only matching M2 routes, restores DNS only from the protected pre-M2 snapshot,
and removes the explicit Exit route only when its interface and gateway still
match. Unexpected external route or DNS changes fail closed instead of being
silently overwritten.

The watchdog and `stop` share the same idempotent cleanup. A process death,
signal, partial route setup, workload failure, normal stop, or repeated stop
must converge to the exact pre-M2 DNS list and a non-utun Target/DNS/Exit/
public route state.

## IPv6 Leak Boundary

mini_vpn's current TUN data plane is IPv4. M2 must not present an IPv4-only
test as a leak-free dual-stack VPN.

Before full-tunnel mutation and throughout M2, a route lookup to a stable
global IPv6 discriminator must have no routable physical interface. A
physical IPv6 default blocks M2 before the 24-hour workload. The user may
disable IPv6 for the dedicated test service and take a fresh run; the runner
must never silently allow an IPv6 side channel.

The pre-baseline operator check and formal M2 must use the same global IPv6
discriminator and classifier. The exact known macOS `not in table` line is
positive absence evidence whether `route` returns zero or nonzero, but only
when no interface is present. A successful lookup with `lo0`/`utun*` is safe;
any physical interface is unsafe; any other status, text, or missing-interface
combination is unknown and fails closed. Formal M2 persists the raw
status/text/class/interface before it accepts or rejects the precondition.

IPv6 tunnelling itself remains a separate future architecture stage.

## Provenance And Preconditions

One formal M2 requires:

1. source at `5e7a97c` or a descendant, a release binary, and runner hash;
2. no existing Knife15 workload or finalized evidence in the current TUN run;
3. a fresh direct forward/reverse baseline in `M2_BASELINE_DIR`;
4. a matching 300-second direct Target receiver continuity PASS in
   `M2_DIRECT_DIR`, completed within 15 minutes;
5. user-run `start` and `smoke`, complete recent controls, and exact TCP-pool
   idle;
6. explicit `DNS_TARGET`, `DNS_NAME`, and usable `curl`, `dig`,
   `networksetup`, `dscacheutil`, `route`, and `jq`;
7. at least `4GiB` free before M2 evidence creation;
8. no routable physical IPv6 default;
9. successful full-tunnel activation and a pre-workload real-client probe.

The pre-workload probe must complete before the 24-hour controller is
registered. A bad route, DNS service, egress identity, fake-IP answer, HTTPS
transaction, or external dependency therefore costs minutes rather than a
24-hour run.

## Exact 24-Hour Timeline

The formal traffic/drain budget is exactly `86,400s`:

| Segment | Mode | Seconds | Purpose |
|---|---:|---:|---|
| steady-a | steady | 14,400 | four-hour warm plateau |
| idle-1 | drain | 600 | ownership/resource checkpoint |
| quiet-a | quiet | 10,800 | three-hour low-load client use |
| idle-2 | drain | 600 | checkpoint |
| steady-b | steady | 14,400 | four-hour repeat |
| idle-3 | drain | 600 | checkpoint |
| churn | churn | 10,800 | three-hour connection churn |
| idle-4 | drain | 600 | checkpoint |
| quiet-b | quiet | 10,800 | second low-load window |
| idle-5 | drain | 600 | checkpoint |
| steady-c | steady | 21,600 | six-hour post-churn plateau |
| final | drain | 600 | final ownership/resource checkpoint |

```text
14,400 + 600 + 10,800 + 600 + 14,400 + 600
+ 10,800 + 600 + 10,800 + 600 + 21,600 + 600
= 86,400 seconds
```

DNS commands, HTTPS probes, health polling, transitions, and final validation
add bounded wall-clock overhead. The user should reserve about 25 hours.

## Workload And Exact Evidence Counts

Each active cycle retains the M1 sequence:

```text
300s forward TCP
300s reverse TCP
180s reverse UDP at 1160B
6 x 10s alternating short TCP (steady/quiet)
or 24 x 10s alternating short TCP (churn)
fake-IP DNS
real-client HTTPS/egress probe
```

Rates remain baseline-derived:

| Mode | persistent TCP/UDP | short TCP |
|---|---:|---:|
| quiet | 25% | 50% |
| steady | 50% | 80% |
| churn | 25% | 50% |

The offered-rate cap remains `200,000,000 bit/s`, below the accepted
EndpointWindowV1 application capacity of about `239.167 Mbit/s`.

Exact schedule arithmetic produces:

```text
active windows=6
idle/resume pairs=5
final drain=1
complete cycles/DNS/real-client probes=93
phase results=1029
TCP results=934
UDP results=95
checkpoints=6
```

Partial final cycles retain structured TCP/UDP evidence but do not claim a DNS
or real-client cycle completion.

## Real-Client Probe

The fixed preflight and every complete active cycle use macOS `curl` through
the system resolver, without `--resolve`, a proxy environment, or an alternate
DNS API.

Each sanitized probe proves:

- the public egress-IP HTTPS response equals the expected Exit IPv4;
- the curl remote address is in `198.18.0.0/15`, proving fake-IP resolution;
- a second HTTPS browsing transaction returns a `2xx` or `3xx` response,
  positive bytes, and a `198.18.0.0/15` remote address;
- `DNS_TARGET`, both public halves, and fake-IP routes use the owned utun;
- the Exit uses the recorded physical route;
- the active DNS service contains exactly `DNS_TARGET`;
- the same-window Target/Exit/gateway/physical controls remain valid.

Response bodies are discarded after their size/status are checked. Evidence
contains only URL host labels, status, byte count, fake address, expected
egress address, routes, and timestamps.

## Evidence And Storage Capacity

The accepted M1 raw directory was about `69MiB`; its compressed bundle was
about `5.6MiB`. Linear 24-hour evidence is approximately `207MiB`.
`mini_vpn.log` was about `36.7MiB`, implying roughly `110MiB` over 24 hours.

M2 therefore requires:

- at least `4GiB` free before activation;
- the existing `1GiB` hard low-disk watchdog floor;
- no mini_vpn log compaction in an accepted run;
- one immutable bundle and checksum;
- no response-body retention;
- bounded `1029` JSON result files and `93` small real-client records.

This is sufficient by more than an order of magnitude without changing
data-plane memory or queues.

## M2 Checkpoints And Resource Plateau

`m2-checkpoints.csv` contains:

```text
timestamp,label,rss_kib,fd_count,thread_rows,
endpoint_available_bytes,endpoint_live_bytes,endpoint_outstanding_bytes,
active_relays,total_relays,fake_ip_active,fake_ip_registered,
dns_forged,dns_dropped
```

Required labels are `idle-1` through `idle-5` and `final`. Every checkpoint
must be based on a fresh process, Endpoint, and data-plane sample taken during
that drain.

At every checkpoint:

- Endpoint live and outstanding bytes are zero;
- Endpoint conservation is at most `61,440B`;
- active relays are zero;
- active fake-IP entries are zero after the 600-second drain;
- registered fake-IP cache entries are at most the two fixed M2 HTTPS host
  mappings and do not grow between checkpoints;
- DNS drop count is zero.

Resource bounds retain the accepted M1 envelope:

- checkpoint RSS maximum `<=131,072KiB`;
- final RSS `<= first RSS + 32,768KiB`;
- checkpoint FD maximum `<= first + 2`, final `<= first + 1`;
- checkpoint threads maximum `<= first + 2`, final `<= first + 1`.

Cumulative relay and DNS-forge counts are expected to grow with work and are
not leaks. The fake-IP pool intentionally retains inactive registrations for
the frozen `1,800s` TTL, so a `600s` drain cannot correctly require
`fake_ip_registered=0`. Zero active ownership plus a stable cache of at most
the two fixed M2 HTTPS hosts is the authoritative accumulation discriminator;
changing the TTL or schedule to manufacture zero is out of scope.

## Acceptance SLOs

### Timeline and useful traffic

- `m2.status=complete`;
- exact six active windows, five idle/resume pairs, one final drain;
- exactly 93 complete cycles, DNS results, and real-client results;
- exactly `934` TCP plus `95` UDP results;
- no phase, DNS, real-client, or health failure;
- no direction-aware receiver-zero interval;
- maximum TCP sender/receiver gap `<=16,777,216B`;
- maximum reverse UDP loss `<=3.0%`.

### Ownership and data plane

- every Endpoint sample obeys
  `available + live + outstanding <= 61,440B`;
- all six checkpoints and the final sample have zero live/outstanding
  ownership;
- D16 same-handle terminal ownership is clean;
- no stalled-write timeout, idle timeout, terminal reap, send-slice error,
  TUN flush failure, pump read error, or pump full wait;
- classified `Stopped(0)` remains visible REVIEW only under the accepted
  zero-ownership rule.

### Resources and evidence

- at least `2,700` numeric process, interface, network, and Endpoint samples;
- all six checkpoints pass the resource/active-state envelope;
- no physical-interface error counter movement/reset;
- no log compaction;
- no low-disk termination;
- secret scan and immutable archive checksum pass.

### Route, DNS, and real client

- full-tunnel activation evidence passes;
- every real-client result passes egress, fake-IP, HTTPS, route, and DNS
  validation;
- no physical IPv6 route appears;
- Exit and gateway controls remain attributable;
- if Endpoint rebind occurs, attempts equal recoveries, failures are zero, and
  maximum first current-socket RX is `<=7,000ms`;
- after `stop`, DNS equals the pre-M2 snapshot and no owned route points to the
  dead utun.

`m2_slo_evidence=PASS` is the pre-stop workload verdict.
`formal_m2_acceptance` remains `PENDING_CLEANUP` until `stop` proves cleanup,
then becomes `PASS`.

## Failure Discriminators

| Observation | Selected boundary | Required response |
|---|---|---|
| preflight egress IP differs, public route physical, or fake-IP absent | full-tunnel/DNS runner | stop before 24h; repair fixture/route contract |
| physical IPv6 route exists | IPv6 leak boundary | block M2; do not claim full tunnel |
| exact `not in table` line, no interface, and status zero | IPv6 route observer | classify safe absence; do not require an interface |
| unknown IPv6 route status/text combination | IPv6 route observer | fail closed and preserve raw evidence before another baseline |
| Target/Exit/gateway degrade together | external path/VPS | preserve evidence; no constant tuning |
| receiver fails while controls and ownership stay clean | connection/QUIC service | architecture review; no unchanged repeat |
| UDP loss exceeds 3% with healthy controls | UDP/TUIC quality | inspect datagram service, not TCP constants |
| checkpoint active relay/fake-IP/Endpoint state persists | lifecycle leak | deterministic log/checkpoint replay |
| RSS/FD/thread exceeds the envelope | resource lifecycle | deterministic resource/checkpoint replay |
| log compacts, disk guard fires, result/probe is missing | evidence architecture | shell TDD repair before another TUN |
| DNS/route cleanup mismatches | runner ownership/cleanup | keep bundle mutable, repair/restore safely |

## Stop Rules

- Expected RED fixtures may proceed directly to the smallest GREEN.
- Unexpected local failures are analyzed against this contract before repair;
  the user's standing instruction authorizes safe in-scope repair without
  repeated confirmation.
- Frozen data-plane values, workload SLOs, route scope, or IPv6 non-goal cannot
  be changed to make a failure pass.
- A local gate or review P0/P1 blocks user execution.
- A real M2 safety/evidence failure preserves `status -> snapshot -> stop`.
- A path-attributed failure is not a product tuning branch.
- M3 remains blocked until the M2 contract and runner are locally accepted;
  M3 events stay one variable at a time.

## Old-Path Audit And Sufficiency

M2 reuses the accepted TUN ingress, smoltcp, D16, TUIC Connect/Packet, Quinn
EndpointWindowV1, TCP pool, Endpoint rebind, watchdog, result validators,
summary, and immutable cleanup. PacerCap64, bounded sender, GSO-only, and
parameter-tuning branches remain closed.

The change is necessary and sufficient for the M2 evidence/full-tunnel gate.
It does not claim a new Mbps capacity path; Knife14 capable Linux/VPS remains
the capacity proof.

## Design Review Scores

- System-design: **10/10**. Scope, topology, route/DNS ownership, exact
  timeline/counts, capacity, observability, attribution, cleanup, and non-goals
  are explicit.
- DDIA/fault-tolerance: **10/10**. Safety, liveness, immutable evidence,
  cumulative-versus-active state, partial failure, and recovery ownership are
  separated.
- Release-readiness: **10/10 design target**. Bounded resources/logs,
  preflight, deep health, steady state, failure discrimination, and rollback
  are specified; the score is provisional until local TDD and the real M2
  bundle pass.
