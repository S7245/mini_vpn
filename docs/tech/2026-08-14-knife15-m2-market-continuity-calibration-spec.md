# Knife15 M2 Market Continuity Calibration Specification

Date: 2026-08-14

Status: **SUPERSEDED; DO NOT RUN THE MATURE-CLIENT COMPARISON**

The user replaced this comparison route with the bounded Tier-A resource and
Tier-B low-frequency continuity strategy in
`docs/tech/2026-08-14-knife15-m2-tiered-continuity-resource-strategy-spec.md`.
This document is retained only as historical decision evidence. Its existing
reducer may be reused for complete-interval parsing, but its cohorts and C0/C1
decision tree are no longer active.

## Decision

Do not start an established-stream protocol/server replacement from the
`cdbfe36` formal failure alone. The failure proves that the current mini_vpn
standard-TUIC path did not satisfy the project's frozen one-second continuity
SLI on that run. It does not prove that this SLI is a normal market contract
or that a custom resumable transport is proportionate to the product goal.

Before any architecture replacement, measure the same continuity evidence
against a mature TUIC client on the same Mac, Exit, Target, server, physical
network, offered-load profile, and observer. Separately measure one or more
commercial VPN products as a user-experience reference without treating their
different exits and protocols as a causal client comparison.

The existing formal artifact and its immutable failure status remain valid.
This stage changes the next decision, not the historical result.

## Why Calibration Is Required

Public product contracts and operating guidance emphasize availability,
reconnection, kill-switch behavior, and aggregate service rather than a
per-established-flow guarantee that every one-second receiver interval is
nonzero:

- ExpressVPN explicitly does not guarantee uninterrupted or error-free
  service: <https://www.expressvpn.com/tos>.
- Proton VPN describes always-on behavior as automatic reconnection after an
  interruption: <https://protonvpn.com/features>.
- Cloudflare documents that WARP tunnel restarts may cause brief connectivity
  loss: <https://developers.cloudflare.com/cloudflare-one/troubleshooting/warp-client/>.
- OpenVPN's official manual uses keepalive/restart behavior and persistent TUN
  state rather than a one-second per-flow continuity contract:
  <https://openvpn.net/community-docs/community-articles/openvpn-2-4-manual.html>.
- WireGuard documents optional persistent keepalives at a much coarser
  interval: <https://www.wireguard.com/quickstart/>.

These sources do not define an alternative numerical SLI. They establish only
that the current one-second rule must be calibrated empirically before it is
used to justify a large architecture replacement.

## Goal

Produce reproducible evidence that answers this question:

> Under the same controlled HK-Mac-to-`.33`-Exit-to-`.77`-Target path and the
> same offered load, is mini_vpn materially worse than a mature TUIC client in
> established TCP continuity?

The stage must minimize expensive long tests. It starts with an approximately
90-minute discriminator and escalates only when the result is genuinely
inconclusive.

## Non-Goals

- Do not accept M2 or reopen M3.
- Do not change the existing formal M2 artifact or call it a pass.
- Do not tune D16, MTU/PLPMTUD, pool, QUIC windows, chunk, Cubic, GSO,
  Endpoint pacing, retry, timeout, or workload constants.
- Do not implement established-stream migration or a custom server protocol.
- Do not claim that a commercial VPN's different exit isolates client quality.
- Do not convert a single competitor run into a universal market standard.
- Do not automate ownership of a third-party GUI or its credentials.

## Cohorts

### Lane A — same-path protocol/client discriminator

This lane is decisive for mini_vpn architecture attribution.

| Variable | Required value |
| --- | --- |
| Mac and physical network | Same HK Mac and one unchanged interface |
| TUIC Exit | `43.153.32.33:8443` |
| Target | `43.130.32.77:5201` |
| Server implementation/config | Same existing sing-box TUIC service |
| Target service | Same iperf3 receiver |
| Observer | Fresh matching `.33` Exit observer for each trial |
| Offered load | One frozen profile derived before either client trial |
| Clients | mini_vpn and one mature TUIC client, preferably Mihomo/Clash or sing-box |
| Other VPNs | Disabled |

The mature client must use the same TUIC credentials, SNI, CA trust, Exit,
and Target route. The operator enters these only in that client. The harness
records the client version/binary identity, exact server, route, and egress
evidence; it must not read, hash, copy, or bundle the third-party configuration
or its credentials.

### Lane B — commercial VPN experience reference

This lane answers what a user experiences, not which transport implementation
is better. Each product may use its own protocol and exit. Record product,
protocol if visible, exit region/IP, target route, and timestamps. Compare
visible stalls, reconnects, completed flows, DNS, egress, and aggregate rate.
Never use this lane alone to select a mini_vpn architecture.

## Frozen Workload Profile

Run one direct baseline before enabling either Lane-A VPN client. Derive a
single profile from that baseline and reuse its exact byte-per-second rates for
all matched trials. Do not derive a new offered rate per client because that
would change the pressure being compared.

One calibration cycle retains the established mixed shape:

1. TCP forward for 300 seconds;
2. TCP reverse for 300 seconds;
3. UDP reverse for 180 seconds;
4. six alternating short TCP forward/reverse phases of 10 seconds each;
5. one DNS and one public-egress probe.

The active traffic time is `300 + 300 + 180 + 6 * 10 = 840` seconds per
cycle. Six cycles therefore provide `5,040` seconds, or 84 minutes, of active
traffic plus bounded preflight/probe/cleanup time. This is the C0 discriminator
and replaces another speculative 25-hour run.

## Evidence Model

For every completed TCP result, record:

- direction, start/end time, requested rate, sent and received bytes;
- complete one-second sender and receiver interval counts;
- receiver-zero interval count and exact interval boundaries;
- maximum consecutive receiver-zero interval count;
- sender-zero interval count;
- maximum sender/receiver byte gap;
- connection error, reset, or incomplete-result status;
- aggregate receiver rate and rate/profile ratio.

For every UDP result, record loss percentage, lost/received datagrams,
out-of-order datagrams, jitter, and completion status. For the full trial,
record DNS success, public egress identity, route snapshots, client process
identity, physical interface, and paired Exit observer identity/checksum.

The Exit observer remains the attribution authority for TUIC ingress,
Exit-to-Target supply, target ACK/retransmit/send-queue behavior, and capture
drops. A Mac-only result cannot distinguish a client supply pause from a
Target-side event.

## Quality Events Versus Invalid Evidence

A complete one-second receiver-zero interval, UDP loss above the former formal
limit, or a completed low-rate phase is a **quality event**. The calibration
runner records it and continues so the trial measures frequency and clustering.
It must not stop at the first event.

The trial stops and is classified invalid when evidence cannot support a
comparison, including:

- source/profile mismatch or a dirty effective source/worktree;
- target/exit route ownership does not match the selected client;
- another VPN or proxy owns the route;
- the harness cannot start and bind a fresh matching Exit observer, or that
  observer becomes unhealthy or loses capture data;
- target/iperf service is unavailable before traffic;
- physical interface changes, the Mac sleeps, or the VPS restarts;
- result JSON is malformed or a scheduled phase is not represented;
- required cleanup/evidence finalization fails.

The external client remains operator-owned. The runner must never stop the GUI,
rewrite its config, or remove routes/DNS it did not create.

## Cost-Controlled Decision Tree

### C0 — mature-client discriminator

Run six cycles with the mature TUIC client first. Compare against the existing
`cdbfe36` mini_vpn failure only as historical context, because it was not a
same-window matched trial.

- If the mature client also records one or more complete receiver-zero
  intervals with valid paired evidence, the strict zero-tolerance one-second
  gate is not a useful client discriminator on this path. Do not select a
  custom mini_vpn architecture. Design the next product SLI from the observed
  gap-frequency and consecutive-gap distribution.
- If the mature client completes C0 with zero receiver-zero intervals, proceed
  to C1. Do not declare mini_vpn worse from unmatched days alone.
- If C0 evidence is invalid, repair only the harness/environment cause and
  repeat C0; do not modify mini_vpn production code.

### C1 — matched alternating discriminator

Run matched 90-minute trials on the same day with one frozen profile and fresh
observers. Prefer `mature -> mini_vpn -> mature -> mini_vpn`, stopping early
only when the outcome is already decisive.

| Valid result | Decision |
| --- | --- |
| Mature has quality events and mini_vpn is comparable | Treat 1-second zero as diagnostic; calibrate a market/product SLI |
| Mature passes twice and mini_vpn fails twice | Select a mini_vpn-specific architecture investigation |
| Both pass | Inconclusive; extend matched duration before changing architecture |
| Both fail, but mini_vpn has materially worse frequency or runs | Investigate the measured differential, not zero tolerance itself |
| Path/VPS invalidates either side | No comparison; repeat only the invalid pair |

"Materially worse" must be stated from the measured sample before setting a
release threshold. Do not invent a percentage in advance and then tune toward
it.

### C2 — architecture selection

Only a valid matched differential may reopen an architecture design. The new
spec must name the exact missing capability and deterministic seam. If the
differential is established-stream continuity under ACK-progressing native
loss recovery, a custom resumable/path-diverse server protocol becomes one
candidate, not an automatic choice. If no differential exists, revise M2 to a
product-calibrated continuity/recovery SLI and retain standard TUIC.

## Capacity And Reachability

This stage is a measurement stage, not a throughput optimization. It retains
the existing Endpoint capacity baseline above 200 Mbit/s and uses offered rates
below the current direct path. It changes no local hot path. The evidence path
is:

```text
iperf sender -> selected VPN client -> .33 TUIC Exit -> .77 iperf receiver
                         |                    |
                  macOS interval JSON   paired packet/counter observer
```

The mature-client lane is sufficient to test whether the one-second event is
unique to mini_vpn. It is not sufficient to prove how the mature client avoids
an event; that requires its own traces or source analysis only after C1 shows a
real differential.

## Acceptance For This Stage

The calibration harness is ready for HITL use only when:

1. deterministic self-tests cover zero-run counting, partial-tail exclusion,
   malformed results, quality-event continuation, and invalid-evidence stop;
2. preflight proves one frozen profile and selected-client route ownership,
   then the harness starts and binds its own fresh matching observer before
   traffic;
3. signal/error paths finalize bounded evidence without taking ownership of
   the external VPN client;
4. shell syntax, self-test, secret scan, diff checks, and code review pass;
5. the runbook gives separate, unambiguous mature-TUIC and mini_vpn commands.

The calibration stage itself completes only after valid C0/C1 evidence yields
one of the decision-tree outcomes. Formal M2 and M3 remain blocked meanwhile.
