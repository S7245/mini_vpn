# Knife15 Exit-to-Target Forwarding Observability Local Results

Date: 2026-08-06

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS — ONE SHORT QUALIFICATION
REQUIRED; FORMAL M2 BLOCKED**

Implementation: `4d02355`

Architecture:
`docs/tech/2026-08-06-knife15-exit-target-forwarding-observability-architecture-spec.md`

Failure evidence:
`docs/tech/2026-08-06-knife15-m2-post-quic-exit-target-forwarding-results.md`

## Decision

Exact-source `727f00b` Mac evidence passed baseline/direct, start/smoke, every
M2 preflight, the first three long mixed phases, and cleanup. Its first
10-second short forward then lost two complete Target receiver intervals after
the client had admitted bytes and the Exit had ACKed QUIC data. The one-turn
startup service was reached and consumed, so it is retained as bounded code
but rejected as sufficient Target-delivery service.

One exact-order Exit-to-Target control and sixty consecutive fresh short
connections all passed. They reject a persistent or readily recurring bare
Exit-to-Target limitation but cannot classify the historical copy boundary.
Formal M2 must not consume another 25 hours without matching Exit evidence.

## Qualification implementation

The public `m2-qualification` action reuses the formal M2 source, immutable
baseline/direct, IPv6, full-tunnel, DNS, real-client, controlled-ownership,
process, network-control, Endpoint, D16, and quality gates. It runs exactly:

1. 300-second forward TCP;
2. 300-second reverse TCP;
3. 180-second reverse UDP;
4. 10-second short forward TCP;
5. one DNS and one post-cycle real-client probe.

It stops on the same command, receiver-zero, TCP-gap, UDP-loss, route/DNS,
process, or conservation failures. Success is written only as
`PASS_NON_ACCEPTANCE`; the formal `m2.status` and `m2-pre-stop-verdict.txt`
remain absent, and the summary reports `formal_m2_acceptance=NOT_RUN`.
`status` and `stop` remain mandatory.

## Exit observer implementation

`scripts/knife15-exit-target-observer.sh` is a separate operations adapter.
It defaults to Exit `.33` and Target `.77:5201`, but keeps those values out of
the product data path. The observer:

- captures only 96-byte snapshots for the exact Target TCP port;
- rotates 17 files capped at 20,000,000 bytes each, for 340,000,000 bytes
  (about 324 MiB) maximum packet evidence;
- samples matching Linux TCP_INFO through `ss -tin` every 250ms;
- runs under `nohup`, `setsid`, and a hard 900–7200 second timeout;
- verifies exact process identities before signaling and uses bounded forced
  cleanup only after graceful stop fails;
- records versions, elapsed time, capture file/byte/drop status, metadata,
  relative SHA-256 manifest, text secret scan, and archive SHA-256;
- refuses stacked starts, ambiguous PIDs, live bundling, and secret-shaped
  text evidence.

No TUIC UDP, credentials, private keys, or full TCP payload capture is
included. The exact iperf-only filter and 96-byte snap length keep any retained
payload prefix bounded.

## TDD and live observer smoke

The compressed qualification test proves four exact phases, one cycle, DNS,
absence of formal timeline windows, receiver-zero immediate stop,
`PASS_NON_ACCEPTANCE`, and absence of a formal verdict. Existing focused
real-client tests cover the shared probe and the public action enables it for
both preflight and post-cycle evidence.

The observer self-test covers the exact filter, snaplen, 340,000,000-byte
ring, two-hour timeout authority, version metadata, live status, elapsed and
file evidence, start/bundle refusal, zero-drop parsing, secret rejection,
ambiguous PID refusal, graceful stop, forced stop, checksum finalization, and
state removal.

The first real Exit smoke exposed a tcpdump rotation-ownership error. Bundle
`/tmp/mini_vpn_knife15_exit_target_observer_20260806_062709.tar.gz`
(SHA-256 `a9cf420660cc6ba0c0378879134e5fa0037337fa639a7905ec3b155c78ef20ac`)
was stopped and finalized. The observer now explicitly retains root file
ownership with `tcpdump -Z root`; its self-test locks that argument.

The final real Exit smoke passed start/live status/stop/inactive status/bundle:

```text
run_dir=/tmp/mini_vpn_knife15_exit_target_observer_20260806_065147
elapsed_secs=17
tcpdump_live=0
sampler_live=0
capture_files=1
capture_bytes=24
capture_kernel_drops=0
sha256=b119ec7efae54f21bd96cda61b86856aa8956aa7fd9ac88aee8bd3e9f781f985
```

No observer process or state remained on the Exit after finalization.

## Local gates

```text
qualification deterministic self-test: PASS
Exit observer deterministic self-test: PASS
Knife15 wrapper self-test:          PASS
Knife14 shell suites:               PASS
root library:                       673 passed, 3 ignored
main binary:                          2 passed
concurrency integration:             10 passed, 4 ignored
release build:                        PASS
established all-target Clippy:        PASS (existing warnings only)
vendored Quinn:                       38 passed, 3 ignored; doc 1 passed
vendored quinn-proto:                310 passed; docs 3 passed
root docs:                             0 failures
Endpoint 32 MiB:                     237.737 Mbit/s; 61,440/0/0B
shell syntax / fmt / diff:            PASS
```

The first standalone Quinn invocation omitted the explicit local proto patch
and selected the registry crate; it was rejected and rerun with the path
patch. The first capacity invocation used an unqualified `--exact` filter and
ran zero tests; it was rejected and rerun with one exact unique match. Neither
failure changed product code or acceptance.

## Review

Review covered formal-M2 behavior preservation, evidence namespace isolation,
signal/interrupt status, route/DNS cleanup, false formal acceptance, phase
order/counts, data-quality early stop, real-client evidence routing, command
identity, PID reuse, process groups, automatic timeout, disk bounds, capture
scope, version/drop evidence, manifest portability, secret handling, and VPS
cleanup.

Review repaired one capacity-unit mismatch before acceptance: tcpdump `-C 20`
means 20,000,000 bytes rather than 20 MiB, so the ring is 17 files and really
exceeds 320 MiB. There are no unresolved P0/P1 findings.

## Next discriminator

Start the reviewed observer on `.33`, then take exactly one fresh Mac:

```text
m2-ipv6-check -> baseline -> direct-discriminator
-> start -> smoke -> m2-qualification -> status -> stop
```

Stop and bundle the Exit observer after the Mac result. A qualification PASS
is diagnostic only and does not reopen formal M2. Matching packet sequence,
ACK, retransmission/RTO, unacked, and client QUIC evidence must first select
the Exit physical/kernel, mature-server application-copy, client-to-Exit QUIC,
or observer-mismatch branch. M3 remains blocked.
