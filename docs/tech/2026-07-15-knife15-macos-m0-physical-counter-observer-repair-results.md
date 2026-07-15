# Knife15 macOS M0 Physical-Counter Observer Repair Results

Date: 2026-07-15

Status: **REAL RECEIVER INTERRUPTION REPEATED; INTERNAL REGRESSION REJECTED;
PHYSICAL-COUNTER FALSE PASS REPAIRED LOCALLY; FRESH MACOS VALIDATION PENDING**

## Goal And Evidence

Review the next user-executed formal M0 and its independent rearm without
blaming operator procedure or tuning any frozen Knife14/Knife15 data-plane or
workload setting. The reviewed artifacts are:

- direct baseline:
  `/tmp/mini_vpn_knife15_macos_baseline_20260715_072138`;
- M0 bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_072254.tar.gz`, SHA-256
  `0127e3af2bd4fa203f2c85887c8f7d6bd462f46a016b6e49a435fb99274b0551`;
- stop/re-create/smoke/stop bundle:
  `/tmp/mini_vpn_knife15_macos_20260715_074823.tar.gz`, SHA-256
  `6d271b0c49c537d15f46295641f3c87121f059a9b86c5065c3ddfeb5ad6496cf`.

Both archive hashes match the user-provided values. Their manifests identify
exact source `b0fcb767528bf39b5101d5a8e4644727a7e19303`, binary SHA-256
`3fda9f9d16635388e6869520b0270ea3e4f7f9d70a742a27f54aad43aad70306`,
and runner SHA-256
`cb802bf4e5f9e89fc7410c901c02f6c023cfc23bbbba5d9632e2c6afba25acb7`.
Target, Exit, and DNS routes were physical before start; the executed
start/smoke/M0/status/snapshot/stop and independent rearm sequence was valid.

## Baseline And Formal Profile

The direct TCP receiver baseline was:

- forward `72,744,960B` at `28.851 Mbit/s`, with `20/20` nonzero intervals;
- reverse `130,416,640B` at `52.162 Mbit/s`, with `20/20` nonzero intervals.

The workload profile recorded the exact baseline file hashes and correctly
derived the frozen relative rates:

- sustained forward `14,425,495 bit/s`;
- sustained reverse and reverse UDP `26,080,955 bit/s`;
- short forward/reverse `23,080,792/41,729,528 bit/s`;
- UDP application payload `1160B`.

The forward direct baseline already had wide interval variation and `1,633`
TCP retransmits. That is path-quality context, not permission to alter the M0
relative-load policy.

## M0 Result

The first forward phase ran its full command duration. The client sent
`216,006,656B` over `300.005s`, while the Target receiver accepted
`208,142,336B` over `300.175s`, or `5.547 Mbit/s`. The sender/receiver gap was
`7,864,320B`. Actual receiver delivery was only about `38.5%` of the requested
M0 offer.

The structured Target receiver evidence contained five complete zero-byte
seconds:

- `211.001033-212.001021s`;
- `220.001033-221.001025s`;
- `276.001029-277.001022s`;
- `278.001020-279.001028s`;
- `280.001026-281.001021s`.

There was also one `0.174s` zero-byte command-tail row. Excluding that short
tail does not change the outcome. The direction-aware receiver SLI correctly
failed M0 at `07:28:43Z` with `reason=receiver_zero_interval`. The user did
not request stop until `07:47:07Z`, so this was neither premature stop nor the
earlier sender-versus-receiver evidence defect.

The local sender had `148` zero intervals. Those remain useful backpressure
diagnostics, but the forward acceptance decision was made from the Target
receiver as designed.

## Internal And QUIC Discriminators

Internal architecture and resource signals stayed clean:

- endpoint conservation was at most `61,440B` and ended
  `61,414/0/0B` available/live/outstanding;
- endpoint would-block and service-delay counters were zero, abandoned bytes
  were zero, and outstanding high-water was only `2,839B`;
- utun input/output errors were zero;
- ingress pump high-water was `67/500`, with zero full waits, read errors, or
  closure, and backlog pause/resume was `8/8`;
- no TUN flush failure, terminal pending reap, stranded D16 queue ownership,
  reconnect, migration, FD/thread growth, or log compaction occurred;
- final D16 queued/leased/reserved ownership was zero.

macOS does not expose the accepted Linux qdisc-drop signal through this
runner. Its internal diagnostic therefore remains `tx_dropped=unknown`; do
not rewrite that as a proved zero.

The bulk QUIC connection instead showed the positive failure discriminator.
Across the receiver interruption region, cwnd contracted from a prior
`793,435B` range through `31,628B` and `32,522B`; lost bytes and congestion
events continued to rise. By phase close it had recovered to `266,998B` cwnd
after reaching `1,919,156` lost bytes and `492` congestion events. The D16
writer recorded a `10,050,820us` maximum wait. This supports:

`flow-specific QUIC/path pressure -> cwnd contraction -> writer backpressure
-> Target receiver interruption -> recovery and clean drain`.

The two logical `Stopped(0)` close-tail events, one from smoke and one after
the complete M0 command, appeared in three log forms each and explain the
summary count of six remote-write matches. They did not cause the mid-command
receiver-zero seconds and ended with zero ownership.

## Network-Control Result And Observer Root Cause

Exit/gateway control collection itself was complete: all `52` process,
network, and interface timestamps corresponded. During the receiver-zero
region, the nearest Exit ICMP samples had zero loss and roughly `169-175ms`
average RTT; gateway samples also had zero loss and roughly `3-11ms` average
RTT. This rejects a broad simultaneous connectivity outage, but three pings
every 30 seconds cannot exclude one-second transients or isolate the local
Wi-Fi, ISP UDP treatment, Exit service, or Exit-to-Target segment.

The physical-interface evidence was invalid despite the old summary saying
`network_control_evidence: PASS`. BSD `netstat -ibn -I` emits a link-layer
Address value for a physical interface but not for the utun fixture used by
the original test. `interface_control_fields_from_text` always read fields
`$4..$10`, so on the physical row it:

- treated the link address as `physical_ipkts`;
- shifted every packet/error/byte counter one column;
- omitted collisions;
- computed rates from the real error counters, which were zero;
- let the old completeness check count the malformed rows as valid.

The archived rows can be manually decoded and show zero physical interface
input/output errors. Their traffic deltas and reconstructed rates correlate
with the tunnel slowdown and recovery. They are diagnostic salvage only: the
published network-v2 summary and native rates are invalid and must fail closed.

## Rearm Result

The independent rearm correctly performed fresh create, TCP/DNS smoke, and
stop cleanup. Reverse receiver traffic was `48.810 Mbit/s` with `20/20`
nonzero intervals; fake-IP DNS returned `198.18.0.2`. Endpoint conservation
was at most `61,408B` and ended with zero live/outstanding ownership. Pump
high-water was `21/500`, with zero waits/read/flush failures. Final Target and
Exit routes returned to the physical interface, the process was dead, and the
new utun no longer existed.

One forward `Stopped(0)` close tail and one reverse terminal close path retain
the generic quality `REVIEW`, but all D16 and endpoint ownership closed. The
rearm lifecycle gate passes. Its five physical-interface rows have the same
observer defect, so they do not qualify network-control acceptance.

## TDD Repair

The local runner repair changes only the observer:

1. a RED fixture reproduces a physical `<Link#>` row with a link Address and
   proves the old parser shifted counters;
2. the parser now detects the optional Address column, requires numeric MTU
   and all seven counters, and emits the same eight-field schema for utun and
   physical interfaces;
3. a second RED fixture preserves 27 CSV columns while placing a link Address
   in the counter position, proving that the old envelope still returned a
   false physical sample;
4. the envelope now counts errors, bytes, and rates only when every physical
   MTU/counter field is numeric;
5. the composed collector fixture now includes a realistic link Address.

Repair commit: `524139b` (`fix(macos): reject shifted physical counters`).

Replaying the two published `network.csv` files through the repaired envelope
returns zero valid physical samples. A formal complete M0 summary therefore
cannot PASS those malformed controls.

No Rust data plane, duration, relative rate, MTU, payload, pool, QUIC window,
chunk, Cubic, GSO, queue, driver, pacing, or wake setting changed.

## Local Gates And Decision

- three focused RED/GREEN parser, fail-closed, and invalid-rate cycles: PASS;
- Knife15 internal and external shell self-tests and shell syntax: PASS;
- root all-target tests: library `635 passed`, `3 ignored`; main `2 passed`;
- release build: PASS with only established vendored smoltcp warnings;
- Knife14 low-RTT, US-client-suite, and sing-box-control self-tests: PASS;
- root formatting and diff checks: PASS.
- focused code review: PASS, no unresolved P0/P1; a P2 freshness-marker
  negative test was added and passes.

M0 remains failed and blocks M1. Before spending another two-hour run, rebuild
the repaired exact source on the dedicated Mac and run one fresh
start/smoke/stop validation. Its `network.csv` must contain numeric physical
packet/error/byte fields, nonzero traffic deltas/rates where traffic occurred,
and no false physical errors. Then take a fresh direct baseline and run formal
M0 plus independent rearm. Do not reuse either reviewed bundle as acceptance,
relax the receiver SLI, or tune frozen constants.
