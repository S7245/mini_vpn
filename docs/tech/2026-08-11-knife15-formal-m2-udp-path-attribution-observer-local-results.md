# Knife15 Formal M2 UDP Path Attribution Observer — Local Results

Date: 2026-08-11

Status: **PASS locally — production data plane unchanged; formal M2 remains
failed and M3 remains blocked**

## Trigger Artifact

The exact-source `0e44a939` formal artifact is
`/tmp/mini_vpn_knife15_macos_20260811_074703.tar.gz`, SHA-256
`a0c268c1ea8d6c45fa82c36d21ad2f739dfcbc3e09189115b2f78a8d232a6d96`.
It passed baseline/direct, smoke, every preflight, fourteen complete formal
cycles, 114 TCP results, fourteen UDP results, DNS/real-client checks, and
cleanup. Formal M2 stopped in cycle 15 `udp-reverse`:

- Target sent `562,748` datagrams and `652,787,680B` at
  `28.985075 Mbit/s`;
- the Mac received `626,516,000B` and reported `22,139` lost datagrams,
  `3.934088%`, above the frozen `3%` limit;
- every earlier UDP phase was between `1.1968%` and `2.3812%`, with a
  `1.533251%` weighted loss rate;
- TCP Target receiver-zero intervals remained zero.

The loss was bursty. During the main burst the physical `en0` receive rate
fell below the offered rate while gateway loss, interface errors, local UDP
drops/backpressure, TUN pump full waits, D16 ownership, Endpoint conservation,
and socket would-block remained zero. `.33` sing-box and `.77` iperf3 stayed
active with zero restarts. A later exact `.77 -> .33` direct reverse-UDP
discriminator at the same `1160B / 28,985,203bps / 180s` delivered all
`562,214` datagrams with `0%` loss; its JSON SHA-256 is
`c92e698361d174cbeeb90e7a5002fae254cdef487ddbf3d1bb2aa1a30f69e1b6`.

This selects a transient external UDP/QUIC path event over a persistent
Target, Exit, or local mini_vpn capacity failure. The old observer was not
running and could not prove whether the historical loss occurred before or
after `.33`; therefore the artifact is a real formal failure, not a product
PASS and not a basis for changing the SLI or frozen values.

## Observer Contract

The revised observer is a test/evidence boundary, not a data-plane change.

1. Formal M2 requires an exact v2 observer on the recorded Exit, Target,
   iperf port, and TUIC port, with all observer processes healthy.
2. Its `93,600s` lifetime must have no more than `900s` elapsed when formal
   M2 starts, leaving more than the frozen 24-hour schedule budget.
3. A dedicated nftables table contains counter-only input/output rules for
   Target ingress/egress and encrypted TUIC ingress/egress. The observer
   records cumulative packets and bytes once per second.
4. A bounded `17 x 20,000,000B`, `96B`-snaplen pcap ring retains recent exact
   packets without storing an unbounded 26-hour capture.
5. The table name and ownership bit are recorded before use. Cleanup refuses
   an ambiguous table, and bundling refuses to bypass an owned table even
   after process hard timeout.
6. Formal M2 arms observer ownership after all non-mutating gates. Workload
   success, workload failure, signal interruption, and unexpected shell exit
   all freeze the ring before bundling. The Mac evidence records the remote
   bundle path and SHA-256.

The observer does not capture credentials. TUIC payload remains encrypted;
only the dedicated Target service and TUIC server port are in scope.

## TDD Evidence

- RED rejected the previous two-hour/TCP-only filter; GREEN accepts the
  `93,600s` bounded Target plus TUIC filter.
- RED had no per-second packet/byte evidence; GREEN requires all four named
  counters, three observer processes, and the owned nftables table.
- RED allowed a hard-timed-out observer to bundle while its nftables table
  remained installed; GREEN requires `freeze` or `stop` cleanup first.
- RED accepted an observer with insufficient remaining lifetime; GREEN
  rejects elapsed time above `900s`.
- RED had no formal preflight/finalization ownership; GREEN requires exact
  status and records `status -> freeze -> bundle`, including the checksum.
- RED could point formal M2 at a different SSH host; GREEN binds the SSH host
  IP to the recorded Exit IP.

## Real Exit Lifecycle Probe

On `.33`, a short isolated observer transaction passed:

- all three processes and the v2 aggregate health predicate were live;
- a `.77 -> .33` reverse-UDP probe advanced Target ingress to
  `230 packets / 260,103B`;
- freeze removed the dedicated nftables table and state;
- sing-box remained active with zero restarts;
- the evidence bundle is
  `/tmp/mini_vpn_knife15_exit_target_observer_20260811_150831.tar.gz`,
  SHA-256
  `fff3011e64645cd0b9c6c362ae9f960a5e11025cac49d00e84d3765a3957585c`.

## Local Gates And Review

- Root library: `705 passed; 3 ignored`.
- Main binary: `2 passed`.
- Integration harness: `10 passed; 4 ignored`.
- Release build, established Clippy, `cargo fmt --check`, shell syntax,
  observer self-test, Mac runner self-test, and `git diff --check`: PASS.
- The runner test covers exact observer status, wrong Exit, stale lifetime,
  explicit `freeze -> bundle`, unexpected EXIT fallback, checksum evidence,
  and preservation of the primary exit status.
- The observer test covers process/PID ownership, one nftables snapshot read
  per one-second sample, bounded capture, ambiguous PID refusal, hard-timeout
  cleanup ownership, secret scan, and immutable bundle checksum.
- Code review found and repaired stale-lifetime admission, bundle-before-table
  cleanup, four-netlink-reads-per-second overhead, signal/finalization ordering,
  wrong-Exit admission, and observer-script override. No unresolved P0/P1
  remains in the changed scripts/docs scope.

## Frozen Scope And Decision

No Rust production code, D16, MTU, pool, QUIC windows, chunk, Cubic, GSO,
Endpoint pacing, self-wake, workload rate/duration, UDP payload, or SLI was
changed. This stage improves attribution and prevents another unpaired
25-hour failure; it does not claim to eliminate genuine WAN loss.

Do not repeat qualification and do not tune constants. After the reviewed
commit is pulled and release is rebuilt, start one fresh v2 observer and take
exactly one formal
`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2 ->
status -> stop`. Formal M2 and M3 remain blocked until that transaction and
cleanup pass.
