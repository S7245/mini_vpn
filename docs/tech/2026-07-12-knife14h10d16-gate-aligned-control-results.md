# Knife14 H10d16 Gate-Aligned Mature Control Results

Date: 2026-07-12
Source: `1b830044504d4bdb468ce03bef00a901a2e76dfe`

## Verdict

The Gate-process correction is implemented and versioned, but the first clean
Gate-aligned mature control did not authorize the scoped `bdaa19c` run. The
control used the exact product-side transport profile, `Cubic + MTU1200`, and
reached only `3.041 Mbit/s` receiver through the independent `.111` Exit.

The direct receiver was `217.220 Mbit/s`, target-only routing and MTU were
correct, both UDP sockets had `16 MiB` buffers with drop `0`, and neither
mature-client nor Exit logs showed an active failure before the expected timed
remote cancel. Fourteen of twenty one-second intervals were exactly zero.

This falsifies the hypothesis that the repeated independent-Exit false
negative was caused by the historical control's `BBR + MTU1500` profile. The
clean `bdaa19c` scoped run, composite Gate A, and Gate B were not run.

## Versioned Gate-process correction

Commit `1b83004` changes only
`scripts/knife14h10d16-singbox-control.sh`:

- preserves a named `historical-mtu1500-bbr-reverse-p1` diagnostic profile;
- adds a named `gate-aligned-mtu1200-cubic-reverse-p1` authorization profile;
- makes profile selection explicit through `--profile` and rejects unknown
  profiles before requiring root or credentials;
- renders congestion control from the fixed selected profile rather than a
  hard-coded BBR value;
- reports exact profile, MTU, congestion control, and Gate A role;
- returns `DIAGNOSTIC_PASS` for an above-floor historical run, so it cannot be
  confused with Gate A authorization;
- authorizes Gate A only when Gate-aligned throughput is strictly greater than
  `150 Mbit/s` and both socket gates pass;
- self-tests both exact configuration shapes, target-only routing, profile
  labels, authorization semantics, and the strict floor boundary.

Local and clean `.27` validation passed:

- control self-test;
- versioned tunnel-suite self-test;
- Bash syntax and diff checks;
- `relay_read_credit_update_keeps_inflight_remote_read_armed` (`1/1`);
- `h10d16_gate_a_test_profile_matches_approved_safe1200_runtime` (`1/1`);
- clean release build from a verified git bundle.

## Independent Exit preflight

The `.111` Exit used the same sing-box `1.13.14` binary as `.33`, with SHA-256
`4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`.
Configuration, certificate, and private key were consumed through separate
mode-0600 FIFOs and removed after startup; none were persisted in an artifact.

Before the control:

- `.27 -> .111:8443` UDP host-arrival capture passed with kernel drop `0`;
- `.111:8443` was listening with zero startup error lines;
- `.111` socket maxima were `16 MiB` and defaults were `1 MiB`;
- `.77` iperf3 was active;
- a 60-minute cleanup watchdog was active.

## Gate-aligned result

Fixed shape:

- one `20s` reverse P1;
- mature sing-box client `Cubic`;
- TUN MTU `1200`;
- target-only route to `.77`;
- Exit route to `.111` remained on `eth0`;
- receiver floor strictly greater than `150 Mbit/s`.

Observed:

- direct sender/receiver: `222.013/217.220 Mbit/s`;
- TUIC sender/receiver: `3.774/3.041 Mbit/s`;
- client UDP socket: `rb=16777216 tb=16777216 drop=0`;
- Exit UDP socket: `rb=16777216 tb=16777216 drop=0`;
- zero-rate receiver intervals: `14/20`;
- maximum one-second receiver interval: `37.749 Mbit/s`;
- client error lines: `0`;
- Exit error: only the expected timed stream cancellation by the remote.

The earlier historical `.111` control had `4.928 Mbit/s` receiver, `13/20`
zero-rate intervals, and a `39.853 Mbit/s` maximum interval. The nearly
identical burst/idle shape across the two fixed profiles rejects client BBR and
MTU1500 as the active root.

Local artifact:

`/tmp/mini_vpn_h10d16_exit111_gate_aligned_1b83004.tar.gz`

SHA-256:

`00bf075f50171e9a26cd140a8ecadf5deed9d06fdcd5472b2ace1efea5d52530`

## Code and route review

The failing mature control does not execute mini_vpn, the D16 reader, the
leased byte queue, the actor, smoltcp, or mini_vpn TUN egress. It therefore
cannot justify another D16 ownership, queue, actor, EOF, MTU, Quinn-window,
chunk-size, or self-wake change.

The versioned control correctly reached the intended profile and routing
shape. Its direct path and socket evidence reject target TCP capacity, routing
recursion, small UDP buffers, and kernel socket drops. The repeated multi-
second zero-rate intervals remain upstream of any mini_vpn data-plane code.

## Revised next discriminator

Do not repeat either control from `.27` against an unchanged Exit and do not
run scoped mini_vpn below the mature floor. The next useful same-window test
changes the client host while holding the `.111` Exit, `.77` target, sing-box
version, Gate-aligned profile, and floor constant:

1. run one FIFO-only Gate-aligned mature control from `.33` through `.111` to
   `.77`, without stopping the existing `.33` TUIC service;
2. if it exceeds `150 Mbit/s` with both socket drops zero while `.27` remains
   burst/idle, classify `.27` or the `.27 <-> .111` UDP/QUIC path as the
   external blocker; decide explicitly whether Gate A may move to that client
   host or whether `.27` must be replaced;
3. if the second client is also burst/idle, keep Gate A frozen and measure a
   same-port raw UDP/QUIC path discriminator before changing product code;
4. only after a same-window Gate-aligned mature pass, run clean `bdaa19c`
   scoped once, review its invariants, and then run composite Gate A.

## Cleanup

- `.111` temporary TUIC process and watchdog stopped;
- `.111` UDP `8443` listener removed;
- `.111` socket maxima/defaults restored to `212992`;
- `.111` binary, log, runtime directory, and FIFO paths removed;
- `.27` control TUN, temporary clone, bundle, binary, and remote artifact
  removed after the verified artifact was copied locally;
- no credential, private key, environment value, or sudo password was written
  into the repository or result artifact.
