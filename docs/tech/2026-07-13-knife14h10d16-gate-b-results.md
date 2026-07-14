# Knife14 H10d16 Gate B Results

Date: 2026-07-13
Source: `a54fb171ad57dc48902a79f9d31bd08d2ac41802`
Verdict: **Gate B PASS; product regression gate remains pending**

## Scope And Frozen Profile

This run executed implementation-plan Task 12 steps 1-3 after composite Gate A
passed. It used one corrected same-window Gate-aligned sing-box control, exactly
three valid mini_vpn `20s` reverse-first P1 measurements, their receiver
median, and exactly one fixed `64 MiB` clean-close measurement. No macOS TUN
test ran.

The measured mini_vpn binary came from a clean detached clone at exact source
`a54fb17`:

```text
binary_sha256=34191751ff80f1408476f874fb66e74757384c061616380a26b54f0659ff5dff
suite_sha256=2091073867a67ba268e2be545d44d963f59ea01766ac693efd1eeb040903c806
probe_sha256=c4038c38ab8b1ee52d710d02ce2e6c9125b020247cee5faf10d7018489e9dfb0
```

The frozen profile remained MTU `1200`, Cubic, pool `2`, zero ordered/native
chunk experiments, the existing QUIC windows, D16 byte ownership enabled, and
D3 self-wake disabled. D16 ownership, actor cadence, DrainOnly, EOF, pool,
QUIC windows, chunk size, and VPS tuning were not changed.

The Exit was the same temporary Shoes `v0.2.7` / Quinn `0.11.9` service on
`.111:8443` throughout the valid control, all three timed measurements, and
A-clean. Its in-memory FIFO-only config explicitly asserted:

```text
alpn_protocols=["h3"]
num_endpoints=1
zero_rtt_handshake=false
workers=2
```

The Shoes binary SHA256 was
`160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147`.
Credentials, TLS keys, and UUIDs were never persisted in the repository or
retained artifacts.

## Same-Window Gate-Aligned Control

The corrected sing-box `1.13.14` control used safe MTU1200, Cubic, P1 reverse,
the same `.111` Exit and `.77` target, and target-only routing. The binary SHA256
was `4ea794fddcb2ad84532adeab979a9b0d7b2052822bb3439dfb321c33c941da19`.

```text
direct reverse sender/receiver: 224.776 / 218.304 Mbit/s
sing-box sender/receiver:       144.519 / 143.228 Mbit/s
client UDP rb/tb/drop:          16777216 / 16777216 / 0
server UDP rb/tb/drop:          17250000 / 17250000 / 0
route assertion:                target-only PASS
cleanup:                        complete
```

The Gate-A-oriented control runner reported `INCAPABLE` because its hard floor
is `150 Mbit/s`. Task 12 Gate B has a distinct rule: when the valid same-window
control is below `170 Mbit/s`, parity may pass when the mini_vpn median is at
least `90%` of the control and every mini_vpn run still exceeds `150 Mbit/s`.
The relative threshold was therefore `128.9052 Mbit/s`. The mini_vpn results
also passed the stricter absolute `170 Mbit/s` median rule, so the final verdict
does not depend on relaxing the absolute median target.

## Three Timed mini_vpn Repeats

Exactly three valid mini_vpn measurement windows ran. Several earlier command
attempts for repeat 3 exited in preflight before mini_vpn or iperf started;
they are configuration/orchestration failures, not additional samples.

| Run | Sender | Receiver | Overall interval avg | Tail avg / min | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 193 | 192 | 192.000 | 187.500 / 150 | PASS |
| 2 | 190 | 188 | 188.150 | 189.833 / 155 | PASS |
| 3 | 195 | 191 | 191.500 | 188.000 / 184 | PASS |

All rates are Mbit/s. Every run had `20/20` nonzero one-second intervals and
`tail_collapse=0`. Receiver values sort to `188, 191, 192`, so:

```text
median receiver = 191 Mbit/s
absolute threshold = 170 Mbit/s
relative threshold = 0.9 * 143.228 = 128.9052 Mbit/s
per-run floor = >150 Mbit/s
```

All three runs had:

- TUN RX/TX drop delta `0/0`;
- actor bypass, send-slice zero/errors, and flush failures `0`;
- pressure/drop debt and terminal pending reap `0`;
- QUIC loss, congestion, and data/stream blocking deltas `0`;
- pool opens on connections `0,1` with no reconnect;
- attribution `no_pressure_signal`.

The timed data sockets remained active/`CloseWait` when the suite process was
stopped, so those data handles emitted neither a terminal cause nor a natural
EOF in the final isolated window. This is not an unclassified terminal: no
terminal/abort event occurred, and all observed terminal counters were zero.
The separate fixed-byte proof below is the authoritative natural EOF and
zero-tail result.

Repeat 3 used a temporary operational runner whose only difference from the
versioned suite was replacing interactive `sudo -v` with `sudo -n true`, because
the host has NOPASSWD command authorization but mixed sudo entries make
credential validation prompt. Its SHA256 was
`3585dc772540e9ac250f13635d9fc939c6046a15080bf81c6782c4dc1f77c216`.
The binary, probe, measurement logic, and frozen profile were unchanged.

## Fixed 64 MiB A-Clean

After the median passed, exactly one fixed-byte reverse measurement ran using
the same binary/profile/Exit:

```text
command shape: iperf3 -n 64M -P 1 -R
sender/receiver: 64.0 / 64.0 MiB
sender/receiver rate: 179 / 179 Mbit/s
data rx_bytes: 67108864
```

The data handle closed with exact clean accounting:

```text
remote EOF pending/inflight: 0 / 0
terminal_reason: clean_queue_lifecycle
queue queued/leased/reserved: 0 / 0 / 0
close pending/egress: 0 / 0
terminal pending reap: 0
actor bypass: 0
send_slice zero/errors: 0 / 0
permit terminal drop bytes/events: 0 / 0
TUN RX/TX drops: 0 / 0
QUIC loss/congestion/blocking deltas: 0
```

The TUN carried `69,626,166` RX bytes and `1,130,870` TX bytes with zero kernel
errors or drops, and the tunnel returned to `TCP relay active=0`. This proves
natural remote EOF, full D16 ownership release, empty smoltcp/TUN close tail,
and clean queue lifecycle on the same accepted build.

## Gate Decision

Task 12 steps 1-3 pass:

1. valid same-window control with target-only routing and zero socket drops;
2. every mini_vpn timed receiver result exceeds `150 Mbit/s`;
3. median receiver `191 Mbit/s` exceeds absolute `170 Mbit/s`;
4. all observed terminal/error/drop causes are exact and clean;
5. one same-build fixed `64 MiB` flow closes with zero tail.

This is a **Gate B PASS**, but it is not yet the plan's final stable-170 claim.
Task 12 step 4 remains mandatory: sustained `60s` reverse TCP, TCP
multi-flow/concurrency, UDP/live-streaming, fake-IP DNS, and TUN lifecycle
regressions. Old-path cleanup and the final release decision remain frozen
until those regressions pass.

## Operational Corrections

The user authorized evidence-backed configuration/orchestration corrections to
continue without a confirmation stop. Before repeat 3, preflight-only attempts
found and corrected four non-product issues: the suite filename, loading the
remote `.env`, the binary path (`mini_vpn client-tun`), and the dedicated
`EXIT_SSH_KEY`; interactive `sudo -v` was replaced only in a temporary runner.
None started mini_vpn or iperf, none consumed a measurement sample, and none
changed a frozen data-plane parameter.

## Artifacts And Cleanup

Sanitized evidence is retained locally under:

```text
/tmp/mini_vpn_h10d16_gateb_a54fb17/
```

Archive checksums:

```text
control  62c50c7e268f4074ff06175566a8feedcb21415faa78399fae340ce296a8732b
P1 run 1 7502e3587712884be19b0c69eca60879f0bc7b0204142fcb3ef79e540f0249eb
P1 run 2 4d93a931396136c8e76189a94d5594ae2e9dcd07f95edc0ff1b5ef8fb3392099
P1 run 3 aa1f6b6dfa2bad5052fadd95d2a501810b74193160a8765febf55cdeb3be420c
A-clean   594486371b77626220a0e16b8c03db1db7add27d798ed3007be1ac894656f857
```

The extracted-artifact scan found no `.env`, private key, UUID, or unredacted
password. `.27` had no remaining mini_vpn process or TUN after each run. `.111`
was restored fail-closed: Shoes and its watchdog are inactive, UDP `8443` is
free, FIFO/binary material is removed, and all four temporary socket sysctls
are back at `212992`. `.77` iperf3 remains active.
