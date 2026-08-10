# Knife15 M2 Transport-Write-Demand Flight Qualification Failure Results

Date: 2026-08-10

Status: **QUALIFICATION FAILED; PAIRED EVIDENCE SELECTS A WRITE-PROGRESS/WORKER-TURN OWNERSHIP GAP; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:

```text
33dddf002e6fcaf98ba5bddf03537f30642bcbcc4ced986d05a38218852c4798
/tmp/mini_vpn_knife15_macos_20260810_021421.tar.gz
```

Exact source: `b437b93ca1eca12f630ec103bced904620a62433`.

Paired Exit artifact:

```text
2bc46085354fc91b1b790691d4578e47e4e36f979ebd27e6b9876a4356b289bb
/tmp/mini_vpn_knife15_exit_target_observer_20260810_020005.tar.gz
```

## Envelope

- baseline forward/reverse: `25.429/54.539 Mbit/s`;
- bounded direct forward: `12.715 Mbit/s`, no complete receiver-zero interval;
- start/smoke and every qualification preflight passed;
- cycle 1 and cycle 2 long forward/reverse TCP plus reverse UDP passed;
- cycle 2 short forward failed one complete Target receiver interval;
- formal M2 was not run; route/DNS/TUN/process cleanup passed.

The seven completed phase results and one failed phase are qualification
evidence only. They do not constitute formal acceptance.

## Exact Client Evidence

The failed short flow used conn1 stable id `44036398096`, generation 3,
stream 8. During the ten-second phase:

```text
local TCP sender admitted:          9,961,472B
D16 writer accepted into Quinn:     6,296,563B
Target result:                      4,194,304B
maximum exact writer Pending:       3,820,792us
sender/receiver zero intervals:     3 / 1
QUIC cwnd before/after:              12,947B / 223,724B
```

Every exact writer-pressure episode continued to make ACK progress, so the
existing ACK-stall rebind correctly did not fire. D16 queued/leased/reserved
ownership settled cleanly. Endpoint conservation, TUN/interface counters,
routes, process, and cleanup stayed healthy.

## Paired Exit Evidence

The exact business socket was `172.26.0.2:35666 -> 43.130.32.77:5201`.
Across `10.179s` the capture saw `4,231,130B` of TCP payload. Positive supply
never paused more than `165.430ms`; the approximate per-second ramp was:

```text
42,664 84,197 110,404 135,266 179,440 196,016
296,784 488,691 802,037 1,232,097 663,534 bytes
```

Target ACK RTT was about `1..4ms`, the sender TCP socket recorded zero
retransmit growth, and capture/kernel drops were zero. The Exit socket stayed
application-limited, meaning it forwarded promptly whenever QUIC supplied
bytes. This rejects Target, Exit-to-Target, and observer loss as the pause.

## Selected Mechanism

The D16 production writer performs a successful `writer.write(...)`, then
awaits delivery of `RelayWriterSignal::Progress` through an async channel.
During that handoff the Quinn connection worker can acquire the connection,
packetize the newly accepted prefix, and publish packets before the writer's
next poll reaches `WriteError::Blocked`.

The reviewed `f9c3c23` logic retained a Blocked bit across Writable delivery,
but cleared it immediately on any nonempty retry. Packet ownership is assigned
later when the connection worker emits STREAM frames, so this exact worker
turn had no demand owner. The previous deterministic test retried immediately
and re-established Blocked before driving the worker, so it did not reproduce
production scheduling.

The new RED inserts the worker turn between successful retry and the next
Blocked call. The old implementation ends at `1,772,034B` cwnd versus the
required `1,990,080B` bound after `2MiB`.

## Stop Boundary

Do not tune D16, MTU, pool, windows, chunk, Cubic, GSO, Endpoint, self-wake,
rate, or burst. First preserve exact accepted-offset flight ownership through
the worker turn, prove ACK/terminal cleanup and cross-stream isolation, run all
local gates/review, then take one fresh paired qualification. Do not run formal
M2 or repeat this exact source.
