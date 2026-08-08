# Knife15 M2 Transport-Write-Demand Qualification Failure Results

Date: 2026-08-08

Status: **QUALIFICATION FAILED; FAILURE SELECTS CLIENT-TO-EXIT BUSINESS CONGESTION OWNERSHIP; FORMAL M2 AND M3 REMAIN BLOCKED**

Mac artifact:

```text
100d316371397d86906df1ab836fd95daf087613edd6134b0357945acd76b822
/tmp/mini_vpn_knife15_macos_20260808_092053.tar.gz
```

Exact source: `eb2185f8237abf5698647fd44047120797854515`.

## Envelope

- baseline forward/reverse: `17.521/65.771 Mbit/s`;
- bounded direct forward: `8.758 Mbit/s`, no complete zero interval;
- smoke forward/reverse: `67.438/46.284 Mbit/s`;
- IPv6, full-tunnel, DNS/real-client, route, process, Endpoint conservation,
  and cleanup gates passed;
- cycle 1 long forward, reverse TCP, and reverse UDP completed;
- formal M2 was not run.

The first failure was cycle 1 `short-forward-1`. The sender admitted
`10,092,544B`, the Target reported `4,194,304B`, sender intervals 5 and 7 were
zero, and the validator reported one complete receiver-zero interval.

## Exact Transport Evidence

The reviewed authentication-flight ownership was present and active. Slot 1's
generation-1 predecessor had advanced from black-hole anchor `135` to `136`.
Nine successor attempts failed closed on an exact `1366B` PLPMTUD probe loss.
Their business opens correctly fell back to qualified slot 0. A later attempt
completed its own service flight and installed generation 2:

```text
predecessor id:       32733085712
successor id:         32733092880
service turn:         329ms
replacement total:    496ms
installed cwnd:       24,886B
installed path RTT:   165.204ms
```

The short-flow data stream used the new generation. During its ten seconds:

```text
D16 writer accepted by Quinn:          6,300,119B
exact writer maximum Pending:          2,367,062us
last observed QUIC acknowledgement:    3,849,795B
Target received:                       4,194,304B
final successor cwnd:                    271,543B
successor QUIC packet loss:                2,818B
successor congestion events:                    1
successor black holes:                          0
```

Every writer-pressure episode made ACK progress; the existing ACK-stall reset
correctly did not fire. The Target had already received more bytes than the
last client-side QUIC acknowledgement snapshot, so the principal debt was
before Exit-to-Target forwarding. D16 queues, TUN, Endpoint conservation, and
cleanup were healthy.

## Selected Failure

Quinn records a stream that returns `WriteError::Blocked` in its writable
notification list. When ACK progress makes it writable, polling the stream
event removes it from that list before the woken application task retries the
write. In that scheduling interval an empty connection transmit poll can set
the global `app_limited` value. ACKs then fail to grow Cubic even though the
application has already proven continuous demand by reaching transport
backpressure.

This is distinct from the already repaired Endpoint-reservation wait. The
Endpoint repair preserves congestion ownership while the connection is
waiting below Quinn; this failure loses ownership between Quinn's Writable
delivery and the application writer's next connection-lock acquisition.

## PLPMTUD Readiness Discriminator

The nine identical failed successor attempts also prove that an already
in-flight PLPMTUD probe is not a safe protocol-readiness prerequisite. The
eventual successor operated with zero black holes after ordinary MTUD probe
loss. Authentication STREAM bytes must remain inside exact readiness ACK/loss
ownership, but an older MTU search probe belongs to the unchanged MTUD state
machine and must not reject a successor by itself.

## Evidence Limitation

No fresh paired `.33` observer was active for this run. The previous observer
had ended before the Mac workload, so no historical capture is presented as
paired evidence. This does not prevent the pre-Exit classification above:
client writer acceptance and QUIC ACK bytes were both below the Target's
reported bytes. It does prevent any finer claim about the exact Exit socket
cadence.

## Stop Boundary

Do not rerun unchanged or tune MTU, pacing, pool, windows, chunk, Cubic, GSO,
or Endpoint values. First lock down transport-write demand and independent MTU
probe ownership with deterministic tests, run all local gates and review, then
take one fresh paired qualification only.
