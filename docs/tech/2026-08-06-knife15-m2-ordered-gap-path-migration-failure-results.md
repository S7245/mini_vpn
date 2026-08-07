# Knife15 M2 Ordered-Gap Path Migration Failure Results

Date: 2026-08-06

Status: **FORMAL M2 FAILED; EXACT ORDERED-DOWNLINK GAP SELECTED**

Source: `0a71ebe4b1c147f9db5982ec3125cd54a495a067`

Artifact:
`/tmp/mini_vpn_knife15_macos_20260806_104632.tar.gz`

SHA-256:
`da12448ca74e56ed29c0b3603484eafff44d2f39d94a5fe2602337271422103b`

## Verdict

The run did not fail after a useful fourteen-hour soak. Formal M2 had already
failed about twenty-five minutes after its schedule began, in cycle 2 reverse
TCP. The test Mac then correctly retained TUN, routes, process, and evidence
until `status/stop` roughly fourteen hours later. The later activity is
diagnostic hold time, not completed M2 time.

The selected defect is an established QUIC stream whose ordered application
read stopped for `12,630ms` while its owning QUIC connection continued to
receive packets and STREAM frames. The existing recovery predicates could not
act: the transfer was downlink-only, its writer was not the blocked edge, and
the connection's PLPMTUD black-hole counter did not advance during the gap.

This is not a reason to tune D16, MTU, pool, QUIC windows, chunk size, Cubic,
GSO, Endpoint pacing, or self-wake. The next architecture needs an exact
ordered receive-gap observer and peer-visible QUIC active migration.

## Provenance And Preflight

- binary SHA-256:
  `23cb2d9a0a93ca93a999f13d5fa2c80a2b531d978c0d592393b2fc299b948c93`;
- runner SHA-256:
  `516ee92772e4051b2de9c7b1a353344cef8808ddff060238f5582ba174439daa`;
- baseline forward/reverse receiver:
  `46.328273 / 59.947268 Mbit/s`;
- fresh 300-second direct discriminator:
  `23.076770 Mbit/s`, with no continuity failure;
- IPv6, full-tunnel, DNS, real-client, start, and smoke preflights passed;
- cycle 1 completed, then cycle 2 forward completed;
- cleanup restored routes/DNS, stopped the process, and finalized the bundle.

The exact workload offered reverse TCP at `29,973,634 bit/s`. The failed
result eventually delivered all `1,124,641,792B` at about `29.9731 Mbit/s`,
so the failure is a transient continuity defect rather than permanent byte
loss.

## Exact Failure Window

Events record:

```text
10:47:33Z  controlled drain complete
11:02:06Z  cycle 1 complete
11:12:11Z  cycle 2 tcp-reverse receiver_zero_interval
11:12:11Z  formal M2 failed
01:26:36Z  next-day cleanup complete
```

The failing TUIC relay was:

```text
connection stable id = 43839895568
pool slot            = conn1
QUIC stream          = 21
Target               = 43.130.32.77:5201
relay mode           = d16_direct_ordered
```

The local ordered reader stopped at `569,624,937B`. At its first complete
one-second Pending report, the owning connection had received another `111`
datagrams, `96,732B`, and `62` STREAM frames since that reader last returned a
byte. The reader remained fixed at the same byte through eleven complete
receiver-zero intervals and resumed after `12,630ms`.

The historical log can attribute those new STREAM frames only to the owning
connection, not conclusively to stream 21. That limitation is the reason for
the new Quinn read-only assembler seam: the bounded qualification must prove
that bytes are buffered on the exact stream strictly after its missing
ordered prefix before recovery is authorized.

## System Discriminators

During the same episode:

- conn0 and conn1 congestion windows fell from about `1.5MB` to `2,818B`;
- conn1 PLPMTUD black holes stayed at `132`; conn0 stayed at `107`;
- no exact writer ACK-stall rebind, connection-local path reset, or Endpoint
  rebind occurred before the transfer's continuity failure;
- D16 queued/leased/reserved ownership stayed bounded and its flush/send
  errors remained zero;
- TUN pump full waits/read errors, send-slice errors, and flush failures were
  zero;
- Endpoint conservation stayed at or below `61,440B`, with zero socket
  would-block;
- routes, interfaces, gateway controls, process, and cleanup remained valid.

The summary's `16/12/0` Endpoint rebind attempt/recovery/failure counts and
large recovery times are from generic post-failure traffic while the TUN was
held for evidence. They occurred after formal M2 had failed and cannot explain
or repair the selected stream-21 window.

## Rejected Branches

- Operator misuse: preflights, workload, evidence retention, and cleanup were
  correct.
- Slow bandwidth: both baseline and direct continuity passed, and the failed
  transfer ultimately delivered its full byte count at the offered rate.
- D16/smoltcp/TUN starvation: queues, permits, local egress, and TUN counters
  continued to make bounded progress without errors.
- Endpoint pacing capacity: conservation and socket outcome remained healthy.
- Existing connection-local path reset: its writer-Pending plus black-hole
  predicate was unreachable for this downlink-only gap.
- Generic TCP no-RX: it cannot distinguish a broken download from legitimate
  Apple Push/long-poll silence and previously caused false migrations.
- Parameter tuning: the run selected an observation/action seam, not a
  capacity constant.

## Selected Next Step

Implement the architecture in
`2026-08-06-knife15-m2-ordered-gap-path-migration-architecture-spec.md`.
After local gates, run exactly one two-cycle `m2-qualification` plus cleanup.
Do not run formal M2 first and do not repeat the unchanged 24-hour schedule.
