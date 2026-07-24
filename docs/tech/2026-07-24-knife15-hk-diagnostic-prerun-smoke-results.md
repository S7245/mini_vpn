# Knife15 HK diagnostic pre-run smoke results

Date: 2026-07-24

Status: **PRE-RUN PASS — M1 diagnostic was not run**

## Provenance And Operation

The user-provided archive is:

```text
/tmp/mini_vpn_knife15_macos_20260724_103153.tar.gz
SHA-256 07f7e620b68cdeb82f51682eb913a4d15646cef7d7d7063aa9436e2deddeeb85
```

The checksum matches, and the archive has no absolute path, parent traversal,
link, or special-device entry. Its manifest records exact pushed source
`f2c1484c4adb10de6c18756e91398df6169a129a`. The release binary and runner
hashes exactly match the local reviewed artifacts:

```text
binary  05425026fcd17502f4e8205e187b881df71607d55b41623b02ee64da3f81d5c0
runner  480338657cccbe6590626c574e0ac5923e175996f0f68ff9bcfab662777af6a2
```

The event sequence was complete:

```text
10:31:53Z start requested
10:31:54Z ready utun=utun4
10:31:56Z smoke start
10:32:39Z smoke TCP pool idle
10:32:40Z smoke complete
10:32:40Z stop requested
10:32:41Z stop cleanup complete
```

Target and DNS routes used `utun4`, the TUIC Exit remained on physical `en0`,
fake-IP DNS returned `198.18.0.2`, and the secret scan passed. No M1 action or
M1 diagnostic action was started.

## Smoke Evidence

Both 20-second iperf commands completed:

```text
direction  sent bytes   received bytes   sent rate       received rate
forward     39,321,600       28,835,840   15.725625 Mb/s  11.436709 Mb/s
reverse    128,057,344      116,523,008   50.786166 Mb/s  46.601635 Mb/s
```

The forward top-level iperf intervals contain four zero-rate seconds:

```text
1.003493s -> 2.003665s
2.003665s -> 3.002034s
3.002034s -> 4.004898s
5.005037s -> 6.005066s
```

These are local sender intervals. Smoke does not request embedded server JSON,
so this artifact cannot classify them as Target receiver-zero intervals. The
reverse top-level receiver intervals are all positive.

The forward sender/receiver aggregate gap is `10,485,760B`, below the frozen
M1 aggregate `16MiB` bound. The forward data writer's maximum wait was
`4,245,473us`. The fresh QUIC data connection grew from a `12,000B` initial
cwnd to `460,775B`; it recorded `18` lost packets, `2,596B` loss, two
congestion events, and no PLPMTUD black hole.

## Historical Discriminator

The same directional smoke parser was replayed over the six preceding real HK
M1 bundles. Their forward cold-start sender-zero counts were:

```text
297dee9  4
f60926e  4
e479013 10
b33a3f6  8
1c587ba  0
dc8cfb1  4
```

Several of those runs subsequently completed hours of formal M1 traffic.
Therefore four cold-start sender-zero intervals are not a new regression and
do not predict the later Target receiver interruption. Turning smoke into a
strict sender-zero gate would reject an already established warm-up pattern
without discriminating the long-duration failure.

The direct Exit control lost one of three probes once during reverse smoke and
recovered to `3/3` in the next sample. The physical gateway remained `3/3`
throughout, and the physical interface had zero errors. This is a path-quality
warning, not enough by itself to override a fresh 300-second direct
discriminator.

## Healthy Invariants

- Endpoint conservation peaked at `61,410B`, below `61,440B`, and ended at
  `61,410/0/0B`.
- Endpoint abandoned bytes, pump-full waits, pump read errors, TUN flush
  failures, interface errors, and rebind failures were zero.
- TCP pool ownership reached exact `active_leases=0` before smoke completion.
- The one forward `Stopped(0)` occurred after remote EOF and closed with
  `queue_queued=0`, `queue_leased=0`, and `queue_reserved=0`; it is the
  accepted command-boundary REVIEW class.
- The reverse application-first close released the accepted bounded D16
  reservoir and ended with zero pool ownership.
- Process, TUN, and owned routes cleaned completely.

## Decision And Next Action

This bundle passes the start/smoke/stop pre-run boundary, but it cannot accept
M1 and contains no diagnostic timeline. It selects neither a mini_vpn repair
nor parameter tuning.

Because `stop` ended the run and the direct prerequisite expires after 15
minutes, the next execution must be one fresh complete sequence:

```text
baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic
-> status -> stop
```

Proceed only after the unchanged fresh baseline and 300-second direct
continuity gates pass. The diagnostic runner may record Target
receiver-zero, reverse-UDP loss above `3%`, and aggregate TCP gap above
`16MiB` and continue. Sender-zero remains visible REVIEW evidence but is not
the direction-aware continuity failure. Malformed evidence, command/DNS
failure, TUN/routes/watchdog failure, ownership/conservation mismatch,
resource/rebind SLO failure, and ledger corruption remain fail-fast.

The diagnostic result cannot accept formal M1 or unblock M2/M3. Formal M1
still requires a separate clean run.
