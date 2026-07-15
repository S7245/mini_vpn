# Knife15 macOS Low-Rate Reverse Observer Results

Date: 2026-07-15

Status: **SHENZHEN OPERATOR/ROUTE CORRECT; 128K OBSERVER QUANTIZATION
CONFIRMED; LOCAL REPAIR PASS; FRESH BASELINE PENDING**

## Evidence And Host Attribution

The user ran source `adbc78b` on the separate Shenzhen Mac with Target and
Exit on physical `en0`. The failed baseline directory was copied to the
current Mac for inspection:

- `/tmp/mini_vpn_knife15_macos_baseline_20260715_131049`;
- forward SHA-256:
  `980faedd1274aeb65110c32666fb30c32cd2eb1b106169f44f7a25dcf748257c`;
- reverse SHA-256:
  `8a02d496d036e9c782b5b13f57b7d6d01e4cae2ecf5ddcb4cd70a814483efb38`.

The current Mac's live `utun1024`/Clash routes are unrelated to the Shenzhen
run and must not be used to attribute this artifact. The user's `en0` route
statement is the accepted run-host evidence.

## Exact Failure

Forward baseline was structurally valid and continuous at the Target:

- `54,788,096B` received;
- `21.732742 Mbit/s`;
- 20 complete positive client intervals and 21 positive server intervals.

Reverse baseline was also structurally valid, but the local receiver averaged
only `0.524183 Mbit/s` and delivered `1,310,720B`. Thirteen of its 20 complete
receiver intervals reported zero bytes, so the strict receiver-continuity
validator correctly rejected the old evidence shape.

Every nonzero reverse receiver interval contained an exact multiple of
`131,072B`, and the result declared iperf TCP `blksize=131072`. At the measured
rate, useful delivery is only about `65,523B/s`, less than one default iperf
application buffer per second. The old observer therefore could not distinguish
a continuously progressing sub-buffer TCP flow from a real one-second path
stall. It was an observer-capacity failure, not proof of physical discontinuity.

The formal M0 reverse rate would be 50% of baseline, about `0.262 Mbit/s` or
`32,761B/s`. With the old 128KiB buffer, one reportable block could take about
four seconds. A strict per-second no-zero SLI was unreachable even for smooth
delivery.

## Goal, Non-Goals, And Capacity

Preserve the strict receiver no-zero SLI by increasing observation resolution,
not by relaxing acceptance. Preserve the exact forward and short-forward
commands used to validate the pool-parity repair.

This stage does not change mini_vpn H10d16/D16 chunking, endpoint pacing, MTU,
pool, QUIC windows, Cubic, GSO, self-wake, M0 rates/durations, UDP payload, or
Target receiver semantics.

Reverse TCP baseline and M0 now use iperf `-l 16384`. At the formal half-rate
this gives about two completed observer buffers per second. At `100 Mbit/s` it
requires about 763 application buffers per second, which is bounded and small
relative to the data-plane capacity. Forward retains iperf's default 128KiB
buffer and the 300-second direct discriminator is unchanged.

## TDD And Implementation

Two tracer bullets were run:

1. RED: the M0 public command log required `-l 16384` on reverse TCP and failed.
   GREEN: the reverse-only branch added the observer length and profile field.
2. RED: a fake iperf baseline probe interface was missing. GREEN: production
   baseline and self-test now share `run_direct_baseline_probe`; forward is
   exact-old-shape and reverse alone uses `-l 16384`.

Implementation commit: `cc32df0`.

Local gates passed:

- Knife15 runner internal and external self-tests;
- Knife14 low-RTT, US-client-suite, and sing-box-control self-tests;
- bash syntax, root formatting, diff checks, and code review;
- no unresolved P0/P1 and no frozen data-plane/workload change.

## Next Gate And Stop Rule

The old baseline remains invalid and must not be reused. Rebuild the final
source, run a fresh physical-`en0` baseline, and inspect its reverse JSON.

- If 16KiB reverse intervals are all positive, continue to the unchanged
  300-second forward direct discriminator, then user-run start/smoke/M0.
- If reverse still contains zero intervals, the smaller observer has rejected
  the quantization hypothesis. Stop before TUN and classify the Shenzhen-to-
  Target reverse physical path as insufficient for this formal M0 window; do
  not relax the SLI or tune mini_vpn constants.

M1 remains blocked until M0 and an independent rearm pass.
