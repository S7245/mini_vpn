# Knife15 macOS Low-Rate Reverse Observer Results

Date: 2026-07-15
Updated: 2026-07-16

Status: **SHENZHEN SPEED PROFILE ACCEPTED; 1K PHYSICAL BASELINE PASS;
300S DIRECT DISCRIMINATOR PENDING**

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

The 16KiB-repaired runner was then replayed in Shenzhen. Its copied archive is:

- `/tmp/mini_vpn_knife15_macos_baseline_20260715_154130.tar.gz`;
- archive SHA-256:
  `49c4b71aed357273aa600e6040d9ed594d62609e4ed653f36b6eb0d4a31b86b8`;
- forward JSON SHA-256:
  `638254835f3c4844ecaeefca37c93cefe4df2b50d25c8feef9e690a6557d1a95`;
- reverse JSON SHA-256:
  `80e300e21f136613f3246aaad89b85e7facc665f279574ea03eca10815e10da0`.

The archive checksum is exact and contains only the expected directory and two
regular JSON files. Its `blksize=16384` proves that the repaired reverse command
was reached. Baseline itself does not bind a source commit, so no stronger
source-provenance claim is made from this archive alone.

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

## 16KiB Replay And Physical Speed Profile

The second forward receiver delivered `80,216,064B` at `31.798980 Mbit/s` with
all 21 Target intervals positive. The client sender had two zero intervals and
`9,420` retransmits; those are physical-path quality evidence, not the forward
receiver SLI.

The second reverse receiver delivered `327,680B` at only
`0.131071 Mbit/s`, approximately `16,384B/s`. It had five zero intervals, and
every positive interval was again exactly one or more 16KiB blocks. The sender
reported `376,832B` at `0.149423 Mbit/s`, 45 retransmits, RTT about
`169-184ms`, and maximum cwnd only `8,328B`.

This is a valid Shenzhen physical speed/path profile and not a mini_vpn bug:
TUN and mini_vpn were not running. The absolute speed has no failure threshold.
However, 16KiB was still not a valid one-second observer because it equaled a
full second of useful delivery and exceeded the observed sender cwnd. The five
zeros therefore remain quantization-ambiguous rather than proof of a physical
one-second outage.

## 1KiB Physical Baseline Acceptance

The fresh copied Shenzhen archive is:

- `/tmp/mini_vpn_knife15_macos_baseline_20260716_064427.tar.gz`;
- archive SHA-256:
  `5ea583c580d59111b8fce8caca013dc140b5e083c6e64c5e26a2402c1dda3f57`;
- forward JSON SHA-256:
  `5ad7483c337d7bb70a2638f7488e11846fc3f0bb336a8c983084333e1282d230`;
- reverse JSON SHA-256:
  `0047bc043e8cb3aaaf378a671e8a6ef8188aeba8d59ca111a52e2c51ee6da2b3`.

The archive checksum is exact and it contains only one directory and the two
expected regular JSON files. Reverse declares `blksize=1024`, proving the 1KiB
observer command was reached. Baseline JSON does not bind an exact source
commit, so the artifact does not make a stronger source-provenance claim.

Forward Target receiver delivered `57,147,392B` at `22.436698 Mbit/s`; all 21
server receiver intervals were positive. The client sender delivered
`58,327,040B` at `23.321742 Mbit/s`, all client intervals were positive, and
reported 4,068 retransmits.

Reverse local receiver delivered `454,656B` at `0.181787 Mbit/s`; all 20
receiver intervals were positive. Each interval delivered at least `12,288B`,
well above one 1KiB observation block. The server sender delivered `518,144B`
at `0.205455 Mbit/s`, with nine sender-zero intervals, 50 retransmits, RTT
about `164-170ms`, and maximum cwnd `8,328B`.

The formal validator passes both files. This accepts receiver continuity at
the observed low physical capacity while preserving sender loss/cadence as
diagnostic evidence. The low speed remains Shenzhen environment capability,
not a mini_vpn defect: baseline is deliberately run before mini_vpn/TUN.

## Goal, Non-Goals, And Capacity

Preserve the strict receiver no-zero SLI by increasing observation resolution,
not by relaxing acceptance. Preserve the exact forward and short-forward
commands used to validate the pool-parity repair.

This stage does not change mini_vpn H10d16/D16 chunking, endpoint pacing, MTU,
pool, QUIC windows, Cubic, GSO, self-wake, M0 rates/durations, UDP payload, or
Target receiver semantics.

Reverse TCP baseline and M0 now use iperf `-l 1024`. At the latest formal
half-rate this gives about eight completed observer buffers per second and each
buffer is far below the observed `8,328B` cwnd. At `100 Mbit/s` it requires
about 12,207 application buffers per second, which remains bounded; macOS M0 is
a relative-rate longevity lane, while high-throughput acceptance remains on
the Knife14 VPS lane. Forward retains iperf's default 128KiB buffer and the
300-second direct discriminator is unchanged.

## TDD And Implementation

Two tracer bullets were run:

1. RED: the M0 public command log required `-l 16384` on reverse TCP and failed.
   GREEN: the reverse-only branch added the observer length and profile field.
2. RED: a fake iperf baseline probe interface was missing. GREEN: production
   baseline and self-test now share `run_direct_baseline_probe`; forward is
   exact-old-shape and reverse alone uses `-l 16384`.

The 16KiB replay then drove two more tracer bullets:

3. RED: reverse baseline/M0/profile command contracts required `-l 1024` and
   rejected 16KiB. GREEN: the single reverse observer constant changed to
   1KiB; no forward command changed.
4. RED: a valid baseline could not produce a direction-aware human speed
   summary. GREEN: baseline now prints receiver Mbit/s and zero counts before
   applying continuity acceptance, including on a later failure.

Implementation commits: `cc32df0` and `e49d83c`.

Local gates passed:

- Knife15 runner internal and external self-tests;
- Knife14 low-RTT, US-client-suite, and sing-box-control self-tests;
- bash syntax, root formatting, diff checks, and code review;
- no unresolved P0/P1 and no frozen data-plane/workload change.

## Next Gate And Stop Rule

The two old baselines remain invalid and must not be reused. The fresh 1KiB
baseline is accepted only as the capacity/short-continuity prerequisite.

- Keep mini_vpn/TUN off and run the unchanged 300-second forward direct
  discriminator with the original Shenzhen baseline directory.
- Treat its rate as environment capacity, never as a minimum product threshold.
- If every Target receiver interval is positive, continue to user-run
  start/smoke/M0 using the exact baseline/direct pair.
- If the direct discriminator reports a Target receiver zero, stop before
  formal M0 and preserve it as physical-path continuity evidence, not a
  mini_vpn bug. A separate degraded-path soak may use relative/direct-control
  acceptance, but must not silently weaken formal M0 or tune mini_vpn
  constants.

M1 remains blocked until M0 and an independent rearm pass.
