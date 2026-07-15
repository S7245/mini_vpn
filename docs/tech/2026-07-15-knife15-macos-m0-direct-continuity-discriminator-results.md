# Knife15 macOS M0 Direct-Continuity Discriminator Results

Date: 2026-07-15

Status: **M0 FAILED ON A GENUINE TARGET RECEIVER INTERRUPTION; OPERATOR AND
INTERNAL DATA-PLANE REGRESSIONS REJECTED; 300S PHYSICAL DIRECT GATE IMPLEMENTED
LOCALLY**

## Scope

Review the user-executed M0 bundle, distinguish an operation error from a real
quality failure, and add the smallest missing discriminator before another
two-hour TUN run. This stage does not change H10d16, pacing, MTU, pool, QUIC
windows, chunking, Cubic, GSO, self-wake, or any M0 load ratio.

Evidence:

- source: `239acba1f8778f629925a2b57ef8317b263e405f`;
- baseline: `/tmp/mini_vpn_knife15_macos_baseline_20260715_090456`;
- M0 bundle: `/tmp/mini_vpn_knife15_macos_20260715_091501.tar.gz`;
- M0 bundle SHA-256:
  `9cf99fa20578f2b2dda914622cc006a2c8a607e2e534709ca8e47731d5a3704d`.

The archive checksum, source, binary, and runner hashes match. The archive has
the expected regular-file shape and contains no symlinks.

## Operation Verdict

The user operation was correct. `start` became ready at `09:15:02Z`, smoke
completed at `09:15:48Z`, and M0 then completed two full mixed cycles. Cycle 3
forward started at `09:44:38Z`, ran its complete 300-second iperf command, and
failed at `09:49:41Z`. The later manual snapshot at `09:53:52Z` and stop at
`09:53:53Z` occurred more than four minutes after the phase had already
failed. Delayed password entry or cleanup therefore cannot have caused the
receiver interruption.

Cleanup was correct and complete: the process and watchdog ended, `utun4` was
removed, Target and Exit routing returned to `en1`, and the secret scan passed.

## M0 Result

The baseline measured `16.733575 Mbit/s` forward and `40.731931 Mbit/s`
reverse. The frozen M0 forward offer was therefore `8,366,787 bit/s`.

Cycles 1 and 2 each delivered exactly `313,786,368B` to the Target over about
300 seconds with zero Target receiver interruptions. Cycle 3 also ultimately
delivered exactly `313,786,368B` at `8.363 Mbit/s`, but its structured Target
receiver output contained three complete zero-throughput seconds:

- `11.001-12.001s`;
- `97.001-98.001s`;
- `100.001-101.001s`.

The client sender output contained 31 zero intervals in this phase. The
direction-aware receiver SLI correctly emitted
`reason=receiver_zero_interval`; complete byte delivery does not erase a
user-visible continuity break.

The two completed reverse-UDP phases passed their existing workload checks
with approximately `1.107%` and `1.224%` loss. M0 stopped before idle/resume
and final-drain qualification, so it remains failed and cannot unlock M1.

## Internal Discriminators

The evidence rejects a local ownership, TUN service, resource, or relay-close
regression:

- endpoint conservation stayed at or below `61,440B` and ended at
  `61,388/0/0B` available/live/outstanding;
- endpoint outstanding peaked at `1,280B`, with no endpoint socket-blocked or
  delayed-service event;
- ingress pump high-water was `291/500`, with zero full waits and read errors;
- utun and internal interface error counters remained zero;
- process RSS had no positive end-to-end slope, and FD/thread counts remained
  `15/11`;
- cycle 3 forward closed with `clean_queue_lifecycle`, zero queued/leased/
  reserved ownership, exact `313,786,405B` writer progress, and a maximum
  writer wait of `8.900252s`;
- no reconnect, terminal pending reap, log compaction, or M0 health failure
  occurred.

The six canonical remote-write failures in the aggregate summary belong to
short-flow close tails and appear as 18 raw/derived log matches. The failed
bulk forward relay itself closed cleanly, so those aggregate close tails are
not its cause.

## Path And QUIC Evidence

All 81 Exit and gateway ICMP samples reported zero loss. Physical and utun
interface errors were zero, and both pre-TUN routes used `en1`. These coarse
30-second controls reject a broad whole-host outage, but cannot reject a
one-second TCP/UDP-specific or flow-specific network event.

The active bulk QUIC connection accumulated roughly `532,638B` additional
formal loss and 139 congestion events during cycle 3 forward, with a cwnd
trough near `43,433B`; its writer waited as long as 8.9 seconds. The sibling
connection stayed near `448,243B` cwnd and did not accumulate comparable loss.
This selects bulk-connection/UDP-path pressure rather than a process-wide
service failure, but does not yet distinguish mini_vpn's tunnel architecture
from the same physical HK-to-Target path under a 300-second TCP workload.

## Missing Discriminator

The old baseline cannot answer that question. It lasted only 20 seconds and,
before this repair, validated forward client-root intervals rather than the
Target receiver intervals in `server_output_json`. It sizes the M0 offer but
does not prove that the direct physical path can meet the same zero-interrupt
SLI for 300 seconds.

Repeating the same two-hour M0 without closing this gap would spend another
TUN run without a decisive attribution signal.

## Local TDD Repair

Commit `885c321` makes the macOS runner add a non-root
`direct-discriminator` action and makes it
a formal M0 prerequisite:

1. `baseline` requests `--get-server-output` in both directions and validates
   the real receiver boundary: Target server for forward, local client for
   reverse.
2. `direct-discriminator` requires that fresh baseline, keeps Target and Exit
   on physical non-utun routes, and runs the exact M0 forward shape directly:
   one stream, 300 seconds, and 50% of the forward Target receiver baseline.
3. PASS requires 300 complete positive Target receiver seconds, a 299-310
   second receiver duration, positive end bytes/rate, unchanged physical
   routes, and exact target/protocol/direction evidence.
4. Its manifest binds the source commit, runner and release-binary hashes,
   both baseline hashes, result hash, route interfaces, target, rate, duration,
   and completion time.
5. Formal `m0` accepts only matching PASS evidence completed within 15 minutes
   and copies the manifest/result into the immutable M0 bundle.

The action never starts a TUN and requires no `sudo`. Baseline receiver-zero,
direct receiver-zero, shortened direct result, stale evidence, changed
baseline, provenance mismatch, or route drift all fail closed.

## Decision Tree And Stop Rules

- If the direct 300-second gate fails, do not start TUN and do not tune
  mini_vpn. The current physical path cannot meet the formal continuity SLI;
  retry only with a fresh baseline/direct pair in a more suitable network
  window.
- If the direct gate passes and the immediately following M0 repeats a Target
  receiver interruption while internal invariants remain clean, classify the
  current single-bulk-QUIC transport behavior as an architecture failure. Do
  not run another identical M0. Start a design stage for connection-health
  isolation/failover with a mature-client control.
- If direct and M0 both pass, run the independent stop/re-create/smoke/stop
  rearm gate; only then may Knife15 proceed to M1.

M1 remains blocked. No data-plane or workload constant changed in this stage.
