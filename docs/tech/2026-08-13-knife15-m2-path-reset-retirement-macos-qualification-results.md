# Knife15 M2 path-reset retirement macOS qualification results

## Verdict

The exact-source `bec6dc8` paired qualification is accepted by reviewed
replay as **PASS_NON_ACCEPTANCE**. It qualifies the ACK-progress path-reset
retirement without constituting formal M2 acceptance. The immutable Mac
artifact remains `failed/MISMATCH`; it is not rewritten.

Formal M2 is reopened on the reviewed terminal-replay repair at `0296688` or
a descendant. M3 remains blocked until one fresh formal M2 and cleanup pass.

## Artifacts

- Mac:
  `/tmp/mini_vpn_knife15_macos_20260813_030113.tar.gz`
- Mac SHA-256:
  `8a65cc5bbcf2ea447ce516acec0a20c77c5acb3f535418c6197f2e11dad78269`
- exact source: `bec6dc8404859825760572a5839b23a5351e863a`
- paired Exit:
  `/tmp/mini_vpn_knife15_exit_target_observer_20260813_030203.tar.gz`
- Exit SHA-256:
  `97b7f704d9f8a54886001e6676b1fa0ed53cd6ec8a807305c264793d0a7f53c4`

Both archive checksums matched their authoritative records. The Exit capture
recorded `11,576,962` packets received by the filter with zero kernel drops.
Its four nftables counters were nonzero, the observer state and nftables table
were removed after bundling, and sing-box remained active.

## Qualification evidence

- baseline: `31.445/60.511 Mbit/s` forward/reverse;
- bounded direct discriminator: `15.716 Mbit/s`;
- two cycles, eight phases, two DNS checks, and two real-client checks;
- six TCP and two reverse-UDP results;
- zero Target receiver-zero intervals;
- maximum TCP sender/receiver gap: `7,208,960B`;
- maximum reverse-UDP loss: `2.230867%`;
- Endpoint maximum/final ownership:
  `61,440B / 61,414/0/0B`;
- recovery starts/ends: `380/380`, with safe exact evidence;
- process, routes, DNS, TUN, interface, conservation, and cleanup: clean.

Three `Stopped(0)` writes were completed timed-transfer close tails. Their
exact remote-write ownership replay passed; there was no stalled-write or
idle timeout, terminal pending reap, send-slice error, TUN flush failure,
pump read error, or pump-full wait.

The workload also reached a useful replacement discriminator. Slot 1 began a
replacement with current/inherited service floor `242,949B`. Its successor
proof lost `1,280B` in round one, so installation failed closed and traffic
fell back to slot 0. Existing traffic retained service and no receiver-zero
interval followed. No mini_vpn connection-local path reset remained.

## Why the archive said failed

Traffic completed at `03:29:10Z`. Final safety waited the full 50-second
drain bound and failed at `03:30:02Z`. Every terminal-safety subcheck passed
except `d16_terminal_ownership_is_clean`.

Two ambient Apple control flows each had the same sequence:

1. D16 queue terminal `queued/leased/reserved = 0/24/0B`;
2. local EOF with `send_queue=24` while the socket was still send-capable;
3. handle close with `pending=0`, terminal reap `0`, permit terminal drop
   `0/0`, `close_egress_class=terminal_closed_no_send`, and a 24-byte send
   queue on `Closed/active=false/can_send=false/may_send=false`.

The D16 lease had already been released when the 24-byte TCP payload was
flushed to TUN. The local TCP peer then closed before acknowledging it, so the
remaining smoltcp queue was neither deliverable nor D16/Endpoint-owned. The
old replay required a literal zero send queue and therefore produced a runner
false negative.

## Repair and gates

Reviewed `0296688` accepts only two same-handle completions after an exact
positive D16 lease/local-EOF byte match:

- the send queue drains to zero; or
- the same byte count remains only in an exact terminal local-close state,
  with pending/reap/permit-drop zero and no send authority.

Missing fields, byte mismatch, pending/reap/drop ownership, a drain candidate,
send capability, incomplete lifecycle, or handle reuse still fails closed.
No Rust production code or frozen data-plane/workload value changed.

TDD recorded the expected terminal-local-close RED and GREEN, plus negative
permit-drop, egress-byte-mismatch, send-capable, incomplete-drain, byte-
mismatch, and handle-reuse fixtures. The complete runner self-test, exact Mac
artifact terminal replay, `bash -n`, diff checks, and secret scan passed.
Review found no unresolved P0/P1 after `2ada935` raised the formal source floor
from the stale predecessor to `0296688`.

## Next action

Do not repeat qualification. Pull and rebuild the pushed reviewed descendant,
start one fresh `.33` observer after smoke, and run exactly one formal:

`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
observer start -> m2 -> status -> stop`

Reserve about 25 uninterrupted hours and sync both final bundles.
