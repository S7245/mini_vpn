# Knife15 M2 terminal-local-close replay architecture

## Stage goal

Repair the final D16 ownership replay so that it distinguishes a live or
owned downlink tail from bytes that have already left D16 ownership and are
only stranded in smoltcp after the local TCP peer becomes terminally closed.

## Evidence selecting this stage

Exact-source `bec6dc8` qualification traffic completed with no Target
receiver-zero interval and clean Endpoint, D16, TUN, route, DNS, process, and
cleanup evidence. Final safety replay rejected two 24-byte Apple control-flow
tails. In both cases the D16 queue closed at `0/24/0B`, local EOF recorded the
same 24-byte send queue, and the handle later closed with:

- `pending=0` and `terminal_pending_reap_bytes=0`;
- `permit_terminal_drop_bytes/events=0/0`;
- `close_egress_class=terminal_closed_no_send` and exactly 24 egress bytes;
- `tcp_state=Closed`, `active=false`, `can_send=false`, `may_send=false`.

The D16 lease was released when the TCP payload was flushed to TUN. The final
smoltcp queue cannot make further progress after the local peer closes and is
not Endpoint or D16 ownership.

## Invariant

For every positive `queue_leased` relay-close record on a handle, replay must
still observe the same byte count at local EOF and then one of exactly two
same-handle terminal outcomes before reuse:

1. **drained:** final `send_queue=0` and terminal pending/reap is zero;
2. **local terminal abandonment:** final send queue and close-egress bytes
   exactly equal the leased byte count, pending/reap and permit-terminal-drop
   are zero, close-egress is not a drain candidate, and the socket is Closed,
   inactive, and unable/may-not-send.

Any missing field, byte mismatch, send-capable state, D16/pending/reap/drop
ownership, open queue, incomplete sequence, or handle reuse remains a hard
failure.

## Non-goals and frozen values

- No Rust production behavior or transport lifecycle changes.
- No D16, MTU, pool, QUIC-window, chunk, Cubic, GSO, Endpoint, self-wake,
  workload, SLI, or timeout changes.
- Do not classify active or potentially drainable close-egress as safe.
- Do not rewrite the immutable qualification artifact or its failed status.

## Acceptance

- A focused runner fixture for the exact terminal-local-close sequence is RED
  before the repair and GREEN afterward.
- Negative fixtures keep mismatched bytes, send-capable states, ownership
  drops, and handle reuse fail closed.
- The full runner self-test, artifact replay, shell syntax, diff checks, and
  code review pass.
