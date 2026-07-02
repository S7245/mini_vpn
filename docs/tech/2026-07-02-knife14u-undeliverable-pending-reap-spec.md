# Knife14u Spec: Reap Undeliverable Pending Downlink

## Grounding

Knife14t was tested with
`/tmp/mvpn_knife14t_usclient_suite_20260702_215901.tar.gz` on commit
`51072c8`.

The run proved the VPS preflight and direct path were healthy:

- `.33` route and ping succeeded.
- `.77:5201` direct iperf completed.
- `cargo build --release` completed and the report recorded the binary checksum.

The tunnel regressed badly: forward throughput dropped to low Mbit/s or below,
reverse had Kbit/s-scale windows and timeout cases, and the client loop stayed
mostly parked. The remaining close diagnostics still showed
`dead_slot_reap ... pending>0`, but the pending amounts were much smaller than
knife14s because knife14t held inactive pending for a progress-sensitive grace
window.

## Problem

Knife14t made the pending grace too broad:

```text
downlink_pending != empty && !active => wait for grace if recent progress exists
```

That protects useful tail bytes, but it also keeps slots alive when the smoltcp
TCP socket is no longer send-capable. Once the local socket cannot accept
downlink bytes, the remaining pending buffer is no longer deliverable. Keeping it
dirty for several seconds can leave stale flows visible between iperf phases and
can make the next run compete with dead local state.

## Required Behavior

1. `Listening` slots are never reaped.
2. Active sockets with pending downlink are preserved.
3. Inactive sockets with pending downlink are preserved only when the local TCP
   socket is still send-capable.
4. Inactive and not-send-capable sockets with pending downlink are immediately
   reaped as undeliverable tail, with close diagnostics that expose the smoltcp
   state and `can_send` value.
5. Inactive but still send-capable pending remains bounded by the existing
   progress-sensitive grace.

## Acceptance

Local:

- Focused `should_reap_slot` tests cover send-capable and not-send-capable
  inactive pending.
- `cargo test --lib client_tun`
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `git diff --check`

VPS:

- The next run should recover toward knife14s throughput while still avoiding
  useful pending loss.
- Any remaining `dead_slot_reap ... pending>0` line must include enough socket
  diagnostics to tell whether the local side was still send-capable.
