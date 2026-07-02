# Knife14u Plan: Reap Undeliverable Pending Downlink

## Tasks

1. Add TDD coverage for inactive pending split by local send capability.
2. Thread `TcpSocket::can_send()` into `should_reap_slot`.
3. Preserve pending grace only for active or still send-capable sockets.
4. Immediately reap inactive pending when the local TCP socket cannot send.
5. Add close-log socket diagnostics for `dead_slot_reap`.
6. Run local acceptance and record the stage learning.

## Review Checklist

- The new predicate must not change idle `Listening` behavior.
- Active `CloseWait` with pending downlink must still be preserved.
- Post-finish `CloseWait` with no pending must still wait for relay close.
- The grace window must stay bounded; no branch should keep inactive pending
  alive forever.
- Diagnostics should be high-signal and limited to lifecycle close logs.
