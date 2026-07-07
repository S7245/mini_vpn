# Knife14eo Local Downlink Credit Controller Plan

Date: 2026-07-07

## Stage Plan

1. Add focused unit tests for a per-flow controller:
   - headroom deferral without egress progress shrinks read credit and staging;
   - observed egress progress grows read credit additively;
   - staging limit prevents pending growth while still allowing a small QUIC
     drain allowance;
   - headroom deferral compresses the next flush budget.
2. Implement the controller inside `SocketCtx`, keeping the public relay
   message contract unchanged.
3. Wire the controller into:
   - `flush_downlink`, where observed egress progress and headroom deferral are
     known;
   - relay read-credit publishing;
   - projected payload credit publishing before payload handling.
4. Run local focused gates, then full lib and script gates.
5. Sync to `.27`, run focused remote gates and release build.
6. Run one reverse-first P1 VPS acceptance with the same safe1200 evidence
   shape only if local gates pass.
7. Pull and parse the bundle, record the result doc and learning memory, then
   commit/push the coherent task.

## Failure Handling

If a local or VPS gate fails, stop and parse the failing signal before editing
again. Do not switch to pool, sing-box, iperf3, MTU, or TUN queue tuning from
this stage unless the new evidence directly points there.
