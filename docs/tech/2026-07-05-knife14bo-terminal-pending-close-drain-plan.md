# Knife14bo Terminal Pending Close-Drain Plan

Spec: `docs/tech/2026-07-05-knife14bo-terminal-pending-close-drain-spec.md`

## Design Tree

1. Pre-terminal backlog fails to drain.
   - Evidence would be large `downlink_pending` before the local socket reaches
     `Closed`, with no terminal-late remote bytes.
   - This remains a close-drain/backpressure issue.
2. Terminal-late remote payload grows pending after local close.
   - Evidence would be remote payload events after `Closed && !can_send`.
   - This is a lifecycle bug: the payload is already undeliverable and must not
     be hidden as generic pending.
3. Local FIN read-only branch is still valid.
   - Evidence is `CloseWait` or active/send-capable socket after local FIN.
   - This branch must keep accepting remote payload.

## Tasks

1. Add terminal payload gate tests.
   - Keep the existing local-FIN read-only test green.
   - Add a terminal no-send test that rejects payload and counts it.
2. Add close accounting tests.
   - `terminal_pending_reap_bytes` remains app-owned pending at reap.
   - new terminal-late bytes are reported separately.
3. Implement the smallest code change.
   - Gate `handle_remote_payload` using `SocketCloseSnapshot` plus relay state.
   - Count terminal-late remote bytes in `TcpDownlinkDiag`.
   - Include the new counter in `tcp-handle-close` diagnostics and aggregate
     diagnostics if needed for parser visibility.
4. Run focused local tests.
   - Close/reap/accounting tests in `src/client_tun.rs`.
   - Parser self-test only if parser-visible log grammar changes require it.
5. Review and commit.
   - Keep this as one behavior commit if tests and implementation are coherent.
   - Update `.learnings/LEARNINGS.md` and `.learnings/ERRORS.md`.
6. Run one VPS acceptance.
   - Preflight `.33` sing-box and `.77` iperf3.
   - Use `.27` `.env` through the existing suite path.
   - Parse bundle and record results before any next patch.

## Stop Conditions

- If local tests show no safe terminal payload gate, stop and reassess the
  close-drain architecture.
- If VPS still has large terminal pending but terminal-late bytes stay zero,
  the next branch is pre-terminal drain/receive-window behavior, not terminal
  payload gating.
- If `.33` reports current TUIC fail-auth, inspect sing-box time/config/service
  state before treating throughput data as valid.
