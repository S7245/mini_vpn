# Knife14as plan - terminal pending lifecycle accounting

Date: 2026-07-04

## Stage Goal

Make terminal pending bytes first-class evidence so the next reverse-first VPS
run can separate expected post-close cleanup from premature local close or
receive-window behavior.

## Tasks

1. Document the Knife14as design tree, invariants, and TDD acceptance.
2. Add a small Rust classifier for terminal pending:
   `downlink_pending > 0 && tcp_state == Closed && !active && !can_send`.
3. Ensure every `rearm_socket_with_reason` path records a pre-abort socket
   snapshot in the TCP close diagnostic.
4. Add `terminal_pending_reap_bytes` to TCP close diagnostics.
5. Parse terminal pending events/bytes/max in `knife14b-lowrtt-probe.sh`.
6. Surface the parser summary in the parent US-client suite report.
7. Run local gates and review the diff before any VPS run.
8. If local gates pass, commit and push the behavior-neutral accounting change.
9. Run one scoped reverse-first VPS acceptance with preflight checks.
10. Record the stage learning in `.learnings/LEARNINGS.md`, and record any
    failed command that changes future behavior in `.learnings/ERRORS.md`.

## TDD Notes

- Rust tests should prove active/send-capable pending is not terminal, while
  inactive `Closed && !can_send` pending is counted exactly.
- Shell self-test should include a synthetic `tcp-handle-close` line with
  terminal pending bytes and assert the summary line.
- The parser should keep backward compatibility with old bundles where the
  explicit field is absent.

## VPS Checklist

Before the expensive run:

- `.33`: `sudo systemctl status sing-box --no-pager`
- `.77`: `systemctl status iperf3 --no-pager`
- direct `.27 -> .77` forward and reverse iperf baselines
- direct `.33 -> .77` forward and reverse iperf baselines
- confirm `.27` checkout commit and release binary checksum in the suite report

Primary acceptance window:

- reverse-first P1 through the tunnel;
- `MINI_VPN_TCP_DIAG=1`;
- `terminal_pending_reap` present in the summary;
- clean-window QUIC, TUN, downlink, late-remote, and sender-side labels read
  together rather than treating terminal pending alone as the root.
