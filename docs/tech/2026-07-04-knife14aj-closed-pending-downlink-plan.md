# Knife14aj plan - closed pending downlink and TUN egress pressure

> Spec:
> `docs/tech/2026-07-04-knife14aj-closed-pending-downlink-spec.md`.

## T1 - Lifecycle guard tests

- Add focused `should_reap_slot` tests for the observed branch:
  `pending>0`, `active=false`, `can_send=false`, `tcp_state=Closed`.
- Assert that no-progress inactive pending remains immediately reapable.
- Assert that recently progressed inactive closed pending is still immediately
  reapable when `can_send=false`.
- Assert that recently progressed inactive pending still gets bounded grace when
  `can_send=true`.

Expected result: the current knife14u predicate should already satisfy the
closed/not-send-capable guard. If it does not, stop and review before changing
data-plane code.

## T2 - TUN drop attribution red self-test

- Add parser self-test coverage using embedded `ip -s link show` samples.
- Assert the summary can compute:
  - `tun_rx_dropped_delta`;
  - `tun_tx_dropped_delta`;
  - the interface name used.

Expected initial result: the new TUN drop parser/summary assertion fails before
script implementation.

## T3 - TUN drop attribution implementation

- Extend the low-RTT probe and parent suite summary to capture pre/post
  `ip -s link show tun0` counters around each scoped probe.
- Print the TUN drop deltas in each attribution summary.
- Make the attribution label prefer explicit TUN egress loss when
  `tun_tx_dropped_delta > 0`.
- Do not alter reap predicate behavior in this task.

## T4 - Local verification and stage review

- Run:
  - `cargo test --lib client_tun`;
  - focused shell syntax checks for the touched scripts;
  - low-RTT probe self-test if the parser gains a self-test case;
  - `git diff --check`.
- Review for:
  - no unbounded pending or grace;
  - no regression to knife14t stale-flow behavior;
  - no secret exposure in docs, logs, scripts, or learning memory;
  - no broad tuning hidden inside lifecycle work.

## T5 - Stage learning and commit

- Record the outcome in `.learnings/LEARNINGS.md`.
- Record any failed command, misleading assumption, or rejected branch in
  `.learnings/ERRORS.md`.
- Commit the coherent task after local review passes.

## T6 - VPS checklist after local pass

- On `.33`, check `sudo systemctl status sing-box` and recent
  `/var/log/sing-box.log`.
- On `.77`, check `systemctl status iperf3` and recent `journalctl -u iperf3`.
- On `.27`, confirm branch/head and run a pool=4 startup smoke before the full
  scoped probe.
- Run the reverse-first P1 suite with:
  - `MINI_VPN_TUIC_TCP_POOL=4`;
  - `MINI_VPN_TUIC_CC=bbr`;
  - `EXIT_TO_TARGET_IPERF_CHECK=1`;
  - direct exit `.33 -> .77` preflight enabled;
  - a slightly longer quiet timeout only to let final metrics flush, not as a
    throughput fix.
- Compare against the post-restart bundle:
  `/tmp/mini_vpn/mvpn_knife14ai_pool4_after_singbox_restart_usclient_suite_20260704_091224.tar.gz`.
