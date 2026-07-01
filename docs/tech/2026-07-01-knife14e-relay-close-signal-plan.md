# Knife14e plan — relay close signal and bounded probes

> Spec: `docs/tech/2026-07-01-knife14e-relay-close-signal-spec.md`.

## Task Tree

1. Relay event contract
   - Add a typed relay event for `Data` and `Closed`.
   - Carry `conn_epoch` on both event kinds and ignore stale events in the main loop.
   - Convert the global TCP relay channel to carry that event.
   - Route `Data` through the existing downlink handler.
   - Route `Closed` through a new close handler that re-arms the socket.

2. TDD guard
   - Add a focused test that `run_relay` emits `Closed { direction: "timer", reason: "idle_timeout" }`
     after the relay idle timeout.
   - Add a focused test that `remote_write_failed` emits a close event.
   - Add a focused test that stale close events are dropped by epoch.
   - Add a focused test that matching close events do not clear `downlink_pending` before it drains.
   - Keep the existing shutdown assertion.

3. Probe guard
   - Preserve the suite's default pre-tunnel `.33`/`.77` VPS service preflight.
   - Add `IPERF_TIMEOUT_SECS` to `scripts/knife14b-lowrtt-probe.sh`.
   - Wrap each iperf command in `timeout`.
   - Preserve existing output shape, including `exit=`.

4. Verification
   - Run the focused relay tests.
   - Run `cargo test --lib client_tun`.
   - Run `bash -n scripts/knife14b-lowrtt-probe.sh scripts/knife14b-usclient-tunnel-suite.sh`.
   - Review the diff for lifecycle regressions and diagnostic clarity.

5. Commit
   - Commit as a single knife14e stage if the tests pass.
