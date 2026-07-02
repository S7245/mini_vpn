# Knife14v Spec: Defer Local Finish While Reverse Downlink Is Active

## Grounding

`a57873a` was tested with
`/tmp/mvpn_knife14c_usclient_suite_20260702_223847.tar.gz`.

The run proved knife14u did what it was meant to do:

- The report records `repo_commit: a57873a`.
- VPS preflight passed, and direct `.77:5201` iperf reached 278 Mbit/s receiver.
- The remaining `dead_slot_reap pending>0` line includes
  `tcp_state=Closed active=false can_send=false can_recv=false`, so it is now an
  undeliverable local tail rather than a potentially useful pending buffer.

Forward P1 recovered to 13.6 Mbit/s receiver, but reverse is still broken:

- Standalone reverse P1: 26.7 Kbit/s receiver.
- Full reverse P1: 32.7 Kbit/s receiver.
- Full reverse P2/P4/P8: timeout or zero throughput.

The accept log shows the same lifecycle pattern during reverse:

```text
tcp-relay-write-half-closed ... reason=local_finish
tcp-relay-close ... reason=half_closed_idle_timeout remote_to_global_rx_bytes=<small>
```

## Problem

Knife14j/n made `CloseWait` propagate a `Finish` command immediately. That is
bounded and correct for cleanup, but too aggressive for reverse traffic in this
TUIC path. Sending the upstream write-half shutdown as soon as the local side has
no more payload appears to make the exit/target side stop after a tiny reverse
burst, leaving the relay parked until half-closed idle timeout.

For reverse-heavy flows, `CloseWait` means "no more local payload" but the remote
read half can still be the main data direction. The data plane should not close
the upstream write half until remote-to-local traffic has gone quiet.

## Required Behavior

1. When an established flow enters `CloseWait` with no local payload, record a
   pending local finish but do not immediately send `RelayCommand::Finish`.
2. While remote payloads continue to arrive, refresh the pending-finish deadline.
3. Once pending local finish has seen no remote progress for a short bounded
   window, send `RelayCommand::Finish` exactly once.
4. Existing cleanup remains bounded: after `Finish`, the relay still uses the
   existing half-closed idle timeout.
5. Existing forward/uplink backpressure semantics are unchanged.

## Acceptance

Local:

- Unit tests prove initial `CloseWait` defers `Finish`.
- Unit tests prove remote progress extends the defer window.
- Existing local-finish, relay close, reap, and downlink tests pass.
- `cargo test --lib client_tun`
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `git diff --check`

VPS:

- Reverse P1 should no longer collapse to Kbit/s scale immediately after
  `local_finish`.
- `tcp-relay-write-half-closed ... local_finish` should appear after remote
  quiet, not at the start of an active reverse transfer.
