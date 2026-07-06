# Knife14bt Core Backpressure Auto Default Plan

## Plan

1. Add a focused parser test.
   - `None/None` high/low with `tx_bytes=1048576` must return the
     conservative default, not `1048576/262144`.
   - Explicit high/low values must remain honored.

2. Change the core auto default.
   - Stop deriving downlink backpressure high/low from TCP tx buffer size.
   - Keep the existing env parser and startup log behavior.

3. Run local gates.
   - Focused Rust parser test first.
   - Focused `client_tun` test subset around downlink backpressure/TUN egress.
   - `git diff --check`.

4. Review operational risk.
   - Confirm the change only affects unset/invalid downlink backpressure env.
   - Confirm explicit A/B env settings still override the default.

5. Update learning memory and commit/push.

6. Run one scoped VPS acceptance if local gates pass.
   - Use the existing suite normalization so stale `.env` legacy values become
     `<auto>`.
   - Verify startup high/low from the mini_vpn log before interpreting
     throughput.
   - Parse the bundle and compare Knife14bs low-average versus the restored
     conservative default branch.

## Risk Review

- Risk: reverting auto high/low can restore the Knife14bq tail-collapse shape.
  - Mitigation: this is expected and is still better than the Knife14bs
    low-average branch; durable TUN egress pressure remains the next slice.

- Risk: operators lose easy large-window A/B behavior.
  - Mitigation: explicit `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES` and
    `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES` still override defaults.

- Risk: the suite report can show `<auto>` while the effective value is no
  longer tx-buffer-scaled.
  - Mitigation: acceptance must always check the startup high/low log line.
