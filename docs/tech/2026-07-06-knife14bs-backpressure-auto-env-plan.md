# Knife14bs Backpressure Auto Env Plan

## Plan

1. Add a suite-side normalization helper.
   - If `MINI_VPN_TCP_TX_BUFFER_BYTES` is larger than `524288` and inherited
     downlink high/low are the old default pair, blank them before launching
     mini_vpn so the binary auto-scaling path is used.
   - Preserve explicit A/B behavior with
     `KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1`.

2. Add self-tests.
   - Legacy `524288/131072` plus tx buffer `1048576` becomes auto.
   - The escape hatch preserves the legacy pair.
   - Non-legacy explicit values are preserved.

3. Make the report explicit.
   - Print a `downlink_backpressure_auto_reset` line when normalization
     happens.
   - Keep the existing config lines rendering blank values as `<auto>`.

4. Run local gates.
   - Bash syntax and suite self-test.
   - Low-RTT parser self-test to protect Knife14br shape attribution.
   - Diff checks.

5. Commit and push the coherent script/doc change.

6. Run one scoped VPS reverse-first P1 acceptance.
   - Source the VPS `.env` for credentials.
   - Use server evidence and exit-target iperf checks.
   - Stop after reverse-first P1.
   - Parse the bundle and compare to Knife14bq.

## Risk Review

- Risk: a deliberate explicit legacy-value A/B could be normalized away.
  - Mitigation: `KNIFE14_KEEP_EXPLICIT_DOWNLINK_BACKPRESSURE=1`.

- Risk: blank exported variables behave differently under `sudo -E env`.
  - Mitigation: current Rust parser already treats empty strings as invalid
    values and falls back to the tx-buffer-scaled default.

- Risk: higher high watermark could increase TUN pressure.
  - Mitigation: the scoped VPS run must inspect TUN drops and
    `downlink_backpressure` churn before declaring progress.
