# Knife14bd suite adaptive backpressure

Date: 2026-07-04

## Problem

Knife14bc changed mini_vpn so runtime default downlink backpressure scales with
the configured TCP tx buffer. Before running VPS acceptance, the suite script
was found to export fixed watermarks:

- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES=524288`
- `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES=131072`

Those explicit env values would override the new binary defaults and make the
VPS run invalid for Knife14bc.

## Fix

`scripts/knife14b-usclient-tunnel-suite.sh` now leaves the backpressure env
values empty by default and reports them as `<auto>`. The binary then applies
its tx-buffer-aware defaults.

Explicit operator values still work:

- if `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES` is set before invoking the
  suite, that value is passed through;
- same for `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES`.

## Verification

- `scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`

## Next

Run the scoped `.27` reverse-first P1 suite from the pushed commit and confirm
startup logs show the adaptive watermarks. With the suite defaults of
`MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`, the expected binary-selected
backpressure is:

- high: `1048576`
- low: `262144`
