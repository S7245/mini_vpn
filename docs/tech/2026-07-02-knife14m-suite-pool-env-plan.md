# Knife14m plan - suite pool env propagation

Date: 2026-07-02

## Task

Patch only the US-client suite script:

- Document `MINI_VPN_TUIC_TCP_POOL`.
- Export a default of `1`.
- Append its value to the report.
- Pass it into `sudo -E env` when launching `client-tun`.

## Verification

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`

## Code Review Checklist

- No credentials are printed.
- Default behavior stays pool size `1`.
- The launch command in the report matches the actual env passed to `mini_vpn`.
