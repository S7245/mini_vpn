# Knife14m spec - pass TUIC TCP pool env through the suite

Date: 2026-07-02

## Grounding

The `4097fe8` live bundle at `/tmp/mvpn_knife14l_usclient_suite_20260702_110654.tar.gz` completed cleanly and
showed a large forward multi-flow improvement, but it did not contain the startup line:

```text
TUIC TCP connection pool=4
```

Reviewing `scripts/knife14b-usclient-tunnel-suite.sh` showed why: the suite reports and starts `client-tun` with an
explicit `sudo -E env ...` command, but that command did not include `MINI_VPN_TUIC_TCP_POOL`. The Rust side remains
fine; the live experiment did not actually prove pool behavior.

## Scope

- Add `MINI_VPN_TUIC_TCP_POOL` to the suite optional env documentation.
- Default it to `1` in the suite, matching Rust.
- Include it in the report.
- Pass it explicitly to `sudo -E env ... mini_vpn client-tun`.

## Acceptance

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`
- Next live report must show both:
  - `MINI_VPN_TUIC_TCP_POOL=4` in the suite env section / command line.
  - `TUIC TCP connection pool=4` in the mini_vpn startup log.
