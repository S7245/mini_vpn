# Knife14bi Drain Default-Off Results

Date: 2026-07-05

## Code Under Test

- Local behavior commit: `76af8dc`
- `.27` repo commit during VPS attempt: `76af8dc`
- Bundle:
  `/tmp/mini_vpn/mvpn_knife14bi_default_usclient_suite_20260705_111824.tar.gz`
- Extracted locally:
  `/tmp/mini_vpn/knife14bi_default_20260705_111824/`

## Local Gates

Passed before the behavior commit:

- `cargo test --lib parse_tun_rx_drain_budget_allows_zero_and_bounds`
- `cargo test --lib tun_runtime_config_defaults_match_stage9_behavior`
- `bash scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash scripts/knife14b-lowrtt-probe.sh --self-test`
- `cargo test --lib client_tun`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `git diff --check`
- `cargo clippy --all-targets --features harness -- -D warnings`
- non-sandbox `cargo test --lib`
- non-sandbox `cargo test --features harness --lib`

## VPS Attempt

The default scoped reverse-first P1 suite did not reach the throughput probe.
`client-tun` failed during TUIC startup:

```text
连接 TUIC 出口失败（启动中止）: Io(Custom { kind: Other, error: "tuic auth finish: sending stopped by peer: error 0" })
```

Evidence collected:

- `.27` pulled and built `76af8dc` with a clean git status.
- Suite env showed `MINI_VPN_TUN_RX_DRAIN_BUDGET=0`.
- `client-tun` startup log confirmed
  `TUN RX drain budget: 0 packets/pass`.
- `.27 -> .77` direct baselines were healthy:
  - forward receiver about `279 Mbit/s`;
  - reverse receiver about `280 Mbit/s`.
- `.33 -> .77` exit-to-target baselines were healthy:
  - forward receiver about `279 Mbit/s`;
  - reverse receiver about `286 Mbit/s`.
- `.33` sing-box was active, had `NRestarts=0`, passed config check, and was
  listening on UDP `8443`.
- `.33` TUIC config summary matched expected shape:
  listen `:::8443`, one user, UUID length `36`, password length `16`,
  ALPN `h3`, server name `example.com`.
- `.33` server certificate was valid for the current date and issued by
  `Dev VPN CA`; `.27` had the corresponding CA file.
- A no-secret exact comparison reported:
  `uuid_match=1`, `password_match=1`, `sni_match=1`, `alpn_match=1`.

## Result

This bundle is not throughput evidence. It only proves the Knife14bi default
config reached startup with drain disabled.

The failed startup matches the earlier Knife14bf TUIC auth-finish environment
failure shape: sing-box is active and credentials match, but the client is
closed by the peer before a useful TUIC data-plane session is established.

## Next Plan

Do not change mini_vpn data-plane code for this failure.

1. Restart `.33` sing-box once.
2. Rerun the same Knife14bi default scoped suite without setting
   `MINI_VPN_TUN_RX_DRAIN_BUDGET`.
3. If startup succeeds, parse whether the default path now matches the
   Knife14bh drain0 shape.
4. If startup fails again with the same auth-finish error after restart and
   exact config match, stop and re-evaluate TUIC startup compatibility before
   spending more throughput runs.
