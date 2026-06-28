# 刀14b low-RTT fat-path probe — preflight record

> 日期：2026-06-28 ｜ 状态：**INCONCLUSIVE / not run**。
> 原因：本机当前没有运行中的 `mini_vpn client-tun`，当前 shell 也没有 `MINI_VPN_TUIC_*` 凭据；无法满足
> `2026-06-28-knife14b-lowrtt-cc-pool-quantify-spec.md` 的「确认真进隧道」红线。

## What was checked

- Branch/state: `main` after `536caff docs: add post-14 future task backlog`.
- Tools present:
  - `iperf3`: present (`/opt/homebrew/bin/iperf3`)
  - `curl`: present (`/usr/bin/curl`)
  - `dig`: present (`/usr/bin/dig`)
  - release binary: present (`target/release/mini_vpn`)
- Tunnel process:
  - `pgrep -fl 'mini_vpn client-tun|target/release/mini_vpn|target/debug/mini_vpn'` returned no process.
- Credentials/env:
  - no `MINI_VPN_TUIC_*` variables were present in the current shell.
- Log:
  - `/tmp/mvpn_accept.log` exists but is stale relative to the current process state; without a running client it cannot be used as
    proof that traffic is entering the tunnel.

## Decision

Do **not** run iperf yet. Any `iperf3` result without a live tunnel would measure the host's direct path, not `mini_vpn`.

## How to complete the run

When a low RTT, low-loss, genuinely >100M path and target are available:

```bash
cargo build --release
export MINI_VPN_TUIC_SERVER=<VPS_IP>:8443
export MINI_VPN_TUIC_UUID=<uuid>
export MINI_VPN_TUIC_PASSWORD=<password>
export MINI_VPN_TUIC_SNI=example.com
export MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem
export MINI_VPN_TUIC_ALPN=h3

sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 \
  bash scripts/knife35-acceptance.sh soak

curl ipinfo.io
dig example.com +short
grep -E '📊 数据面|🔬 主循环' /tmp/mvpn_accept.log | tail

scripts/knife14b-lowrtt-probe.sh <iperf-target> [port]
```

Only create a `knife14b-lowrtt-results` document after the gold checks pass and the probe actually runs.
