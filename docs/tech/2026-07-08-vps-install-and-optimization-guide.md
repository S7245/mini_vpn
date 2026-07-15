# mini_vpn VPS Install And Optimization Guide

Date: 2026-07-08

This guide captures the VPS-side setup needed for mini_vpn's TUIC/QUIC data
plane. It is written for production exit nodes and for the current
Client/Exit/Target acceptance topology.

## Answer For Production

Yes: any production VPS that acts as a high-throughput TUIC exit should apply
the same Linux socket-buffer optimization before it is accepted for `100+
Mbit/s` service.

Knife14fp proved that an otherwise healthy sing-box TUIC exit with the Linux
defaults below can cap both mini_vpn and an official sing-box client in the
`20-30 Mbit/s` band:

```text
net.core.rmem_max = 212992
net.core.wmem_max = 212992
net.core.rmem_default = 212992
net.core.wmem_default = 212992
```

After raising only the exit VPS socket buffers and restarting sing-box, the
same mature sing-box client reached `185.242 Mbit/s` receiver and mini_vpn
safe1200 reverse-first P1 reached a reported `114.000 Mbit/s` receiver with
stable high intervals. A later same-topology mature sing-box repeat still
reached `173/173 Mbit/s`, confirming that the current VPS configuration can
cross the `100+ Mbit/s` target.

This optimization is necessary, not a standalone bandwidth guarantee. A later
clean-repeat run exited normally and cleaned close-tail accounting, but fell
back below target with mini_vpn-local pressure/headroom or stream-read
starvation. Treat the socket-buffer preflight as mandatory before blaming
mini_vpn credit control, TUIC pool size, MTU/PLPMTUD, iperf3, or the VPS path;
then use a mature-client A/B to distinguish VPS capacity from mini_vpn
client/data-plane behavior.

## Roles

Use generic roles for production:

| Role | Purpose | Production notes |
| --- | --- | --- |
| Client | Runs mini_vpn client or acceptance suite | Future desktop/mobile clients may not allow sysctl tuning; Linux test clients should still verify UDP socket buffers. |
| Exit | Runs sing-box TUIC inbound | This is the mandatory socket-buffer optimization point. |
| Target | Runs iperf3 or real destination services | Used only for acceptance and path baselines. |

## sing-box Baseline

Install sing-box from the official project or package source appropriate for
the VPS OS. For Knife14fp, `.33` used sing-box `v1.13.14`, which matched the
latest GitHub release observed during the run.

Official references:

- TUIC inbound: `https://sing-box.sagernet.org/configuration/inbound/tuic/`
- TUIC outbound: `https://sing-box.sagernet.org/configuration/outbound/tuic/`
- Listen fields: `https://sing-box.sagernet.org/configuration/shared/listen/`
- QUIC fields: `https://sing-box.sagernet.org/configuration/shared/quic/`
- TLS fields: `https://sing-box.sagernet.org/configuration/shared/tls/`
- Releases: `https://github.com/SagerNet/sing-box/releases`

The official sing-box docs expose TUIC fields such as `congestion_control`,
`zero_rtt_handshake`, `heartbeat`, TLS, listen fields, and shared QUIC
`initial_packet_size` / `disable_path_mtu_discovery`. They do not publish a
fixed bandwidth guarantee or a single JSON option that means "make this
100M+". Bandwidth acceptance still depends on OS socket buffers, VPS network,
RTT, MTU, and end-to-end path quality.

Use placeholders in config and deployment notes. Do not commit real UUIDs,
passwords, private keys, certificates, or `.env` content.

Minimal TUIC inbound shape:

```jsonc
{
  "inbounds": [
    {
      "type": "tuic",
      "tag": "tuic-in",
      "listen": "::",
      "listen_port": 8443,
      "users": [
        {
          "name": "<USER_NAME>",
          "uuid": "<TUIC_UUID>",
          "password": "<TUIC_PASSWORD>"
        }
      ],
      "congestion_control": "bbr",
      "zero_rtt_handshake": true,
      "heartbeat": "10s",
      "tls": {
        "enabled": true,
        "server_name": "<SNI>",
        "certificate_path": "/etc/sing-box/cert.pem",
        "key_path": "/etc/sing-box/key.pem",
        "alpn": ["h3"]
      }
    }
  ],
  "outbounds": [
    { "type": "direct", "tag": "direct" }
  ]
}
```

The exact `congestion_control` choice should be measured on the path. The
Knife14fp unlock was not caused by changing this field; the server already had
an explicit TUIC congestion-control value before the socket-buffer fix.

## Mandatory Exit Socket Buffers

Apply this on every Linux VPS that runs the TUIC exit:

```bash
sudo install -d -m 0755 /etc/sysctl.d
sudo tee /etc/sysctl.d/99-mini-vpn-quic.conf >/dev/null <<'EOF'
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
EOF
sudo sysctl --system
sudo systemctl restart sing-box
```

Why restart sing-box: the TUIC UDP socket must be recreated after the kernel
caps/defaults change. A sysctl update alone does not prove the already-open
socket inherited the larger buffers.

Verify:

```bash
sysctl -n net.core.rmem_max
sysctl -n net.core.wmem_max
sysctl -n net.core.rmem_default
sysctl -n net.core.wmem_default
systemctl is-active sing-box
sudo ss -lunp | grep ':8443'
```

Expected values:

```text
16777216
16777216
1048576
1048576
active
```

For a Linux client/test VPS, verify that mini_vpn can obtain large UDP socket
buffers. A healthy Knife14fp client logged:

```text
QUIC UDP socket buffers: requested=8388608B recv=16777216B send=16777216B
```

If a Linux test client reports tiny receive/send buffers, apply the same
sysctl values there as well. For future mobile/desktop apps, this may not be
available; the exit-side requirement remains the first production gate.

## Firewall And Security Group

For TUIC-only exits:

- Allow inbound UDP on the TUIC listen port, for example `8443`.
- Allow SSH only from trusted admin IPs.
- Allow outbound TCP/UDP to the public Internet.
- Do not expose iperf3 on production exits unless it is a temporary acceptance
  target protected by security groups.

For REALITY fallback exits, also allow the configured TCP REALITY port. Keep
TUIC and REALITY credentials separate and rotate them outside the repository.

## Preflight Checklist

Run this before every expensive throughput suite:

```bash
sing-box version
sudo sing-box check -c /etc/sing-box/config.json
systemctl is-active sing-box
sysctl -n net.core.rmem_max net.core.wmem_max net.core.rmem_default net.core.wmem_default
sudo ss -lunp | grep ':8443'
timedatectl show -p NTPSynchronized -p SystemClockSynchronized
```

Target host:

```bash
systemctl is-active iperf3
ss -ltnp | grep ':5201'
```

Knife15 macOS receiver-quality evidence also requires structured server
output. Use a systemd drop-in on the dedicated acceptance Target:

```bash
sudo mkdir -p /etc/systemd/system/iperf3.service.d
sudo tee /etc/systemd/system/iperf3.service.d/20-mini-vpn-json-output.conf >/dev/null <<'EOF'
[Service]
ExecStart=
ExecStart=/usr/bin/iperf3 -s --json --forceflush
EOF
sudo systemctl daemon-reload
sudo systemctl restart iperf3
systemctl is-active iperf3
ss -ltnp | grep ':5201'
```

Qualify the capability from a client with
`iperf3 ... --json --get-server-output` and require
`.server_output_json` to be an object with positive receiver intervals. To
roll this Target-only evidence change back:

```bash
sudo rm -f /etc/systemd/system/iperf3.service.d/20-mini-vpn-json-output.conf
sudo systemctl daemon-reload
sudo systemctl restart iperf3
```

Direct path baseline from the exit to the target must be higher than the tunnel
goal before mini_vpn is blamed:

```bash
iperf3 -c <TARGET_IP> -p 5201 -t 30 -P 1
iperf3 -c <TARGET_IP> -p 5201 -t 30 -P 1 -R
```

For a `100+ Mbit/s` tunnel goal, reject or investigate the VPS/path if the
exit-to-target direct reverse baseline is not comfortably above `100 Mbit/s`.

## mini_vpn Acceptance Recipe

On the Linux Client VPS, keep credentials in a local `.env` file that is not
committed and is not copied into reports. Load it without printing it:

```bash
cd /home/ubuntu/mini_vpn
. "$HOME/.cargo/env"
set -a
. ./.env
set +a
```

Focused reverse-first P1 run after the socket-buffer preflight:

```bash
env \
  SUITE_TAG=prod_socketbuf_reverse_p1 \
  TARGET=<TARGET_IP> \
  OUT_DIR=/tmp/conn \
  DURATION=30 \
  IPERF_TIMEOUT_SECS=120 \
  POST_IPERF_METRICS_SETTLE_SECS=5 \
  MTU=1200 \
  RUN_REVERSE_FIRST_P1=1 \
  STOP_AFTER_REVERSE_FIRST_P1=1 \
  BUILD_RELEASE=1 \
  KILL_OLD=1 \
  KEEP_TUNNEL=0 \
  METRICS_SECS=5 \
  CHECK_VPS_SERVICES=1 \
  DIRECT_IPERF_REVERSE_CHECK=1 \
  EXIT_TO_TARGET_IPERF_CHECK=1 \
  SERVER_EVIDENCE_CHECK=1 \
  MINI_VPN_TUIC_MTU_POLICY=safe1200 \
  MINI_VPN_TUIC_MTU_MODE=safe1200 \
  MINI_VPN_TCP_DIAG=1 \
  bash scripts/knife14b-usclient-tunnel-suite.sh
```

When running over SSH and the suite needs sudo, start the SSH command with a
TTY and type the sudo password only at the prompt. Never place sudo passwords
or TUIC credentials in commands, scripts, docs, bundles, or final summaries.

## Acceptance Criteria

For the current Knife14 TCP reverse-first gate:

- receiver `>=100 Mbit/s`
- `throughput_shape=stable_high`
- direct exit-to-target baseline healthy in the same window
- QUIC loss/congestion/blocked deltas `0`
- `pending_at_close=0`
- `terminal_pending_reap=0`
- `tun_tx_dropped_delta=0`
- `rx_blocked_stream=0`
- no current sing-box TUIC auth failure

If throughput is high but close-tail metrics are dirty, keep the high-throughput
unlock as accepted and isolate close-tail lifecycle separately. Do not lower
the target to `30 Mbit/s` because of a post-data timeout tail.

For production readiness, require at least one repeat where the iperf command
exits normally and the close-tail metrics are also clean. A run killed by an
external timeout is useful evidence, but not final acceptance.

## Troubleshooting

| Symptom | First checks | Likely direction |
| --- | --- | --- |
| Mature sing-box client and mini_vpn both stay `20-30 Mbit/s` | Exit `rmem_*` / `wmem_*`, sing-box restart time, direct exit-to-target iperf | Exit socket buffers or VPS path |
| mini_vpn low but mature sing-box high | mini_vpn logs: read gaps, pending, TUN drops, QUIC blocked/loss | mini_vpn client/data-plane |
| Direct exit-to-target is low | iperf3 service, VPS provider path, security group, CPU steal | Not mini_vpn |
| TUIC auth fails | no-secret config match, time sync, sing-box logs | Config/time/service |
| QUIC blocked/loss appears | MTU policy, PLPMTUD, path loss, UDP firewall | Transport/path |
| High throughput but timeout tail dirty | Increase iperf timeout/settle, inspect close lifecycle | Acceptance close-tail, not throughput unlock |

## Rollback

If the socket-buffer change must be rolled back:

```bash
sudo rm -f /etc/sysctl.d/99-mini-vpn-quic.conf
sudo sysctl --system
sudo systemctl restart sing-box
```

Then rerun the direct and tunnel baselines. Expect high-throughput TUIC
acceptance to regress on paths similar to Knife14fp.

## Current Evidence

- Knife14en safe1200 rejected PLPMTUD/MTU as the remaining root while local
  downlink pressure was still visible:
  `docs/tech/2026-07-07-knife14en-quic-safe1200-results.md`.
- Knife14fi-fo rejected further local credit/MTU/pool tuning as the final 100M
  root and used a mature sing-box client A/B:
  `docs/tech/2026-07-08-knife14fi-fo-downlink-credit-stream-gap-results.md`.
- Knife14fp proved exit-side Linux socket buffers unlock the target:
  `docs/tech/2026-07-08-knife14fp-server-socket-buffer-results.md`.
- Knife14fq showed that longer timeout cleans the close tail but does not yet
  provide stable final acceptance:
  `docs/tech/2026-07-08-knife14fq-timeout120-clean-tail-regression-results.md`.
- Knife14fu/fw/fx proved the current VPS configuration still reaches
  `173/173 Mbit/s` with a mature sing-box client while mini_vpn remains low:
  `docs/tech/2026-07-08-knife14fu-fw-fx-reverse-discriminator-results.md`.
