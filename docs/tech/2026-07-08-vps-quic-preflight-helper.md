# 2026-07-08 VPS QUIC Preflight Helper

## Goal

Make the Knife14fp VPS socket-buffer prerequisite repeatable for production
exit nodes and formal acceptance hosts without storing or printing secrets.

## Non-Goals

- Do not change Rust data-plane behavior.
- Do not tune the Knife14 local downlink final 1%.
- Do not read `.env`, dump sing-box config, print certificates, or print TUIC
  UUIDs/passwords.
- Do not make direct path throughput claims; the helper only checks readiness.

## Design Tree

- Exit sysctl missing or too small: fail before expensive tunnel acceptance.
- Sysctl applied but sing-box not restarted: install mode restarts sing-box so
  the TUIC UDP socket is recreated under the larger kernel buffers.
- Runtime value high but persistence missing: fail exit checks because reboot
  would silently regress the deployment.
- sing-box unhealthy or not listening on UDP `:8443`: fail before blaming
  mini_vpn.
- Target iperf3 inactive: fail the target preflight before tunnel tests.
- Operator diagnostics needed: print service/socket/time facts and a bounded
  redacted log tail, never config or secret material.

## Implementation

Added `scripts/vps-quic-preflight.sh` with these commands:

```bash
sudo bash scripts/vps-quic-preflight.sh check-exit
sudo bash scripts/vps-quic-preflight.sh install-exit
sudo bash scripts/vps-quic-preflight.sh diagnose-exit
bash scripts/vps-quic-preflight.sh check-client
bash scripts/vps-quic-preflight.sh check-target
bash scripts/vps-quic-preflight.sh checklist
bash scripts/vps-quic-preflight.sh --self-test
```

The required Linux QUIC socket-buffer floors are:

```text
net.core.rmem_max >= 16777216
net.core.wmem_max >= 16777216
net.core.rmem_default >= 1048576
net.core.wmem_default >= 1048576
```

`install-exit` writes `/etc/sysctl.d/99-mini-vpn-quic.conf`, applies
`sysctl --system`, and restarts `sing-box` by default. Use
`RESTART_SING_BOX=0` only when staging the change during a maintenance window;
restart sing-box before throughput acceptance.

## Acceptance

- Local syntax/self-test pass:
  `bash -n scripts/vps-quic-preflight.sh` and
  `bash scripts/vps-quic-preflight.sh --self-test`.
- `shellcheck` passes when installed.
- The production guide points operators to the helper before the manual
  sysctl commands and before expensive acceptance suites.
- No command or doc stores TUIC UUIDs/passwords, private keys, `.env` contents,
  or sudo passwords.
