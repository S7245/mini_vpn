# Knife14ad Exit-Target Path Preflight Spec

## Grounding

`/tmp/mini_vpn/mvpn_knife14ad_usclient_suite_20260703_183503.tar.gz` tested
commit `33659c9`. Client-to-target direct baselines were healthy:

- client `.27 -> .77`: 276 Mbit/s receiver
- target `.77 -> .27`: 268 Mbit/s receiver

The tunnel result was mixed:

- fresh reverse-first P1: 22.4 Mbit/s receiver
- standalone forward P1: 125 Mbit/s receiver
- standalone reverse P1 after forward: 15.8 Mbit/s receiver
- full forward P1: 191 Mbit/s receiver
- full reverse P1: 20.8 Mbit/s receiver

The new stream diagnostics worked. Reverse data was not absent at the client:
`tcp-relay-live` showed tens of MB of `remote_to_global_rx_bytes` on the data
flow, and `tcp-global-rx-pressure` did not fire. The current missing baseline is
the path actually used by tunnel reverse: `.77 -> .33` before sing-box/TUIC sends
bytes back to the client.

## Goal

Make the US-client suite able to collect an optional direct `.33 <-> .77`
iperf3 baseline before the tunnel run, so reverse tunnel failures can be
attributed to the Exit-Target path/server versus mini_vpn/TUIC client logic.

## Non-Goals

- Do not change mini_vpn data-plane behavior in this stage.
- Do not require SSH access for every run.
- Do not hard-code SSH users, keys, or provider-specific hostnames.

## Design

Add opt-in env controls:

- `EXIT_TO_TARGET_IPERF_CHECK=1`
- `EXIT_TO_TARGET_IPERF_REQUIRED=1`
- `EXIT_SSH_HOST=ubuntu@43.153.32.33`
- optional `EXIT_SSH_PORT` and `EXIT_SSH_KEY`
- `EXIT_SSH_STRICT_HOST_KEY_CHECKING=accept-new`
- `EXIT_SSH_KNOWN_HOSTS_FILE=$OUT_DIR/exit_ssh_known_hosts`

When enabled, the suite SSHes to the Exit VPS and runs:

```bash
timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1
timeout $DIRECT_IPERF_TIMEOUT iperf3 -c $TARGET -p $IPERF_PORT -t $DIRECT_IPERF_DURATION -P 1 -R
```

When disabled, the report prints those manual commands so the attribution gap is
visible.

The suite usually runs under `sudo -E`, so Exit SSH cannot rely on the invoking
user's `known_hosts`. It uses a suite-local known-hosts file and `accept-new` by
default so first contact is noninteractive while changed host keys still fail.

## Acceptance

- Existing default suite behavior is unchanged.
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh` passes.
- The report records whether Exit-Target checking was enabled, required, and
- which SSH destination and host-key policy were used.
- If enabled and required, SSH/path/iperf failures stop the suite before VPS time
is spent on an ambiguous tunnel run.
