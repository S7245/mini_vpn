# 2026-07-08 Knife14fp Server Socket Buffer Results

## Goal

Test whether the remaining `100+ Mbit/s` reverse-first gap can be solved on
the sing-box server side instead of lowering the target to `30 Mbit/s`.

The specific hypothesis was that the exit VPS had OS UDP socket buffer limits
too low for high-rate TUIC/QUIC downlink. This is separate from sing-box JSON
protocol settings such as `congestion_control`.

## Official Config Notes

The official sing-box docs do not publish a fixed TUIC bandwidth guarantee or a
single "set this to 100M" option.

Relevant documented knobs:

- TUIC inbound exposes listen fields, `congestion_control`, auth/0-RTT/heartbeat
  fields, and shared QUIC fields.
- TUIC outbound exposes the client-side TUIC fields.
- Shared QUIC fields expose `initial_packet_size` and
  `disable_path_mtu_discovery`.
- Listen fields expose `udp_fragment` and UDP timeout behavior, not a TCP
  Connect throughput cap.

References:

- `https://sing-box.sagernet.org/configuration/inbound/tuic/`
- `https://sing-box.sagernet.org/configuration/outbound/tuic/`
- `https://sing-box.sagernet.org/configuration/shared/listen/`
- `https://sing-box.sagernet.org/configuration/shared/quic/`
- `https://sing-box.sagernet.org/configuration/shared/tls/`

The latest official GitHub release observed during the run was still
`v1.13.14`, matching the `.33` server version, so there was no newer stable
server binary to test.

## Server Finding

Before the experiment, `.33` had very small Linux socket buffer caps/defaults:

```text
net.core.rmem_max = 212992
net.core.wmem_max = 212992
net.core.rmem_default = 212992
net.core.wmem_default = 212992
```

The TUIC inbound already had:

- sing-box `1.13.14`
- `congestion_control=bbr`
- `zero_rtt_handshake=true`
- UDP `:8443` active

So the failure was not caused by a conservative server congestion-control
default.

## Socket Buffer A/B

The server was temporarily changed to:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

`sing-box` was restarted after the sysctl change so the TUIC UDP socket could
be recreated under the higher limits.

### Mature sing-box client

Using official sing-box `v1.13.14` on `.27`, with a TUN inbound routing only
`.77/32` through TUIC to `.33`:

| Condition | Receiver |
| --- | ---: |
| Before server socket buffer change, MTU 1500 | `27.751 Mbit/s` |
| After server socket buffer change, MTU 1500 | `185.242 Mbit/s` |
| Same-window direct `.33 -> .77` reverse control | `208.750 Mbit/s` |

Local pulled artifacts:

- `/tmp/mini_vpn/knife14_server_socketbuf_client/iperf3-reverse-30s.json`
- `/tmp/mini_vpn/knife14_server_socketbuf_client/sing-box-client.log`

This is the decisive A/B: the same mature client, same target, and same TUIC
server moved from the `20-30 Mbit/s` band to `185 Mbit/s` after only the exit
VPS socket buffer limit changed.

## mini_vpn Acceptance Run

After keeping the `.33` socket buffers high, mini_vpn was tested with the
existing safe1200 reverse-first P1 path.

Bundle:

- `/tmp/mini_vpn/knife14fp_server_socketbuf_p1_30/mvpn_knife14fp_server_socketbuf_p1_30_usclient_suite_20260708_091606.tar.gz`

Remote bundle:

- `/tmp/conn/mvpn_knife14fp_server_socketbuf_p1_30_usclient_suite_20260708_091606.tar.gz`

Key throughput:

- Reported final receiver: `114.000 Mbit/s`
- Non-zero intervals before the timeout tail: 29 intervals, average
  `189.483 Mbit/s`, min `163 Mbit/s`, max `261 Mbit/s`
- `throughput_shape=stable_high`

Clean surfaces:

- QUIC loss/congestion: `0`
- `max_tx_blocked_data_delta=0`
- `max_tx_blocked_stream_delta=0`
- `max_rx_blocked_data_delta=0`
- `send_slice_zero=0`
- `send_slice_errors=0`
- `tun_flush_failures=0`
- data stream `data_max_read_gap_ms=189`
- data stream `data_pending_gap_max_ms=189`

Remaining dirty close-tail surfaces:

- iperf was killed by the suite timeout after the data phase did not close
  cleanly.
- `terminal_pending_reap=349932`
- `pending_at_close=349932`
- `egress_at_close=449999`
- `tun_tx_dropped_delta=16`
- `max_rx_blocked_stream_delta=1`

## Persistent Server Change

The high-buffer setting was made persistent on `.33`:

```text
/etc/sysctl.d/99-mini-vpn-quic.conf
```

Current persisted values:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

`sing-box` was restarted and remained active after applying the persistent
sysctl file.

## Conclusion

Do not lower the reverse-first target to `30 Mbit/s`. The `100+ Mbit/s` target
is reachable on the current `.27/.33/.77` topology once the exit VPS socket
buffer cap is raised.

The remaining work is no longer "unlock throughput"; it is "make high-rate
close-tail acceptance clean":

- avoid iperf timeout-driven terminal reaping after a successful high-rate data
  phase;
- remove terminal pending at close;
- keep TUN drop delta at zero under the higher sustained rate;
- keep `rx_blocked_stream` at zero.

## Next Work

1. Treat `.33` socket buffer preflight as mandatory for high-throughput TUIC
   acceptance.
2. Add an acceptance preflight or ops note that checks:
   `net.core.rmem_max`, `net.core.wmem_max`, `net.core.rmem_default`, and
   `net.core.wmem_default` on the exit.
3. Add a focused high-throughput close-tail fix/test. This is a lifecycle drain
   problem after the throughput bottleneck has been removed, not a reason to
   lower the throughput target.
