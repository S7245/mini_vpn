# Knife14bn A/B Evidence Results

Date: 2026-07-05

## Code Under Test

- Baseline commit: `09bb67c`
  (`fix(knife14bl): gate immediate downlink flush on pressure`)
- Comparison commit: `3d06bea`
  (`fix(knife14bm): latch recent pressure for tun drops`)
- Client VPS: `.27` (`43.172.75.27`)
- Exit VPS: `.33` (`43.153.32.33`)
- Target VPS: `.77` (`43.130.32.77`)
- Local bundle directory:
  `/tmp/mini_vpn/knife14bn_ab_20260705`

Valid bundles:

- `09bb67c`:
  `/tmp/mini_vpn/knife14bn_ab_20260705/mvpn_knife14bn_ab_09bb67c_usclient_suite_20260705_174746.tar.gz`
- `3d06bea`:
  `/tmp/mini_vpn/knife14bn_ab_20260705/mvpn_knife14bn_ab_3d06bea_usclient_suite_20260705_175004.tar.gz`

Invalid bundle:

- `/tmp/conn/mvpn_knife14bn_ab_09bb67c_usclient_suite_20260705_174716.tar.gz`
  failed before throughput because the run command did not source `.env`, so
  TUIC env vars were missing. It is not throughput evidence.

## Preflight

- `.27`, `.33`, and `.77` reported NTP synchronized.
- `.33` sing-box was active.
- `.77` iperf3 was active.
- Both valid suite runs loaded TUIC env from `.27` `.env` and redacted secret
  values in reports.
- Neither valid server evidence artifact contained a current TUIC fail-auth
  signal. The captured `.33` log still included unrelated VLESS/REALITY scan
  noise.

## A/B Summary

| Signal | `09bb67c` | `3d06bea` |
| --- | ---: | ---: |
| Direct `.27 -> .77` reverse | `297/267 Mbit/s` | `311/282 Mbit/s` |
| Direct `.33 -> .77` reverse | `312/284 Mbit/s` | `306/283 Mbit/s` |
| Tunnel reverse P1 | `0.210/0.021 Mbit/s` | `19.3/18.2 Mbit/s` |
| Target `.77` sender | `768 KiB / 210 Kbit/s` | `68.9 MiB / 19.3 Mbit/s` |
| Data first RX | `3ms` | `3ms` |
| Data read gap max | `20525ms` | `4608ms` |
| Data RX bytes max | `128504B` | `69240907B` |
| Downlink backpressure | `0/0` | `6/6` |
| TUN tx drops | `0` | `1272` |
| TUN feedback | `0/0`, drops `0` | `1/1`, drops `1272` |
| Terminal pending reap | `46958B` | `809715B` |
| QUIC loss/congestion | `0/0` | `0/0` |

## Classification

Knife14bn does not support the hypothesis that `3d06bea` introduced the
Knife14bm no-data-stream regression.

Evidence:

- `09bb67c`, the Knife14bl code that previously reached about `130 Mbit/s`,
  also produced a low-byte quiet-egress run in the same VPS window.
- `3d06bea` did not repeat the exact no-data-stream shape. It moved back to a
  TUN/downlink pressure shape with `19.3/18.2 Mbit/s`.
- `.77` target sender throughput matched the tunnel result in both runs, so
  neither run proves mini_vpn silently lost a high-rate target sender stream.
- `.33` showed current TUIC inbound and direct outbound opens and no current
  fail-auth evidence.
- QUIC loss and congestion remained zero in both runs.

The best classification is mixed run variance plus still-active local
downlink/TUN pressure:

- `09bb67c`: stream/send-side starvation with quiet local egress.
- `3d06bea`: local TUN/downlink pressure with TUN drop feedback firing.

## Implications

Knife14bm's recent-pressure latch is validated as an active diagnostic/control
path in this run: `tun_egress_feedback` now counted the sampled TUN drop and
reported one pause/resume pair. This directly fixes the Knife14bl observation
where `runtime_tun_egress` saw the drop but feedback did not.

However, Knife14 is not accepted:

- Throughput is still in the `18-19 Mbit/s` band on the better A/B leg.
- Terminal pending remains large (`809715B`) when the local TCP state is closed
  and cannot send.
- The close/reap path still hides useful downlink bytes after local pressure.

## Next Plan

Do not revert Knife14bm based on the prior no-data-stream run. Continue from
`3d06bea` and move the next behavior stage to close-drain / terminal pending
under local downlink pressure:

1. Add deterministic tests around terminal pending classification when a socket
   transitions to `Closed/can_send=false` after downlink pressure.
2. Add or refine accounting that separates bytes still pending before close
   from bytes read after close and bytes impossible to deliver.
3. Change close-drain/reap behavior only if the tests prove a safe bounded drain
   point exists.
4. Re-run a scoped VPS reverse-first P1 acceptance after local tests pass.

Because the A/B stage still failed final Knife14 acceptance, the next
behavior-code change should wait for explicit confirmation of this plan.
