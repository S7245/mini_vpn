# Knife14m result - TUIC TCP pool live run

Date: 2026-07-02

## Source

- Commit: `0d62624` (`test(knife14m): pass tuic tcp pool through suite`)
- Bundle: `/tmp/mvpn_knife14m_usclient_suite_20260702_113819.tar.gz`
- Extracted report: `/tmp/mvpn14m.inspect.XZwH6J/mvpn_knife14m_usclient_suite_20260702_113819.md`
- Extracted client log: `/tmp/mvpn14m.inspect.XZwH6J/mvpn_accept_20260702_113819.log`

## What This Tested

`4097fe8` added an opt-in TUIC TCP connection pool, but the suite was not passing
`MINI_VPN_TUIC_TCP_POOL` into the `sudo -E env ... mini_vpn client-tun` launch.
`0d62624` fixed the suite propagation so this run is the first live pool=4 run.

The hypothesis was that distributing TCP streams across four TUIC/QUIC
connections could avoid a single-connection bottleneck during concurrent TCP
iperf sweeps.

## Grounding

- The suite environment reported `MINI_VPN_TUIC_TCP_POOL=4`.
- The startup command included `MINI_VPN_TUIC_TCP_POOL=4`.
- The client log printed `TUIC TCP connection pool=4`.
- The suite completed with `status: COMPLETED`.

## Throughput Summary

Full MTU 1200 sweep:

| Direction | P1 | P2 | P4 | P8 |
| --- | ---: | ---: | ---: | ---: |
| Forward receiver | 2.41 Mbits/sec | 2.39 Mbits/sec | 2.80 Mbits/sec | 3.09 Mbits/sec |
| Reverse receiver | 14.5 Mbits/sec | 27.3 Mbits/sec | 19.8 Mbits/sec | failed |

Standalone P1:

| Direction | Receiver |
| --- | ---: |
| Forward | 2.30 Mbits/sec |
| Reverse | 21.3 Mbits/sec |

The P8 reverse run ended with `Broken pipe` and `exit=1`.

## Diagnostics

The accept log shows the forward-path failure mode is still concentrated in
upstream QUIC stream writes:

- `write upstream timeout`: 14 events.
- `write upstream failed`: 2 events.
- `remote_write_timeout` close records: 28 lines.
- `dead_slot_reap`: 4 lines.

The mini_vpn main loop was mostly parked during the bad runs, so this does not
look like a local event-loop CPU saturation issue.

## Decision

Reject pool=4 as the next default or primary fix. Keep the Rust default at
pool=1 and keep `MINI_VPN_TUIC_TCP_POOL` as an explicit experiment knob only.

Before the next data-plane patch, run one controlled pool=1 suite on `0d62624`
with the same script and VPS conditions. If pool=1 recovers the multi-flow
profile, the next design stage should focus on upstream write/backpressure
behavior instead of adding more TUIC connections.

## Code Review Result

No code issue was found in the `0d62624` suite patch itself:

- The reported launch command matches the actual `sudo -E env` command.
- The default remains `MINI_VPN_TUIC_TCP_POOL=1`.
- Secret redaction behavior is unchanged.
- VPS preflight remains enabled by default through `CHECK_VPS_SERVICES=1`.
