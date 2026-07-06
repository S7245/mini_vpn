# Knife14cp Recent-Active ACK Drain Spec

Date: 2026-07-06

## Goal

Repair the Knife14co remaining burst/idle failure by letting the 5ms timer run
a small bounded TUN RX ACK/window drain for a short period after accepted
downlink work, even when local egress is below the pressure edge.

Knife14co proved payload-triggered active-flow drain removes local pressure but
does not remove low-average burst/idle throughput. Knife14cp tests the missing
case: ACK/window packets generated after a burst may arrive after the immediate
post-payload drain, and no next remote payload arrives to trigger another drain.

## Evidence

Knife14co valid VPS run:

- reverse-first P1 improved to `25.5/24.3 Mbit/s` but stayed `low_average`
- local pressure was clean during the P1 attribution window:
  `local_pressure=0`, `downlink_backpressure pause_edges=0`,
  `tun_tx_dropped_delta=0`, `drop_delta_total=0`, `send_queue_max=427496`
- active-flow ACK drain ran and mostly reached `would_block`:
  `attempts=7506`, `packets=24055`, `would_block=7491`
- pending/close/reap stayed clean and QUIC loss/congestion/blocking stayed zero
- remaining shape had many zero-throughput intervals and stream read/pending
  gaps despite clean local pressure surfaces

## Non-Goals

- Do not increase ACK drain budget size.
- Do not make timer drain unconditional.
- Do not change stale pool, iperf3, sing-box, egress pacer, or receive-window
  sizing from this evidence.
- Do not use post-server-evidence `.77:22` client-log tail pressure as a P1
  root cause.

## Invariants

- Timer active-flow drain is enabled only for a short recent-active window
  after accepted downlink work or pending downlink work.
- No recent downlink work means no below-pressure timer TUN RX scan.
- Existing pressure maintenance keeps priority and keeps the larger pressure
  budget.
- Explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` remains an operator override once
  the recent-active condition is true.
- Diagnostic counters distinguish payload-triggered drain, timer pressure
  drain, and timer active-flow drain.

## Acceptance

Local:

- Add RED/GREEN tests proving recent-active timer drain returns a bounded
  active-flow budget below pressure, while inactive timer drain returns zero.
- Prove pressure maintenance still takes the pressure budget.
- Prove diagnostics expose the new timer active-flow source.
- Run focused TUN RX/backpressure/debt/close tests and broad gates.

VPS:

- Reverse-first P1 must not be no-data.
- Desired improvement: fewer zero-throughput intervals and materially higher
  average than Knife14co without reintroducing local pressure.
- Keep `tun_tx_dropped_delta=0` or explain any new drop/debt signal.
- Keep pending/close/reap accounting clean.
- Check current `.33` logs for `fail auth` separately from mini_vpn behavior.
