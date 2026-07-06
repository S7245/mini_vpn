# Knife14cj Pre-Payload Pressure ACK Drain Spec

Date: 2026-07-06

## Goal

Move Knife14ci's ACK/TUN-RX drain earlier and keep it alive while local egress
pressure persists.

Knife14ci proved that ready local TCP ACK/window packets exist at the egress
credit edge: `tcp-tun-rx-drain attempts=5 packets=105 tcp=105
budget_exhausted=5`. It still failed because the drain ran only after remote
payload bytes were accepted and flushed. The retry saw `tun_tx_dropped_delta=82`
and `send_queue_max=892928` before the drain could unwind pressure, then closed
with active send-capable backlog.

Knife14cj keeps the same pressure model but changes timing:

- before accepting a new remote payload, if that socket is already near
  `tx_queue_credit_high`, drain ready TUN RX first;
- during timer maintenance, if dirty downlink pressure remains near the credit
  edge, drain ready TUN RX before the next dirty flush pass;
- keep all drains bounded, pressure-gated, and off below the existing credit
  edge.

## Non-Goals

- Do not increase `MINI_VPN_TUN_RX_DRAIN_BUDGET` defaults.
- Do not restore unconditional opportunistic TUN RX drain.
- Do not change TUIC pool, sing-box, iperf3, receive-window expansion, or TUN
  queue length.
- Do not hide terminal pending or close-tail egress; those stay acceptance
  signals.

## Invariants

- Explicit `MINI_VPN_TUN_RX_DRAIN_BUDGET>0` still defines the packet budget
  when a pressure drain is triggered.
- Default `MINI_VPN_TUN_RX_DRAIN_BUDGET=0` derives budget from
  `tx_queue_credit_guard_bytes / tun_mtu`, capped by the Knife14ci max.
- Pre-payload drain only runs for current-epoch, non-empty remote payloads that
  would be accepted by the local socket.
- Timer maintenance drain only runs while dirty downlink exists and aggregated
  dirty pressure reaches the credit edge.
- Diagnostics must distinguish pre-payload, post-payload, and maintenance
  drain attempts.

## Acceptance

Local:

- Focused tests cover pre-payload budget suppression and enablement.
- Focused tests cover dirty pressure maintenance suppression and enablement.
- Drain diagnostics include source counters.
- Existing Knife14ci/pressure-credit/local gates pass.

VPS:

- Same clean reverse-first P1 suite with required `.27 -> .77` and
  `.33 -> .77` baselines.
- Expected improvement signal: pre/maintenance drain source counters engage,
  `tun_tx_dropped_delta` does not recur, and reverse-first P1 materially leaves
  the `10-20 Mbit/s` class.
- If throughput remains low but drops disappear, the next target is local
  egress scheduling after ACK progress, not auth/pool/iperf.
