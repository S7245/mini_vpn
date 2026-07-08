# Knife14ev ACK cadence controller plan

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Steps

1. Add focused TDD around the ACK cadence boost in
   `DownlinkCreditController`.
2. Add a narrow main-loop hook that records current-epoch relay gap hints on
   the target `SocketCtx`.
3. Feed the boost into existing pressure ACK/window read-credit calculation
   without changing staging limits.
4. Run local and `.27` focused gates.
5. Run one scoped safe1200 reverse-first P1 only after gates pass.
6. Record results and learning memory.

## Stop Conditions

Stop and analyze instead of tuning constants if:

- the focused tests require weakening the staging/pending invariant;
- VPS reintroduces `tun_tx_dropped_delta`;
- QUIC blocking/loss becomes nonzero;
- the run returns to pressure-free no-data shape with `read_credit_limit_bytes_min=65536`;
- the run stays near `20 Mbit/s` with the boost clearly active.
