# Knife14ac spec - reverse-first tunnel attribution

## Grounding

- Input bundle: `/tmp/mini_vpn/mvpn_knife14ab_usclient_suite_20260703_172934.tar.gz`.
- Tested commit: `3af4f7c` (`7621bc6` in the user message was not the run's
  actual commit).
- The suite confirmed `MINI_VPN_TCP_RX_BUFFER_BYTES=1048576` and
  `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576`; startup printed
  `TCP socket buffers: rx=1048576B tx=1048576B`.
- Direct baselines were healthy: forward receiver 276 Mbit/s, reverse receiver
  281 Mbit/s.
- Tunnel forward improved materially: standalone P1 reached 190 Mbit/s, and
  full P2/P4/P8 reached 192/150/163 Mbit/s receiver.
- Tunnel reverse remained low and bursty: standalone P1 was 20.7 Mbit/s, full
  P1/P2/P4/P8 were 17.5/26.2/32.2/26.5 Mbit/s receiver.

## Problem

Knife14ab disproved "64KiB smoltcp socket buffer is the sufficient reverse
cause." The reverse path still has long zero-throughput windows even with 1MiB
socket buffers. Client-side `send_slice_zero`/`send_slice_errors` stayed zero,
so this is no longer a simple local socket write-capacity failure.

Every current probe runs forward before reverse on the same client process and
usually the same TUIC TCP connection. That makes it hard to tell whether reverse
is intrinsically weak on `.33 -> .27`, or whether previous forward pressure
poisons the same QUIC connection/congestion state before reverse starts.

## Goal

Add a minimal acceptance diagnostic that can run a fresh reverse-only P1 probe
before the normal forward-first suite. The next VPS run should answer:

- Does fresh reverse P1 recover toward the direct reverse baseline?
- Does reverse only collapse after forward pressure on the same process?
- Does raising `MINI_VPN_TUIC_TCP_POOL` isolate flow-level congestion enough to
  improve reverse without changing product defaults?

## Non-goals

- Do not change TUIC protocol behavior.
- Do not change production defaults.
- Do not tune socket buffers again in this stage.
- Do not require server-side sing-box changes before client-side attribution is
  sharper.

## Design

- `scripts/knife14b-lowrtt-probe.sh` accepts `PROBE_ORDER`:
  `forward-first`, `reverse-first`, `forward-only`, or `reverse-only`.
- The parent suite accepts `RUN_REVERSE_FIRST_P1=1`, which runs a fresh
  `reverse-only` P1 probe before the normal forward-first P1/full suite.
- The parent suite waits for quiet tunnel gauges after the reverse-first probe
  before continuing, so later probes are not polluted by leftover relays.
- The next checklist should set `MINI_VPN_TUIC_TCP_POOL=4` as an acceptance
  parameter while keeping normal runtime defaults unchanged.

## Acceptance

- `bash -n` passes for both modified scripts.
- The next report includes a
  `mvpn_<tag>_usclient_tunnel_mtu1200_reverse_first_p1_*.md` artifact.
- That artifact's header includes `probe_order: reverse-only`.
- If fresh reverse P1 is still low, ask for `.33`/sing-box or path-level
  evidence next because the client local socket-window hypothesis is exhausted.
- If fresh reverse P1 is healthy but normal reverse after forward is low, focus
  on QUIC connection isolation/recovery rather than VPS service state.
