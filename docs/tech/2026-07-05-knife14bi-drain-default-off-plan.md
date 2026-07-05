# Knife14bi Drain Default-Off Plan

Date: 2026-07-05

## Tasks

1. Flip `DEFAULT_TUN_RX_DRAIN_BUDGET` from `8` to `0`.
2. Update runtime tests so default config and parser fallback expect `0`, while
   explicit `8` remains accepted.
3. Flip suite `DEFAULT_TUN_RX_DRAIN_BUDGET` from `8` to `0` and update help
   wording from "set 0 to disable" to "set >0 for explicit A/B".
4. Run local gates.
5. Commit/push the behavior patch.
6. Deploy to `.27` and run a default scoped reverse-first VPS suite without
   setting `MINI_VPN_TUN_RX_DRAIN_BUDGET`.
7. Parse the bundle and record whether the default path now matches the
   Knife14bh drain0 shape.

## Stop Conditions

- If default `0` still shows `tun_rx_drain` attempts in VPS, stop and inspect
  env propagation before changing data-plane code.
- If default `0` reproduces the `0.035 Mbit/s` collapse, stop and re-evaluate
  the A/B interpretation before further patches.
