# Knife15 M1 TCP write-stall Endpoint rebind implementation plan

Date: 2026-07-22

1. Record exact bundle provenance, prerequisite passes, the complete
   receiver-zero interval, QUIC deltas, writer wait, and rejected hypotheses.
2. Add one RED policy test: continuous TCP write pressure reaches the frozen
   RTT bound despite ongoing RX and selects an Endpoint rebind.
3. Implement only the policy input/action needed for that test; preserve the
   no-RX state machine and post-rebind recovery contract.
4. Add one RED episode-latch test, then cover all write episodes present at a
   rebind so the same continuous pressure cannot loop after recovery.
5. Add one RED write-pressure ownership test, then implement the TUIC-local
   atomic module and writer adapter hooks for generic and native TUIC TCP
   relays.
6. Wire one tracker per TCP pool slot into the Endpoint recovery snapshot and
   add discriminating log fields without changing script-compatible prefixes.
7. Run focused tests after each GREEN, then the relevant TUIC/Quinn/rebind
   suites and format checks.
8. Run the full project gates, release/Clippy, shell self-tests, diff/secret
   checks, and the exact 32 MiB capacity gate.
9. Perform code review for correctness, hot-path cost, false rebinds, episode
   races, pool lifecycle, D16/TUN/UDP regressions, and missing evidence. Repair
   any P0/P1 and rerun affected gates.
10. Update result docs, `HANDOFF.md`, `TODO.md`, and learning memory; commit and
    push the coherent stage. Then provide the exact fresh HK M1 user flow.

Stop rules: an expected RED may enter its minimal GREEN. Any unrelated or
unexpected regression is diagnosed and repaired without changing frozen
values. A local capacity result `<=170 Mbit/s` rejects the architecture and
forbids constant tuning. No macOS TUN command is agent-run.
