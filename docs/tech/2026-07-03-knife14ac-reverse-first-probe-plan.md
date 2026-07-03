# Knife14ac plan - reverse-first tunnel attribution

1. Record knife14ab as a successful buffer-application run but failed reverse
   acceptance run.
2. Add `PROBE_ORDER` to the low-RTT probe so traffic order is explicit.
3. Add `RUN_REVERSE_FIRST_P1` to the US-client suite so a fresh reverse-only P1
   can run before forward pressure.
4. Keep product defaults unchanged; use `MINI_VPN_TUIC_TCP_POOL=4` only in the
   next acceptance checklist.
5. Verify shell syntax, stage review, learning updates, commit, push, and ask
   for one focused VPS run.
