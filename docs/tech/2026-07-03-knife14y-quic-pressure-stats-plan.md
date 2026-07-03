# Knife14y plan - QUIC pressure stats for TCP stalls

1. Record the knife14x result and classify the remaining blocker as unresolved
   QUIC stream write pressure.
2. Inspect quinn 0.10 connection stats fields and choose the minimum useful
   signal set.
3. Add a gated TUIC QUIC stats interval that follows `MINI_VPN_TCP_DIAG=1` in
   acceptance runs.
4. Log per-connection stats for initial connections and reconnects.
5. Add unit tests for interval parsing and stats line formatting.
6. Run local tests, clippy, and diff checks.
7. Perform stage code-review, update learnings, commit, push, and provide the
   next VPS test checklist.
