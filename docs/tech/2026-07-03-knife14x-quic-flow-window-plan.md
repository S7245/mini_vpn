# Knife14x plan - explicit QUIC flow-control windows

1. Record the knife14w2 result: environment and VPS preflight are healthy, but
   tunnel TCP is blocked by long local-to-remote stream writes.
2. Read quinn 0.10 `TransportConfig` defaults and identify relevant knobs:
   stream receive window, connection receive window, send window, and bidi
   stream count.
3. Set explicit VPN-sized constants in `src/quic.rs`.
4. Keep congestion-control and pool defaults unchanged.
5. Print the selected QUIC window values during TUIC startup.
6. Add unit coverage for the configured values.
7. Run local gates, review the diff, update `.learnings/`, then commit.
