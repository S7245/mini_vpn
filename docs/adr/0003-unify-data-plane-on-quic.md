# Unify the relay data plane on QUIC; first cut is UDP-over-QUIC-datagram, TCP stays on yamux for now

The north star is to migrate the relay data plane to QUIC (`quinn`): TCP relay over reliable QUIC
streams, UDP relay over QUIC DATAGRAM (RFC 9221). We chose this because the existing single
TLS + yamux + TCP upstream cannot deliver the platform goal of "high concurrency, high throughput,
long-lived, hard quality" for UDP — tunnelling UDP over TCP imposes reliability, in-order delivery,
and cross-flow head-of-line blocking, which is semantically *wrong* for QUIC/HTTP3 (it assumes an
unreliable, unordered, self-congestion-controlled substrate) and bad for live streaming (TCP
retransmit defeats loss-tolerant low latency). The degradation is structural, not tunable.

We deliver in stages so each remains a verifiable stepping stone with zero regression. The **first
cut** adds a NEW QUIC datagram data plane that carries UDP relay only, running **alongside** (not
replacing) the existing TCP/yamux upstream. Migrating TCP relay onto QUIC streams (retiring yamux
and its HOL blocking) and platform-scale work (multi-upstream/failover, server-side UDP session-table
hardening, graceful drain) are explicitly later stages, tracked in TODO.md.

## Considered Options

- **UDP over the existing TCP/yamux tunnel (functional stepping stone)** — rejected. It closes the
  "UDP/443 silently dropped" blind spot cheaply but inherits TCP HOL + a single shared congestion
  controller; it cannot meet the hard-quality/scale bar, and the degradation is structural.
- **A dedicated non-QUIC UDP datapath (DTLS / Noise-WireGuard / KCP over UDP)** — rejected. We would
  rebuild encryption, congestion control, connection migration, and multiplexing that QUIC already
  provides; QUIC's DATAGRAM extension is purpose-built for exactly an unreliable encrypted substrate.
- **Big-bang migrate TCP + UDP + platform onto QUIC at once** — rejected. Maximal regression risk
  against the only proven (TCP) path. Staged delivery keeps zero-regression as an invariant.

## Consequences

- Two data planes coexist during the transition (yamux/TCP for TCP relay, QUIC for UDP); accepted as
  transitional debt with a committed end-state (unify on QUIC).
- The server's existing SOCKS5-UDP-over-yamux skeleton (`server.rs` `RelayRequest::Udp`) is superseded
  by the QUIC datagram path and will not be used by it. It remains only for the legacy SOCKS5
  direct-proxy (`client.rs`), if at all.
- QUIC reuses the existing dev CA / TLS 1.3 material; the server gains a UDP/QUIC endpoint alongside
  its TCP listener.
