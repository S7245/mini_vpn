# Adopt the TUIC v5 protocol for the data plane (implemented on quinn), instead of a self-designed transport

The data plane will speak the **TUIC v5 protocol** (a mature, QUIC-based 0-RTT proxy protocol), **implemented
on our existing quinn stack**, replacing the previously-planned self-designed TCP→QUIC transport. We keep the
TUN interception + fake-IP DNS front-end we already built.

We chose this because the project's priority was sharpened to **「用成熟方案拿到最好体验」> 「复用自研引擎」**
(prefer a mature solution for the best experience over extending the self-built engine), and because mobile
(iOS/Android, weak networks, WiFi↔cellular roaming) raises the bar far beyond what hand-rolling our own
transport/migration/congestion can reasonably reach. TUIC gives, by design: 0-RTT, TCP over QUIC bidirectional
streams, full-cone UDP in both a native (QUIC datagram) and a quic-stream (fragmented, >MTU) mode, heartbeat,
and — via QUIC itself — connection migration. Our Stage-12 UDP-over-QUIC-datagram is already ≈ TUIC's native
UDP mode, and TUIC's quic-stream UDP mode is exactly the oversized-datagram stream-fallback we had deferred.

**The decisive payoff is interoperability**: speaking the standard means our client can use a mature,
battle-tested **sing-box / Mihomo TUIC server** at the exit (best experience, maintained, no production server
code from us), and mature clients can use our server. A bespoke protocol gives none of that.

## Considered Options

- **Continue the self-designed TCP→QUIC transport** — rejected: violates the new priority, reinvents a solved
  protocol (mux/migration/congestion), and yields no ecosystem interoperability. (This work was reverted.)
- **WireGuard data plane** — rejected: it is L3 packet forwarding, not proxy/fake-IP semantics, and its
  fingerprint is DPI-blocked by the GFW; it would need an obfuscation wrapper anyway and discard the fake-IP
  design.
- **Vendor/fork a Rust `tuic` library** — not viable: the canonical `tuic-protocol/tuic` repo is now
  **spec-only**, the reference Rust impl (EAimTY) is **archived**, and the crates.io `tuic` crate is
  **all-versions-yanked**. So adopting TUIC means implementing the (stable) spec ourselves.
- **Integrate the sing-box core (Go) wholesale** — rejected: heavy Go/FFI integration that abandons the Rust
  codebase, learning, and control; effectively "ship sing-box".
- **Implement TUIC v5 on quinn (chosen)** — mature protocol *design*, our *code*, on the quinn we already use;
  reuses Stage 12; interoperates with the mature TUIC ecosystem.

## Consequences

- We implement the TUIC v5 spec ourselves; acceptance includes an **interop test against a sing-box TUIC
  server** (proves we speak the real protocol, not a look-alike).
- yamux is retired for TCP relay (TCP moves onto TUIC `Connect` bi-streams) — staged for zero regression.
- Auth uses `rustls` keying-material export for the TUIC token — **must verify rustls 0.21 / quinn 0.10 expose
  it**; this (plus migration/0-RTT config) may force a quinn/rustls version bump.
- This supersedes the "self-built transport" reading of ADR-0003's north star: the data plane still unifies on
  QUIC, but via the **TUIC protocol** rather than a bespoke one. ADR-0003's staging (UDP first, TCP next)
  still holds.
- The TUN + fake-IP front-end is unaffected; fake-IP→domain maps to TUIC's FQDN address type.
