# VLESS+REALITY as a second Upstream transport, via a hand-rolled TLS 1.3 client

The client gains a **second Transport** to the same **Upstream** — **VLESS over REALITY over TCP** — as the
anti-censorship fallback to the default **TUIC-over-QUIC** data plane. QUIC/TUIC is a censorship target (GFW
does QUIC-Initial SNI blocking and can throttle/drop QUIC); REALITY is byte-for-byte indistinguishable from a
real TLS 1.3 HTTPS connection to a borrowed site, and a prober that fails REALITY auth is transparently
forwarded to that real site. This transport is **orthogonal** to the Rules.md three goals (QUIC connects today)
and does **not** block them; it is resilience for when QUIC is degraded. REALITY is **TCP-only** — UDP stays on
the QUIC datagram plane.

Decided in 刀6 grill, 2026-06-22; see `docs/tech/2026-06-22-knife6-reality-transport-*`.

## How REALITY is implemented: a hand-rolled TLS 1.3 client (not a TLS library)

REALITY hides its authentication inside the TLS 1.3 ClientHello's `legacy_session_id` field (an AES-128-GCM
ciphertext over the ClientHello transcript, keyed by X25519-ECDH-derived HKDF auth key). Writing a custom
`legacy_session_id` is something **stock TLS stacks deliberately do not expose**. We therefore **hand-roll the
TLS 1.3 ClientHello (and the rest of the handshake) as raw bytes**, using mature RustCrypto primitives
(`x25519-dalek`, `hkdf`+`sha2`, `aes-gcm`, `hmac`) for the crypto and a custom cert-verify (HMAC-SHA512 over the
server's temporary ed25519 cert pubkey) instead of CA-chain validation. This is the approach the reference Rust
REALITY implementation (`shoes`) uses; it is the blueprint, not a dependency.

## Considered Options (TLS layer for REALITY)

- **`boring` (BoringSSL bindings) — REJECTED.** Verified against the crate API + BoringSSL C source: there is
  **no way to set a custom ClientHello `legacy_session_id`** — `Ssl::set_session()` is resumption,
  `set_session_id_context` is the server-side resumption scope, and `ClientHello` is a server-side **read-only**
  inspection type. BoringSSL even removed the `SSL_set_generate_session_id` callback ("no callers"), so not even
  `boring-sys` FFI helps. The boring path requires **patching BoringSSL C and maintaining a fork** — the worst
  maintenance profile, and nobody maintains a REALITY-boring fork. (We initially chose boring in grill, then
  this finding overturned it.)
- **`craftls` (rustls fork) — REJECTED.** Pure-Rust and gives uTLS-style ClientHello **fingerprint** control,
  but does **not** expose `legacy_session_id` injection — we'd have to fork-and-extend it further, taking on
  rustls-fork rebase maintenance for a capability it doesn't ship.
- **Hand-rolled TLS 1.3 ClientHello (chosen).** Pure-Rust, **no forked TLS crate and no C dependency** on the
  critical path; RustCrypto primitives are mature (satisfies the "prefer mature frameworks" spirit for the
  crypto — only the TLS *assembly* is ours). Trade-off: we own a minimal TLS 1.3 record/handshake encoder
  (bounded; `shoes` is the blueprint). This matches Go's reality (uTLS is itself a forked/hand-driven
  crypto/tls) — REALITY fundamentally requires escaping stock TLS.
- **Drop REALITY, ship plain VLESS+TLS — REJECTED.** Trivially DPI-detectable (TLS-in-TLS), defeating the
  entire anti-censorship purpose of this orthogonal line.

## Consequences

- **Relationship to ADR-0003 ("single rustls / unify on QUIC"):** preserved. We add **no second rustls version**
  and no second TLS *library* — we hand-write the REALITY handshake and reuse RustCrypto primitives. The QUIC
  data plane (quinn + rustls 0.21) is untouched.
- **Scope is sliced across knives** (grill: transport-first, failover last): 刀6 = REALITY auth crypto +
  ClientHello construction (sans-IO, 100% offline-TDD — the slice holding the #1 correctness risk: the
  session_id AEAD-over-transcript with the session_id **zeroed before computing the AAD**); 刀7 = ServerHello +
  TLS 1.3 key schedule + record AEAD; 刀8 = server-flight verify + Finished + live handshake + VLESS framing +
  `RealityUpstream` (`ProxyUpstream::open_tcp`) + env selector + real-egress acceptance; 刀9 = auto-failover.
- **Stealth is best-effort, not byte-exact:** a Chrome-like ClientHello (GREASE, X25519 keyshare, cipher/curve/
  ALPN/extension order) — enough to defeat naive DPI, not a guaranteed byte-exact match to any Chrome build
  (which drifts). The X25519 keyshare is a **hard** requirement (sing-box extracts that ECDHE pubkey to derive
  the auth key — issue #2084), not just a fingerprint detail.
- **Vision (xtls-rprx-vision) deferred:** empty VLESS flow; the server's user must be configured empty-flow to
  match. Known limitation: traffic-pattern (TLS-in-TLS) remains detectable even though the handshake is stealthy.
- **Server-side dependency:** acceptance (刀8) needs a sing-box VLESS+REALITY inbound (uuid / reality keypair /
  short_id / borrowed handshake server / empty flow). Not in git.
