# TUIC data plane — evaluation (one-pager)

> Context: new priority **「用成熟方案拿到最好体验」> 「复用自研引擎」** + mobile (iOS/Android) target.
> Question: should the data plane use a mature tunnel instead of self-designed transport?

## TL;DR

Adopt the **TUIC v5 protocol**, **implemented on our existing quinn** (option C1 below). Mature *design*
(proven in sing-box/Mihomo), our *code*, and — crucially — **interoperable with the mature TUIC ecosystem**:
our client can talk to a battle-tested **sing-box TUIC server** at the exit (best experience, maintained,
zero server code from us), and vice-versa. Reuses our Stage 12 work (our UDP-over-QUIC-datagram is already
≈ TUIC "native" UDP mode). See ADR-0004.

## What TUIC v5 is (from the spec)

QUIC-based 0-RTT proxy protocol. Commands `[VER][TYPE][OPT]`:
- **Authenticate(0x00)**: UUID(16) + token(32) derived via **TLS keying-material exporter** (label=UUID,
  context=password) — no extra round trip.
- **Connect(0x01)**: TCP relay over a **QUIC bidirectional stream**; client relays immediately after the
  header (0-RTT, no wait for server reply).
- **Packet(0x02)**: UDP relay, two modes the client must support — **native** (QUIC datagram) and **quic**
  (unidirectional stream, with PKT_ID/FRAG_TOTAL/FRAG_ID fragmentation for >MTU packets). Full-cone via a
  synced session/associate id.
- **Dissociate(0x03)** / **Heartbeat(0x04)** (datagram, while relaying).
- Address = `[TYPE][ADDR][PORT]` (FQDN / IPv4 / IPv6) → fake-IP→domain (FQDN) maps cleanly.
- Connection migration / 0-RTT resume come from **QUIC itself** (quinn), not the protocol layer.

## Why it fits mini_vpn

- **Reuses our stack**: quinn 0.10 already in; our Stage-12 UDP datagram ≈ TUIC native mode; fake-IP→domain
  = TUIC FQDN address.
- **Solves deferred items for free**: TUIC "quic mode" UDP = the **oversized-datagram stream-fallback** we
  deferred; migration = the **WiFi↔cellular roaming** mobile need.
- **Mobile**: pure-Rust + quinn cross-compiles to iOS/Android; migration/0-RTT = the mobile experience win.
- **Interop = the mature payoff**: speak the standard → use mature sing-box/Mihomo servers/clients.

## Options compared

| Option | What | Verdict |
|---|---|---|
| **A. Integrate sing-box core** (Go) wholesale | Drop the self-built core, embed sing-box libcore | ❌ Heavy Go/FFI, loses Rust/learning/control; ≈ "just ship sing-box". |
| **B. Continue self-designed TCP→QUIC** | Invent our own mux/migration/CC | ❌ Violates new priority; reinvents a solved protocol; **no ecosystem interop**. |
| **WireGuard data plane** | Mature L3 tunnel | ❌ Wrong arch (packet forwarding, not proxy/fake-IP); **DPI-blocked by GFW**; needs obfuscation wrapper anyway. |
| **C2. Vendor/fork a Rust `tuic` lib** | Reuse reference impl | ❌ crates.io `tuic` **all-yanked**; reference impl **archived**; old deps + maintenance burden. |
| **C1. Implement TUIC v5 on quinn** ✅ | Mature *design*, our code, on existing quinn | ✅ **Recommended** — interop with sing-box/Mihomo, reuses Stage 12, solves stream-fallback + roaming, no dead-dep risk. |

## C1 — what implementing it actually involves

- **Auth**: TUIC token via keying-material export — ✅ **CONFIRMED available on our current stack**:
  `quinn 0.10.2` exposes `Connection::export_keying_material` (backed by rustls 0.21's
  `export_keying_material`). **No version bump forced for auth.**
- **TCP relay**: open QUIC bi-stream, write `Connect` header (with FQDN/IP target from our `TargetAddr` /
  fake-IP resolve), then pump bytes. Replaces the yamux TCP path → retires yamux.
- **UDP relay**: keep native (datagram) mode (≈ what we have); add quic-stream mode for oversized + the
  associate-id/fragmentation framing per spec.
- **Migration / 0-RTT**: enable + configure on quinn (Connection IDs, path validation, session resumption).
- **Interop test**: run a **sing-box TUIC server** at the exit and verify our client connects (this is the
  acceptance bar that proves "we speak the real protocol").

## Risks / unknowns to pin in the grill

1. ~~rustls 0.21 keying-material export~~ — ✅ confirmed (`quinn 0.10.2 Connection::export_keying_material`).
2. quinn 0.10 migration/0-RTT config surface (vs a newer quinn) — verify migration is enabled + 0-RTT resume.
3. Fake-IP→domain + TUIC FQDN address interplay (should be clean; verify).
4. Keeping TCP-relay zero-regression while swapping yamux → TUIC bi-streams (stage it; dual-run if needed).

Sources: [TUIC spec (tuic-protocol/tuic)](https://github.com/tuic-protocol/tuic/blob/dev/SPEC.md),
[sing-box TUIC outbound](https://sing-box.sagernet.org/configuration/outbound/tuic/),
[DeepWiki TUIC protocol](https://deepwiki.com/tuic-protocol/tuic/2-protocol-specification).
