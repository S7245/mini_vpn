# Hijack all plaintext DNS (:53) via raw-packet forge, dropping the smoltcp DNS socket

The client intercepts **every** plaintext DNS query on the TUN (UDP destination port **53**, to **any**
resolver IP) and answers it **locally** with a forged **fake-IP** response, instead of only forging
queries sent to our own resolver `198.18.0.1`. Without this, an app pointed at its own resolver
(`8.8.8.8:53`, a corporate DNS, etc. — or one that an app falls back to after 刀4 blocks its
encrypted DNS) has its query **tunnelled to the real DNS**, gets a **real** address, and bypasses the
fake-IP map. This completes 刀4 (ADR-0006): blocking encrypted DNS only works if the plaintext
fallback — to *whatever resolver the app uses* — is also forged. Fake-IP routing therefore no longer
depends on the system DNS being set to `198.18.0.1` (the requirement that made seamless on/off, e.g.
under a NetworkExtension, impossible).

Decided in 刀5 grill, 2026-06-22; see `docs/tech/2026-06-22-knife5-dns-hijack-*`.

## Considered Options

- **Keep using a smoltcp UDP socket, bound wildcard / via AnyIP** — rejected. To answer a query sent to
  `8.8.8.8:53`, the reply's **source** must be `8.8.8.8:53`, or the app's socket drops it. smoltcp
  selects the reply source from the interface's configured IPs (the existing code had to add
  `198.18.0.1/32` as an interface IP *precisely* for this). Resolvers are an **unbounded set**
  (`8.8.8.8`, `1.1.1.1`, LAN/corporate DNS, …); adding each as an interface IP is impractical and makes
  smoltcp "own" those addresses for other traffic. Dead end.
- **Raw-packet forge (chosen)** — intercept any `:53` at `classify_inbound` before `iface.poll`, and
  build the reply with `build_udp_ip_packet(src = the queried resolver, dst = the app)` injected via
  `inject_ip_packet`. This is the **same already-accepted mechanism the UDP relay downlink uses**, and
  `build_udp_ip_packet` sets an arbitrary source — so any resolver is handled with no per-IP setup.
- **Keep `198.18.0.1` on the smoltcp path, raw-forge only other resolvers** — rejected: two DNS-forging
  code paths to keep in sync. We **dropped** the smoltcp DNS socket, its `198.18.0.1:53` bind, the
  `198.18.0.1/32` interface IP, and `drain_dns` entirely → one unified raw path. `198.18.0.1` is now
  just an *advertised* resolver address (for the frontend's NE config), not special in the data plane.

## Consequences

- **Scope is "all `:53`", not filtered by destination** (grill D4): even RFC1918 / LAN DNS is forged.
  Split-horizon / intranet-only domains will resolve at the exit and may fail — accepted as a known
  limitation; under a full tunnel, on-link LAN DNS uses the physical interface and never reaches the TUN,
  so this is rare. A destination allowlist can be added later if measured to matter.
- **TCP :53 is RST'd**, not forged: `resolve_target` blocks port 53 (via `is_dns_relay_port`), reusing
  the 刀4 TCP `Block → rearm` wiring. The invariant that makes this TCP-only — UDP :53 is siphoned to the
  hijack path at `classify_inbound` and never reaches `resolve_target` — is load-bearing and documented
  at the call site. Our forged UDP answers never set TC, so standard stubs don't escalate to TCP.
- **Unparseable `:53` packets are dropped, never forwarded** — forwarding would leak a real resolution.
  The parser covers standard single-question A/AAAA (all of `getaddrinfo`); exotic queries time out and
  retry. IPv6 DNS is not intercepted (crate is `proto-ipv4` only) — deferred with the rest of IPv6.
- 刀4 (ADR-0006) is unchanged: `:53`→forge, `:853`→block, `:443`-DoH→block are disjoint by port.
  fake-IP (ADR-0002) and the data plane (ADR-0004/0005) are untouched.
