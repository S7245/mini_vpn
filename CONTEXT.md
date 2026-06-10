# mini_vpn

A learning-oriented VPN: a local client intercepts traffic through a TUN device and a userspace TCP/IP stack, then tunnels it over TLS + Yamux to a remote proxy server, which connects out to the real destination on the client's behalf.

## Language

**Upstream**:
The remote proxy/relay server the client tunnels through (e.g. the US server). The client establishes the TLS + Yamux connection to it.
_Avoid_: relay-server, gateway (when meaning the proxy box)

**Target**:
The final `IP:port` an intercepted connection wants to reach (e.g. a website). On the TUN path it is extracted from smoltcp's `local_endpoint()`. The Upstream connects out to the Target.
_Avoid_: upstream, destination, remote (these collide with other concepts)

**Reconnect epoch**:
A monotonically increasing counter for the Upstream connection's generation; it increments each time a new Upstream connection is established. Used so relay tasks belonging to a previous connection cannot feed data into a socket served by the new connection (anti-crosstalk).
_Avoid_: session id, connection id (epoch is about generation, not identity)

**fake-IP**:
A placeholder IPv4 from the `198.18.0.0/15` range handed back to an application in a forged DNS response, instead of the domain's real address. The application connects to the fake-IP; the client maps it back to the domain when the TCP SYN arrives. Not a real address — never routed on the public internet.
_Avoid_: virtual IP, proxy IP (fake-IP is the precise term, matching Clash's fake-ip mode)

**fake-IP map**:
The client-held bidirectional table `domain ↔ fake-IP`. Populated when forging a DNS response; read ("resolve") when an intercepted TCP SYN's destination falls in the fake-IP range, to recover the domain for the relay request.
_Avoid_: dns cache (it is not a cache of real DNS answers)

**UDP flow**:
One intercepted UDP conversation, identified by the app's 4-tuple `(srcIP:srcPort, dstIP:dstPort)`. Unlike a TCP **session** it has no handshake or teardown — it is born on the first datagram and reclaimed by idle timeout. Each **UDP flow** carries exactly one **Target** (the `dstIP:dstPort`, possibly a fake-IP resolved to a domain).
_Avoid_: udp session, udp connection (UDP is connectionless; "flow" signals there is no connection state)

**flow-id**:
A `u32` minted by the client, one per **UDP flow**, carried in both directions on the QUIC datagram so each side can demux a datagram back to its flow. It exists because the server's reply addresses the real Target IP, which the client cannot reverse-map to a fake-IP — the flow-id is the only reliable demux key.
_Avoid_: session id, stream id (it identifies a UDP flow, not a QUIC stream or TCP session)

**assoc-id**:
The TUIC UDP association id (`u16`) carried in a TUIC `Packet`. In mini_vpn it is allocated **one per UDP flow** (4-tuple) — the same role as **flow-id**, just 16-bit and on the TUIC wire — so the reply (tagged with assoc-id) maps back to the app endpoint and fake-IP source. We deliberately do not use TUIC's full-cone "one association per local socket" model, because that would reintroduce the reply-demux problem flow-id solves.
_Avoid_: session id (it identifies a UDP flow, not a QUIC stream or TCP session)

## Relationships

- The client opens one **Upstream** connection and multiplexes many intercepted sessions over it (Yamux substreams).
- Each intercepted session carries exactly one **Target**; the **Upstream** dials that **Target**.
- UDP relay rides a second transport to the same **Upstream** — a QUIC datagram data plane — where many **UDP flows** are multiplexed by **flow-id** (no per-flow stream).

## Flagged ambiguities

- "upstream" was proposed for the final destination, but the codebase already uses **Upstream** for the proxy server — resolved: final destination is **Target**, proxy server stays **Upstream**.
- smoltcp's `local_endpoint()` sounds like "this machine's address" but on the TUN path it is the **Target** (the SYN's destination address) — it is NOT the local machine's address.
