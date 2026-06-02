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

## Relationships

- The client opens one **Upstream** connection and multiplexes many intercepted sessions over it (Yamux substreams).
- Each intercepted session carries exactly one **Target**; the **Upstream** dials that **Target**.

## Flagged ambiguities

- "upstream" was proposed for the final destination, but the codebase already uses **Upstream** for the proxy server — resolved: final destination is **Target**, proxy server stays **Upstream**.
- smoltcp's `local_endpoint()` sounds like "this machine's address" but on the TUN path it is the **Target** (the SYN's destination address) — it is NOT the local machine's address.
