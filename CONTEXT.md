# mini_vpn

A learning-oriented VPN: a local client intercepts traffic through a TUN device and a userspace TCP/IP stack, then tunnels it over TLS + Yamux to a remote proxy server, which connects out to the real destination on the client's behalf.

## Language

**Upstream**:
The remote proxy/relay server the client tunnels through (e.g. the US server). The client establishes the TLS + Yamux connection to it.
_Avoid_: relay-server, gateway (when meaning the proxy box)

**Target**:
The final `IP:port` an intercepted connection wants to reach (e.g. a website). On the TUN path it is extracted from smoltcp's `local_endpoint()`. The Upstream connects out to the Target.
_Avoid_: upstream, destination, remote (these collide with other concepts)

## Relationships

- The client opens one **Upstream** connection and multiplexes many intercepted sessions over it (Yamux substreams).
- Each intercepted session carries exactly one **Target**; the **Upstream** dials that **Target**.

## Flagged ambiguities

- "upstream" was proposed for the final destination, but the codebase already uses **Upstream** for the proxy server — resolved: final destination is **Target**, proxy server stays **Upstream**.
- smoltcp's `local_endpoint()` sounds like "this machine's address" but on the TUN path it is the **Target** (the SYN's destination address) — it is NOT the local machine's address.
