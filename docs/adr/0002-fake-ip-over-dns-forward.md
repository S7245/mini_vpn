# Use fake-IP, not DNS-forward, to reach poisoned/blocked domains

To make GFW-poisoned domains reachable, the client intercepts DNS queries and hands
back a placeholder IP from `198.18.0.0/15` (forged A response, no real resolution),
mapping `fake-IP ↔ domain`. When the application then sends a TCP SYN to that fake-IP
(the whole range is routed into the TUN), the client looks up the domain and sends a
`RelayRequest::Tcp { target: DomainPort }`; the Upstream resolves the domain with its
own clean DNS and connects. Domain resolution thus happens at the exit, bypassing local
poisoning.

## Considered Options

- **DNS-forward**: tunnel the DNS query itself to the Upstream, resolve there, return the
  real IP, let the app connect to the real IP directly. Rejected because (1) it needs UDP
  relay first (DNS is UDP/53), lengthening the dependency chain; (2) the real IP must then
  be routed into the TUN — but we don't know which real IPs should be tunneled, forcing
  either global IP capture or a dynamic per-IP route table; (3) the real IP may still be
  IP-blocked.
- **fake-IP** (chosen): the exit side is already done (server connects `DomainPort` via its
  clean DNS), the whole fake range routes into the TUN so "which traffic to tunnel" is
  answered for free, resolution at the exit bypasses poisoning, and no UDP relay is needed
  (responses forged locally). Matches Clash/Mihomo's fake-ip mode.

## Consequences

- Known blind spots (the app must use plaintext UDP/53 to our resolver):
  - DoH/DoT encrypted DNS bypasses interception → app gets a real IP, IpPort relay, blocked
    IPs still fail.
  - Apps connecting to hardcoded IPs (no DNS) never enter the fake-IP map → IpPort relay.
  - QUIC/UDP has no relay this stage; apps usually fall back to TCP.
- The fake-IP map is in-memory and not reclaimed this stage; a client restart drops it, so
  a TCP SYN to a stale (cached) fake-IP with no mapping is refused — the app re-queries DNS
  and self-heals. Forged responses use a short TTL to shrink the stale window.
