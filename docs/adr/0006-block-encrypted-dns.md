# Block encrypted DNS (DoH / DoT / DoQ / DoH3) to keep fake-IP routing working

The client **actively blocks** connections to known encrypted-DNS endpoints so the application
falls back to plaintext DNS, which we forge into a **fake-IP**. Without this, an app that uses
encrypted DNS resolves to a **real** address, bypasses the fake-IP map, and — because that real
address is not routed into the tunnel (and is GFW-blocked when reached directly) — the connection
**fails**. This is the fake-IP-proxy family's standard posture (Clash/Surge do the same).

Detection + action (decided in 刀4 grill, 2026-06-18; see `docs/tech/2026-06-18-knife4-connect-success-*`):

- **DoT / DoQ** = port **853** → block (any IP, TCP or UDP).
- **DoH / DoH3** = port **443** AND the destination is a known DoH endpoint:
  - resolved **domain** ∈ built-in DoH-domain list (when the app resolved it through our fake DNS), OR
  - destination **IP** ∈ built-in DoH-bootstrap-IP list (when the app hardcodes a bootstrap IP).
- The decision lives in one place, `resolve_target`, which both the TCP first-packet path and the UDP
  datagram path funnel through, so a single rule covers all four transports.
- **Action**: TCP → **RST** (reuse `rearm_socket`, fast fallback); UDP → **silent drop**. Either way the
  encrypted-DNS attempt fails and the app retries over plaintext DNS → fake-IP → tunnel.
- We **do not** block by raw port :443 (that is normal HTTPS / HTTP-3 / our own video) — only the
  domain/IP allowlist on :443, so normal traffic is untouched.

## Considered Options

- **Do nothing (status quo)** — rejected: default-DoH browsers (Chrome/Safari/Firefox) get real IPs and
  fail to connect; the fake-IP design is silently defeated for the most common client.
- **Honour encrypted DNS, route its real IPs through the tunnel instead** — rejected: that means
  abandoning fake-IP / domain-based routing for those flows (loses per-domain rules + exit-side
  resolution that avoids DNS pollution), and would require full-tunnelling every real IP. It is a
  different architecture (L3 full-tunnel), not this proxy.
- **Redirect encrypted DNS to our own DoH/DoT server** — rejected: requires terminating TLS / speaking
  the encrypted-DNS protocol; heavy, and gains nothing over plaintext fallback.
- **Block encrypted DNS, force plaintext fallback (chosen)** — minimal, one decision point, no TLS
  termination; matches the fake-IP-proxy norm.

## Consequences

- **A user who deliberately wants encrypted DNS does not get it while the tunnel is on.** That is the
  accepted trade-off: connectivity-via-fake-IP over honouring DoH. (Plaintext DNS is safe here because
  it is answered **locally** by our fake resolver and never leaves the device as plaintext on the wire.)
- The block list is **built-in and best-effort, not exhaustive**: default-browser providers
  (Cloudflare / Google / Quad9) are covered by domain **and** bootstrap IP; exotic/opt-in providers
  (NextDNS, dns.sb, self-hosted) may slip and are tuned from real-egress acceptance. A general catch
  (TLS **SNI** inspection on :443) is deliberately deferred until measured to leak.
- Risk of over-block is minimal: :853 is DNS-only; the :443 list uses **dedicated** DoH hostnames
  (`dns.google`, `cloudflare-dns.com`, …) and anycast resolver IPs, not general-purpose hosts.
- `udp_relay_mode` / congestion choices (ADR-0005) and fake-IP (ADR-0002) are unaffected; this is a new
  filter in front of `resolve_target`, not a change to the data plane.
