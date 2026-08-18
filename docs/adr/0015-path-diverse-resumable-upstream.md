# Add a server-owned, path-diverse resumable upstream for enhanced continuity

Date: 2026-08-18

Status: **ACCEPTED**

Standard TUIC remains a supported compatibility profile, but it is no longer
the sufficient architecture for mini_vpn's enhanced-continuity profile. That
profile will use a mini_vpn-owned upstream protocol and server-side session
owner reachable through at least two independent ingress paths.

The session owner keeps the one Target TCP socket and assigns application byte
offsets. Two L4 ingresses forward encrypted transport packets to that owner
without terminating session authentication or owning Target state. A
replacement transport leg attaches to the same session and replays only
unacknowledged bytes. UDP uses per-flow packet sequence, independent per-leg
queues, receiver feedback, deduplication, and bounded hot-path failover. A
failed leg must not block the other leg.

This partially supersedes ADR-0004's client-only decision. ADR-0004 remains
valid for standard TUIC interoperability and ordinary compatibility mode. It
does not govern the new enhanced-continuity profile. ADR-0005's Cubic and
native-datagram default also remains valid for the compatibility profile; it
does not authorize all-stream UDP as a substitute for path diversity.

## Why the boundary changed

Knife15 exhausted the bounded standard-TUIC decision tree:

- multiple materially different Exit paths failed the strict continuity and
  UDP gates;
- Tier B then failed its first valid run at `3.076146%` UDP reverse loss;
- paired evidence placed `2.486849%` before observed Exit outer TUIC egress;
- existing TUIC connection pools can redirect new flows but cannot move an
  established TCP stream or preserve one Target socket across independent
  connections;
- a second independent Exit cannot inherit another host's kernel TCP socket.

The required ownership therefore cannot live only in the client or in a
standard sing-box TUIC server. One stable server-side session owner must own
Target sockets and application acknowledgements while transport legs remain
replaceable.

## Considered options

- **Repeat or tune standard single-path TUIC** — rejected by the frozen
  Knife15 stop rule and real paired evidence.
- **Add more TUIC pool connections** — useful for future opens, but cannot
  resume an established stream or move its Target socket.
- **Use two independent standard Exits** — cannot share Target socket and byte
  ownership; replaying an open would create a different Target connection.
- **Carry every UDP packet on a QUIC stream** — rejected by prior high-rate
  collapse and head-of-line behavior.
- **Continuously duplicate all UDP over two paths** — rejected as the default:
  the measured HK reverse baseline cannot safely carry two full workload
  copies.
- **One session owner behind two independent ingress paths (chosen)** — keeps
  Target ownership stable while allowing transport replacement and bounded
  UDP path switching.

## Consequences

- mini_vpn now owns a small server component and a versioned protocol for the
  enhanced profile.
- TCP acknowledgement means application-byte ownership, not QUIC ACK. Replay
  buffers are bounded and apply backpressure rather than dropping bytes.
- UDP keeps real-time semantics: expired packets may be dropped explicitly;
  recovery does not create unbounded retransmission or ordering delay.
- TUN, smoltcp, fake-IP DNS, D16, Endpoint pacing, MTU, Cubic, GSO, and the
  standard TUIC adapter remain unchanged during branch-by-abstraction.
- The first stage does not promise process-crash persistence or migration of a
  Target socket between two session-owner processes.
- M3 stays blocked until local deterministic gates and real two-path bounded
  qualification pass.
