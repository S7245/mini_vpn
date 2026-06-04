# TODO

## Roadmap: TUN transparent proxy (Target extraction)

The TUN client must relay each intercepted connection to its real **Target**
(the destination `IP:port` the OS routed into the TUN), not a hardcoded address.
Split into two stages so the hot-path rewrite stays isolated and reversible:

- **Stage 8 — fixed-port Target extraction (stepping stone).**
  Enable smoltcp AnyIP + a default route so arbitrary destination IPs are accepted
  on a single fixed listen port. Extract the Target from `local_endpoint()` and use
  it instead of the removed hardcoded target. Deliverable is mechanism-only:
  verifiable by routing a real port-80 host into the TUN; cannot browse real sites yet.
- **Stage 9 — SYN-sniffing dynamic ports.**
  Parse inbound IP/TCP headers (etherparse) in the rx path; on a SYN to a port with
  no listener, create a smoltcp socket listening on that port; maintain a connection
  table with idle reclamation. Only after this can arbitrary ports (incl. 443) work.

### Gating dependencies for real browsing (e.g. facebook from Shenzhen)

Target extraction alone does NOT make real sites work. In order of blocking severity:

1. Arbitrary ports (Stage 9) — needed for 443/HTTPS.
2. DNS over tunnel or fake-IP — blocked domains resolve to poisoned IPs locally,
   so the extracted Target IP would be wrong. Required for any GFW-blocked domain.
3. UDP relay for QUIC/HTTP3 and live streaming (Rules.md UDP scenario).
4. Exit IP reputation — datacenter IPs trigger target-site risk control; protocol-independent.
5. MSS clamping / MTU handling — prevents large packets from stalling through the tunnel.

### Architectural constraints to revisit

- Yamux multiplexes all substreams over one TLS/TCP connection -> TCP head-of-line
  blocking under high concurrency (Rules.md high-concurrency scenario). May need
  multiple upstream connections or a different transport.
- The remote Yamux session opens on the first local payload, so server-speaks-first
  protocols (no client data before the server talks) never trigger relay. Pre-existing
  limitation, unchanged by Stage 8; revisit if such protocols are needed.

## Future architecture topics

These cut across multiple stages and may need their own design before being scheduled.

### Multi-Hop

Chain multiple Upstream hops (e.g. Shenzhen → HK → US) for jurisdiction layering
and exit-IP rotation. Affects relay protocol, connection-table, and TLS chaining.

### fake-IP / DNS interception

Core implemented in Stage 11 (ADR-0002): intercept DNS in the TUN, forge A responses
with `198.18.0.0/15` placeholders, map fake-IP↔domain, rewrite TCP target to DomainPort
so the Upstream resolves at the exit. Follow-ups not in Stage 11:

- **DoH/DoT interception**: encrypted DNS (browser/system) bypasses the plaintext UDP/53
  resolver → app gets real IP → blocked IPs still fail. Intercept known DoH endpoints or
  guide users to disable in-app DoH.
- **Hardcoded-IP domains**: apps connecting to a literal IP never enter the fake-IP map;
  stays IpPort. No clean fix without app cooperation.
- **QUIC/UDP relay**: needed for QUIC (UDP/443) and UDP services; until then apps usually
  fall back to TCP. (This is also the separate "UDP relay" roadmap task.)
- **fake-IP reclamation / LRU**: pool is never reclaimed this stage (131k addresses); add
  LRU + TTL-based eviction if it ever matters.
- **Switch DNS codec to hickory-proto** when any of: parsing real upstream responses
  (compression pointers), EDNS0/DNSSEC/DoH, more record types (CNAME/HTTPS/SVCB), or
  hardening against malicious packets. Only the dns.rs codec module changes; interface stable.
- **First-SYN-to-fresh-fake-IP can get `connection refused`** (observed Stage 11 e2e):
  curl does NOT retry TCP on refused (unlike on timeout), so a one-off RST kills the
  connect. Likely a race between the SYN inspector building the listener pool and the
  SYN being processed in the same poll. Add a tolerance (pre-arm, or brief retry).
- **Large HTTP/2 / multiplexed streams fail mid-transfer with `bad decrypt`** (observed
  Stage 11 e2e): TLS handshake succeeds and the first request returns a full 200, but
  high-throughput / many-stream transfers corrupt mid-flight. Investigate relay
  byte-stream ordering/buffering under load (yamux substream multiplexing, relay copy).

### Scale & reconnection resilience (100+ servers / 5000+ users)

Stage 10 ships client-side full-jitter reconnect as the baseline. To survive
reconnect storms and scale, layered work beyond client code:

- **Architecture**
  - Multiple Upstream addresses + failover (rotate / health-aware pick); spreads
    5000 users across the server pool (~50/server).
  - Control plane / service discovery: clients pull a live healthy-server list with
    weights instead of hardcoding one address; enables dynamic eviction + steering.
  - L4 load balancer (LVS / NLB / nginx stream) in front of the pool. NOTE: a yamux
    long connection is pinned to one backend, so if that backend dies the connection
    still drops — client reconnect+jitter remains the foundation, LB does not replace it.
  - Connection epoch/generation to discard stale relays after reconnect (anti-crosstalk).
  - Application-layer heartbeat to detect half-open connections instead of waiting for
    TCP timeout.
- **Ops / deployment**
  - Rolling restart + graceful drain (stop accepting, let existing connections drain)
    — turns "5000 disconnect at once" into "a few dozen per batch"; most effective
    anti-thundering-herd measure, more so than client jitter.
  - Health checks with automatic node eviction; server-side accept rate limiting +
    connection cap + SYN cookies to avoid being overwhelmed.
  - Metrics (reconnect rate, concurrent connections, handshake failure rate) for
    observability + alerting on reconnect storms.
  - Cross-AZ / multi-region redundancy so single-point failure does not affect everyone.

## Deferred Work

These items are intentionally out of scope for the current stage, but are likely to be needed later.

### TLS / Certificates

- Support multiple server certificates selected by SNI.
- Support certificate hot reload without restarting the server.
- Support client certificate authentication (mTLS).
- Unify TLS config loading between `client-direct` and `client-tun`.
- Add explicit certificate expiry diagnostics at startup.
- Consider separating CA bundle path from leaf certificate path more strictly in default dev assets.

### Runtime / Reliability

- Add reconnect policy for `client-tun` upstream TLS/Yamux connection.
- Add upstream failover support with multiple server addresses.
- Replace remaining runtime `unwrap()` paths in TLS material loading with structured errors.
- Add retry/backoff strategy for transient upstream connection failures.

### Testing / Tooling

- Add scripted local dev certificate generation with stable output paths.
- Add an end-to-end local test recipe covering `localhost` and `example.com` SANs.
- Consider adding integration tests for TLS config loading with temporary test certificates.

### Product / Config

- Consider sharing a single top-level config model across `server`, `client-direct`, and `client-tun`.
- Add config file support in addition to environment variables.
- Evaluate whether `cert_path`, `key_path`, and `ca_path` should be documented in a single deployment guide.
