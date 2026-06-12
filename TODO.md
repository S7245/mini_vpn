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

1. ~~Arbitrary ports (Stage 9)~~ — DONE.
2. ~~DNS over tunnel or fake-IP (Stage 11)~~ — DONE (fake-IP).
3. ~~UDP relay for QUIC/HTTP3 and live streaming~~ — first cut DONE (Stage 12):
   UDP rides a new QUIC datagram data plane (ADR-0003), not the TCP/yamux tunnel.
   Quality/scale hardening + TCP→QUIC migration are the follow-on QUIC track below.
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

### Data plane → TUIC protocol on QUIC (ADR-0004, supersedes self-built transport)

ADR-0003's north star (unify on QUIC) is now realized via the **TUIC v5 protocol** on quinn
(**ADR-0004**), NOT a self-designed transport (that work was reverted). Priority rule:
「用成熟方案拿到最好体验」>「复用自研引擎」. **Client-only**: the exit is a mature **sing-box TUIC
server** (interop = best experience, zero server code from us). Stage 12's UDP-over-QUIC-datagram
≈ TUIC native mode and is reused. quinn 0.10 already exposes `export_keying_material` (TUIC auth).

- **Stage 13 — TUIC client** (staged 13a→13d; dual-run `MINI_VPN_UPSTREAM=legacy|tuic`):
  - ~~**13a — TCP relay over TUIC Connect**~~ — DONE (2026-06-10): Authenticate (token via
    keying-material) + per-flow Connect bi-stream behind a `ProxyUpstream` trait; **verified end-to-end
    against a real sing-box TUIC server** (curl https://1.1.1.1 → Cloudflare 301, US egress).
  - ~~**13b — UDP relay over TUIC `Packet`**~~ — DONE (2026-06-11): native QUIC datagram, one u16
    assoc-id per UDP 4-tuple (`AssocTable`), `send_udp` + a self-healing downlink datagram pump +
    periodic Heartbeat over the *same* authenticated connection as TCP; **verified end-to-end against a
    real sing-box** (`dig @1.1.1.1 example.com/facebook.com` → real A records; `curl https://1.1.1.1`
    still HTTP/2 301 = TCP non-regression). See `docs/tech/13b-tuic-udp-packet.md`. Deferred: the
    quic-stream oversized fallback (native drops + counts datagrams over the QUIC datagram limit).
  - **13c — 0-RTT reconnect + keepalive clarification** — PARTIAL (2026-06-11). Scope narrowed (real
    migration + battery-adaptive heartbeat moved to the mobile-readiness stage; they need iOS/Android
    packet-flow backends to truly accept).
    - DONE: **on-demand TUIC Heartbeat** — fires only while UDP is recently active (`last_udp_activity`
      + `should_send_heartbeat`); pure-TCP sessions rely on the QUIC keep-alive PING. Two keepalive
      layers clarified (QUIC PING = connection liveness; TUIC Heartbeat = UDP-session liveness).
    - **0-RTT DEFERRED (ecosystem wall)**: quinn 0.10 / rustls 0.21 cannot `export_keying_material`
      during the 0-RTT (handshake-incomplete) phase, and TUIC's auth token derives from it — so TUIC
      0-RTT auth is **structurally impossible on this stack**. Verified vs sing-box (`zero_rtt_handshake`
      on): auth fails → self-healing 1-RTT fallback, traffic flows. The 0-RTT code path + the
      `MINI_VPN_TUIC_ZERO_RTT` switch are kept (default **OFF**, opt-in only).
      - **Follow-up to actually get 0-RTT**: bump quinn/rustls to a version that exposes 0-RTT
        keying-material (early-exporter) export — the version bump ADR-0004 foresaw. Best folded into
        the **mobile-readiness stage** (weak-net / radio-sleep is where 0-RTT fast resume pays off),
        alongside real connection migration + adaptive heartbeat. Re-validate against sing-box after the bump.
  - **13d — retire legacy** (yamux + Stage-12 self-server QUIC datagram + self server).

#### Transport / protocol extensibility — two tiers

- **Proxy-transport trait** (the abstraction Stage 13 builds): pluggable L4/L7 proxy protocols sharing
  "intercept flow → relay to exit → exit dials target". TUIC = impl #1; **VLESS+REALITY (TCP)** = planned
  #2, the GFW-crackdown-resistant fallback (REALITY ≈ indistinguishable from real HTTPS; QUIC/TUIC is a
  censorship target — GFW does QUIC-Initial SNI blocking, mitigated only by SNI-slicing). Trojan/SS/
  Hysteria could follow the same trait.
  - **Protocol selection** (REALITY stage, not Stage 13 — meaningless with one protocol): **auto-failover
    by default** (urltest-style: TUIC → TCP/REALITY on QUIC block/timeout/loss) + a **manual 3-way
    override** (auto / force-UDP / force-TCP).
- **Data-plane "tunnel mode"** (separate future path, NOT behind the proxy trait): WireGuard / OpenVPN
  forward raw IP packets (L3), bypassing smoltcp/fake-IP/per-flow — a different mode for "general VPN
  (non-circumvention)" use, and easily GFW-blocked without obfuscation. Add only if that product need
  appears. **StealthVPN (Astrill, proprietary/closed): not doing** (no open spec).

#### Mobile (iOS/Android) readiness
- Abstract packet I/O behind a trait (current `tun` crate backend; **iOS NEPacketTunnelFlow**; **Android
  VpnService fd**) and library-ize the core (config struct in, not env/CLI) for FFI.
- Adaptive keep-alive (battery vs NAT timeout; current 5s is battery-hostile) + memory budget (iOS
  NetworkExtension ~15–50MB limit) + QUIC 0-RTT for fast resume after radio sleep.
- UDP→TCP upstream fallback where UDP/QUIC is blocked (ties into the protocol selector above).

#### Other follow-ons
- **DNS hardening**: intercept all `:53` (not just 198.18.0.1) / known DoH endpoints.
- **Scale/ops** (only if our own server returns): session-table hardening, multi-upstream/failover,
  graceful drain, control-plane discovery + metrics (external stores belong here, not the hot path).

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
- ~~**Large HTTP/2 / multiplexed streams fail mid-transfer with `bad decrypt`**~~
  RESOLVED 2026-06-04 (commit b476854): root cause was `send_slice` dropping the
  unwritten tail when the tx buffer was full; fixed with a per-handle downlink
  pending buffer that never drops bytes. Verified: `curl https://www.facebook.com/`
  downloads a full ~415KB repeatedly with no bad decrypt.

### Client-side concurrency bottlenecks (knife1 findings, 2026-06-12)

Quantified by the mock loopback harness (`src/harness.rs`, feature `harness`);
full data in `docs/tech/2026-06-12-knife1-bottleneck-findings.md`. For Rules.md ③
(大并发) the dominant cost is **client-side**, not the network:

- **P0 #1 — `run_event_loop` sweeps `registry.all_handles()` O(total listener slots)
  every tick.** relay/call scales linearly with `distinct_ports × pool_size` and is
  independent of active connections (0.13→0.45ms as slots 512→2048, N fixed). knife2:
  process only handles with readiness (event/dirty-set), not a full per-tick sweep.
- **P0 #2 — per-port `pool_size` is a hard concurrency ceiling.** 256 conns to ONE
  port with default pool=2 complete only 2/256; hot-port bursts stall (rearm-under-churn
  doesn't drain — overlaps the first-SYN-refused race below). knife2: elastic per-port
  pool / reuse + accept backlog. Global ceiling today ≈ 64 ports × pool 2 = 128.
- **P1 #4 single-thread select ceiling** (throughput halves as per-tick work grows;
  coupled to #1) and **P2 #5 128KB/socket** (2048 slots ≈ 256MB, mostly idle swept slots).
- **#3 single QUIC connection** (HOL under concurrency) is unmeasured by the mock —
  needs the end-to-end sing-box probe in the findings doc. Same concern as the yamux
  HOL note above, now on the TUIC/QUIC data plane.

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
