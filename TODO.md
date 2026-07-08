# TODO

## Current Knife14 Status (2026-07-08)

Knife14fp found a mandatory high-throughput prerequisite: the exit VPS `.33`
had Linux socket buffer caps/defaults of only `212992B`, which capped both
mini_vpn and a mature sing-box client around the `20-30 Mbit/s` band. `.33` is
now persistently raised to:

```text
net.core.rmem_max = 16777216
net.core.wmem_max = 16777216
net.core.rmem_default = 1048576
net.core.wmem_default = 1048576
```

After restarting sing-box under those limits, the mature sing-box client reached
`185.242 Mbit/s` receiver and a later current-window mature sing-box repeat
still reached `173/173 Mbit/s`. The VPS install/optimization checklist is
captured in
`docs/tech/2026-07-08-vps-install-and-optimization-guide.md`.

Do **not** lower the target to `30 Mbit/s`. The `100+ Mbit/s` target is
reachable on the current `.27/.33/.77` topology. Final mini_vpn acceptance is
not closed because mini_vpn's reverse TUIC TCP data path is still below the
mature-client baseline.

Latest discriminator:

- Knife14fp mini_vpn safe1200 reverse-first P1 reached a reported
  `114.000 Mbit/s` receiver with high intervals averaging `189.483 Mbit/s`, but
  iperf exited by external timeout and close-tail accounting was dirty.
- Knife14fq repeated from a clean `f8765c1` clone with `IPERF_TIMEOUT_SECS=120`.
  It exited normally and cleaned close-tail accounting, but regressed to
  `35.700 Mbit/s` receiver with local pressure/headroom gating
  (`may_recv_false=13594`, `headroom_deferred_bytes=17512861`) while QUIC
  loss/blocking and TUN drops stayed `0`.
- Knife14fu restored the ordered default after rejecting unordered TUIC chunk
  reassembly, but the current branch at `6eb52e9` collapsed to a no-data
  reverse-first shape: `0.349/0.046 Mbit/s`, with local pending/headroom/TUN
  and QUIC loss/blocking surfaces clean.
- Knife14fw reran the known Knife14fp-era commit `f8765c1` from a clean
  detached worktree and got `19.900/18.700 Mbit/s` with clean close-tail and
  healthy direct baselines. This shows the current branch has a worse no-data
  regression, but the fp-era code still does not reliably reproduce `100+` in
  the current window.
- Knife14fx ran a mature sing-box `v1.13.14` client in the same current
  environment and reached `173/173 Mbit/s`. Therefore VPS config can reach
  `100+`; the remaining blocker is mini_vpn client/data-plane behavior.
- Knife14gm added local TDD and code for the Knife14gl low-byte ordered stream
  gap. Commit `9e50a32` made active payload-shaped ordered streams below the
  old `64KiB` ACK/window service gate reach both the ACK-drain due predicate
  and the relay supervisor polling gate. The focused safe1200 reverse-first P1
  retry restored data movement (`23.2/21.2 Mbit/s`) but did **not** exceed
  `30 Mbit/s`; the run stayed `low_average` with local-pressure/credit-edge
  signals and clean TUN/QUIC surfaces.
- Knife14gn added TDD and code for accepted downlink flush progress. Commit
  `97cb55e` moves downlink credit-controller feedback until after
  `send_slice`, so bytes accepted into smoltcp count as useful local egress
  progress and wake read-credit publishers. The focused safe1200 reverse-first
  P1 completed normally but regressed to `18.8/18.0 Mbit/s`, **not** `>30`.
  The important discriminator changed: `local_pressure=0`, pressure/drop debt
  and TUN drops stayed `0`, while `connection_stream_frames_pending` gaps
  remained at `3405ms`.
- Knife14go added TDD and code for active connection-RX self-wake. Commit
  `39112ca` makes pending TUIC reads self-wake for both
  `connection_stream_frames_pending` and `connection_rx_no_stream_frames`.
  Focused safe1200 reverse-first P1 improved only to `24.6/22.4 Mbit/s`, still
  **not** `>30`. The useful discriminator: data-stream polling cadence improved
  (`data_poll_gap_max_ms=43`, `self_wake_armed=11702`,
  `self_wake_fired=8726`), but ordered delivery still had multi-second gaps
  (`data_read_gap_max_ms=3872`, `data_pending_gap_max_ms=3006`) and the final
  burst reintroduced one local pressure edge (`downlink_backpressure
  pause_edges=1`, `read_credit_pause_updates=1`).
- Knife14gp added T13/T14 TDD and code for since-last-pending TUIC stream
  diagnostics plus post-flush residual pressure debt. Commit `bd0d264` removed
  the obvious false pre-flush debt shape: the focused run had
  `downlink_backpressure pause_edges=0 resume_edges=0`, clean TUN drops,
  clean close-tail accounting, and clean QUIC loss/blocking. It still regressed
  to `15.0/13.5 Mbit/s`, **not** `>30`. The new discriminator showed many
  repeated multi-second pending windows with
  `conn_rx_stream_frames_since_pending=0`, meaning older
  `connection_stream_frames_pending` evidence was often stale since the last
  successful stream read. The remaining active branch is useful read-credit
  collapse to `1200B` under residual local pressure/headroom evidence, not VPS
  capacity, iperf3, MTU, stale pool, broad QUIC windows, or simply sleeping
  stream polling.
- Knife14gq implemented the B0-B7 feature-flagged buffered downlink
  architecture. Commit `8a85ce7` promoted `SocketCtx.downlink_pending` into an
  explicit buffered layer for TUIC read-credit decisions. The B7 focused
  safe1200 reverse-first P1 did **not** pass: `18.3/17.2 Mbit/s`, below the
  `>30 Mbit/s` gate. The old read-service collapse was fixed
  (`remote_read_service_len_min=65536`,
  `remote_batch_limit_bytes_min=524288`, `read_credit_pause_updates=0`), but
  throughput stayed `low_average` with
  `attribution: local_downlink_backpressure`, one local pressure pause/resume
  edge, and multi-second ordered-stream pending gaps. Buffered read credit is
  useful but not sufficient; the next architecture target is local
  writer/egress cadence, not another credit-floor or self-wake tweak.
- Knife14gs implemented the next G2-G5 local egress service lane after the
  Knife14gr performance gate. The main loop now has an explicit bounded local
  egress window that can alternate TUN RX ACK intake, `iface.poll`, dirty
  downlink flush, and `flush_tx` until a target, no-progress, hard-pause, or
  cycle-budget stop. Local gates passed, including the code-level G5 capacity
  floor of `128KiB/5ms` active window. This is a local architecture/capacity
  gate only; VPS acceptance still must prove first `>30 Mbit/s` before any
  `100+ Mbit/s` claim.

Current result doc:

- `docs/tech/2026-07-08-knife14gq-buffered-downlink-results.md`
- Performance gate for the next slice:
  `docs/tech/2026-07-09-knife14gr-egress-cadence-reachability.md`
- Local G2-G5 result:
  `docs/tech/2026-07-09-knife14gs-local-egress-service-g5-results.md`

Current follow-up queue:

1. Keep the ordered-default gate, Knife14gm low-byte ACK/window service,
   Knife14gn accepted-flush progress feedback, Knife14go active connection-RX
   self-wake, Knife14gp since-last-pending diagnostics/post-flush debt
   separation, and Knife14gq buffered read-credit isolation; unordered
   reassembly remains diagnostic only.
2. Do not continue self-wake timer work as the main branch. Knife14go proved
   frequent polling, Knife14gp proved repeated pending windows often have no
   fresh `conn_rx_stream_frames_since_pending` progress, and Knife14gq proved
   preserving a useful read-service floor alone does not clear `30 Mbit/s`.
3. Do not keep changing local pressure-credit constants blindly. Knife14gq
   removed the `1200B` credit-collapse symptom but still failed at
   `17.2 Mbit/s`.
4. Knife14gs added the first explicit local writer/egress service lane. Keep
   its bounded stop reasons and diagnostics; do not convert it into an
   unbounded pump or bypass hard pending/drop/terminal guards.
5. The next focused acceptance target is still first `>30 Mbit/s` on the same
   safe1200 reverse-first P1, not yet `100+`. Do not claim the `100+ Mbit/s`
   path is clear until a later slice passes `>30` and then a clean `100+`
   repeat exits normally.
6. For G6 parsing, require the new `tcp-local-egress-service` diagnostics to
   show useful cycles/accepted bytes and a low no-progress share. If VPS still
   stays below `30 Mbit/s`, compare those counters against `tcp-tun-rx-drain`,
   `tcp-downlink-flush`, TUN drops, and QUIC blocking before changing code.

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
3. ~~UDP relay for QUIC/HTTP3 and live streaming~~ — DONE through TUIC:
   UDP now rides TUIC `Packet` over QUIC datagrams (ADR-0004), oversized packets have
   uni-stream fallback (刀3), and native+cubic high-rate soak passed (刀3.5). Re-test
   on new exits/paths, but this is no longer a known missing feature.
4. Exit IP reputation — datacenter IPs trigger target-site risk control; protocol-independent.
5. MSS clamping / MTU handling — prevents large packets from stalling through the tunnel.

### Historical architectural constraints retired by TUIC

- Yamux/TLS is no longer the data plane. TUIC Connect uses one QUIC bi-stream per TCP
  session, and the legacy server/yamux path was retired in 13d.
- Server-speaks-first protocols remain outside the current proxy trigger model (the
  local relay still opens on first local payload), but this is now a product/protocol
  requirement question, not a yamux limitation.

## Future architecture topics

These cut across multiple stages and may need their own design before being scheduled.

### Prioritized future task backlog (post-14d)

Keep this list as the canonical next-work queue after 刀14c/14d. Items are ordered by current leverage.
The 2026-06-30 US-client result moved connection-pool work behind downlink/backpressure + async-open
validation; see `docs/tech/2026-06-30-knife14b-usclient-results.md` and
`docs/tech/2026-06-30-knife14d-downlink-reap-open-spec.md`.

1. **Re-run the US-client suite after 14c+14d.** Keep the same Client/Exit/Target shape and upload the
   generated markdown/tar bundle for analysis.
2. **Use the bundle to choose the next knife.** If reverse/P2 still fails, read the TCP diagnostics and loop
   profile first; do not jump straight to connection-pool work.
3. **#3 connection-pool spike.** Only start this if post-14d measurements still prove a single TUIC/QUIC
   connection is the wall. Otherwise do not add pool complexity.
4. **Mobile/productization core seam.** Add packet I/O traits for macOS tun / iOS `NEPacketTunnelFlow` /
   Android `VpnService`, library-style config structs, and config injection for knobs such as `cc` and
   `udp_mode`.
5. **0-RTT / weak-network resume.** Revisit quinn/rustls once early exporter support is available, and pair
   it with adaptive keepalive / mobile radio-sleep behavior.
6. **DNS edge hardening.** Decide policy for IPv6 DNS, split-horizon/internal domains, exotic multi-question
   DNS, hardcoded-IP apps, and when to switch parsing to `hickory-proto`.
7. **Anti-censorship resilience beyond TCP failover.** Evaluate UDP-over-VLESS/TCP fallback only if QUIC is
   blocked and UDP service continuity matters more than latency/complexity.
8. **Scale / ops.** If multi-server or many-user operation returns, design service discovery, weighted
   upstream health, graceful drain, metrics/alerting, and multi-region steering.
9. **Longer-horizon product modes.** Multi-Hop, L3 tunnel mode, REALITY Vision flow / broader TLS cipher
   fingerprinting, and exit-IP reputation handling are product-line decisions, not current data-plane blockers.

### Data plane → TUIC protocol on QUIC (ADR-0004, supersedes self-built transport)

ADR-0003's north star (unify on QUIC) is now realized via the **TUIC v5 protocol** on quinn
(**ADR-0004**), NOT a self-designed transport (that work was reverted). Priority rule:
「用成熟方案拿到最好体验」>「复用自研引擎」. **Client-only**: the exit is a mature **sing-box TUIC
server** (interop = best experience, zero server code from us). Stage 12's UDP-over-QUIC-datagram
≈ TUIC native mode and is reused. quinn 0.10 already exposes `export_keying_material` (TUIC auth).

- **Stage 13 — TUIC client** (13a→13d complete; legacy/yamux retired):
  - ~~**13a — TCP relay over TUIC Connect**~~ — DONE (2026-06-10): Authenticate (token via
    keying-material) + per-flow Connect bi-stream behind a `ProxyUpstream` trait; **verified end-to-end
    against a real sing-box TUIC server** (curl https://1.1.1.1 → Cloudflare 301, US egress).
  - ~~**13b — UDP relay over TUIC `Packet`**~~ — DONE (2026-06-11): native QUIC datagram, one u16
    assoc-id per UDP 4-tuple (`AssocTable`), `send_udp` + a self-healing downlink datagram pump +
    periodic Heartbeat over the *same* authenticated connection as TCP; **verified end-to-end against a
    real sing-box** (`dig @1.1.1.1 example.com/facebook.com` → real A records; `curl https://1.1.1.1`
    still HTTP/2 301 = TCP non-regression). See `docs/tech/13b-tuic-udp-packet.md`. Oversized-packet
    stream fallback was completed in 刀3.
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
  - ~~**13d — retire legacy** (yamux + Stage-12 self-server QUIC datagram + self server)~~ — DONE.

#### Transport / protocol extensibility — two tiers

- **Proxy-transport trait** (the abstraction Stage 13 builds): pluggable L4/L7 proxy protocols sharing
  "intercept flow → relay to exit → exit dials target". TUIC = impl #1; **VLESS+REALITY (TCP)** is impl #2
  and is wired into health-aware failover (刀6→刀10). Trojan/SS/Hysteria could follow the same trait if
  a real product need appears.
  - **Protocol selection** is implemented as `MINI_VPN_UPSTREAM=tuic|reality|failover`; failover keeps
    UDP on TUIC and switches TCP to REALITY when QUIC/TUIC is unhealthy.
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
- **DNS hardening**: core DoH/DoT/DoQ blocking and all plaintext `:53` hijack are done (刀4/刀5).
  Remaining work is edge-case policy: IPv6 DNS, split-horizon/internal domains, exotic multi-question DNS,
  or moving to hickory-proto when richer DNS record handling is needed.
- **Scale/ops** (only if our own server returns): session-table hardening, multi-upstream/failover,
  graceful drain, control-plane discovery + metrics (external stores belong here, not the hot path).

### Multi-Hop

Chain multiple Upstream hops (e.g. Shenzhen → HK → US) for jurisdiction layering
and exit-IP rotation. Affects relay protocol, connection-table, and TLS chaining.

### fake-IP / DNS interception

Core implemented in Stage 11 (ADR-0002): intercept DNS in the TUN, forge A responses
with `198.18.0.0/15` placeholders, map fake-IP↔domain, rewrite TCP target to DomainPort
so the Upstream resolves at the exit. Follow-ups not in the original Stage 11, with current status:

- ~~**DoH/DoT interception**: encrypted DNS (browser/system) bypasses the plaintext UDP/53
  resolver → app gets real IP → blocked IPs still fail.~~ DONE in 刀4 for known encrypted DNS endpoints.
- **Hardcoded-IP domains**: apps connecting to a literal IP never enter the fake-IP map;
  stays IpPort. No clean fix without app cooperation.
- ~~**QUIC/UDP relay**: needed for QUIC (UDP/443) and UDP services; until then apps usually
  fall back to TCP.~~ DONE via TUIC Packet + native/cubic default.
- ~~**fake-IP reclamation / LRU**: pool is never reclaimed this stage (131k addresses); add
  LRU + TTL-based eviction if it ever matters.~~ DONE in 刀2 with refcount + idle sweep.
- **Switch DNS codec to hickory-proto** when any of: parsing real upstream responses
  (compression pointers), EDNS0/DNSSEC/DoH, more record types (CNAME/HTTPS/SVCB), or
  hardening against malicious packets. Only the dns.rs codec module changes; interface stable.
- **First-SYN-to-fresh-fake-IP refused race is closed**. 刀4 acceptance confirmed knife2's same-frame
  listener creation and elastic spare listener logic eliminated the observed refused race.
- ~~**Large HTTP/2 / multiplexed streams fail mid-transfer with `bad decrypt`**~~
  RESOLVED 2026-06-04 (commit b476854): root cause was `send_slice` dropping the
  unwritten tail when the tx buffer was full; fixed with a per-handle downlink
  pending buffer that never drops bytes. Verified: `curl https://www.facebook.com/`
  downloads a full ~415KB repeatedly with no bad decrypt.

### Client-side concurrency bottlenecks (knife1/12/13 findings)

Quantified by the mock loopback harness (`src/harness.rs`, feature `harness`) and later
real-egress probes. Full data starts in `docs/tech/2026-06-12-knife1-bottleneck-findings.md`;
ADR-0013 records the 100M attribution update.

- ~~**P0 #1 — `run_event_loop` sweeps `registry.all_handles()` O(total listener slots)
  every tick.** relay/call scales linearly with `distinct_ports × pool_size` and is
  independent of active connections (0.13→0.45ms as slots 512→2048, N fixed). knife2:
  process only handles with readiness (event/dirty-set), not a full per-tick sweep.~~ DONE in 刀2.
- ~~**P0 #2 — per-port `pool_size` is a hard concurrency ceiling.** 256 conns to ONE
  port with default pool=2 complete only 2/256; hot-port bursts stall (rearm-under-churn
  doesn't drain — overlaps the first-SYN-refused race below). knife2: elastic per-port
  pool / reuse + accept backlog.~~ DONE in 刀2.
- **P1 #4 single-thread select ceiling** was re-tested by 刀12 LoopProfiler. `iface.poll` is not the 100M
  bottleneck on measured paths, so event-loop sharding is cancelled for now.
- **Cross-flow HoL from blocking TCP uplink send** was found by 刀12 and fixed by 刀13 using non-blocking
  `try_reserve`: Full leaves bytes in smoltcp rx buffer and lets TCP window apply backpressure.
- **#3 single QUIC connection / connection pool** remains unproven. Only revisit on a low RTT, genuinely
  >100M end-to-end path where single-connection-vs-pool can be measured cleanly.

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
