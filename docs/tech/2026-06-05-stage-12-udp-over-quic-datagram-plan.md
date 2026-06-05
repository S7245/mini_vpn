# Stage 12 UDP-over-QUIC-datagram Implementation Plan

**Goal:** Add a QUIC datagram data plane carrying UDP relay, alongside (not replacing) the
existing TCP/yamux upstream. client-tun intercepts UDP in the TUN via **raw packets** (bypassing
smoltcp), tags each flow with a client-minted **flow-id**, and relays target-inline datagrams over
QUIC to the Upstream, which dials the real Target and relays back. fake-IP reused for UDP. Existing
TCP path untouched (zero regression). See spec + ADR-0003.

**Architecture:** One pure module (`udp_relay`: wire codec + FlowTable + IP packet build/parse)
heavily unit-tested first; one tiny device helper; QUIC config/endpoint plumbing reusing rustls 0.21
certs; server QUIC relay loop + session table gated by a deterministic integration test; client QUIC
pump + main-loop wiring (mostly pre-tested pure pieces as glue); then cross-machine e2e + teaching note.

**Tech Stack:** Rust, `quinn = "0.10"` (shares existing rustls 0.21.12 — single rustls, cert-loading
code reused), etherparse 0.13 (UDP build/parse — already a dep), tokio. ALPN `b"mvpn"`.

**Module placement:** new `src/udp_relay.rs` (codec + FlowTable + packet build/parse, declared in
`main.rs`/`lib.rs`); device helper in `src/device.rs`; QUIC wired from `src/server.rs` &
`src/client_tun.rs`. Keeps client_tun.rs from bloating, mirrors `fake_ip.rs`/`dns.rs`.

**Constants:** `UDP_FLOW_IDLE_SECS=60`, `MAX_UDP_FLOWS=1024`, `UDP_SWEEP_INTERVAL=1s`, ALPN=`b"mvpn"`.

---

## File Map

- Create: `src/udp_relay.rs` — wire codec + `FlowTable` + `build_udp_ip_packet`/`parse_inbound_udp` + tests
- Modify: `src/main.rs` (or `src/lib.rs`) — `mod udp_relay;`
- Modify: `src/device.rs` — `inject_ip_packet` + test
- Create: `src/quic.rs` — shared QUIC server/client config + endpoint builders (cert reuse + ALPN + datagram transport) + test
- Modify: `src/server.rs` — QUIC endpoint accept loop + datagram relay + flow-id→socket session table
- Modify: `src/client_tun.rs` — QUIC connect + pump task + channels + rx UDP split + downlink injection + sweep
- Create: `tests/udp_quic_relay.rs` — layer-2 deterministic integration (Task 6 gate)
- Create: `docs/tech/12-udp-over-quic-datagram.md` — teaching note
- Modify: `Cargo.toml` — `quinn = "0.10"`
- Modify: `TODO.md` — mark UDP-relay first cut done; list non-goals as future track

---

### Task 1: UDP wire codec (TDD, pure)

**Files:** Create `src/udp_relay.rs`; Modify `src/main.rs`/`src/lib.rs`

- [ ] Step 1: Failing tests:

```rust
    use crate::shared::TargetAddr;

    #[test]
    fn uplink_roundtrips_ipv4_target() {
        let t = TargetAddr::IpPort("1.2.3.4:443".parse().unwrap());
        let buf = encode_uplink(7, &t, b"hello");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 7);
        assert_eq!(target, t);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn uplink_roundtrips_domain_target() {
        let t = TargetAddr::DomainPort { host: "facebook.com".into(), port: 443 };
        let buf = encode_uplink(9, &t, b"q");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!((fid, target, payload), (9, t, &b"q"[..]));
    }

    #[test]
    fn downlink_roundtrips() {
        let buf = encode_downlink(42, b"resp");
        assert_eq!(decode_downlink(&buf).unwrap(), (42, &b"resp"[..]));
    }

    #[test]
    fn decode_rejects_truncated_without_panic() {
        assert!(decode_uplink(&[0u8; 3]).is_none());      // < flow-id
        assert!(decode_uplink(&[0,0,0,1, 3, 200]).is_none()); // domain len overruns
        assert!(decode_downlink(&[0u8; 2]).is_none());
    }
```

- [ ] Step 2: `cargo test --lib udp_relay` → FAIL
- [ ] Step 3: Implement `encode_uplink`/`decode_uplink`/`encode_downlink`/`decode_downlink` (big-endian,
  ATYP 1=IPv4 / 3=`[len][domain]`; every bounds check returns `None`, never panics).
- [ ] Step 4: `cargo test --lib udp_relay` → PASS
- [ ] Commit: `feat(udp): add QUIC-datagram wire codec for UDP relay`

---

### Task 2: `FlowTable` (TDD, pure)

**Files:** Modify `src/udp_relay.rs`

- [ ] Step 1: Failing tests:

```rust
    use std::net::{IpAddr, Ipv4Addr};

    fn ft() -> FlowTable { FlowTable::new() }
    fn tuple(p: u16) -> FourTuple { /* (src 10.0.0.1:p → dst 198.18.0.5:443) */ }

    #[test]
    fn intern_is_stable_per_tuple_and_unique_across() {
        let mut t = ft();
        let a = t.intern(tuple(1000));
        assert_eq!(a, t.intern(tuple(1000)), "same tuple reuses flow-id");
        assert_ne!(a, t.intern(tuple(1001)), "different tuple new flow-id");
    }

    #[test]
    fn resolve_returns_entry_then_sweep_reclaims_idle() {
        let mut t = ft();
        let id = t.intern(tuple(1000));
        assert!(t.resolve(id).is_some());
        t.sweep(/*now*/ 61, /*idle*/ 60);         // last_activity=0
        assert!(t.resolve(id).is_none(), "idle > 60s reclaimed");
    }

    #[test]
    fn touch_keeps_flow_alive() {
        let mut t = ft();
        let id = t.intern(tuple(1000));     // t=0
        t.touch(id, /*now*/ 50);
        t.sweep(/*now*/ 100, 60);           // 100-50 < 60
        assert!(t.resolve(id).is_some());
    }

    #[test]
    fn lru_evicts_oldest_beyond_cap() {
        let mut t = FlowTable::with_cap(2);
        let a = t.intern(tuple(1)); let _b = t.intern(tuple(2));
        let _c = t.intern(tuple(3));        // evicts a (oldest)
        assert!(t.resolve(a).is_none());
    }
```

- [ ] Step 2: FAIL
- [ ] Step 3: Implement `FlowTable` (`intern`/`resolve`/`touch`/`sweep`/cap+LRU; `FourTuple`,
  `FlowEntry{app_endpoint, target_src, last_activity}`). Inject `now` (seconds) for testability —
  no `Instant::now()` inside (mirrors `backoff_delay`'s rand injection).
- [ ] Step 4: PASS
- [ ] Commit: `feat(udp): add FlowTable with idle sweep + LRU eviction`

---

### Task 3: UDP IP packet build/parse (TDD, pure)

**Files:** Modify `src/udp_relay.rs`

- [ ] Step 1: Failing tests:

```rust
    #[test]
    fn build_then_parse_roundtrips_with_checksums() {
        let src = (Ipv4Addr::new(198,18,0,5), 443);
        let dst = (Ipv4Addr::new(10,0,0,1), 51000);
        let pkt = build_udp_ip_packet(src, dst, b"payload");
        let got = parse_inbound_udp(&pkt).unwrap();
        assert_eq!(got.src_ip, src.0); assert_eq!(got.src_port, src.1);
        assert_eq!(got.dst_ip, dst.0); assert_eq!(got.dst_port, dst.1);
        assert_eq!(got.payload, b"payload");
    }

    #[test]
    fn parse_rejects_tcp_and_garbage() {
        assert!(parse_inbound_udp(&[0u8; 4]).is_none());
        // a TCP packet parses but is not UDP → None
    }
```

- [ ] Step 2: FAIL
- [ ] Step 3: Implement `build_udp_ip_packet` (etherparse `PacketBuilder::ipv4(..).udp(..).write`)
  and `parse_inbound_udp` (`PacketHeaders::from_ip_slice` → `.ip` Version4 + `.transport` Udp +
  `.payload`; non-UDP/garbage → `None`).
- [ ] Step 4: PASS
- [ ] Commit: `feat(udp): add etherparse IPv4/UDP packet build + parse helpers`

---

### Task 4: `device.inject_ip_packet` (TDD)

**Files:** Modify `src/device.rs`

- [ ] Step 1: Failing test: pushing a raw IP packet enqueues it (with the macOS 4-byte PI header
  prepended on macOS; raw on Linux) so `flush_tx` will send it.

```rust
    #[test]
    fn inject_enqueues_with_platform_header() {
        let mut q = VecDeque::new();
        let pkt = vec![69u8, 0, 0, 28];           // IPv4 header start
        push_injected(&mut q, &pkt);              // pure helper under test
        let framed = q.pop_front().unwrap();
        #[cfg(target_os = "macos")]
        assert_eq!(&framed[..4], &[0,0,0,2]);     // PI header
        #[cfg(not(target_os = "macos"))]
        assert_eq!(&framed[..4], &[69,0,0,28]);
    }
```

- [ ] Step 2: FAIL
- [ ] Step 3: Add `inject_ip_packet(&mut self, pkt: &[u8])` on `VirtualTunDevice` delegating to a
  pure `push_injected(queue, pkt)` (PI header on macOS) so the framing is unit-testable.
- [ ] Step 4: PASS
- [ ] Commit: `feat(device): inject raw IP packets into tx_queue for UDP downlink`

---

### Task 5: QUIC config + endpoint builders (TDD-light)

**Files:** Create `src/quic.rs`; Modify `src/main.rs`/`src/lib.rs`

- [ ] Step 1: Failing tests (build configs from dev certs; ALPN set):

```rust
    #[test]
    fn server_config_builds_with_dev_cert_and_alpn() {
        let cfg = server_quic_config("certs/dev/server-cert.pem", "certs/dev/server-key.pem");
        assert!(cfg.is_ok());
    }
    #[test]
    fn client_config_builds_with_dev_ca_and_alpn() {
        assert!(client_quic_config("certs/dev/ca-cert.pem").is_ok());
    }
```

- [ ] Step 2: FAIL
- [ ] Step 3: Implement `server_quic_config`/`client_quic_config`: build **rustls 0.21** Server/Client
  config (reuse the existing cert/CA loading code paths), set `alpn_protocols = vec![b"mvpn".to_vec()]`,
  set a `TransportConfig` with datagram send/recv buffer sizes (and `max_datagram_frame_size` generous
  enough that a 1200-byte QUIC initial + our ≤19-byte header fits), wrap via
  `quinn::ServerConfig::with_crypto(Arc::new(..))` / `quinn::ClientConfig::new(Arc::new(..))`. Plus thin
  `server_endpoint(addr)` / `client_endpoint()` helpers.
- [ ] Step 4: PASS
- [ ] Commit: `feat(quic): add shared QUIC server/client config reusing rustls 0.21 certs + ALPN`

---

### Task 6: Server QUIC datagram relay + session table (integration-gated)

**Files:** Modify `src/server.rs`; Create `tests/udp_quic_relay.rs`

- [ ] Step 1: Failing integration test `tests/udp_quic_relay.rs` (layer 2, deterministic, no TUN):
  start the server QUIC endpoint on `127.0.0.1:0`; start a local UDP echo server; connect a bare
  `quinn` client; `send_datagram(encode_uplink(fid, echo_target, b"ping"))`; assert
  `decode_downlink(read_datagram)` yields `(fid, b"ping")`. Add a **two-flow** case (different
  flow-ids to the same echo target) asserting each reply carries its own flow-id (demux). Add an
  **idle-reclaim** assertion (session count drops after idle).
- [ ] Step 2: `cargo test --test udp_quic_relay` → FAIL
- [ ] Step 3: Implement in `server.rs`: spawn QUIC endpoint accept loop (alongside the TCP one;
  reuse cert loading; QUIC listener bind failure ⇒ startup failure). Per connection: `read_datagram`
  loop → `decode_uplink` → `flow-id → UdpSocket` session table (create ephemeral socket on first
  sight; one recv task per socket: reply → `encode_downlink` → `send_datagram`) → `send_to(target)`.
  Idle-reclaim sockets after `UDP_FLOW_IDLE_SECS`. Existing TCP/yamux loop + `RelayRequest::Udp`
  skeleton left untouched (skeleton now unused).
- [ ] Step 4: `cargo test --test udp_quic_relay` → PASS
- [ ] Commit: `feat(server): relay UDP over QUIC datagrams with a flow-id session table`

---

### Task 7: Client QUIC pump + main-loop wiring

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: (Net-new pure bits already covered by Tasks 1–4.) Add focused unit tests for the rx
  classifier only:

```rust
    #[test]
    fn classify_dns_vs_relay_vs_nonudp() {
        assert_eq!(classify_inbound(&udp_to("198.18.0.1", 53)), Inbound::Dns);
        assert_eq!(classify_inbound(&udp_to("198.18.0.5", 443)), Inbound::UdpRelay);
        assert_eq!(classify_inbound(&tcp_syn_to("1.1.1.1", 443)), Inbound::Other);
    }
```

- [ ] Step 2: FAIL
- [ ] Step 3: Implement: `classify_inbound` in the rx peek (DNS→smoltcp, UdpRelay→raw path, Other→
  existing). `connect_quic_upstream()` + QUIC pump task (dumb pipe) + `udp_uplink`/`udp_downlink`
  channels + QUIC reconnect (reuse `backoff_delay`). On UdpRelay: `parse_inbound_udp` → `resolve_target`
  (fake→domain / non-fake→IPv4 / fake-no-map→drop+log) → `FlowTable.intern` → `encode_uplink` →
  `udp_uplink`. On `udp_downlink` select branch: `decode_downlink` → `FlowTable.resolve` →
  `build_udp_ip_packet(src=target, dst=app)` → `device.inject_ip_packet` → poll/flush. Add 1s sweep
  branch calling `FlowTable.sweep`. Oversized (> `max_datagram_size`) → drop + count + log.
- [ ] Step 4: `cargo test --lib client_tun` → PASS; `cargo build` clean.
- [ ] Commit: `feat(tun): drive UDP relay over the QUIC datagram data plane`

---

### Task 8: Cross-machine e2e + docs (no code)

**Files:** Create `docs/tech/12-udp-over-quic-datagram.md`; Modify `TODO.md`

- [ ] Step 1: Run layer-3 e2e (spec recipe): UDP echo first (prove datagram path), then Reqable
  HTTP/3 → `https://www.facebook.com/` (force h3, disable its DoH, capture-proxy off), then quality
  smoke (~1–2 min sustained + a few concurrent flows). Confirm TCP non-regression
  (`curl http://1.1.1.1/`, `curl https://1.1.1.1/`).
- [ ] Step 2: Write `docs/tech/12-udp-over-quic-datagram.md` teaching note (raw-packet UDP, flow-id,
  QUIC datagram, session table, reclaim, observed gotchas) + any LEARNINGS entry.
- [ ] Step 3: Update `TODO.md`: mark UDP-relay first cut DONE; record the QUIC north-star track and
  this stage's non-goals (TCP→QUIC migration, stream-fallback, all-:53/DoH, server socket pooling,
  multi-upstream, external stores) as the follow-on roadmap.
- [ ] Commit: `docs(udp): stage 12 acceptance signed — QUIC datagram UDP relay verified`

---

## Then: `/code-review`

Stage complete → run `/code-review` over the branch diff (per the project rhythm), address findings.

## Notes / risk watch

- **Topology:** the client→server QUIC traffic (to the server's real IP) must NOT be routed into the
  TUN — same pre-existing constraint as the TCP upstream. Acceptance recipe routes only
  `198.18.0.0/15` + test targets.
- **`send_datagram` has no `_wait` in 0.10:** on buffer-full Err → drop + count (UDP semantics, non-blocking).
- **Server session table is naive (one socket per flow):** first-cut accepted; pooling/port-exhaustion
  hardening is a later platform stage.
- **Tasks 6 & 7 are the heavy ones;** keep each independently green (Task 6 via integration test, Task 7
  via rx-classifier unit test + reused pure tests) before moving on.
