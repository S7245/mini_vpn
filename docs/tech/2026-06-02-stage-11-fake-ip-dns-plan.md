# Stage 11 fake-IP DNS Implementation Plan

**Goal:** Add fake-IP mode to client-tun: intercept DNS in the TUN, forge A responses
with placeholder IPs from `198.18.0.0/15`, map `fake-IP ↔ domain`, and on a TCP SYN to a
fake-IP rewrite the relay target to `DomainPort` so the Upstream resolves the domain with
its clean DNS. Server unchanged.

**Architecture:** Two pure modules (FakeIpPool, DNS codec) heavily unit-tested, then two
integration points in the TUN runtime (UDP DNS interception, target rewrite), then e2e.

**Tech Stack:** Rust, smoltcp 0.10 (udp socket), no new deps (hand-written DNS codec).

**Module placement:** new `src/fake_ip.rs` (pool) + `src/dns.rs` (codec), declared in
`main.rs`/`lib.rs`; wired from `src/client_tun.rs`. Keeps client_tun.rs from bloating.

---

## File Map

- Create: `src/fake_ip.rs` — `FakeIpPool` + tests
- Create: `src/dns.rs` — `parse_query` / `build_response` + tests
- Modify: `src/main.rs` (or `src/lib.rs`) — `mod fake_ip; mod dns;`
- Modify: `src/client_tun.rs` — UDP DNS socket interception (T3) + target rewrite (T4)
- Create: `docs/tech/11-fake-ip-dns.md` — teaching note
- Modify: `TODO.md` — blind spots, reclamation, hickory trigger

---

### Task 1: `FakeIpPool` (TDD, pure)

**Files:** Create `src/fake_ip.rs`; Modify `src/main.rs`/`src/lib.rs`

- [ ] Step 1: Write failing tests:

```rust
    use std::net::Ipv4Addr;

    #[test]
    fn alloc_is_stable_per_domain() {
        let mut p = FakeIpPool::new();
        let a = p.alloc("facebook.com");
        let b = p.alloc("facebook.com");
        assert_eq!(a, b, "same domain must reuse the same fake-IP");
        let c = p.alloc("google.com");
        assert_ne!(a, c, "different domains get different fake-IPs");
    }

    #[test]
    fn alloc_starts_at_dot_two_skipping_resolver() {
        let mut p = FakeIpPool::new();
        // .1 is reserved for the DNS resolver; first alloc is .2
        assert_eq!(p.alloc("a.com"), Ipv4Addr::new(198, 18, 0, 2));
        assert_eq!(p.alloc("b.com"), Ipv4Addr::new(198, 18, 0, 3));
    }

    #[test]
    fn resolve_round_trips_and_misses() {
        let mut p = FakeIpPool::new();
        let ip = p.alloc("x.com");
        assert_eq!(p.resolve(ip).as_deref(), Some("x.com"));
        assert_eq!(p.resolve(Ipv4Addr::new(198, 18, 0, 250)), None);
    }

    #[test]
    fn is_fake_range_boundaries() {
        let p = FakeIpPool::new();
        assert!(p.is_fake(Ipv4Addr::new(198, 18, 0, 0)));
        assert!(p.is_fake(Ipv4Addr::new(198, 19, 255, 255)));
        assert!(!p.is_fake(Ipv4Addr::new(198, 17, 255, 255)));
        assert!(!p.is_fake(Ipv4Addr::new(198, 20, 0, 0)));
        assert!(!p.is_fake(Ipv4Addr::new(1, 1, 1, 1)));
    }
```

- [ ] Step 2: `cargo test --lib fake_ip` (or module path) → FAIL.
- [ ] Step 3: Implement `FakeIpPool`:
  - `range`: `198.18.0.0/15`; `next: u32` cursor starting at `u32::from(198.18.0.2)`.
  - `domain_to_ip: HashMap<String, Ipv4Addr>`, `ip_to_domain: HashMap<Ipv4Addr, String>`.
  - `alloc(&mut self, domain: &str) -> Ipv4Addr`: return existing if mapped, else take
    `next` (wrap within range, skip `.0`/`.1`), insert both maps, bump cursor.
  - `resolve(&self, ip) -> Option<String>`; `is_fake(&self, ip) -> bool` (range check).
- [ ] Step 4: `cargo test` → PASS.
- [ ] Step 5: Commit `feat(fake-ip): add FakeIpPool with stable per-domain allocation`.

### Task 2: minimal DNS codec (TDD, pure)

**Files:** Create `src/dns.rs`; Modify `src/main.rs`/`src/lib.rs`

- [ ] Step 1: Write failing tests (build query bytes by hand or with a helper):

```rust
    // Helper builds a standard A query for "test.com" with id=0x1234.
    fn a_query_testcom() -> Vec<u8> { /* 12B header + qname 04 test 03 com 00 + 0001 0001 */ }

    #[test]
    fn parse_a_query() {
        let q = parse_query(&a_query_testcom()).expect("should parse");
        assert_eq!(q.id, 0x1234);
        assert_eq!(q.qname, "test.com");
        assert_eq!(q.qtype, QTYPE_A);
    }

    #[test]
    fn parse_rejects_truncated_and_multi_question() { /* short buf -> None; qdcount=2 -> None */ }

    #[test]
    fn build_a_response_fields() {
        let q = parse_query(&a_query_testcom()).unwrap();
        let resp = build_response(&q, Answer::A(Ipv4Addr::new(198,18,0,2), 5));
        let p = parse_response_for_test(&resp); // tiny test-only reader
        assert_eq!(p.id, 0x1234);
        assert!(p.qr && p.ancount == 1);
        assert_eq!(p.answer_ip, Some(Ipv4Addr::new(198,18,0,2)));
        assert_eq!(p.answer_ttl, 5);
        assert_eq!(p.question_echoed_qname, "test.com");
    }

    #[test]
    fn build_nodata_response_fields() {
        let q = parse_query(&a_query_testcom()).unwrap();
        let resp = build_response(&q, Answer::NoData);
        let p = parse_response_for_test(&resp);
        assert!(p.qr && p.ancount == 0 && p.rcode == 0); // success, no answer
    }
```

- [ ] Step 2: `cargo test dns` → FAIL.
- [ ] Step 3: Implement:
  - consts `QTYPE_A=1`, `QTYPE_AAAA=28`.
  - `struct DnsQuery { id: u16, qname: String, qtype: u16, raw_question: Vec<u8> }`.
  - `parse_query`: read 12B header; require `qdcount==1`; walk labels (reject `0xC0`
    compression in question), build `qname`; read qtype/qclass. Return None on any short read.
  - `enum Answer { A(Ipv4Addr, u32 /*ttl*/), NoData }`.
  - `build_response(&DnsQuery, Answer)`: header (id, flags QR=1 RA=0 RD copied, qd=1,
    an = 1 for A else 0), echo question, for A append answer `0xC00C, TYPE=A, CLASS=IN,
    TTL, RDLENGTH=4, RDATA=ip`.
- [ ] Step 4: `cargo test` → PASS.
- [ ] Step 5: Commit `feat(dns): add minimal A/AAAA query parse + response build`.

### Task 3: TUN DNS interception (integration)

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: Add a smoltcp `udp::Socket` to the SocketSet, bound to `198.18.0.1:53`
  (rx/tx metadata + payload buffers sized for a few DNS packets). Hold its `SocketHandle`.
- [ ] Step 2: Hold `let mut fake_pool = FakeIpPool::new();` in the main loop scope.
- [ ] Step 3: After `iface.poll` in BOTH rx and timer branches, drain the DNS socket:
  ```text
  while udp_socket.can_recv():
      (data, endpoint) = udp_socket.recv()
      match dns::parse_query(data):
          Some(q) if q.qtype == A   -> ip = fake_pool.alloc(&q.qname);
                                       resp = build_response(&q, Answer::A(ip, 5));
                                       log 🪪 ...; udp_socket.send_slice(resp, endpoint)
          Some(q) (AAAA/other)      -> resp = build_response(&q, Answer::NoData);
                                       udp_socket.send_slice(resp, endpoint)
          None                      -> drop (no response)
  then flush_tx so the forged response goes out.
  ```
- [ ] Step 4: `cargo check` + `clippy -D warnings` clean. (No unit test; integration.)
- [ ] Step 5: Commit `feat(tun): intercept DNS in TUN and forge fake-IP responses`.

### Task 4: target rewrite (integration)

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: In `process_listener_activity`, after extracting `local_endpoint() ->
  IpEndpoint`, before building the relay target:
  ```text
  let ip = endpoint.addr (Ipv4);
  if fake_pool.is_fake(ip):
      match fake_pool.resolve(ip):
          Some(domain) -> target = TargetAddr::DomainPort{ host: domain, port }
                          log 🔁 ...
          None         -> log 🚫 无映射，拒绝; rearm this handle; skip open-remote
  else:
      target = target_from_endpoint(endpoint)   // IpPort, unchanged
  ```
  (Thread `&mut fake_pool` / `&fake_pool` into the call path as needed; it lives in the
  main loop and is accessed single-threaded.)
- [ ] Step 2: `cargo test` (existing 24+7 still pass) + `clippy -D warnings` clean.
- [ ] Step 3: Commit `feat(tun): rewrite fake-IP target to DomainPort, refuse stale`.

### Task 5: teaching note + full validation + cross-machine e2e

**Files:** Create `docs/tech/11-fake-ip-dns.md`; Modify `TODO.md`

- [ ] Step 1: Teaching note: data flow, fake-IP range, DNS codec scope, AAAA→NODATA
  rationale, target rewrite + stale-refuse, blind spots (DoH/直连IP/QUIC), OS setup.
- [ ] Step 2: `TODO.md`: DoH interception, hardcoded-IP, QUIC/UDP relay, fake-IP
  reclamation, hickory trigger conditions.
- [ ] Step 3: Full validation: `cargo test` / `check` / `clippy -D warnings` / `doc --no-deps`.
- [ ] Step 4: Manual cross-machine e2e (pending user, requires sudo/TUN/DNS setup):
  ```bash
  # client: route fake range + DNS into utun, point system DNS at 198.18.0.1
  UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
  sudo route -n add -net 198.18.0.0/15 -interface "$UT"
  # macOS DNS: networksetup -setdnsservers Wi-Fi 198.18.0.1   (remember to restore!)
  curl -v -k --resolve-from-system https://www.facebook.com/   # or just: curl -v https://www.facebook.com/
  ```
  Expect client logs: `🪪 DNS facebook.com → fake-IP 198.18.0.x`, `🔁 resolve → facebook.com`,
  relay DomainPort; server logs `解析出的目标地址是: www.facebook.com:443`; curl gets a
  real TLS response from facebook (proving exit-side resolution bypassed local poisoning).
  Restore DNS afterwards.
- [ ] Step 5: Commit `docs(fake-ip): add stage 11 teaching note + acceptance`.

## Validation checklist (every code task)

- `cargo test` green
- `cargo clippy --all-targets --all-features -- -D warnings` clean
- each task = one commit; check off the boxes here as you go
