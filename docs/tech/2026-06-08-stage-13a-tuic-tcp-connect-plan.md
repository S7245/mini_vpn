# Stage 13a TUIC TCP-Connect Implementation Plan

**Goal:** Add a `tuic` upstream so client-tun relays intercepted **TCP** to a mature **sing-box TUIC server**
via TUIC `Connect`, selectable by `MINI_VPN_UPSTREAM=legacy|tuic` (default `legacy` = zero regression). Behind
a new `ProxyUpstream` trait (legacy yamux / tuic impls). UDP stays on legacy (→13b). See 13a spec + ADR-0004.

**Architecture:** Two pure modules heavily unit-tested first (TUIC command codec, TuicClientConfig), then the
`ProxyUpstream` trait + a legacy wrapper, then `TuicUpstream` (connect + Authenticate + open_tcp), then wire the
upstream selection into client_tun, then interop e2e against real sing-box.

**Tech Stack:** Rust, quinn 0.10 (already in; `Connection::export_keying_material` for the TUIC token), rustls
0.21, bytes. No new deps (TUIC protocol self-implemented). ALPN `h3` (match sing-box).

**Module placement:** new `src/tuic.rs` (config + command codec + TuicUpstream) and `src/upstream.rs`
(`ProxyUpstream` trait + legacy/tuic impls) in the lib crate; wired from `src/client_tun.rs`.

---

## TUIC v5 wire reference (authoritative for this stage)

Header on each command: `[VER=0x05][TYPE]`. Address: `[ATYP][ADDR][PORT:u16 BE]`,
ATYP **0x00=domain `[len:1][bytes]`**, **0x01=IPv4 (4B)**, **0x02=IPv6 (16B)**, 0xff=None.
(⚠️ NOT our Stage-12 codes.)

- **Authenticate (0x00)** [uni-stream]: `[0x05][0x00][UUID:16][TOKEN:32]` (50 B).
  `TOKEN = export_keying_material(out=32, label=UUID(16B), context=password_bytes)`.
- **Connect (0x01)** [bi-stream]: `[0x05][0x01][ADDR]`, then relay raw bytes (0-RTT, no server reply).
- (Packet/Dissociate/Heartbeat → 13b/13c.)

---

## File Map

- Create: `src/tuic.rs` — `TuicClientConfig` + command codec (`encode_address`/`encode_authenticate`/
  `encode_connect`) + `TuicUpstream` (connect/auth/open_tcp) + tests
- Create: `src/upstream.rs` — `ProxyUpstream` trait + `RelayStream` + `LegacyYamuxUpstream` + tests
- Modify: `src/lib.rs` — `pub mod tuic; pub mod upstream;`
- Modify: `src/client_tun.rs` — build upstream by `MINI_VPN_UPSTREAM`; route TCP open through the trait
- Create: `docs/tech/13-tuic-tcp-connect.md` — teaching note
- Modify: `docs/tech/...` acceptance after e2e

---

### Task 1: TUIC command codec (TDD, pure)

**Files:** Create `src/tuic.rs`; Modify `src/lib.rs`

- [ ] Step 1: Failing tests (exact bytes):

```rust
    use crate::shared::TargetAddr;

    #[test]
    fn address_ipv4() {
        let a = encode_address(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(a, vec![0x01, 1, 2, 3, 4, 0x01, 0xBB]); // ATYP=1, IP, port 443
    }
    #[test]
    fn address_domain() {
        let a = encode_address(&TargetAddr::DomainPort { host: "ab.com".into(), port: 443 });
        assert_eq!(a, vec![0x00, 6, b'a', b'b', b'.', b'c', b'o', b'm', 0x01, 0xBB]);
    }
    #[test]
    fn address_ipv6() {
        let a = encode_address(&TargetAddr::IpPort("[::1]:53".parse().unwrap()));
        assert_eq!(a[0], 0x02);
        assert_eq!(a.len(), 1 + 16 + 2);
        assert_eq!(&a[17..19], &[0x00, 0x35]); // port 53
    }
    #[test]
    fn authenticate_layout() {
        let uuid = [0xABu8; 16];
        let token = [0xCDu8; 32];
        let c = encode_authenticate(&uuid, &token);
        assert_eq!(c.len(), 2 + 16 + 32);
        assert_eq!(&c[..2], &[0x05, 0x00]);
        assert_eq!(&c[2..18], &uuid);
        assert_eq!(&c[18..50], &token);
    }
    #[test]
    fn connect_prefixes_header() {
        let c = encode_connect(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(&c[..2], &[0x05, 0x01]);
        assert_eq!(&c[2..], &[0x01, 1, 2, 3, 4, 0x01, 0xBB]);
    }
    #[test]
    fn domain_over_255_is_truncated_safely() { /* host.len() capped to u8, no panic */ }
```

- [ ] Step 2: `cargo test --lib tuic` → FAIL
- [ ] Step 3: Implement `encode_address` (TUIC ATYP 0/1/2; domain len u8 saturating),
  `encode_authenticate`, `encode_connect`. Constants `TUIC_VER=0x05`, `CMD_AUTH=0x00`, `CMD_CONNECT=0x01`,
  `ATYP_DOMAIN=0x00`/`ATYP_IPV4=0x01`/`ATYP_IPV6=0x02`.
- [ ] Step 4: PASS
- [ ] Commit: `feat(tuic): add TUIC v5 command codec (address/authenticate/connect)`

---

### Task 2: `TuicClientConfig` (TDD, pure)

**Files:** Modify `src/tuic.rs`

- [ ] Step 1: Failing tests: `from_sources` requires server/uuid/password in tuic use; ALPN default `h3`;
  invalid server addr rejected; password/uuid never rendered in `Debug` (redacted).
- [ ] Step 2: FAIL
- [ ] Step 3: Implement `TuicClientConfig { server: SocketAddr, uuid: [u8;16], password: String, sni, ca_path,
  alpn, congestion_control, udp_relay_mode }` + `from_sources`/`from_env` + a redacting `Debug`.
  (UUID parse from hyphenated string → 16 bytes.)
- [ ] Step 4: PASS
- [ ] Commit: `feat(tuic): add TuicClientConfig with redacted credentials`

---

### Task 3: `ProxyUpstream` trait + legacy wrapper

**Files:** Create `src/upstream.rs`; Modify `src/lib.rs`

- [ ] Step 1: Define `trait ProxyUpstream { async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream,
  ClientError>; }` and `RelayStream` (enum over `Compat<yamux::Stream>` and the TUIC bi-stream compat, both
  `AsyncRead+AsyncWrite+Unpin+Send`). `LegacyYamuxUpstream` wraps a cloneable `yamux::Control` and calls
  `open_remote_session(.., RelayRequest::Tcp{target})`.
- [ ] Step 2: Unit test: `RelayStream` implements AsyncRead/AsyncWrite (type-level; a compile test + a tiny
  in-memory duplex round-trip through the enum variant that wraps a `tokio::io::DuplexStream` for coverage).
- [ ] Step 3: Implement.
- [ ] Step 4: `cargo test` PASS; `cargo build` clean.
- [ ] Commit: `feat(upstream): add ProxyUpstream trait + legacy yamux upstream`

---

### Task 4: `TuicUpstream` — connect + authenticate + open_tcp

**Files:** Modify `src/tuic.rs`

- [ ] Step 1: (Network/async — covered by interop Task 6; the pure encoders are Task 1.) Implement:
  - `connect(cfg) -> TuicUpstream`: build quinn client config (rustls 0.21 + roots from `ca_path` + ALPN
    from cfg via `quic.rs`-style helper), `Endpoint::client`, `connect(server, sni)`, `.await`; then
    derive token = `conn.export_keying_material(32, uuid(16), password)`, open a **uni-stream**, write
    `encode_authenticate(uuid, token)`, finish the stream.
  - `impl ProxyUpstream for TuicUpstream { open_tcp }`: `conn.open_bi()`, write `encode_connect(target)`,
    return the bi-stream's send/recv compat-wrapped as `RelayStream::Tuic`.
  - Self-reconnect (reuse `backoff_delay`) + keep-alive transport (reuse `quic.rs` keepalive/idle/MTU).
- [ ] Step 2: Build clean; a smoke test that `connect` to a closed port errors gracefully (no panic).
- [ ] Step 3: —
- [ ] Step 4: `cargo build`/`cargo test` green.
- [ ] Commit: `feat(tuic): TuicUpstream connect + authenticate + Connect bi-stream`

---

### Task 5: Wire upstream selection into client_tun

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: Unit test for the selector: `MINI_VPN_UPSTREAM` parse → `Legacy | Tuic` (default `Legacy`;
  unknown → error).
- [ ] Step 2: FAIL
- [ ] Step 3: Build `Box<dyn ProxyUpstream>` at startup per the switch; in `handle_local_payload`, replace
  `open_remote_session(ctrl, &RelayRequest::Tcp{target})` with `upstream.open_tcp(&target)`. Legacy branch
  byte-identical (build the legacy upstream from the existing `ctr`). UDP path unchanged (legacy).
- [ ] Step 4: `cargo test --lib --bins --tests` PASS; legacy mode unchanged (zero regression).
- [ ] Commit: `feat(tun): select upstream (legacy|tuic) and route TCP through ProxyUpstream`

---

### Task 6: Interop e2e + docs (no code)

**Files:** Create `docs/tech/13-tuic-tcp-connect.md`; Modify TODO

- [ ] Step 1: Stand up a minimal **sing-box TUIC server** (spec recipe: dev certs, uuid/password, alpn h3,
  bbr). Run client with `MINI_VPN_UPSTREAM=tuic` + matching creds; route a target into the TUN;
  `curl -k https://1.1.1.1/` → response. Confirm sing-box logs the `Connect` from our UUID. Confirm
  `MINI_VPN_UPSTREAM=legacy` still works (zero regression).
- [ ] Step 2: Teaching note `docs/tech/13-tuic-tcp-connect.md` (TUIC handshake, Connect, ProxyUpstream,
  interop gotchas: alpn/sni/cert alignment, byte-exact token).
- [ ] Step 3: TODO: mark 13a done; 13b (UDP Packet) next.
- [ ] Commit: `docs(tuic): stage 13a acceptance — TCP via sing-box TUIC verified`

---

## Then: `/code-review` + interop acceptance.

## Notes / risk watch
- **Byte-exact interop** is the gate: token label/context + ATYP codes verified against sing-box (encoders
  unit-tested; truth proven by a successful sing-box handshake in Task 6).
- **`RelayStream` unified type**: enum over legacy/tuic streams keeps the existing relay pump unchanged.
- **Zero regression**: default `legacy`; the legacy code path is not modified, only wrapped.
- UUID config: accept hyphenated string → 16 bytes; never log uuid/password.
