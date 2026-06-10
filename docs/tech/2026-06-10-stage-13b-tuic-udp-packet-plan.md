# Stage 13b TUIC UDP-Packet Implementation Plan

**Goal:** In `tuic` mode, relay intercepted **UDP** via **TUIC `Packet` (native datagram)** through sing-box.
One u16 `assoc-id` per 4-tuple (≈ flow-id) for clean downlink demux. Reuse Stage-12 raw-packet interception +
downlink injection; TuicUpstream becomes the datagram pump; periodic Heartbeat. legacy UDP (Stage 12) unchanged.
See 13b spec + ADR-0004.

**Architecture:** Two pure modules first (TUIC Packet codec, AssocTable) heavily unit-tested; then TuicUpstream
gains UDP send + a downlink datagram pump + heartbeat; then wire the tuic-mode UDP path into client_tun; then
interop e2e against sing-box.

**Tech Stack:** Rust, quinn 0.10 datagrams (already used in Stage 12), no new deps.

**Module placement:** `src/tuic.rs` (Packet codec + AssocTable + TuicUpstream UDP); `src/client_tun.rs` wiring.

---

## TUIC Packet wire reference (native datagram)

```
[VER=0x05][TYPE=0x02][ASSOC_ID:u16][PKT_ID:u16][FRAG_TOTAL:u8][FRAG_ID:u8][SIZE:u16][ADDR][data]
```
native: FRAG_TOTAL=1, FRAG_ID=0, PKT_ID=0. ADDR = TUIC address (ATYP 0x00 domain / 0x01 IPv4 / 0x02 IPv6).
Heartbeat: `[0x05][0x04]`.

---

## File Map
- Modify: `src/tuic.rs` — `encode_packet`/`decode_packet`/`encode_heartbeat` + `AssocTable` + TuicUpstream UDP
- Modify: `src/client_tun.rs` — tuic-mode UDP path (uplink encode→send_udp; downlink pump→inject)
- Create: `docs/tech/...` teaching note (after acceptance); Modify TODO

---

### Task 1: TUIC Packet codec (TDD, pure)

**Files:** Modify `src/tuic.rs`

- [ ] Step 1: Failing tests (exact bytes):

```rust
    #[test]
    fn packet_ipv4_layout() {
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        // [05 02][assoc 00 07][pkt 00 00][ftot 01][fid 00][size 00 02][atyp 01][ip][port 00 35]["hi"]
        assert_eq!(&p[..2], &[0x05, 0x02]);
        assert_eq!(&p[2..4], &[0x00, 0x07]);           // assoc-id
        assert_eq!(p[6], 1); assert_eq!(p[7], 0);      // frag total/id
        assert_eq!(&p[8..10], &[0x00, 0x02]);          // size
        assert_eq!(&p[10..15], &[0x01, 1, 2, 3, 4]);   // atyp ipv4 + ip
        assert_eq!(&p[15..17], &[0x00, 0x35]);         // port 53
        assert_eq!(&p[17..], b"hi");
    }
    #[test]
    fn packet_domain_roundtrip_assoc_and_data() {
        let p = encode_packet(9, &TargetAddr::DomainPort{host:"a.com".into(),port:443}, b"q");
        let (assoc, data) = decode_packet(&p).unwrap();
        assert_eq!(assoc, 9);
        assert_eq!(data, b"q");
    }
    #[test]
    fn decode_rejects_truncated() {
        assert!(decode_packet(&[0u8;5]).is_none());
        assert!(decode_packet(&[0x05,0x02,0,7,0,0,1,0,0,200,0x01]).is_none()); // size/addr overrun
    }
    #[test]
    fn heartbeat_layout() { assert_eq!(encode_heartbeat(), vec![0x05, 0x04]); }
```

- [ ] Step 2: `cargo test --lib tuic` → FAIL
- [ ] Step 3: Implement `encode_packet` (header + assoc + pkt_id=0 + frag 1/0 + size + `encode_address` + data),
  `decode_packet` (parse assoc, skip pkt/frag/size, skip ADDR per ATYP, return (assoc, data); bounds → None),
  `encode_heartbeat`. Constants `CMD_PACKET=0x02`, `CMD_HEARTBEAT=0x04`.
- [ ] Step 4: PASS
- [ ] Commit: `feat(tuic): add TUIC Packet codec + heartbeat`

---

### Task 2: `AssocTable` (u16) (TDD, pure)

**Files:** Modify `src/tuic.rs`

- [ ] Step 1: Failing tests: intern stable-per-tuple + unique; resolve → (app_endpoint, target_src);
  sweep reclaims idle; LRU at cap; ids are u16.
- [ ] Step 2: FAIL
- [ ] Step 3: Implement `AssocTable` (mirror udp_relay::FlowTable but `u16` ids; reuse `FourTuple`/`FlowEntry`
  from udp_relay, or a small local entry). `intern`/`resolve`/`touch`/`sweep`/`MAX_UDP_FLOWS` LRU; injected `now`.
- [ ] Step 4: PASS
- [ ] Commit: `feat(tuic): add AssocTable (u16 assoc-id per UDP 4-tuple)`

---

### Task 3: TuicUpstream UDP — send + downlink pump + heartbeat

**Files:** Modify `src/tuic.rs`

- [ ] Step 1: (Network — covered by interop Task 5.) Implement:
  - `send_udp(&self, datagram: Vec<u8>)`: `conn.send_datagram(bytes)`; TooLarge/err → drop + count + log.
  - `spawn_udp_downlink(conn, tx)`: read_datagram loop → forward bytes to a channel (main loop decodes).
  - periodic Heartbeat task (e.g. every 3s) while connected.
  - Expose the downlink channel receiver from `connect()` (or a getter) so the main loop can select on it.
- [ ] Step 2: Build clean; reuse existing reconnect (re-spawn pump/heartbeat on reconnect).
- [ ] Step 4: `cargo build`/`cargo test` green.
- [ ] Commit: `feat(tuic): TuicUpstream UDP send + datagram downlink pump + heartbeat`

---

### Task 4: Wire tuic-mode UDP into client_tun

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: In tuic mode, enable the UDP path: rx UdpRelay → `parse_inbound_udp` → `resolve_target` →
  `AssocTable.intern` → `encode_packet` → `TuicUpstream.send_udp`. Downlink: select on the TuicUpstream
  downlink channel → `decode_packet` → `AssocTable.resolve` → `build_udp_ip_packet` → `device.inject_ip_packet`.
  Reuse Stage-12 helpers; AssocTable owned by main loop. legacy UDP path unchanged.
- [ ] Step 2: `cargo test --lib --bins --tests` PASS; legacy unchanged (zero regression).
- [ ] Commit: `feat(tun): drive UDP over TUIC Packet in tuic mode`

---

### Task 5: Interop e2e + docs (no code)

**Files:** teaching note; TODO

- [ ] Step 1: Against sing-box (tuic mode): `dig @1.1.1.1` (UDP DNS) → answer; UDP echo via domain (ATYP=domain,
  fake-IP) → echo; optional QUIC/HTTP3 → facebook. legacy UDP non-regression.
- [ ] Step 2: teaching note + LEARNINGS; TODO marks 13b done, 13c next.
- [ ] Commit: `docs(tuic): stage 13b acceptance — UDP via sing-box TUIC verified`

---

## Then: `/code-review` + interop acceptance.

## Notes
- assoc-id u16; MAX_UDP_FLOWS=1024 ≪ 65536. native = no fragmentation; oversized dropped+counted (quic-stream
  fallback deferred).
- Byte-exact Packet layout verified against sing-box (encoders unit-tested; truth = a working UDP relay).
