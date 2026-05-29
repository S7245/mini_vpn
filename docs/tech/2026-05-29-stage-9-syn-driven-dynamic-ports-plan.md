# Stage 9 SYN-Driven Dynamic Ports Implementation Plan

**Goal:** Inspect each inbound IP packet for a clean TCP SYN, learn `dst_port`, and
dynamically ensure a smoltcp listener pool exists for that port before `iface.poll`
processes the frame. Drop the fixed `MINI_VPN_TUN_LOCAL_PORT` config. Per-port pool
size + a port-count cap to bound memory under SYN flood.

**Architecture:** Pure SYN inspector helper + `ListenerRegistry` (port → pool of
SocketHandles). Hook into the rx branch of the main `select!` BEFORE `iface.poll`.
Main loop iterates the registry's all_handles for `process_listener_activity`.

**Tech Stack:** Rust, smoltcp 0.10, etherparse 0.13 (already a dep).

---

## File Map

- Modify: `src/client_tun.rs`
  - Drop `DEFAULT_TUN_LISTEN_PORT`, `MINI_VPN_TUN_LOCAL_PORT`, `TunListenerConfig.local_port`
  - Lower `DEFAULT_TUN_POOL_SIZE` default 4 → 2 (per-port now)
  - Add `MAX_INTERCEPTED_PORTS` constant (64)
  - Add `inspect_inbound_syn(&[u8]) -> Option<u16>` + tests
  - Replace `ListenerPool { handles: Vec<SocketHandle> }` with `ListenerRegistry`
    holding `HashMap<u16, Vec<SocketHandle>>` and a `pool_size`
  - Pre-`iface.poll` rx hook: `if let Some(port) = inspect_inbound_syn(buf) { registry.ensure_port(...) }`
  - Tests updated to new arity / removed-local_port assertions
- Create: `docs/tech/09-syn-driven-dynamic-ports.md` (teaching note)

---

### Task 1: `inspect_inbound_syn` helper (TDD) — DONE

**Files:** Modify `src/client_tun.rs`

- [x] **Step 1: Failing tests** in `mod tests`:

```rust
    fn build_ipv4_tcp(
        src: [u8; 4], dst: [u8; 4], src_port: u16, dst_port: u16,
        syn: bool, ack: bool,
    ) -> Vec<u8> {
        let builder = etherparse::PacketBuilder::ipv4(src, dst, 64).tcp(src_port, dst_port, 0, 1024);
        let mut buf = Vec::new();
        let payload: [u8; 0] = [];
        // toggle flags via the builder chain
        let builder = if syn { builder.syn() } else { builder };
        let builder = if ack { builder.ack(0) } else { builder };
        builder.write(&mut buf, &payload).unwrap();
        buf
    }

    #[test]
    fn inspect_inbound_syn_returns_dst_port_for_clean_syn() {
        let pkt = build_ipv4_tcp([10,0,0,1], [1,1,1,1], 60000, 443, true, false);
        assert_eq!(inspect_inbound_syn(&pkt), Some(443));
    }

    #[test]
    fn inspect_inbound_syn_rejects_syn_ack() {
        let pkt = build_ipv4_tcp([1,1,1,1], [10,0,0,1], 443, 60000, true, true);
        assert_eq!(inspect_inbound_syn(&pkt), None);
    }

    #[test]
    fn inspect_inbound_syn_rejects_plain_ack() {
        let pkt = build_ipv4_tcp([10,0,0,1], [1,1,1,1], 60000, 80, false, true);
        assert_eq!(inspect_inbound_syn(&pkt), None);
    }

    #[test]
    fn inspect_inbound_syn_rejects_garbage() {
        assert_eq!(inspect_inbound_syn(&[0u8; 4]), None);
    }
```

- [ ] **Step 2:** `cargo test inspect_inbound_syn` → FAIL.

- [ ] **Step 3: Minimal implementation** near the other relay helpers:

```rust
/// Identify a clean inbound TCP SYN and return its destination port.
/// 中文要点：纯解析，无副作用；不是 SYN 或非 IPv4 TCP 一律 None。
fn inspect_inbound_syn(packet: &[u8]) -> Option<u16> {
    use etherparse::{PacketHeaders, TransportHeader};
    let parsed = PacketHeaders::from_ip_slice(packet).ok()?;
    let TransportHeader::Tcp(tcp) = parsed.transport? else { return None };
    if tcp.syn && !tcp.ack {
        Some(tcp.destination_port)
    } else {
        None
    }
}
```

- [ ] **Step 4:** `cargo test inspect_inbound_syn` → PASS.

- [ ] **Step 5: Commit** `feat(tun): add inspect_inbound_syn helper`.

### Task 2: `ListenerRegistry` replacing fixed pool — DONE

**Files:** Modify `src/client_tun.rs`

- [x] **Step 1: Failing tests**:

```rust
    #[test]
    fn registry_ensure_port_is_idempotent_and_caps_at_max() {
        let mut sockets = SocketSet::new(vec![]);
        let mut ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
        let mut reg = ListenerRegistry::new(2);
        for port in 0..MAX_INTERCEPTED_PORTS as u16 {
            reg.ensure_port(port + 1, &mut sockets, &mut ctxs).unwrap();
        }
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        // idempotent re-add
        reg.ensure_port(1, &mut sockets, &mut ctxs).unwrap();
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
        // capped
        let err = reg.ensure_port(9999, &mut sockets, &mut ctxs).unwrap_err();
        assert!(matches!(err, RegistryError::Capped));
        assert_eq!(reg.port_count(), MAX_INTERCEPTED_PORTS);
    }
```

- [ ] **Step 2:** test FAIL (types missing).

- [ ] **Step 3: Implementation**:

```rust
const MAX_INTERCEPTED_PORTS: usize = 64;

#[derive(Debug)]
enum RegistryError { Capped }

#[derive(Debug)]
struct ListenerRegistry {
    ports: HashMap<u16, Vec<SocketHandle>>,
    pool_size: usize,
}

impl ListenerRegistry {
    fn new(pool_size: usize) -> Self {
        Self { ports: HashMap::new(), pool_size }
    }

    fn port_count(&self) -> usize { self.ports.len() }

    fn ensure_port(
        &mut self,
        port: u16,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if self.ports.contains_key(&port) { return Ok(()); }
        if self.ports.len() >= MAX_INTERCEPTED_PORTS { return Err(RegistryError::Capped); }
        let spec = ListenerSpec { local_port: port, pool_size: self.pool_size };
        let mut handles = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let h = sockets.add(build_listener_socket(&spec));
            socket_ctxs.insert(h, SocketCtx::new(port));
            handles.push(h);
        }
        self.ports.insert(port, handles);
        println!("🆕 listener pool created for port {port} (pool_size={})", self.pool_size);
        Ok(())
    }

    fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }
}
```

- [ ] **Step 4:** `cargo test registry_ensure_port` → PASS.

- [ ] **Step 5: Commit** `feat(tun): add ListenerRegistry for per-port dynamic pools`.

### Task 3: wire registry into main loop, drop fixed listen port

**Files:** Modify `src/client_tun.rs`

- [ ] **Step 1:** Replace `ListenerPool` field with `ListenerRegistry` in `start_tun_proxy`,
  drop `DEFAULT_TUN_LISTEN_PORT`, `MINI_VPN_TUN_LOCAL_PORT`, `TunListenerConfig.local_port`,
  `ListenerSpec.local_port` callers; lower `DEFAULT_TUN_POOL_SIZE` to 2.
- [ ] **Step 2:** In the rx branch, BEFORE `iface.poll`:

```rust
if let Some(buf) = &device.rx_buffer {
    if let Some(port) = inspect_inbound_syn(buf) {
        if let Err(e) = registry.ensure_port(port, &mut sockets, &mut socket_ctxs) {
            println!("⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}", e);
        }
    }
}
```

- [ ] **Step 3:** Replace `for handle in &listener_pool.handles { process_listener_activity(...) }`
  (both rx and timer branches) with iteration over `registry.all_handles()`.
- [ ] **Step 4:** Update startup banner: drop `local_port=`.
- [ ] **Step 5:** Fix all config tests: `local_port` parameter gone from
  `TunListenerConfig::from_sources` and `TunRuntimeConfig::from_sources`; pool default 2.
- [ ] **Step 6:** `cargo test` PASS, `cargo clippy -D warnings` clean.
- [ ] **Step 7: Commit** `feat(tun): wire ListenerRegistry into main loop`.

### Task 4: teaching note + full validation + manual cross-machine e2e

**Files:** Create `docs/tech/09-syn-driven-dynamic-ports.md`; Modify `src/client_tun.rs` if lint surfaces

- [ ] **Step 1:** Write teaching note (mechanism, per-port pool, port cap, cross-machine
  acceptance for ports 80 AND 443).
- [ ] **Step 2:** Full validation:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

- [ ] **Step 3:** Manual cross-machine e2e (pending user, requires sudo/TUN):
  - port 80: `curl -v http://1.1.1.1/` → 301
  - port 443: `curl -v -k https://1.1.1.1/` → Cloudflare HTTPS response
  - client logs `🆕 listener pool created for port 80` AND `for port 443`
  - server logs `解析出的目标地址是: 1.1.1.1:80` AND `1.1.1.1:443`

- [ ] **Step 4: Commit** `docs(tun): add stage 9 syn-driven dynamic ports note`.
