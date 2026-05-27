# Stage 8 Target Extraction Implementation Plan

**Goal:** Replace the hardcoded TUN relay Target with the real destination extracted from
smoltcp's `local_endpoint()`, and enable smoltcp AnyIP so arbitrary destination IPs reach
the stack — on a single fixed listen port (Stage 9 will add arbitrary ports).

**Architecture:** Keep the existing listener-pool + first-payload-open-remote flow. Add a
pure `target_from_endpoint` helper (unit-testable), read `local_endpoint()` in
`process_listener_activity` alongside payload extraction, thread the extracted Target into
`handle_local_payload`, and remove all hardcoded-target config. Enable AnyIP in `start_tun_proxy`
with a default route whose gateway is the interface's own IP.

**Tech Stack:** Rust, Tokio, smoltcp 0.10, Yamux, tokio-rustls

---

## File Map

- Modify: `src/client_tun.rs`
  - Add `target_from_endpoint(IpEndpoint) -> TargetAddr` + unit test
  - Enable AnyIP + default route in `start_tun_proxy`
  - Read `local_endpoint()` in `process_listener_activity`; thread Target into `handle_local_payload`
  - Remove `DEFAULT_TUN_TARGET`, `TunListenerConfig.target_addr`, `SocketCtx.target`,
    `MINI_VPN_TUN_TARGET_ADDR`, `default_target` wiring
  - Update existing config tests
- Create: `docs/tech/08-target-extraction.md` (teaching note)

---

### Task 1: `target_from_endpoint` helper (TDD) — DONE

**Files:** Modify `src/client_tun.rs`

- [x] **Step 1: Write the failing test** in the `#[cfg(test)]` module:

```rust
    #[test]
    fn target_from_endpoint_builds_ipv4_target() {
        use smoltcp::wire::{IpAddress, IpEndpoint};
        let ep = IpEndpoint::new(IpAddress::v4(93, 184, 216, 34), 80);
        let target = target_from_endpoint(ep);
        assert_eq!(target.to_wire_string(), "93.184.216.34:80");
    }
```

- [ ] **Step 2:** `cargo test target_from_endpoint` → FAIL (fn missing).

- [ ] **Step 3: Minimal implementation** near the relay helpers:

```rust
/// Convert a smoltcp endpoint into a relay Target.
/// 中文要点：TUN 链路只会得到 IPv4 目的地址，统一转成 TargetAddr::IpPort。
fn target_from_endpoint(endpoint: smoltcp::wire::IpEndpoint) -> TargetAddr {
    let ip = match endpoint.addr {
        smoltcp::wire::IpAddress::Ipv4(v4) => {
            std::net::IpAddr::V4(std::net::Ipv4Addr::from(v4.0))
        }
    };
    TargetAddr::IpPort(std::net::SocketAddr::new(ip, endpoint.port))
}
```

(If the `IpAddress` match is non-exhaustive under current features, add `#[allow]` or a
catch-all that returns the same IPv4 path; only `proto-ipv4` is enabled.)

- [ ] **Step 4:** `cargo test target_from_endpoint` → PASS.

- [ ] **Step 5: Commit** `feat(tun): add target_from_endpoint helper`.

### Task 2: Enable AnyIP + default route

**Files:** Modify `src/client_tun.rs`

- [ ] **Step 1:** After `iface.update_ip_addrs(... 10.0.0.2/24 ...)` in `start_tun_proxy`, add:

```rust
    // AnyIP: accept packets whose destination IP is not our own (the real Target).
    // 中文要点：默认路由网关填本机 IP 10.0.0.2 是 AnyIP 接收判定的硬性要求，不是笔误。
    iface.set_any_ip(true);
    iface
        .routes_mut()
        .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(10, 0, 0, 2))
        .unwrap();
```

- [ ] **Step 2:** `cargo check` → PASS.

- [ ] **Step 3: Commit** `feat(tun): enable smoltcp AnyIP for arbitrary destination IPs`.

### Task 3: Extract Target at first payload; drop hardcoded target

**Files:** Modify `src/client_tun.rs`

- [ ] **Step 1: Read `local_endpoint()` in `process_listener_activity`** alongside payload:

```rust
    let extracted = {
        let tcp_socket = sockets.get_mut::<TcpSocket>(handle);
        let payload = extract_socket_payload(tcp_socket);
        let target = tcp_socket.local_endpoint().map(target_from_endpoint);
        payload.map(|p| (p, target))
    };

    if let Some((payload, target)) = extracted {
        let Some(target) = target else {
            println!("⚠️ handle {:?} 无 local_endpoint，跳过开远端", handle);
            return Ok(());
        };
        handle_local_payload(handle, payload, target, socket_ctxs, ctrl, global_tx).await?;
    }
```

- [ ] **Step 2: Thread Target into `handle_local_payload`**: add `target: TargetAddr` param,
  and in the open-remote branch replace `target: ctx.target.clone()` with `target`:

```rust
    let request = RelayRequest::Tcp { target };
    println!("🎯 handle {:?} extracted target {}", handle, request_target_display);
```

(Print the target before moving it into the request, or clone for the log.)

- [ ] **Step 3: Remove hardcoded-target plumbing:**
  - delete `const DEFAULT_TUN_TARGET`
  - remove `target_addr` field from `TunListenerConfig` + its `from_sources` param/parse
  - remove `target` field from `SocketCtx` + fix `SocketCtx::new` signature
  - remove `target_addr` from `TunRuntimeConfig::from_sources` and `MINI_VPN_TUN_TARGET_ADDR` from `from_env`
  - remove `default_target` from `start_tun_proxy` / `build_listener_pool` / startup log

- [ ] **Step 4: Update tests** in `src/client_tun.rs`:
  - drop `target_addr` / `to_wire_string` assertions and the `httpbin.org:80` default-target test
  - drop `rejects_invalid_target_addr` (no longer a config field)
  - fix `build_listener_pool` and `rearm_socket` tests to construct `SocketCtx` without `target`
  - fix `from_sources` call sites to the new arity

- [ ] **Step 5:** `cargo test` → PASS.

- [ ] **Step 6: Commit** `feat(tun): relay to extracted Target instead of hardcoded address`.

### Task 4: Teaching note + full validation

**Files:** Create `docs/tech/08-target-extraction.md`; Modify `src/client_tun.rs` if lints surface

- [ ] **Step 1: Write the teaching note** covering:
  - what AnyIP is and why the default route gateway is the interface's own IP
  - that the Target is extracted from `local_endpoint()` and is always IP:port (link ADR-0001)
  - the single-fixed-port limitation and that Stage 9 adds arbitrary ports
  - the manual acceptance recipe (example.com:80 routed into utun)

- [ ] **Step 2: Full validation suite:**

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Expected: all PASS.

- [ ] **Step 3: Manual end-to-end** (from the spec recipe): route `93.184.216.34` into utun,
  `curl http://93.184.216.34/`, confirm client logs the extracted target and server connects
  to that IP and returns example.com HTML.

- [ ] **Step 4: Commit** `docs(tun): add stage 8 target extraction note`.
