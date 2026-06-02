# Stage 10 Yamux Auto-Reconnect Implementation Plan

**Goal:** client-tun auto-reconnects its upstream TLS+Yamux connection on disconnect,
with exponential backoff + full jitter (infinite retry, reset on success), resetting
in-flight smoltcp sockets so upper-layer TCP self-heals. Server unchanged.

**Architecture:** Extract `connect_upstream` returning `(Control, JoinHandle)` plus a
disconnect signal. Main loop owns a `mut Control`, gains a select branch on the
disconnect signal, and runs a backoff reconnect loop that replaces the Control and
rearms in-flight handles. `backoff_delay` is a pure, unit-tested function.

**Tech Stack:** Rust, tokio, yamux 0.10, tokio-rustls, rand (already a dep).

---

## File Map

- Modify: `src/client_tun.rs`
  - Add `backoff_delay(attempt, rand_unit) -> Duration` + tests
  - Add consts `RECONNECT_BASE_MS = 500`, `RECONNECT_CAP_MS = 30_000`
  - Extract `connect_upstream(...)` building TCP+TLS+Yamux, returning Control + a
    disconnect signal + epoch
  - Main loop: `let mut ctr`; select branch on disconnect; reconnect-with-backoff;
    replace ctr; rearm in-flight handles
  - Optional epoch tagging on relay payloads (may downgrade per spec)
- Create: `docs/tech/10-yamux-auto-reconnect.md`
- Modify: `CONTEXT.md` (Reconnect epoch term), `TODO.md` (scale/ops extensions)

---

### Task 1: `backoff_delay` pure function (TDD)

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: Failing tests:

```rust
    #[test]
    fn backoff_delay_full_jitter_lower_bound_is_zero() {
        assert_eq!(backoff_delay(0, 0.0), std::time::Duration::ZERO);
        assert_eq!(backoff_delay(10, 0.0), std::time::Duration::ZERO);
    }

    #[test]
    fn backoff_delay_attempt_zero_upper_is_base() {
        let d = backoff_delay(0, 1.0_f64.next_down());
        assert!(d < std::time::Duration::from_millis(RECONNECT_BASE_MS));
        assert!(d >= std::time::Duration::from_millis(RECONNECT_BASE_MS * 99 / 100));
    }

    #[test]
    fn backoff_delay_is_capped() {
        // huge attempt -> upper bound clamps to CAP
        let d = backoff_delay(30, 1.0_f64.next_down());
        assert!(d <= std::time::Duration::from_millis(RECONNECT_CAP_MS));
        assert!(d >= std::time::Duration::from_millis(RECONNECT_CAP_MS * 99 / 100));
    }
```

- [ ] Step 2: `cargo test backoff_delay` → FAIL.

- [ ] Step 3: Implementation:

```rust
const RECONNECT_BASE_MS: u64 = 500;
const RECONNECT_CAP_MS: u64 = 30_000;

/// Full-jitter exponential backoff delay.
/// 中文要点：random(0, min(CAP, BASE * 2^attempt))，下界取 0 摊平惊群。
/// rand_unit ∈ [0,1) 由调用方注入（运行时 rand::random，测试传固定值）。
fn backoff_delay(attempt: u32, rand_unit: f64) -> std::time::Duration {
    let exp = RECONNECT_BASE_MS.saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX));
    let upper = exp.min(RECONNECT_CAP_MS);
    let ms = (upper as f64 * rand_unit) as u64;
    std::time::Duration::from_millis(ms)
}
```

- [ ] Step 4: `cargo test backoff_delay` → PASS.
- [ ] Step 5: Commit `feat(tun): add full-jitter backoff_delay`.

### Task 2: extract `connect_upstream` with disconnect signal

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: Pull the existing TCP→TLS→Yamux setup (lines ~378-403) into:

```rust
async fn connect_upstream(
    connector: &TlsConnector,
    server_addr: &str,
    domain: ServerName,
    disconnect_tx: mpsc::Sender<()>,
) -> Result<yamux::Control, ClientError> {
    let server_stream = TcpStream::connect(server_addr).await?;
    let tls_stream = connector.clone().connect(domain, server_stream).await
        .map_err(/* map to ClientError */)?;
    let mut yamux_conn = Connection::new(tls_stream.compat(), YamuxConfig::default(), Mode::Client);
    let ctr = yamux_conn.control();
    tokio::spawn(async move {
        while let Ok(Some(_)) = yamux_conn.next_stream().await {}
        let _ = disconnect_tx.send(()).await; // 断开信号回主循环
    });
    Ok(ctr)
}
```

- [ ] Step 2: In `start_tun_proxy`, create `let (disconnect_tx, mut disconnect_rx) = mpsc::channel(1);`
  and bootstrap with a backoff loop calling `connect_upstream` until it succeeds; store `let mut ctr`.
- [ ] Step 3: `cargo check` → PASS (no behavior change yet beyond extraction).
- [ ] Step 4: Commit `refactor(tun): extract connect_upstream + disconnect signal`.

### Task 3: reconnect-on-disconnect in main loop

**Files:** Modify `src/client_tun.rs`

- [ ] Step 1: Add a `select!` branch:

```rust
    _ = disconnect_rx.recv() => {
        println!("🔌 上游连接断开，准备重连");
        // rearm all in-flight handles
        let handles: Vec<SocketHandle> = registry.all_handles().collect();
        let mut reset = 0;
        for h in handles {
            if let Some(c) = socket_ctxs.get_mut(&h) {
                if c.uplink_tx.is_some() {
                    let sock = sockets.get_mut::<TcpSocket>(h);
                    rearm_socket(sock, c);
                    reset += 1;
                }
            }
        }
        println!("♻️ 重连后复位 {reset} 条在途连接");
        // backoff reconnect loop
        let mut attempt = 0u32;
        loop {
            let delay = backoff_delay(attempt, rand::random::<f64>());
            println!("⏳ 第 {} 次重连，等待 {}ms", attempt + 1, delay.as_millis());
            tokio::time::sleep(delay).await;
            match connect_upstream(&connector, &upstream_server_addr, domain.clone(), disconnect_tx.clone()).await {
                Ok(new_ctr) => { ctr = new_ctr; epoch += 1; println!("✅ 上游重连成功 (epoch={epoch})"); break; }
                Err(e) => { println!("重连失败: {e:?}"); attempt = attempt.saturating_add(1); }
            }
        }
    }
```

- [ ] Step 2: Ensure `domain` is `Clone` (ServerName is Clone) and captured re-usably;
  `upstream_server_addr` already a String; `connector` already in scope.
- [ ] Step 3: `cargo test` + `cargo clippy -D warnings` → PASS/clean.
- [ ] Step 4: Commit `feat(tun): auto-reconnect upstream with full-jitter backoff`.

### Task 4: epoch guard (optional hardening) + teaching note + validation

**Files:** Modify `src/client_tun.rs`, `CONTEXT.md`, `TODO.md`; Create teaching note

- [ ] Step 1: (optional) Thread `epoch` into `spawn_remote_relay` and `global_tx`
  payloads; drop stale-epoch payloads in `handle_remote_payload`. If cost/complexity
  is high, downgrade: rely on rearm + natural task exit, note the gap in TODO.
- [ ] Step 2: Write `docs/tech/10-yamux-auto-reconnect.md` (ownership model, backoff +
  jitter rationale at 5000-client scale, in-flight reset, server unchanged, manual recipe).
- [ ] Step 3: Update `CONTEXT.md` (Reconnect epoch), `TODO.md` (failover, control plane,
  L4 LB, rolling restart + graceful drain, heartbeat).
- [ ] Step 4: Full validation: `cargo test` / `check` / `clippy -D warnings` / `doc --no-deps`.
- [ ] Step 5: Manual cross-machine e2e (pending user): kill+restart US server, confirm
  client logs disconnect → backoff → reconnect, and `curl http://1.1.1.1/` works again
  without restarting the client.
- [ ] Step 6: Commit `docs(tun): add stage 10 yamux reconnect note`.
