# Stage 4 Listener Pool Activation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Activate a real `pool_size = 4` listener pool for the TUN TCP path, keep each `SocketHandle` isolated with its own `SocketCtx`, and verify repeated `curl 10.0.0.2:80` requests no longer fall back to the old single-slot failure mode.

**Architecture:** Keep the shared relay protocol unchanged and evolve only `src/client_tun.rs` from a single logical listener slot into a real 4-slot `ListenerPool`. Extract pool construction into a dedicated helper, preserve per-handle state/rearm behavior, add richer EN/CN comments on runtime structures and helpers, then validate with both static checks and, if permissions allow, `server -> client-tun -> curl` runtime checks.

**Tech Stack:** Rust, Tokio, smoltcp, tun, yamux, rustls, cargo test/check/clippy/doc, git

---

## File Map

- Modify: `src/client_tun.rs`
  - Activate real 4-slot pool allocation
  - Add richer EN/CN comments on enums, structs, helpers, and key variables
  - Keep per-handle state and rearm isolated
- Create: `docs/tech/04-real-listener-pool-activation.md`
  - Teaching note for Stage 4
- Modify: `docs/tech/2026-05-07-stage-4-listener-pool-activation-spec.md`
  - Only if implementation uncovers a necessary wording clarification

## Task 1: Extract Real Listener Pool Construction

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs` (unit tests in `#[cfg(test)]`)

- [ ] **Step 1: Write the failing unit test for pool construction**

Add this test module near the bottom of `src/client_tun.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use smoltcp::iface::SocketSet;

    #[test]
    fn build_listener_pool_creates_four_handles_and_contexts() {
        let spec = ListenerSpec {
            local_port: 80,
            pool_size: 4,
        };
        let default_target = TargetAddr::parse("httpbin.org:80").expect("target should parse");
        let mut sockets = SocketSet::new(vec![]);

        let (pool, socket_ctxs) = build_listener_pool(&mut sockets, &spec, &default_target);

        assert_eq!(pool.handles.len(), 4);
        assert_eq!(socket_ctxs.len(), 4);
        assert!(pool.handles.iter().all(|handle| socket_ctxs.contains_key(handle)));
    }
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test build_listener_pool_creates_four_handles_and_contexts -- --nocapture
```

Expected:

```text
error[E0425]: cannot find function `build_listener_pool` in this scope
```

- [ ] **Step 3: Implement `build_listener_pool()` with rich EN/CN comments**

Add this helper in `src/client_tun.rs` above `start_tun_proxy()`:

```rust
/// Build the real smoltcp listener pool for the TUN runtime.
/// 中文要点：一次性创建多个监听槽位，让后续连接不再依赖单个 socket 反复复位。
fn build_listener_pool(
    sockets: &mut SocketSet<'_>,
    spec: &ListenerSpec,
    default_target: &TargetAddr,
) -> (ListenerPool, HashMap<SocketHandle, SocketCtx>) {
    let mut handles = Vec::with_capacity(spec.pool_size);
    let mut socket_ctxs = HashMap::with_capacity(spec.pool_size);

    for slot_index in 0..spec.pool_size {
        let handle = sockets.add(build_listener_socket(spec));
        let ctx = SocketCtx::new(spec.local_port, default_target.clone());
        println!(
            "🧩 listener slot {} created on local port {} with handle {:?}",
            slot_index, spec.local_port, handle
        );
        handles.push(handle);
        socket_ctxs.insert(handle, ctx);
    }

    (
        ListenerPool {
            spec: *spec,
            handles,
        },
        socket_ctxs,
    )
}
```

- [ ] **Step 4: Replace the single-slot initialization with the pool helper**

Change the setup block in `start_tun_proxy()` from single-handle allocation to:

```rust
let (listener_pool, mut socket_ctxs) =
    build_listener_pool(&mut sockets, &listener_spec, &default_target);
```

Remove the old block:

```rust
let mut socket_ctxs: HashMap<SocketHandle, SocketCtx> = HashMap::new();
let socket_handle = sockets.add(build_listener_socket(&listener_spec));
socket_ctxs.insert(
    socket_handle,
    SocketCtx::new(listener_spec.local_port, default_target),
);
let listener_pool = ListenerPool {
    spec: listener_spec,
    handles: vec![socket_handle],
};
```

- [ ] **Step 5: Run the test to verify it passes**

Run:

```bash
cargo test build_listener_pool_creates_four_handles_and_contexts -- --nocapture
```

Expected:

```text
test tests::build_listener_pool_creates_four_handles_and_contexts ... ok
```

- [ ] **Step 6: Commit the pool-construction milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "refactor(tun): add real listener pool construction"
```

Expected:

```text
[main ...] refactor(tun): add real listener pool construction
```

## Task 2: Strengthen Per-Handle Lifecycle And Comments

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Add richer EN/CN comments to runtime types**

Update the type declarations to this shape:

```rust
/// Describes how many local TCP listener slots the TUN runtime should create.
/// 中文要点：这是监听池的蓝图，不代表连接本身，只描述“开几间房、监听哪个端口”。
#[derive(Debug, Clone, Copy)]
struct ListenerSpec {
    /// Local TCP port intercepted on the TUN-side smoltcp stack.
    /// 中文要点：这是虚拟网卡内侧被 smoltcp 截获的本地端口。
    local_port: u16,
    /// Number of independent listener slots created for the same local port.
    /// 中文要点：用多个监听槽位模拟 backlog，避免单 socket 退房时堵住后续连接。
    pool_size: usize,
}

/// Explicit lifecycle state for one listener slot.
/// 中文要点：每个 handle 都要有自己的状态，避免“一个槽位出错、全局都混乱”。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SocketState {
    Listening,
    OpeningRemote,
    Relaying,
    Closing,
    Rearming,
}

/// Per-handle runtime context owned by a single listener slot.
/// 中文要点：这是“房间上下文”，每个 handle 都有一份，专门存本槽位的状态和上行通道。
#[derive(Debug)]
struct SocketCtx {
    local_port: u16,
    state: SocketState,
    target: TargetAddr,
    uplink_tx: Option<mpsc::Sender<Vec<u8>>>,
}
```

- [ ] **Step 2: Add a lifecycle unit test for rearm isolation**

Add this unit test:

```rust
#[test]
fn rearm_socket_restores_listening_state_and_clears_sender() {
    let spec = ListenerSpec {
        local_port: 80,
        pool_size: 1,
    };
    let default_target = TargetAddr::parse("httpbin.org:80").expect("target should parse");
    let mut socket = build_listener_socket(&spec);
    let (tx, _rx) = mpsc::channel(1);
    let mut ctx = SocketCtx {
        local_port: 80,
        state: SocketState::Relaying,
        target: default_target,
        uplink_tx: Some(tx),
    };

    rearm_socket(&mut socket, &mut ctx);

    assert_eq!(ctx.state, SocketState::Listening);
    assert!(ctx.uplink_tx.is_none());
}
```

- [ ] **Step 3: Run the lifecycle test to verify it passes**

Run:

```bash
cargo test rearm_socket_restores_listening_state_and_clears_sender -- --nocapture
```

Expected:

```text
test tests::rearm_socket_restores_listening_state_and_clears_sender ... ok
```

- [ ] **Step 4: Add per-handle state transition logs**

Augment the helpers with logs like:

```rust
println!("🔄 handle {:?} entering {:?}", handle, ctx.state);
println!("🚪 handle {:?} remote session opened", handle);
println!("♻️ handle {:?} rearmed on local port {}", handle, ctx.local_port);
```

Place them in:

- `handle_local_payload()`
- `handle_remote_payload()`
- `rearm_socket()`
- `build_listener_pool()`

- [ ] **Step 5: Run `cargo check` to verify the runtime still compiles**

Run:

```bash
cargo check
```

Expected:

```text
Finished `dev` profile ... target(s) in ...
```

- [ ] **Step 6: Commit the lifecycle and observability milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "refactor(tun): isolate per-handle lifecycle state"
```

Expected:

```text
[main ...] refactor(tun): isolate per-handle lifecycle state
```

## Task 3: Validate Repeated And Parallel TUN Requests

**Files:**
- Modify: `src/client_tun.rs` (only if runtime logs or minor fixes are needed)
- Test: runtime manual check using local terminals

- [ ] **Step 1: Run the full static validation suite**

Run:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Expected:

```text
test result: ok
Finished `dev` profile ...
Finished `dev` profile ...
Generated .../target/doc/mini_vpn/index.html
```

- [ ] **Step 2: Start the relay server**

Run in terminal A:

```bash
cargo run -- server
```

Expected:

```text
服务端启动，正在监听 ...
```

- [ ] **Step 3: Start the TUN client**

Run in terminal B:

```bash
cargo run -- client-tun
```

Expected:

```text
TUN 虚拟网卡主循环启动！监听端口 80，当前槽位数 4
```

If TUN creation fails because of local permissions or device setup, stop here and ask the user to run the same command with the required privileges on their machine.

- [ ] **Step 4: Run repeated curl checks**

Run in terminal C:

```bash
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
curl 10.0.0.2:80
```

Expected:

```text
Each request returns HTTP content instead of failing on the second request.
```

- [ ] **Step 5: Run a light parallel curl check**

Run:

```bash
seq 1 4 | xargs -I{} -P4 sh -c 'curl -s 10.0.0.2:80 >/tmp/mini_vpn_curl_{}.out'
```

Expected:

```text
The command exits successfully and the client logs show multiple handle IDs being used.
```

- [ ] **Step 6: If runtime validation passes, capture the exact user-facing test steps**

Prepare this checklist for the final handoff:

```text
1. Start `cargo run -- server`
2. Start `cargo run -- client-tun`
3. Run `curl 10.0.0.2:80` four times
4. Optionally run the parallel curl command
5. Watch for per-handle `rearmed` logs
```

- [ ] **Step 7: Commit any runtime-only follow-up fixes if needed**

Run only if code changed during runtime debugging:

```bash
git add src/client_tun.rs
git commit -m "fix(tun): stabilize 4-slot listener pool runtime"
```

Expected:

```text
[main ...] fix(tun): stabilize 4-slot listener pool runtime
```

## Task 4: Write The Stage 4 Teaching Note

**Files:**
- Create: `docs/tech/04-real-listener-pool-activation.md`

- [ ] **Step 1: Write the teaching note**

Create `docs/tech/04-real-listener-pool-activation.md` with content shaped like:

```md
# 04 Real Listener Pool Activation

## 背景

Stage 3 只是把 TUN runtime 变成“池化友好骨架”，并没有真的创建多个监听槽位。

Stage 4 真正做的事，是把：

- `pool_size = 1`

变成：

- `pool_size = 4`

并确保每个 `SocketHandle` 都有自己的：

- `SocketCtx`
- `SocketState`
- rearm 路径
- relay 任务

## 为什么不能只把数量从 1 改到 4

如果只改数量，不改状态隔离和日志，最后只是把旧问题复制四份。

## 这一步的核心变化

- 引入真实的 `build_listener_pool()`
- 每个 handle 一份上下文
- 每个 handle 独立回收
- 连续 `curl` 不再依赖单个 socket 的运气

## 手动测试

```bash
cargo run -- server
cargo run -- client-tun
curl 10.0.0.2:80
curl 10.0.0.2:80
seq 1 4 | xargs -I{} -P4 sh -c 'curl -s 10.0.0.2:80 >/tmp/mini_vpn_curl_{}.out'
```
```

- [ ] **Step 2: Run doc generation to ensure the docs change does not introduce issues**

Run:

```bash
cargo doc --no-deps
```

Expected:

```text
Generated .../target/doc/mini_vpn/index.html
```

- [ ] **Step 3: Commit the documentation milestone**

Run:

```bash
git add docs/tech/04-real-listener-pool-activation.md
git commit -m "docs(tun): add stage 4 listener pool teaching note"
```

Expected:

```text
[main ...] docs(tun): add stage 4 listener pool teaching note
```

## Task 5: Final Validation And Handoff

**Files:**
- Modify: `src/client_tun.rs` (only if last-minute fixes are required)
- Modify: `docs/tech/04-real-listener-pool-activation.md` (only if manual test wording needs tuning)

- [ ] **Step 1: Re-run the full validation suite**

Run:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Expected:

```text
All commands succeed without warnings.
```

- [ ] **Step 2: Verify git history and worktree state**

Run:

```bash
git status --short
git log --oneline -n 5
```

Expected:

```text
Working tree clean, and the recent commits show the Stage 4 runtime/doc milestones.
```

- [ ] **Step 3: Prepare the final user handoff**

Include:

```text
- What changed in `client_tun.rs`
- Which tests were run
- Whether `server -> client-tun -> curl` ran successfully or needs the user to run privileged steps
- The exact manual verification commands
- The relevant commit hashes
```

- [ ] **Step 4: If everything is clean, create one final umbrella commit only when needed**

Run only if there are still staged-but-uncommitted Stage 4 changes:

```bash
git add src/client_tun.rs docs/tech/04-real-listener-pool-activation.md
git commit -m "feat(tun): activate 4-slot listener pool"
```

Expected:

```text
[main ...] feat(tun): activate 4-slot listener pool
```

## Self-Review

### Spec Coverage

- Real `pool_size = 4` activation: covered by Task 1
- Per-handle isolation and rearm: covered by Task 2
- Richer EN/CN comments: covered by Task 2
- Static validation: covered by Tasks 3 and 5
- Runtime `server -> client-tun -> curl` checks: covered by Task 3
- Teaching note: covered by Task 4
- Commit strategy: covered by Tasks 1, 2, 3, 4, and 5

### Placeholder Scan

- No `TODO`, `TBD`, or “similar to above” steps remain
- Every task includes concrete files, commands, and expected outcomes

### Type Consistency

- `ListenerSpec`, `ListenerPool`, `SocketState`, `SocketCtx`, and `build_listener_pool()` use one consistent naming scheme across all tasks
