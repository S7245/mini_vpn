# 刀14c spec — TCP downlink/backpressure instrumentation + TUN MTU alignment

> 日期：2026-06-30 ｜ 分支：`codex/knife14c-downlink-mtu-instrumentation`
> 配套 plan：`docs/tech/2026-06-30-knife14c-downlink-mtu-plan.md`。
> 输入：`docs/tech/2026-06-30-knife14b-usclient-results.md` + 原始 bundle
> `/tmp/conn/mvpn_knife14b_usclient_suite_20260630_142932.tar.gz`。

## TL;DR

刀14b 已证明 US Client / Exit / Target 路由和 TUIC 认证都正确，但 TCP 下行已经在 P1 reverse
塌到约 `2.06 Mbit/s`，P2+ 还会 reset。因此 **不要先写 connection pool**。刀14c 第一 stage 只做两件事：

1. **把 TUN MTU 与 smoltcp DeviceCapabilities 对齐**，让测试基准的 MTU=1200 从进程启动时即生效。
2. **给 TCP downlink / relay / TUN flush 加定位级观测**，能回答 reset 时每个 handle 到底读了多少远端字节、
   `send_slice` 接受了多少、pending 堆了多少、是否卡在 global_rx/back_tx 或 TUN flush。

行为重写只在下一步做：如果复跑后 counters 指向 dirty 自旋、global_rx 背压或 pending 不排空，再针对性改。

## Grounding

已确认的事实：

- Target route：`43.130.32.77 dev tun0 src 10.0.0.1`；Exit route 不进 tun0。
- TUIC 启动、认证、UDP driver ready 正常。
- MTU 1500 forward P1：receiver `476 Kbit/s`。
- MTU 1200 forward P1：receiver `29-33 Mbit/s`。
- MTU 1200 reverse P1：client receiver 仍约 `2.06 Mbit/s`，remote sender 约 `141 Mbit/s`。
- MTU 1200 P2+：`iperf3: unable to receive results` / `Connection reset by peer`。
- client log 多次只有粗粒度：
  - `写入上游流失败: Custom { kind: ConnectionReset, error: Stopped(0) }`
  - `远端服务器关闭了车厢 SocketHandle(...)`
- reverse 期间 `🔬`：`relay≈97-99%`，`poll≈0.2-0.6%`。这不是 smoltcp poll CPU 墙。
- 代码事实：`VirtualTunDevice::capabilities()` 仍硬编码 `max_transmission_unit=1500`，而 14b 脚本是在
  client 已启动后再 `ip link set tun0 mtu 1200`。OS MTU 与 smoltcp cap 可能分裂。

## Scope

### In

- 新 runtime env：`MINI_VPN_TUN_MTU`，默认保持 1500；14c suite 显式设 1200。
- `tun::Configuration::mtu()`、`VirtualTunDevice` read buffer、`DeviceCapabilities.max_transmission_unit`
  全部使用同一个 configured MTU。
- TCP downlink counters:
  - per handle current/high-water `downlink_pending`;
  - remote -> global_rx bytes;
  - bytes accepted by `tcp_socket.send_slice`;
  - zero-write / send error count;
  - relay close/reset reason with handle and cumulative up/down bytes;
  - back_tx/global_rx pressure events when relay task sends remote bytes to the loop;
  - TUN `flush_tx` calls/failures with stage labels.
- Extend the existing US-client suite so the user still runs one command and gets md/tar.

### Out

- No connection pool.
- No event-loop sharding.
- No broad TCP relay rewrite before counters identify the mechanism.
- No default MTU policy change beyond adding the knob; the script/test baseline chooses 1200.
- MSS clamp is deferred until after MTU/cap alignment is retested. If reverse still collapses with aligned
  MTU, the next stage can add explicit SYN MSS clamp / advertised MSS control.

## Design

### D1. MTU source of truth

`TunRuntimeConfig` gains `tun_mtu`. `from_env()` reads `MINI_VPN_TUN_MTU`; `from_sources()` keeps tests on
the default unless a unit test overrides it. Valid range: `576..=9000` for IPv4 sanity and practical jumbo
headroom; invalid/zero falls back to default in env parsing, while explicit config parsing returns a typed error
where tests need it.

`create_tun_device(tun_mtu)` applies `.mtu(tun_mtu as i32)` before `.up()`. `VirtualTunDevice::new(device, mtu)`
stores the same IP MTU, uses it to size `wait_for_rx` buffers, and returns it in `capabilities()`.

This makes the 14c acceptance baseline exact: starting with `MINI_VPN_TUN_MTU=1200` means Linux TUN MTU and
smoltcp's IP MTU agree from the first SYN.

### D2. Keep MetricsSnapshot stable; add TCP diagnostic logs/counters locally

刀11 `MetricsSnapshot` is a coarse data-plane contract. The 14c per-handle data is diagnostic and potentially
high-cardinality, so it should not be forced into the frontend snapshot yet.

Implementation shape:

- Extend `SocketCtx` with downlink counters owned by the loop task.
- Extend `run_relay` with task-local counters for uplink/downlink bytes and global_rx/back_tx pressure.
- On close/reset/idle, print one directional line with handle, reason, bytes, pending counters, and pressure.
- On each `metrics_tick`, print an aggregate TCP-downlink diagnostic line: pending total/max/high-water max,
  downlink remote bytes, `send_slice` accepted bytes, zero/write-error counts, flush calls/failures.

If later the frontend needs these values, promote only stable aggregate fields into `MetricsSnapshot` in a
separate contract change.

### D3. Flush failures become observable

Current TCP poll/timer paths call `device.flush_tx().await.unwrap()`. 14c wraps all flush sites in one helper
that increments call/failure counters and logs the stage on error. The loop should keep running after a single
flush error when possible; the error is a signal to diagnose, not a reason to lose every other handle.

### D4. Dirty/backpressure behavior is not changed in stage 1

Existing `flush_downlink` already preserves partial `send_slice` tails in `downlink_pending`. Stage 1 only
measures whether it drains in real US-client reverse runs. Dirty-handle scheduling, global_rx channel sizing,
and relay read/write chunking stay unchanged unless the next bundle proves they are the mechanism.

## Acceptance

Local gates:

- `cargo test`
- `cargo test --features harness`
- `cargo clippy --all-targets --features harness`
- `cargo build --release`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`
- `git diff --check`

US-client rerun gates:

- Script starts client with `MINI_VPN_TUN_MTU=1200`; report shows TUN MTU and smoltcp configured MTU.
- Forward P1 at MTU 1200 does not regress.
- Reverse P1 no longer collapses to ~2M, or the report contains per-handle counters explaining where it stalls.
- P2 no longer breaks iperf result/control, or reset logs identify direction, handle, and byte counters.
- The bundle is sufficient to decide whether connection pool remains relevant.
