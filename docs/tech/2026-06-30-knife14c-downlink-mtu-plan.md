# 刀14c plan — TDD 分解

> 配套 spec：`docs/tech/2026-06-30-knife14c-downlink-mtu-spec.md`。
> 节奏：每 task 红→绿→质量子门→commit；阶段结束跑 `/code-review`。保留当前交接类 dirty 文件，不回退。

## 任务树

```text
T0 spec/plan
 ├─ T1 MINI_VPN_TUN_MTU config parser
 ├─ T2 VirtualTunDevice MTU storage + smoltcp cap alignment
 ├─ T3 create_tun_device applies configured MTU and startup logs it
 ├─ T4 TCP downlink per-handle counters around pending/send_slice
 ├─ T5 relay close/reset directional counters + global_rx pressure
 ├─ T6 observed flush_tx helper + aggregate diagnostic line
 └─ T7 US-client suite exports MINI_VPN_TUN_MTU and captures 14c diagnostics
```

## T0 — spec/plan

- Add this spec and plan.
- Commit: `docs(knife14c): spec and plan for downlink instrumentation + MTU alignment`

## T1 — runtime MTU config parser

Red:

- `TunRuntimeConfig::from_sources` or a pure parser test accepts `1200`.
- Rejects / falls back for `0`, non-number, below `576`, above `9000`.
- Default remains `1500`.

Green:

- Add `DEFAULT_TUN_MTU: usize = 1500`.
- Add `tun_mtu: usize` to `TunRuntimeConfig`.
- Add env parser for `MINI_VPN_TUN_MTU`.

Commit: `feat(knife14c): add TUN MTU runtime config`

## T2 — VirtualTunDevice MTU/cap alignment

Red:

- Unit test constructs `VirtualTunDevice` or a small helper with MTU 1200 and asserts
  `capabilities().max_transmission_unit == 1200`.
- Unit test covers receive buffer sizing helper, including macOS PI header headroom.

Green:

- Store IP MTU in `VirtualTunDevice`.
- Replace hard-coded 1500/1504 receive buffers with `mtu + platform_header_len`.
- Return stored MTU in `DeviceCapabilities`.

Commit: `fix(knife14c): align smoltcp device MTU with configured TUN MTU`

## T3 — apply MTU at OS TUN creation

Red:

- Unit test for a `build_tun_config` helper asserts address/destination/netmask/up plus MTU.

Green:

- Extract `build_tun_config(tun_mtu)`.
- `create_tun_device(tun_mtu)` calls `config.mtu(tun_mtu as i32)`.
- `start_tun_proxy` logs configured MTU and constructs `VirtualTunDevice::new(raw_tun, runtime_config.tun_mtu)`.

Commit: `fix(knife14c): configure OS TUN MTU at startup`

## T4 — loop-owned TCP downlink counters

Red:

- Unit test for `flush_downlink` with a tiny TCP socket or helper asserts:
  - pending high-water updates when remote payload enters;
  - `send_slice` accepted bytes accumulates;
  - partial writes leave tail pending.

Green:

- Add downlink counters to `SocketCtx`.
- Update them in `handle_remote_payload` and `flush_downlink`.
- Keep existing delivery behavior unchanged.

Commit: `feat(knife14c): instrument TCP downlink pending and send_slice progress`

## T5 — relay task close/reset counters

Red:

- `run_relay` unit tests cover remote EOF / write error / idle close and assert the emitted summary data
  through a pure `RelayCounters` formatter/helper.
- Backpressure helper counts a pressure event when `back_tx.capacity()==0` before sending remote bytes.

Green:

- Add task-local counters for local->remote bytes, remote->loop bytes, read/write errors, EOF, idle.
- Log one summary on exit with direction, handle, and counters.
- Count global_rx pressure locally; do not change channel size yet.

Commit: `feat(knife14c): log relay close direction and byte counters`

## T6 — observed TUN flush helper + aggregate diagnostics

Red:

- Unit test helper increments calls and failures on success/error.
- Formatter test for aggregate `📊 TCP↓` diagnostic line.

Green:

- Wrap all `flush_tx` call sites in an observed helper.
- Replace TCP path `unwrap()` with logged failure accounting.
- On metrics tick, print aggregate TCP-downlink/TUN-flush diagnostic line from loop-owned state.

Commit: `feat(knife14c): observe TUN flush and TCP downlink aggregate pressure`

## T7 — US-client suite update

Red:

- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`.
- Grep/static check that startup command exports `MINI_VPN_TUN_MTU`.

Green:

- Add `TUN_MTU=${TUN_MTU:-1200}` env.
- Start client with `MINI_VPN_TUN_MTU=$TUN_MTU`.
- Keep `ip link set` as a verification/fallback step, but label the configured startup MTU separately.
- Capture `📊 TCP↓`, relay summary, flush failures, and MTU startup lines in probe summaries.

Commit: `test(knife14c): extend US-client suite with startup MTU and TCP downlink diagnostics`

## Stage close

- Full local quality gates from spec.
- `/code-review` over the 14c-a diff.
- Fix findings with additional commits.
- Push branch after each commit if remote is available.
- Ask user to rerun the US-client suite and send the generated bundle.
