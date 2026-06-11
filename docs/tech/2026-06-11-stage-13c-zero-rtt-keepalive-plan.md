# Stage 13c — 0-RTT 快速重连 + 厘清两层保活 实施计划 (plan)

**Goal:** `tuic` 模式下:① 重连用 **QUIC 0-RTT** 省一次握手 RTT(失败自动 fallback 1-RTT);② **厘清两层保活**
——QUIC keep-alive(连接层)与 TUIC Heartbeat(应用层 UDP 会话)职责分明、周期参数化,TUIC Heartbeat 改为
**按需(仅活跃 UDP 时)发**。legacy + 现有 TCP/UDP 零回归。见 13c spec + ADR-0004。

**排序原则(系统稳定优先):** 先落**低风险、桌面可测、即时价值**的保活厘清(Task 1–2),把 **0-RTT 的互通未知**
(early-exporter / sing-box 行为)集中到 Task 3–4,前面稳了再碰。

**Tech Stack:** Rust,quinn 0.10.2 / rustls 0.21.12(**无需 version bump**,见 spec「能力确认」),无新依赖。

---

## 关键事实(spec 摘要)
- 0-RTT:rustls `crypto.enable_early_data = true`(默认 false);quinn `Connecting::into_0rtt()` →
  `Result<(Connection, ZeroRttAccepted), Connecting>`,失败 `.await` 走 1-RTT。
- 头号风险:TUIC token = `export_keying_material`,0-RTT 用 *early* exporter ≠ 1-RTT exporter → auth 可能不齐;
  退路是 **1-RTT-auth**(握手后发 Authenticate),由互通 e2e 定默认。
- 保活嵌套:`TUIC HB(3s) < QUIC keepalive(5s) < idle(30s)`。按需 HB 用 `last_udp_activity: AtomicU64`。

---

## File Map
- Modify `src/tuic.rs` — `should_send_heartbeat` 纯函数 + `last_udp_activity` + 按需 heartbeat;0-RTT 重连路径。
- Modify `src/quic.rs` — 客户端 `enable_early_data`;keepalive/idle/MTU 周期参数化(默认不变)。
- Modify `src/client_tun.rs` — (按需)从 env 透传 0-RTT 开关 / heartbeat 周期。
- Create `docs/tech/13c-...md` 教学笔记(验收后);Modify TODO / LEARNINGS。

---

### Task 1:按需 Heartbeat + 参数化(TDD,纯逻辑为主)

**Files:** `src/tuic.rs`

- [ ] Step 1:失败测试
  - `should_send_heartbeat(last_activity, now, idle_window)`:窗口内(now-last < window)→ true;窗口外 → false;
    边界(==window)取一侧明确;`last_activity=0`(从未发过 UDP)→ false。
  - (可选)`heartbeat_period`/`keepalive_period` 常量断言(默认值不变)。
- [ ] Step 2:`cargo test --lib tuic` → FAIL
- [ ] Step 3:实现
  - `TuicUpstream` 加字段 `last_udp_activity: AtomicU64`(init 0);`send_udp` 成功/进入时
    `last_udp_activity.store(now_secs)`。**注意**:`send_udp` 当前无 `now`——传入 `now_secs`(主循环
    `udp_clock.elapsed().as_secs()`,与 AssocTable 同源),或 driver/调用方注入,保持可测。
  - 驱动任务 heartbeat tick:`if should_send_heartbeat(last, now, HB_IDLE_WINDOW) { send_datagram(HB) }`,
    否则跳过(纯 TCP idle 不发)。
  - 抽 `should_send_heartbeat` 为模块级纯函数。
- [ ] Step 4:PASS;`cargo build` clean
- [ ] Commit:`feat(tuic): send TUIC heartbeat only while UDP is active`

> 注:`send_udp` 加 `now_secs` 参数会改 `client_tun.rs` 调用点(`handle_tuic_udp_uplink`)——同刀一起改,
> legacy 不受影响。driver 取 `now` 用任务内单调时钟(同 AssocTable 的 `udp_clock` 语义;driver 独立时钟亦可,
> 只要与 store 的 now 同源——优先把 `now` 从主循环/共享时钟喂入,避免双时钟漂移)。

---

### Task 2:客户端开 enable_early_data + 参数化保活常量(TDD config)

**Files:** `src/quic.rs`

- [ ] Step 1:失败/回归测试
  - `client_quic_config_alpn` 构建成功**且** early data 已启用(若无 getter,至少断言构建不报错;
    可加 `#[cfg(test)]` 旁路验证 `enable_early_data` 已设)。
  - keepalive/idle/MTU 参数化后默认值断言不变(常量值或注入默认)。
- [ ] Step 2:FAIL
- [ ] Step 3:实现
  - `crypto.enable_early_data = true`(client config;server 不变)。
  - `quic_transport_config` 的 keepalive/idle/MTU 收敛为可注入参数(默认 = 现常量,逐字不变 → 零回归)。
- [ ] Step 4:PASS
- [ ] Commit:`feat(quic): enable client 0-RTT early data; parameterize keepalive`

---

### Task 3:0-RTT 重连路径(into_0rtt + fallback + 开关)

**Files:** `src/tuic.rs`(+ `src/client_tun.rs` 透传开关)

- [ ] Step 1:可测的纯逻辑
  - 0-RTT 开关解析(`MINI_VPN_TUIC_ZERO_RTT` / `_AUTH_IN_0RTT`)→ config 字段,默认值测试。
  - (握手 I/O 不强测,同 13a/13b;由层 2 兜底。)
- [ ] Step 2:FAIL(开关解析)
- [ ] Step 3:实现
  - `TuicClientConfig` 加 `zero_rtt: bool`(默认 true)、`auth_in_0rtt`(默认按 e2e;先 true,e2e 不通再切)。
  - `handshake`(重连复用):`let connecting = endpoint.connect(...)?;` 然后
    ```
    let conn = if cfg.zero_rtt {
        match connecting.into_0rtt() {
            Ok((conn, _accepted)) => { /* 0-RTT: 立即发 auth(或按 auth_in_0rtt 退路) */ conn }
            Err(connecting) => connecting.await?,  // 1-RTT fallback
        }
    } else { connecting.await? };
    ```
  - `auth_in_0rtt=false` 时:在 0-RTT 连接上**等握手完成**再发 Authenticate(`conn` 仍可用,
    export_keying_material 用 1-RTT exporter)→ 规避 early-exporter 不一致。
  - 日志:`0-RTT accepted/rejected`(可观测,验收要看)。
  - 首连(`connect`)可复用同路径,但无 ticket 时 `into_0rtt` 必 Err → 自然 1-RTT。
- [ ] Step 4:`cargo test --lib --bins --tests` PASS;legacy 零回归。
- [ ] Commit:`feat(tuic): 0-RTT reconnect via into_0rtt with 1-RTT fallback`

---

### Task 4:互通 e2e + docs(无代码,或按 e2e 结果微调 auth_in_0rtt 默认)

**Files:** 教学笔记;TODO;LEARNINGS

- [ ] Step 1:对真 sing-box(开 `zero_rtt_handshake: true`)
  1. **0-RTT**:kill sing-box → 重连日志 `0-RTT accepted`,重连后首个 `curl https://1.1.1.1/` 立即成功;
     `MINI_VPN_TUIC_ZERO_RTT=false` 对照恒 1-RTT 仍正常。
  2. **auth 对齐**:0-RTT auth 被接受?否 → 切 `auth_in_0rtt=false`(1-RTT-auth 退路),记 LEARNINGS。
  3. **保活**:idle>30s 不断连;活跃 UDP(`dig` 循环)关联不被回收;纯 TCP idle 时 TUIC HB 静默(抓包确认)。
  4. **零回归**:legacy TCP/UDP + tuic 13a(curl)/13b(dig)全过。
- [ ] Step 2:教学笔记 + LEARNINGS(尤其 early-exporter 实测结论);TODO 标 13c done,13d / 移动端 next。
- [ ] Commit:`docs(tuic): stage 13c acceptance — 0-RTT reconnect + keepalive clarified`

---

## Then: `/code-review` over the diff + interop acceptance against sing-box.

## Rhythm
- TDD per task:failing test → red → implement → green → commit;**`git push` after every commit**(单写者)。
- End stage with `/code-review` + 对真 sing-box 的 e2e。

## Notes
- 0-RTT 的核心未知(early-exporter vs sing-box)由 Task 3 留两条路 + Task 4 实测定;**先保证 1-RTT 退路恒可用**
  (稳定优先),再追 0-RTT 的满分收益。
- migration / 电量自适应 heartbeat **不在本刀**(移动端 stage);13c 只把保活周期参数化、留接口。
