# mini_vpn 下行 Egress 架构评审与重设计（Gate A + h10d16 框架）

Date: 2026-07-10
Status: 架构评审 / 提案（未实现）
Scope: 纯架构分析。不含代码改动。面向"架构师定方案 + dev agent 执行"的分工。

> **2026-07-10 review disposition:** partially adopted, not executable as
> written. The approved D16 architecture spec and its post-Gate-A amendment
> remain the design source of truth. In particular, do not execute S1, S4, or
> Phase 1 directly from this document.

## Review Disposition

Accepted into the D16 post-Gate-A stage:

- the global drop-debt / per-flow Recovery scope mismatch in B2/B4;
- the production-vs-harness semantic gap in B3;
- the `Some(0)` false drain-progress defect;
- diagnostic Gate A sub-results and eventual experiment/flag convergence,
  without weakening the approved acceptance AND gate.

Rejected or deferred:

- `tun0 tx_dropped` is not evidence that `flush_tx` writes are dropping. On
  Linux TUN it is the kernel-to-userspace transmit-ring drop counter: local
  ACK/control/uplink packets were not read from the TUN file descriptor in
  time. `VirtualTunDevice::flush_tx` is the opposite userspace-to-kernel
  direction and the failed Gate A reported zero flush failures.
- Therefore the TUN-write-writability redesign in S1 targets the wrong edge.
  A bounded kernel-to-userspace TUN RX starvation harness is required before
  selecting a preventive device-pressure mechanism.
- The raw-splice experiment in S4 repeats a capacity question already answered
  by H10d15's `185 Mbit/s` run and bypasses the correctness seams under test.
- Gate A remains one strict `>150 Mbit/s` run with drop/tail all zero. Sub-gates
  may improve attribution but cannot independently unlock Gate B.
- D16 state/credit deletion and old-engine cleanup remain deferred until
  evidence proves a replacement and Gate B completes.

---

## 0. TL;DR

- **现象**：同一 `.27 → .33 → .77` 路径、同一 sing-box 出口（TUIC/BBR）、同为 200M 带宽下，
  **sing-box 客户端 ~178 Mbit/s，mini_vpn ~30 Mbit/s**。传输层与链路已被 sing-box 证明无瓶颈。
  6 倍差距 100% 在 mini_vpn 客户端**下行 egress 数据面架构**里。
- **当前卡点**：knife14h10d16"byte-owned egress"连续三次 Gate A 失败，最新一次（credit-rearm）
  是一个**已在代码层面确认的反馈组合死锁**（drop-debt × DrainOnly 互锁）。
- **根本判断**：Gate A 与 h10d16 框架**各有结构性缺陷，且同源**——都在用"控制律 / 单次测量"
  去对付一个本质是"方差 / 调度"的问题。sing-box 两样都不做，所以它快且不死锁。
- **推荐方向**：**减法** —— 删掉应用层第三重流控（Running/DrainOnly/Recovery + drop-debt），
  背压回归单一信号 `smoltcp send_capacity` + TUN 写边界可写性；Gate 改为**阶梯 + 同窗口对照**；
  重构必须带**强制删除步骤**（收敛为门的一部分）。

---

## 1. 已钉死的关键事实（证据）

| 事实 | 来源 | 含义 |
|---|---|---|
| sing-box 同路径 173–185M；mini_vpn 30M | h10d16 spec §Evidence + 用户实测 | 差距在客户端 egress，不在传输/链路 |
| raw TUIC 流 → sink = 127M（同路径） | D2.4 | QUIC/传输层容量充足 |
| 出口 sing-box = TUIC + `congestion_control: bbr`，port 8443 | `.33:/etc/sing-box/config.json`（结构字段） | 下行由服务端 BBR 发送，客户端只负责消费 |
| QUIC 接收窗已开到 conn 32MB / stream 8MB | `src/quic.rs` | 非窗口饥饿（180ms×22MB/s≈4MB BDP 够用） |
| 单核 smoltcp poll 不是墙 | 刀12 | 单 owner 循环 CPU 不是瓶颈 |
| 反复出现 `data_read_gap_max_ms ≈ 3.4–3.8s`，而所有 counter 全 clean | knife14 gm→gv→h4→h10d* 30+ 份 result doc | gap 在两任务间调度缝隙，不在任何已埋点 |
| 失败方向恒为 reverse-first（下行） | 三次 Gate A | 瓶颈是客户端**接收→消费** |

**推论**：这是一个"客户端消费侧调度/背压"问题，不是传输、CC、窗口、MTU（MTU 是次级杠杆）、
或 CPU 问题。

---

## 2. 问题清单

### Part A —— Gate A（验收门）的缺陷

**A1. 6 个条件 AND 成单点 pass/fail，与自身 Failure Discriminators 脱节。**
spec 的 discriminator 段能区分 capacity / cadence / lifecycle 三类限制，但 Gate A 是"任一破即 FAIL"
的黑箱。三次 Gate A 各死在不同子条件（stall / 0 Mbps / drop+死锁），门只回一个"fail"，信号散落。

**A2. 单次 20s run 无统计效力，却是解锁一切的门。**
knife14gv A/B 已证此系统**不可复现**（147M 一次、0.3M 下一次）。在已知高方差系统上用单跑做 Gate A
= 抛硬币。Gate B 已正确要求"3 跑取中位数 + 同窗口 sing-box 对照"，Gate A 却只有 1 跑、无对照。

**A3. "只准跑一次 Gate A"配额 = 加机器的元凶（流程级缺陷）。**
VPS 跑被配额化 → 每次失败逼出一次本地"最大化修复（加特判）"再跑，而非加探针测量。
稀缺驱动投机式打补丁。修法不是多给几次跑，而是让门**更便宜 + 带对照**，让两次跑之间是测量。

**A4. 150 Mbit/s 绝对阈值，无视同窗口路况。**
三次跑 direct baseline 277/280/286 有 ~3% 抖动；sing-box 173–185。若某窗口路况塌到 120M，代码再好
也过不了 150 → **假 FAIL**。Gate A 无"相对对照地板"（Gate B 有）。

**A5. 对一条永不终止的 reverse flow 测 close 语义，本身不可测。**
credit-rearm 原话："close accounting remained unobserved because both flows stayed active in
DrainOnly until suite shutdown." Gate A 要求 `close_egress_bytes=0` 等，但 flow 在 20s 内没干净终止时
这些条件是 **untestable，不是 pass/fail**。稳态吞吐门与 teardown 干净度门必须拆开。

### Part B —— h10d16 框架的缺陷

> 说明：h10d16 spec 本身质量高——byte-ownership 不变量、single-owner、bounded caps、EOF 契约、
> failure discriminators 都对。以下缺陷针对**架构选型 / altitude**，不否定这些正确部分。

**B1. altitude 错：在 QUIC + smoltcp 已免费提供的流控之上，重造第三层控制律。**
QUIC 有拥塞控制（服务端 BBR）+ 流控（receive_window）；smoltcp TCP 有发送窗（对 app 的 TCP window）。
h10d16 又叠 Running/DrainOnly/Recovery + drop-debt + drain-credit 时钟。**三层流控互相打架**，
finding#1 死锁即此层产物。sing-box 用一句 `io.Copy` 跑到 178M，无任何此类结构。

**B2. "drop 反馈作断路器"被做反：断路器的复位路径被它自己切断。**
spec §195 意图正确（"circuit breaker, not the primary pacer"），但实现让 drop-debt 成为一个
**只能由它自己禁止的 admission 来复位**的锁存器：
- 代码层确认（`src/client_tun.rs`）：`note_observed_send_queue`（~L3166）只在观察到**本 flow
  send_queue 下降**时才 `pay()` debt；DrainOnly（`allow_admission=false, max_quantum=0`）禁止 admission
  → send_queue 掉到 0 后不再增长 → 永远观察不到下降 → `drop_debt` 永不清 →
  `transition_egress_phase`（`src/tcp_egress.rs:52`）恒返回 DrainOnly。
- 实测：`d16_drain_only_cycles=8283 / drain_bytes=0`，12.2 Mbit/s 后冻结。
- **任何断路器，若其 reset 路径被它跳闸的东西挡住，就是设计缺陷，不是偶发 bug。** 死锁是此拓扑的必然。

**B3. 框架核心不变量在本地不可证伪。**
本地契约 harness（`D16HarnessFlow::cycle`）用 `downlink_inflight_permit_bytes` 驱动 `below_low`、
`drop_debt`/`drain_progress` 用字面量（第二次相变硬编码 `true`）；生产用 `snapshot.send_queue` +
真实 `DownlinkEgressDropDebt` 时钟派生。**恰恰是"DrainOnly 下 send_queue 不再下降 → 债永不付"这个
跨任务时序决定了生产死锁，而 deterministic harness 天生抽象掉了它** → 39/39 本地全绿仍在真机死锁。
一个关键不变量本地无法证伪的框架，会持续把死锁发到 VPS。

**B4. 全局 drop-debt 压在 per-flow 相变之上，是 spec 未 own 的耦合。**
debt 全局一份（一个 generation），却强制**所有** flow 进 DrainOnly，又要求**逐 flow**偿付。
spec §189"drop debt is clear 后才 Recovery"未规定**哪条 flow 的 drain 清哪份全局 debt**。
这个 one-global-debt × N-per-flow-clock 的歧义即死锁钻入的缝——**spec 缺口，非仅代码缺口**。

**B5. 扩张而非替换，无强制收敛（元缺陷）。**
stage 11："Remove or retain old H4/D2/D3/D6/D15 paths only according to evidence." → 即便 h10d16
完全正确，也只是落成**第 5 套 relay engine + 又一个 flag**（现 58 个 `MINI_VPN_*`）。
保留是默认、删除永远可推迟 → 架构永不收敛，测试面组合爆炸。

**附：一个即使修好死锁也会激活的 bug** —— `transition_dirty_d16_egress_phases` 用
`completed_drain_bytes.is_some()` 作 `drain_progress`，`Some(0)` 也算"有进展" → 零字节 drain 周期被当作
Recovery 进展，四个空周期即 Recovery→Running 过早重开 admission。当前被 drop-debt 优先级掩盖。

### 共同根因（一句话）

> **Gate A 与框架同源：都在用"控制律 / 单次测量"对付一个本质是"方差 / 调度"的问题。**
> 框架**加控制律**（debt、相变）去驯服本该由 TCP/QUIC 流控免费驯服的东西；
> Gate A **单次采样**一个高方差随机过程。sing-box 两样都不做（用原生流控 + 天然带对照的跑），
> 所以它 178M 且不死锁。

---

## 3. 推荐解决方案

### S1. Egress 背压重设计（核心）—— 从"控制律"回到"原生流控"

**原则：背压 = 单一信号，且在压力发生的边界就地吸收，而不是用应用层状态机建模。**

删除：`EgressPhase{Running,DrainOnly,Recovery}` 状态机、`DownlinkEgressDropDebt`、
`DownlinkEgressClock` drain-credit、以及围绕它们的 `pressure_credit_debt` / headroom / target-edge /
adaptive-credit 全部旋钮。

保留（这些不依赖控制律，删控制律不动它们）：single-owner smoltcp/TUN、byte-ownership 会计与
bounded caps（per-flow 512KiB / process 64MiB）、EOF/close 契约、独立 quinn 读/写泵任务、observability seam。

**目标数据流（无 credit、无 phase）：**

```text
quinn::RecvStream owner task（每 flow 一个）
  └─ 仅当 per-flow byte queue 有容量时才读 quinn；容量满则 await（队列容量 = 背压）
       └─ commit 到 per-flow leased byte queue
            └─ 单 owner egress actor：把队列排入 smoltcp
                 └─ 唯一 admission 闸门 = socket.send_capacity()；写不动就停，不建债、不切相
                      └─ iface.poll → TunIo::flush_tx
```

**端到端背压链（全靠原生流控，自复位）：**
```
smoltcp send_capacity==0 → 停止 admission → byte queue 不再被 drain
  → queue 满 → reader 停读 quinn → quinn 停发 window → 服务端 BBR 自动降速
```
这正是 sing-box 依赖、也是 mini_vpn **刀13 上行已经证明可行**的模型（`try_reserve` + TCP 窗背压）。
本方案 = 把上行这条**已验证正确**的哲学对称地搬到下行。

**TUN tx drop 的正确处理（drop-debt 存在的真正理由，必须正面解决）：**

drop-debt 机器存在，是因为高吞吐下 `flush_tx` 写 TUN 时 qdisc/txqueue 满 → 内核丢包（`tx_dropped`），
而 smoltcp 以为已发出 → 光靠 `send_capacity` 无法感知 TUN 侧压力。**正确的架构不是应用层建债，而是
在 TUN 写边界就地吸收压力：**

- 让 egress actor 的 TUN 写路径**对 device 可写性施加背压**：当 `flush_tx` 会丢包时，**不推进**
  （bytes 留在 smoltcp TX buffer / TUN tx 队列），这自然让 `send_capacity` 保持低位 →
  下一周期 admission 被抑制 → 逐级回压 quinn 读。**丢包变成暂停，且随 TUN drain 自动复位，无锁存。**
- TUN device 是**共享**资源（一条），所以此背压是**per-device 而非 per-flow**：device 满则**所有**
  egress 暂停——这是正确的（device 是共享瓶颈），且只需 await 一次 device 可写性，不 per-flow 阻塞。
- dev agent 须先核实当前 `TunIo::flush_tx`（`src/device.rs`）语义：是丢包还是阻塞。
  目标是把"丢包"改成"可写性背压 / 有界 txqueue + await"。这是"在压力发生的边界吸收"的落点。

> altitude 论证：B2 的死锁根源是"把 TUN 压力搬到应用层用债务建模"。S1 把它搬回**压力真正发生的边界
> （TUN 写）**，用**已存在的机制（smoltcp send buffer + async 可写性）**吸收，自复位、无相变、本地可测。

### S2. Gate 重设计（不改产品代码即可做）

- **拆成阶梯，各自独立记录、可分别推进：**
  - **G-cap**：稳态吞吐 ≥ 同窗口 sing-box 对照的 90% 且 ≥ 150M；
  - **G-clean**：一次**刻意 close** 后 tail 全零（`tx_dropped_delta / close_egress_bytes /
    terminal_pending_reap` = 0）；
  - **G-gap**：active read/egress gap < 1s。
- **Gate A 从第一次就并列一条同窗口 sing-box 对照跑**，阈值改**相对**（对照的 90%）→ 消除 A4 假 FAIL。
- **取消"只准一次"配额**；把 suite 做**便宜/可重复/自带对照** → 两次跑之间是**测量**（A3）。
- **teardown 干净度用刻意 close 测**，与稳态吞吐分离（A5）。
- Gate 输出映射到 spec 的 Failure Discriminators（A1）：每次失败直接落到某个 discriminator，而非黑箱。

### S3. 收敛强制（元缺陷 B5 的解）

- 重构落地即**删除**被它取代的引擎与 flag（D2/D3/D6/D15/H4 相关 + 对应 `MINI_VPN_*`）。
- **acceptance 门加一条硬指标：`MINI_VPN_*` flag 数量净减少、relay engine 数量净减少。**
  让"收敛"成为门的一部分，而非可推迟的可选项。

### S4. 迁移 / 去风险（先证伪，再动大刀）

- **D0 falsifier（第一步，最省成本）**：加一条 raw-splice 旁路——单流下行，把整套 credit/debt/phase
  旁路，仅 `quinn read → send_slice → poll → flush_tx` 紧循环，看能否直接冲到 ~120M+。
  - 若能（D2.4 sink=127M 已强烈暗示）→ 证明整层控制律是可删负债，放心执行 S1。
  - 若不能 → 墙在 TUN 逐包成本 / MTU，转 MTU-1500 黑洞修复 + 批量 I/O（见 S5）。
- branch-by-abstraction 允许，但**必须带删除 deadline**，不是开放式共存。

### S5. 次级杠杆（S1 达标后按需，非首要）

- **修 MTU-1500 黑洞**：knife14b 实测 MTU 1500 → 476Kbit/s（分片/PMTU 黑洞），1200 → 33M。
  sing-box 跑 ~1500 + GSO。修好回收 20%+ 头开销与 pps。
- **批量 TUN I/O**：`readv/writev` + buffer 复用，消灭逐包 `Vec::to_vec`；Linux client 上评估 GSO/GRO。
- **分片 smoltcp（scale-out）**：K 片 {Interface, SocketSet, owner task} 按 4-tuple 哈希——
  仅 ③ 大并发 / 单核 headroom 需要时做，单流 178M 用不到。

---

## 4. 优点保留映射（回答"新设计能否保留优点"）

| 类别 | 内容 | 重构后 |
|---|---|---|
| **A 完全正交，一行不动** | fake-IP + :53 DNS 劫持 + 加密 DNS 阻断（ADR-0006/7）；REALITY 第二 Transport + KeyUpdate（ADR-0008/9/10）；failover（ADR-0011）；UDP plane（datagram/stream/frag）；刀2 脏集合/弹性扩容/fake-IP refcount；Metrics 核心（ADR-0012） | 不进本次实验爆炸半径 |
| **B 被保留并强化** | single smoltcp/TUN owner 不变量；刀13 非阻塞 uplink（TCP 窗背压）→ 下行对称化；刀1 harness + `TunIo/ProxyUpstream/DatagramUpstream/MetricsSink` traits（新循环的 TDD 载体）；h10d16 spec 的 byte-ownership/EOF/bounded-caps/observability | 更纯、被复用 |
| **C 被删除（即目的）** | credit/pacing/pressure-debt/drain-clock/actor credit 变体；`D2/D3/D4/D5/D6/D11/D15/thin/continuous/buffered` flag 族；4 套并存 engine 的多余 3 套 | 失败假设的残渣 |

**4 条动刀护栏（非优点，是易误伤的正确性）：**
1. lifecycle / close-tail / reap-epoch / anti-crosstalk 守卫——删 credit/pacing，**不删** close-tail 兜底与重连代际守卫；
2. `ProxyUpstream` seam 不塌——下行 drain 须仍在 upstream 抽象之下，TUIC 与 REALITY 两腿共享同一 `send_capacity` 背压；
3. 两平面公平性——TCP drain 不得饿死 UDP 下行（drain cycle 保留 budget 边界，让出 select）；
4. 区分生产旋钮（`TUIC_CC / UDP_MODE / MTU_POLICY / REALITY_* / TUN_MTU`，保留并从 env 提升为可注入 config）
   vs 实验 flag（删）。

---

## 5. 给 dev agent 的执行清单

**Phase 0 — 证伪（不动生产默认）**
- [ ] 加 raw-splice 旁路（feature-gated），单流下行紧循环 `quinn read → send_slice → poll → flush_tx`。
- [ ] 本地 TDD：证明旁路满足 byte-ownership 会计与 EOF；harness 用**真实 send_capacity 语义**（非 inflight-permit）。
- [ ] 一次单流 reverse-first P1 + 同窗口 sing-box 对照，看是否 ≥ 对照 90%。
- [ ] 若达标 → 进 Phase 1；若不达标 → 转 S5（MTU/批量），暂停控制律讨论。

**Phase 1 — 减法重构（核心）**
- [ ] 删 `EgressPhase` 状态机 + `DownlinkEgressDropDebt` + `DownlinkEgressClock` drain-credit 及配套旋钮。
- [ ] 背压统一为 `socket.send_capacity()`（admission）+ TUN device 可写性（`flush_tx` 边界）。
- [ ] 核实并改造 `TunIo::flush_tx`：丢包 → 可写性背压 / 有界 txqueue + await。
- [ ] 保留 byte-ownership 会计、bounded caps、EOF/close、observability。
- [ ] 补 harness：用**生产信号（send_queue / device 可写性）**驱动，锁住"TUN 满 → 全局暂停 → 自复位"，
      使本次死锁类问题本地可证伪（修 B3）。

**Phase 2 — Gate 重设计（并行）**
- [ ] suite 拆 G-cap / G-clean / G-gap，各自独立记录。
- [ ] 每次 mini_vpn 跑并列同窗口 sing-box 对照；阈值相对化（≥90%）。
- [ ] teardown 干净度用刻意 close 单独测。
- [ ] 去掉单次配额，suite 做便宜可重复。

**Phase 3 — 收敛与验收**
- [ ] 删除被取代的 engine/flag；acceptance 加"`MINI_VPN_*` 与 engine 数量净减少"硬指标。
- [ ] 跑 G-cap → G-clean → G-gap 阶梯；再 Gate B（3 跑中位数 vs 对照）。
- [ ] 回归：UDP/直播、fake-IP DNS、TUN lifecycle、③ 大并发。

**停止规则**
- 未过本地"send_capacity/TUN 背压 + byte-ownership + EOF"证伪测试前，不跑 VPS。
- 失败后先用 discriminator 定位 capacity vs cadence，再改代码；**不再新增一版 debt/credit 语义**。
- 单次 >170M 不算稳定验收。

---

## 6. 一句话结论

> Gate A 有缺陷（单次采样高方差过程、无对照、AND 黑箱、测不可终止流的 close）；
> h10d16 框架 spec 好但 altitude 错（在 QUIC+smoltcp 流控上再造控制律，其断路器无法自动复位、
> 核心不变量本地不可证伪、只扩张不收敛）。二者同源——都在用控制律/单次测量对付方差与调度问题。
> **正确的架构动作是减法（背压回归 `send_capacity` + TUN 写边界，删掉第三层控制律）+ 相对对照的阶梯门 +
> 强制收敛，而不是再修一版 debt 语义。**
