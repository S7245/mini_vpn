# mini_vpn — 现状评估与后续路线图（Assessment & Roadmap）

> **本文档给未来的 Claude / 维护者**：读完这一篇 + 下面「关键文档」里的 4 个文件，你就掌握了
> 项目当前能力、距离「日常丝滑」的差距、以及按什么顺序往下做。继续沿用既定节奏：
> **grill 设计树 → spec → plan → TDD 逐 task commit → 每 stage 跑 /code-review → 跨机验收**。
> 工作原则见文末「工作约定」。最后更新：2026-06-08。

---

## 1. 这是什么项目

学习导向的 VPN：macOS（深圳）本地用 TUN + 用户态 TCP/IP 栈（smoltcp）拦截流量，经
TLS/QUIC 隧道转发到远端代理（美国出口），由出口代连真实目标。fake-IP 模式绕本地 DNS 污染。

- 客户端入口：`src/client_tun.rs`（TUN 主循环，单线程独占 smoltcp + flow 表）
- 服务端：`src/server.rs`（TCP/TLS/yamux + 新增 QUIC endpoint）
- 共享协议：`src/shared/`；fake-IP/DNS：`src/fake_ip.rs` `src/dns.rs`
- UDP relay（QUIC datagram）：`src/udp_relay.rs` `src/quic.rs` `src/device.rs`

---

## 2. 当前状态（已完成且验收）

| 能力 | 状态 | 备注 |
|---|---|---|
| TCP 透明代理（任意 IP/端口，含 443） | ✅ Stage 8/9 | smoltcp + yamux |
| 上游断线自动重连（full-jitter） | ✅ Stage 10 | 客户端侧 |
| fake-IP DNS（绕污染，出口解析域名） | ✅ Stage 11 | 全量模式（系统 DNS 全指 fake resolver） |
| **UDP relay（QUIC datagram，QUIC/HTTP3 + UDP）** | ✅ **Stage 12** | 跨机验收：ATYP=1/3、1200B 冷启动、并发 160/160、长稳。见 `docs/tech/12-*.md` |

**Stage 12 北极星定位（重要）**：数据平面要**统一到 QUIC**（ADR-0003）——TCP 走 QUIC stream、
UDP 走 QUIC datagram。Stage 12 是第一刀（只做 UDP，新数据面与现有 TCP/yamux 并存、零回归）。

---

## 3. 距离「日常丝滑」的差距分析（按场景）

当前「功能能通」，但要**正常速度、不卡顿**地刷 Facebook / YouTube / 打游戏，还有差距。
诚实结论：**一半是代码/架构，一半是基建（出口 IP / 带宽 / 位置 / 抗封），后者常是真瓶颈。**

| 场景 | 现在能通吗 | 主要卡点 |
|---|---|---|
| 浏览 Facebook（HTTPS/TCP + 部分 QUIC） | 能 | **TCP 全走一条 yamux/TCP 长连 → 跨流 HOL 阻塞**：一个网页几十个并发请求挤一条 TCP，丢包全停 → 转圈 |
| YouTube（大吞吐流媒体 + QUIC） | 能起 | HOL + **单连接吞吐天花板**（高 RTT×小窗口）+ 出口带宽 + UDP 超限丢包 |
| 打游戏（低延迟 UDP） | 能转发 | **延迟由物理 RTT 决定**（深圳↔美国 ~180ms，代码降不下来）→ 要低延迟得换近节点 |

---

## 4. 后续路线图（优先级 = 性价比）

### 代码 / 架构（挂在 ADR-0003「统一 QUIC」北极星下）

1. **规则分流 geosite/geoip（像 Clash）** — 中等工作量，体感收益最大。
   现在全量走美国，国内站点也绕一圈 → 慢。改成「只代理被墙域名 / 国内直连」。
   不依赖传输改造，可独立先做。
2. **TCP relay 迁 QUIC stream（退役 yamux）** — 大工作量，**干掉 HOL 的根治项**。
   每流独立 stream、连接级拥塞、0-RTT。这是北极星的下一刀。
   - 选型注记：可评估直接对标/借鉴**成熟的 QUIC 代理协议**（如 TUIC——仓库已出现 `src/tuic.rs`
     探索），而非全手搓（契合「优先成熟框架」原则）。grill 时先定：自研 QUIC stream 协议 vs 采用
     TUIC/类似协议。
3. **吞吐：大流控窗口 + MSS clamping / MTU** — 小到中。
   RTT ~180ms 高 BDP，现在每 socket 64KB、yamux 固定窗口 → 单流吞吐被卡死。QUIC 迁移自带自适应流控，
   迁过去顺带解决；MSS/MTU 钳制防大包卡死（TODO 已列）。
4. **数据面性能**：主循环**批量收包 + 批量 flush**（现在一轮一个包、逐包 async flush），进阶 GSO/GRO 或多线程。
5. **UDP 加固**：超限 datagram **stream-fallback**（>~1400B 现在丢弃）、服务端会话表 socket 池化 / 端口耗尽。
6. **DoH/DoT 拦截**：浏览器/系统自带加密 DNS 绕过 fake-IP → 失败（现在只能手动关浏览器 DoH）。
7. **规模 & 可靠**：多 upstream / failover + 控制面（健康列表、服务发现、优雅 drain）。
   外部存储（Redis/etcd/Postgres）属于这一层的**控制面**，**绝不进每包数据面热路径**（见记忆）。

### 非代码 / 基建（常是「卡不卡」的真瓶颈）

- **出口 IP 质量**：数据中心 IP 会被 FB/YT 限速/验证码 → 要「丝滑」常需**住宅/干净 IP**。
- **VPS 带宽**：直接决定 YouTube 清晰度（现 2核2G，看带宽套餐）。
- **出口位置 / RTT**：游戏延迟靠物理 RTT → 低延迟需**更近节点（香港/日本/新加坡）**。
- **抗封 / 混淆**：现在的「fake HTTP header」只是玩具级伪装；GFW 限速/探测会让隧道本身卡 → 长期或需真混淆
  （TLS-in-TLS / REALITY 等）。

### 建议落地顺序

| 优先 | 项 | 性质 | 收益 |
|---|---|---|---|
| 1 | 规则分流（geosite/geoip） | 代码·中 | 国内直连，体感立刻顺 |
| 2 | TCP→QUIC stream（评估 TUIC） | 代码·大 | 干掉 HOL，网页/视频不卡 |
| 3 | 大窗口 + MSS/MTU | 代码·小-中 | 高 RTT 下吞吐上得去 |
| 4 | 换干净 IP / 够带宽 / 近节点 | 基建 | FB/YT 不被限速、游戏低延迟 |
| 5 | 主循环批量 + UDP 加固 + DoH + failover | 代码·分批 | 高负载稳、覆盖更全 |

> 一句话：**「丝滑刷 FB/YT」≈ 第 2+3 项代码 + 第 4 项基建；「丝滑打游戏」主要靠第 4 项近节点**
> （RTT 是物理，代码无能为力）。

---

## 5. 当前已知限制（来自 Stage 12，细节见各文档）

- UDP 超限 datagram（>warm 后 ~1400B）丢弃，无 stream-fallback（QUIC-inside 靠内层 PMTUD 自愈）。
- 服务端 UDP 会话表朴素「每流一 socket」，无端口耗尽/池化抗压。
- 全量 fake-IP（无分流）→ 国内流量也走美国。
- DoH/DoT、硬编码 IP 的 DNS 拦不到。
- TCP 仍在 yamux（HOL）；单 upstream、无 failover。

---

## 6. 工作约定（务必沿用）

- **节奏**：grill 设计树（`grill-with-docs`）→ spec → plan → **TDD 逐 task red→green→commit** → 每 stage 跑
  `/code-review` → 跨机验收 → 签 acceptance（写 teaching note + LEARNINGS）→ 开 PR。
- **原则**（见 memory）：**系统稳定 > 代码漂亮**；**优先成熟框架 + 外部存储按层落位**（数据面热路径坚决内存、
  无外部依赖；外部 DB 只进控制面/平台层）。
- **跨机测试**：让用户测时**必列分支名 + commit + 两台机（client/server）的 checkout/build 命令**；两端
  `git log -1` 必须一致才可信。UDP echo 用**单 socket Python**（`recvfrom`/`sendto`），别用 `ncat -k -u -e`。
- **通知**：需要用户手动操作时邮件通知（Resend：from `mini-vpn@zkwcloud.com` → to `870941563@qq.com`）。
- **worktree 坑**：本仓库有多个 git worktree；Stage 12 的工作在分支 `claude/recursing-mclean-645d44`。
  跑命令前确认 `pwd`/分支，别在错的 worktree 里 build/测（详见 LEARNINGS）。

---

## 7. 关键文档指针（读这些就够上手）

- `docs/adr/0003-unify-data-plane-on-quic.md` — **北极星决策**：数据平面统一到 QUIC，分阶段。
- `docs/tech/12-udp-over-quic-datagram.md` — Stage 12 设计 + 验收结果 + MTU/datagram 行为。
- `docs/tech/2026-06-05-stage-12-*-spec.md` / `*-plan.md` — Stage 12 spec 与 TDD plan（模板可复用）。
- `TODO.md` — 「Future architecture topics → Unify the data plane on QUIC」即本路线图的权威待办。
- `.learnings/LEARNINGS.md` — 踩坑记录（尤其 2026-06-08：quinn idle/keepalive/MTU 默认值、逐包日志拖垮主
  循环、DNS buffer、echo 软件红鲱鱼）。
- `CONTEXT.md` — 术语表（Upstream / Target / fake-IP / UDP flow / flow-id）。
