# 刀4 — 连接成功率(加密 DNS 拦截)plan / TDD 分解

> 配套 spec：`2026-06-18-knife4-connect-success-spec.md`。分支 `claude/knife4-connect-success`(从 main 起)。
> 每个 Task：写失败测试 → red → 实现 → green → `cargo test` + `clippy` 绿 → commit → **`git push`**。
> 一个分支一个 writer。纯逻辑先行(T1–T2),再接线(T3),harness(T4),收尾(T9)。

## 决策溯源(grill 2026-06-18)

| Q | 决策 | 依据 |
|---|---|---|
| 范围 | 主刃=拦截加密 DNS(DoT/DoH/DoQ/DoH3);拦全:53 defer;first-SYN 仅探针 | 模型 a 下明文已覆盖;DoH 是浏览器主漏口 |
| 动作 | TCP=RST(复用 rearm)、UDP=丢包,逼回落明文 | 回落快、与 Refuse 同构 |
| 识别 | :853 端口判;:443 按 DoH 域名∨DoH-IP 名单(不碰普通 :443);SNI defer | 不误伤普通 HTTPS/QUIC |
| 落点 | `resolve_target` 加 `Block`,TCP+UDP 共享一处决策 | resolve_target 两路径共享 |
| first-SYN | acceptance 探针(curl rc=7),复现才修 | 静态分析已被 knife2 修,疑陈旧 |

## 执行顺序与依赖

```
T0(spec/plan 落库 + CONTEXT「加密 DNS」术语)
 └─ T1 加密 DNS 识别纯函数 ──┐
                            ├─→ T3 两路径接 Block ─→ T4 harness ─→ T9 收尾(含 ADR-0006 + acceptance)
    T2 resolve_target Block ─┘
```
T1 纯逻辑先行;T2 依赖 T1(resolve_target 调识别函数);T3 依赖 T2;T4 依赖 T3;T9 最后。

## Task 0 — spec/plan 落库 ✅(本 commit)

`docs(knife4): spec + plan for connection-success (encrypted-DNS block)`。含 CONTEXT.md「加密 DNS」术语。

## Task 1 — 加密 DNS 识别(纯,TDD)

- **red**:测——
  - `is_encrypted_dns_port`:`853→true`、`443/53/80→false`。
  - `is_doh_domain`:`dns.google→true`、`DNS.GOOGLE→true`(大小写)、`cloudflare-dns.com→true`、子域 `x.cloudflare-dns.com→true`(若采子域匹配)、`example.com→false`、`mygoogle.com→false`(防误配后缀)。
  - `is_doh_ip`:`1.1.1.1/8.8.8.8/9.9.9.9→true`、`93.184.216.34→false`。
- **green**:三个纯函数 + 内置默认名单常量(见 spec C1)。子域匹配用「精确 ∨ `.`+后缀」避免 `mygoogle.com` 误中。
- commit：`feat(knife4): encrypted-DNS endpoint detection (T1)`。

## Task 2 — `resolve_target` 扩 `Block`(纯/半纯,TDD)

- **red**:扩 `TargetResolve::Block`;测——
  - 港 853(任意 IP)→ `Block`。
  - :443 + fake-IP 映射到 DoH 域名(`dns.google`)→ `Block`;映射到普通域名(`example.com`)→ `Direct{DomainPort}`。
  - :443 + 非 fake 的 DoH-IP(`1.1.1.1`)→ `Block`;非 fake 普通 IP → `Direct{IpPort}`。
  - :443 + fake-IP 无映射 → `Refuse`(现有,不变)。
  - 普通端口(:80/:443 非 DoH)→ 维持现有 `Direct`/`Refuse`(零回归)。
- **green**:`resolve_target` 先跑 block 决策(调 T1),再走现有逻辑。
- commit：`feat(knife4): TargetResolve::Block for encrypted DNS (T2)`。

## Task 3 — 两路径接 `Block`(I/O,harness/acceptance 验证)

- TCP(`process_listener_activity`):`Block => { 限频 log + rearm_socket(RST) + dns_blocks++ }`。
- UDP(`handle_tuic_udp_uplink`):`Block => { 限频 log + return(丢包) + dns_blocks++ }`。
- `dns_blocks` 计数器(`MetricsSink` 扩或事件循环局部 + 周期日志);log 限频防洪水。
- 测：计数器/限频可抽小纯函数测;I/O 路径归 harness(T4)+ acceptance。clippy 绿。
- commit：`feat(knife4): wire Block to RST(TCP)/drop(UDP) + blocks counter (T3)`。

## Task 4 — harness Block 端到端(TDD,若成本可控)

- harness 注入到「DoH 域名 fake-IP」的 TCP 连接 → 断言 socket 被 rearm(RST)、无 relay 到上游;普通域名 → 正常 relay(零回归)。
- 若 harness 接入 Block 场景成本过高(需造 fake-IP→DoH 域名映射 + 注入 TCP),降级为「T2 纯单测 + acceptance」覆盖,本 task 记边界。
- commit：`test(knife4): harness encrypted-DNS block path (T4)`。

## Task 9 — 收尾

- `/code-review` over diff → 修。
- 真出口 acceptance(需用户 `MINI_VPN_TUIC_*` env;复用 `scripts/knife35-acceptance.sh soak`)：
  - **DoH 拦截**:浏览器开安全 DNS(Chrome/Firefox/Safari)→ 经隧道仍能上网(DoH 被 block→回落明文→fake-IP);client 日志 `dns_blocks` 增长、命中端点。
  - **first-SYN 探针**:plan T-probe 脚本,`rc=7≈0` → 确认陈旧。
  - 续写 knife1 findings 末节(刀4 结果)。
- 按 acceptance 校准 DoH 名单后补 `docs/adr/0006-block-encrypted-dns.md`。
- 更新 HANDOFF(刀4 完成、刀4 后入口)。

## T-probe — first-SYN-refused 复现探针(acceptance 用)

前提：`sudo -E bash scripts/knife35-acceptance.sh soak`(全局隧道)。脚本见 findings 配方:
高并发(`xargs -P 26`)× 多轮连发 ~26 个不同真实域名,统计 `curl rc=7`(connect refused)。
判据：`rc=7≈0` → first-SYN 已被 knife2 修、本刀不碰;非零集中 → 仍复现,贴 `rc=7` 域名 + client 日志深挖。
