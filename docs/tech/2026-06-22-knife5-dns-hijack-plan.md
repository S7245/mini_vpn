# 刀5 — 拦全 :53 裸包 DNS 劫持 plan / TDD 分解

> 配套 spec：`2026-06-22-knife5-dns-hijack-spec.md`、ADR `docs/adr/0007-hijack-all-plaintext-dns.md`。
> 分支 `claude/knife5-dns-hijack`(从 main 起)。每个 Task：写失败测试 → red → 实现 → green →
> `cargo test` + `cargo clippy --all-targets --features harness` 绿 → commit → **`git push`**。
> 一个分支一个 writer。纯逻辑先行(T1–T2),再端口判定(T3),最后接线+删旧(T4),收尾(T9)。

## 决策溯源(grill 2026-06-22,详见 ADR-0007)

| Q | 决策 | 依据 |
|---|---|---|
| 裸包 vs AnyIP | **裸包** | AnyIP 无法为无界 resolver IP 集合设 src;裸包复用已验收下行注入 |
| 198.18.0.1 socket | **废弃,统一裸包** | DRY、一条伪造路径;198.18.0.1 降格为广告地址 |
| 劫持范围 | **全劫持,不按 dst 过滤** | 最简最稳、无 DNS 逃逸;split-horizon 记为已知限制 |
| TCP :53 | **RST**(resolve_target `port==53→Block`,只命中 TCP) | 闭合泄漏口;复用刀4 接线、成本极低 |
| 与刀4 | 纯叠加(端口分流)、刀5 不改刀4 一行 | :53/:853/:443 端口互不干扰 |

## 执行顺序与依赖

```
T0(spec/plan/ADR-0007 落库 + CONTEXT 已更新)
 ├─ T1 forge_dns_reply 纯函数 ──────────────┐
 ├─ T2 classify 任意 :53 → Dns ─────────────┤
 └─ T3 is_dns_relay_port + resolve_target ──┤
                                            └─→ T4 接线 handle_dns_hijack + 删 smoltcp DNS 旧路 ─→ T9 收尾
```
T1/T2/T3 互相独立(纯逻辑/判定),可任意序;T4 依赖三者(接线 + 删旧);T9 最后(code-review + acceptance)。

## Task 0 — spec/plan/ADR-0007 落库 ✅(本 commit)

`docs(knife5): spec + plan + ADR-0007 for plaintext-DNS hijack (T0)`。含已更新的 CONTEXT.md
(fake-IP map「拦截任意 resolver 明文查询」、Encrypted DNS「明文回落不依赖系统 DNS」)。

## Task 1 — `forge_dns_reply` 纯函数(TDD,核心)

- **red**:在 `client_tun.rs` test mod 加测(先写、不编译/红)——
  - `8.8.8.8:53` 的 A 查询(`example.com`)→ 回包 `parse_inbound_udp` 得 `src=8.8.8.8:53`、`dst=app(原src)`、payload 是含 fake-IP 的 A 响应(`dns::parse_query` 不便复核响应,直接断言 RDATA 落 `198.18.0.0/15`)。
  - `198.18.0.1:53` 查询同样被伪造(回包 src=198.18.0.1)。
  - dst 落 fake-IP 段(如 `198.18.0.9:53`)也伪造(不调 resolve_target、不 Refuse)。
  - AAAA 查询 → 回包 ANCOUNT=0(NODATA)。
  - 不可解析 payload(截断/多 question)→ `None`。
  - 同域名两次 → 同一 fake-IP(稳定复用)。
- **green**:实现 `forge_dns_reply`(见 spec C1)= `parse_query` →(A:`alloc`+`Answer::A(ip,5)` / 否则 `NoData`)→ `build_response` → `build_udp_ip_packet` → `Some(bytes)`;不可解析 → `None`。保留 `🪪` 日志。
- commit：`feat(knife5): forge_dns_reply pure fn (raw fake-IP reply for any resolver) (T1)`。

## Task 2 — `classify_inbound` 任意 :53 → Dns(TDD)

- **red**:改 `classify_routes_dns_relay_and_other`——
  - `classify(udp_pkt([8,8,8,8],53)) == Dns`(原断言 UdpRelay,翻转)。
  - `classify(udp_pkt([1,1,1,1],53)) == Dns`、`classify(udp_pkt([198,18,0,1],53)) == Dns`。
  - `classify(udp_pkt(_,853)) == UdpRelay`、`classify(udp_pkt([198,18,0,5],443)) == UdpRelay`(不变)。
  - TCP/垃圾 → Other(不变)。
- **green**:`classify_inbound` 改为 `udp.dst_port == 53 → Dns`,删 `FAKE_DNS_RESOLVER` 依赖(暂留常量,T4 删)。
- commit：`feat(knife5): classify any :53 to Dns (hijack all plaintext DNS) (T2)`。

## Task 3 — `is_dns_relay_port` + `resolve_target` Block TCP :53(TDD)

- **red**:
  - `dns_block` test:`is_dns_relay_port(53)==true`、`is_dns_relay_port(853)==true`、`443/80/0/65535==false`。
  - `resolve_target` test(扩 `resolve_target_blocks_encrypted_dns` 或新测):`(任意IP,53) → Block`;`:853 → Block`(回归);普通 :443/:80 → Direct(零回归)。
- **green**:`dns_block::is_dns_relay_port`;`resolve_target` 用它替换 `is_encrypted_dns_port` 调用点,注释写死不变量(UDP :53 已被 classify 截走 → port==53 只命中 TCP)。
- commit：`feat(knife5): RST TCP :53 via resolve_target Block (is_dns_relay_port) (T3)`。

## Task 4 — 接线 `handle_dns_hijack` + 删 smoltcp DNS 旧路(I/O,harness/acceptance 验证)

- 新 `handle_dns_hijack`(async):`parse_inbound_udp` → `forge_dns_reply` → `Some(pkt)`→`inject_ip_packet`+`flush_tx`;`None`→丢弃。
- rx 分支加 `else if class == Some(Inbound::Dns) { rx_take → handle_dns_hijack }`(在 `UdpRelay` 与 smoltcp 路径之间)。
- **删**:`dns_handle` socket+`bind`、接口 IP `198.18.0.1/32`、`drain_dns` 函数+两处调用([:577](../../src/client_tun.rs)/[:643](../../src/client_tun.rs))、`FAKE_DNS_RESOLVER` 常量;更新 fake_ip.rs:36 `.1` 预留注释。
- 测:I/O 路径归 acceptance(+ harness smoke 若 inject/capture 成本可控,否则记边界,同刀4)。`cargo test` + clippy 全绿、release build 绿。
- commit：`feat(knife5): wire handle_dns_hijack + drop smoltcp DNS socket (T4)`。

## Task 9 — 收尾

- `/code-review` over diff(high effort)→ 修。
- **真出口 acceptance**(需用户 `MINI_VPN_TUIC_*` env;复用 `scripts/knife35-acceptance.sh soak`,见下 T-DNS 配方):
  - **核心判据**:系统 DNS **不指** 198.18.0.1(设 8.8.8.8 或留 DHCP)→ app 查 `8.8.8.8:53` 仍被本地伪造 fake-IP(client 日志 `🪪 DNS … → fake-IP`)→ **仍能上网**(浏览/curl 主流站点)。
  - **回归**:系统 DNS 设回 198.18.0.1 → 仍正常(统一裸包路径覆盖)。
  - **TCP :53**:`dig +tcp @8.8.8.8 example.com` → 被 RST/失败(或回落 UDP),不泄漏真实 IP。
  - **DoH 回归**:浏览器开/关安全 DNS → 开时 Block→回落明文→fake-IP;关时明文直伪造。
  - 续写 knife1 findings 末节(刀5 结果 + DNS 劫持配方)。
- 更新 HANDOFF(刀5 完成、刀5 后入口、已知坑「拦全:53」标 ✅)。
- ADR-0007 已在 T0 落库;acceptance 若暴露名单/边界问题再校准。

## T-DNS — 真出口 acceptance 配方(收尾用)

前提：`sudo -E bash scripts/knife35-acceptance.sh soak`(全局隧道),**且系统 DNS 改为非 198.18.0.1**
(macOS:`networksetup -setdnsservers Wi-Fi 8.8.8.8`;或留 DHCP)。
1. `dig @8.8.8.8 example.com` → 返回 `198.18.x.x`(fake-IP,非真实 IP);client 日志见 `🪪 DNS example.com → fake-IP`。
2. 浏览器/`curl https://example.com` → 经隧道正常(域名经 fake-IP→DomainPort→出口解析)。
3. `dig +tcp @8.8.8.8 example.com` → 连接被拒/超时(TCP :53 RST),不返回真实 IP。
4. 恢复:`networksetup -setdnsservers Wi-Fi empty`(或原值)。
判据:① dig 全返 fake-IP(无真实 IP 泄漏);② 仍能上网;③ TCP :53 不泄漏;④ DoH 开关回归正常。
