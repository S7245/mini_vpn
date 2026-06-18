# 刀4 — 连接成功率(加密 DNS 拦截)spec

> 配套：plan(同目录 `2026-06-18-knife4-connect-success-plan.md`)、findings(复用并续写
> `2026-06-12-knife1-bottleneck-findings.md` 末节)。分支 `claude/knife4-connect-success`(从 main 起)。
> 对症「真实场景能连上」:浏览器/系统用**加密 DNS**(DoH/DoT/DoQ/DoH3)拿到真实 IP → 绕过 fake-IP →
> 真实 IP 没进隧道 → 直连被 GFW 墙 → **连接失败**。北极星:让应用回落明文 DNS → 伪造 fake-IP → 进隧道。

## TL;DR

| 项 | 缺口 | 本刀做法 |
|---|---|---|
| **加密 DNS 绕过 fake-IP** | DoH(:443/TCP)、DoT(:853/TCP)、DoQ(:853/UDP)、DoH3(:443/QUIC) 拿真实 IP,fake-IP 形同虚设(连接失败) | `resolve_target` 加 **`Block`** 决策:命中加密 DNS 端点 → TCP 发 RST(复用 `rearm_socket`)、UDP 丢包 → 逼应用回落明文 :53 → 我方伪造 fake-IP |
| **识别(不误伤普通 HTTPS/QUIC)** | :443 与正常 HTTPS/HTTP3 同端口 | 分层:`:853`→端口判定;`:443`→**解析域名∈DoH域名名单** ∨ **dst IP∈DoH-IP名单**(精确,不碰普通 :443) |
| **first-SYN-to-fresh-fake-IP refused** | HANDOFF 列为未解(疑陈旧) | 静态分析已被 knife2 同帧 `ensure_port`+`ensure_spare_listeners` 修;**acceptance 探针验证,复现才修**(不预先动 SYN 热路径) |
| **拦全 :53** | 仅拦 198.18.0.1:53,其它 resolver 漏 | **defer 后续刀**(需裸包 DNS 路径,中等重构;模型 a 下系统 DNS=198.18.0.1 已覆盖明文) |

## 现状(代码事实,已查证)

- **DNS 伪造**:`classify_inbound`([client_tun.rs:1145](../../src/client_tun.rs))仅 `dst_ip==198.18.0.1 && dst_port==53` 走 `Dns`(本地 fake-A);其它 :53 → `UdpRelay`(隧道转发到真 DNS → 真实 IP)。
- **加密 DNS 零处理**:DoH/DoT/DoQ/DoH3 当普通 TCP/UDP 流量;经它们解析的真实 IP 若未进隧道则直连失败。
- **`resolve_target`**([client_tun.rs:807](../../src/client_tun.rs))= 每条 TCP 首包(`process_listener_activity`:947)与每个 UDP 包(`handle_tuic_udp_uplink`:1175)**共享**的决策点;现有 `Direct`/`Refuse`,`Refuse`→`rearm_socket`(`abort()` 发 RST + 释放 refcount + 重挂 listen)。
- **first-SYN 路径**:clean SYN 在 `rx_peek` 同帧 `ensure_port`(新端口建池)+`ensure_spare_listeners`(弹性补空闲 Listen 槽),`receive()`(单包 `rx_buffer.take()`)同帧把该 SYN 交刚建的 listener accept → **同帧 accept 成立**。现存 `Refuse` 仅:fake-IP 无映射(重启/旧缓存,故意)、端口撞 64 上限(罕见)——均非"新 fake-IP 首 SYN"。

## 已查证背景(决定方案)

- **fake-IP 派(Clash/Surge)标配拦加密 DNS**:不拦则漏真实 IP,与本项目同病。
- **加密 DNS 四条腿**:DoH=TCP:443、DoT=TCP:853、DoQ=UDP:853、DoH3=QUIC(UDP:443)。`resolve_target` 共享 TCP+UDP → 一处决策天然覆盖四者。
- **普通 HTTP/3 网页 + 本项目视频也是 UDP:443** → :443 **只能按 DoH 域名/IP 名单精确 block,绝不整端口封**。

## 设计决策(grill 对齐,2026-06-18)

- **D1 范围**:主刃=拦截加密 DNS(DoT/DoH/DoQ/DoH3)逼回落明文。拦全:53 **defer**(裸包 DNS 重构,模型 a 下明文已覆盖);first-SYN **仅探针验证**(疑已被 knife2 修)。
- **D2 动作**:命中 → **TCP 发 RST**(复用 `rearm_socket`,回落快、与 `Refuse` 同构)、**UDP 丢包**(连接无关,丢弃该 datagram)。逼应用回落明文 :53。**不重定向到自建 DoH**(要终结 TLS、太重)。
- **D3 识别(分层,不误伤普通 :443)**:`:853`→端口判定(DoT/DoQ);`:443`→`resolve_target` 已查回的**域名∈DoH域名名单** ∨ **dst IP∈DoH-IP名单**(复用现成 fake-IP→域名映射,零额外解析);**SNI 解析 defer**(最重,实测漏才上)。
- **D4 落点**:`resolve_target` 扩 **`Block`** 变体,TCP/UDP 两路径各自处理(TCP→rearm RST、UDP→drop)。**内置默认名单**(常量),**可配置 defer**(YAGNI)。
- **D5 first-SYN**:acceptance 探针(高并发连发新域名,统计 `curl rc=7`)。`rc=7≈0`→确认陈旧、本刀不碰;复现才回头查。

## 组件设计

### C1 加密 DNS 识别(纯函数,`client_tun.rs` 或新 `dns_block` 模块)
- `is_encrypted_dns_port(port: u16) -> bool`:`port == 853`(DoT/DoQ)。
- `is_doh_domain(domain: &str) -> bool`:大小写不敏感,精确或子域匹配内置 DoH 域名名单。
- `is_doh_ip(ip: Ipv4Addr) -> bool`:内置 DoH-IP 名单成员判定。
- 内置默认:
  - DoH 域名:`dns.google`、`cloudflare-dns.com`、`mozilla.cloudflare-dns.com`、`chrome.cloudflare-dns.com`、`dns.quad9.net`、`dns11.quad9.net`、`doh.opendns.com`、`dns.adguard-dns.com`、`doh.cleanbrowsing.org` 等。
  - DoH-IP:`1.1.1.1`、`1.0.0.1`、`8.8.8.8`、`8.8.4.4`、`9.9.9.9`、`149.112.112.112`、`208.67.222.222`、`208.67.220.220` 等。

### C2 `resolve_target` 扩 `Block`(`client_tun.rs`)
- `enum TargetResolve { Direct{target,fake_ip}, Refuse, Block }`。
- 决策序(先 block 再常规):
  - `is_encrypted_dns_port(endpoint.port)` → `Block`(DoT:853 / DoQ:853)。
  - `endpoint.port == 443`:fake-IP → 查回域名 → `is_doh_domain` → `Block`;非 fake → `is_doh_ip(ip)` → `Block`。
  - 否则维持现有 `Direct`/`Refuse`(零回归)。
- 纯/半纯:`resolve_target` 仍只依赖 `endpoint` + `fake_pool`,可单测(构造 FakeIpPool 注入 DoH 域名映射 / 普通域名 / 非 fake IP)。

### C3 两路径接 `Block`(`client_tun.rs`)
- **TCP**(`process_listener_activity`:947):`TargetResolve::Block => { log(首次/限频) + rearm_socket(RST) + 计数 }`(与 `Refuse` 分支同构)。
- **UDP**(`handle_tuic_udp_uplink`:1175):`TargetResolve::Block => { log + return(丢包) + 计数 }`。
- 计数:`dns_blocks` 计数器(可观测,放 `MetricsSink` 或事件循环局部 + 周期日志);避免每包日志洪水(限频)。

### C4 first-SYN acceptance 探针(无代码,acceptance 脚本)
- 高并发(`xargs -P`)连发 N 个不同真实域名,统计 `curl rc=7`(connect refused);对照 client 日志 `无映射/拒绝/cap reached`。详见 plan T-probe / findings 配方。

## 测试边界(诚实分层)

- **纯单元(TDD red→green)**:`is_encrypted_dns_port`/`is_doh_domain`/`is_doh_ip`(全名单 + 大小写 + 子域 + 非命中);`resolve_target` 的 `Block` 判定(port 853 / :443+DoH域名 / :443+DoH IP / 普通 :443 不 block / 普通域名不 block)。本刀逻辑主战场。
- **harness(真主循环,如成本可控)**:注入到 DoH-域名 fake-IP 的 TCP 连接 → 断言 socket 被 rearm(RST)、无 relay;普通域名 → 正常 relay(零回归)。
- **真出口 acceptance**:① 浏览器开 DoH(Chrome `chrome://settings/security` 安全 DNS / Firefox / Safari)经隧道仍能上网(DoH 被 block → 回落明文 → fake-IP → 隧道);② client 日志见 `dns_blocks` 增长、命中域名/IP;③ first-SYN 探针 `rc=7≈0`。

## 风险 / 已知边界

- **DoH 名单不全**:硬编非名单内的 DoH 端点会漏 → 回落不触发。缓解:SNI 解析(defer);名单可后续补/配置化。acceptance 用真浏览器验证主流覆盖。
- **误伤**:理论上某正常服务恰好用名单内 IP/域名的 :443 会被误 block。名单取**公认纯 DoH 端点**(`dns.google` 等专用名),误伤面极小;:853 是 DNS 专用端口,封之无误伤。
- **DoH3/QUIC :443 按 IP block**:对硬编 DoH IP 的 QUIC,丢 UDP:443 到该 IP → 普通 :443 QUIC(非该 IP)不受影响。
- **ADR**:「阻断加密 DNS 以保 fake-IP 路由」满足 hard-to-reverse + surprising(未来读者会问"为何 RST 1.1.1.1:443")+ 真权衡(牺牲用户的加密 DNS 偏好换连通性)→ 补 `docs/adr/0006-block-encrypted-dns.md`(T9,acceptance 校准名单后定稿)。
