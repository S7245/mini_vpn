# 刀6 — VLESS+REALITY 第二传输（hand-rolled TLS 1.3）spec

> 配套：plan(`2026-06-22-knife6-reality-transport-plan.md`)、ADR `docs/adr/0008-vless-reality-second-transport.md`。
> 分支 `claude/knife6-reality-transport`(从 main 起)。**正交线 A**：QUIC/TUIC 是 GFW 审查目标(QUIC-Initial SNI 封锁)，
> REALITY ≈ 与真实 HTTPS 不可区分，作 TUIC 的抗封锁 fallback。本刀是 REALITY 传输 mini-project 的**第一片**。

## 北极星与边界

- 目标：给 `Upstream` 加**第二 Transport** = VLESS over REALITY over TCP，与 TUIC-over-QUIC 并列(CONTEXT.md)。
- **不阻塞 Rules.md 三目标**(当前 QUIC 能连)；REALITY=TCP-only，UDP 永留 QUIC datagram。

## grill 裁决(2026-06-22,见 ADR-0008)

| # | 裁决 | 依据 |
|---|---|---|
| TLS 层 | **手写 TLS 1.3 握手(shoes 蓝本)**，rustls/aws-lc-rs/ring 仅作密码学原语+证书校验 | boring **无法**写 ClientHello `session_id`(API 无、BoringSSL 删了回调、需 patch C)；craftls 只给指纹、session_id 仍要再 patch；REALITY 本质要 escape stock TLS(Go 那边也 fork crypto/tls) |
| scope | **传输先行，failover 拆刀9**；REALITY 传输本身再分片(刀6→刀8) | 一刀一 session + 稳定优先；手写 TLS 1.3 是 2–3 刀工程 |
| 隐身 | Chrome-like 指纹 best-effort(GREASE+Chrome cipher/曲线含 X25519/ALPN/扩展序)，不卡 byte-exact | boring 出局后指纹靠手写 ClientHello 控制；研究 sing-box #2084 硬要求 key_share 含 X25519 |
| Vision | 跳过(空 flow)，服务端配空 flow 匹配 | 独立大工程；记为已知隐身限制(TLS-in-TLS 可检测) |
| UDP | force-reality = TCP-only，UDP no-op | 分离上游重构属 failover(刀9) |

## REALITY 传输 mini-project 路线(本刀=刀6)

```
刀6  REALITY auth 密码学 + TLS 1.3 ClientHello 构造(sans-IO, 100% 离线 TDD)  ← 本刀
 └─ 刀7  ServerHello 解析 + TLS 1.3 key schedule + record-layer AEAD(seal/open)(离线 TDD,RFC 8448 向量)
     └─ 刀8  server-flight 解密 + REALITY HMAC 证书校验 + client Finished + 实 TCP 握手 + VLESS 帧
              + RealityUpstream(impl ProxyUpstream open_tcp) + env 选择器 + 真出口 acceptance(需服务端 VLESS+REALITY inbound)
              └─ 刀9  auto-failover(健康感知 TUIC↔REALITY；分离 TCP/UDP 上游；UDP 留 QUIC)
```

## 本刀(刀6)范围 —— sans-IO，零网络

只做「能离线单测」的两块，**不碰 socket/不碰主循环**：

### C1 REALITY auth 密码学(`src/reality/auth.rs` 新模块)
已查证字节布局(研究 + XTLS/REALITY 源 + shoes)：
- `x25519` 临时密钥对；ECDH(client 临时私钥 × server 静态 `public_key`/pbk) → shared secret。
- `derive_auth_key`：HKDF-SHA256(IKM=shared_secret, **salt=ClientHello.random[0..20]**, info=`"REALITY"`) → 32B AuthKey。
- `session_id` 明文布局(32B)：`[0..4]`=版本`[1,8,?,?]`(我方标识,可仿 Xray) / 实测以 shoes 的 `[1,8,0,0]` 为蓝本；`[4..8]`=Unix 时间戳(u32 BE)；`[8..32]`=`short_id`(hex 解码,零填充,≤8B)。
  - ⚠️ shoes 蓝本版本字节 `[1,8,0,0]`；以**与 sing-box 服务端互通**为准(刀8 acceptance 校准)。
- `seal_session_id`：AES-128-GCM，key=AuthKey，**nonce=ClientHello.random[20..32]**(12B)，**AAD=完整 ClientHello 字节(session_id 字段先清零)**，密文 16B + tag 写回 session_id 前段。
  - 🔑 **最易错点**(impl #1 失败原因)：AAD 必须是「session_id 已清零」的序列化 ClientHello；本刀用离线测钉死。
- `verify_server_cert`(刀8 用,本刀先实现纯函数)：`HMAC-SHA512(AuthKey, server_temp_cert.ed25519_pubkey) == cert.signature` —— REALITY 不走 CA 链,靠此 HMAC 认证服务端临时证书。

### C2 TLS 1.3 ClientHello 构造(`src/reality/client_hello.rs` 新模块)
手写字节(shoes `reality_tls13_messages.rs` 蓝本)：
- 结构：legacy_version `0x0303`、random(32B)、**session_id(32B,放 seal 后的密文,wire 偏移 39)**、cipher_suites(Chrome 序:含 `TLS_AES_128_GCM_SHA256`)、compression `null`、extensions。
- 扩展(Chrome-like 序 + GREASE)：`server_name`(借用站 SNI)、`supported_versions`(仅 1.3)、`supported_groups`(含 **X25519**)、`key_share`(我方 X25519 pubkey)、`signature_algorithms`、`ALPN`(`h2,http/1.1`)、`ec_point_formats`、`session_ticket`、`psk_key_exchange_modes`、GREASE 占位。
- 产出：可直接上线的 ClientHello record 字节 + 留出 session_id 偏移给 C1 seal(seal 的 AAD 依赖完整 ClientHello → 构造与 seal 协作：先建 session_id 清零的 ClientHello → C1 seal → 回写)。

### C3 crypto 依赖(Cargo.toml,成熟 RustCrypto 原语)
`x25519-dalek`、`hkdf`+`sha2`、`aes-gcm`、`hmac`(SHA-512 via sha2)、`rand`(临时密钥/random)。证书 ed25519 pubkey 解析刀8 再加(`x509`/手解)。**这些是成熟原语**(契合 prefer-mature)——手写的只是 TLS 组装。

## 测试边界(本刀 100% 离线 TDD)

- `derive_auth_key`:固定 shared_secret/random → 固定 AuthKey(向量,与 shoes/手算对齐)。
- `session_id` 布局:版本/时间戳/short_id 编解码 round-trip;short_id hex 解析(空/≤8B/超长拒绝)。
- `seal_session_id`:**「server 视角」解密 round-trip** —— 用同一 shared_secret 派生 AuthKey、按 nonce/AAD 解密密文 → 还原明文 short_id+时间戳(证明 seal 正确、AAD 用清零 ClientHello)。
- ClientHello:用 `tls-parser`(dev-dep)解析自构造的 ClientHello → 断言结构合法、session_id @偏移39 长32、key_share 含 X25519、SNI=借用站、cipher 含 AES_128_GCM。
- `verify_server_cert`:已知 AuthKey+pubkey → HMAC 命中/失配。
- **测不到(归刀7/8 + acceptance)**:真握手、ServerHello、key schedule、record 解密、与 sing-box 互通。

## 风险 / 已知边界

- **手写 TLS 1.3 易错**:session_id AAD(清零)、偏移、GREASE、扩展序。缓解:离线向量测 + shoes 蓝本 + 刀8 真互通校准。
- **指纹漂移**:Chrome ClientHello 版本会变;本刀 best-effort,不追字节级。
- **服务端依赖**:刀8 acceptance 需用户在 sing-box 加 VLESS+REALITY inbound(uuid/reality keypair/short_id/handshake server,空 flow)。
- **TLS-in-TLS 可检测**(无 Vision):已知隐身限制,Vision 留后续。
- **ADR**:手写 TLS 1.3 + 否决 boring/craftls 满足 hard-to-reverse + surprising(纯 Rust 项目为何不接 TLS 库)+ 真权衡 → `docs/adr/0008-*`(T0 落库)。
