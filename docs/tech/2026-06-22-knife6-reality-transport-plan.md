# 刀6 — VLESS+REALITY 第二传输 plan / TDD 分解

> 配套 spec：`2026-06-22-knife6-reality-transport-spec.md`、ADR `docs/adr/0008-vless-reality-second-transport.md`。
> 分支 `claude/knife6-reality-transport`(从 main 起)。每 Task：写失败测试 → red → 实现 → green →
> `cargo test` + `cargo clippy --all-targets --features harness` 绿 → commit → **`git push`**。一个分支一个 writer。
> **本刀 100% 离线**(sans-IO，零网络)；真握手/互通 acceptance 归刀8。

## 决策溯源(grill 2026-06-22,详见 ADR-0008)

| Q | 决策 | 依据 |
|---|---|---|
| TLS 层 | 手写 TLS 1.3(shoes 蓝本)，原语用 RustCrypto | boring 写不了 session_id(需 patch C)；craftls 只给指纹 |
| scope | 本刀=REALITY auth + ClientHello(sans-IO 离线)；握手/VLESS/acceptance→刀7/8；failover→刀9 | 手写 TLS 1.3 是多刀工程 |
| 隐身 | Chrome-like best-effort(GREASE+X25519+ALPN+扩展序) | 指纹靠手写 ClientHello 控制 |
| Vision | 跳过(空 flow) | 独立大工程 |
| UDP | force-reality TCP-only(归刀8/9) | 分离上游属 failover |

## 执行顺序与依赖

```
T0(spec/plan/ADR-0008 落库 + CONTEXT 已更新)
 └─ T1 crypto deps + auth 原语(ECDH + derive_auth_key + session_id 布局) ──┐
     └─ T2 TLS 1.3 ClientHello builder(Chrome-like, sans seal) ───────────┤
         └─ T3 seal_session_id + build↔seal 集成 + server 视角解密 round-trip ┤
             └─ T4 verify_server_cert(HMAC-SHA512 纯函数) ─────────────────┘
                 └─ T9 收尾(/code-review;无 acceptance,归刀8)
```

## Task 0 — spec/plan/ADR-0008 落库 ✅(本 commit)

`docs(knife6): spec + plan + ADR-0008 for VLESS+REALITY transport (hand-rolled TLS 1.3) (T0)`。
含 CONTEXT.md 已加 Transport/VLESS/REALITY 术语 + 修正 Upstream/Relationships 残留 yamux。

## Task 1 — crypto deps + REALITY auth 原语(纯,TDD)

- Cargo.toml 加：`x25519-dalek`、`hkdf`、`sha2`、`aes-gcm`、`hmac`、`rand`(成熟 RustCrypto 原语)。
- 新 `src/reality/mod.rs` + `src/reality/auth.rs`。lib.rs 挂 `mod reality`。
- **red**：
  - `derive_auth_key(shared_secret, client_random, info="REALITY")`：固定输入 → 固定 32B 输出(向量，HKDF-SHA256 salt=random[0..20])。
  - `SessionIdPlaintext { version, timestamp, short_id }` build/parse round-trip：`[0..4]`版本、`[4..8]`u32 BE 时间戳、`[8..32]`short_id 零填充。
  - `short_id` hex 解析：空→全零、`"01ab"`→`[01,ab,0,..]`、>16 hex 字符→Err。
- **green**：x25519 ECDH 封装 + `derive_auth_key`(hkdf crate) + session_id 布局编解码 + short_id 解析。
- commit：`feat(knife6): REALITY auth key derivation + session_id layout (T1)`。

## Task 2 — TLS 1.3 ClientHello builder(纯,TDD)

- 新 `src/reality/client_hello.rs`。dev-dep `tls-parser` 验证结构。
- **red**(用 `tls-parser` 解析自构造 ClientHello)：
  - 解析成功、是 TLS 1.3 ClientHello；`session_id` 字段长 32(此刻可为占位/全零)、wire 偏移 39。
  - `key_share` 扩展含 **X25519**(group 0x001D)且 pubkey 32B(sing-box #2084 硬要求)。
  - `server_name` = 传入借用站；`supported_versions` 仅 1.3；ALPN=`h2,http/1.1`；cipher 含 `TLS_AES_128_GCM_SHA256`(0x1301)。
  - GREASE 占位存在(cipher/扩展)。
- **green**：手写 ClientHello 字节编码器(legacy_version/random/session_id/ciphers/扩展，Chrome-like 序)；入参 = 借用站 SNI + 我方 X25519 pubkey + random + (占位)session_id。
- commit：`feat(knife6): hand-rolled TLS1.3 ClientHello (Chrome-like, X25519 keyshare) (T2)`。

## Task 3 — seal_session_id + build↔seal 集成(纯,TDD)

- **red**：
  - `seal_session_id(auth_key, client_hello_bytes_with_zeroed_sid, random)` → 16B 密文+tag 写入 session_id 段。
  - **server 视角 round-trip**(核心)：用同一 shared_secret 派生 AuthKey → AES-128-GCM 解密(nonce=random[20..32]，AAD=session_id 清零的 ClientHello) → 还原明文 → short_id+时间戳与原值一致。**证明 AAD 用的是清零 ClientHello、偏移/nonce 正确**。
  - `build_authed_client_hello(...)` 高层：建 session_id 清零的 CH → seal over 完整 CH → 回写密文 → 返回上线字节；解析后 session_id @39 是密文(非全零)。
- **green**：aes-gcm seal；高层 `build_authed_client_hello` 串 C2 build + seal 回写。
- commit：`feat(knife6): seal REALITY session_id into ClientHello (AEAD over transcript) (T3)`。

## Task 4 — verify_server_cert(纯,TDD)

- **red**：`verify_server_cert(auth_key, ed25519_pubkey, signature)` = `HMAC-SHA512(auth_key, pubkey) == signature` → 命中/失配/长度异常不 panic。
- **green**：hmac+sha2 实现。(证书 DER 解析 → 提取 ed25519 pubkey 归刀8 实握手时；本刀只做 HMAC 判定纯函数。)
- commit：`feat(knife6): REALITY temp-cert HMAC verification (T4)`。

## Task 9 — 收尾

- `cargo test`(离线全绿)+ `clippy --all-targets --features harness` 0 warning + release build 绿。
- `/code-review`(high effort) over diff → 修。
- **本刀无真出口 acceptance**(无活握手)——归刀8(届时需服务端 VLESS+REALITY inbound)。
- 更新 HANDOFF：刀6 完成(REALITY 传输 mini-project 第一片)、刀7 入口、正交线 A 进度。
- ADR-0008 已 T0 落库。

## 刀7+ 预告(非本刀)

- **刀7**：ServerHello 解析 + TLS 1.3 key schedule(HKDF-Expand-Label，RFC 8448 向量离线测)+ record-layer AEAD(seal/open)。
- **刀8**：server-flight 解密 + REALITY HMAC 证书校验接线 + client Finished + 实 TCP 握手 + VLESS 帧 + `RealityUpstream`(impl ProxyUpstream open_tcp) + env 选择器 + 真出口 acceptance(sing-box VLESS+REALITY inbound,空 flow)。
- **刀9**：auto-failover(健康感知 TUIC↔REALITY；分离 TCP/UDP 上游；UDP 留 QUIC)。
