# 刀7 — REALITY 握手核心 plan / TDD 分解

> 配套 spec：`2026-06-23-knife7-reality-handshake-spec.md`、ADR `docs/adr/0009-tls13-cipher-scope-0x1301-first.md`。
> 分支 `claude/knife7-reality-handshake`(从 main 起)。每 Task：写失败测试 → red → 实现 → green →
> `cargo test` + `cargo clippy --all-targets --features harness` 绿 → commit → **`git push`**。一个分支一个 writer。
> **本刀 100% 离线**，KAT 用 **RFC 8448 §3**。真握手/socket/VLESS/acceptance 归刀8。

## 决策溯源（understand-phase research workflow + grill 2026-06-23）

| Q | 决策 | 依据 |
|---|---|---|
| cipher 范围 | 仅 KAT 0x1301 + 泛型-over-hash 骨架 + 0x1302/0x1303 gap 写 ADR-0009 | 唯一有 RFC 8448 向量;decoy 选 0x1302 风险靠 ADR/泛型缓解 |
| 刀7/刀8 边界 | 刀7=ServerHello 解析+key schedule+record AEAD(sans-IO);刀8=socket+解密 flight+证书+Finished+VLESS | research 锁定;离线可 KAT |
| app secrets | 刀7 顺带做(T7,推荐)——RFC 8448 有向量、纯函数、de-risk 刀8 | cheap KAT |

## 执行顺序与依赖

```
T0 wire 模块骨架
 └─ T1 HkdfLabel + HKDF 原语(KAT) ──┐
     └─ T2 握手 key schedule 链(KAT + 全零 ecdhe 拒) ─┐
         └─ T3 compute_finished_verify_data(KAT) ─────┤
     T4 record AEAD seal/open(round-trip) ────────────┤
         └─ T5 record open RFC 8448 server flight(golden KAT) ─┤
     T6 ServerHello 解析 + 拒绝路径(KAT + tls-parser) ─────────┤
         └─ T7(可选) app-secret 派生(KAT) ──────────────────────┤
                                                               └─ T8 收尾(ADR-0009 + /code-review;无 acceptance,归刀8)
```
T1→T2→T3 链;T4→T5 链(T5 用 T2 的 server key/iv);T6 独立;T7 接 T2 之后。T4/T6 可与 T1-T3 并行推进。

## Task 0 — wire 模块骨架 + spec/plan/ADR 占位

`pub mod {server_hello,key_schedule,record}` 入 mod.rs;建三文件 + auth.rs 式 `hex()/arr32()` 测试 helper(可提到一处共用)。无逻辑,cargo build/test 绿。spec/plan 落库。
commit：`docs+chore(knife7): spec + plan + module skeleton (T0)`。

## Task 1 — HkdfLabel + HKDF 原语（TDD，KAT）

- **red**：`hkdf_label(16,"key",b"")==00100974…6b657900`、`(12,"iv",..)`、`(32,"finished",..)`；`Sha256("")==e3b0c4…`；**端到端 `derive_secret(early,"derived",sha256(""))==6f2615a1…`**。
- **green**：`hkdf_label`(`tls13 ` 含尾空格、u8 前缀、顶层 u16)/`expand_label`/`derive_secret`/`extract`/`transcript_hash`。
- commit：`feat(knife7): TLS1.3 HKDF-Expand-Label + Derive-Secret (RFC 8448 KAT) (T1)`。

## Task 2 — 握手 key schedule 链（TDD，KAT）

- **red**：喂 ecdhe `8bd4054f…`+CH(196B)+SH(90B) → 断言 early `33ad0a1c…`/derived `6f2615a1…`/handshake `1dc826e9…`/c_hs `b3eddb12…`/s_hs `b67b7d69…`、transcript_hash `860c06ed…`、server key/iv `3fce…03bc`/`5d31…0b30`、client key/iv `dbfa…8d01`/`5bd3…265f`；**全零 ecdhe → Err**。
- **green**：`derive_handshake_keys`(Early→derived→Handshake→{c,s}_hs→key/iv) + 全零拒（Extract 前）。`HsKeys` 结构。
- commit：`feat(knife7): handshake key schedule chain + zero-ecdhe reject (RFC 8448 KAT) (T2)`。

## Task 3 — compute_finished_verify_data（TDD，KAT）

- **red**：finished_key（s_hs `008d3b66…`、c_hs `b80ad010…`）。
- **green**：finished_key=Expand-Label(.,"finished","",32) → HMAC-SHA256 over transcript。
- commit：`feat(knife7): TLS1.3 Finished verify_data primitive (RFC 8448 KAT) (T3)`。

## Task 4 — record AEAD seal/open（TDD）

- **red**：`per_record_nonce(iv,0)==iv`；seal→open round-trip 还原 (content_type,content)；篡改 AAD/密文→Err；全零明文→Err；seq 递增。
- **green**：`per_record_nonce`/`record_header`/`RecordKeys::seal/open`（inner type+zero-pad、剥尾零、checked seq++）。
- commit：`feat(knife7): TLS1.3 record-layer AES-128-GCM seal/open (T4)`。

## Task 5 — record open RFC 8448 server flight（golden KAT）

- **red**：硬编 RFC 8448 §3 server 加密握手 record（从 `/tmp/rfc8448.txt` 抄、写时重抓确认）；用 server key/iv `3fce…03bc`/`5d31…0b30` @read-seq 0 → 断言解密成功、content_type==0x16、内层起始 EncryptedExtensions(0x08)。
- **green**：（T2+T4 已实现，本 task 主要是 KAT 接线）。**schedule+AEAD 字节级一致的最强离线证明。**
- commit：`test(knife7): RFC 8448 server-flight record open golden KAT (T5)`。

## Task 6 — ServerHello 解析 + 拒绝路径（TDD，KAT）

- **red**：解析 90B RFC 8448 SH → cipher==0x1301/key_share==`c982…1f0f`/version==0x0304；HRR-sentinel→Err、downgrade-sentinel→Err、compression!=0→Err、version!=0x0304→Err、echo mismatch→Err；tls-parser 交叉验证。
- **green**：`parse_server_hello` + 各 extract/检查。注释写明 **echo≠auth**（auth 决策在刀8 证书 HMAC）。
- commit：`feat(knife7): ServerHello parser + reject paths (RFC 8448 KAT + tls-parser) (T6)`。

## Task 7（可选，推荐）— application-secret 派生（TDD，KAT）

- **red**：2nd derived `43de77e0…`→Master `18df0684…`→{c,s}_ap（c_ap `9e40646c…`/s_ap `a11af9f0…`），transcript CH..serverFinished `96081020…`。
- **green**：扩展 key schedule 链。纯函数、de-risk 刀8。范围紧则跳过、记 deferred。
- commit：`feat(knife7): application traffic secrets derivation (RFC 8448 KAT) (T7)`。

## Task 8 — 收尾

- `/code-review`(high effort) over diff → 修。
- **无真出口 acceptance**（无活握手）——归刀8。
- 写 `docs/adr/0009-tls13-cipher-scope-0x1301-first.md`：**显式记 0x1302/0x1303 cipher gap + "echo-match≠auth" 不变量**，防刀8 回归。泛型-over-hash 设计已述（仅 0x1301 wire）。
- 更新 HANDOFF：刀7 完成、刀8 入口（实握手+证书+VLESS+acceptance）、REALITY 进度。
- 续写 knife1 findings 末节「刀7」(离线完成 + RFC 8448 KAT 全绿;无 acceptance)。

## 刀8 预告（非本刀）

socket + 握手状态机；真 ECDH(network keyshare,**用 T2 的全零拒**)+解密 server flight；跳明文 dummy CCS(不进 read-seq)；X.509 DER 提 ed25519 pubkey+sig → 刀6 `verify_server_cert`(REALITY auth 决策)；CertificateVerify ed25519 检；server-Finished MAC 验+发 client Finished；app keys(若 T7 已做则复用)；VLESS 帧(空 flow)；`RealityUpstream`(ProxyUpstream open_tcp)+env 选择器+真出口 acceptance(sing-box VLESS+REALITY inbound)。**可能需 x509 parser crate**。
