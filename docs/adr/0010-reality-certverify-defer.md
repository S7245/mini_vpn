# REALITY 客户端 defer 标准 TLS CertificateVerify 验签（仅折入 transcript），auth 锚=证书 HMAC + Finished MAC

手写 TLS 1.3 REALITY 客户端（ADR-0008）在 刀8 解密 server flight 后，对收到的 **CertificateVerify(0x0f)**
消息**不验证其 ed25519 签名**——但**必须把它的全部字节折入握手 transcript**。REALITY 的真正认证锚是
① 服务端临时证书的 **HMAC-SHA512(AuthKey, ed25519_pubkey) == cert.signature**（刀6 `verify_server_cert`，
刀8 在 Certificate 消息处做出 REALITY auth 决策）+ ② TLS 1.3 **server Finished MAC**（证明 ECDHE 握手完整、
未被篡改）。标准 PKI 链校验与 CertificateVerify 签名校验都**不做**。

刀8 grill 裁决，2026-06-23，基于 understand-phase 研究 workflow（见
`docs/tech/2026-06-23-knife8-reality-live-handshake-spec.md` §2c、`2026-06-23-knife8-research-brief.md` §1.3）。

## 为什么 defer CertificateVerify 验签

- **零安全增益**：REALITY 服务端的临时证书是**每会话自签 ed25519 证书**。CertificateVerify 只证明对端持有
  该临时证书对应的私钥——但一个中间人攻击者同样能现场自签一张证书并对 transcript 签名。因此对 REALITY 而言，
  CertificateVerify 不提供任何「这是真 server」的保证。真正不可伪造的是 **HMAC-SHA512**——它要求对端持有
  REALITY X25519 **静态私钥**（攻击者没有），HMAC 才能匹配。Finished MAC 再保证 ECDHE 与整条握手未被篡改。
- **省一个依赖 + 一条误拒路径**：验 CertificateVerify 需引入 ed25519 签名验证依赖（如 `ed25519-dalek`），且多一条
  签名解析/验证可能误拒合法 server 的路径，性价比低（系统稳定优先）。
- 这与 Go 版 REALITY 客户端（Xray/sing-box）的行为一致：它们也是用 HMAC 校验临时证书、不走 PKI 链。

## ⚠️ Footgun（实现者必须知道）：defer 验签 ≠ defer 折叠

**CertificateVerify 的全部字节仍必须折入握手 transcript**，即便不验它的签名。原因：

- **server Finished** 的 verify_data = HMAC over `Transcript-Hash(CH..CertificateVerify)`——transcript **包含**
  CertificateVerify 字节。漏折 → transcript hash 错（如 RFC 8448 §3 正确值
  `edb7725f…10ed` 会变成别的）→ server Finished MAC 不匹配 → 握手**静默失败**。
- **application traffic keys** 派生用 `Transcript-Hash(CH..serverFinished)`——同样依赖 CertificateVerify 已折入。
  漏折 → app keys 错 → VLESS 请求与所有 app data 不可解密。

「验签」与「折叠」是正交的两件事：CertificateVerify 内的签名签的是它**之前**的 transcript（`CH..Certificate`），
而我们关心的是把 CertificateVerify 本身折进**之后**消息（Finished）的 transcript。本刀 defer 前者、**绝不** defer 后者。

## Considered Options

- **defer 验签 + 折入 transcript（chosen）。** 最小正确切片：auth 由 HMAC + Finished MAC 保证，CertificateVerify
  字节照折不验签。零额外依赖、零误拒路径。
- **完整验 CertificateVerify ed25519 签名。** 与 REALITY 信任模型正交（多一道与「是否真 server」无关的检查），
  多一个依赖 + 一条可能误拒的路径，性价比低。Rejected（YAGNI；若将来有需求再加，SPKI ed25519 pubkey 已提取在手）。
- **连 Finished MAC 一起跳过。** Rejected——Finished MAC 是 ECDHE 完整性的关键保证，跳过会让握手对篡改无感。

## Consequences

- 刀8 握手驱动（`src/reality/handshake.rs`）：Certificate 消息 → 提 ed25519 pubkey + sig → `verify_server_cert`
  做 REALITY auth 决策（false → 握手 Err）；CertificateVerify 消息 → **只折 transcript，不验签**；server Finished
  → `compute_finished_verify_data` 验。
- 未来若要验 CertificateVerify：SPKI ed25519 pubkey 已由 `cert.rs::extract_ed25519_pubkey_and_sig` 提出可复用；
  需引 ed25519 verify 依赖 + 核对 SignatureScheme（ed25519 = `0x0807`，RFC 8446 §4.2.3）。
- 不影响 ADR-0008（auth 仍在 session_id + 证书 HMAC）/ ADR-0009（cipher 0x1301）。

## 刀8 收尾登记的 gap（code-review）

- **post-handshake TLS1.3 KeyUpdate（RFC 8446 §4.6.3）未实现**：`RealityStream` 不保留 application traffic
  secret，无法在收到 KeyUpdate 时把 server 发送密钥轮换到 `application_traffic_secret_N+1`。若静默丢弃该消息，
  其后每条 server record 会用错密钥 AEAD 失败、看似随机断连。故 `decode_one` 对内层 `0x16`/msg `0x18`（KeyUpdate）
  **显式 loud-fail**（`io::ErrorKind::Unsupported`），与 0x1302/0x1303 cipher gap 同纪律（显式拒绝而非静默误用）。
  NewSessionTicket（msg `0x04`）等无密钥影响的 post-handshake 消息照常丢弃。完整 KeyUpdate 轮换（traffic secret
  进 RealityStream + update-requested 时回发）留后续刀。长连接/大流量出口才可能触发；短连接 acceptance 不受影响。
- **REALITY 握手要求 server flight 必含可校验的 Certificate**（H1 守卫）：`drive` 完成握手前强制 `verify_cert`
  通过过一次 Certificate(0x0b)——否则只发 EE+Finished 的回落 decoy 会绕过 REALITY auth（server Finished MAC 仅
  ECDHE 派生、与静态 pbk 无关，诚实 server 也算得对）。这是 REALITY「证书 HMAC 是唯一 auth 锚」的强制点。
