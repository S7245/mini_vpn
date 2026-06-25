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

- **post-handshake TLS1.3 KeyUpdate（RFC 8446 §4.6.3）→ ✅ 已实现（刀10/F5，2026-06-25）。** 原 loud-fail
  （`io::ErrorKind::Unsupported`）是刀8 的占位止血。刀10 把它换成正确轮换：`HandshakeOutput` 透出
  `{s,c}_ap_secret`（application_traffic_secret_0）→ `RealityStream` 持两 secret；`decode_one` 内层 `0x16` 按
  message 逐条切分，对 KeyUpdate(`0x18`) 调 `on_key_update`：步骤 A 总轮接收方向（`secret_{N+1}=ExpandLabel(secret_N,
  "traffic upd","",32)` → 新 recv key/iv，seq 归 0）；`update_requested(1)` 时 B1 用旧 send key 封回发
  `KeyUpdate(update_not_requested=0)` 入 write_pending（铁律 B1 先于 B2）→ B2 轮发送方向；`update_not_requested(0)`
  只轮接收；非法 request_update/帧长前置校验 Err 零 mutation。NewSessionTicket(`0x04`) 等仍照常丢弃。规范见
  `docs/tech/2026-06-25-knife10-keyupdate-spec.md`（照 brief §6 V1 字节级核验）。测试 T16（KAT）/T17（时序铁律
  crypto-evidence）/T18（recv-only+非法值）/T19（端到端 loopback）/T20a（coalesced record）。acceptance：服务端主动
  KeyUpdate 不可由客户端诱发、生产服务端极少发，故以 T19 loopback（真 read/write 路径 + 真 KeyUpdate + 双向轮换）为
  高保真替身；真出口 server-initiated KeyUpdate 未触发，如实记录（brief §8 T20「尽力而为」）。
- **REALITY 握手要求 server flight 必含可校验的 Certificate**（H1 守卫）：`drive` 完成握手前强制 `verify_cert`
  通过过一次 Certificate(0x0b)——否则只发 EE+Finished 的回落 decoy 会绕过 REALITY auth（server Finished MAC 仅
  ECDHE 派生、与静态 pbk 无关，诚实 server 也算得对）。这是 REALITY「证书 HMAC 是唯一 auth 锚」的强制点。

## 证书提取：x509-cert → 手解 DER（真出口实测反转 grill 裁决 a，2026-06-24）

grill 裁决 (a) 原选 `x509-cert` crate 解析临时证书（override 了 research brief 的「手解 DER」推荐）。**真出口
acceptance 推翻了它**：真 sing-box 临时证书的 `Validity` 用 **GeneralizedTime**（notAfter ≥ 2050），`x509-cert`
的严格 RFC 5280 解析报 `malformed ASN.1 DER value for GeneralizedTime` 拒掉整张证书——而我们**根本不需要
Validity**（REALITY auth 锚是证书 HMAC，与有效期/证书链无关）。

改回 **手解 DER 定点提取**（`src/reality/cert.rs`，shoes 蓝本同法）：扫 ed25519 SPKI marker
`06 03 2b 65 70 03 21 00` 取其后裸 32B 公钥；取 leaf DER **末 64B** 为签名（= 服务端 `h.Sum(cert[:len-64])`
写入处）。**不解析 Validity/issuer 等无关字段** → 不受 GeneralizedTime 严格性影响，更稳健、且去掉 `x509-cert`
+ 一串 transitive deps。回归守卫：`cert.rs` 测试含 GeneralizedTime-validity 证书 fixture。这印证了 research brief
的原始判断（「只需两个定长字段，全 X.509 解析是 overkill」）。
