# 刀8 研究 Brief — VLESS over REALITY over TCP 端到端收官（understand-phase 综合）

> 来源：刀8 understand-phase 5 路并行研究（R1–R5）+ 对 20 条互通-critical 断言的对抗验证裁决（26 agents）。
> **矛盾处一律以裁决为准**：本刀涉及 4 条 `refuted`（R4-C1/C3/C4 + 嵌套子句），已用修正版替换并标注。
> 所有字节精确到 hex。本地 API 已逐一核对（`src/reality/*`、`src/tuic.rs`、`src/upstream.rs`、`src/client_tun.rs`、`Cargo.toml`、`docs/adr/0009`）。

---

## 1. 确认的互通-critical 事实（字节级）

### 1.1 record 分帧 / CCS

- **明文 ClientHello record 头 = `16 03 01`**（0x16 handshake + 0x0301 legacy_version）。RFC 8448 真线 `16 03 01 00 c4`。
  `build_authed_client_hello` 只返回纯 handshake message（`0x01`+3B len，**无 record 头**），刀8 发送时须自加 5B `16 03 01 <len_hi> <len_lo>`。
- **建立后密文 record 头 = `17 03 03`**（0x17 application_data + 0x0303 frozen version）。`record.rs::record_header()` 是 **ciphertext-only**，
  只产 `[0x17,0x03,0x03,hi,lo]`，明文 CH 不能用它。
  - ⚠️ 细节（非互通强制）：initial CH 的 `0x0301` 是 SHOULD/MAY（`0x0303` 亦合法），但 REALITY 指纹须按 Go/uTLS 发 `16 03 01`。
- **Dummy CCS = 6B `14 03 03 00 01 01`**（content_type 0x14 + version 0x0303 + len 0x0001 + payload 0x01）。
  - **收：** 读到 5B header 后，在调 `open()`/AEAD 之前检测 `content_type==0x14`，**整条丢弃，绝不 `recv_keys.open()`，绝不递增 server read seq**。
    计入 seq 会让后续每条 0x17 的 nonce=`iv XOR seq` 全错、flight AEAD 全败、握手静默死。
  - **发：** 客户端须**自己发一次** dummy CCS `14 03 03 00 01 01`，时序 = 收到 ServerHello 之后、发送加密 Finished 之前
    （RFC 8446 D.4；REALITY ClientHello 恒带 32B 非空 session_id → server 必发 CCS，client 也须发）。Chrome+uTLS 恒发，不发可被指纹识别。

### 1.2 握手时序、seq、密钥切换、跨 record 重组

- **server flight 内层消息可跨多条 `17 03 03` record 分片或合并**（RFC 8446 §5.1：MAY coalesce / fragment）。**record 边界 ≠ message 边界**。
- **重组规则：** 每条 record `open()` 出 `(inner_type, content)`；当 `inner_type==0x16` 时把 content **拼进一个独立于 record buffer 的 handshake buffer**，
  再按 **1B type + 3B uint24 len** 切出消息。绝不可假设「一 record = 一 message」（真实大证书必然跨 record）。
  `record.rs` RFC8448 KAT 仅证单 record 解密，刀8 须新增跨 record 累积。
- **内层 handshake 消息类型：** EncryptedExtensions=`0x08`、Certificate=`0x0b`、CertificateVerify=`0x0f`、Finished=`0x14`。
  握手后 NewSessionTicket=inner `0x16`/msg `0x04`、KeyUpdate=`0x18` → drop。
- **seq / 密钥切换（沿用刀7 不变量）：**
  - read：server handshake key/iv @ seq 0 → 收完 server Finished 后切 `s_ap` key/iv，**seq 归零**。
  - write：client Finished 用 `c_hs` key/iv @ seq 0（自己一条 record）→ 之后切 `c_ap`，**seq 归零**。
  - 读/写 seq 独立（`record.rs` 已分离）。
- **transcript 折叠纪律（刀7 KAT 已锁）：**
  - server Finished verify 用 transcript = `CH..SH..EE..Cert..CertVerify`，RFC 8448 真值 hash `edb7725fa7a3473b031ec8ef65a2485493900138a2b91291407d7951a06110ed`，
    verify_data `9b9b141d906337fbd2cbdce71df4deda4ab42c309572cb7fffee5454b78f0718`。**漏折 CertVerify** → MAC 不匹配 → 静默失败。
  - client Finished / app keys 用 transcript = `CH..server Finished`（server Fin 须先折进再派生 app keys）。

### 1.3 证书与 X.509 提取

- **REALITY auth 决策（客户端侧）= `HMAC-SHA512(AuthKey, ed25519_pubkey) == cert.Signature`**，**不是** session_id echo（decoy 也回显）。
  本地 `auth.rs:114 verify_server_cert(auth_key:[u8;32], ed25519_pubkey:&[u8], signature:&[u8])` 已正是此式（constant-time）。
- **签名所在字节 = 临时 cert DER 的最后 64B**，正是自签 ed25519 证书的 `signatureValue` BIT STRING 内容。
  服务端 `h.Sum(cert[:len(cert)-64])`；Go 客户端比 `certs[0].Signature`（= 那最后 64B）。
  - **互通要点：签名必取自 leaf ed25519 cert 末尾（DER 最后 64B），不取自 CertificateVerify，也不取自 SPKI 段。** 取错 → HMAC 永不匹配 → 合法 server 被当 decoy 拒。
- **ed25519 公钥在标准 SPKI 内**，OID `06 03 2b 65 70` + BIT STRING 头 `03 21 00` 之后的裸 32B。
  完整 SPKI 44B = `30 2a 30 05 06 03 2b 65 70 03 21 00 || <32B pubkey>`；裸公钥从 **SPKI offset 12** 起。**HMAC 必须只喂裸 32B**（剥掉 `03 21 00` 头），否则 mismatch。
- **Certificate(0x0b) 消息内第一张 cert DER 起于相对 offset 11** = 4B handshake 头 + 1B ctx len(0) + 3B list len + 3B cert_data len。
  RFC 8448 真线 `0b 00 01 b9 | 00 | 00 01 b5 | 00 01 b0 | 30 82...`。该消息须先用 server handshake key/iv 从 `17 03 03` record 解密。
- **CertificateVerify(0x0f) 全字节须折进 transcript，即便本刀 defer 其 ed25519 签名验证。** 「fold ≠ verify」正交；漏折 → server Finished MAC 错 + app keys 错 → 静默失败。

### 1.4 VLESS 帧（空 flow）

**请求头（空 flow，TCP）字段顺序与宽度：**
```
version(1B=0x00) | UUID(16B 裸) | addon_length(1B=0x00) | addons(0B)
  | command(1B；TCP=0x01/UDP=0x02/Mux=0x03) | port(2B 大端) | atyp(1B) | address(S B)
```
- `addon_length` 是单字节 **u8**（≤255）；空 flow 下 command 在 offset 18。
- **地址段 = PortThenAddress：port(2B BE) 在前，atyp(1B) 在后，再 host。**
- **ATYP：IPv4=`0x01`、Domain=`0x02`、IPv6=`0x03`**；Domain = `0x02` + 1B 长度前缀 + N 字节。
- **【头号双重踩坑】** 与本地 `tuic.rs::encode_address`（`ATYP_DOMAIN=0x00/IPV4=0x01/IPV6=0x02`，顺序 `[ATYP][ADDR][PORT BE]`）**顺序相反 + 三个 ATYP 数值错位**。
  **必须新写 VLESS 专用地址编码器，绝不复用 `encode_address`。**
- **UUID = 裸 16B RFC4122**（无 VMess cmdKey/HMAC 派生），可复用 `tuic.rs::parse_uuid` 风格。

**完整请求头 KAT**（UUID 全 `0x11`、TCP、1.2.3.4:443，26B）：
```
00  11111111111111111111111111111111  00  01  01bb  01  01020304
ver UUID(16B)                         alen cmd port  aty addr
```
连续：`0011111111111111111111111111111111000101bb0101020304`
域名变体（example.com:443）：`00 11×16 00 01 01bb 02 0b 6578616d706c652e636f6d`

**响应头：** `version(1B) + addon_length(1B=N) + addons(N B)`，**无 command/address/port**。客户端第一段 application_data（record 解密后明文）须**动态 strip `2+N` 字节**
（空 addons 恰 2B `[0x00, 0x00]`），**仅首读一次性剥离（boolean 门控）**。⚠️ 头部 `2+N` 可能跨多 record 到达须累积；**硬编 2 在非空 addons 会污染** → 须读 `byte[1]` 后 strip `2+byte[1]`。

**发送时机：** VLESS 请求作为握手完成后**第一条 application_data**（`c_ap` key/iv，content_type `0x17`）发出，目标=被代理 Target。

### 1.5 sing-box 服务端（R4-C1/C3/C4 经对抗验证修正）

- **【修正：原 R4-C1】** ServerHello.cipher_suite 由 dest 借用站**每连接协商**决定（REALITY server 透传 dest 真实 ServerHello、仅替证书改签名，**不自选 cipher**）。
  但 cipher 不是 dest 主机的固定属性，而是 `dest 选择策略 + AES 硬件偏好` 作用于 **`mini_vpn ClientHello offer 的套件 ∩ dest 支持套件`** 的结果。
  - 现状：`client_hello.rs:19` offer 了 `GREASE, 0x1301, 0x1302, 0x1303`。对偏好 AES-256 的 server-pref 站 → `server_hello.rs:113` loud-fail。
  - **真正失败源是「mini_vpn offer 了自己解不了的 0x1302/0x1303」**。出路：**(a) 收紧 ClientHello cipher 仅 `0x1301`**（任何合规 dest 被 RFC 8446 §9.1 强制只能回 0x1301）；**(b) 选会协商 0x1301 的 dest**。
- **【修正：原 R4-C2，取值非站点常量】** Chrome/uTLS 指纹（AES-128 在前）下实测（**本机 macOS，未在 VPS egress 复现**）：
  - **0x1301（可借）：** gateway.icloud.com / dl.google.com / www.google.com / www.cloudflare.com / swcdn.apple.com / www.yahoo.com / www.amazon.com / python.org
  - **0x1302（本刀 loud-fail）：** www.microsoft.com / www.apple.com / www.bing.com / www.nvidia.com / www.tesla.com / www.icloud.com / telegram.org
  - ⚠️ cloudflare/google/icloud/swcdn 跟随客户端顺序（256-first 会翻 0x1302）；换指纹会掉坑。借站前须从真 VPS 重跑。
- **【修正：原 R4-C3 子句】** sing-box `public_key`/`private_key` = Go `base64.RawURLEncoding`（URL-safe、无 `=`），32B → **恰 43 字符**；解码须**恰好 32B**（否则 loud error）。
  当前 Xray `xray x25519` 默认**同样**用 base64url（非 hex）。short_id 才是 hex。**mini_vpn 现无 base64 dep**，须新增。
- **【修正：原 R4-C4 + 机制更正】** SNI 门是**集合成员判定**：客户端 SNI 必须 ∈ 服务端 `serverNames` 集合。三处设同一域名是保 decoy 伪装的推荐默认，非 auth 硬约束。
  - **机制更正：服务端 REALITY auth 门 = 对 session_id 的 AES-128-GCM `aead.Open`**（key=`HKDF-SHA256(X25519(server静态私钥, client临时pub), salt=CH.random[:20], info="REALITY")`，AAD=session_id 清零的 CH）。
    **客户端的 HMAC-SHA512 是反向检查（验服务端临时证书 = 本地 `verify_server_cert`）**，与服务端回落决策无关。
  - 失败静默两条独立成因：SNI∉serverNames，或服务端 AEAD.Open 失败/short_id·version·time 不过 → server `io.Copy` 转发 decoy，**不发 alert**。
- **R4-C5 confirmed：** 验收握手成功判据须 **`verify_server_cert==true`**，**不能用 session_id echo 充数**。
- **两个 ECDH 别混：** TLS 握手 ECDHE = `x25519(client 临时, server SH 临时 keyshare)`（→ record 密钥）；REALITY AuthKey = `x25519(client 临时, server 静态 pbk)`（→ AuthKey/session_id/证书 HMAC）。

---

## 2. 待 grill 裁决的开放决策

| # | 问题 | 推荐 + 理由 |
|---|------|-------------|
| **(a)** | x509 提取：加 crate vs 手解 DER | **手解 DER**。只需两个定长字段（SPKI off12 取裸 32B 公钥；DER 末 64B 取签名）；`x509-parser` 拉一堆 transitive + verify feature 引 `ring` 破 ADR-0003。信任锚是 HMAC+Finished、与证书链无关。**须加长度/marker 校验 loud-fail**。 |
| **(b)** | 握手 IO：sans-IO 核心 + 薄驱动 vs 泛型 async | **sans-IO 核心 + 薄 async 驱动**（over generic `AsyncRead+AsyncWrite`，用 duplex 测）。延续刀6/7，整条 flow 用 RFC 8448 §3 KAT 离线钉死；产出 `recv_app_keys / send_app_keys / leftover_bytes`。 |
| **(c)** | CertificateVerify 验 / defer | **defer 验签、但全字节折入 transcript**。REALITY 信任锚是 cert HMAC + Finished MAC、非 PKI；验 CV 零安全增益却要引 ed25519-verify dep。记 ADR。 |
| **(d)** | 客户端是否发 CCS | **发一次** `14 03 03 00 01 01`（SH 后、加密 Finished 前）。RFC 8446 D.4 + Chrome/uTLS 恒发。 |
| **(e)** | VLESS UUID / PBK env 编码 | UUID：新增 `MINI_VPN_REALITY_UUID`，`parse_uuid` 暂内联复制。PBK：**base64url 优先 + 回退 std，解码后强断言恰 32B 否则 loud-fail**。须加 `base64` crate。 |
| **(f)** | 借用站 cipher 0x1301 选择 | **首选收紧 ClientHello 仅 offer 0x1301（根除 loud-fail）+ dest 选 gateway.icloud.com / dl.google.com + helper 加 openssl 出口预检**。⚠️ 收紧 offer 会偏离 Chrome 三套件指纹 → 健壮性 vs 指纹真实性，本身值得 grill。 |

---

## 3. RealityStream / RealityUpstream / 接线架构草图

```
src/reality/
  handshake.rs (新)  sans-IO 握手核心 + 薄 async 驱动
                     send CH(16 03 01) → send CCS(14 03 03 00 01 01) → recv SH(0x16)
                     → derive_handshake_keys → skip 明文 CCS(0x14,不计 seq)
                     → loop recv 0x17 → open → 累积 inner 0x16 → reframe(1B+3B) → EE/Cert/CertVerify/Finished
                     → 提 ed25519 pubkey(SPKI off12)+sig(DER 末64B) → verify_server_cert(REALITY auth 决策)
                     → fold CV(不验签) → verify server Finished → derive_application_keys
                     → send client Finished(c_hs @seq0) → 切 c_ap
                     产出 { recv_keys(s_ap), send_keys(c_ap), leftover_bytes }
  vless.rs    (新)  encode_vless_request(uuid,command,target)（PortThenAddress；ATYP v4=01/domain=02/v6=03；新写不复用 tuic）
                    strip_vless_response(&mut buf)->Option<usize>（2+N,累积,一次性）
  cert.rs     (新)  extract_ed25519_pubkey_and_sig(cert_msg)->([u8;32],[u8;64])（off11 定位 DER→扫 06 03 2b 65 70+03 21 00→裸32B→DER末64B）

src/reality_upstream.rs (新)
  struct RealityConfig{ server,uuid:[u8;16],pbk:[u8;32],short_id,sni }  // from_env, Debug redacted
  struct RealityStream{ read_half,write_half(into_split), recv_keys,send_keys, read_raw,plaintext_out:BytesMut, vless_stripped:bool }
  impl AsyncRead+AsyncWrite: poll_write 切 <=16384B 0x17 record(seal); poll_read frame outer→open→demux(0x17 上抛+首次 strip 响应头; inner 0x16 NST/KeyUpdate drop)
  impl ProxyUpstream::open_tcp: connect→handshake.drive→send encode_vless_request(c_ap 0x17)→Ok(Box::new(RealityStream)); 失败→Err(Reality)（0x1302 loud-fail propagate 不 panic），无连接复用
  impl DatagramUpstream::send_udp: no-op（UDP-over-VLESS 是刀9）

src/client_tun.rs (改)
  start_tun_proxy: match MINI_VPN_UPSTREAM { "reality" => RealityUpstream + 永不 send 的 dummy mpsc → run_event_loop; _ => 现 TUIC }  // 两次单态调用
```

**关键贴合：** `RelayStream=Box<dyn AsyncStream>` 已是 trait object；`into_split` 避免 poll_read/poll_write 跨借用；
UDP 下行 channel **持有 tx 永不 send**（已 Read `run_event_loop:586`：`Some(dg)=recv()` 在 None 时该分支被 disable；永不 send → 永久 pending，最安全）。

---

## 4. TDD 任务分解草案（有序；红→绿→commit→push）

| # | 任务 | 类型 | KAT/判据 |
|---|------|------|----------|
| T1 | `vless.rs::encode_vless_request` | 离线 KAT | 26B IPv4 KAT `0011×16 000101bb0101020304`；域名变体；port 在 atyp 前、ATYP v4=01/domain=02/v6=03 |
| T2 | `vless.rs::strip_vless_response` | 离线 KAT | 空 addons strip 2B；非空(N) strip 2+N；跨 2 段累积；一次性门控 |
| T3 | `cert.rs::extract_ed25519_pubkey_and_sig` | 离线 KAT | 构造 fresh ed25519 自签 cert（RFC 8448 cert 是 RSA 不可用）：off11 定位、SPKI off12 取裸 32B、DER 末 64B 取 sig；marker/len mismatch loud-fail |
| T4 | `cert` ⊕ `verify_server_cert` 端到端 | 离线 KAT | known AuthKey 对 sample cert 算 HMAC 写入末 64B → `verify_server_cert==true`；篡改 1B → false |
| T5 | `handshake.rs` sans-IO 核心全流程 | 离线 KAT | RFC 8448 §3 全序列：CH→SH→[fold]→server Finished verify(`9b9b141d...0718`)→app keys；漏折 CV 必红；CCS 不计 seq |
| T6 | 跨 record 重组（reframe 1B+3B） | 离线 KAT | 人为切 2~3 条 record，断言重组出同一 transcript hash `edb7725f...10ed` |
| T7 | PBK base64url 解码 + 32B 校验 | 离线 KAT | 43 字符 base64url → 32B；带 `=` std 回退；非 32B loud-fail |
| T8 | `RealityStream` poll_read/poll_write + 响应头 strip | 离线 KAT | mock duplex：seal/open 往返 + 首读 strip 响应头 + NST drop；>16384B 分块 |
| T9 | `RealityUpstream::from_env` + open_tcp 接线 | 离线 | env 解析、Debug redacted、failure → `ClientError::Reality` |
| T10 | `client_tun` MINI_VPN_UPSTREAM 分支 + dummy mpsc | 离线 | env=reality 走 RealityUpstream；tx 永不 send 分支 pending；send_udp no-op |
| T11 | helper：sing-box inbound 配置 + openssl 0x1301 出口预检 | acceptance | dest 探到非 0x1301 拒启动 |
| T12 | 真出口 acceptance（仿 `knife35-acceptance.sh`） | acceptance | 三端闭环：client 日志 `verify_server_cert true`（非 echo）+ utun + 路由 + curl 经隧道 200/301；sing-box vless accept 非 decoy；量化握手延迟；JA3 best-effort |

---

## 5. 风险与坑

1. **0x1302 decoy / cipher 归因**（最致命）：根因是 offer 了解不了的套件，非 dest 属性。收紧 offer 仅 0x1301 或选 0x1301 dest + VPS 出口 openssl 预检。
2. **跨 record 重组**：真实大证书必跨 record；须独立 handshake buffer 按 1B+3B 重组。
3. **seq 归零 / dummy CCS 计数**：CCS 误计入 read seq → 后续 nonce 全错、静默死。`s_hs→s_ap`、`c_hs→c_ap` 切密钥 seq 必归零。
4. **AAD / nonce**：record AAD=5B 头（len=密文含 tag）；nonce=`iv XOR seq`。明文 CH/CCS 不进 `record_header()`。
5. **VLESS port-first 错位**（最难诊断）：复用 tuic `encode_address` → 表现「握手成功但无数据」。新写 VLESS encoder + golden KAT。
6. **VLESS 响应头 strip**：`2+N` 可能与首段数据同 record；累积 `≥2+N` 后一次性 strip，读 `byte[1]` 动态算 N，禁硬编 2。
7. **CertVerify fold 漏折**：defer 的是验签不是折叠；漏折 → Finished MAC + app keys 全错、静默失败。
8. **手解 DER 多字节长格式**：只定位固定 SPKI 子模式取裸 32B，签名直接取 DER 末 64B，避开外层 SEQUENCE len 解析。
9. **PBK 编码混淆**：sing-box=base64url(43)；解错或非 32B → AuthKey 错 → session_id 服务端解不开 → **静默回落 decoy**。解码后强校验 32B。
10. **三处域名一致性**：client SNI ∈ server `serverNames` 集合（推荐三处同域）；不命中 → server 静默转发 decoy。
11. **server keyshare = 不可信网络输入**：`key_schedule` 已拒全零 ECDHE；真握手首次喂真 keyshare 须验该检查生效。
12. **PQ keyshare**：cloudflare/google/icloud 可能发 X25519MLKEM768；mini_vpn 只发纯 X25519 → `supported_groups` 仅含 X25519（已是），keyshare group 不匹配会错算 ECDHE。
13. **无连接复用**：每条 TCP = 全握手，高并发成本远超 TUIC；刀8 可接受，acceptance 须量化握手延迟，复用留刀9。

---

## 6. 仍不确定 / 需真出口才能定的点

1. **cipher 实测须从 VPS egress 复现**（当前 16 站分区在本机 macOS 测得；CDN 站按地域可能不同）。
2. **收紧 ClientHello cipher offer 的指纹代价**：仅 offer 0x1301 根除 loud-fail，但偏离 Chrome 真实三套件 → 需抓包权衡（影响决策 (f)）。
3. **server keyshare 全零 / contributory 检查在真 keyshare 下的行为**（仅理论锁定）。
4. **PQ group 协商实测**（dest 在只发纯 X25519 时是否稳定回退）。
5. **VLESS 字节合约 KAT 为推导非抓包**（须对真 sing-box VLESS 出口跑 acceptance 验证字节级互通）。
6. **ed25519 SignatureScheme `0x0807`**（本刀 defer CV 验签故不影响；将来验 CV 须复核）。
