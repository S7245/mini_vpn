# 刀7 — REALITY 握手核心（手写 TLS 1.3：ServerHello + key schedule + record AEAD）spec

> 配套：plan(`2026-06-23-knife7-reality-handshake-plan.md`)、ADR `docs/adr/0009-tls13-cipher-scope-0x1301-first.md`。
> 分支 `claude/knife7-reality-handshake`(从 main 起)。REALITY 第二 Transport mini-project **第二片**(刀6→刀9)。
> 设计输入 = understand-phase research workflow（5 路并行 + 综合，brief 见 session）。**本片 sans-IO、100% 离线**，
> 用 **RFC 8448 §3 字节级向量**做 KAT；真握手/socket/解密 server flight/VLESS/acceptance 归刀8。

## 北极星与边界

- 刀6 已给出 REALITY **auth**（ClientHello + session_id seal AES-256-GCM）。刀7 给出**完成 TLS 1.3 握手所需的离线密码学**：
  解析 ServerHello、跑 key schedule、record-layer AEAD（seal/open）。刀8 把它们接上 socket 跑活握手。
- **刀7 sans-IO**：`&[u8]` 进 / bytes+keys 出，零网络，每个产物有 RFC 8448 KAT。

## grill 裁决（2026-06-23）

- **cipher 范围（ADR-0009）**：**仅 KAT-pin `0x1301`(TLS_AES_128_GCM_SHA256)**——唯一有 RFC 8448 字节级向量的套件、且我方 ClientHello 首选、sing-box/REALITY 默认。但 **key_schedule 泛型-over-hash、record 泛型-over-cipher 从第一天起**（`enum CipherSuite{Aes128GcmSha256, Aes256GcmSha384, ChaCha20Sha256}` 携 hash_len/key_len/iv_len），只 wire+KAT 0x1301。
  **0x1302(AES-256-GCM-SHA384，无 RFC 向量、第二 hash)/0x1303(ChaCha20，需新 crate) 留 gap，写进 ADR**——避免刀8 遇到选 0x1302 的 decoy 站**静默失败**（cipher 由借用站选、不由 sing-box，大型 AES-NI 站常选 0x1302）。

## 刀7/刀8 边界（research 锁定）

**刀7 产出**（给定 ServerHello 字节 + 我方 client 临时 X25519 私钥）：
1. **ServerHello 解析 only**：提取 cipher_suite、server key_share X25519(单 entry)、确认 supported_versions==0x0304。廉价拒绝：HRR sentinel（random==`CF21AD74…8339C` → Err，完整 HRR 重协商 defer 刀8）、downgrade sentinel（random[24..32]==`DOWNGRD\x01/\x00` → Err）、compression!=0 → Err、version!=0x0304 → Err、**session_id_echo != 我方 sealed 32B → Err（仅 RFC 一致性检查；注释+ADR 写明 REALITY auth 成败由刀8 的证书 HMAC 定，NOT echo 匹配）**。
2. **key schedule** 到 `{c,s}_handshake_traffic_secret` + 每方向 key/iv，外加纯函数 `compute_finished_verify_data`。**HKDF-Extract 前必须拒绝 network ECDHE 全零/非贡献点**（auth.rs:22 已 flag）。
3. **record AEAD seal/open**（AES-128-GCM）：TLS 1.3 nonce（iv XOR seq，seq 右对齐进低 8B）、AAD=5B 头 `[0x17,0x03,0x03,len_hi,len_lo]`（len=密文含 16B tag）、inner=content||type||zero-pad、open 剥尾零→最后非零字节=真 content type、全零明文→Err、读/写**两个独立 seq**。
- **可选+推荐**：app-secret 派生（2nd derived→Master→{c,s}_ap_traffic+finished）——RFC 8448 也有向量、纯函数、cheap KAT、de-risk 刀8。

**刀8 拥有**（需活字节/socket）：TCP 连接 + 读写循环 + 握手状态机；真 ECDH（对 network keyshare）+ 解密 server flight（EncryptedExtensions/Certificate/CertificateVerify/Finished，外层 type=0x17、解密后内层 type=0x16）；跳过明文 dummy CCS（`14 03 03 00 01 01`，**不**进 read-seq）；X.509 DER 解析提 ed25519 pubkey+signature → 调刀6 `verify_server_cert`（REALITY auth 决策）；标准 CertificateVerify ed25519 检；server-Finished MAC 验 + 发 client Finished；app keys；VLESS 帧；RealityUpstream + env 选择器 + 真出口 acceptance。

## 组件设计（research 模块）

### C1 `src/reality/key_schedule.rs`（RFC 8446 §7.1/§7.3，泛型-over-hash，SHA-256 先 wire）
- `hkdf_label(length:u16, label:&str, context:&[u8]) -> Vec<u8>`：`tls13 `(**含尾空格**)+label 用 **u8** 长前缀、context u8 长前缀、顶层 length u16。**#1 静默互通杀手**——用 RFC 8448 KAT 钉死。
- `expand_label`/`derive_secret`/`extract`/`transcript_hash`（hkdf+sha2）。
- `derive_handshake_keys(ecdhe, client_hello, server_hello) -> Result<HsKeys>`：Early→derived→Handshake(from ECDHE)→{c,s}_hs→key/iv；**全零 ecdhe → Err（在 Extract 前）**。
- `compute_finished_verify_data(base_secret, transcript_hash)`：finished_key=Expand-Label(.,"finished","",hash_len) → HMAC。
- `struct HsKeys{ c_hs_secret,s_hs_secret:[u8;32], client_key,server_key:[u8;16], client_iv,server_iv:[u8;12] }`。
- ⚠️ **ECDHE 混淆风险**：TLS 握手 ECDHE = x25519(client 临时, **server 临时** keyshare from SH)；与刀6 REALITY AuthKey 的 x25519(client 临时, **server 静态** pbk) 是**不同**密钥——接错不过任何 KAT 但破活握手。

### C2 `src/reality/record.rs`（RFC 8446 §5.2/§5.3/§5.4，AES-128-GCM 先）
- `per_record_nonce(write_iv,seq) -> [u8;12]`（seq.to_be_bytes XOR 进 nonce[4..12]；seq=0 时 nonce==iv，首条 sanity）。
- `record_header(enc_len) -> [u8;5]`。
- `struct RecordKeys{ cipher, write_iv:[u8;12], seq:u64 }`；`seal(content_type, content)`/`open(header, encrypted) -> Result<(u8,Vec<u8>)>`（剥尾零、全零→Err、checked seq++）。
- ⚠️ AAD 的 u16 len = **密文长（含 16B tag）**，非明文长（off-by-16 风险）。

### C3 `src/reality/server_hello.rs`（RFC 8446 §4.1.3）
- `parse_server_hello(bytes, expected_session_id) -> Result<ParsedServerHello>` + 各拒绝路径；`struct ParsedServerHello{ cipher_suite:u16, server_key_share:[u8;32], session_id_echo:[u8;32] }`。
- 手写字节 walk + `#[cfg(test)]` 里 tls-parser 交叉验证（沿 client_hello.rs 纪律）。

### C4 `src/reality/mod.rs`：`pub mod {server_hello,key_schedule,record};`

## 测试边界（本刀 100% 离线，RFC 8448 §3 KAT）

- HkdfLabel KAT（`00100974…6b657900` 等）、Sha256("")、**`derive_secret(early,"derived",sha256(""))==6f2615a1…`**（端到端验 HkdfLabel）。
- key schedule KAT：喂 ecdhe `8bd4054f…`+CH(196B)+SH(90B) → early/derived/handshake/{c,s}_hs secret、transcript_hash `860c06ed…`、server/client key+iv（`3fce…03bc`/`5d31…0b30`，`dbfa…8d01`/`5bd3…265f`）；全零 ecdhe → Err。
- finished_key KAT（s_hs `008d3b66…`、c_hs `b80ad010…`）。
- record：nonce(iv,0)==iv、seal→open round-trip、篡改→Err、全零明文→Err；**golden：open RFC 8448 server flight record（key=`3fce…03bc` iv=`5d31…0b30` seq=0）→ content_type==0x16、内层起始 EncryptedExtensions(0x08)**——schedule+AEAD 字节级一致的最强离线证明。
- ServerHello：解析 90B RFC 8448 SH → cipher==0x1301/key_share==`c982…1f0f`/version==0x0304；HRR/downgrade/compression/echo-mismatch → Err；tls-parser 交叉验证。
- **测不到（归刀8+acceptance）**：真握手、解密真 server flight、证书 HMAC、与 sing-box 互通。

## 风险 / 已知边界（research）

- **HkdfLabel 编码**（#1）：`tls13 ` 含尾空格、label/context u8 前缀、顶层 u16。`derived_secret_1` KAT 一错即挂。
- **AAD off-by-16**；**nonce XOR 偏移 4**；**ECDHE 混淆**（见 C1）；**network keyshare 须拒全零**；**读/写两 seq**；**echo-match≠auth**。
- **cipher gap**：仅 0x1301；0x1302/0x1303 留 gap（ADR-0009）——刀8 遇 0x1302 decoy 会失败，泛型骨架使补齐 cheap。
- **RFC 8448 hex 转录**：server-flight record 字节从保存的 `/tmp/rfc8448.txt` 抄，勿从报告重打（防 wrap/空白损坏）；写测时重新抓取确认。
- **ADR**：cipher 范围 + gap + "echo≠auth" 不变量满足 surprising + 真权衡 + 操作者必须知道 → `docs/adr/0009-*`（T8 落库）。
