# 刀8 PLAN — TDD 分解（红→绿→commit→**每 commit 后 git push**）

> 配套 [spec](2026-06-23-knife8-reality-live-handshake-spec.md) + [研究 brief](2026-06-23-knife8-research-brief.md)。
> 分支 `claude/knife8-reality-live-handshake`（从 main `b6190d0` 起）。一个分支一个 writer。
> 每 task：先写失败测试 → red → 实现 → green → commit → push。离线 KAT（T1-T10）先行，acceptance（T11-T12）待服务端。

## T0 — 脚手架 + ADR + deps

- Cargo.toml：`+ x509-cert`（解析临时证书）、`+ base64`（PBK 解码）。
- `src/lib.rs`：`pub mod reality_upstream;`；`src/reality/mod.rs` 加 `pub mod {handshake,vless,cert};`。
- ADR：**新增 `docs/adr/0010-reality-certverify-defer.md`**（CertVerify defer + fold）；**修订 `docs/adr/0009-*`**（收紧 ClientHello cipher offer 为 0x1301，consequence 段）。
- commit：`chore+docs(knife8): scaffold modules + x509-cert/base64 deps + ADR-0010/0009 (T0)`。

## T1 — VLESS 请求头编码（离线 KAT）

- `vless.rs::encode_vless_request(uuid:&[u8;16], cmd:u8, target:&TargetAddr) -> Vec<u8>`。
  布局：`ver(0x00) | uuid(16) | addon_len(0x00) | cmd | port(2 BE) | atyp | addr`；ATYP v4=01/domain=02/v6=03；PortThenAddress。
- KAT：IPv4 `1.2.3.4:443`、UUID 全 `0x11`、TCP → `0011…11 00 01 01bb 01 01020304`（26B）；
  域名 `example.com:443` → `…00 01 01bb 02 0b 6578616d706c652e636f6d`；IPv6 变体；域名 >255 截断不 panic。
  **断言 port 在 atyp 前、ATYP 数值正确**（防 tuic 错位）。
- commit + push：`feat(knife8): VLESS request frame encoder (golden KAT) (T1)`。

## T2 — VLESS 响应头 strip（离线 KAT）

- `vless.rs::VlessResponseStripper{ stripped:bool }`，`strip(&mut self, buf:&mut BytesMut) -> bool`（true=已剥完，可放行）。
  逻辑：未剥时需 `buf.len()>=2`，读 `n=buf[1]`，需 `buf.len()>=2+n` → drain `2+n`、置 stripped；不足则等下次（累积）。
- KAT：空 addons（`00 00`）strip 2B；非空 addons（`00 03 aabbcc`）strip 5B；跨两段累积（先喂 1B 再喂余下）；**仅首次剥一次**（stripped 后透传）。
- commit + push：`feat(knife8): VLESS response header stripper (T2)`。

## T3 — 证书 ed25519 pubkey + 签名提取（离线 KAT）

- `cert.rs::extract_ed25519_pubkey_and_sig(cert_msg:&[u8]) -> Result<([u8;32],Vec<u8>), ClientError>`。
  从 Certificate(0x0b) message：off11 定位第一条 leaf DER（4B hdr + 1B ctx=0 + 3B list len + 3B cert len）；
  `x509-cert` parse DER → `tbs.subject_public_key_info.subject_public_key` 裸 32B（**校验 SPKI alg OID == ed25519 1.3.101.112**）+ `cert.signature` 裸字节（64B HMAC）。
  长度/marker/OID mismatch → `ClientError::Reality` loud-fail（防静默 decoy）。
- KAT：测试内构造 ed25519 自签 cert 样本（RFC 8448 cert 是 RSA 不可用）：用已知 32B pubkey + 任意 64B 占位签名组 DER → 包成 0x0b message → 提取断言 pubkey==已知、sig==占位。截断/错 OID → Err。
- commit + push：`feat(knife8): extract ed25519 pubkey+sig from REALITY temp cert via x509-cert (T3)`。

## T4 — cert 提取 ⊕ verify_server_cert 端到端（离线 KAT）

- 用已知 AuthKey 对 T3 样本 cert 的 32B pubkey 算 `HMAC-SHA512(AuthKey, pubkey)` 写入末 64B → `verify_server_cert(AuthKey, pubkey, sig)==true`。
- 篡改 pubkey/sig 任一字节 → false。错 AuthKey → false。
- commit + push：`test(knife8): cert extract ⊕ verify_server_cert REALITY auth end-to-end (T4)`。

## T5 — 跨 record handshake 重组（离线 KAT）

- `handshake.rs::HandshakeReassembler`：喂解密后 inner 0x16 字节，按 `1B type + 3B uint24 len` 切出完整 message；
  半条缓存等后续；一次喂入可含多条；message 跨喂入累积。
- KAT：把 RFC 8448 §3 flight payload（657B，EE||Cert||CertVerify||Finished）人为切成 2~3 段喂入 → 重组出 4 条 message，
  断言 `transcript_hash(CH,SH,EE,Cert,CV) == edb7725f…10ed`（钉死重组正确性）。
- commit + push：`feat(knife8): cross-record TLS1.3 handshake message reassembler (RFC 8448 KAT) (T5)`。

## T6 — 握手 drive 全流程 sans-IO 编排（离线 KAT，duplex + server 模拟器）

- `handshake.rs::drive<S: AsyncRead+AsyncWrite+Unpin>(stream, HandshakeParams) -> Result<HandshakeOutput, ClientError>`
  + `RecordReader`（读完整 outer record，跨 read 缓冲）。编排 spec §5 步骤 1-17。
  **cert-verify 接缝可注入**：drive 接受一个 `verify: impl Fn(&[u8] cert_msg) -> Result<(),_>`，
  使 RFC 8448 transcript（RSA cert）与真 ed25519 cert HMAC 解耦（RFC KAT 用 always-ok verify；真 RealityUpstream 注入 `verify_server_cert`）。
- KAT：测试内 server 模拟器在 duplex 一端按序写 [SH record][server CCS][加密 flight record(s)]（用 RFC 8448 server hs key/iv 复算 flight 密文，或直接用 brief 的 SFLIGHT_RECORD）；
  client drive 跑通 → 断言 ① 收到并验过 server Finished（`9b9b141d…0718`）；② 输出 app keys == RFC 8448 app KAT（c_ap/s_ap key/iv）；③ 客户端发出的 client Finished record 字节正确（用 c_hs 解开验 verify_data）；④ server CCS 未计入 read seq（否则 flight 解密失败）。
  反向：漏折 CertVerify → server Finished 验证红；server CCS 误计 seq → flight open 红。
- commit + push：`feat(knife8): async REALITY handshake driver (RFC 8448 e2e via duplex sim) (T6)`。

## T7 — PBK base64 解码 + 32B 校验（离线 KAT）

- `reality_upstream.rs::parse_pbk(s:&str) -> Result<[u8;32], ClientError>`：base64url(RawURL) 优先 → 失败试 RawStd/Std → 解码后**强断言恰 32B** 否则 loud-fail。
- KAT：sing-box 风 43 字符 base64url → 32B；带 `=` std；非 32B（短/长）→ Err；hex(64) → Err（非 base64 字母表多半失败，显式断言）。
- commit + push：`feat(knife8): REALITY public_key base64url/std decode + 32B guard (T7)`。

## T8 — RealityStream poll_read/poll_write（离线 KAT，mock duplex）

- `RealityStream` impl `AsyncRead+AsyncWrite`：poll_write 把明文切 ≤16384B → `send_keys.seal(0x17,..)` 写；
  poll_read 读完整 outer record → `recv_keys.open` → demux：inner 0x17 →（首次过 `VlessResponseStripper`）→ 上抛 `plaintext_out`；
  inner 0x16（NST 0x04 / KeyUpdate 0x18）→ 解密后丢弃；inner 其它 → Err 或忽略（记日志）。半条 record 跨 poll 缓冲（`read_raw`）。
- KAT：mock duplex 两端各持对称 app keys：A `RealityStream` write → B 用 recv keys open 还原；B write（前置 VLESS 响应头）→ A `RealityStream` read 自动 strip 响应头后得真数据；
  注入一条 NST record → 被吞、不污染；写 >16384B → 分多 record；半条 record 分两次喂 → 正确重组。
- commit + push：`feat(knife8): RealityStream AsyncRead/Write over TLS1.3 app records + VLESS strip (T8)`。

## T9 — RealityUpstream + RealityClientConfig（离线）

- `RealityClientConfig::from_sources/from_env`（MINI_VPN_REALITY_*；UUID 仿 parse_uuid；PBK 经 T7；short_id 经 `auth::parse_short_id`）；
  自定义 Debug 脱敏（uuid/pbk redacted）。
- `RealityUpstream` impl `ProxyUpstream::open_tcp`：connect → drive（注入 `verify_server_cert`）→ send VLESS 请求 → RealityStream；
  失败 → `ClientError::Reality`（0x1302 loud-fail / 握手失败 propagate，不 panic）。impl `DatagramUpstream::send_udp` no-op。
- KAT：from_env 解析（设临时 env）、Debug redacted 断言、缺字段 Err、PBK 非 32B Err。open_tcp 用测试内 server 模拟器（或最小 loopback）走通一次（可选）。
- commit + push：`feat(knife8): RealityUpstream(ProxyUpstream) + RealityClientConfig from_env redacted (T9)`。

## T10 — client_tun env 选择器 + dummy downlink（离线）

- `start_tun_proxy`：`match std::env::var("MINI_VPN_UPSTREAM").as_deref()`：
  `Ok("reality")` → `RealityClientConfig::from_env` → `RealityUpstream` → 造 `mpsc::channel(1)` 持 tx（永不 send）→ `run_event_loop(device, up, rx, cfg, NoopSink)`；
  `_`（含 tuic/缺省）→ 现 TUIC 路径（零回归）。两次单态化 `run_event_loop`。
- 测试：选择器纯函数化（`fn select_upstream_kind(env:Option<&str>)->Kind`）单测 tuic|reality|缺省；DatagramUpstream::send_udp no-op 不 panic。
  （真 utun 启动归 acceptance。）
- commit + push：`feat(knife8): MINI_VPN_UPSTREAM selector (tuic|reality) + dummy downlink (T10)`。

## T11 — acceptance helper（待服务端；可先写脚本）

- 仿 `scripts/knife35-acceptance.sh`：`scripts/knife8-reality-acceptance.sh`，env 读 `MINI_VPN_REALITY_*` + `MINI_VPN_UPSTREAM=reality`；
  setup/soak/stop（建 utun + 加路由 + 还原）；**openssl 出口预检**：`openssl s_client -connect <dest>:443 -tls1_3 -servername <sni>` 抓 `Cipher` 非 `TLS_AES_128_GCM_SHA256` → 拒启动（ADR-0009 gap 变启动期 loud）。
- commit + push：`feat(knife8): REALITY acceptance helper + openssl 0x1301 egress preflight (T11)`。

## T12 — 真出口 acceptance（待服务端 + 凭据）

- 用户备 sing-box VLESS+REALITY inbound（uuid / reality-keypair / short_id / handshake.server 0x1301 借用站 / flow 空）→ 给 client env。
- 验收：`MINI_VPN_UPSTREAM=reality` 起 client-tun → 日志 **`verify_server_cert==true`**（非 echo 充数）→ `curl https://…` 经 REALITY 隧道 200/301，三端闭环；
  force-reality 下 UDP no-op 符合预期；量化握手延迟；JA3 best-effort（tcpdump + Wireshark 看 ClientHello 像 Chrome）。
- findings 落 spec/brief 末节；HANDOFF「刀8 完成」段；ADR/CONTEXT 收尾。

## 收尾
- `/code-review`（high effort）over diff → 修。
- HANDOFF.md 加「刀8 完成」段；core-roadmap 记忆更新（刀8 done → 刀9 next）。
- merge to main（fast-forward，按惯例用户主导或本 session 收尾）。

## 质量门（每 task 维持）
- 离线 KAT 全绿；`cargo clippy --all-targets --features harness` 0 warning；`cargo build --release` 绿。
- cwd 陷阱：每条 git/cargo 前 `cd` 到 worktree、绝对路径；`git branch --show-current` == `claude/knife8-reality-live-handshake`。
