# 刀8 SPEC — REALITY 收官片：实 TCP 握手 + VLESS + RealityUpstream + 真出口 acceptance

> 正交线 A（VLESS+REALITY 第二 Transport）第三片 = 收官。把刀6/刀7 的离线积木接上真 socket，让
> **VLESS over REALITY over TCP** 端到端跑通 + 真出口 acceptance。这是 REALITY 第一次接网络、第一次需要服务端配合。
>
> 设计输入：understand-phase 5 路研究 workflow（26 agents、20 条互通-critical 断言对抗验证）→
> [研究 brief](2026-06-23-knife8-research-brief.md)（**所有字节级事实以 brief 为准**）+ grill 裁决（见下 §2）。
> 前置积木：ADR-0008（手写 TLS1.3）、ADR-0009（cipher 0x1301）、刀6/7 spec、`src/reality/{auth,client_hello,server_hello,key_schedule,record}.rs`。

## 1. 目标与范围

### 北极星
`Rules.md` ① TCP 连接。REALITY 是抗封锁韧性（QUIC 被 GFW 封时的 TCP fallback），正交于三目标、不阻塞。

### In scope（本刀做）
1. **实 TCP 握手状态机**（tokio `TcpStream`）：发 ClientHello → 读 ServerHello → ECDH → 跳明文 dummy CCS →
   解密 server flight（EE/Cert/CertVerify/Finished）→ 提 ed25519 SPKI pubkey + 签名 → `verify_server_cert`
   HMAC-SHA512（**REALITY auth 决策**）→ server-Finished MAC 验 → 发 client Finished → app keys。
2. **VLESS 空 flow 帧**：请求头编码（新写，不复用 tuic）+ 响应头 strip。
3. **`RealityStream`**：impl `AsyncRead+AsyncWrite` over TCP + app RecordKeys（TLS1.3 record AEAD）+ VLESS 响应 strip + post-handshake record drop。
4. **`RealityUpstream`**：impl `ProxyUpstream::open_tcp`（每条 TCP 一次完整握手 + VLESS 请求）+ `DatagramUpstream::send_udp` no-op。
5. **env 选择器** `MINI_VPN_UPSTREAM=tuic|reality`（默认 tuic）；reality 下喂空 downlink channel（持 tx 永不 send）。
6. **真出口 acceptance**：仿 `scripts/knife35-acceptance.sh`，三端日志闭环 + openssl 出口预检。

### Out of scope（不碰，刀9+ 或 ADR gap）
- UDP-over-VLESS、auto-failover、分离 TCP/UDP 上游（刀9）。
- 连接复用（每条 TCP 一次握手；reuse 留刀9）。
- 0x1302/0x1303 record/key schedule（ADR-0009 gap）。
- Vision flow（已 defer，空 flow）。
- 标准 CertificateVerify 验签（本刀 defer，见 §2c）。
- post-handshake KeyUpdate 密钥轮换（本刀 loud-fail，见 ADR-0010 gap；完整轮换留刀9）。

### Code-review deferred（→ 刀9，本刀已止血/登记）
- **M3 握手并发化**：`open_tcp` 现在主循环 inline await 跑完整握手（多 RTT），高并发下串行化、单连接握手延迟拖累
  所有 flow（TUIC 因复用既有 QUIC 连接不暴露）。**本刀止血 = `open_tcp` 10s 超时**（H2，防慢/半开 server stall
  整个事件循环）；**根治 = 把 connect+握手+发 VLESS 整体 spawn 出主循环、经 channel 交回 RelayStream**（类
  `spawn_remote_relay`），属并发架构改造，留刀9（与 failover 一起）。
- **L2 RealityStream relay 阶段缺 idle/读超时**：握手后的 relay 读无超时——server 不发够 VLESS 响应头 addons 或
  发完后 hang 会让该 flow 的 relay task 永久 pending（仅占一个 spawned task + listener 槽，非忙等、非全局 stall；
  威胁模型下 REALITY server 是用户自配出口）。根治 = relay 读 idle 超时，与 M3 同根，留刀9。

## 2. Grill 裁决（2026-06-23）

| # | 决策 | 裁决 | 影响 |
|---|------|------|------|
| (f) | 0x1302 decoy 风险 | **两者都做**：收紧 ClientHello 仅 offer 0x1301 + 选 0x1301 dest + openssl 出口预检 | 改 `client_hello.rs` CIPHERS；**修订 ADR-0009** |
| (a) | x509 提取 | **加 `x509-cert` crate**（RustCrypto 系，纯 Rust，不引 ring/不破 ADR-0003） | 新 dep；`cert.rs` 用它解析 |
| (c) | 标准 CertificateVerify | **defer 验签，但全字节折入 transcript** | **新增 ADR-00010**；footgun 锁死 |
| (b) | 握手 IO 架构 | 薄 async 驱动 `drive<S: AsyncRead+AsyncWrite>` 编排已有纯步骤；duplex + 测试内 server 模拟器离线 e2e | — |
| (d) | 客户端 CCS | **发**一次 `14 03 03 00 01 01`（SH 后、加密 Finished 前） | — |
| (e) | env 编码 | `MINI_VPN_REALITY_{SERVER,UUID,PBK,SHORT_ID,SNI}`；PBK base64url 优先+std 回退+强断言 32B；**加 `base64` crate**；脱敏 Debug | 新 dep |
| scope | 连接复用 | 无（每 TCP 一次握手）；reuse 留刀9 | acceptance 量化握手延迟 |

## 3. 互通-critical 不变量（别再踩，详见 brief §1）

1. **两个 ECDH 别混**：TLS 握手 ECDHE = `x25519(client 临时, server SH 临时 keyshare)`（→ record 密钥）；
   REALITY AuthKey = `x25519(client 临时, server 静态 pbk)`（→ AuthKey/session_id/证书 HMAC）。
2. **record 头**：明文 CH = `16 03 01`（自加 5B）；密文 = `17 03 03`（`record.rs::record_header` 已是）。
3. **dummy CCS**：收到的 `14 03 03 00 01 01` **整条丢弃、不 open、不递增 server read seq**；客户端自己**也发**一次。
4. **跨 record 重组**：内层 0x16 字节拼进**独立 handshake buffer**，按 `1B type + 3B len` 切消息；一 record ≠ 一 message。
5. **seq 归零**：read `s_hs→s_ap`、write `c_hs→c_ap` 切密钥各归零；读/写 seq 独立。
6. **transcript 折叠**：server Finished verify = hash(CH..CertVerify)；client Finished + app keys = hash(CH..serverFinished)。
   **CertVerify 字节必折**（即便 defer 验签），漏折 → MAC + app keys 全错、静默死。
7. **REALITY auth 决策** = `verify_server_cert`（HMAC-SHA512(AuthKey, 裸 32B ed25519 pubkey) == cert 末 64B），**非** session_id echo。
8. **VLESS 地址 = PortThenAddress**（port 2B BE 在前、atyp 在后）；ATYP v4=`0x01`/domain=`0x02`/v6=`0x03`（与 tuic 错位）。**新写编码器**。
9. **VLESS 响应头**：首段 app data strip `2+addons_len`（读 byte[1] 动态算，禁硬编 2），跨 record 累积 ≥2+N 后一次性。
10. **server keyshare 不可信**：`derive_handshake_keys` 已拒全零 ECDHE（真握手首次喂真 keyshare 须验生效）。
11. **PBK 编码**：base64url(43 字符) 优先，std 回退，解码后强断言**恰 32B** 否则 loud-fail（错 → AuthKey 错 → 静默回落 decoy）。

## 4. 模块布局

```
src/reality/
  handshake.rs   (新)  async drive<S: AsyncRead+AsyncWrite+Unpin>(stream, HandshakeParams)
                       -> Result<HandshakeOutput{ recv_keys:RecordKeys(s_ap), send_keys:RecordKeys(c_ap),
                                                   leftover:Vec<u8> }, ClientError>
                       + RecordReader（读一条完整 outer record，跨 read 缓冲）
                       + HandshakeReassembler（解密后 inner 0x16 跨 record 重组成 message）
  vless.rs       (新)  encode_vless_request(uuid:&[u8;16], cmd:u8, target:&TargetAddr) -> Vec<u8>
                       VlessResponseStripper{ stripped:bool }  strip(&mut self, &mut BytesMut) -> bool
  cert.rs        (新)  extract_ed25519_pubkey_and_sig(cert_msg:&[u8]) -> Result<([u8;32],Vec<u8>), ClientError>
                       （x509-cert 解析；msg off11 定位 leaf DER → SPKI subject_public_key 32B + cert.signature 64B）

src/reality_upstream.rs (新)
  struct RealityClientConfig{ server:SocketAddr, uuid:[u8;16], pbk:[u8;32], short_id:[u8;8], sni:String }
     impl from_sources/from_env（MINI_VPN_REALITY_*）；自定义 Debug 脱敏（uuid/pbk redacted）；PBK base64url+std→32B
  struct RealityUpstream{ cfg }
     impl ProxyUpstream::open_tcp：TcpStream::connect → handshake::drive → send VLESS 请求 → Ok(Box::new(RealityStream))
     impl DatagramUpstream::send_udp：no-op（可选 debug 计数）
  struct RealityStream{ read_half, write_half(into_split), recv_keys, send_keys,
                        read_raw:BytesMut, plaintext_out:BytesMut, resp:VlessResponseStripper }
     impl AsyncRead+AsyncWrite+Unpin+Send

src/client_tun.rs (改 start_tun_proxy)
  match MINI_VPN_UPSTREAM { "reality" => RealityUpstream + dummy mpsc(tx 持有永不 send) → run_event_loop; _ => TUIC }

src/reality/client_hello.rs (改)  CIPHERS：删 0x1302/0x1303（收紧 offer，裁决 f）
src/lib.rs (改)                   pub mod reality_upstream; reality::{handshake,vless,cert}
Cargo.toml (改)                   + x509-cert, + base64
```

## 5. 握手时序（drive 内部，逐步核对密钥/seq）

```
1. TcpStream::connect(server)
2. 生成 client 临时 X25519 keypair (sk_c, pk_c)
3. AuthKey = derive_auth_key(x25519(sk_c, pbk静态), random)        ← REALITY ECDH（静态 pbk）
4. ch_msg = build_authed_client_hello{ server_name=sni, key_share=pk_c, random, auth_key, short_id, timestamp=now }
5. 发 [16 03 01 || u16(ch_msg.len) || ch_msg]
6. 发 [14 03 03 00 01 01]                                          ← 客户端 dummy CCS（裁决 d）
7. 读 record：期望 0x16 → sh_msg = parse_server_hello(sh, expected=ch_msg[39..71])  ← echo 一致性（非 auth）
8. ecdhe = x25519(sk_c, sh.server_key_share)                       ← TLS 握手 ECDH（SH 临时 keyshare）
9. hs = derive_handshake_keys(ecdhe, ch_msg, sh_msg)               ← 全零 ECDHE 已拒
   recv = RecordKeys::new(hs.server_key, hs.server_iv)  [read-seq 0]
   send = RecordKeys::new(hs.client_key, hs.client_iv)  [write-seq 0]
10. 读 record 循环：
    - 0x14 (CCS) → 丢弃，不 open、不动 recv.seq                    ← 不变量 3
    - 0x17 → recv.open() → (inner_type, content)
        inner 0x16 → 拼进 handshake buffer → 切 message → EE(0x08)/Cert(0x0b)/CertVerify(0x0f)/Finished(0x14)
    收集到 server Finished 为止
11. (Cert) extract_ed25519_pubkey_and_sig → verify_server_cert(AuthKey, pubkey, sig)  ← REALITY auth 决策；false→Err
12. (CertVerify) 字节折入 transcript（不验签，裁决 c）
13. 验 server Finished：compute_finished_verify_data(hs.s_hs_secret, hash(CH..CertVerify)) == finished.verify_data → false→Err
14. th_sfin = hash(CH..serverFinished)
15. 发 client Finished：fin = compute_finished_verify_data(hs.c_hs_secret, th_sfin)
    msg = [14 00 00 20 || fin]；用 send(c_hs)@write-seq seal 0x16 record → 写出
16. app = derive_application_keys(hs.handshake_secret, th_sfin)
    recv_keys = RecordKeys::new(app.server_key, app.server_iv)  [read-seq 0]   ← seq 归零
    send_keys = RecordKeys::new(app.client_key, app.client_iv)  [write-seq 0]  ← seq 归零
17. 返回 HandshakeOutput{ recv_keys, send_keys, leftover=RecordReader 多读的未消费字节 }
```

随后 open_tcp：`send_keys.seal(0x17, encode_vless_request(uuid, 0x01, target))` 写出 → 构造 RealityStream（首读 strip VLESS 响应头）。

## 6. 测试策略（离线优先；活 socket 归 acceptance）

- **纯步骤**已 RFC 8448 §3 KAT 绿（reality/*）：直接复用作锚。
- **新增离线 KAT**（详见 plan）：VLESS 请求/响应、cert 提取 + verify 端到端、跨 record 重组、PBK 解码、RealityStream poll 往返、握手 drive 全流程（duplex + 测试内 server 模拟器跑 RFC 8448 序列）。
- **测试内 REALITY server 模拟器**：用 RFC 8448 §3 的 CH/SH/flight 字节 + 已知 server hs key/iv，在 `tokio::io::duplex` 一端按序喂 SH record、server CCS、加密 flight；client `drive` 跑通 → 断言 recv/send app keys == RFC 8448 app KAT。**注**：RFC 8448 cert 是 RSA、verify_server_cert 不适用 → 模拟器对 cert 步骤用「构造的 ed25519 自签 cert + 已知 AuthKey 写入 HMAC」分支覆盖（T3/T4 单独 KAT；drive 全流程用一个可注入的 cert-extract/verify 接缝以便 RFC 8448 transcript 与真 ed25519 cert 解耦）。
- **活 socket**（真 sing-box）：acceptance（T11/T12）。

## 7. 风险（详见 brief §5）

最致命三条：① **0x1302 decoy / cipher**（裁决 f 已收紧 offer + 预检根治）；② **跨 record 重组**（真证书必跨 record，独立 buffer 按 1B+3B）；③ **VLESS port-first 错位 + 响应头 strip**（表现「握手成功但无数据」，新写 encoder + golden KAT + 动态 strip）。

## 8. 完成判据
- T1-T10 离线 KAT 全绿、`clippy --all-targets --features harness` 0 warning、release build 绿。
- `/code-review`（high effort）findings 处理完。
- 真出口 acceptance：client 日志 `verify_server_cert==true`（非 echo 充数）+ curl HTTPS 经 REALITY 隧道三端闭环；force-reality 下 UDP no-op 符合预期；握手延迟量化；JA3 best-effort。
- ADR-00010 + ADR-0009 修订 + CONTEXT/HANDOFF 更新。
