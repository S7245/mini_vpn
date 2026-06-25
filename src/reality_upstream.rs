//! REALITY 第二 Transport 上游（刀8 T9/T10，见 spec §4 + brief §3）。
//!
//! 中文要点：`RealityUpstream` impl `ProxyUpstream::open_tcp`——每条 TCP 新建一次完整 REALITY 握手、
//! 接着发 VLESS 请求 → 返回 `RealityStream`（impl AsyncRead+AsyncWrite over TLS 1.3 app record 层、含 VLESS 响应 strip）。
//! impl `DatagramUpstream::send_udp` = **no-op 静默丢**（REALITY 是 TCP-only，UDP-over-VLESS 是刀9）。
//! `RealityClientConfig::from_env`（MINI_VPN_REALITY_*，脱敏 Debug）。无连接复用（reuse 留刀9）。

use crate::reality::auth::{
    derive_auth_key, generate_ephemeral_keypair, parse_short_id, verify_server_cert,
    x25519_shared_secret,
};
use crate::reality::cert::extract_ed25519_pubkey_and_sig;
use crate::reality::client_hello::{AuthedClientHelloParams, authed_session_id, build_authed_client_hello};
use crate::reality::handshake::{self, HandshakeInput};
use crate::reality::key_schedule::{expand_label, next_application_traffic_secret};
use crate::reality::record::RecordKeys;
use crate::reality::vless::{VLESS_CMD_TCP, VlessResponseStripper, encode_vless_request};
use crate::shared::{ClientError, TargetAddr};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use bytes::BytesMut;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::TcpStream;

/// TLS 1.3 record 明文上限（2^14）——每条 0x17 record 至多封这么多明文。
const MAX_PLAINTEXT: usize = 16384;
/// TLS record 密文上限（2^14 + 256，RFC 8446 §5.2）——防恶意巨 length 字段无界分配。
const MAX_TLS_RECORD: usize = 16640;

/// 一条 REALITY app 流：在 TCP 上跑 TLS 1.3 app-data record AEAD + VLESS 响应头 strip（刀8 T8，spec §4）。
/// 中文要点：
/// - **读**：逐条 outer record（5B 头 + 密文）解密 → 内层 demux：`0x17` app data（首读经 VlessResponseStripper
///   **strip VLESS 响应头**——`2+addons_len`，动态、可跨 record 累积，互通-critical，brief §1.4）上抛给消费者；
///   `0x16` post-handshake（NewSessionTicket/KeyUpdate）解密后**丢弃**；`0x15` alert → EOF。
/// - **写**：明文切 ≤16384B/record，`send_keys.seal(0x17,..)` 封 → 写出（带 write_pending 背压）。
/// - 读/写各用独立的 read/write 半（`into_split`），避免读阻塞写。
pub struct RealityStream<R, W> {
    read_half: R,
    write_half: W,
    recv_keys: RecordKeys,
    send_keys: RecordKeys,
    /// 未解密的 outer record 字节累积（含握手结束后多读的 leftover）。
    read_raw: BytesMut,
    /// 已解密但 VLESS 响应头尚未剥完的暂存（剥完后清空、内容转入 plaintext_out）。
    staging: BytesMut,
    /// 已解密 + 已剥响应头、待消费者读取的明文。
    plaintext_out: BytesMut,
    /// 已封装待写出的密文（背压：未排空前不接受新明文）。
    write_pending: BytesMut,
    resp: VlessResponseStripper,
    /// server→client application_traffic_secret_N（KeyUpdate 时就地轮到 N+1，派新 recv key/iv）。刀10/F5。
    server_ap_secret: [u8; 32],
    /// client→server application_traffic_secret_N（收 update_requested 时轮到 N+1，派新 send key/iv）。刀10/F5。
    client_ap_secret: [u8; 32],
}

/// 从（已轮换的）application_traffic_secret 派一个新 `RecordKeys`（RFC 8446 §7.3）：
/// key=ExpandLabel(secret,"key","",16)、iv=ExpandLabel(secret,"iv","",12)，新实例 seq 天然归 0。
fn record_keys_from_secret(secret: &[u8; 32]) -> RecordKeys {
    let key: [u8; 16] = expand_label(secret, "key", b"", 16).try_into().expect("key 16B");
    let iv: [u8; 12] = expand_label(secret, "iv", b"", 12).try_into().expect("iv 12B");
    RecordKeys::new(&key, &iv)
}

/// 一条 record 解码后的去向。
enum Decoded {
    Data(Vec<u8>), // 内层 0x17 app data
    Drop,          // 内层 0x16 post-handshake（NST/KeyUpdate）/ 其它 → 丢
    Eof,           // 内层 0x15 alert（close_notify 等）→ 视为流结束
}

impl<R, W> RealityStream<R, W> {
    /// 握手完成后构造：`leftover` = 握手阶段多读的未消费 outer record 字节；
    /// `{server,client}_ap_secret` = application_traffic_secret_0（KeyUpdate 轮换起点，刀10/F5）。
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        read_half: R,
        write_half: W,
        recv_keys: RecordKeys,
        send_keys: RecordKeys,
        leftover: BytesMut,
        server_ap_secret: [u8; 32],
        client_ap_secret: [u8; 32],
    ) -> Self {
        Self {
            read_half,
            write_half,
            recv_keys,
            send_keys,
            read_raw: leftover,
            staging: BytesMut::new(),
            plaintext_out: BytesMut::new(),
            write_pending: BytesMut::new(),
            resp: VlessResponseStripper::new(),
            server_ap_secret,
            client_ap_secret,
        }
    }

    /// 从 read_raw 解一条完整 record（不足返回 None）。AEAD/超长失败 → io::Error。
    fn decode_one(&mut self) -> io::Result<Option<Decoded>> {
        if self.read_raw.len() < 5 {
            return Ok(None);
        }
        let header: [u8; 5] = self.read_raw[..5].try_into().expect("≥5B");
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if len > MAX_TLS_RECORD {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "REALITY record 超长"));
        }
        if self.read_raw.len() < 5 + len {
            return Ok(None);
        }
        let _ = self.read_raw.split_to(5);
        let payload = self.read_raw.split_to(len);
        let (inner, content) = self
            .recv_keys
            .open(&header, &payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        let decoded = match inner {
            0x17 => Decoded::Data(content),
            0x15 => {
                // alert `[level, description]`：close_notify(1,0) → 干净 EOF；fatal/其它 → loud-fail（L1）。
                // 不区分会把 server 主动拒绝/目标侧失败当正常关闭，返回不完整响应而无错误信号。
                match (content.first().copied(), content.get(1).copied()) {
                    (Some(1), Some(0)) => Decoded::Eof,
                    (level, desc) => {
                        return Err(io::Error::new(
                            io::ErrorKind::ConnectionAborted,
                            format!("REALITY 收到 TLS alert level={level:?} desc={desc:?}（server 拒绝/错误）"),
                        ));
                    }
                }
            }
            0x16 => {
                // post-handshake handshake message：按 `1B type + 3B u24 len` 切分（同握手期重组纪律；RFC 8446 §5.1
                // 允许同向多条 message 合并进一条 record，如 [NewSessionTicket][KeyUpdate]）。仅 KeyUpdate(0x18) →
                // 正确轮换（刀10/F5，§4.6.3/§7.2）；其余（NST 0x04 等无密钥影响）→ 丢。轮换后本条无 app data 上抛
                // （Drop）；回发 reply（若有）已入 write_pending，poll_read 顶部机会性 flush 出去。
                // ⚠️ **只看首字节会漏**：若 NST 在前、KeyUpdate 合并在后，仅判 content.first() 会把整条丢弃 →
                // 接收方向不轮换 → 后续 record bad-decrypt 掉线（正是本刀要消解的失效）。故必须逐 message 切。
                // 已知限制（稳定优先）：**跨 record 分片**的 post-handshake message 不重组——KeyUpdate 恒 5B 不分片；
                // 大 NST 即便被分片也只是丢弃，不影响密钥轮换正确性。
                let mut rest: &[u8] = &content;
                while rest.len() >= 4 {
                    let msg_len = ((rest[1] as usize) << 16) | ((rest[2] as usize) << 8) | rest[3] as usize;
                    let total = 4 + msg_len;
                    if rest.len() < total {
                        break; // 半条（跨 record 分片）：本条不处理（见上「已知限制」）。
                    }
                    if rest[0] == 0x18 {
                        self.on_key_update(&rest[..total])?;
                    }
                    rest = &rest[total..];
                }
                Decoded::Drop
            }
            _ => Decoded::Drop,
        };
        Ok(Some(decoded))
    }

    /// 处理 post-handshake TLS 1.3 KeyUpdate（刀10/F5，RFC 8446 §4.6.3/§7.2/§5.3；规范见 spec §2，V1 核验）。
    /// `msg` = 解密后内层 handshake message（已确认 `msg[0]==0x18`）：`[0x18, len_u24, request_update]`。
    /// 中文铁律：
    /// - 步骤 A 总是先轮换**接收**方向（对端已轮它的发送密钥，否则后续 record bad-decrypt）。
    /// - 仅 `update_requested(1)` 才回发 + 轮**发送**方向；**B1（旧 send key 封 reply）必先于 B2（轮 send 密钥）**
    ///   （先换再封 = 对端用旧 key 解新 key record → 掉线）。回发的 request_update 恒 0（防环）。
    /// - 非法 `request_update`（非 0/1）→ Err（前置校验，零 mutation）。换密钥后 record seq 由新实例天然归 0。
    fn on_key_update(&mut self, msg: &[u8]) -> io::Result<()> {
        // 帧校验（先于任何 mutation）：调用方已按 message 边界切出 → KeyUpdate 必恰 5 字节（18 00 00 01 RU）。
        if msg.len() != 5 || msg[1..4] != [0x00, 0x00, 0x01] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REALITY KeyUpdate 帧非法（须 18 00 00 01 RU）",
            ));
        }
        let request_update = msg[4];
        if request_update > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "REALITY KeyUpdate request_update 非法值（须 0/1）",
            ));
        }
        // 步骤 A：总是先轮换“接收”方向。
        self.server_ap_secret = next_application_traffic_secret(&self.server_ap_secret);
        self.recv_keys = record_keys_from_secret(&self.server_ap_secret);
        // 步骤 B：仅 update_requested(1) 才回发 + 轮“发送”方向。
        if request_update == 1 {
            // B1：必须用“旧”send key（旧 seq）封装回发 KeyUpdate(update_not_requested=0)，入 write_pending。
            // 不对 write_pending 设独立上限（对抗式 review nit）：reply 仅 ~26B/条，要无界增长需「TCP 写满 +
            // 消费者持续读 + 服务端用正确轮换密钥持续刷 KeyUpdate」三者同时成立——REALITY 出口是用户自有可信端，
            // 出模型；且 write_pending 与 poll_write 的 app-data 背压共用，硬上限会误伤正常大下载背压（footgun）。
            let reply = self.send_keys.seal(0x16, &[0x18, 0x00, 0x00, 0x01, 0x00]);
            self.write_pending.extend_from_slice(&reply);
            // B2：封装完毕“才”轮发送方向（铁律 B1 必先于 B2）。
            self.client_ap_secret = next_application_traffic_secret(&self.client_ap_secret);
            self.send_keys = record_keys_from_secret(&self.client_ap_secret);
        }
        Ok(())
    }

    /// 吸收一条 app-data 内容：首读经 VLESS 响应头 strip（可跨 record 累积），剥完后转入 plaintext_out。
    /// 中文要点（L7）：响应头剥完后短路直接入 plaintext_out，省每条 record 经 staging 的双拷贝。
    fn absorb_app(&mut self, content: &[u8]) {
        if self.resp.is_stripped() {
            self.plaintext_out.extend_from_slice(content);
            return;
        }
        self.staging.extend_from_slice(content);
        if self.resp.strip(&mut self.staging) {
            self.plaintext_out.extend_from_slice(&self.staging);
            self.staging.clear();
        }
    }

    /// 排空 write_pending（背压核心）。Pending=对端 TCP 写满。
    fn poll_flush_pending(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>>
    where
        W: AsyncWrite + Unpin,
    {
        while !self.write_pending.is_empty() {
            match Pin::new(&mut self.write_half).poll_write(cx, &self.write_pending) {
                Poll::Ready(Ok(0)) => {
                    return Poll::Ready(Err(io::Error::new(io::ErrorKind::WriteZero, "REALITY 写 0 字节")));
                }
                Poll::Ready(Ok(k)) => {
                    let _ = self.write_pending.split_to(k);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
        Poll::Ready(Ok(()))
    }
}

// 中文要点（刀10/F5）：`W: AsyncWrite` bound——KeyUpdate(update_requested) 须在读路径上**回发** reply
// （入 write_pending），故 poll_read 顶部机会性 flush write_pending。prod=OwnedWriteHalf、test=WriteHalf 均可写。
impl<R: AsyncRead + Unpin, W: AsyncWrite + Unpin> AsyncRead for RealityStream<R, W> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            // 0. 机会性排空待写密文（含 KeyUpdate 回发 reply）：best-effort，Pending 不阻塞读，错误传播。
            //    常态 write_pending 空 → is_empty 快路 no-op；非空（reply/背压）→ 尽量写出，剩余留待下次。
            if !this.write_pending.is_empty() {
                let _ = this.poll_flush_pending(cx)?;
            }
            // 1. 先把已就绪明文交给消费者。
            if !this.plaintext_out.is_empty() {
                let n = buf.remaining().min(this.plaintext_out.len());
                buf.put_slice(&this.plaintext_out[..n]);
                let _ = this.plaintext_out.split_to(n);
                return Poll::Ready(Ok(()));
            }
            // 2. 尝试从 read_raw 解一条 record。
            match this.decode_one()? {
                Some(Decoded::Data(content)) => {
                    this.absorb_app(&content);
                    continue;
                }
                Some(Decoded::Drop) => continue,
                Some(Decoded::Eof) => return Poll::Ready(Ok(())), // alert → EOF（0 填充）
                None => {}
            }
            // 3. read_raw 不足一条 → 读更多。
            let mut tmp = [0u8; 8192];
            let mut rb = ReadBuf::new(&mut tmp);
            match Pin::new(&mut this.read_half).poll_read(cx, &mut rb) {
                Poll::Ready(Ok(())) => {
                    let filled = rb.filled();
                    if filled.is_empty() {
                        // 底层 EOF。read_raw 空（裸 FIN 在 record 边界）→ 干净 EOF；非空（半条 record 残留）→
                        // 截断（M4，RFC 8446 §6.1）。与 RecordReader::next 截断纪律对齐，不静默丢字节。
                        if this.read_raw.is_empty() {
                            return Poll::Ready(Ok(()));
                        }
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "REALITY 流在 record 中途被截断",
                        )));
                    }
                    this.read_raw.extend_from_slice(filled);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl<R: Unpin, W: AsyncWrite + Unpin> AsyncWrite for RealityStream<R, W> {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?; // 背压：旧密文未排空不接受新明文
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let n = buf.len().min(MAX_PLAINTEXT);
        let rec = this.send_keys.seal(0x17, &buf[..n]);
        this.write_pending.extend_from_slice(&rec);
        let _ = this.poll_flush_pending(cx)?; // best-effort 写出，剩余留待下次
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?;
        Pin::new(&mut this.write_half).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        ready!(this.poll_flush_pending(cx))?;
        Pin::new(&mut this.write_half).poll_shutdown(cx)
    }
}

/// 解析 REALITY `public_key`（pbk）字符串 → 32B（刀8 T7，brief §1.5 / 风险 9）。
/// 中文要点：sing-box / 当前 Xray 的 `public_key` 是 Go `base64.RawURLEncoding`（URL-safe、无 `=`，32B→43 字符）。
/// 优先 base64url，回退 std（兼容历史/std 变体）；**解码后强断言恰 32B 否则 loud-fail**——错编码/错长度 →
/// AuthKey 错 → session_id 服务端解不开 → **静默回落 decoy**（看似连上 TLS 实则 REALITY auth 必败），故此处从严。
pub fn parse_pbk(s: &str) -> Result<[u8; 32], ClientError> {
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let s = s.trim();
    let decoded = URL_SAFE_NO_PAD
        .decode(s)
        .or_else(|_| URL_SAFE.decode(s))
        .or_else(|_| STANDARD_NO_PAD.decode(s))
        .or_else(|_| STANDARD.decode(s))
        .map_err(|_| ClientError::Reality(format!("REALITY public_key 非合法 base64: {s:?}")))?;
    decoded.try_into().map_err(|v: Vec<u8>| {
        ClientError::Reality(format!(
            "REALITY public_key 解码后 {} 字节，须恰 32（错编码 → AuthKey 错 → 静默回落 decoy）",
            v.len()
        ))
    })
}

/// 解析带连字符的 UUID 字符串 → 16B（仿 tuic::parse_uuid，刀8 grill 裁决 e：暂内联复制不提 shared）。
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// REALITY 客户端配置（单一事实源；桌面从 env 加载）。
/// 中文要点：`uuid`/`pbk`/`short_id` 经自定义 Debug **脱敏**，绝不随日志泄漏（grill 裁决 e）。
#[derive(Clone)]
pub struct RealityClientConfig {
    /// VPS 端点 `host:port`（实连地址，TcpStream::connect）。
    pub server: String,
    /// VLESS UUID（16B）。
    pub uuid: [u8; 16],
    /// 服务端 REALITY 静态公钥（pbk，32B）。
    pub pbk: [u8; 32],
    /// short_id（8B 零填充）。
    pub short_id: [u8; 8],
    /// 借用站 SNI（须 ∈ 服务端 serverNames；推荐与 handshake.server 同域）。
    pub sni: String,
}

impl std::fmt::Debug for RealityClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RealityClientConfig")
            .field("server", &self.server)
            .field("uuid", &"<redacted>")
            .field("pbk", &"<redacted>")
            .field("short_id", &"<redacted>")
            .field("sni", &self.sni)
            .finish()
    }
}

impl RealityClientConfig {
    /// 从可选字符串源构建（server/uuid/pbk/sni 必填；short_id 可空=全零）。
    pub fn from_sources(
        server: Option<&str>,
        uuid: Option<&str>,
        pbk: Option<&str>,
        short_id: Option<&str>,
        sni: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server = server
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ClientError::Reality("MINI_VPN_REALITY_SERVER 必填".into()))?
            .to_string();
        let uuid = parse_uuid(uuid.ok_or_else(|| ClientError::Reality("MINI_VPN_REALITY_UUID 必填".into()))?)
            .ok_or_else(|| ClientError::Reality("MINI_VPN_REALITY_UUID 非法（须 RFC4122 UUID）".into()))?;
        let pbk = parse_pbk(pbk.ok_or_else(|| ClientError::Reality("MINI_VPN_REALITY_PBK 必填".into()))?)?;
        let short_id = parse_short_id(short_id.unwrap_or(""))?;
        let sni = sni
            .filter(|s| !s.is_empty())
            .ok_or_else(|| ClientError::Reality("MINI_VPN_REALITY_SNI 必填（借用站域名）".into()))?
            .to_string();
        Ok(Self { server, uuid, pbk, short_id, sni })
    }

    /// 从进程环境读取（`MINI_VPN_REALITY_*`）。
    pub fn from_env() -> Result<Self, ClientError> {
        let g = |k: &str| std::env::var(k).ok();
        Self::from_sources(
            g("MINI_VPN_REALITY_SERVER").as_deref(),
            g("MINI_VPN_REALITY_UUID").as_deref(),
            g("MINI_VPN_REALITY_PBK").as_deref(),
            g("MINI_VPN_REALITY_SHORT_ID").as_deref(),
            g("MINI_VPN_REALITY_SNI").as_deref(),
        )
    }
}

/// REALITY 第二 Transport 上游：每条 TCP 新建一次完整 REALITY 握手 + VLESS 请求（无连接复用，刀8）。
pub struct RealityUpstream {
    cfg: RealityClientConfig,
}

impl RealityUpstream {
    pub fn new(cfg: RealityClientConfig) -> Self {
        Self { cfg }
    }

    pub fn from_env() -> Result<Self, ClientError> {
        Ok(Self::new(RealityClientConfig::from_env()?))
    }

    /// 在已连流上跑完整 REALITY 握手 + 发 VLESS 请求，返回 app 密钥/leftover（`HandshakeOutput`）。
    /// 中文要点：取 `&mut S` 不消费流——调用方握手后再拆分（prod=`TcpStream::into_split` lock-free；
    /// 测试=`tokio::io::split` over duplex），故抽成泛型便于离线 loopback 测试。
    async fn establish<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut S,
        target: &TargetAddr,
    ) -> Result<handshake::HandshakeOutput, ClientError> {
        use rand::RngCore;
        // 1. 客户端临时 X25519 密钥 + ClientHello.random。
        let (sk_c, pk_c) = generate_ephemeral_keypair();
        let mut random = [0u8; 32];
        rand::rngs::OsRng.fill_bytes(&mut random);
        // 2. REALITY AuthKey = HKDF(x25519(client 临时, server **静态** pbk), random)。
        let auth_key = derive_auth_key(&x25519_shared_secret(sk_c, self.cfg.pbk), &random);
        // 3. 建带 REALITY auth 的 ClientHello。
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as u32)
            .unwrap_or(0);
        let ch = build_authed_client_hello(&AuthedClientHelloParams {
            server_name: &self.cfg.sni,
            key_share: pk_c,
            random,
            auth_key,
            short_id: self.cfg.short_id,
            timestamp,
        });
        let expected_session_id = authed_session_id(&ch).to_vec();
        // 4. 跑握手，注入 verify_server_cert 做 REALITY auth 决策。
        let input = HandshakeInput { client_hello: ch, client_eph_secret: sk_c, expected_session_id };
        let mut out = handshake::drive(stream, input, move |cert_msg| {
            let (pk, sig) = extract_ed25519_pubkey_and_sig(cert_msg)?;
            if verify_server_cert(&auth_key, &pk, &sig) {
                Ok(())
            } else {
                Err(ClientError::Reality(
                    "REALITY 服务端证书 HMAC 校验失败（疑被回落到 decoy/SNI 不在 serverNames/pbk 错）".into(),
                ))
            }
        })
        .await?;
        // REALITY auth 决策已通过（drive 成功 ⟺ verify_server_cert==true；否则上面 `?` 已返回）。
        // 这行是真出口 acceptance 的核心证据（**非** session_id echo 充数，见 ADR-0009/0010）。
        println!("🔐 REALITY 握手成功（证书 HMAC 校验通过）→ {}", target.to_wire_string());
        // 5. 发 VLESS 请求（握手后第一条 app record，send_keys seq 0）。
        let vless = out.send_keys.seal(0x17, &encode_vless_request(&self.cfg.uuid, VLESS_CMD_TCP, target));
        stream.write_all(&vless).await.map_err(|e| ClientError::Reality(format!("写 VLESS 请求: {e}")))?;
        stream.flush().await.map_err(|e| ClientError::Reality(format!("flush VLESS 请求: {e}")))?;
        Ok(out)
    }
}

/// REALITY connect + 握手超时（H2 止血）。`run_event_loop` 是单任务顺序循环，`open_tcp` 在主循环 inline
/// await——零超时下慢/半开 server 会 stall **整个**事件循环（所有 flow 饿死）。10s 封顶把它降级为单 flow 失败。
/// 中文要点：根治（把握手 spawn 出主循环并发化）是 M3，留刀9；本超时是本刀止血。
const REALITY_HANDSHAKE_TIMEOUT_SECS: u64 = 10;

#[async_trait::async_trait]
impl ProxyUpstream for RealityUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        let timeout = std::time::Duration::from_secs(REALITY_HANDSHAKE_TIMEOUT_SECS);
        let mut stream = tokio::time::timeout(timeout, TcpStream::connect(&self.cfg.server))
            .await
            .map_err(|_| ClientError::Reality(format!("TCP 连 REALITY 出口 {} 超时", self.cfg.server)))?
            .map_err(|e| ClientError::Reality(format!("TCP 连 REALITY 出口 {} 失败: {e}", self.cfg.server)))?;
        let out = tokio::time::timeout(timeout, self.establish(&mut stream, target))
            .await
            .map_err(|_| ClientError::Reality("REALITY 握手超时（慢/半开 server）".into()))??;
        // prod 用 into_split（lock-free 独立读写半），喂给同一套双向中继泵。
        let (rh, wh) = stream.into_split();
        Ok(Box::new(RealityStream::new(
            rh,
            wh,
            out.recv_keys,
            out.send_keys,
            out.leftover,
            out.s_ap_secret,
            out.c_ap_secret,
        )))
    }

    /// REALITY 每条 TCP 一次完整多-RTT 手写 TLS 握手——**不廉价**，主循环须 spawn 出去并发化（刀9 M3），
    /// 否则一条慢握手 stall 整个单任务 select 循环（H2 的 10s 超时只是止血，根治靠并发化）。
    fn open_is_cheap(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for RealityUpstream {
    /// REALITY 是 **TCP-only**：UDP **no-op 静默丢**（UDP-over-VLESS 是刀9；force-reality 下 UDP 不可用符合预期）。
    async fn send_udp(&self, _datagram: Vec<u8>) {}
}

/// 直连探针（诊断用，**无 TUN/无 sudo**）：用 `MINI_VPN_REALITY_*` env 对 `target` 跑一次 REALITY 握手 +
/// 一个明文 HTTP GET，逐步打印卡点。把 REALITY 协议/服务端问题从 TUN/主循环 inline-blocking/curl 突发里隔离出来。
/// 中文要点：`target` 用**明文 HTTP 端口**（如 `example.com:80`）——探针发裸 HTTP，目标须能直接收 HTTP（:443 会要 TLS）。
/// 建议配 `MINI_VPN_REALITY_DEBUG=1` 看握手逐步日志。
pub async fn reality_probe(target: &str) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let cfg = match RealityClientConfig::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[probe] ❌ 配置错误: {e:?}");
            return;
        }
    };
    eprintln!("[probe] REALITY 出口 server={} sni={} → target={target}", cfg.server, cfg.sni);
    let t = match TargetAddr::parse(target) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[probe] ❌ target 非法: {e:?}");
            return;
        }
    };
    let host = match &t {
        TargetAddr::DomainPort { host, .. } => host.clone(),
        TargetAddr::IpPort(a) => a.ip().to_string(),
    };
    let up = RealityUpstream::new(cfg);
    match up.open_tcp(&t).await {
        Ok(mut s) => {
            eprintln!("[probe] ✅ open_tcp 成功（REALITY 握手 + VLESS 请求完成）→ 发 HTTP GET…");
            let req = format!(
                "GET / HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\nUser-Agent: mini-vpn-probe\r\n\r\n"
            );
            if let Err(e) = s.write_all(req.as_bytes()).await {
                eprintln!("[probe] ❌ 写请求失败: {e}");
                return;
            }
            let mut buf = vec![0u8; 4096];
            match s.read(&mut buf).await {
                Ok(0) => eprintln!("[probe] ⚠️ 读到 EOF（隧道通但目标无响应）"),
                Ok(n) => eprintln!(
                    "[probe] ✅ 经 REALITY 隧道收到 {n} 字节响应（前 300）:\n{}",
                    String::from_utf8_lossy(&buf[..n.min(300)])
                ),
                Err(e) => eprintln!("[probe] ❌ 读响应失败: {e}"),
            }
        }
        Err(e) => eprintln!("[probe] ❌ open_tcp 失败: {e:?}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// sing-box 风 43 字符 base64url（无 pad）→ bytes 0..31。
    #[test]
    fn pbk_base64url_singbox_style() {
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"; // base64url(bytes 0..31)
        assert_eq!(s.len(), 43, "32B base64url 无 pad = 43 字符");
        let expected: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(parse_pbk(s).unwrap(), expected);
    }

    /// std base64（含 `+`/`/`，url 字母表不接受）→ 走 std 回退。
    #[test]
    fn pbk_std_base64_fallback() {
        let s = "+//7//v/+//7//v/+//7//v/+//7//v/+//7//v/+/8"; // base64std(bytes [0xfb,0xff]×16)
        let expected: [u8; 32] = std::array::from_fn(|i| if i % 2 == 0 { 0xfb } else { 0xff });
        assert_eq!(parse_pbk(s).unwrap(), expected, "std 变体经回退解码");
    }

    /// 带 `=` padding 的 base64url 也接受。
    #[test]
    fn pbk_padded_accepted() {
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";
        let expected: [u8; 32] = std::array::from_fn(|i| i as u8);
        assert_eq!(parse_pbk(s).unwrap(), expected);
    }

    /// 解码后非 32B（这里 31B）→ loud-fail。
    #[test]
    fn pbk_wrong_length_rejected() {
        // base64url(bytes 0..30) = 31B，解码成功但长度错。
        let s = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGx0e"; // 30 bytes
        assert!(parse_pbk(s).is_err(), "非 32B 须拒");
    }

    /// 64-char hex（误把 short_id/其它 hex 当 pbk）→ base64 解码成 48B → 非 32B → 拒。
    #[test]
    fn pbk_hex_rejected() {
        let s = "df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0b";
        assert!(parse_pbk(s).is_err(), "64-char hex 解出 48B ≠ 32 → 拒");
    }

    /// 非 base64 垃圾 → 拒（不 panic）。
    #[test]
    fn pbk_garbage_rejected() {
        assert!(parse_pbk("!!!not base64!!!").is_err());
        assert!(parse_pbk("").is_err(), "空串解出 0B ≠ 32");
    }

    // ---- T8 RealityStream ----
    use crate::reality::handshake::RecordReader;
    use crate::reality::record::RecordKeys;
    use crate::reality::testutil::hex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// 读路径：首读剥 VLESS 响应头(`00 00`) + post-handshake NST(inner 0x16) 丢弃 + 跨 record app data 拼接。
    /// 关键：NST record **也被 open**（recv seq 推进），故后续 app record seq 仍对齐。
    #[tokio::test]
    async fn realitystream_read_strips_vless_and_drops_nst() {
        let (recv_k, recv_iv) = ([0x11u8; 16], [0x22u8; 12]);
        let (send_k, send_iv) = ([0x33u8; 16], [0x44u8; 12]);
        let (client_end, mut server_end) = tokio::io::duplex(65536);
        let (cr, cw) = tokio::io::split(client_end);
        let mut stream = RealityStream::new(
            cr,
            cw,
            RecordKeys::new(&recv_k, &recv_iv),
            RecordKeys::new(&send_k, &send_iv),
            BytesMut::new(),
            [0u8; 32], // server_ap_secret（本测试不触发 KeyUpdate）
            [0u8; 32], // client_ap_secret
        );

        // server 用与 stream.recv 同 key/iv 封（按 seq 0,1,2 顺序）。
        let mut srv = RecordKeys::new(&recv_k, &recv_iv);
        let mut r1 = vec![0x00, 0x00]; // VLESS 响应头（空 addons）
        r1.extend_from_slice(b"hello");
        server_end.write_all(&srv.seal(0x17, &r1)).await.unwrap(); // seq0
        server_end.write_all(&srv.seal(0x16, &[0x04, 0, 0, 1, 0])).await.unwrap(); // seq1 NST → 丢
        server_end.write_all(&srv.seal(0x17, b" world")).await.unwrap(); // seq2
        server_end.flush().await.unwrap();

        let mut got = vec![0u8; 11];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"hello world", "剥响应头 + 丢 NST + 拼接 app data");
    }

    /// 写路径：>16384B 明文分块成多条 0x17 record；server 用 send key 还原原字节。
    #[tokio::test]
    async fn realitystream_write_chunks_and_roundtrips() {
        let (send_k, send_iv) = ([0x33u8; 16], [0x44u8; 12]);
        let (client_end, mut server_end) = tokio::io::duplex(1 << 20);
        let (cr, cw) = tokio::io::split(client_end);
        let mut stream = RealityStream::new(
            cr,
            cw,
            RecordKeys::new(&[0x11; 16], &[0x22; 12]),
            RecordKeys::new(&send_k, &send_iv),
            BytesMut::new(),
            [0u8; 32], // server_ap_secret（本测试不触发 KeyUpdate）
            [0u8; 32], // client_ap_secret
        );

        let payload = vec![0xABu8; 20000]; // > 16384 → 须分 2 record
        stream.write_all(&payload).await.unwrap();
        stream.flush().await.unwrap();
        drop(stream); // 关写半，server 读到 EOF 后停

        let mut srv = RecordKeys::new(&send_k, &send_iv);
        let mut rr = RecordReader::new();
        let mut got = Vec::new();
        let mut records = 0;
        while got.len() < payload.len() {
            let (ct, hdr, pl) = rr.next(&mut server_end).await.unwrap();
            assert_eq!(ct, 0x17);
            let (inner, content) = srv.open(&hdr, &pl).unwrap();
            assert_eq!(inner, 0x17);
            got.extend_from_slice(&content);
            records += 1;
        }
        assert_eq!(got, payload, "分块往返字节级一致");
        assert!(records >= 2, "20000B 应分 ≥2 条 record（每条 ≤16384 明文），实得 {records}");
    }

    // ---- T9 RealityClientConfig + open_tcp loopback ----

    /// from_sources 解析 + 脱敏 Debug（uuid/pbk/short_id 不泄漏）。
    #[test]
    fn config_from_sources_and_redacted_debug() {
        let cfg = RealityClientConfig::from_sources(
            Some("1.2.3.4:443"),
            Some("12345678-1234-1234-1234-123456789abc"),
            Some("AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"), // base64url(bytes 0..31)
            Some("01ab"),
            Some("www.example.com"),
        )
        .unwrap();
        assert_eq!(cfg.server, "1.2.3.4:443");
        assert_eq!(cfg.pbk, std::array::from_fn::<u8, 32, _>(|i| i as u8));
        assert_eq!(&cfg.short_id[..2], &[0x01, 0xab]);
        assert_eq!(cfg.sni, "www.example.com");
        let dbg = format!("{cfg:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("123456789abc"), "uuid 不泄漏");
        assert!(dbg.contains("www.example.com"), "sni 可见（非敏感）");
    }

    /// 缺字段 / 非法 uuid / 非法 pbk / 缺 sni → Err。
    #[test]
    fn config_missing_or_invalid_fields_err() {
        let uuid = "12345678-1234-1234-1234-123456789abc";
        let pbk = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8";
        assert!(RealityClientConfig::from_sources(None, Some(uuid), Some(pbk), None, Some("s")).is_err(), "缺 server");
        assert!(RealityClientConfig::from_sources(Some("h:1"), Some("bad-uuid"), Some(pbk), None, Some("s")).is_err(), "非法 uuid");
        assert!(RealityClientConfig::from_sources(Some("h:1"), Some(uuid), Some("short"), None, Some("s")).is_err(), "非法 pbk");
        assert!(RealityClientConfig::from_sources(Some("h:1"), Some(uuid), Some(pbk), None, None).is_err(), "缺 sni");
    }

    // 真 ed25519 自签证书 fixture（同 cert.rs；用于 loopback server 模拟器构 Certificate）。
    const ED25519_CERT_DER: &str = "308201423081f5a0030201020214650b853b02ac2a3e0f05b44644695bcaeec01154300506032b657030173115301306035504030c0c7265616c6974792d74656d70301e170d3236303632333134333935305a170d3336303632303134333935305a30173115301306035504030c0c7265616c6974792d74656d70302a300506032b6570032100df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0ba3533051301d0603551d0e041604146657983d0890461f3d4c21d1d6af8b1626144811301f0603551d230418301680146657983d0890461f3d4c21d1d6af8b1626144811300f0603551d130101ff040530030101ff300506032b6570034100ab056a660a043ddb36de3bd9031d346142dceb6ae874fc45219c33c6a5b57c7b9c196f1aad5fb124ec84697377bb15f03b44d2a2c63dc3a9589002dfc23a570f";
    const ED25519_PUBKEY: &str = "df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0b";

    /// 从 ClientHello message walk 出 X25519 key_share（32B）。
    fn find_ch_keyshare(ch: &[u8]) -> [u8; 32] {
        let mut p = 4 + 2 + 32; // handshake hdr + legacy_version + random
        let sid_len = ch[p] as usize;
        p += 1 + sid_len;
        let cipher_len = u16::from_be_bytes([ch[p], ch[p + 1]]) as usize;
        p += 2 + cipher_len;
        let comp_len = ch[p] as usize;
        p += 1 + comp_len;
        p += 2; // ext_len
        let exts = &ch[p..];
        let mut i = 0;
        while i + 4 <= exts.len() {
            let t = u16::from_be_bytes([exts[i], exts[i + 1]]);
            let l = u16::from_be_bytes([exts[i + 2], exts[i + 3]]) as usize;
            let body = &exts[i + 4..i + 4 + l];
            if t == 0x0033 {
                return body[6..38].try_into().unwrap(); // list_len(2)+group(2)+len(2)+pubkey
            }
            i += 4 + l;
        }
        panic!("ClientHello 无 key_share");
    }

    /// 构 ServerHello message：echo client session_id、cipher 0x1301、supported_versions 1.3、key_share X25519。
    fn build_sh(server_random: [u8; 32], client_sid: &[u8], server_ks: [u8; 32]) -> Vec<u8> {
        let mut ext = vec![0x00, 0x2b, 0x00, 0x02, 0x03, 0x04]; // supported_versions=0x0304
        ext.extend_from_slice(&[0x00, 0x33, 0x00, 0x24, 0x00, 0x1d, 0x00, 0x20]); // key_share X25519
        ext.extend_from_slice(&server_ks);
        let mut body = vec![0x03, 0x03];
        body.extend_from_slice(&server_random);
        body.push(client_sid.len() as u8);
        body.extend_from_slice(client_sid);
        body.extend_from_slice(&[0x13, 0x01]); // cipher 0x1301
        body.push(0x00); // compression
        body.extend_from_slice(&(ext.len() as u16).to_be_bytes());
        body.extend_from_slice(&ext);
        let l = body.len();
        let mut msg = vec![0x02, (l >> 16) as u8, (l >> 8) as u8, l as u8];
        msg.extend_from_slice(&body);
        msg
    }

    /// 构 REALITY Certificate(0x0b) message：ed25519 cert 末 64B 换成 HMAC-SHA512(auth_key, cert_pubkey)。
    fn build_reality_cert_msg(cert_der: &[u8], cert_pubkey: &[u8], auth_key: &[u8; 32]) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        use sha2::Sha512;
        let mut mac = Hmac::<Sha512>::new_from_slice(auth_key).unwrap();
        mac.update(cert_pubkey);
        let hmac = mac.finalize().into_bytes();
        let mut der = cert_der.to_vec();
        let n = der.len();
        der[n - 64..].copy_from_slice(&hmac);
        let u24 = |n: usize| [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        let mut list = Vec::new();
        list.extend_from_slice(&u24(der.len()));
        list.extend_from_slice(&der);
        list.extend_from_slice(&[0, 0]); // entry ext len
        let mut bdy = vec![0u8];
        bdy.extend_from_slice(&u24(list.len()));
        bdy.extend_from_slice(&list);
        let mut msg = vec![0x0b];
        msg.extend_from_slice(&u24(bdy.len()));
        msg.extend_from_slice(&bdy);
        msg
    }

    /// 服务端 REALITY 握手（CH→SH/flight→验 client Finished），返回 `RecordReader` + app keys。
    /// `reality_server_sim` 与 KeyUpdate loopback 共用此握手段，握手后各自续读（VLESS / KeyUpdate）。
    async fn reality_sim_handshake<S: AsyncRead + AsyncWrite + Unpin>(
        io: &mut S,
        server_static_sk: [u8; 32],
    ) -> (RecordReader, crate::reality::key_schedule::AppKeys) {
        use crate::reality::auth::{derive_auth_key, generate_ephemeral_keypair, x25519_shared_secret};
        use crate::reality::key_schedule::{
            compute_finished_verify_data, derive_application_keys, derive_handshake_keys, transcript_hash,
        };
        let mut rr = RecordReader::new();
        let (_ct, _h, ch) = rr.next(io).await.unwrap(); // client CH
        let (ct_ccs, _h2, _b) = rr.next(io).await.unwrap(); // client CCS
        assert_eq!(ct_ccs, 0x14);

        let client_random: [u8; 32] = ch[6..38].try_into().unwrap();
        let client_sid = ch[39..71].to_vec();
        let client_ks = find_ch_keyshare(&ch);
        // REALITY AuthKey（服务端视角）：x25519(server 静态私钥, client 临时 pub)。
        let auth_key = derive_auth_key(&x25519_shared_secret(server_static_sk, client_ks), &client_random);

        let (sk_se, pk_se) = generate_ephemeral_keypair();
        let sh = build_sh([0x55u8; 32], &client_sid, pk_se);
        let ecdhe = x25519_shared_secret(sk_se, client_ks);
        let hs = derive_handshake_keys(&ecdhe, &ch, &sh).unwrap();

        let ee = hex("08 00 00 02 00 00"); // EncryptedExtensions（空）
        let cert_msg = build_reality_cert_msg(&hex(ED25519_CERT_DER), &hex(ED25519_PUBKEY), &auth_key);
        let mut cv = vec![0x0f, 0x00, 0x00, 0x44, 0x08, 0x07, 0x00, 0x40]; // CertVerify（ed25519 scheme + dummy sig）
        cv.extend_from_slice(&[0u8; 64]);
        let th_cv = transcript_hash(&[&ch, &sh, &ee, &cert_msg, &cv]);
        let mut sfin = vec![0x14, 0x00, 0x00, 0x20];
        sfin.extend_from_slice(&compute_finished_verify_data(&hs.s_hs_secret, &th_cv));
        let mut flight = Vec::new();
        for m in [&ee, &cert_msg, &cv, &sfin] {
            flight.extend_from_slice(m);
        }

        let mut shrec = vec![0x16, 0x03, 0x03];
        shrec.extend_from_slice(&(sh.len() as u16).to_be_bytes());
        shrec.extend_from_slice(&sh);
        io.write_all(&shrec).await.unwrap();
        io.write_all(&[0x14, 0x03, 0x03, 0x00, 0x01, 0x01]).await.unwrap(); // server CCS
        let mut s_hs = RecordKeys::new(&hs.server_key, &hs.server_iv);
        io.write_all(&s_hs.seal(0x16, &flight)).await.unwrap();
        io.flush().await.unwrap();

        // 读 client Finished + 验。
        let (_ctf, fh, fp) = rr.next(io).await.unwrap();
        let mut c_hs = RecordKeys::new(&hs.client_key, &hs.client_iv);
        let (_it, cfin) = c_hs.open(&fh, &fp).unwrap();
        let th_sfin = transcript_hash(&[&ch, &sh, &ee, &cert_msg, &cv, &sfin]);
        let mut want = vec![0x14, 0x00, 0x00, 0x20];
        want.extend_from_slice(&compute_finished_verify_data(&hs.c_hs_secret, &th_sfin));
        assert_eq!(cfin, want, "client Finished 验证");

        let app = derive_application_keys(&hs.handshake_secret, &th_sfin);
        (rr, app)
    }

    /// 测试内最小 REALITY **服务端**模拟器：完整跑服务端握手 + 收 VLESS 请求 + echo（验客户端整条路径）。
    async fn reality_server_sim<S: AsyncRead + AsyncWrite + Unpin>(
        mut io: S,
        server_static_sk: [u8; 32],
        expected_uuid: [u8; 16],
    ) {
        let (mut rr, app) = reality_sim_handshake(&mut io, server_static_sk).await;
        // app keys + 收 VLESS 请求 + "ping" → echo。
        let mut c_ap = RecordKeys::new(&app.client_key, &app.client_iv);
        let mut s_ap = RecordKeys::new(&app.server_key, &app.server_iv);
        let (_c1, vh, vp) = rr.next(&mut io).await.unwrap();
        let (_iv, vless) = c_ap.open(&vh, &vp).unwrap();
        assert_eq!(vless[0], 0x00, "VLESS version 0");
        assert_eq!(&vless[1..17], &expected_uuid, "VLESS UUID 命中");
        let (_c2, ph, pp) = rr.next(&mut io).await.unwrap();
        let (_ip, ping) = c_ap.open(&ph, &pp).unwrap();
        assert_eq!(ping, b"ping", "收到 client app data");
        let mut resp = vec![0x00, 0x00]; // VLESS 响应头（空 addons）
        resp.extend_from_slice(b"pong");
        io.write_all(&s_ap.seal(0x17, &resp)).await.unwrap();
        io.flush().await.unwrap();
    }

    /// T19（刀10/F5）服务端模拟器：握手 + 收 VLESS 后**主动发 KeyUpdate(update_requested=1)**，
    /// 验客户端 `RealityStream` 端到端：轮接收密钥解 server 新-key 数据 + 回发 reply（旧 c_ap 解）+ 轮发送密钥后续读。
    async fn reality_server_sim_keyupdate<S: AsyncRead + AsyncWrite + Unpin>(
        mut io: S,
        server_static_sk: [u8; 32],
        expected_uuid: [u8; 16],
    ) {
        let (mut rr, app) = reality_sim_handshake(&mut io, server_static_sk).await;
        let mut c_ap = RecordKeys::new(&app.client_key, &app.client_iv); // server 解 client→server（seq0）
        let mut s_ap = RecordKeys::new(&app.server_key, &app.server_iv); // server 封 server→client（seq0）

        // 1. 读 client 的 VLESS 请求（establish 发的首条 app record，c_ap seq0→1）。
        let (_c, vh, vp) = rr.next(&mut io).await.unwrap();
        let (_iv, vless) = c_ap.open(&vh, &vp).unwrap();
        assert_eq!(&vless[1..17], &expected_uuid, "VLESS UUID 命中");

        // 2. server 主动发 KeyUpdate(update_requested=1)（用当前/旧 s_ap seq0 封），随后轮自己发送 secret。
        io.write_all(&s_ap.seal(0x16, &[0x18, 0x00, 0x00, 0x01, 0x01])).await.unwrap();
        let s_ap1 = next_application_traffic_secret(&app.s_ap_secret);
        let mut s_ap_new = keys_from(&s_ap1);

        // 3. server 用新发送密钥发 app data（含 VLESS 响应头）。client 须用轮换后的 recv 解。
        let mut down = vec![0x00, 0x00]; // VLESS 响应头（空 addons）
        down.extend_from_slice(b"down-after-update");
        io.write_all(&s_ap_new.seal(0x17, &down)).await.unwrap();
        io.flush().await.unwrap();

        // 4. 读 client 回发的 KeyUpdate(update_not_requested=0)（client 用旧 c_ap seq1 封），随后轮 server 接收 secret。
        let (_k, kh, kp) = rr.next(&mut io).await.unwrap();
        let (kt, kbody) = c_ap.open(&kh, &kp).unwrap();
        assert_eq!((kt, kbody.as_slice()), (0x16, &[0x18u8, 0x00, 0x00, 0x01, 0x00][..]), "client 回发 update_not_requested(0)");
        let c_ap1 = next_application_traffic_secret(&app.c_ap_secret);
        let mut c_ap_new = keys_from(&c_ap1);

        // 5. 读 client 用新发送密钥发的 app data（c_ap1 seq0）。
        let (_u, uh, up) = rr.next(&mut io).await.unwrap();
        let (ut, ubody) = c_ap_new.open(&uh, &up).unwrap();
        assert_eq!((ut, ubody.as_slice()), (0x17, &b"up-after-update"[..]), "client 轮 send 后 app data 用 c_ap1 解");
    }

    /// **T9 capstone**：完整 REALITY 握手 over duplex（测试内服务端模拟器）→ verify_server_cert 真路径通过
    /// → VLESS 请求被服务端正确解析 → app data 往返。一次性证明 open_tcp 接线 + REALITY ECDH 一致 + VLESS 互通。
    #[tokio::test]
    async fn open_tcp_full_reality_handshake_loopback() {
        use crate::reality::auth::generate_ephemeral_keypair;
        let (sk_s, pk_s) = generate_ephemeral_keypair(); // 服务端静态密钥
        let uuid = [0x42u8; 16];
        let cfg = RealityClientConfig {
            server: "unused-loopback".into(),
            uuid,
            pbk: pk_s,
            short_id: parse_short_id("01ab").unwrap(),
            sni: "example.com".into(),
        };
        let upstream = RealityUpstream::new(cfg);

        let (client_end, server_end) = tokio::io::duplex(65536);
        let server = tokio::spawn(reality_server_sim(server_end, sk_s, uuid));

        let mut client = client_end;
        let target = TargetAddr::parse("1.2.3.4:443").unwrap();
        let out = upstream.establish(&mut client, &target).await.expect("REALITY 握手应成功（cert HMAC 通过）");
        let (rh, wh) = tokio::io::split(client);
        let mut stream =
            RealityStream::new(rh, wh, out.recv_keys, out.send_keys, out.leftover, out.s_ap_secret, out.c_ap_secret);
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        let mut got = vec![0u8; 4];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"pong", "经 REALITY 隧道 + VLESS 帧端到端往返");
        server.await.unwrap();
    }

    /// **T19 capstone（刀10/F5）**：完整 REALITY 握手后服务端发 KeyUpdate(update_requested=1)，
    /// 经真 read/write 路径端到端验：① `RealityStream` 轮接收密钥仍能解 server 新-key app data（剥 VLESS 头）；
    /// ② 读路径上回发 reply（server 用旧 c_ap 解出 update_not_requested）；③ 轮发送密钥后 client 新-key 数据 server 用 c_ap1 解。
    /// 一次性证明 decode_one→on_key_update 接线 + poll_read 机会性 flush + 双向轮换字节级一致。
    #[tokio::test]
    async fn keyupdate_loopback_end_to_end() {
        use crate::reality::auth::generate_ephemeral_keypair;
        let (sk_s, pk_s) = generate_ephemeral_keypair();
        let uuid = [0x42u8; 16];
        let cfg = RealityClientConfig {
            server: "unused-loopback".into(),
            uuid,
            pbk: pk_s,
            short_id: parse_short_id("01ab").unwrap(),
            sni: "example.com".into(),
        };
        let upstream = RealityUpstream::new(cfg);

        let (client_end, server_end) = tokio::io::duplex(65536);
        let server = tokio::spawn(reality_server_sim_keyupdate(server_end, sk_s, uuid));

        let mut client = client_end;
        let target = TargetAddr::parse("1.2.3.4:443").unwrap();
        let out = upstream.establish(&mut client, &target).await.expect("REALITY 握手应成功");
        let (rh, wh) = tokio::io::split(client);
        let mut stream =
            RealityStream::new(rh, wh, out.recv_keys, out.send_keys, out.leftover, out.s_ap_secret, out.c_ap_secret);

        // 读：触发 KeyUpdate 处理（轮 recv + 回发 reply + 轮 send），返回 server 用新密钥发的 app data。
        let mut got = vec![0u8; b"down-after-update".len()];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"down-after-update", "KeyUpdate 后用轮换 recv 解 server 数据（剥 VLESS 头）");

        // 写：用轮换后的 send_keys 发，server 用新 c_ap1 解。
        stream.write_all(b"up-after-update").await.unwrap();
        stream.flush().await.unwrap();
        server.await.unwrap();
    }

    /// 跨 read 的半条 record：record 字节分两次到达 → decode_one 等齐才解，read_exact 拿全。
    #[tokio::test]
    async fn realitystream_partial_record_reassembled() {
        let (recv_k, recv_iv) = ([0x55u8; 16], [0x66u8; 12]);
        let (client_end, mut server_end) = tokio::io::duplex(65536);
        let (cr, cw) = tokio::io::split(client_end);
        let mut stream = RealityStream::new(
            cr,
            cw,
            RecordKeys::new(&recv_k, &recv_iv),
            RecordKeys::new(&[0x33; 16], &[0x44; 12]),
            BytesMut::new(),
            [0u8; 32], // server_ap_secret（本测试不触发 KeyUpdate）
            [0u8; 32], // client_ap_secret
        );
        let mut srv = RecordKeys::new(&recv_k, &recv_iv);
        let mut r1 = vec![0x00, 0x00]; // VLESS 响应头
        r1.extend_from_slice(b"split-record-payload");
        let rec = srv.seal(0x17, &r1);
        let mid = rec.len() / 2;
        let (first, second) = (rec[..mid].to_vec(), rec[mid..].to_vec());
        let writer = tokio::spawn(async move {
            server_end.write_all(&first).await.unwrap();
            server_end.flush().await.unwrap();
            tokio::task::yield_now().await;
            server_end.write_all(&second).await.unwrap();
            server_end.flush().await.unwrap();
            server_end
        });
        let mut got = vec![0u8; "split-record-payload".len()];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"split-record-payload", "半条 record 跨 read 重组");
        let _ = writer.await;
    }

    /// 构一个最小 RealityStream over duplex（test helper）。
    fn mk_stream(rk: [u8; 16], iv: [u8; 12]) -> (
        RealityStream<tokio::io::ReadHalf<tokio::io::DuplexStream>, tokio::io::WriteHalf<tokio::io::DuplexStream>>,
        tokio::io::DuplexStream,
    ) {
        let (client_end, server_end) = tokio::io::duplex(4096);
        let (cr, cw) = tokio::io::split(client_end);
        let s = RealityStream::new(
            cr,
            cw,
            RecordKeys::new(&rk, &iv),
            RecordKeys::new(&[0x33; 16], &[0x44; 12]),
            BytesMut::new(),
            [0u8; 32],
            [0u8; 32],
        );
        (s, server_end)
    }

    /// 从 application_traffic_secret 派 RecordKeys（与 prod `record_keys_from_secret` 同式，测试本地独立实现，
    /// 用作对端/独立校验，避免拿被测代码当 oracle）。
    fn keys_from(secret: &[u8; 32]) -> RecordKeys {
        let key: [u8; 16] = expand_label(secret, "key", b"", 16).try_into().unwrap();
        let iv: [u8; 12] = expand_label(secret, "iv", b"", 12).try_into().unwrap();
        RecordKeys::new(&key, &iv)
    }

    /// 构带受控 application_traffic_secret 的 RealityStream（KeyUpdate 单测用）：
    /// recv_keys=keys_from(s_ap)、send_keys=keys_from(c_ap)，两 secret 字段亦置 s_ap/c_ap。
    fn mk_stream_secrets(
        s_ap: [u8; 32],
        c_ap: [u8; 32],
    ) -> (
        RealityStream<tokio::io::ReadHalf<tokio::io::DuplexStream>, tokio::io::WriteHalf<tokio::io::DuplexStream>>,
        tokio::io::DuplexStream,
    ) {
        let (client_end, server_end) = tokio::io::duplex(65536);
        let (cr, cw) = tokio::io::split(client_end);
        let s = RealityStream::new(cr, cw, keys_from(&s_ap), keys_from(&c_ap), BytesMut::new(), s_ap, c_ap);
        (s, server_end)
    }

    /// T17（刀10/F5）时序铁律：收 update_requested(1) →
    /// B1 回发 reply 用「旧」send key/旧 seq 封装（crypto evidence：旧 key/seq 能解出 `18 00 00 01 00`
    /// ⟺ 封装发生在轮换之前——若先轮 send，旧 key 解必败）；B2 之后 send_keys 才轮到 N+1 seq0；接收方向也轮 N+1。
    #[tokio::test]
    async fn keyupdate_requested_reply_old_key_then_rotate_send() {
        let (s0, c0) = ([0xA0u8; 32], [0xC0u8; 32]);
        let (mut stream, _srv) = mk_stream_secrets(s0, c0);

        // 对齐“旧 seq”：客户端先发一条 app record（send seq0→1），对端 peer_old 同步解开（recv seq0→1）。
        let mut peer_old = keys_from(&c0); // 对端旧 recv（= 我方旧 send c0）
        let pre = stream.send_keys.seal(0x17, b"vless-request");
        let ph: [u8; 5] = pre[..5].try_into().unwrap();
        peer_old.open(&ph, &pre[5..]).unwrap(); // peer_old → seq1

        // 收 update_requested(1)。
        stream.on_key_update(&[0x18, 0x00, 0x00, 0x01, 0x01]).unwrap();

        // B1：reply 必须用「旧」send key c0 @ seq1 封装 → peer_old(seq1) 解出 KeyUpdate(update_not_requested=0)。
        assert!(!stream.write_pending.is_empty(), "update_requested(1) 必回发 reply");
        let reply = stream.write_pending.split().to_vec();
        let rh: [u8; 5] = reply[..5].try_into().unwrap();
        let (rit, rbody) = peer_old.open(&rh, &reply[5..]).expect("reply 须用旧 send key/seq 封装（B1 先于 B2）");
        assert_eq!(rit, 0x16, "reply 内层 handshake");
        assert_eq!(rbody, vec![0x18, 0x00, 0x00, 0x01, 0x00], "回发 KeyUpdate(update_not_requested=0)，防环");

        // B2：send_keys 已轮到 c1 @ seq0 → 再发一条用 c1 新 key seq0 解。
        let c1 = next_application_traffic_secret(&c0);
        let mut peer_new = keys_from(&c1);
        let after = stream.send_keys.seal(0x17, b"after-update");
        let ah: [u8; 5] = after[..5].try_into().unwrap();
        let (ait, abody) = peer_new.open(&ah, &after[5..]).expect("轮换后 send_keys 须用 c1 新 key seq0");
        assert_eq!((ait, abody.as_slice()), (0x17, &b"after-update"[..]));
        assert_eq!(stream.client_ap_secret, c1, "发送 secret 轮到 N+1");

        // 接收方向也轮到 s1：server 用 s1 封一条 → stream.recv_keys 解。
        let s1 = next_application_traffic_secret(&s0);
        let mut srv_new = keys_from(&s1);
        let down = srv_new.seal(0x17, b"down-after");
        let dh: [u8; 5] = down[..5].try_into().unwrap();
        let (dit, dbody) = stream.recv_keys.open(&dh, &down[5..]).expect("recv_keys 须轮到 s1");
        assert_eq!((dit, dbody.as_slice()), (0x17, &b"down-after"[..]));
        assert_eq!(stream.server_ap_secret, s1, "接收 secret 轮到 N+1");
    }

    /// T18（刀10/F5）：收 update_not_requested(0) 只轮接收、不回发、不动发送；
    /// 非法 request_update / 非法帧长 → Err 且零 mutation（前置校验）。
    #[tokio::test]
    async fn keyupdate_not_requested_recv_only_and_illegal_rejected() {
        let (s0, c0) = ([0x1Au8; 32], [0x2Bu8; 32]);

        // ---- update_not_requested(0)：只轮 recv，不回发、不动 send。----
        let (mut stream, _srv) = mk_stream_secrets(s0, c0);
        stream.on_key_update(&[0x18, 0x00, 0x00, 0x01, 0x00]).unwrap();
        assert!(stream.write_pending.is_empty(), "update_not_requested(0) 不回发");
        assert_eq!(stream.client_ap_secret, c0, "不动发送 secret");
        // send_keys 仍 c0 @ seq0。
        let mut peer_send = keys_from(&c0);
        let snd = stream.send_keys.seal(0x17, b"still-c0");
        let sh: [u8; 5] = snd[..5].try_into().unwrap();
        let (_st, sbody) = peer_send.open(&sh, &snd[5..]).expect("发送方向未轮换：仍 c0 seq0");
        assert_eq!(sbody, b"still-c0");
        // recv 已轮到 s1。
        let s1 = next_application_traffic_secret(&s0);
        assert_eq!(stream.server_ap_secret, s1, "接收 secret 轮到 N+1");
        let mut srv_new = keys_from(&s1);
        let down = srv_new.seal(0x17, b"down-s1");
        let dh: [u8; 5] = down[..5].try_into().unwrap();
        let (_dt, dbody) = stream.recv_keys.open(&dh, &down[5..]).expect("recv 轮到 s1");
        assert_eq!(dbody, b"down-s1");

        // ---- 非法 request_update（2）→ Err 且零 mutation。----
        let (mut s2, _e2) = mk_stream_secrets(s0, c0);
        assert!(s2.on_key_update(&[0x18, 0x00, 0x00, 0x01, 0x02]).is_err(), "request_update=2 → Err");
        assert_eq!(s2.server_ap_secret, s0, "非法值不改接收 secret");
        assert!(s2.write_pending.is_empty(), "非法值不回发");
        let mut srv_s0 = keys_from(&s0);
        let d0 = srv_s0.seal(0x17, b"still-s0");
        let dh0: [u8; 5] = d0[..5].try_into().unwrap();
        let (_t3, b3) = s2.recv_keys.open(&dh0, &d0[5..]).expect("非法值：recv_keys 未轮、仍 s0 seq0");
        assert_eq!(b3, b"still-s0");

        // ---- 非法帧长（body_len != 1）→ Err。----
        let (mut s3, _e3) = mk_stream_secrets(s0, c0);
        assert!(s3.on_key_update(&[0x18, 0x00, 0x00, 0x02, 0x00, 0x00]).is_err(), "body_len=2 → Err");
        assert!(s3.on_key_update(&[0x18, 0x00, 0x00]).is_err(), "截断帧 → Err");
    }

    /// T20a（刀10/F5 robustness，对抗式 review finding）：单条 0x16 record **合并** `[NST][KeyUpdate]`
    /// （RFC 8446 §5.1 同向允许合并）→ KeyUpdate 仍被切出处理（recv 轮换），而非「只看首字节(=NST 0x04)整条丢」。
    /// 反例保护：旧实现只判 content.first()==0x18 会漏掉尾部 KeyUpdate → recv 不轮 → 下条 record bad-decrypt（本 read_exact 会 Err）。
    #[tokio::test]
    async fn keyupdate_coalesced_after_nst_in_one_record() {
        let (s0, c0) = ([0x3Cu8; 32], [0x4Du8; 32]);
        let (mut stream, mut srv_end) = mk_stream_secrets(s0, c0);
        // server 用 s0（=client recv）封一条 0x16：content = [NST 0x04 len4 body] || [KeyUpdate update_not_requested]。
        let mut srv = keys_from(&s0);
        let mut content = vec![0x04, 0x00, 0x00, 0x04, 0xAA, 0xBB, 0xCC, 0xDD]; // NewSessionTicket（4B body）
        content.extend_from_slice(&[0x18, 0x00, 0x00, 0x01, 0x00]); // 合并的 KeyUpdate(update_not_requested=0)
        srv_end.write_all(&srv.seal(0x16, &content)).await.unwrap();
        // 紧接着用「轮换后」s1 发 app data（含 VLESS 头）；client 只有真处理了合并的 KeyUpdate（recv→s1）才能解。
        let s1 = next_application_traffic_secret(&s0);
        let mut srv_new = keys_from(&s1);
        let mut down = vec![0x00, 0x00];
        down.extend_from_slice(b"after-coalesced");
        srv_end.write_all(&srv_new.seal(0x17, &down)).await.unwrap();
        srv_end.flush().await.unwrap();

        let mut got = vec![0u8; b"after-coalesced".len()];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"after-coalesced", "合并 record 中的 KeyUpdate 被处理 → recv 轮到 s1，新-key 数据可解");
    }

    /// L1：fatal alert → Err；close_notify → 干净 EOF（read 返回 0）。
    #[tokio::test]
    async fn realitystream_alert_distinguished() {
        // fatal(2) handshake_failure(0x28) → Err。
        let (rk, iv) = ([0x55u8; 16], [0x66u8; 12]);
        let (mut s1, mut e1) = mk_stream(rk, iv);
        let mut srv1 = RecordKeys::new(&rk, &iv);
        e1.write_all(&srv1.seal(0x15, &[0x02, 0x28])).await.unwrap();
        e1.flush().await.unwrap();
        let mut b = [0u8; 4];
        assert!(s1.read(&mut b).await.is_err(), "fatal alert → Err（L1）");

        // close_notify(1,0) → 干净 EOF。
        let (mut s2, mut e2) = mk_stream(rk, iv);
        let mut srv2 = RecordKeys::new(&rk, &iv);
        e2.write_all(&srv2.seal(0x15, &[0x01, 0x00])).await.unwrap();
        e2.flush().await.unwrap();
        drop(e2);
        let mut b2 = [0u8; 4];
        assert_eq!(s2.read(&mut b2).await.unwrap(), 0, "close_notify → 干净 EOF");
    }

    /// M4：半条 record + 裸 FIN → 截断 Err（不静默丢字节/不伪装干净 EOF）。
    #[tokio::test]
    async fn realitystream_truncated_record_errs() {
        let (rk, iv) = ([0x77u8; 16], [0x88u8; 12]);
        let (mut stream, mut srv_end) = mk_stream(rk, iv);
        srv_end.write_all(&[0x17, 0x03, 0x03]).await.unwrap(); // 只发 record 头的一部分
        srv_end.flush().await.unwrap();
        drop(srv_end); // FIN
        let mut b = [0u8; 4];
        assert!(stream.read(&mut b).await.is_err(), "半条 record + FIN → 截断 Err（M4）");
    }
}
