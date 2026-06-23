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
use crate::reality::client_hello::{AuthedClientHelloParams, build_authed_client_hello};
use crate::reality::handshake::{self, HandshakeInput};
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
}

/// 一条 record 解码后的去向。
enum Decoded {
    Data(Vec<u8>), // 内层 0x17 app data
    Drop,          // 内层 0x16 post-handshake（NST/KeyUpdate）/ 其它 → 丢
    Eof,           // 内层 0x15 alert（close_notify 等）→ 视为流结束
}

impl<R, W> RealityStream<R, W> {
    /// 握手完成后构造：`leftover` = 握手阶段多读的未消费 outer record 字节。
    pub fn new(
        read_half: R,
        write_half: W,
        recv_keys: RecordKeys,
        send_keys: RecordKeys,
        leftover: BytesMut,
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
        Ok(Some(match inner {
            0x17 => Decoded::Data(content),
            0x15 => Decoded::Eof,
            _ => Decoded::Drop, // 0x16 post-handshake / 未知 → 丢
        }))
    }

    /// 吸收一条 app-data 内容：首读经 VLESS 响应头 strip（可跨 record 累积），剥完后转入 plaintext_out。
    fn absorb_app(&mut self, content: &[u8]) {
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

impl<R: AsyncRead + Unpin, W: Unpin> AsyncRead for RealityStream<R, W> {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
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
                        return Poll::Ready(Ok(())); // EOF
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
        let expected_session_id = ch[39..71].to_vec();
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
        // 5. 发 VLESS 请求（握手后第一条 app record，send_keys seq 0）。
        let vless = out.send_keys.seal(0x17, &encode_vless_request(&self.cfg.uuid, VLESS_CMD_TCP, target));
        stream.write_all(&vless).await.map_err(|e| ClientError::Reality(format!("写 VLESS 请求: {e}")))?;
        stream.flush().await.map_err(|e| ClientError::Reality(format!("flush VLESS 请求: {e}")))?;
        Ok(out)
    }
}

#[async_trait::async_trait]
impl ProxyUpstream for RealityUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        let mut stream = TcpStream::connect(&self.cfg.server)
            .await
            .map_err(|e| ClientError::Reality(format!("TCP 连 REALITY 出口 {} 失败: {e}", self.cfg.server)))?;
        let out = self.establish(&mut stream, target).await?;
        // prod 用 into_split（lock-free 独立读写半），喂给同一套双向中继泵。
        let (rh, wh) = stream.into_split();
        Ok(Box::new(RealityStream::new(rh, wh, out.recv_keys, out.send_keys, out.leftover)))
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for RealityUpstream {
    /// REALITY 是 **TCP-only**：UDP **no-op 静默丢**（UDP-over-VLESS 是刀9；force-reality 下 UDP 不可用符合预期）。
    async fn send_udp(&self, _datagram: Vec<u8>) {}
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

    /// 测试内最小 REALITY **服务端**模拟器：完整跑服务端握手 + 收 VLESS 请求 + echo（验客户端整条路径）。
    async fn reality_server_sim<S: AsyncRead + AsyncWrite + Unpin>(
        mut io: S,
        server_static_sk: [u8; 32],
        expected_uuid: [u8; 16],
    ) {
        use crate::reality::auth::{derive_auth_key, generate_ephemeral_keypair, x25519_shared_secret};
        use crate::reality::key_schedule::{
            compute_finished_verify_data, derive_application_keys, derive_handshake_keys, transcript_hash,
        };
        let mut rr = RecordReader::new();
        let (_ct, _h, ch) = rr.next(&mut io).await.unwrap(); // client CH
        let (ct_ccs, _h2, _b) = rr.next(&mut io).await.unwrap(); // client CCS
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
        let (_ctf, fh, fp) = rr.next(&mut io).await.unwrap();
        let mut c_hs = RecordKeys::new(&hs.client_key, &hs.client_iv);
        let (_it, cfin) = c_hs.open(&fh, &fp).unwrap();
        let th_sfin = transcript_hash(&[&ch, &sh, &ee, &cert_msg, &cv, &sfin]);
        let mut want = vec![0x14, 0x00, 0x00, 0x20];
        want.extend_from_slice(&compute_finished_verify_data(&hs.c_hs_secret, &th_sfin));
        assert_eq!(cfin, want, "client Finished 验证");

        // app keys + 收 VLESS 请求 + "ping" → echo。
        let app = derive_application_keys(&hs.handshake_secret, &th_sfin);
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
        let mut stream = RealityStream::new(rh, wh, out.recv_keys, out.send_keys, out.leftover);
        stream.write_all(b"ping").await.unwrap();
        stream.flush().await.unwrap();
        let mut got = vec![0u8; 4];
        stream.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"pong", "经 REALITY 隧道 + VLESS 帧端到端往返");
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
}
