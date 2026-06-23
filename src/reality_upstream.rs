//! REALITY 第二 Transport 上游（刀8 T9/T10，见 spec §4 + brief §3）。
//!
//! 中文要点：`RealityUpstream` impl `ProxyUpstream::open_tcp`——每条 TCP 新建一次完整 REALITY 握手、
//! 接着发 VLESS 请求 → 返回 `RealityStream`（impl AsyncRead+AsyncWrite over TLS 1.3 app record 层、含 VLESS 响应 strip）。
//! impl `DatagramUpstream::send_udp` = **no-op 静默丢**（REALITY 是 TCP-only，UDP-over-VLESS 是刀9）。
//! `RealityClientConfig::from_env`（MINI_VPN_REALITY_*，脱敏 Debug）。无连接复用（reuse 留刀9）。

use crate::reality::record::RecordKeys;
use crate::reality::vless::VlessResponseStripper;
use crate::shared::ClientError;
use bytes::BytesMut;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
