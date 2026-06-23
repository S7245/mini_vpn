//! REALITY 实 TLS 1.3 握手驱动（刀8 T5/T6，见 spec §5 时序 + brief §1.1/1.2）。
//!
//! 中文要点：薄 async 驱动 `drive<S: AsyncRead+AsyncWrite>` 编排已有纯步骤（parse_server_hello /
//! derive_handshake_keys / verify_server_cert / compute_finished_verify_data / derive_application_keys /
//! RecordKeys），延续刀6/7 sans-IO + RFC 8448 KAT 纪律。互通-critical（别再踩）：
//! - 明文 CH record 头 `16 03 01`（自加 5B）；密文 `17 03 03`；客户端自发 dummy CCS `14 03 03 00 01 01`。
//! - 收到的 server CCS(0x14) 整条丢弃、**不 open、不递增 server read seq**。
//! - server flight 内层 0x16 跨 record 重组（独立 handshake buffer，按 1B type + 3B len 切 message）。
//! - seq：read s_hs→s_ap、write c_hs→c_ap 切密钥各归零。
//! - transcript：server Finished 验=hash(CH..CertVerify)；client Finished+app keys=hash(CH..serverFinished)；
//!   **CertVerify 字节必折**（即便 defer 验签，ADR-0010）。
//! - 两个 ECDH 别混：TLS 握手 ECDHE=x25519(client 临时, server SH 临时 keyshare)；REALITY AuthKey=x25519(client 临时, server 静态 pbk)。

/// 跨 record 的 handshake message 重组器（**纯状态机**，brief §1.2/风险 2）。
/// 中文要点：server flight 的内层 0x16 handshake 字节可跨多条 `17 03 03` record 分片、或多条 message 合并在一条
/// record——**record 边界 ≠ message 边界**。本器把解密后的 inner-0x16 字节累积进**独立** buffer，按
/// `1B type + 3B uint24 len` 切出完整 message（含 4B 头）。半条缓存等后续；一次喂入可切多条；message 跨喂入累积。
#[derive(Default)]
pub struct HandshakeReassembler {
    buf: Vec<u8>,
}

impl HandshakeReassembler {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// 喂入一段解密后的 inner-0x16 字节，返回本次能切出的所有完整 handshake message（每条含 `1B type + 3B len + body`）。
    /// 中文要点：不足一条则缓存、返回空；调用方对每条 message 按 `msg[0]` 分类（EE 0x08 / Cert 0x0b /
    /// CertVerify 0x0f / Finished 0x14）并折入 transcript。
    pub fn push(&mut self, data: &[u8]) -> Vec<Vec<u8>> {
        self.buf.extend_from_slice(data);
        let mut out = Vec::new();
        loop {
            if self.buf.len() < 4 {
                break; // 连 type+len 头都不够
            }
            let len = ((self.buf[1] as usize) << 16) | ((self.buf[2] as usize) << 8) | self.buf[3] as usize;
            let total = 4 + len;
            if self.buf.len() < total {
                break; // message body 未集齐 → 等后续
            }
            out.push(self.buf[..total].to_vec());
            self.buf.drain(..total);
        }
        out
    }

    /// 缓冲是否已清空（所有喂入字节都已切成完整 message）。调用方据此判断 flight 是否干净结束。
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }
}

use crate::reality::record::{MAX_TLS_PLAINTEXT, MAX_TLS_RECORD, RecordKeys};
use crate::shared::ClientError;
use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

fn err(m: impl Into<String>) -> ClientError {
    ClientError::Reality(m.into())
}
fn io_err<E: std::fmt::Display>(ctx: &str, e: E) -> ClientError {
    ClientError::Reality(format!("{ctx}: {e}"))
}

/// 逐条读 outer TLS record（5B 头 + body），跨 read 缓冲粘包/拆包。
/// 中文要点：握手与 app data 都按 record 分帧；`leftover` 在握手结束后交给 RealityStream 续读。
pub(crate) struct RecordReader {
    buf: BytesMut,
}

impl RecordReader {
    pub(crate) fn new() -> Self {
        Self { buf: BytesMut::with_capacity(4096) }
    }

    /// 用握手阶段多读的残留字节初始化（RealityStream 接手时用——T8 接线后此 allow 移除）。
    #[allow(dead_code)]
    pub(crate) fn with_leftover(buf: BytesMut) -> Self {
        Self { buf }
    }

    /// 读一条完整 record，返回 `(content_type, 5B 头, payload)`。EOF / 超长 → Err。
    pub(crate) async fn next<R: AsyncRead + Unpin + ?Sized>(
        &mut self,
        r: &mut R,
    ) -> Result<(u8, [u8; 5], Vec<u8>), ClientError> {
        while self.buf.len() < 5 {
            if r.read_buf(&mut self.buf).await.map_err(|e| io_err("读 record 头", e))? == 0 {
                return Err(err("连接关闭（读 record 头 EOF）"));
            }
        }
        let header: [u8; 5] = self.buf[..5].try_into().expect("≥5B");
        let len = u16::from_be_bytes([header[3], header[4]]) as usize;
        if len > MAX_TLS_RECORD {
            return Err(err(format!("record 长度 {len} 超 TLS 上限")));
        }
        while self.buf.len() < 5 + len {
            if r.read_buf(&mut self.buf).await.map_err(|e| io_err("读 record body", e))? == 0 {
                return Err(err("连接关闭（读 record body EOF）"));
            }
        }
        let _ = self.buf.split_to(5);
        let payload = self.buf.split_to(len).to_vec();
        Ok((header[0], header, payload))
    }

    /// 取走未消费的残留字节（握手结束后交给 RealityStream）。
    pub(crate) fn into_leftover(self) -> BytesMut {
        self.buf
    }
}

/// 写一条明文 record：`content_type | version(2B) | u16(len) | body`。
/// 中文要点（L3）：body 超 2^14 明文上限 → 显式 Err（防 `as u16` 静默截断 → 对端按错长解析握手失败无线索）。
/// 与读路径 `MAX_TLS_RECORD` 防御对称；当前 CH 约 200-300B，纯防御性（将来扩 CH 才可能触及）。
async fn write_record<W: AsyncWrite + Unpin + ?Sized>(
    w: &mut W,
    content_type: u8,
    version: u16,
    body: &[u8],
) -> Result<(), ClientError> {
    if body.len() > MAX_TLS_PLAINTEXT {
        return Err(err(format!("明文 record body {} 超 2^14 上限", body.len())));
    }
    let mut rec = Vec::with_capacity(5 + body.len());
    rec.push(content_type);
    rec.extend_from_slice(&version.to_be_bytes());
    rec.extend_from_slice(&(body.len() as u16).to_be_bytes());
    rec.extend_from_slice(body);
    w.write_all(&rec).await.map_err(|e| io_err("写 record", e))
}

/// 握手驱动入参（与 ClientHello 构造解耦：RealityUpstream 生成临时密钥 + 建 authed CH；KAT 喂 RFC 8448 fixture）。
pub struct HandshakeInput {
    /// ClientHello **handshake message** 字节（已含 REALITY auth session_id，由 build_authed_client_hello 产）。
    pub client_hello: Vec<u8>,
    /// 客户端临时 X25519 私钥（与 ServerHello 的 server keyshare 算 TLS 握手 ECDHE）。
    pub client_eph_secret: [u8; 32],
    /// 我方 ClientHello 发出的 session_id（REALITY=32B sealed；RFC 8448 示例为空），供 SH echo 一致性检查。
    pub expected_session_id: Vec<u8>,
}

/// 握手产出：app 阶段读/写密钥（seq 各从 0）+ 握手结束后多读的残留字节。
pub struct HandshakeOutput {
    pub recv_keys: RecordKeys,
    pub send_keys: RecordKeys,
    pub leftover: BytesMut,
}

/// 驱动一次完整 REALITY TLS 1.3 握手（spec §5 时序）。`verify_cert` = 对 Certificate(0x0b) message
/// 做 REALITY auth 决策的接缝：RealityUpstream 注入 `extract + verify_server_cert`；KAT 注入 always-ok
/// （RFC 8448 cert 是 RSA、不可走 ed25519 HMAC）。**CertificateVerify 仅折 transcript 不验签**（ADR-0010）。
pub async fn drive<S, V>(
    stream: &mut S,
    input: HandshakeInput,
    verify_cert: V,
) -> Result<HandshakeOutput, ClientError>
where
    S: AsyncRead + AsyncWrite + Unpin,
    V: Fn(&[u8]) -> Result<(), ClientError>,
{
    use crate::reality::auth::x25519_shared_secret;
    use crate::reality::key_schedule::{
        compute_finished_verify_data, derive_application_keys, derive_handshake_keys, transcript_hash,
    };
    use crate::reality::server_hello::parse_server_hello;

    // 1. 发 ClientHello record（明文 16 03 01）+ 客户端 dummy CCS（裁决 d；SH 后再切密钥）。
    write_record(stream, 0x16, 0x0301, &input.client_hello).await?;
    stream
        .write_all(&[0x14, 0x03, 0x03, 0x00, 0x01, 0x01])
        .await
        .map_err(|e| io_err("写 client CCS", e))?;
    stream.flush().await.map_err(|e| io_err("flush CH", e))?;

    let mut transcript: Vec<u8> = input.client_hello.clone();

    let mut reader = RecordReader::new();
    // 2. 读 ServerHello（首条 record 须 0x16）。
    let (ct, _hdr, sh_msg) = reader.next(stream).await?;
    if ct != 0x16 {
        return Err(err(format!("首条 record 非 handshake（type 0x{ct:02x}，期望 ServerHello）")));
    }
    let sh = parse_server_hello(&sh_msg, &input.expected_session_id)?;
    transcript.extend_from_slice(&sh_msg);

    // 3. TLS 握手 ECDHE = x25519(client 临时, server SH 临时 keyshare)；派生握手密钥（全零 ECDHE 已拒）。
    let ecdhe = x25519_shared_secret(input.client_eph_secret, sh.server_key_share);
    let hs = derive_handshake_keys(&ecdhe, &input.client_hello, &sh_msg)?;
    let mut recv = RecordKeys::new(&hs.server_key, &hs.server_iv); // read-seq 0
    let mut send_hs = RecordKeys::new(&hs.client_key, &hs.client_iv); // write-seq 0

    // 4. 读 + 解密 server flight，跨 record 重组消息，处理到 server Finished。
    // **H1 安全守卫**：REALITY auth 唯一判据是 Certificate(0x0b) 上的 verify_cert。server Finished 的 MAC 只用
    // ECDHE 派生的 s_hs（与 REALITY 静态 pbk 无关），任何完成 ECDHE 的诚实 TLS server/decoy 都能算对 → 若 flight
    // 只发 EE+Finished（被回落的 decoy 可如此塑形）会干净走到 break 而 verify_cert 从未执行、auth 被静默绕过。
    // 故 `cert_verified` 门控：未见过通过校验的 Certificate 不许完成握手（brief R4-C5）。
    let mut reasm = HandshakeReassembler::new();
    let mut cert_verified = false;
    'outer: loop {
        let (ct, hdr, payload) = reader.next(stream).await?;
        match ct {
            // server dummy CCS：整条丢弃，**不 open、不递增 server read seq**（不变量 3）。
            0x14 => continue,
            0x17 => {
                let (inner_type, content) = recv.open(&hdr, &payload)?;
                if inner_type != 0x16 {
                    return Err(err(format!("flight 阶段内层非 handshake（0x{inner_type:02x}）")));
                }
                for msg in reasm.push(&content) {
                    match msg[0] {
                        0x08 => transcript.extend_from_slice(&msg), // EncryptedExtensions
                        0x0b => {
                            // Certificate → **REALITY auth 决策**（verify_cert）。失败 → 握手 Err。
                            verify_cert(&msg)?;
                            cert_verified = true;
                            transcript.extend_from_slice(&msg);
                        }
                        0x0f => transcript.extend_from_slice(&msg), // CertificateVerify：折但不验签（ADR-0010）
                        0x14 => {
                            // 完成握手前强制：必须已通过 Certificate 的 REALITY auth（H1）。
                            if !cert_verified {
                                return Err(err(
                                    "server flight 未含通过校验的 Certificate → REALITY auth 未执行，拒绝握手",
                                ));
                            }
                            // server Finished：用 transcript(CH..CertVerify) 验 MAC，再折入。
                            let th = transcript_hash(&[&transcript]);
                            let expected = compute_finished_verify_data(&hs.s_hs_secret, &th);
                            let got = msg.get(4..).ok_or_else(|| err("server Finished 截断"))?;
                            if got != expected {
                                return Err(err("server Finished MAC 不匹配（握手完整性失败）"));
                            }
                            transcript.extend_from_slice(&msg);
                            break 'outer;
                        }
                        other => return Err(err(format!("flight 意外 handshake type 0x{other:02x}"))),
                    }
                }
            }
            other => return Err(err(format!("flight 阶段意外 record type 0x{other:02x}"))),
        }
    }

    // 5. 发 client Finished（c_hs @ write-seq 0；transcript=CH..serverFinished）。
    let th_sfin = transcript_hash(&[&transcript]);
    let cfin = compute_finished_verify_data(&hs.c_hs_secret, &th_sfin);
    let mut fin_msg = Vec::with_capacity(4 + 32);
    fin_msg.extend_from_slice(&[0x14, 0x00, 0x00, 0x20]);
    fin_msg.extend_from_slice(&cfin);
    let fin_record = send_hs.seal(0x16, &fin_msg);
    stream.write_all(&fin_record).await.map_err(|e| io_err("写 client Finished", e))?;
    stream.flush().await.map_err(|e| io_err("flush client Finished", e))?;

    // 6. app keys（read/write seq 各归零，新 RecordKeys）。
    let app = derive_application_keys(&hs.handshake_secret, &th_sfin);
    let recv_keys = RecordKeys::new(&app.server_key, &app.server_iv);
    let send_keys = RecordKeys::new(&app.client_key, &app.client_iv);

    Ok(HandshakeOutput {
        recv_keys,
        send_keys,
        leftover: reader.into_leftover(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::key_schedule::transcript_hash;
    use crate::reality::testutil::{arr32, hex};

    // RFC 8448 §3 ClientHello / ServerHello handshake message + server flight payload(657B = EE||Cert||CertVerify||Finished)。
    const RFC8448_CH: &str = "010000c00303cb34ecb1e78163ba1c38c6dacb196a6dffa21a8d9912ec18a2ef6283024dece7000006130113031302010000910000000b0009000006736572766572ff01000100000a00140012001d0017001800190100010101020103010400230000003300260024001d002099381de560e4bd43d23d8e435a7dbafeb3c06e51c13cae4d5413691e529aaf2c002b0003020304000d0020001e040305030603020308040805080604010501060102010402050206020202002d00020101001c00024001";
    const RFC8448_SH: &str = "020000560303a6af06a4121860dc5e6e60249cd34c95930c8ac5cb1434dac155772ed3e2692800130100002e00330024001d0020c9828876112095fe66762bdbf7c672e156d6cc253b833df1dd69b1b04e751f0f002b00020304";
    const SFLIGHT_PAYLOAD: &str = "080000240022000a00140012001d00170018001901000101010201030104001c00024001000000000b0001b9000001b50001b0308201ac30820115a003020102020102300d06092a864886f70d01010b0500300e310c300a06035504031303727361301e170d3136303733303031323335395a170d3236303733303031323335395a300e310c300a0603550403130372736130819f300d06092a864886f70d010101050003818d0030818902818100b4bb498f8279303d980836399b36c6988c0c68de55e1bdb826d3901a2461eafd2de49a91d015abbc9a95137ace6c1af19eaa6af98c7ced43120998e187a80ee0ccb0524b1b018c3e0b63264d449a6d38e22a5fda430846748030530ef0461c8ca9d9efbfae8ea6d1d03e2bd193eff0ab9a8002c47428a6d35a8d88d79f7f1e3f0203010001a31a301830090603551d1304023000300b0603551d0f0404030205a0300d06092a864886f70d01010b05000381810085aad2a0e5b9276b908c65f73a7267170618a54c5f8a7b337d2df7a594365417f2eae8f8a58c8f8172f9319cf36b7fd6c55b80f21a03015156726096fd335e5e67f2dbf102702e608ccae6bec1fc63a42a99be5c3eb7107c3c54e9b9eb2bd5203b1c3b84e0a8b2f759409ba3eac9d91d402dcc0cc8f8961229ac9187b42b4de100000f000084080400805a747c5d88fa9bd2e55ab085a61015b7211f824cd484145ab3ff52f1fda8477b0b7abc90db78e2d33a5c141a078653fa6bef780c5ea248eeaaa785c4f394cab6d30bbe8d4859ee511f602957b15411ac027671459e46445c9ea58c181e818e95b8c3fb0bf3278409d3be152a3da5043e063dda65cdf5aea20d53dfacd42f74f3140000209b9b141d906337fbd2cbdce71df4deda4ab42c309572cb7fffee5454b78f0718";

    /// 整段一次喂入 → 切出 4 条 message，类型 [EE 0x08, Cert 0x0b, CertVerify 0x0f, Finished 0x14]。
    #[test]
    fn reassembles_whole_flight() {
        let mut r = HandshakeReassembler::new();
        let msgs = r.push(&hex(SFLIGHT_PAYLOAD));
        assert!(r.is_empty(), "干净结束");
        let types: Vec<u8> = msgs.iter().map(|m| m[0]).collect();
        assert_eq!(types, vec![0x08, 0x0b, 0x0f, 0x14]);
    }

    /// **互通-critical KAT**：把 flight 人为切成 3 段跨喂入 → 重组的 EE/Cert/CertVerify 字节级正确，
    /// 由 `transcript_hash(CH,SH,EE,Cert,CV) == edb7725f…10ed`（RFC 8448 §3 server Finished 的 transcript）钉死。
    #[test]
    fn cross_record_reassembly_byte_exact() {
        let flight = hex(SFLIGHT_PAYLOAD);
        let mut r = HandshakeReassembler::new();
        // 3 段切点故意落在 message 内部（模拟跨 record 分片）。
        let mut msgs = Vec::new();
        for chunk in [&flight[..30], &flight[30..400], &flight[400..]] {
            msgs.extend(r.push(chunk));
        }
        assert!(r.is_empty());
        assert_eq!(msgs.len(), 4);
        // 取前 3 条（EE/Cert/CertVerify），与 CH/SH 一起算 transcript。
        let (ch, sh) = (hex(RFC8448_CH), hex(RFC8448_SH));
        let th = transcript_hash(&[&ch, &sh, &msgs[0], &msgs[1], &msgs[2]]);
        assert_eq!(
            th,
            arr32("edb7725fa7a3473b031ec8ef65a2485493900138a2b91291407d7951a06110ed"),
            "重组的 EE/Cert/CertVerify 字节须与 RFC 8448 一致"
        );
        // 重组结果拼回 == 原 flight（无丢字节/无错位）。
        let rejoined: Vec<u8> = msgs.concat();
        assert_eq!(rejoined, flight);
    }

    /// 逐字节喂入（极端分片）→ 仍切出 4 条完整 message。
    #[test]
    fn byte_by_byte_feed() {
        let flight = hex(SFLIGHT_PAYLOAD);
        let mut r = HandshakeReassembler::new();
        let mut msgs = Vec::new();
        for b in &flight {
            msgs.extend(r.push(std::slice::from_ref(b)));
        }
        assert_eq!(msgs.len(), 4);
        assert!(r.is_empty());
    }

    // RFC 8448 §3 server 加密握手 flight 的完整 on-wire record（679B，17 03 03 ... 含 5B 头）。
    const SFLIGHT_RECORD: &str = "17030302a2d1ff334a56f5bff6594a07cc87b580233f500f45e489e7f33af35edf7869fcf40aa40aa2b8ea73f848a7ca07612ef9f945cb960b4068905123ea78b111b429ba9191cd05d2a389280f526134aadc7fc78c4b729df828b5ecf7b13bd9aefb0e57f271585b8ea9bb355c7c79020716cfb9b1183ef3ab20e37d57a6b9d7477609aee6e122a4cf51427325250c7d0e509289444c9b3a648f1d71035d2ed65b0e3cdd0cbae8bf2d0b227812cbb360987255cc744110c453baa4fcd610928d809810e4b7ed1a8fd991f06aa6248204797e36a6a73b70a2559c09ead686945ba246ab66e5edd8044b4c6de3fcf2a89441ac66272fd8fb330ef8190579b3684596c960bd596eea520a56a8d650f563aad27409960dca63d3e688611ea5e22f4415cf9538d51a200c27034272968a264ed6540c84838d89f72c24461aad6d26f59ecaba9acbbb317b66d902f4f292a36ac1b639c637ce343117b659622245317b49eeda0c6258f100d7d961ffb138647e92ea330faeea6dfa31c7a84dc3bd7e1b7a6c7178af36879018e3f252107f243d243dc7339d5684c8b0378bf30244da8c87c843f5e56eb4c5e8280a2b48052cf93b16499a66db7cca71e4599426f7d461e66f99882bd89fc50800becca62d6c74116dbd2972fda1fa80f85df881edbe5a37668936b335583b599186dc5c6918a396fa48a181d6b6fa4f9d62d513afbb992f2b992f67f8afe67f76913fa388cb5630c8ca01e0c65d11c66a1e2ac4c85977b7c7a6999bbf10dc35ae69f5515614636c0b9b68c19ed2e31c0b3b66763038ebba42f3b38edc0399f3a9f23faa63978c317fc9fa66a73f60f0504de93b5b845e275592c12335ee340bbc4fddd502784016e4b3be7ef04dda49f4b440a30cb5d2af939828fd4ae3794e44f94df5a631ede42c1719bfdabf0253fe5175be898e750edc53370d2b";
    // RFC 8448 §3 客户端临时 X25519 私钥（产出 ECDHE 8bd4054f…）。
    const RFC8448_CLIENT_EPH: &str = "49af42ba7f7994852d713ef2784bcbcaa7911de26adc5642cb634540e7ea5005";

    fn arr16(s: &str) -> [u8; 16] {
        hex(s).try_into().unwrap()
    }
    fn arr12(s: &str) -> [u8; 12] {
        hex(s).try_into().unwrap()
    }

    /// **T6 拱心石 KAT**：用 RFC 8448 §3 真序列 + 测试内 server 模拟器（duplex）跑通整条客户端握手驱动。
    /// 跑通即一次性证明：ECDHE 接线 + 握手密钥 + CCS-skip（不计 read seq）+ flight 解密 + 跨 record 重组
    /// + server Finished 验证（transcript CH..CertVerify）+ client Finished 生成（transcript CH..serverFin）
    /// + app keys 派生（seq 归零）全部字节级正确——刀8 真握手就靠这条链。
    #[tokio::test]
    async fn rfc8448_handshake_drive_e2e() {
        use crate::reality::key_schedule::{compute_finished_verify_data, derive_application_keys};
        use crate::reality::record::RecordKeys;
        use tokio::io::AsyncWriteExt;

        let ch = hex(RFC8448_CH);
        let sh = hex(RFC8448_SH);
        let flight_record = hex(SFLIGHT_RECORD);
        let client_eph = arr32(RFC8448_CLIENT_EPH);

        let (mut client_io, mut server_io) = tokio::io::duplex(32768);

        let ch_for_sim = ch.clone();
        let sim = tokio::spawn(async move {
            // 1. 写 ServerHello(16 03 03) + server dummy CCS + 加密 flight record。
            let mut shrec = vec![0x16, 0x03, 0x03];
            shrec.extend_from_slice(&(sh.len() as u16).to_be_bytes());
            shrec.extend_from_slice(&sh);
            server_io.write_all(&shrec).await.unwrap();
            server_io.write_all(&[0x14, 0x03, 0x03, 0x00, 0x01, 0x01]).await.unwrap();
            server_io.write_all(&flight_record).await.unwrap();
            server_io.flush().await.unwrap();

            // 2. 读 client CH record + client CCS + client Finished record（验字节）。
            let mut rr = RecordReader::new();
            let (ct_ch, _h, got_ch) = rr.next(&mut server_io).await.unwrap();
            assert_eq!(ct_ch, 0x16);
            assert_eq!(got_ch, ch_for_sim, "client 发的 CH 字节 == 输入");
            let (ct_ccs, _h2, ccs_body) = rr.next(&mut server_io).await.unwrap();
            assert_eq!((ct_ccs, ccs_body.as_slice()), (0x14, &[0x01][..]), "client dummy CCS");
            let (ct_fin, fhdr, fpayload) = rr.next(&mut server_io).await.unwrap();
            assert_eq!(ct_fin, 0x17, "client Finished 是加密 record");

            // 用 c_hs key/iv（RFC 8448）解 client Finished，验 verify_data 字节正确。
            let mut crecv = RecordKeys::new(&arr16("dbfaa693d1762c5b666af5d950258d01"), &arr12("5bd3c71b836e0b76bb73265f"));
            let (inner, fin_msg) = crecv.open(&fhdr, &fpayload).unwrap();
            assert_eq!(inner, 0x16, "client Finished 内层 handshake");
            let th_sfin = arr32("9608102a0f1ccc6db6250b7b7e417b1a000eaada3daae4777a7686c9ff83df13");
            let c_hs = arr32("b3eddb126e067f35a780b3abf45e2d8f3b1a950738f52e9600746a0e27a55a21");
            let mut want = vec![0x14, 0x00, 0x00, 0x20];
            want.extend_from_slice(&compute_finished_verify_data(&c_hs, &th_sfin));
            assert_eq!(fin_msg, want, "client Finished verify_data 字节正确");

            // 3. app round-trip：用 RFC 8448 published handshake_secret + th_sfin 独立派生 app keys。
            let handshake_secret = arr32("1dc826e93606aa6fdc0aadc12f741b01046aa6b99f691ed221a9f0ca043fbeac");
            let app = derive_application_keys(&handshake_secret, &th_sfin);
            let mut s_ap = RecordKeys::new(&app.server_key, &app.server_iv);
            server_io.write_all(&s_ap.seal(0x17, b"server-app-hello")).await.unwrap();
            server_io.flush().await.unwrap();
            let (ct_a, ahdr, apayload) = rr.next(&mut server_io).await.unwrap();
            assert_eq!(ct_a, 0x17);
            let mut c_ap = RecordKeys::new(&app.client_key, &app.client_iv);
            let (it, content) = c_ap.open(&ahdr, &apayload).unwrap();
            assert_eq!((it, content.as_slice()), (0x17, &b"client-app-hi"[..]), "client app data 经 send_keys 正确");
        });

        // client：跑 drive（KAT 用 always-ok verify_cert——RFC 8448 cert 是 RSA 不走 ed25519 HMAC）。
        let input = HandshakeInput {
            client_hello: ch.clone(),
            client_eph_secret: client_eph,
            expected_session_id: vec![], // RFC 8448 CH 空 session_id
        };
        let mut out = drive(&mut client_io, input, |_cert| Ok(())).await.expect("握手应成功");

        // recv_keys(app) 解 server 的 app record；send_keys(app) 封 client app record。
        let mut rr_app = RecordReader::with_leftover(out.leftover);
        let (ct, hdr, payload) = rr_app.next(&mut client_io).await.unwrap();
        assert_eq!(ct, 0x17);
        let (it, content) = out.recv_keys.open(&hdr, &payload).unwrap();
        assert_eq!((it, content.as_slice()), (0x17, &b"server-app-hello"[..]), "recv_keys 字节级正确（app server key/iv + seq0）");
        client_io.write_all(&out.send_keys.seal(0x17, b"client-app-hi")).await.unwrap();
        client_io.flush().await.unwrap();

        sim.await.unwrap();
    }

    // RFC 8448 §3 EncryptedExtensions message（供 H1 负向测试构造「无 Certificate」的 flight）。
    const RFC8448_EE: &str = "080000240022000a00140012001d00170018001901000101010201030104001c0002400100000000";

    /// **H1 安全守卫负向 KAT**：server flight 只发 EE + Finished（无 Certificate），transcript 自洽、
    /// server Finished MAC 正确（ECDHE 派生，与 REALITY 静态 pbk 无关）——模拟被回落的 decoy。
    /// 即便 verify_cert=always-ok，drive 也**必须拒绝**（verify_cert 从未执行 → REALITY auth 未发生）。
    #[tokio::test]
    async fn drive_rejects_flight_without_certificate() {
        use crate::reality::key_schedule::compute_finished_verify_data;
        use crate::reality::record::RecordKeys;
        use tokio::io::AsyncWriteExt;

        let ch = hex(RFC8448_CH);
        let sh = hex(RFC8448_SH);
        let ee = hex(RFC8448_EE);
        // 恶意 flight = EE || Finished(MAC over transcript CH..EE，无 Cert/CV)。
        let s_hs = arr32("b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38");
        let th = transcript_hash(&[&ch, &sh, &ee]);
        let mut flight = ee.clone();
        flight.extend_from_slice(&[0x14, 0x00, 0x00, 0x20]);
        flight.extend_from_slice(&compute_finished_verify_data(&s_hs, &th));

        let (mut client_io, mut server_io) = tokio::io::duplex(16384);
        let sim = tokio::spawn(async move {
            let mut shrec = vec![0x16, 0x03, 0x03];
            shrec.extend_from_slice(&(sh.len() as u16).to_be_bytes());
            shrec.extend_from_slice(&sh);
            server_io.write_all(&shrec).await.unwrap();
            server_io.write_all(&[0x14, 0x03, 0x03, 0x00, 0x01, 0x01]).await.unwrap();
            let server_key: [u8; 16] = hex("3fce516009c21727d0f2e4e86ee403bc").try_into().unwrap();
            let server_iv: [u8; 12] = hex("5d313eb2671276ee13000b30").try_into().unwrap();
            let mut s_hs_rk = RecordKeys::new(&server_key, &server_iv);
            server_io.write_all(&s_hs_rk.seal(0x16, &flight)).await.unwrap();
            server_io.flush().await.unwrap();
        });

        let input = HandshakeInput {
            client_hello: ch.clone(),
            client_eph_secret: arr32(RFC8448_CLIENT_EPH),
            expected_session_id: vec![],
        };
        // verify_cert=always-ok：证明拒绝不是因为 verify_cert 失败，而是 Certificate 根本没出现。
        let r = drive(&mut client_io, input, |_cert| Ok(())).await;
        assert!(r.is_err(), "无 Certificate 的 flight 必须拒绝（REALITY auth 未执行）");
        let _ = sim.await;
    }

    /// 负：首条 record 非 ServerHello → Err（不 panic）。
    #[tokio::test]
    async fn rejects_non_serverhello_first() {
        use tokio::io::AsyncWriteExt;
        let (mut client_io, mut server_io) = tokio::io::duplex(4096);
        let sim = tokio::spawn(async move {
            // 写一条 app-data record 当首条（非 0x16）。
            server_io.write_all(&[0x17, 0x03, 0x03, 0x00, 0x01, 0x00]).await.unwrap();
            // 读掉 client 的 CH/CCS 防止其 write 阻塞（buffer 够大其实不阻塞）。
            let mut buf = [0u8; 512];
            let _ = tokio::io::AsyncReadExt::read(&mut server_io, &mut buf).await;
        });
        let input = HandshakeInput { client_hello: hex(RFC8448_CH), client_eph_secret: [1u8; 32], expected_session_id: vec![] };
        let r = drive(&mut client_io, input, |_| Ok(())).await;
        assert!(r.is_err(), "首条非 SH → Err");
        let _ = sim.await;
    }

    /// 负：server 立即关闭（EOF）→ Err（不 panic）。
    #[tokio::test]
    async fn early_eof_errs() {
        let (mut client_io, server_io) = tokio::io::duplex(4096);
        drop(server_io); // 立即 EOF
        let input = HandshakeInput { client_hello: hex(RFC8448_CH), client_eph_secret: [1u8; 32], expected_session_id: vec![] };
        let r = drive(&mut client_io, input, |_| Ok(())).await;
        assert!(r.is_err(), "EOF → Err");
    }
}
