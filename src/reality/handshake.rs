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
}
