//! TLS 1.3 record-layer AEAD（刀7，RFC 8446 §5.2/§5.3/§5.4，sans-IO；AES-128-GCM 先 wire，泛型-over-cipher）。
//!
//! 中文要点：per-record nonce（write_iv XOR seq，seq 右对齐进低 8B → nonce[4..12]）、AAD=5B record 头
//! `[0x17,0x03,0x03,len_hi,len_lo]`（len=**密文含 16B tag**，off-by-16 风险）、inner=content||content_type||zero-pad、
//! open 剥尾零→最后非零字节=真 content type、全零明文→Err、读/写**两个独立 seq**（密钥切换时各归零）。
//! KAT：seq=0 时 nonce==iv；round-trip；RFC 8448 §3 server-flight record open golden KAT（见本刀 plan T5）。

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes128Gcm, Nonce};
use crate::shared::ClientError;

/// TLS 1.3 record 上限（RFC 8446 §5.2，单一事实源；handshake/reality_upstream 共用，避免常量漂移，L5）：
/// 明文 2^14；密文 = 明文 + 256（AEAD tag + 内层 type + padding 余量）。读路径据此防恶意巨 length 无界分配，
/// 写路径据此防 `as u16` 静默截断。
pub(crate) const MAX_TLS_PLAINTEXT: usize = 16384;
pub(crate) const MAX_TLS_RECORD: usize = 16640;

/// per-record nonce（RFC 8446 §5.3）：seq（u64 big-endian，8B）右对齐 XOR 进 12B iv 的低 8B（nonce[4..12]）。
/// seq=0 时 nonce==iv（首条 sanity）。
pub fn per_record_nonce(iv: &[u8; 12], seq: u64) -> [u8; 12] {
    let mut nonce = *iv;
    let seq_be = seq.to_be_bytes();
    for i in 0..8 {
        nonce[4 + i] ^= seq_be[i];
    }
    nonce
}

/// TLS 1.3 密文 record 头（RFC 8446 §5.2）：`[0x17,0x03,0x03,len_hi,len_lo]`。
/// 中文要点：`enc_len` = **密文长（含 16B GCM tag）**，不是明文长（off-by-16 风险）；它既是 wire 头也是 AEAD 的 AAD。
pub fn record_header(enc_len: u16) -> [u8; 5] {
    let [hi, lo] = enc_len.to_be_bytes();
    [0x17, 0x03, 0x03, hi, lo]
}

/// 单方向的 record 保护状态（cipher + iv + 自增 seq）。
/// 中文要点：每方向**独立** seq（client 发用一份、收 server 用另一份），密钥切换（hs→app）时各建新实例从 seq=0 起。
pub struct RecordKeys {
    cipher: Aes128Gcm,
    iv: [u8; 12],
    seq: u64,
}

impl RecordKeys {
    /// 从 16B AES-128 key + 12B iv 建（seq 从 0 起）。
    pub fn new(key: &[u8; 16], iv: &[u8; 12]) -> Self {
        use aes_gcm::KeyInit;
        Self {
            cipher: Aes128Gcm::new_from_slice(key).expect("16B AES-128 key"),
            iv: *iv,
            seq: 0,
        }
    }

    /// 封一条 record：inner = content || content_type（本刀不加 padding）；返回 `5B 头 || 密文(含 tag)`。
    pub fn seal(&mut self, content_type: u8, content: &[u8]) -> Vec<u8> {
        let mut inner = Vec::with_capacity(content.len() + 1);
        inner.extend_from_slice(content);
        inner.push(content_type);
        let enc_len = (inner.len() + 16) as u16; // +16B GCM tag
        let header = record_header(enc_len);
        let nonce = per_record_nonce(&self.iv, self.seq);
        let ct = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), Payload { msg: &inner, aad: &header })
            .expect("AES-128-GCM encrypt infallible for valid key/nonce");
        self.seq = self.seq.checked_add(1).expect("record seq 溢出");
        let mut out = Vec::with_capacity(5 + ct.len());
        out.extend_from_slice(&header);
        out.extend_from_slice(&ct);
        out
    }

    /// 开一条 record：解密 → 剥尾零 → 最后非零字节 = 真 content_type，其余 = content。
    /// 认证失败 / 全零明文 → Err。AAD = 传入的 `header`（收到的 5B）。
    pub fn open(&mut self, header: &[u8; 5], encrypted_record: &[u8]) -> Result<(u8, Vec<u8>), ClientError> {
        let nonce = per_record_nonce(&self.iv, self.seq);
        let mut inner = self
            .cipher
            .decrypt(Nonce::from_slice(&nonce), Payload { msg: encrypted_record, aad: header })
            .map_err(|_| ClientError::Reality("record 解密/认证失败".into()))?;
        self.seq = self.seq.checked_add(1).expect("record seq 溢出");
        while inner.last() == Some(&0) {
            inner.pop();
        }
        let content_type = inner
            .pop()
            .ok_or_else(|| ClientError::Reality("record 明文全零（无 content type）".into()))?;
        Ok((content_type, inner))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::hex;

    /// seq=0 → nonce==iv；seq 跨字节 XOR 进低 8B。
    #[test]
    fn nonce_construction() {
        let iv = [0x5d, 0x31, 0x3e, 0xb2, 0x67, 0x12, 0x76, 0xee, 0x13, 0x00, 0x0b, 0x30];
        assert_eq!(per_record_nonce(&iv, 0), iv, "seq=0 → nonce==iv");
        let zero = [0u8; 12];
        let mut e1 = [0u8; 12];
        e1[11] = 1;
        assert_eq!(per_record_nonce(&zero, 1), e1);
        let mut e2 = [0u8; 12];
        e2[10] = 1;
        e2[11] = 2;
        assert_eq!(per_record_nonce(&zero, 0x0102), e2, "seq 右对齐进 nonce[4..12]");
    }

    /// record 头：版本 0x17 0x0303 + 密文长(含 tag)。
    #[test]
    fn header_encodes_ciphertext_len() {
        assert_eq!(record_header(0x0123), [0x17, 0x03, 0x03, 0x01, 0x23]);
    }

    /// seal→open round-trip 还原 (content_type, content)；两方向独立 seq 顺序处理。
    #[test]
    fn seal_open_roundtrip() {
        let (key, iv) = ([0x11u8; 16], [0x22u8; 12]);
        let mut send = RecordKeys::new(&key, &iv);
        let mut recv = RecordKeys::new(&key, &iv);
        for (ct, msg) in [(0x16u8, &b"hello handshake"[..]), (0x17, b"app data")] {
            let rec = send.seal(ct, msg);
            assert_eq!(&rec[..3], &[0x17, 0x03, 0x03]);
            let header: [u8; 5] = rec[..5].try_into().unwrap();
            let (got_ct, content) = recv.open(&header, &rec[5..]).expect("open ok");
            assert_eq!(got_ct, ct);
            assert_eq!(content, msg);
        }
    }

    /// 篡改密文/tag → 认证失败 Err。
    #[test]
    fn tampered_record_rejected() {
        let (key, iv) = ([1u8; 16], [2u8; 12]);
        let mut send = RecordKeys::new(&key, &iv);
        let mut recv = RecordKeys::new(&key, &iv);
        let mut rec = send.seal(0x17, b"secret");
        let last = rec.len() - 1;
        rec[last] ^= 0xff;
        let header: [u8; 5] = rec[..5].try_into().unwrap();
        assert!(recv.open(&header, &rec[5..]).is_err(), "篡改 tag → Err");
    }

    /// 全零明文（content="" + type=0x00）→ open 剥光 → Err（不 panic）。
    #[test]
    fn all_zero_plaintext_rejected() {
        let (key, iv) = ([1u8; 16], [2u8; 12]);
        let mut send = RecordKeys::new(&key, &iv);
        let mut recv = RecordKeys::new(&key, &iv);
        let rec = send.seal(0x00, b""); // inner = [0x00]
        let header: [u8; 5] = rec[..5].try_into().unwrap();
        assert!(recv.open(&header, &rec[5..]).is_err(), "全零明文应 Err");
    }

    // RFC 8448 §3：server 加密握手 flight（679B on-wire record）+ 其明文 payload（657B：EE||Cert||CertVerify||Finished）。
    // 脚本从 RFC 文本抽取、字节数核对（679/657）。这是 schedule+AEAD 字节级一致的最强离线证明。
    const SFLIGHT_RECORD: &str = "17030302a2d1ff334a56f5bff6594a07cc87b580233f500f45e489e7f33af35edf7869fcf40aa40aa2b8ea73f848a7ca07612ef9f945cb960b4068905123ea78b111b429ba9191cd05d2a389280f526134aadc7fc78c4b729df828b5ecf7b13bd9aefb0e57f271585b8ea9bb355c7c79020716cfb9b1183ef3ab20e37d57a6b9d7477609aee6e122a4cf51427325250c7d0e509289444c9b3a648f1d71035d2ed65b0e3cdd0cbae8bf2d0b227812cbb360987255cc744110c453baa4fcd610928d809810e4b7ed1a8fd991f06aa6248204797e36a6a73b70a2559c09ead686945ba246ab66e5edd8044b4c6de3fcf2a89441ac66272fd8fb330ef8190579b3684596c960bd596eea520a56a8d650f563aad27409960dca63d3e688611ea5e22f4415cf9538d51a200c27034272968a264ed6540c84838d89f72c24461aad6d26f59ecaba9acbbb317b66d902f4f292a36ac1b639c637ce343117b659622245317b49eeda0c6258f100d7d961ffb138647e92ea330faeea6dfa31c7a84dc3bd7e1b7a6c7178af36879018e3f252107f243d243dc7339d5684c8b0378bf30244da8c87c843f5e56eb4c5e8280a2b48052cf93b16499a66db7cca71e4599426f7d461e66f99882bd89fc50800becca62d6c74116dbd2972fda1fa80f85df881edbe5a37668936b335583b599186dc5c6918a396fa48a181d6b6fa4f9d62d513afbb992f2b992f67f8afe67f76913fa388cb5630c8ca01e0c65d11c66a1e2ac4c85977b7c7a6999bbf10dc35ae69f5515614636c0b9b68c19ed2e31c0b3b66763038ebba42f3b38edc0399f3a9f23faa63978c317fc9fa66a73f60f0504de93b5b845e275592c12335ee340bbc4fddd502784016e4b3be7ef04dda49f4b440a30cb5d2af939828fd4ae3794e44f94df5a631ede42c1719bfdabf0253fe5175be898e750edc53370d2b";
    const SFLIGHT_PAYLOAD: &str = "080000240022000a00140012001d00170018001901000101010201030104001c00024001000000000b0001b9000001b50001b0308201ac30820115a003020102020102300d06092a864886f70d01010b0500300e310c300a06035504031303727361301e170d3136303733303031323335395a170d3236303733303031323335395a300e310c300a0603550403130372736130819f300d06092a864886f70d010101050003818d0030818902818100b4bb498f8279303d980836399b36c6988c0c68de55e1bdb826d3901a2461eafd2de49a91d015abbc9a95137ace6c1af19eaa6af98c7ced43120998e187a80ee0ccb0524b1b018c3e0b63264d449a6d38e22a5fda430846748030530ef0461c8ca9d9efbfae8ea6d1d03e2bd193eff0ab9a8002c47428a6d35a8d88d79f7f1e3f0203010001a31a301830090603551d1304023000300b0603551d0f0404030205a0300d06092a864886f70d01010b05000381810085aad2a0e5b9276b908c65f73a7267170618a54c5f8a7b337d2df7a594365417f2eae8f8a58c8f8172f9319cf36b7fd6c55b80f21a03015156726096fd335e5e67f2dbf102702e608ccae6bec1fc63a42a99be5c3eb7107c3c54e9b9eb2bd5203b1c3b84e0a8b2f759409ba3eac9d91d402dcc0cc8f8961229ac9187b42b4de100000f000084080400805a747c5d88fa9bd2e55ab085a61015b7211f824cd484145ab3ff52f1fda8477b0b7abc90db78e2d33a5c141a078653fa6bef780c5ea248eeaaa785c4f394cab6d30bbe8d4859ee511f602957b15411ac027671459e46445c9ea58c181e818e95b8c3fb0bf3278409d3be152a3da5043e063dda65cdf5aea20d53dfacd42f74f3140000209b9b141d906337fbd2cbdce71df4deda4ab42c309572cb7fffee5454b78f0718";

    /// 刀7 最强 golden KAT：用 RFC 8448 §3 server handshake key/iv（来自 key_schedule KAT）@read-seq 0
    /// open 真 server flight record → content_type==0x16(handshake)、明文 == 657B payload（起始 EncryptedExtensions 0x08）。
    /// 跑通即证明 key schedule + record AEAD 字节级一致——刀8 解密真 server flight 就靠这条链。
    #[test]
    fn rfc8448_server_flight_record_open() {
        let key: [u8; 16] = hex("3fce516009c21727d0f2e4e86ee403bc").try_into().unwrap();
        let iv: [u8; 12] = hex("5d313eb2671276ee13000b30").try_into().unwrap();
        let mut recv = RecordKeys::new(&key, &iv);

        let record = hex(SFLIGHT_RECORD);
        let header: [u8; 5] = record[..5].try_into().unwrap();
        let (content_type, plaintext) = recv.open(&header, &record[5..]).expect("server flight 应解开");

        assert_eq!(content_type, 0x16, "外层解密后内层 content type = handshake");
        assert_eq!(plaintext, hex(SFLIGHT_PAYLOAD), "明文 == RFC 8448 657B payload");
        assert_eq!(&plaintext[..4], &[0x08, 0x00, 0x00, 0x24], "起始 EncryptedExtensions");
    }
}
