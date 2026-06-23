//! TLS 1.3 key schedule（刀7，RFC 8446 §7.1/§7.3，sans-IO 纯函数；泛型-over-hash，SHA-256 先 wire）。
//!
//! 中文要点：HKDF-Expand-Label / Derive-Secret / 握手阶段密钥链（Early→derived→Handshake(from ECDHE)→
//! {c,s}_hs_traffic→key/iv）+ `compute_finished_verify_data`。KAT 用 RFC 8448 §3（见 testutil + 本刀 plan）。
//! ⚠️ 这里的 ECDHE = x25519(client 临时, **server 临时** keyshare from ServerHello)，与刀6 REALITY AuthKey 的
//! x25519(client 临时, **server 静态** pbk) 是**不同**密钥——别接错（接错不过 KAT 但破活握手）。
//! ⚠️ network 来的 keyshare 不可信：Extract 前必须拒绝全零/非贡献点 ECDHE（见 auth.rs `x25519_shared_secret` 注）。

use crate::shared::ClientError;
use hkdf::Hkdf;
use sha2::{Digest, Sha256};

/// 握手阶段密钥材料（TLS_AES_128_GCM_SHA256：key 16B / iv 12B）。
/// 中文要点：secret 字段留给刀8 算 Finished（`compute_finished_verify_data`）；key/iv 给 record 层 open/seal。
pub struct HsKeys {
    pub c_hs_secret: [u8; 32],
    pub s_hs_secret: [u8; 32],
    pub client_key: [u8; 16],
    pub server_key: [u8; 16],
    pub client_iv: [u8; 12],
    pub server_iv: [u8; 12],
}

/// 跑握手阶段密钥链（RFC 8446 §7.1）：Early→derived→Handshake(from ECDHE)→{c,s}_hs_traffic→key/iv。
/// 中文要点：`ecdhe` = x25519(client 临时, **server 临时** keyshare from ServerHello)——与刀6 AuthKey 的
/// (client 临时 × server **静态** pbk) 不同，别接错。`client_hello`/`server_hello` 是 handshake message 字节
/// （transcript = hash(CH || SH)）。**network keyshare 不可信**：全零 ECDHE（低阶点/非贡献点）→ Err（Extract 前）。
pub fn derive_handshake_keys(
    ecdhe: &[u8; 32],
    client_hello: &[u8],
    server_hello: &[u8],
) -> Result<HsKeys, ClientError> {
    if ecdhe == &[0u8; 32] {
        return Err(ClientError::Reality(
            "ECDHE 共享密钥全零（低阶点/非贡献点）→ 拒绝握手".into(),
        ));
    }
    let early = extract(&[0u8; 32], &[0u8; 32]);
    let derived1 = derive_secret(&early, "derived", &transcript_hash(&[]));
    let handshake = extract(&derived1, ecdhe);
    let th = transcript_hash(&[client_hello, server_hello]);
    let c_hs = derive_secret(&handshake, "c hs traffic", &th);
    let s_hs = derive_secret(&handshake, "s hs traffic", &th);
    let to16 = |v: Vec<u8>| -> [u8; 16] { v.try_into().expect("key 16B") };
    let to12 = |v: Vec<u8>| -> [u8; 12] { v.try_into().expect("iv 12B") };
    Ok(HsKeys {
        client_key: to16(expand_label(&c_hs, "key", b"", 16)),
        server_key: to16(expand_label(&s_hs, "key", b"", 16)),
        client_iv: to12(expand_label(&c_hs, "iv", b"", 12)),
        server_iv: to12(expand_label(&s_hs, "iv", b"", 12)),
        c_hs_secret: c_hs,
        s_hs_secret: s_hs,
    })
}

/// HkdfLabel 编码（RFC 8446 §7.1）：`length(u16) || u8len("tls13 "+label) || u8len(context)+context`。
/// 中文要点（**#1 静默互通杀手**）：`tls13 ` **含尾空格**；label 与 context 各用 **u8** 长前缀；仅顶层 length 是 u16。
pub fn hkdf_label(length: u16, label: &str, context: &[u8]) -> Vec<u8> {
    let full_label = [b"tls13 ".as_slice(), label.as_bytes()].concat();
    let mut out = Vec::with_capacity(2 + 1 + full_label.len() + 1 + context.len());
    out.extend_from_slice(&length.to_be_bytes());
    out.push(full_label.len() as u8);
    out.extend_from_slice(&full_label);
    out.push(context.len() as u8);
    out.extend_from_slice(context);
    out
}

/// HKDF-Expand-Label(secret, label, context, length)（RFC 8446 §7.1）。secret 作 PRK。
pub fn expand_label(secret: &[u8; 32], label: &str, context: &[u8], length: usize) -> Vec<u8> {
    let info = hkdf_label(length as u16, label, context);
    let hk = Hkdf::<Sha256>::from_prk(secret).expect("32B PRK ≥ HashLen");
    let mut okm = vec![0u8; length];
    hk.expand(&info, &mut okm).expect("length within HKDF-SHA256 limit");
    okm
}

/// Derive-Secret(Secret, Label, transcript_hash) = Expand-Label(Secret, Label, transcript_hash, Hash.length=32)。
pub fn derive_secret(secret: &[u8; 32], label: &str, transcript_hash: &[u8; 32]) -> [u8; 32] {
    expand_label(secret, label, transcript_hash, 32)
        .try_into()
        .expect("expand_label(.,32) 返回 32 字节")
}

/// HKDF-Extract(salt, IKM) → PRK（32B）。salt 全零等价于 None（HKDF 语义）。
pub fn extract(salt: &[u8], ikm: &[u8]) -> [u8; 32] {
    let (prk, _hk) = Hkdf::<Sha256>::extract(Some(salt), ikm);
    let mut out = [0u8; 32];
    out.copy_from_slice(&prk);
    out
}

/// Finished verify_data（RFC 8446 §4.4.4）= HMAC-SHA256(finished_key, transcript_hash)，
/// 其中 finished_key = Expand-Label(base_secret, "finished", "", 32)。base_secret = 对应方向的 hs_traffic_secret。
/// 中文要点（刀8 用）：server 用 s_hs + transcript(CH..CertificateVerify) 算出的应等于其 Finished 内容；
/// client 用 c_hs + transcript(CH..serverFinished) 算出自己的 Finished。本刀提供纯函数 + RFC 8448 KAT。
pub fn compute_finished_verify_data(base_secret: &[u8; 32], transcript_hash: &[u8; 32]) -> [u8; 32] {
    use hmac::{Hmac, Mac};
    let finished_key = expand_label(base_secret, "finished", b"", 32);
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&finished_key).expect("HMAC key any len");
    mac.update(transcript_hash);
    let mut out = [0u8; 32];
    out.copy_from_slice(&mac.finalize().into_bytes());
    out
}

/// Transcript-Hash = SHA-256(msg1 || msg2 || ...)（RFC 8446 §4.4.1）。
pub fn transcript_hash(msgs: &[&[u8]]) -> [u8; 32] {
    let mut h = Sha256::new();
    for m in msgs {
        h.update(m);
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(&h.finalize());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::{arr32, hex};

    /// HkdfLabel 字节编码（RFC 8446 §7.1）——`tls13 ` 含尾空格 + u8 前缀。
    #[test]
    fn hkdf_label_encoding() {
        assert_eq!(hkdf_label(16, "key", b""), hex("00 10 09 74 6c 73 31 33 20 6b 65 79 00"));
        assert_eq!(hkdf_label(12, "iv", b""), hex("00 0c 08 74 6c 73 31 33 20 69 76 00"));
        assert_eq!(
            hkdf_label(32, "finished", b""),
            hex("00 20 0e 74 6c 73 31 33 20 66 69 6e 69 73 68 65 64 00")
        );
    }

    /// SHA-256("") —— derive_secret(Early,"derived","") 的 context。
    #[test]
    fn sha256_empty() {
        assert_eq!(
            transcript_hash(&[]),
            arr32("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
        );
    }

    /// RFC 8448 §3 端到端：Early Secret = Extract(0,0)；derive_secret(early,"derived",SHA256(""))。
    /// 这条 KAT 一次性验穿 HkdfLabel 编码 + expand_label + derive_secret + extract。
    #[test]
    fn rfc8448_early_and_derived() {
        let early = extract(&[0u8; 32], &[0u8; 32]);
        assert_eq!(
            early,
            arr32("33ad0a1c607ec03b09e6cd9893680ce210adf300aa1f2660e1b22e10f170f92a"),
            "Early Secret"
        );
        let empty_hash = transcript_hash(&[]);
        let derived = derive_secret(&early, "derived", &empty_hash);
        assert_eq!(
            derived,
            arr32("6f2615a108c702c5678f54fc9dbab69716c076189c48250cebeac3576c3611ba"),
            "derived secret（端到端验 HkdfLabel）"
        );
    }

    // RFC 8448 §3 "Simple 1-RTT Handshake" 的 ClientHello / ServerHello handshake message 字节。
    const RFC8448_CH: &str = "010000c00303cb34ecb1e78163ba1c38c6dacb196a6dffa21a8d9912ec18a2ef6283024dece7000006130113031302010000910000000b0009000006736572766572ff01000100000a00140012001d0017001800190100010101020103010400230000003300260024001d002099381de560e4bd43d23d8e435a7dbafeb3c06e51c13cae4d5413691e529aaf2c002b0003020304000d0020001e040305030603020308040805080604010501060102010402050206020202002d00020101001c00024001";
    const RFC8448_SH: &str = "020000560303a6af06a4121860dc5e6e60249cd34c95930c8ac5cb1434dac155772ed3e2692800130100002e00330024001d0020c9828876112095fe66762bdbf7c672e156d6cc253b833df1dd69b1b04e751f0f002b00020304";
    const RFC8448_ECDHE: &str =
        "8bd4054fb55b9d63fdfbacf9f04b9f0d35e6d63f537563efd46272900f89492d";

    /// RFC 8448 §3 transcript hash(CH || SH) —— {c,s}_hs_traffic 的 context。
    #[test]
    fn rfc8448_transcript_ch_sh() {
        let ch = hex(RFC8448_CH);
        let sh = hex(RFC8448_SH);
        assert_eq!(
            transcript_hash(&[&ch, &sh]),
            arr32("860c06edc07858ee8e78f0e7428c58edd6b43f2ca3e6e95f02ed063cf0e1cad8")
        );
    }

    /// RFC 8448 §3 完整握手密钥链：喂 ECDHE+CH+SH → {c,s}_hs secret + server/client key/iv 全部字节级命中。
    #[test]
    fn rfc8448_handshake_key_schedule() {
        let ks = derive_handshake_keys(&arr32(RFC8448_ECDHE), &hex(RFC8448_CH), &hex(RFC8448_SH))
            .expect("KAT 应成功");
        assert_eq!(
            ks.c_hs_secret,
            arr32("b3eddb126e067f35a780b3abf45e2d8f3b1a950738f52e9600746a0e27a55a21"),
            "client_handshake_traffic_secret"
        );
        assert_eq!(
            ks.s_hs_secret,
            arr32("b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38"),
            "server_handshake_traffic_secret"
        );
        assert_eq!(&ks.server_key[..], &hex("3fce516009c21727d0f2e4e86ee403bc")[..], "server key");
        assert_eq!(&ks.server_iv[..], &hex("5d313eb2671276ee13000b30")[..], "server iv");
        assert_eq!(&ks.client_key[..], &hex("dbfaa693d1762c5b666af5d950258d01")[..], "client key");
        assert_eq!(&ks.client_iv[..], &hex("5bd3c71b836e0b76bb73265f")[..], "client iv");
    }

    /// network keyshare 不可信：全零 ECDHE（低阶点/非贡献点）→ Err（在 Extract 前）。
    #[test]
    fn zero_ecdhe_rejected() {
        let err = derive_handshake_keys(&[0u8; 32], &hex(RFC8448_CH), &hex(RFC8448_SH));
        assert!(err.is_err(), "全零 ECDHE 必须拒绝");
    }

    // RFC 8448 §3 server flight 后续 handshake messages（脚本从 RFC 文本抽取、字节数核对 40/445/136）。
    const RFC8448_EE: &str = "080000240022000a00140012001d00170018001901000101010201030104001c0002400100000000";
    const RFC8448_CERT: &str = "0b0001b9000001b50001b0308201ac30820115a003020102020102300d06092a864886f70d01010b0500300e310c300a06035504031303727361301e170d3136303733303031323335395a170d3236303733303031323335395a300e310c300a0603550403130372736130819f300d06092a864886f70d010101050003818d0030818902818100b4bb498f8279303d980836399b36c6988c0c68de55e1bdb826d3901a2461eafd2de49a91d015abbc9a95137ace6c1af19eaa6af98c7ced43120998e187a80ee0ccb0524b1b018c3e0b63264d449a6d38e22a5fda430846748030530ef0461c8ca9d9efbfae8ea6d1d03e2bd193eff0ab9a8002c47428a6d35a8d88d79f7f1e3f0203010001a31a301830090603551d1304023000300b0603551d0f0404030205a0300d06092a864886f70d01010b05000381810085aad2a0e5b9276b908c65f73a7267170618a54c5f8a7b337d2df7a594365417f2eae8f8a58c8f8172f9319cf36b7fd6c55b80f21a03015156726096fd335e5e67f2dbf102702e608ccae6bec1fc63a42a99be5c3eb7107c3c54e9b9eb2bd5203b1c3b84e0a8b2f759409ba3eac9d91d402dcc0cc8f8961229ac9187b42b4de10000";
    const RFC8448_CV: &str = "0f000084080400805a747c5d88fa9bd2e55ab085a61015b7211f824cd484145ab3ff52f1fda8477b0b7abc90db78e2d33a5c141a078653fa6bef780c5ea248eeaaa785c4f394cab6d30bbe8d4859ee511f602957b15411ac027671459e46445c9ea58c181e818e95b8c3fb0bf3278409d3be152a3da5043e063dda65cdf5aea20d53dfacd42f74f3";

    /// finished_key = Expand-Label(s_hs,"finished","",32) —— RFC 8448 §3 直接给出的中间值。
    #[test]
    fn rfc8448_finished_key() {
        let s_hs = arr32("b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38");
        assert_eq!(
            expand_label(&s_hs, "finished", b"", 32),
            hex("008d3b66f816ea559f96b537e885c31fc068bf492c652f01f288a1d8cdc19fc8"),
            "server finished_key"
        );
        let c_hs = arr32("b3eddb126e067f35a780b3abf45e2d8f3b1a950738f52e9600746a0e27a55a21");
        assert_eq!(
            expand_label(&c_hs, "finished", b"", 32),
            hex("b80ad01015fb2f0bd65ff7d4da5d6bf83f84821d1f87fdc7d3c75b5a7b42d9c4"),
            "client finished_key"
        );
    }

    /// 最强端到端 KAT：用 RFC 8448 §3 真 server flight 算 server Finished verify_data。
    /// 跑通即证明 compute_finished_verify_data（finished_key + HMAC over transcript）字节级正确——刀8 验证服务端 Finished 就靠它。
    #[test]
    fn rfc8448_server_finished_verify_data() {
        let s_hs = arr32("b67b7d690cc16c4e75e54213cb2d37b4e9c912bcded9105d42befd59d391ad38");
        let (ch, sh) = (hex(RFC8448_CH), hex(RFC8448_SH));
        let (ee, cert, cv) = (hex(RFC8448_EE), hex(RFC8448_CERT), hex(RFC8448_CV));
        // server Finished 的 transcript = CH..CertificateVerify。
        let th = transcript_hash(&[&ch, &sh, &ee, &cert, &cv]);
        assert_eq!(
            th,
            arr32("edb7725fa7a3473b031ec8ef65a2485493900138a2b91291407d7951a06110ed"),
            "transcript_hash(CH..CertVerify)"
        );
        assert_eq!(
            compute_finished_verify_data(&s_hs, &th),
            arr32("9b9b141d906337fbd2cbdce71df4deda4ab42c309572cb7fffee5454b78f0718"),
            "server Finished verify_data（RFC 8448 §3 真值）"
        );
    }
}
