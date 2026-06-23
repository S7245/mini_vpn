//! TLS 1.3 key schedule（刀7，RFC 8446 §7.1/§7.3，sans-IO 纯函数；泛型-over-hash，SHA-256 先 wire）。
//!
//! 中文要点：HKDF-Expand-Label / Derive-Secret / 握手阶段密钥链（Early→derived→Handshake(from ECDHE)→
//! {c,s}_hs_traffic→key/iv）+ `compute_finished_verify_data`。KAT 用 RFC 8448 §3（见 testutil + 本刀 plan）。
//! ⚠️ 这里的 ECDHE = x25519(client 临时, **server 临时** keyshare from ServerHello)，与刀6 REALITY AuthKey 的
//! x25519(client 临时, **server 静态** pbk) 是**不同**密钥——别接错（接错不过 KAT 但破活握手）。
//! ⚠️ network 来的 keyshare 不可信：Extract 前必须拒绝全零/非贡献点 ECDHE（见 auth.rs `x25519_shared_secret` 注）。

use hkdf::Hkdf;
use sha2::{Digest, Sha256};

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
}
