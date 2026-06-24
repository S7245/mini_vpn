//! REALITY 服务端临时证书：提 ed25519 SPKI 公钥 + 签名（刀8 T3/T4，见 brief §1.3 + ADR-0010）。
//!
//! 中文要点：**手解 DER**（不引 x509 库）——只抓两个定长字段：ed25519 SPKI 裸 32B 公钥 + DER 末 64B 签名
//! （实为 HMAC-SHA512，服务端 `h.Sum(cert[:len-64])` 写入处）。**不解析 Validity 等无关字段**：真出口实测
//! （2026-06-24）sing-box 临时证书的 Validity 用 GeneralizedTime，严格的 x509-cert(RFC 5280) 会拒之，
//! 但 REALITY auth 锚是 `auth::verify_server_cert`（HMAC over 公钥），与证书链/有效期无关（ADR-0010）→
//! 故只做定点提取（shoes 蓝本同法），更稳健。Certificate(0x0b) message 内第一张 leaf DER 起于相对 offset 11。

use crate::shared::ClientError;

/// REALITY 临时证书签名（实为 HMAC-SHA512）长度。
const REALITY_SIG_LEN: usize = 64;
/// ed25519 公钥长度。
const ED25519_PUBKEY_LEN: usize = 32;
/// ed25519 SPKI 公钥定位 marker：OID 1.3.101.112(`06 03 2b 65 70`) + BIT STRING 头(`03 21 00`，33B/0 unused)。
/// 中文要点：此 8 字节序列**唯一**定位 SPKI 公钥——signatureAlgorithm 的同 OID 后跟的是签名 BIT STRING `03 41`
/// （不是 `03 21`），故不会误匹配；其后紧跟裸 32B 公钥。非 ed25519 证书（decoy）无此 marker → 提取失败。
const ED25519_SPKI_MARKER: [u8; 8] = [0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00];

fn err(m: impl Into<String>) -> ClientError {
    ClientError::Reality(format!("cert: {}", m.into()))
}

/// 大端 uint24（3B）→ usize。`b` 须恰 3 字节（调用点用 `.get(..3)` 保证）。
fn u24(b: &[u8]) -> usize {
    ((b[0] as usize) << 16) | ((b[1] as usize) << 8) | b[2] as usize
}

/// 从 TLS 1.3 **Certificate(0x0b) handshake message** 提第一张 leaf cert 的
/// ed25519 公钥（裸 32B）+ 签名（64B，REALITY 实为 HMAC-SHA512）。
///
/// 中文要点（brief §1.3）：先按长度字段定位 leaf DER（`0x0b | 3B hs_len | 1B ctx_len | ctx |
/// 3B list_len | 3B cert_data_len | DER | 2B ext_len`，REALITY ctx_len=0 → DER 起于 offset 11），
/// 再在 DER 内**定点扫** ed25519 SPKI marker 取裸 32B 公钥 + 取 DER **末 64B** 为签名。**不解析无关字段**
/// （Validity/issuer 等；REALITY auth 锚=HMAC，ADR-0010）。无 SPKI marker（非 ed25519=decoy）/长度不符 → loud-fail。
pub fn extract_ed25519_pubkey_and_sig(
    cert_msg: &[u8],
) -> Result<([u8; ED25519_PUBKEY_LEN], Vec<u8>), ClientError> {
    if cert_msg.first() != Some(&0x0b) {
        return Err(err("非 Certificate（handshake type != 0x0b）"));
    }
    let hs_len = u24(cert_msg.get(1..4).ok_or_else(|| err("handshake 头截断"))?);
    let body = cert_msg.get(4..).ok_or_else(|| err("handshake body 截断"))?;
    if hs_len != body.len() {
        return Err(err("handshake 长度字段与 body 不符"));
    }
    // certificate_request_context（REALITY 恒空，但按长度字段跳过以求稳健）。
    let ctx_len = *body.first().ok_or_else(|| err("ctx_len 截断"))? as usize;
    let mut p = 1 + ctx_len;
    let list_len = u24(body.get(p..p + 3).ok_or_else(|| err("list_len 截断"))?);
    p += 3;
    let list = body.get(p..p + list_len).ok_or_else(|| err("certificate_list 截断"))?;
    // 第一条 CertificateEntry：3B cert_data_len + DER + 2B ext_len。
    let cert_len = u24(list.get(0..3).ok_or_else(|| err("cert_data_len 截断"))?);
    let der = list.get(3..3 + cert_len).ok_or_else(|| err("leaf cert DER 截断"))?;

    // 定点扫 ed25519 SPKI marker → 其后裸 32B 公钥。无 marker = 非 ed25519 证书（多半 decoy 回落）。
    let Some(pos) = der.windows(ED25519_SPKI_MARKER.len()).position(|w| w == ED25519_SPKI_MARKER) else {
        return Err(err(
            "未找到 ed25519 SPKI（OID 1.3.101.112）→ 非 ed25519 证书，多半服务端 REALITY auth 失败、回落 decoy。\
             查：①pbk=服务端 public_key(非 private_key) ②SNI∈serverNames ③short_id 匹配",
        ));
    };
    let key_start = pos + ED25519_SPKI_MARKER.len();
    let pubkey: [u8; ED25519_PUBKEY_LEN] = der
        .get(key_start..key_start + ED25519_PUBKEY_LEN)
        .ok_or_else(|| err("ed25519 公钥截断"))?
        .try_into()
        .expect("切片长度 == 32");
    // 签名 = leaf DER **末 64B**（服务端 h.Sum(cert[:len-64]) 写入处；brief §1.3 + 源码确证）。
    if der.len() < REALITY_SIG_LEN {
        return Err(err("leaf DER 太短，无 64B 签名"));
    }
    let sig = der[der.len() - REALITY_SIG_LEN..].to_vec();
    Ok((pubkey, sig))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::hex;

    // 真 ed25519 自签证书 DER（openssl ed25519 自签生成，326B）——离线 KAT fixture。
    // 末 64B = 签名 BitString（`03 41 00` 之后）；SPKI 末 32B = 公钥。
    const ED25519_CERT_DER: &str = "308201423081f5a0030201020214650b853b02ac2a3e0f05b44644695bcaeec01154300506032b657030173115301306035504030c0c7265616c6974792d74656d70301e170d3236303632333134333935305a170d3336303632303134333935305a30173115301306035504030c0c7265616c6974792d74656d70302a300506032b6570032100df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0ba3533051301d0603551d0e041604146657983d0890461f3d4c21d1d6af8b1626144811301f0603551d230418301680146657983d0890461f3d4c21d1d6af8b1626144811300f0603551d130101ff040530030101ff300506032b6570034100ab056a660a043ddb36de3bd9031d346142dceb6ae874fc45219c33c6a5b57c7b9c196f1aad5fb124ec84697377bb15f03b44d2a2c63dc3a9589002dfc23a570f";
    const ED25519_PUBKEY: &str = "df18652c451afa44c276c60475d9f4f6f4ae3bf9d389dd6f3215383d6d5dda0b";
    const ED25519_SIG: &str = "ab056a660a043ddb36de3bd9031d346142dceb6ae874fc45219c33c6a5b57c7b9c196f1aad5fb124ec84697377bb15f03b44d2a2c63dc3a9589002dfc23a570f";

    // **生产真因回归 fixture**（2026-06-24）：真 sing-box 临时证书 Validity 用 GeneralizedTime（notAfter≥2050），
    // 严格的 x509-cert(RFC 5280) 报「malformed GeneralizedTime」拒之。本 cert = notBefore UTCTime + notAfter
    // GeneralizedTime 的 ed25519 自签证书；手解 DER **不碰 Validity** → 不受影响。
    const ED25519_CERT_GT_DER: &str = "308201403081f3a003020102021478b8af12a2ccbe7d59c3a66cdd513c159d617056300506032b657030153113301106035504030c0a7265616c6974792d67743020170d3236303632343130343931395a180f32303539303530323130343931395a30153113301106035504030c0a7265616c6974792d6774302a300506032b65700321001fdaec697021e21bc98c0a49223c6439c1b10738017fe027e8bb6da054cbf2d1a3533051301d0603551d0e041604143c59fba583f1b973dc17d84f6dfbf4a66340c384301f0603551d230418301680143c59fba583f1b973dc17d84f6dfbf4a66340c384300f0603551d130101ff040530030101ff300506032b65700341006834e4f0b5158656a2c7d15bcff59baf4a5bceae9417c519d43f6386201cf46e95c25ab465650634ec055db1d5e045e6f67039e3b1986196bd34e3f936fa8200";
    const ED25519_GT_PUBKEY: &str = "1fdaec697021e21bc98c0a49223c6439c1b10738017fe027e8bb6da054cbf2d1";

    // RFC 8448 §3 Certificate(0x0b) message（RSA leaf）——负样本：SPKI OID 非 ed25519 → 拒。
    const RFC8448_CERT_MSG: &str = "0b0001b9000001b50001b0308201ac30820115a003020102020102300d06092a864886f70d01010b0500300e310c300a06035504031303727361301e170d3136303733303031323335395a170d3236303733303031323335395a300e310c300a0603550403130372736130819f300d06092a864886f70d010101050003818d0030818902818100b4bb498f8279303d980836399b36c6988c0c68de55e1bdb826d3901a2461eafd2de49a91d015abbc9a95137ace6c1af19eaa6af98c7ced43120998e187a80ee0ccb0524b1b018c3e0b63264d449a6d38e22a5fda430846748030530ef0461c8ca9d9efbfae8ea6d1d03e2bd193eff0ab9a8002c47428a6d35a8d88d79f7f1e3f0203010001a31a301830090603551d1304023000300b0603551d0f0404030205a0300d06092a864886f70d01010b05000381810085aad2a0e5b9276b908c65f73a7267170618a54c5f8a7b337d2df7a594365417f2eae8f8a58c8f8172f9319cf36b7fd6c55b80f21a03015156726096fd335e5e67f2dbf102702e608ccae6bec1fc63a42a99be5c3eb7107c3c54e9b9eb2bd5203b1c3b84e0a8b2f759409ba3eac9d91d402dcc0cc8f8961229ac9187b42b4de10000";

    /// 把一张 DER 证书包成 Certificate(0x0b) handshake message（按长度字段构造，不硬编偏移）。
    fn wrap_cert_message(der: &[u8]) -> Vec<u8> {
        let u24b = |n: usize| [(n >> 16) as u8, (n >> 8) as u8, n as u8];
        let mut list = Vec::new();
        list.extend_from_slice(&u24b(der.len())); // cert_data_len
        list.extend_from_slice(der);
        list.extend_from_slice(&0u16.to_be_bytes()); // extensions len = 0
        let mut body = Vec::new();
        body.push(0); // ctx_len = 0
        body.extend_from_slice(&u24b(list.len()));
        body.extend_from_slice(&list);
        let mut msg = vec![0x0b];
        msg.extend_from_slice(&u24b(body.len()));
        msg.extend_from_slice(&body);
        msg
    }

    /// T3 核心：从真 ed25519 cert message 提出 32B 公钥（SPKI）+ 64B 签名（cert 末 64B）。
    #[test]
    fn extract_pubkey_and_sig_from_ed25519_cert() {
        let msg = wrap_cert_message(&hex(ED25519_CERT_DER));
        let (pubkey, sig) = extract_ed25519_pubkey_and_sig(&msg).expect("应提取成功");
        assert_eq!(&pubkey[..], &hex(ED25519_PUBKEY)[..], "SPKI 裸 32B 公钥");
        assert_eq!(sig, hex(ED25519_SIG), "末 64B 签名");
        // 与裸 DER 末 64B 一致（坐实「签名=DER 末 64B」）。
        let der = hex(ED25519_CERT_DER);
        assert_eq!(sig, der[der.len() - 64..], "签名 == cert DER 末 64B");
    }

    /// **生产真因回归**：GeneralizedTime Validity 的 ed25519 证书（x509-cert 会拒「malformed GeneralizedTime」）
    /// → 手解 DER 仍正确提取（不碰 Validity）。这是真出口实测穿过 decoy 后的第二道坎，本测试钉死修复。
    #[test]
    fn extract_handles_generalizedtime_validity() {
        let der = hex(ED25519_CERT_GT_DER);
        let msg = wrap_cert_message(&der);
        let (pubkey, sig) = extract_ed25519_pubkey_and_sig(&msg)
            .expect("GeneralizedTime Validity 不应阻碍提取（手解不碰 Validity）");
        assert_eq!(&pubkey[..], &hex(ED25519_GT_PUBKEY)[..], "SPKI 裸 32B 公钥");
        assert_eq!(sig, der[der.len() - 64..], "签名 == cert DER 末 64B");
        assert_eq!(sig.len(), 64);
    }

    /// 负样本：RFC 8448 RSA cert → SPKI 非 ed25519 OID（或解析拒）→ Err（不 panic）。
    #[test]
    fn rejects_non_ed25519_cert() {
        let err = extract_ed25519_pubkey_and_sig(&hex(RFC8448_CERT_MSG));
        assert!(err.is_err(), "RSA cert 应被拒（OID 非 ed25519）");
    }

    /// 各拒绝路径（不 panic）：错 handshake type / 截断 / 长度字段不符。
    #[test]
    fn reject_malformed() {
        assert!(extract_ed25519_pubkey_and_sig(&[0x02, 0, 0, 0]).is_err(), "非 0x0b");
        assert!(extract_ed25519_pubkey_and_sig(&[0x0b]).is_err(), "极短截断");
        let mut msg = wrap_cert_message(&hex(ED25519_CERT_DER));
        msg[1] ^= 0xff; // 破坏 handshake 长度字段
        assert!(extract_ed25519_pubkey_and_sig(&msg).is_err(), "长度字段不符");
    }

    /// 把一张 ed25519 cert DER 的末 64B 签名换成 `HMAC-SHA512(auth_key, 裸 32B 公钥)`，
    /// 模拟 REALITY 服务端临时证书（服务端正是这样写签名的）。
    fn realityize_cert(der: &[u8], auth_key: &[u8; 32]) -> Vec<u8> {
        use hmac::{Hmac, Mac};
        use sha2::Sha512;
        let pubkey = &hex(ED25519_PUBKEY);
        let mut mac = Hmac::<Sha512>::new_from_slice(auth_key).unwrap();
        mac.update(pubkey);
        let hmac = mac.finalize().into_bytes();
        let mut out = der.to_vec();
        let n = out.len();
        out[n - 64..].copy_from_slice(&hmac); // 覆写签名 BitString 内容（结构仍合法，x509-cert 不验签）
        out
    }

    /// T4 端到端：cert 提取 ⊕ verify_server_cert（REALITY auth 决策）。
    /// 跑通即证明「解析临时证书 → HMAC-SHA512 校验」整条 REALITY auth 链离线正确。
    #[test]
    fn cert_extract_then_verify_server_cert_e2e() {
        use crate::reality::auth::verify_server_cert;
        let auth_key = [0x5a_u8; 32];
        let der = realityize_cert(&hex(ED25519_CERT_DER), &auth_key);
        let msg = wrap_cert_message(&der);
        let (pubkey, sig) = extract_ed25519_pubkey_and_sig(&msg).expect("提取成功");
        assert_eq!(&pubkey[..], &hex(ED25519_PUBKEY)[..]);
        assert!(verify_server_cert(&auth_key, &pubkey, &sig), "正确 AuthKey → REALITY auth 通过");
        // 错 AuthKey（decoy / 攻击者无静态私钥）→ 拒。
        assert!(!verify_server_cert(&[0u8; 32], &pubkey, &sig), "错 AuthKey → 拒");
        // 篡改公钥任一字节 → HMAC 失配 → 拒。
        let mut bad_pk = pubkey;
        bad_pk[0] ^= 0x01;
        assert!(!verify_server_cert(&auth_key, &bad_pk, &sig), "篡改公钥 → 拒");
        // 篡改签名任一字节 → 拒。
        let mut bad_sig = sig.clone();
        bad_sig[0] ^= 0x01;
        assert!(!verify_server_cert(&auth_key, &pubkey, &bad_sig), "篡改签名 → 拒");
    }
}
