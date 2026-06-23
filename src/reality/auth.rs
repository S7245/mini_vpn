//! REALITY auth 密码学（刀6 T1/T3/T4，sans-IO 纯函数）。
//!
//! 中文要点（已查证 XTLS/REALITY 源 + shoes 蓝本，见 spec C1）：
//! - X25519 ECDH(我方临时私钥 × server 静态 pbk) → shared secret。
//! - AuthKey = HKDF-SHA256(IKM=shared, salt=ClientHello.random[0..20], info="REALITY")。
//! - session_id 明文 **16B** = version[4] + timestamp(u32 BE)[4] + short_id[8]；**AES-256-GCM** seal
//!   (key=完整 32B AuthKey, nonce=random[20..32], AAD=session_id 清零的 ClientHello) → ct(16)+tag(16)=**32B** 填满字段。
//! - 服务端临时证书校验 = HMAC-SHA512(AuthKey, cert.ed25519_pubkey) == cert.signature（不走 CA 链）。

use aes_gcm::aead::{Aead, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use crate::shared::ClientError;
use hkdf::Hkdf;
use rand::RngCore;
use sha2::Sha256;

/// session_id 明文里的客户端版本标识（XTLS 风格 `x.y.z` + 0）。shoes 蓝本用 `1.8.0`。
/// 中文要点：服务端一般不严格校验此字段（信息性）；刀8 真互通时按 sing-box 行为校准。
pub const REALITY_VERSION: [u8; 4] = [1, 8, 0, 0];

/// X25519 ECDH：我方私钥标量 × 对端公钥点 → 32B 共享密钥。
/// 中文要点：底层 `x25519` 函数内部对标量做 clamp（与 RFC 7748 一致），故存原始 32B 私钥即可。
/// ⚠️ **安全前提**：底层 `x25519` **不**拒绝低阶点（恶意对端发小阶点 → 全零共享密钥）。REALITY auth 路径
/// 的 `peer_public` 是**可信配置 pbk**（非攻击者可控）→ 本刀安全。**刀7 的 TLS1.3 握手 ECDH 用的是网络来的
/// server keyshare（不可信）→ 届时必须加 contributory/全零检查**（见 x25519-dalek 的 `was_contributory`）。
pub fn x25519_shared_secret(my_secret: [u8; 32], peer_public: [u8; 32]) -> [u8; 32] {
    x25519_dalek::x25519(my_secret, peer_public)
}

/// 生成临时 X25519 密钥对，返回 `(私钥 32B, 公钥 32B)`。公钥 = x25519(私钥, basepoint)。
pub fn generate_ephemeral_keypair() -> ([u8; 32], [u8; 32]) {
    let mut secret = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut secret);
    let mut base = [0u8; 32];
    base[0] = 9; // X25519 basepoint u=9
    let public = x25519_dalek::x25519(secret, base);
    (secret, public)
}

/// REALITY AuthKey = HKDF-SHA256(IKM=shared_secret, salt=client_random[0..20], info="REALITY") → 32B。
pub fn derive_auth_key(shared_secret: &[u8; 32], client_random: &[u8; 32]) -> [u8; 32] {
    let hk = Hkdf::<Sha256>::new(Some(&client_random[0..20]), shared_secret);
    let mut okm = [0u8; 32];
    hk.expand(b"REALITY", &mut okm)
        .expect("32 bytes is within HKDF-SHA256 output limit");
    okm
}

/// session_id 明文（16B）：version[4] + timestamp(u32 BE)[4] + short_id[8]。
/// 中文要点：AES-256-GCM seal 后 = ct(16)+tag(16) = 32B，正好填满 TLS session_id 字段。
pub struct SessionIdPlaintext {
    pub version: [u8; 4],
    pub timestamp: u32,
    pub short_id: [u8; 8],
}

impl SessionIdPlaintext {
    /// 序列化为 16B 明文（待 seal）。
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&self.version);
        b[4..8].copy_from_slice(&self.timestamp.to_be_bytes());
        b[8..16].copy_from_slice(&self.short_id);
        b
    }
}

/// 解析 short_id（hex 字符串）→ 8B 零填充（左对齐）。空串→全零；>8 字节 / 非 hex / 奇数位 → Err。
pub fn parse_short_id(hex: &str) -> Result<[u8; 8], ClientError> {
    if !hex.len().is_multiple_of(2) {
        return Err(ClientError::Reality(format!("short_id hex 位数为奇数: {hex:?}")));
    }
    if hex.len() > 16 {
        return Err(ClientError::Reality(format!(
            "short_id 超过 8 字节: {hex:?}"
        )));
    }
    let mut out = [0u8; 8];
    for (i, pair) in hex.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(pair)
            .map_err(|_| ClientError::Reality("short_id 非法字节".into()))?;
        out[i] = u8::from_str_radix(s, 16)
            .map_err(|_| ClientError::Reality(format!("short_id 非 hex: {s:?}")))?;
    }
    Ok(out)
}

/// REALITY session_id seal：**AES-256-GCM**(key=完整 32B AuthKey,**不截断**)，
/// nonce=ClientHello.random[20..32]，AAD=session_id 清零的 ClientHello handshake message，明文 16B
/// → ct(16)+tag(16)=**32B**。中文要点(已查证 Xray-core,见 ADR-0008/findings)：用 AES-128 或截断 key
/// 会让 sing-box 静默拒绝并回落 decoy。
pub fn seal_session_id(auth_key: &[u8; 32], plaintext: &[u8; 16], nonce: &[u8; 12], aad: &[u8]) -> [u8; 32] {
    use aes_gcm::KeyInit;
    let cipher = Aes256Gcm::new_from_slice(auth_key).expect("32-byte AuthKey");
    let out = cipher
        .encrypt(Nonce::from_slice(nonce), Payload { msg: plaintext, aad })
        .expect("AES-256-GCM encrypt is infallible for valid key/nonce");
    out.try_into()
        .expect("16B plaintext + 16B tag = 32B sealed session_id")
}

/// 「服务端视角」解封 session_id（也用于离线 round-trip 自检）：解密失败(认证不过)→ None。
pub fn open_session_id(auth_key: &[u8; 32], sealed: &[u8; 32], nonce: &[u8; 12], aad: &[u8]) -> Option<[u8; 16]> {
    use aes_gcm::KeyInit;
    let cipher = Aes256Gcm::new_from_slice(auth_key).ok()?;
    let pt = cipher
        .decrypt(Nonce::from_slice(nonce), Payload { msg: sealed, aad })
        .ok()?;
    pt.try_into().ok()
}

/// 服务端临时证书校验：`HMAC-SHA512(AuthKey, ed25519_pubkey) == signature`（不走 CA 链）。
/// 中文要点(刀6 T4)：key=完整 32B AuthKey；constant-time 比较由 hmac crate 的 `verify_slice` 保证。
pub fn verify_server_cert(auth_key: &[u8; 32], ed25519_pubkey: &[u8], signature: &[u8]) -> bool {
    use hmac::{Hmac, Mac};
    use sha2::Sha512;
    let Ok(mut mac) = Hmac::<Sha512>::new_from_slice(auth_key) else {
        return false;
    };
    mac.update(ed25519_pubkey);
    mac.verify_slice(signature).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reality::testutil::arr32;

    /// RFC 7748 §5.2 X25519 已知答案向量（钉死 ECDH 接线 + 内部 clamp）。
    #[test]
    fn x25519_rfc7748_vector() {
        let k = arr32("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4");
        let u = arr32("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c");
        let out = arr32("c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552");
        assert_eq!(x25519_shared_secret(k, u), out);
    }

    /// 临时密钥对自洽：public == x25519(secret, basepoint=9)；两次生成不同。
    #[test]
    fn ephemeral_keypair_self_consistent() {
        let (sk, pk) = generate_ephemeral_keypair();
        let mut base = [0u8; 32];
        base[0] = 9;
        assert_eq!(x25519_shared_secret(sk, base), pk);
        let (sk2, _) = generate_ephemeral_keypair();
        assert_ne!(sk, sk2, "两次生成应不同");
    }

    /// AuthKey 确定性 + salt(=random[0..20]) 敏感 + random[20..] 不进 salt。
    #[test]
    fn auth_key_deterministic_and_salt_scoped() {
        let ss = [7u8; 32];
        let mut r1 = [0u8; 32];
        r1[0] = 1;
        let mut r2 = [0u8; 32];
        r2[0] = 2;
        let k1 = derive_auth_key(&ss, &r1);
        assert_eq!(k1, derive_auth_key(&ss, &r1), "确定性");
        assert_ne!(k1, derive_auth_key(&ss, &r2), "salt(random[0..20]) 变 → key 变");
        let mut r3 = r1;
        r3[25] = 9; // 仅改 random[20..]，不在 salt 范围
        assert_eq!(k1, derive_auth_key(&ss, &r3), "random[20..] 不进 salt → key 不变");
    }

    /// session_id 明文 16B 布局 round-trip。
    #[test]
    fn session_id_plaintext_layout() {
        let sid = SessionIdPlaintext {
            version: REALITY_VERSION,
            timestamp: 0x0102_0304,
            short_id: parse_short_id("ab12").unwrap(),
        };
        let b = sid.to_bytes();
        assert_eq!(&b[0..4], &REALITY_VERSION);
        assert_eq!(&b[4..8], &[1, 2, 3, 4], "u32 BE 时间戳");
        assert_eq!(b[8], 0xab);
        assert_eq!(b[9], 0x12);
        assert_eq!(&b[10..16], &[0u8; 6], "short_id 零填充");
    }

    /// short_id hex 解析：空→全零、满 8B、超长/非 hex/奇数位拒绝。
    #[test]
    fn short_id_parse() {
        assert_eq!(parse_short_id("").unwrap(), [0u8; 8]);
        assert_eq!(
            parse_short_id("0123456789abcdef").unwrap(),
            [0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]
        );
        assert!(parse_short_id("0123456789abcdef00").is_err(), ">8 字节应拒绝");
        assert!(parse_short_id("xy").is_err(), "非 hex 应拒绝");
        assert!(parse_short_id("abc").is_err(), "奇数位应拒绝");
    }

    /// seal→open round-trip（任意 AAD）；篡改 AAD / 错 key → open 失败（AEAD 认证绑定）。
    #[test]
    fn seal_open_roundtrip_and_aad_binding() {
        let key = [9u8; 32];
        let nonce = [3u8; 12];
        let aad = b"the-client-hello-bytes";
        let pt = [7u8; 16];
        let sealed = seal_session_id(&key, &pt, &nonce, aad);
        assert_eq!(sealed.len(), 32, "ct16+tag16");
        assert_eq!(open_session_id(&key, &sealed, &nonce, aad), Some(pt));
        assert_eq!(
            open_session_id(&key, &sealed, &nonce, b"tampered-client-hello"),
            None,
            "篡改 AAD → 认证失败"
        );
        assert_eq!(open_session_id(&[1u8; 32], &sealed, &nonce, aad), None, "错 key → 失败");
    }

    /// verify_server_cert：HMAC-SHA512 命中/失配/长度异常不 panic。
    #[test]
    fn verify_server_cert_hmac() {
        use hmac::{Hmac, Mac};
        use sha2::Sha512;
        let key = [5u8; 32];
        let pubkey = [0xeeu8; 32];
        let mut mac = Hmac::<Sha512>::new_from_slice(&key).unwrap();
        mac.update(&pubkey);
        let sig = mac.finalize().into_bytes();
        assert!(verify_server_cert(&key, &pubkey, &sig), "正确 HMAC 命中");
        assert!(!verify_server_cert(&key, &pubkey, &[0u8; 64]), "错签名失配");
        assert!(!verify_server_cert(&key, &pubkey, &[]), "空签名不 panic");
        assert!(!verify_server_cert(&[1u8; 32], &pubkey, &sig), "错 key 失配");
    }
}
