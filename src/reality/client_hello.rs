//! TLS 1.3 ClientHello 手写编码器（刀6 T2，Chrome-like best-effort，见 ADR-0008 / spec C2）。
//!
//! 中文要点：REALITY 要把 auth 写进 ClientHello 的 `session_id`（offset 39），stock TLS 库不让写 → 手写字节。
//! 本模块产出 **handshake message**（`0x01` + 3B 长度 + body，session_id 落 message 偏移 39）——
//! seal/AAD(T3) 即对此 message 计算；record-layer 5B 包头在发送时(刀8)另加。
//! 指纹 best-effort：GREASE + Chrome 风 cipher/曲线/ALPN/扩展序；**supported_versions 仅 1.3**（REALITY=TLS1.3，
//! 不能让借用站谈成 1.2），key_share 含 **X25519**（sing-box 硬要求，从中取 ECDHE pubkey 派生 AuthKey）。

/// 固定 GREASE 值（真 Chrome 随机选；固定值对"够用"的指纹足够，离线测也确定）。
/// 用于 cipher / supported_groups / supported_versions 列表里的 GREASE 占位值，以及**第一个** GREASE 扩展的 type。
const GREASE: u16 = 0x0a0a;
/// **第二个** GREASE 扩展的 type，必须与 [`GREASE`] **不同**（互通-critical）：
/// 两个 GREASE 扩展若同 type 0x0a0a = **重复扩展类型**，违反 RFC 8446 §4.2「同一扩展块不得有重复 type」。
/// 宽容解析器（Apple/tls-parser）容忍，但 sing-box 的 Go-tls 严格解析器**拒整个 ClientHello → REALITY auth 前
/// 回落 decoy**（真出口实测根因，2026-06-24）。真 Chrome 的两个 GREASE 扩展正是用不同 GREASE 值。0x1a1a 是合法 GREASE 值。
const GREASE_EXT2: u16 = 0x1a1a;

/// session_id 字段在 ClientHello **handshake message** 里的偏移与长度（REALITY seal 回写处）。
/// 4(handshake hdr) + 2(legacy_version) + 32(random) + 1(session_id len) = **39**；长度恒 **32**
/// （`build_client_hello` 永远写 32B session_id，故偏移稳定——load-bearing，勿动布局而不改此处）。
const SESSION_ID_OFFSET: usize = 39;
const SESSION_ID_LEN: usize = 32;

/// 取 ClientHello message 里的 session_id 区（REALITY=32B sealed 值），供握手层 SH echo 一致性检查复用，
/// 避免调用方裸抄 `[39..71]` 魔数（L4，单一布局事实源）。
pub(crate) fn authed_session_id(client_hello: &[u8]) -> &[u8] {
    &client_hello[SESSION_ID_OFFSET..SESSION_ID_OFFSET + SESSION_ID_LEN]
}

/// Chrome 风 cipher 列表（GREASE 头）。**TLS1.3 仅 offer 0x1301**（刀8 grill 裁决 f，ADR-0009 修订）：
/// 删 0x1302/0x1303，使任何合规借用站被 RFC 8446 §9.1 强制只能回 0x1301（唯一交集），根除 0x1302 decoy
/// loud-fail（我方 schedule/record 硬编 SHA-256/AES-128，完成不了 0x1302/0x1303）。代价：偏离 Chrome 真实
/// 三套件指纹（robustness>fingerprint，系统稳定优先）；0x1302/0x1303 实现后可恢复全列表。TLS1.2 套件仅指纹。
const CIPHERS: &[u16] = &[
    GREASE, 0x1301, 0xc02b, 0xc02f, 0xc02c, 0xc030, 0xcca9, 0xcca8, 0xc013, 0xc014, 0x009c, 0x009d,
    0x002f, 0x0035,
];

/// 构造 ClientHello 的入参（session_id 在 T2 为占位，T3 由 seal 回写密文）。
pub struct ClientHelloParams<'a> {
    /// 借用站 SNI（REALITY handshake server，如 `www.microsoft.com`）。
    pub server_name: &'a str,
    /// 我方临时 X25519 公钥（key_share）。
    pub key_share: [u8; 32],
    /// ClientHello.random（32B）。
    pub random: [u8; 32],
    /// session_id（32B）；T2 传占位（全零），T3 seal 后回写。
    pub session_id: [u8; 32],
}

/// 编码一个扩展：`type(2) + len(2) + body`。
fn ext(typ: u16, body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + body.len());
    v.extend_from_slice(&typ.to_be_bytes());
    v.extend_from_slice(&(body.len() as u16).to_be_bytes());
    v.extend_from_slice(body);
    v
}

/// `u16 长度前缀 + body`。
fn u16_vec(body: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + body.len());
    v.extend_from_slice(&(body.len() as u16).to_be_bytes());
    v.extend_from_slice(body);
    v
}

fn u16s(values: &[u16]) -> Vec<u8> {
    let mut v = Vec::with_capacity(values.len() * 2);
    for &x in values {
        v.extend_from_slice(&x.to_be_bytes());
    }
    v
}

fn build_extensions(p: &ClientHelloParams) -> Vec<u8> {
    let mut e = Vec::new();
    // GREASE（空）
    e.extend_from_slice(&ext(GREASE, &[]));
    // server_name（SNI）：server_name_list = u16 列表长 + [type(0) + u16 host 长 + host]
    {
        let host = p.server_name.as_bytes();
        let mut entry = vec![0u8]; // host_name type
        entry.extend_from_slice(&u16_vec(host));
        e.extend_from_slice(&ext(0x0000, &u16_vec(&entry)));
    }
    // extended_master_secret（空）
    e.extend_from_slice(&ext(0x0017, &[]));
    // renegotiation_info：1B 长=0
    e.extend_from_slice(&ext(0xff01, &[0]));
    // supported_groups：u16 列表（GREASE + X25519 + secp256r1 + secp384r1）
    e.extend_from_slice(&ext(0x000a, &u16_vec(&u16s(&[GREASE, 0x001d, 0x0017, 0x0018]))));
    // ec_point_formats：1B 长=1 + uncompressed(0)
    e.extend_from_slice(&ext(0x000b, &[1, 0]));
    // ALPN：u16 列表 of (1B 长 + proto)
    {
        let mut list = Vec::new();
        for proto in [b"h2".as_slice(), b"http/1.1".as_slice()] {
            list.push(proto.len() as u8);
            list.extend_from_slice(proto);
        }
        e.extend_from_slice(&ext(0x0010, &u16_vec(&list)));
    }
    // signature_algorithms：u16 列表
    e.extend_from_slice(&ext(
        0x000d,
        &u16_vec(&u16s(&[
            0x0403, 0x0804, 0x0401, 0x0503, 0x0805, 0x0501, 0x0806, 0x0601,
        ])),
    ));
    // key_share：u16 列表 of [group(2) + u16 pubkey 长 + pubkey]，仅 X25519。
    {
        let mut entry = Vec::new();
        entry.extend_from_slice(&0x001du16.to_be_bytes());
        entry.extend_from_slice(&u16_vec(&p.key_share));
        e.extend_from_slice(&ext(0x0033, &u16_vec(&entry)));
    }
    // psk_key_exchange_modes：1B 长=1 + psk_dhe_ke(1)
    e.extend_from_slice(&ext(0x002d, &[1, 1]));
    // supported_versions：1B 长 + 版本列表（GREASE + TLS1.3 0x0304）。**不含 1.2**（REALITY=1.3）。
    {
        let vers = u16s(&[GREASE, 0x0304]);
        let mut b = vec![vers.len() as u8];
        b.extend_from_slice(&vers);
        e.extend_from_slice(&ext(0x002b, &b));
    }
    // 尾部 GREASE（空）——**必须用与开头不同的 GREASE type**（GREASE_EXT2），否则重复扩展类型被 Go-tls 拒。
    e.extend_from_slice(&ext(GREASE_EXT2, &[]));
    e
}

/// 构造 TLS 1.3 ClientHello **handshake message**（`0x01` + 3B 长 + body）。
pub fn build_client_hello(p: &ClientHelloParams) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&[0x03, 0x03]); // legacy_version
    body.extend_from_slice(&p.random); // random[32]
    body.push(32); // session_id 长
    body.extend_from_slice(&p.session_id); // session_id[32] → message 偏移 39
    body.extend_from_slice(&u16_vec(&u16s(CIPHERS))); // cipher_suites
    body.extend_from_slice(&[1, 0]); // compression: 1B 长 + null(0)
    body.extend_from_slice(&u16_vec(&build_extensions(p))); // extensions

    let mut msg = Vec::with_capacity(4 + body.len());
    msg.push(0x01); // handshake type ClientHello
    let len = body.len();
    msg.extend_from_slice(&[(len >> 16) as u8, (len >> 8) as u8, len as u8]);
    msg.extend_from_slice(&body);
    msg
}

/// 构造带 REALITY auth 的 ClientHello 的入参。
pub struct AuthedClientHelloParams<'a> {
    pub server_name: &'a str,
    pub key_share: [u8; 32],
    pub random: [u8; 32],
    /// REALITY AuthKey（= HKDF(ECDH(我方临时私钥, server pbk))），见 `auth::derive_auth_key`。
    pub auth_key: [u8; 32],
    pub short_id: [u8; 8],
    pub timestamp: u32,
}

/// 构造 **带 REALITY auth 的 ClientHello**：先建 session_id 清零的 ClientHello（即 AEAD 的 AAD），
/// 对其 seal 16B 明文，把 32B 密文回写进 session_id 字段（message 偏移 39..71）。
/// 中文要点(最易错点)：AAD 必须是「session_id 区清零」的完整 handshake message——本函数天然满足
/// （步骤1 就用全零 session_id 建）。nonce=random[20..32]。
pub fn build_authed_client_hello(p: &AuthedClientHelloParams) -> Vec<u8> {
    use crate::reality::auth::{REALITY_VERSION, SessionIdPlaintext, seal_session_id};
    // 1. 用全零 session_id 建 ClientHello —— 这份字节即 seal 的 AAD。
    let mut msg = build_client_hello(&ClientHelloParams {
        server_name: p.server_name,
        key_share: p.key_share,
        random: p.random,
        session_id: [0u8; 32],
    });
    // 2. seal 16B 明文（version+timestamp+short_id），AAD = 上面清零的 message。
    let pt = SessionIdPlaintext {
        version: REALITY_VERSION,
        timestamp: p.timestamp,
        short_id: p.short_id,
    }
    .to_bytes();
    let nonce: [u8; 12] = p.random[20..32].try_into().expect("random 有 32 字节");
    // 不变量（最易错点）：seal 前 session_id 区必须全零（AAD 与服务端一致）。由步骤1 构造保证。
    debug_assert_eq!(
        &msg[SESSION_ID_OFFSET..SESSION_ID_OFFSET + SESSION_ID_LEN],
        &[0u8; SESSION_ID_LEN],
        "AAD 不变量：seal 前 session_id 区须清零"
    );
    let sealed = seal_session_id(&p.auth_key, &pt, &nonce, &msg);
    // 3. 密文回写 session_id 字段。
    msg[SESSION_ID_OFFSET..SESSION_ID_OFFSET + SESSION_ID_LEN].copy_from_slice(&sealed);
    msg
}

#[cfg(test)]
mod tests {
    use super::*;
    use tls_parser::{TlsMessage, TlsMessageHandshake, parse_tls_message_handshake};

    /// 在 raw 扩展块里按类型找一个扩展的 body（独立于 tls-parser 的扩展变体 API）。
    fn find_ext(exts: &[u8], typ: u16) -> Option<&[u8]> {
        let mut i = 0;
        while i + 4 <= exts.len() {
            let t = u16::from_be_bytes([exts[i], exts[i + 1]]);
            let l = u16::from_be_bytes([exts[i + 2], exts[i + 3]]) as usize;
            let body = exts.get(i + 4..i + 4 + l)?;
            if t == typ {
                return Some(body);
            }
            i += 4 + l;
        }
        None
    }

    fn sample() -> Vec<u8> {
        let p = ClientHelloParams {
            server_name: "www.microsoft.com",
            key_share: [0x42; 32],
            random: [0x11; 32],
            session_id: [0u8; 32],
        };
        build_client_hello(&p)
    }

    /// session_id 落 message 偏移 39、长 32（REALITY seal 回写处）。
    #[test]
    fn session_id_at_offset_39() {
        let msg = sample();
        assert_eq!(msg[0], 0x01, "handshake type ClientHello");
        assert_eq!(msg[38], 32, "session_id 长度字节");
        assert_eq!(&msg[39..71], &[0u8; 32], "session_id（占位全零，T3 seal 回写）");
    }

    /// tls-parser 独立解析成功，且 cipher 含 AES_128_GCM + GREASE。
    #[test]
    fn parses_as_clienthello_with_expected_ciphers() {
        let msg = sample();
        let (_rest, hs) = parse_tls_message_handshake(&msg).expect("应解析为 handshake");
        let ch = match hs {
            TlsMessage::Handshake(TlsMessageHandshake::ClientHello(c)) => c,
            _ => panic!("不是 ClientHello"),
        };
        assert_eq!(ch.version.0, 0x0303, "legacy_version=0x0303");
        let ciphers: Vec<u16> = ch.ciphers.iter().map(|c| c.0).collect();
        assert!(ciphers.contains(&0x1301), "TLS_AES_128_GCM_SHA256");
        assert!(ciphers.contains(&GREASE), "GREASE cipher");
        // 刀8 裁决 f / ADR-0009 修订：收紧 TLS1.3 offer 仅 0x1301——绝不 offer 我方完成不了的 0x1302/0x1303
        // （否则借用站选中即 loud-fail；详见 server_hello::parse_server_hello 的 cipher guard）。
        assert!(!ciphers.contains(&0x1302), "不 offer 0x1302（schedule/record 不支持，避 decoy loud-fail）");
        assert!(!ciphers.contains(&0x1303), "不 offer 0x1303（同上）");
        assert_eq!(ch.session_id, Some(&[0u8; 32][..]));
    }

    /// 关键扩展存在且正确（SNI / key_share X25519 / supported_versions 仅 1.3 / 曲线含 X25519 / ALPN）。
    #[test]
    fn required_extensions_present() {
        let msg = sample();
        let (_rest, hs) = parse_tls_message_handshake(&msg).unwrap();
        let TlsMessage::Handshake(TlsMessageHandshake::ClientHello(ch)) = hs else {
            panic!("不是 ClientHello")
        };
        let exts = ch.ext.expect("有扩展块");

        // SNI 含借用站。
        let sni = find_ext(exts, 0x0000).expect("server_name 扩展");
        assert!(
            sni.windows(b"www.microsoft.com".len())
                .any(|w| w == b"www.microsoft.com"),
            "SNI 含借用站"
        );

        // key_share 含 X25519(0x001d) + 32B pubkey。
        let ks = find_ext(exts, 0x0033).expect("key_share 扩展");
        // 跳过 u16 列表长 → group(2) + u16 长(2) + pubkey。
        assert_eq!(&ks[2..4], &0x001du16.to_be_bytes(), "key_share group=X25519");
        assert_eq!(u16::from_be_bytes([ks[4], ks[5]]), 32, "X25519 pubkey 32B");
        assert_eq!(&ks[6..38], &[0x42u8; 32], "key_share=我方 pubkey");

        // supported_versions 含 1.3、不含真 1.2。
        let sv = find_ext(exts, 0x002b).expect("supported_versions 扩展");
        let vers: Vec<u16> = sv[1..]
            .chunks(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        assert!(vers.contains(&0x0304), "offers TLS 1.3");
        assert!(!vers.contains(&0x0303), "不 offer TLS 1.2（强制 1.3）");

        // supported_groups 含 X25519。
        let groups = find_ext(exts, 0x000a).expect("supported_groups 扩展");
        let glist: Vec<u16> = groups[2..]
            .chunks(2)
            .map(|c| u16::from_be_bytes([c[0], c[1]]))
            .collect();
        assert!(glist.contains(&0x001d), "supported_groups 含 X25519");

        // ALPN 含 h2。
        let alpn = find_ext(exts, 0x0010).expect("ALPN 扩展");
        assert!(alpn.windows(2).any(|w| w == b"h2"), "ALPN 含 h2");
    }

    /// **互通-critical 回归守卫**：ClientHello 扩展块**无重复扩展类型**（RFC 8446 §4.2）。
    /// 历史 bug：两个 GREASE 扩展同 type 0x0a0a → 严格的 Go-tls(sing-box REALITY) 拒整个 CH → auth 前回落 decoy
    /// （真出口实测根因，2026-06-24）。两个 GREASE 扩展须用不同 GREASE type。
    #[test]
    fn no_duplicate_extension_types() {
        let msg = sample();
        let (_rest, hs) = parse_tls_message_handshake(&msg).unwrap();
        let TlsMessage::Handshake(TlsMessageHandshake::ClientHello(ch)) = hs else {
            panic!("不是 ClientHello")
        };
        let exts = ch.ext.expect("有扩展块");
        let mut seen = std::collections::HashSet::new();
        let mut i = 0;
        while i + 4 <= exts.len() {
            let t = u16::from_be_bytes([exts[i], exts[i + 1]]);
            let l = u16::from_be_bytes([exts[i + 2], exts[i + 3]]) as usize;
            assert!(seen.insert(t), "重复扩展类型 0x{t:04x}（违反 RFC 8446 §4.2，被 Go-tls 拒 → 回落 decoy）");
            i += 4 + l;
        }
        // 两个 GREASE 扩展都在，但 type 不同。
        assert!(seen.contains(&GREASE), "含第一个 GREASE 扩展");
        assert!(seen.contains(&GREASE_EXT2), "含第二个 GREASE 扩展（不同 type）");
    }

    /// 刀6 T3 核心：build_authed → 「服务端视角」解封 round-trip。
    /// 跑通即证明 ECDH + derive_auth_key + ClientHello 编码 + seal + **AAD 清零纪律** 全链正确。
    #[test]
    fn authed_client_hello_server_view_roundtrip() {
        use crate::reality::auth::{
            REALITY_VERSION, derive_auth_key, generate_ephemeral_keypair, open_session_id,
            parse_short_id, x25519_shared_secret,
        };
        // server 静态 + client 临时密钥；双向 ECDH 应一致。
        let (sk_s, pk_s) = generate_ephemeral_keypair();
        let (sk_c, pk_c) = generate_ephemeral_keypair();
        let shared_c = x25519_shared_secret(sk_c, pk_s);
        let shared_s = x25519_shared_secret(sk_s, pk_c);
        assert_eq!(shared_c, shared_s, "ECDH 对称");

        let random = [0x33u8; 32];
        let auth_key = derive_auth_key(&shared_c, &random);
        let short_id = parse_short_id("01ab").unwrap();
        let ts = 0x0102_0304u32;
        let authed = build_authed_client_hello(&AuthedClientHelloParams {
            server_name: "www.microsoft.com",
            key_share: pk_c,
            random,
            auth_key,
            short_id,
            timestamp: ts,
        });
        assert_ne!(&authed[39..71], &[0u8; 32], "session_id 已 seal（非全零）");
        // 仍是结构合法的 ClientHello（session_id 现为密文，仍 32B）。
        assert!(parse_tls_message_handshake(&authed).is_ok());

        // 服务端视角：取出密文 → session_id 区清零重建 AAD → 用 server 侧 auth_key 解封。
        let sealed: [u8; 32] = authed[39..71].try_into().unwrap();
        let mut aad = authed.clone();
        aad[39..71].fill(0);
        let nonce: [u8; 12] = random[20..32].try_into().unwrap();
        let auth_key_server = derive_auth_key(&shared_s, &random);
        let pt = open_session_id(&auth_key_server, &sealed, &nonce, &aad)
            .expect("服务端解封成功 → key/nonce/AAD 全对");
        assert_eq!(&pt[0..4], &REALITY_VERSION);
        assert_eq!(u32::from_be_bytes(pt[4..8].try_into().unwrap()), ts);
        assert_eq!(&pt[8..16], &short_id);
    }

    /// 篡改 ClientHello 任一字节 → 服务端 AAD 变 → 解封失败（AEAD 把 auth 绑定到整条 ClientHello）。
    #[test]
    fn authed_client_hello_tamper_breaks_auth() {
        use crate::reality::auth::{
            derive_auth_key, generate_ephemeral_keypair, open_session_id, parse_short_id,
            x25519_shared_secret,
        };
        let (sk_s, pk_s) = generate_ephemeral_keypair();
        let (sk_c, pk_c) = generate_ephemeral_keypair();
        let shared = x25519_shared_secret(sk_c, pk_s);
        let random = [0x44u8; 32];
        let auth_key = derive_auth_key(&shared, &random);
        let mut authed = build_authed_client_hello(&AuthedClientHelloParams {
            server_name: "www.apple.com",
            key_share: pk_c,
            random,
            auth_key,
            short_id: parse_short_id("ff").unwrap(),
            timestamp: 1,
        });
        let sealed: [u8; 32] = authed[39..71].try_into().unwrap();
        // 篡改一个扩展字节（最后一字节，远离 session_id）。
        let last = authed.len() - 1;
        authed[last] ^= 0xff;
        let mut aad = authed.clone();
        aad[39..71].fill(0);
        let nonce: [u8; 12] = random[20..32].try_into().unwrap();
        let auth_key_server = derive_auth_key(&x25519_shared_secret(sk_s, pk_c), &random);
        assert_eq!(
            open_session_id(&auth_key_server, &sealed, &nonce, &aad),
            None,
            "篡改 ClientHello → AAD 变 → 解封失败"
        );
    }
}
