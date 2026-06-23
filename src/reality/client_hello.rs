//! TLS 1.3 ClientHello 手写编码器（刀6 T2，Chrome-like best-effort，见 ADR-0008 / spec C2）。
//!
//! 中文要点：REALITY 要把 auth 写进 ClientHello 的 `session_id`（offset 39），stock TLS 库不让写 → 手写字节。
//! 本模块产出 **handshake message**（`0x01` + 3B 长度 + body，session_id 落 message 偏移 39）——
//! seal/AAD(T3) 即对此 message 计算；record-layer 5B 包头在发送时(刀8)另加。
//! 指纹 best-effort：GREASE + Chrome 风 cipher/曲线/ALPN/扩展序；**supported_versions 仅 1.3**（REALITY=TLS1.3，
//! 不能让借用站谈成 1.2），key_share 含 **X25519**（sing-box 硬要求，从中取 ECDHE pubkey 派生 AuthKey）。

/// 固定 GREASE 值（真 Chrome 随机选；固定值对"够用"的指纹足够，离线测也确定）。
const GREASE: u16 = 0x0a0a;

/// Chrome 风 cipher 列表（GREASE 头；TLS1.3 套件 0x1301/02/03 供真协商，TLS1.2 套件仅指纹）。
const CIPHERS: &[u16] = &[
    GREASE, 0x1301, 0x1302, 0x1303, 0xc02b, 0xc02f, 0xc02c, 0xc030, 0xcca9, 0xcca8, 0xc013, 0xc014,
    0x009c, 0x009d, 0x002f, 0x0035,
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
    // 尾部 GREASE（空）
    e.extend_from_slice(&ext(GREASE, &[]));
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
}
