//! REALITY 服务端临时证书：提 ed25519 SPKI 公钥 + 签名（刀8 T3/T4，见 brief §1.3 + ADR-0010）。
//!
//! 中文要点：用 x509-cert(RustCrypto 系)解析临时证书 DER——`subject_public_key` 取裸 32B ed25519 公钥、
//! `signature` 取末 64B（实为 HMAC-SHA512，非真 ed25519 签名）。**结构解析不验签**：REALITY auth 锚是
//! `auth::verify_server_cert`（HMAC-SHA512(AuthKey, 裸 32B pubkey) == 签名），不走 PKI 链（ADR-0010）。
//! Certificate(0x0b) message 内第一张 leaf DER 起于相对 offset 11；长度/marker/OID mismatch → loud-fail。
