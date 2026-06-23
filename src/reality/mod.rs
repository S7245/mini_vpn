//! 刀6/刀7：VLESS+REALITY 抗封锁第二传输（手写 TLS 1.3，见 ADR-0008/0009 / docs/tech/2026-06-2x-knife{6,7}-*）。
//!
//! 中文要点：REALITY 把 auth 藏进 TLS 1.3 ClientHello 的 `session_id`（AES-256-GCM 密文 over ClientHello transcript），
//! stock TLS 库不让写 session_id → 我方**手写 TLS 1.3 字节**，RustCrypto 仅作密码学原语（不引入第二个 TLS 库，不破 ADR-0003）。
//! 刀6 = sans-IO auth 密码学 + ClientHello；刀7 = sans-IO ServerHello 解析 + TLS 1.3 key schedule + record-layer AEAD
//! （全离线，RFC 8448 §3 KAT）；刀8 = 实 TCP 握手 + 解密 server flight + 证书 HMAC + VLESS + RealityUpstream + acceptance。

pub mod auth;
pub mod cert;
pub mod client_hello;
pub mod handshake;
pub mod key_schedule;
pub mod record;
pub mod server_hello;
pub mod vless;

/// reality 模块测试共用 helper（hex 解码），供 auth/key_schedule/record/server_hello 单测复用。
#[cfg(test)]
pub(crate) mod testutil;
