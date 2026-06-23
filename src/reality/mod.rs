//! 刀6：VLESS+REALITY 抗封锁第二传输（手写 TLS 1.3，见 ADR-0008 / docs/tech/2026-06-22-knife6-*）。
//!
//! 中文要点：REALITY 把 auth 藏进 TLS 1.3 ClientHello 的 `session_id`
//! （AES-128-GCM 密文 over ClientHello transcript），stock TLS 库不让写 session_id → 我方**手写 ClientHello 字节**，
//! RustCrypto 仅作密码学原语（不引入第二个 TLS 库，不破 ADR-0003）。本刀(刀6) = sans-IO 的 auth 密码学 + ClientHello 构造，
//! 100% 离线 TDD；真握手 / ServerHello / key schedule / VLESS / 互通 acceptance 归刀7/8。

pub mod auth;
pub mod client_hello;
