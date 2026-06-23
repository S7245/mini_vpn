//! REALITY 实 TLS 1.3 握手驱动（刀8 T5/T6，见 spec §5 时序 + brief §1.1/1.2）。
//!
//! 中文要点：薄 async 驱动 `drive<S: AsyncRead+AsyncWrite>` 编排已有纯步骤（parse_server_hello /
//! derive_handshake_keys / verify_server_cert / compute_finished_verify_data / derive_application_keys /
//! RecordKeys），延续刀6/7 sans-IO + RFC 8448 KAT 纪律。互通-critical（别再踩）：
//! - 明文 CH record 头 `16 03 01`（自加 5B）；密文 `17 03 03`；客户端自发 dummy CCS `14 03 03 00 01 01`。
//! - 收到的 server CCS(0x14) 整条丢弃、**不 open、不递增 server read seq**。
//! - server flight 内层 0x16 跨 record 重组（独立 handshake buffer，按 1B type + 3B len 切 message）。
//! - seq：read s_hs→s_ap、write c_hs→c_ap 切密钥各归零。
//! - transcript：server Finished 验=hash(CH..CertVerify)；client Finished+app keys=hash(CH..serverFinished)；
//!   **CertVerify 字节必折**（即便 defer 验签，ADR-0010）。
//! - 两个 ECDH 别混：TLS 握手 ECDHE=x25519(client 临时, server SH 临时 keyshare)；REALITY AuthKey=x25519(client 临时, server 静态 pbk)。
