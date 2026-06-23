//! REALITY 第二 Transport 上游（刀8 T9/T10，见 spec §4 + brief §3）。
//!
//! 中文要点：`RealityUpstream` impl `ProxyUpstream::open_tcp`——每条 TCP 新建一次完整 REALITY 握手
//! + VLESS 请求 → 返回 `RealityStream`（impl AsyncRead+AsyncWrite over TLS 1.3 app record 层 + VLESS 响应 strip）。
//! impl `DatagramUpstream::send_udp` = **no-op 静默丢**（REALITY 是 TCP-only，UDP-over-VLESS 是刀9）。
//! `RealityClientConfig::from_env`（MINI_VPN_REALITY_*，脱敏 Debug）。无连接复用（reuse 留刀9）。
