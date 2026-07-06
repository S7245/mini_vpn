//! TUIC v5 client (Stage 13a) — command codec + client upstream.
//!
//! 中文要点：实现成熟的 TUIC v5 协议(出口对接 sing-box,client-only,见 ADR-0004)。
//! 本文件先落「命令编码」纯函数(TDD 主战场),字节布局**严格按 TUIC v5 规范**,与 sing-box 字节级互通。
//! 线格式参考见 docs/tech/2026-06-08-stage-13a-tuic-tcp-connect-plan.md。

use crate::metrics::{Metrics, note_pressure_edge};
use crate::quic;
use crate::shared::{ClientError, TargetAddr};
use crate::udp_relay::{FlowEntry, FourTuple, MAX_UDP_FLOWS};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use quinn::{Connection, Endpoint, VarInt};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::Mutex;
use tokio::sync::{mpsc, watch};

/// 默认 ALPN：TUIC over QUIC 常用 `h3`，必须与 sing-box `tls.alpn` 一致。
const DEFAULT_TUIC_ALPN: &str = "h3";
const DEFAULT_TUIC_SNI: &str = "localhost";
const DEFAULT_TUIC_CA_PATH: &str = "cert.pem";
/// 默认拥塞控制器 = **cubic**（刀3.5 真出口实测裁决，2026-06-17）：在高 RTT(深圳→US ~175ms)+
/// datagram 数据面上，Cubic 显著优于 BBR——40M offered 下 Cubic 39.8M/0.25%，BBR 仅 30.1M/24%
/// （cwnd 暴涨到 245K、RTT 178→252ms bufferbloat，对不可靠 datagram 过驱狂发不退）。quinn 0.10 的 BBR
/// 对 unreliable datagram 有害。BBR 仍可经 `MINI_VPN_TUIC_CC=bbr` 显式选用（实验/特定链路）。
const DEFAULT_TUIC_CC: &str = "cubic";
const DEFAULT_TUIC_UDP_MODE: &str = "native";
const MIN_TUIC_TCP_POOL: usize = 1;
// Knife14cd: pool=2 keeps concurrent TCP control/data streams off the same QUIC connection by default.
// Explicit pool=1 remains supported for constrained servers and single-connection A/B diagnostics.
const DEFAULT_TUIC_TCP_POOL: usize = 2;
const MAX_TUIC_TCP_POOL: usize = 16;
const DEFAULT_TUIC_QUIC_STATS_SECS: u64 = 30;
const TUIC_TCP_STREAM_READ_GAP_LOG_MS: u128 = 1_000;
const TUIC_TCP_STREAM_PENDING_LOG_MS: u128 = TUIC_TCP_STREAM_READ_GAP_LOG_MS;

/// TUIC 客户端配置（单一事实源；桌面从 env 加载，移动端将来从 file/FFI 注入）。
/// 中文要点：凭据(uuid/password)经自定义 Debug **脱敏**，绝不随日志泄漏。
#[derive(Clone)]
pub struct TuicClientConfig {
    pub server: SocketAddr,
    pub uuid: [u8; 16],
    pub password: String,
    pub sni: String,
    pub ca_path: String,
    pub alpn: String,
    pub congestion_control: String,
    pub udp_relay_mode: String,
    pub tcp_pool: usize,
    /// QUIC connection stats logging interval. `None` keeps normal runtime quiet; acceptance
    /// enables it through `MINI_VPN_TCP_DIAG=1`, or explicitly via
    /// `MINI_VPN_TUIC_QUIC_STATS_SECS`.
    pub quic_stats_secs: Option<u64>,
    /// 重连是否尝试 QUIC 0-RTT（**默认 false**：quinn 0.10 在 0-RTT 阶段不支持 `export_keying_material`，
    /// TUIC auth 必失败、自愈回落 1-RTT；显式开仅供实验/未来 quinn 升级。失败时总能回落，不致命）。
    pub zero_rtt: bool,
}

impl std::fmt::Debug for TuicClientConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TuicClientConfig")
            .field("server", &self.server)
            .field("uuid", &"<redacted>")
            .field("password", &"<redacted>")
            .field("sni", &self.sni)
            .field("ca_path", &self.ca_path)
            .field("alpn", &self.alpn)
            .field("congestion_control", &self.congestion_control)
            .field("udp_relay_mode", &self.udp_relay_mode)
            .field("tcp_pool", &self.tcp_pool)
            .field("quic_stats_secs", &self.quic_stats_secs)
            .field("zero_rtt", &self.zero_rtt)
            .finish()
    }
}

/// 解析带连字符的 UUID 字符串 → 16 字节。非法返回 None。
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

impl TuicClientConfig {
    /// 从可选字符串源构建（server/uuid/password 必填）。
    pub fn from_sources(
        server: Option<&str>,
        uuid: Option<&str>,
        password: Option<&str>,
        sni: Option<&str>,
        ca_path: Option<&str>,
        alpn: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server = server
            .ok_or_else(|| ClientError::InvalidTarget("tuic server addr required".into()))?
            .parse::<SocketAddr>()
            .map_err(|_| ClientError::InvalidTarget("invalid tuic server addr".into()))?;
        let uuid = parse_uuid(
            uuid.ok_or_else(|| ClientError::InvalidTarget("tuic uuid required".into()))?,
        )
        .ok_or_else(|| ClientError::InvalidTarget("invalid tuic uuid".into()))?;
        let password = password
            .filter(|p| !p.is_empty())
            .ok_or_else(|| ClientError::InvalidTarget("tuic password required".into()))?
            .to_string();
        Ok(Self {
            server,
            uuid,
            password,
            sni: sni.unwrap_or(DEFAULT_TUIC_SNI).to_string(),
            ca_path: ca_path.unwrap_or(DEFAULT_TUIC_CA_PATH).to_string(),
            alpn: alpn.unwrap_or(DEFAULT_TUIC_ALPN).to_string(),
            congestion_control: DEFAULT_TUIC_CC.to_string(),
            udp_relay_mode: DEFAULT_TUIC_UDP_MODE.to_string(),
            tcp_pool: DEFAULT_TUIC_TCP_POOL,
            quic_stats_secs: None,
            // 默认关：quinn 0.10 在 0-RTT 阶段不支持 export_keying_material（TUIC auth 必失败回落）。
            // 显式 `MINI_VPN_TUIC_ZERO_RTT=true` 可启用（实验/未来 quinn 升级）。
            zero_rtt: false,
        })
    }

    /// 从进程环境读取（`MINI_VPN_TUIC_*`）。
    pub fn from_env() -> Result<Self, ClientError> {
        let g = |k: &str| std::env::var(k).ok();
        let mut cfg = Self::from_sources(
            g("MINI_VPN_TUIC_SERVER").as_deref(),
            g("MINI_VPN_TUIC_UUID").as_deref(),
            g("MINI_VPN_TUIC_PASSWORD").as_deref(),
            g("MINI_VPN_TUIC_SNI").as_deref(),
            g("MINI_VPN_TUIC_CA_PATH").as_deref(),
            g("MINI_VPN_TUIC_ALPN").as_deref(),
        )?;
        cfg.zero_rtt = parse_zero_rtt(g("MINI_VPN_TUIC_ZERO_RTT").as_deref());
        // 刀3.5：CC / UDP relay mode 经 env 覆盖（A/B 归因必需）。原始字符串存字段，
        // 解析+回落在使用点（`parse_cc` / `UdpRelayMode::parse`）——存而未用的字段终于接上。
        cfg.congestion_control = override_field(cfg.congestion_control, g("MINI_VPN_TUIC_CC"));
        cfg.udp_relay_mode = override_field(cfg.udp_relay_mode, g("MINI_VPN_TUIC_UDP_MODE"));
        cfg.tcp_pool = parse_tcp_pool(g("MINI_VPN_TUIC_TCP_POOL").as_deref());
        cfg.quic_stats_secs = parse_quic_stats_secs(
            g("MINI_VPN_TUIC_QUIC_STATS_SECS").as_deref(),
            g("MINI_VPN_TCP_DIAG").as_deref(),
            g("MINI_VPN_METRICS_SECS").as_deref(),
        );
        Ok(cfg)
    }
}

/// 用 env 值覆盖默认：非空白则取 env 值，否则保留默认。
/// 中文要点：抽成纯函数便于单测（不污染进程环境），与 `parse_zero_rtt` 同 idiom。
fn override_field(default: String, env_val: Option<String>) -> String {
    match env_val {
        Some(v) if !v.trim().is_empty() => v,
        _ => default,
    }
}

/// 解析 `MINI_VPN_TUIC_TCP_POOL`：默认 2；非法/空白回默认；0 裁到最小 1；大值裁到上限。
/// 中文要点：Knife14cd 默认隔离并发 TCP streams；显式 1 仍可复现单连接 A/B，且永不产生空连接池。
fn parse_tcp_pool(s: Option<&str>) -> usize {
    s.and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_TUIC_TCP_POOL)
        .clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL)
}

/// 解析 QUIC stats 日志周期。
/// 中文要点：acceptance 已用 `MINI_VPN_TCP_DIAG=1` 打开 TCP 诊断，因此默认跟随该开关并复用
/// `MINI_VPN_METRICS_SECS` 周期；生产默认静默。显式 `MINI_VPN_TUIC_QUIC_STATS_SECS=0` 可关闭。
fn parse_quic_stats_secs(
    explicit: Option<&str>,
    tcp_diag: Option<&str>,
    metrics_secs: Option<&str>,
) -> Option<u64> {
    if let Some(v) = explicit {
        return v
            .trim()
            .parse::<u64>()
            .ok()
            .and_then(|secs| (secs > 0).then_some(secs));
    }
    if !parse_truthy(tcp_diag) {
        return None;
    }
    Some(
        metrics_secs
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|&secs| secs > 0)
            .unwrap_or(DEFAULT_TUIC_QUIC_STATS_SECS),
    )
}

fn parse_truthy(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true") | Some("1") | Some("on") | Some("yes")
    )
}

fn tcp_diag_enabled() -> bool {
    static TCP_DIAG: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TCP_DIAG.get_or_init(|| parse_truthy(std::env::var("MINI_VPN_TCP_DIAG").ok().as_deref()))
}

/// TCP pool 轮询选择。`pool_len` 在生产中恒非 0；纯函数保底处理 0，避免测试/未来误用 panic。
fn tcp_pool_index(pool_len: usize, cursor: u64) -> usize {
    if pool_len == 0 {
        0
    } else {
        (cursor % pool_len as u64) as usize
    }
}

fn tcp_pool_slot_stale(now_secs: u64, last_used_secs: u64) -> bool {
    now_secs.saturating_sub(last_used_secs) >= TUIC_TCP_POOL_STALE_RECONNECT_SECS
}

fn tcp_pool_stale_reconnect_reason(
    index: usize,
    now_secs: u64,
    last_used_secs: u64,
    idle_exclusive: bool,
) -> Option<&'static str> {
    if index == 0 || !idle_exclusive {
        None
    } else {
        tcp_pool_slot_stale(now_secs, last_used_secs).then_some("stale_tcp_pool_slot")
    }
}

struct TcpPoolSlotLease {
    active: Arc<AtomicU64>,
}

impl TcpPoolSlotLease {
    fn reserve(active: Arc<AtomicU64>) -> (Self, bool) {
        match active.compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => (Self { active }, true),
            Err(_) => {
                active.fetch_add(1, Ordering::AcqRel);
                (Self { active }, false)
            }
        }
    }
}

impl Drop for TcpPoolSlotLease {
    fn drop(&mut self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

struct TrackedRelayStream<S> {
    inner: S,
    tcp_diag: Option<TuicTcpStreamDiag>,
    _lease: TcpPoolSlotLease,
}

impl<S> TrackedRelayStream<S> {
    fn new(inner: S, lease: TcpPoolSlotLease, tcp_diag: Option<TuicTcpStreamDiag>) -> Self {
        Self {
            inner,
            tcp_diag,
            _lease: lease,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for TrackedRelayStream<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before_len = buf.filled().len();
        let now = Instant::now();
        if let Some(diag) = self.tcp_diag.as_mut() {
            diag.note_poll_at(now);
        }
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        match &poll {
            Poll::Ready(Ok(())) => {
                let read_bytes = buf.filled().len().saturating_sub(before_len);
                if read_bytes > 0
                    && let Some(diag) = self.tcp_diag.as_mut()
                {
                    let event = diag.note_read_at(read_bytes, now);
                    let meta = diag.meta.clone();
                    if let Some(first_rx_ms) = event.first_rx_ms {
                        println!(
                            "{}",
                            format_tuic_tcp_stream_first_rx_line(
                                &meta,
                                first_rx_ms,
                                event.read_bytes,
                                event.reads
                            )
                        );
                    }
                    if let Some(gap_ms) = event.gap_ms
                        && gap_ms >= TUIC_TCP_STREAM_READ_GAP_LOG_MS
                    {
                        println!(
                            "{}",
                            format_tuic_tcp_stream_read_gap_line(
                                &meta,
                                gap_ms,
                                event.read_bytes,
                                event.reads,
                                event.rx_bytes
                            )
                        );
                    }
                }
            }
            Poll::Pending => {
                if let Some(diag) = self.tcp_diag.as_mut()
                    && let Some(event) = diag.note_pending_at(now)
                {
                    let meta = diag.meta.clone();
                    println!(
                        "{}",
                        format_tuic_tcp_stream_pending_line(
                            &meta,
                            event.pending_gap_ms,
                            event.pending_polls,
                            event.polls,
                            event.max_poll_gap_ms,
                            event.rx_bytes,
                            event.reads
                        )
                    );
                }
            }
            Poll::Ready(Err(_)) => {}
        }
        poll
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for TrackedRelayStream<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

impl<S> Drop for TrackedRelayStream<S> {
    fn drop(&mut self) {
        if let Some(diag) = &self.tcp_diag {
            let snapshot = diag.close_snapshot();
            println!(
                "{}",
                format_tuic_tcp_stream_close_line(&diag.meta, &snapshot)
            );
        }
    }
}

/// 解析 `MINI_VPN_TUIC_ZERO_RTT`：**默认关**；显式 `true`/`1`/`on`/`yes`（大小写无关）开。
/// 中文要点：quinn 0.10 / rustls 0.21 在 0-RTT(握手未完成)阶段**不支持 `export_keying_material`**，
/// 而 TUIC token 依赖它 → 0-RTT 认证必失败、自愈回落 1-RTT（实测 2026-06-11，见 13c 验收）。默认开纯属
/// 每次重连白跑一次握手，故默认关；保留开关供未来 quinn 支持 0-RTT keying-material 后启用。
fn parse_zero_rtt(s: Option<&str>) -> bool {
    parse_truthy(s)
}

/// TUIC 协议版本字节。
const TUIC_VER: u8 = 0x05;
/// 命令类型。
const CMD_AUTHENTICATE: u8 = 0x00;
const CMD_CONNECT: u8 = 0x01;
const CMD_PACKET: u8 = 0x02;
const CMD_HEARTBEAT: u8 = 0x04;
/// 地址 None 类型(回程可能省略地址)。
const ATYP_NONE: u8 = 0xff;

/// TUIC Heartbeat 周期：连接空闲时维持 NAT 映射/路径活性。取 3s，给 sing-box 的空闲超时留足余量。
const TUIC_HEARTBEAT_SECS: u64 = 3;
/// TUIC Heartbeat 活跃窗口：距上次 UDP 上行多久(秒)内仍算「UDP 活跃」、需要发心跳。
/// 中文要点：Heartbeat 是**应用层 UDP 会话保活**(让 sing-box 不回收关联)；纯 TCP 会话由 QUIC keep-alive
/// 保活、不需要它。取 60s 与 UDP flow 空闲回收(`UDP_FLOW_IDLE_SECS`)对齐：UDP 静默到该回收时，心跳也停。
const TUIC_HB_IDLE_WINDOW_SECS: u64 = 60;
/// 下行 datagram channel 容量。背压由 pump 承担（`send().await`），不丢下行（DNS 响应不能丢）。
const TUIC_DOWNLINK_CAPACITY: usize = 1024;
/// 一个 TUIC Packet 命令的字节上限（读 uni-stream 的 `read_to_end` size_limit，防恶意/异常无界流）。
/// UDP 载荷最大 65507 + TUIC 头/地址 ≈ 65537 → 取 66KiB 兜头。
const MAX_TUIC_PACKET_BYTES: usize = 66 * 1024;
/// 下行 uni-stream 并发读取上限：超额直接丢弃该 stream（reset），防 flood 下无界派生任务。
/// quic-relay-mode 下每包一条 uni-stream；256 并发够吸收突发，又不至失控。
const MAX_CONCURRENT_DOWNLINK_STREAMS: usize = 256;
/// UDP 上行统计行打印周期（秒）：周期性把 stream 兜底/丢弃计数打一行，供真出口 acceptance 与生产观测。
const UDP_STATS_LOG_SECS: u64 = 30;
/// UDP 驱动重连退避上限（确定性指数退避，无需 rand；UDP 自愈，重连节奏不敏感）。
const UDP_RECONNECT_CAP_MS: u64 = 30_000;
const UDP_RECONNECT_BASE_MS: u64 = 500;
/// 地址类型(注意：TUIC 的 ATYP 取值与我们 Stage-12 自定义的不同)。
const ATYP_DOMAIN: u8 = 0x00;
const ATYP_IPV4: u8 = 0x01;
const ATYP_IPV6: u8 = 0x02;

/// 编码 TUIC 地址：`[ATYP][ADDR][PORT:u16 BE]`。
/// 中文要点：域名 `[len:u8][bytes]`，IPv4 4B，IPv6 16B；域名超 255 字节按 255 截断(不 panic)。
pub fn encode_address(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::new();
    match target {
        TargetAddr::IpPort(SocketAddr::V4(a)) => {
            v.push(ATYP_IPV4);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::IpPort(SocketAddr::V6(a)) => {
            v.push(ATYP_IPV6);
            v.extend_from_slice(&a.ip().octets());
            v.extend_from_slice(&a.port().to_be_bytes());
        }
        TargetAddr::DomainPort { host, port } => {
            let bytes = host.as_bytes();
            let len = bytes.len().min(u8::MAX as usize);
            v.push(ATYP_DOMAIN);
            v.push(len as u8);
            v.extend_from_slice(&bytes[..len]);
            v.extend_from_slice(&port.to_be_bytes());
        }
    }
    v
}

/// 编码 Authenticate 命令(走单向流)：`[0x05][0x00][UUID:16][TOKEN:32]`。
pub fn encode_authenticate(uuid: &[u8; 16], token: &[u8; 32]) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 16 + 32);
    v.push(TUIC_VER);
    v.push(CMD_AUTHENTICATE);
    v.extend_from_slice(uuid);
    v.extend_from_slice(token);
    v
}

/// 编码 Connect 命令(走双向流，随后直接搬字节)：`[0x05][0x01][ADDR]`。
pub fn encode_connect(target: &TargetAddr) -> Vec<u8> {
    let mut v = Vec::with_capacity(2 + 19);
    v.push(TUIC_VER);
    v.push(CMD_CONNECT);
    v.extend_from_slice(&encode_address(target));
    v
}

/// 编码 TUIC `Packet`(native datagram)：
/// `[0x05][0x02][ASSOC:u16][PKT_ID:u16=0][FRAG_TOTAL=1][FRAG_ID=0][SIZE:u16][ADDR][data]`。
pub fn encode_packet(assoc_id: u16, target: &TargetAddr, data: &[u8]) -> Vec<u8> {
    let addr = encode_address(target);
    let mut v = Vec::with_capacity(10 + addr.len() + data.len());
    v.push(TUIC_VER);
    v.push(CMD_PACKET);
    v.extend_from_slice(&assoc_id.to_be_bytes());
    v.extend_from_slice(&0u16.to_be_bytes()); // PKT_ID(native 不重组,固定 0)
    v.push(1); // FRAG_TOTAL
    v.push(0); // FRAG_ID
    v.extend_from_slice(&(data.len() as u16).to_be_bytes()); // SIZE
    v.extend_from_slice(&addr);
    v.extend_from_slice(data);
    v
}

/// 下行 Packet 的 frag 感知元信息（native 分片重组用）。
/// 中文要点：`data` 是**本（分片）chunk**（SIZE 字节）；整包由同 `(assoc_id,pkt_id)` 的
/// 各 `frag_id` 按序拼接而成（ADDR 仅在 `frag_id==0`，后续分片为 ATYP_NONE，由 `address_len` 跳过）。
#[derive(Debug, PartialEq, Eq)]
pub struct PacketMeta<'a> {
    pub assoc_id: u16,
    pub pkt_id: u16,
    pub frag_total: u8,
    pub frag_id: u8,
    pub data: &'a [u8],
}

/// 解码下行 `Packet` 的完整元信息（含 FRAG 字段，供重组）。越界/地址类型未知返回 None（不 panic）。
pub fn decode_packet_meta(buf: &[u8]) -> Option<PacketMeta<'_>> {
    // 固定前缀 10 字节:ver type assoc(2) pkt(2) ftot fid size(2)。
    if buf.len() < 10 {
        return None;
    }
    let size = u16::from_be_bytes([buf[8], buf[9]]) as usize;
    let addr_len = address_len(buf, 10)?;
    let data_start = 10 + addr_len;
    let data_end = data_start.checked_add(size)?;
    if buf.len() < data_end {
        return None;
    }
    Some(PacketMeta {
        assoc_id: u16::from_be_bytes([buf[2], buf[3]]),
        pkt_id: u16::from_be_bytes([buf[4], buf[5]]),
        frag_total: buf[6],
        frag_id: buf[7],
        data: &buf[data_start..data_end],
    })
}

/// 解码下行 `Packet`,只取 `(assoc_id, data)`(跳过 ADDR/FRAG)。越界/地址类型未知返回 None。
/// 中文要点：`decode_packet_meta` 的薄包装，服务 `FRAG_TOTAL==1` 快路径（零回归）。
pub fn decode_packet(buf: &[u8]) -> Option<(u16, &[u8])> {
    decode_packet_meta(buf).map(|m| (m.assoc_id, m.data))
}

/// 下行 native 分片重组的并发未完成包上限（到顶 LRU 驱逐最老）。
pub const FRAG_REASSEMBLY_CAP: usize = 256;
/// 未集齐的分片包存活上限（秒）：超时即弃（一片丢 → 整包弃，保直播 liveness，不无限等）。
pub const FRAG_REASSEMBLY_TTL_SECS: u64 = 10;

/// 一个未完成 UDP 包的分片缓冲（按 `(assoc_id, pkt_id)` 索引）。
/// 中文要点：`frags.len()` 即 frag_total（构造后不变），不再单独存字段（单一事实源）。
struct FragPartial {
    frags: Vec<Option<Vec<u8>>>, // 下标 = frag_id；len == frag_total
    received: usize,
    first_seen: u64,
}

/// native 下行分片重组器（**纯状态机**，主循环独占、无锁，与 `AssocTable` 同寿）。
/// 中文要点：server native 模式把大下行包拆成多个 `FRAG_TOTAL>1` 的 Packet 命令，
/// 本器按 `(assoc_id, pkt_id)` 收集、集齐按 `frag_id` 序拼接还原整包。`FRAG_TOTAL==1` 直通快路径。
pub struct FragReassembler {
    partials: HashMap<(u16, u16), FragPartial>,
    cap: usize,
}

impl Default for FragReassembler {
    fn default() -> Self {
        Self::new()
    }
}

impl FragReassembler {
    pub fn new() -> Self {
        Self::with_cap(FRAG_REASSEMBLY_CAP)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            partials: HashMap::new(),
            cap: cap.max(1),
        }
    }

    /// 喂入一个（分片）Packet。集齐返回完整 payload；未齐/无效返回 None。
    /// 中文要点：`FRAG_TOTAL==1` 直通不入表；`frag_id>=frag_total` 或 `frag_total==0` 视为无效丢弃；
    /// 重复 frag_id **last-writer-wins**（保留较新；见下「跨重连」）；新 key 到 cap 触发 LRU（按 first_seen）驱逐最老。
    /// 跨重连：连接断/重连后 server 可能复用同 `(assoc_id, pkt_id)`。frag_total 变 → 整体重建；frag_total 同
    /// → last-writer-wins 让新连接的 frag 覆盖残片（减少串味），未被新包重发的残留 slot 仍可能混入，由
    /// `FRAG_REASSEMBLY_TTL_SECS` sweep 兜底（窄窗、可容忍：分片仅大包尾部，重组失败应用自愈）。
    pub fn accept(&mut self, m: &PacketMeta, now: u64) -> Option<Vec<u8>> {
        if m.frag_total == 0 || m.frag_id >= m.frag_total {
            return None; // 无效分片头，丢弃（防越界/除零）
        }
        if m.frag_total == 1 {
            return Some(m.data.to_vec()); // 快路径：单帧直通，不入表
        }
        let key = (m.assoc_id, m.pkt_id);
        let frag_total = m.frag_total as usize;
        // pkt_id 复用但 frag_total 变化 → 旧残留作废，按新分片重建。
        if self
            .partials
            .get(&key)
            .is_some_and(|p| p.frags.len() != frag_total)
        {
            self.partials.remove(&key);
        }
        // 新 key 到 cap → 先驱逐最老未完成（在借用 entry 前做，避免交叉借用）。
        if !self.partials.contains_key(&key) {
            self.evict_if_full();
        }
        let p = self.partials.entry(key).or_insert_with(|| FragPartial {
            frags: (0..frag_total).map(|_| None).collect(),
            received: 0,
            first_seen: now,
        });
        let slot = &mut p.frags[m.frag_id as usize];
        if slot.is_none() {
            p.received += 1;
        }
        *slot = Some(m.data.to_vec()); // last-writer-wins（覆盖；跨重连残片被新 frag 顶替）
        if p.received < p.frags.len() {
            return None;
        }
        // 集齐：按 frag_id 序拼接，清出该项。
        let done = self.partials.remove(&key).expect("present");
        let mut whole = Vec::with_capacity(done.frags.iter().flatten().map(Vec::len).sum());
        for frag in done.frags.into_iter().flatten() {
            whole.extend_from_slice(&frag);
        }
        Some(whole)
    }

    /// 回收未集齐且超 `ttl` 秒的分片包（丢片自愈，防内存泄漏）。
    pub fn sweep(&mut self, now: u64, ttl: u64) {
        self.partials
            .retain(|_, p| now.saturating_sub(p.first_seen) <= ttl);
    }

    /// 新 key 到 cap 前驱逐最老（first_seen 最小）未完成包。
    fn evict_if_full(&mut self) {
        if self.partials.len() < self.cap {
            return;
        }
        if let Some(victim) = self
            .partials
            .iter()
            .min_by_key(|(k, p)| (p.first_seen, **k))
            .map(|(k, _)| *k)
        {
            self.partials.remove(&victim);
        }
    }

    #[cfg(test)]
    fn pending_len(&self) -> usize {
        self.partials.len()
    }
}

/// 编码 Heartbeat：`[0x05][0x04]`。
pub fn encode_heartbeat() -> Vec<u8> {
    vec![TUIC_VER, CMD_HEARTBEAT]
}

/// ADDR 段的字节长度(用于解码时跳过地址)。
fn address_len(buf: &[u8], pos: usize) -> Option<usize> {
    match *buf.get(pos)? {
        ATYP_IPV4 => Some(1 + 4 + 2),
        ATYP_IPV6 => Some(1 + 16 + 2),
        ATYP_DOMAIN => {
            let l = *buf.get(pos + 1)? as usize;
            Some(1 + 1 + l + 2)
        }
        ATYP_NONE => Some(1),
        _ => None,
    }
}

/// TUIC UDP 关联表:每条 UDP flow(4 元组)分配一个 **u16 assoc-id**(≈ Stage 12 flow-id),
/// 双向 demux 用。主循环独占、无锁;`now` 注入便于单测。结构同 udp_relay::FlowTable,只是 id 宽 16 位。
/// 中文要点:复用 FourTuple/FlowEntry,不动 Stage 12 的 FlowTable(系统稳定优先,接受少量重复)。
#[derive(Debug)]
pub struct AssocTable {
    tuple_to_id: HashMap<FourTuple, u16>,
    id_to_entry: HashMap<u16, FlowEntry>,
    /// 刀2：assoc-id → 本 UDP flow 占用的 fake-IP（若 target 经 fake-IP 改写）。
    /// 中文要点：用于回收（evict/sweep）该 assoc 时知道要 `release` 哪个 fake-IP（引用计数）。
    id_to_fake_ip: HashMap<u16, Ipv4Addr>,
    /// 刀2：本轮被回收（evict/sweep）且有 fake-IP 的 assoc 的 fake-IP，待主循环 drain 后 `release`。
    reclaimed_fake_ips: Vec<Ipv4Addr>,
    next_id: u16,
    cap: usize,
}

impl Default for AssocTable {
    fn default() -> Self {
        Self::new()
    }
}

impl AssocTable {
    pub fn new() -> Self {
        Self::with_cap(MAX_UDP_FLOWS)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            tuple_to_id: HashMap::new(),
            id_to_entry: HashMap::new(),
            id_to_fake_ip: HashMap::new(),
            reclaimed_fake_ips: Vec::new(),
            next_id: 1,
            cap: cap.max(1).min(u16::MAX as usize),
        }
    }

    /// 刀2：登记该 assoc 占用的 fake-IP（UDP 新 flow 时调，调用方随后 `fake_pool.acquire`）。
    pub fn set_fake_ip(&mut self, assoc_id: u16, ip: Ipv4Addr) {
        self.id_to_fake_ip.insert(assoc_id, ip);
    }

    /// 刀2：取走本轮被回收（evict/sweep）的 fake-IP 列表，主循环对每个 `fake_pool.release`。
    /// 中文要点：assoc 回收与 fake-IP release 解耦——AssocTable 不持有 FakeIpPool，
    /// 只累积「该 release 谁」，由独占两者的主循环执行，避免交叉借用/循环依赖。
    pub fn take_reclaimed_fake_ips(&mut self) -> Vec<Ipv4Addr> {
        std::mem::take(&mut self.reclaimed_fake_ips)
    }

    /// assoc 被回收时，若它占用了 fake-IP，移出映射并记入待 release 队列。
    fn note_reclaimed(&mut self, assoc_id: u16) {
        if let Some(ip) = self.id_to_fake_ip.remove(&assoc_id) {
            self.reclaimed_fake_ips.push(ip);
        }
    }

    /// 查/铸 assoc-id（已在册稳定返回;否则铸新,到顶 LRU 驱逐）。
    pub fn intern(&mut self, tuple: FourTuple) -> u16 {
        if let Some(&id) = self.tuple_to_id.get(&tuple) {
            return id;
        }
        if self.id_to_entry.len() >= self.cap {
            self.evict_lru();
        }
        let id = self.alloc_id();
        self.id_to_entry.insert(
            id,
            FlowEntry {
                tuple,
                last_activity: 0,
            },
        );
        self.tuple_to_id.insert(tuple, id);
        id
    }

    /// 分配一个**空闲且非 0** 的 assoc-id（单调推进，跳过 0 与仍在册的 id）。
    /// 中文要点：u16 空间仅 65535,回绕后 `next_id` 可能落到仍在册的 flow 上——不跳过就会
    /// `insert` 覆盖活跃 flow(回程串到错误端点)并泄漏其 tuple_to_id 映射。活跃集(≤cap≤1024)
    /// 远小于 id 空间,故必有空闲 id,扫描代价极小。调用前已 evict 保证 len<cap,循环必然终止。
    fn alloc_id(&mut self) -> u16 {
        loop {
            let id = self.next_id;
            self.next_id = self.next_id.wrapping_add(1);
            if self.next_id == 0 {
                self.next_id = 1; // 跳过 0,保持非零、单调
            }
            if !self.id_to_entry.contains_key(&id) {
                return id;
            }
        }
    }

    pub fn resolve(&self, assoc_id: u16) -> Option<&FlowEntry> {
        self.id_to_entry.get(&assoc_id)
    }

    /// 该 4 元组是否已在册（用于"每流一次"日志，避免热路径刷屏）。
    pub fn contains(&self, tuple: &FourTuple) -> bool {
        self.tuple_to_id.contains_key(tuple)
    }

    pub fn touch(&mut self, assoc_id: u16, now: u64) {
        if let Some(e) = self.id_to_entry.get_mut(&assoc_id) {
            e.last_activity = now;
        }
    }

    /// 回收 idle 超 `idle_secs` 的 assoc，**直接返回**这些 assoc 占用的 fake-IP（供调用方 release）。
    /// 中文要点：review #8——sweep 自包含返回，不再依赖 stash-and-drain（调用方无需记得随后 take）；
    /// `take_reclaimed_fake_ips` 仅服务 intern 内部的 LRU 驱逐（那里无法返回值）。
    pub fn sweep(&mut self, now: u64, idle_secs: u64) -> Vec<Ipv4Addr> {
        let expired: Vec<(u16, FourTuple)> = self
            .id_to_entry
            .iter()
            .filter(|(_, e)| now.saturating_sub(e.last_activity) > idle_secs)
            .map(|(id, e)| (*id, e.tuple))
            .collect();
        let mut reclaimed = Vec::new();
        for (id, tuple) in expired {
            self.id_to_entry.remove(&id);
            self.tuple_to_id.remove(&tuple);
            if let Some(ip) = self.id_to_fake_ip.remove(&id) {
                reclaimed.push(ip); // 刀2：该 assoc 占用的 fake-IP → 调用方 release
            }
        }
        reclaimed
    }

    fn evict_lru(&mut self) {
        let victim = self
            .id_to_entry
            .iter()
            .min_by_key(|(id, e)| (e.last_activity, **id))
            .map(|(id, _)| *id);
        if let Some(id) = victim
            && let Some(e) = self.id_to_entry.remove(&id)
        {
            self.tuple_to_id.remove(&e.tuple);
            self.note_reclaimed(id); // 刀2：LRU 驱逐时同样回收 fake-IP
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.id_to_entry.len()
    }
}

/// 上行 UDP 的传输选择：datagram 快路径 vs uni-stream 兜底。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UdpSend {
    Datagram,
    Stream,
}

/// TUIC UDP relay mode（per-association；本项目按 config 全局选一种）。
/// 中文要点（已查证 TUIC SPEC + 刀3.5 spec）：`Native` = QUIC datagram（低延迟、但高 RTT/丢包路径有
/// ~5.3M 硬天花板、溢出静默丢）；`Quic` = 每个 `Packet` 一条 uni-stream（可靠、摆脱天花板，代价是每包建流）。
/// **server 按某 assoc 首包 mode 镜像下行**——故 `Quic` 模式首包即走 stream → 下行也镜像 stream（高码率必需）。
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum UdpRelayMode {
    Native,
    Quic,
}

impl UdpRelayMode {
    /// 解析配置字符串（大小写不敏感）。非法返回 None（调用方决定回落/报错）。
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "native" => Some(Self::Native),
            "quic" => Some(Self::Quic),
            _ => None,
        }
    }
}

/// 纯决策：给定 relay mode、当前 datagram 发送上限（`conn.max_datagram_size()`）与待发字节数，选传输。
/// 中文要点：
/// - `Quic` → **恒 Stream**（首包起即 uni-stream，触发 server 下行镜像 stream，摆脱 datagram 天花板）。
/// - `Native` → size-based：装得下走 datagram（含边界 `len==max`）；超上限或 datagram 不可用（`None`）→ stream 兜底
///   （刀3 现行语义，零回归）。主动分流（先查 max_datagram_size），避免 `send_datagram` 返回 `TooLarge` 的往返。
pub fn udp_send_plan(mode: UdpRelayMode, max_datagram: Option<usize>, len: usize) -> UdpSend {
    match mode {
        UdpRelayMode::Quic => UdpSend::Stream,
        UdpRelayMode::Native => match max_datagram {
            Some(max) if len <= max => UdpSend::Datagram,
            _ => UdpSend::Stream,
        },
    }
}

/// datagram 背压压力信号（刀3.5 可观测）：出向 datagram 缓冲剩余 `< 1 个 MTU` 时，
/// 再发大 datagram 极可能触发 quinn「丢最老」——不报错的静默丢（刀3 acceptance 盲点）。
/// 中文要点：这是**代理信号**（pressure），非逐包精确丢包数；逐包精确留待真做主动背压时一起上。
pub fn is_datagram_pressured(send_buffer_space: usize, mtu: usize) -> bool {
    send_buffer_space < mtu
}

/// 格式化 30s UDP 上行统计行（**纯函数**，便于单测；I/O 取数在 `start_udp`）。
/// 中文要点：在刀3 的「stream 兜底/丢弃」基础上加 quinn 级量化——`RTT/cwnd/丢包/send_buf 余`，
/// 供真出口 acceptance 归因 datagram 天花板成因（CC vs 无背压）。
#[allow(clippy::too_many_arguments)]
pub fn format_udp_stats(
    max_datagram: Option<usize>,
    fallbacks: u64,
    drops: u64,
    rtt_ms: u128,
    cwnd: u64,
    lost: u64,
    sent: u64,
    send_buffer_space: usize,
) -> String {
    format!(
        "📊 TUIC UDP↑: datagram 上限={max_datagram:?} stream 兜底={fallbacks} 丢弃={drops} \
         | RTT={rtt_ms}ms cwnd={cwnd} 丢包={lost}/{sent} send_buf 余={send_buffer_space}B"
    )
}

#[derive(Debug, Default, Clone, Copy)]
struct QuicStatsSnapshot {
    rtt_ms: u128,
    cwnd: u64,
    lost_packets: u64,
    sent_packets: u64,
    lost_bytes: u64,
    congestion_events: u64,
    tx_data_blocked: u64,
    tx_stream_data_blocked: u64,
    tx_streams_blocked_bidi: u64,
    tx_streams_blocked_uni: u64,
    tx_max_data: u64,
    tx_max_stream_data: u64,
    rx_data_blocked: u64,
    rx_stream_data_blocked: u64,
    rx_max_data: u64,
    rx_max_stream_data: u64,
    udp_tx_datagrams: u64,
    udp_tx_bytes: u64,
    udp_rx_datagrams: u64,
    udp_rx_bytes: u64,
    datagram_max: Option<usize>,
    datagram_send_buffer_space: usize,
}

/// 格式化 QUIC 连接级诊断（刀14y）。
/// 中文要点：`tx_blocked` 是本端发送 DATA_BLOCKED/STREAM_DATA_BLOCKED，直接指向对端 flow-control；
/// `cwnd/lost/congestion_events` 指向拥塞/丢包。两组指标一起看，下一轮 acceptance 不再盲猜。
fn format_quic_stats_line(conn_index: usize, stable_id: usize, stats: QuicStatsSnapshot) -> String {
    format!(
        "📊 TUIC QUIC stats conn={conn_index} id={stable_id} \
         rtt={}ms cwnd={} lost={}/{} lost_bytes={} congestion_events={} \
         tx_blocked(data={},stream={},streams_bidi={},streams_uni={}) \
         rx_blocked(data={},stream={}) tx_window(max_data={},max_stream_data={}) \
         rx_window(max_data={},max_stream_data={}) udp_tx={}/{}B udp_rx={}/{}B \
         dg_max={:?} dg_space={}B",
        stats.rtt_ms,
        stats.cwnd,
        stats.lost_packets,
        stats.sent_packets,
        stats.lost_bytes,
        stats.congestion_events,
        stats.tx_data_blocked,
        stats.tx_stream_data_blocked,
        stats.tx_streams_blocked_bidi,
        stats.tx_streams_blocked_uni,
        stats.rx_data_blocked,
        stats.rx_stream_data_blocked,
        stats.tx_max_data,
        stats.tx_max_stream_data,
        stats.rx_max_data,
        stats.rx_max_stream_data,
        stats.udp_tx_datagrams,
        stats.udp_tx_bytes,
        stats.udp_rx_datagrams,
        stats.udp_rx_bytes,
        stats.datagram_max,
        stats.datagram_send_buffer_space
    )
}

fn format_tuic_tcp_open_line(target: &TargetAddr, conn_index: usize, stable_id: usize) -> String {
    format!(
        "🔎 tuic-open-tcp target={} conn={} id={}",
        target.to_wire_string(),
        conn_index,
        stable_id
    )
}

#[derive(Debug, Clone)]
struct TuicTcpStreamDiagMeta {
    target: String,
    conn_index: usize,
    stable_id: usize,
    stream_id: u64,
}

impl TuicTcpStreamDiagMeta {
    fn new(target: &TargetAddr, conn_index: usize, stable_id: usize, stream_id: u64) -> Self {
        Self {
            target: target.to_wire_string(),
            conn_index,
            stable_id,
            stream_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamReadEvent {
    first_rx_ms: Option<u128>,
    gap_ms: Option<u128>,
    read_bytes: usize,
    rx_bytes: u64,
    reads: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamCloseSnapshot {
    first_rx_ms: u128,
    max_read_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    pending_polls: u64,
    max_pending_gap_ms: u128,
    polls: u64,
    max_poll_gap_ms: u128,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TuicTcpStreamPendingEvent {
    pending_gap_ms: u128,
    pending_polls: u64,
    polls: u64,
    max_poll_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
}

#[derive(Debug, Clone)]
struct TuicTcpStreamDiag {
    meta: TuicTcpStreamDiagMeta,
    opened_at: Instant,
    first_rx_ms: Option<u128>,
    last_rx_at: Option<Instant>,
    max_read_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
    polls: u64,
    last_poll_at: Option<Instant>,
    max_poll_gap_ms: u128,
    pending_polls: u64,
    max_pending_gap_ms: u128,
    last_pending_log_at: Option<Instant>,
}

impl TuicTcpStreamDiag {
    fn new(meta: TuicTcpStreamDiagMeta, opened_at: Instant) -> Self {
        Self {
            meta,
            opened_at,
            first_rx_ms: None,
            last_rx_at: None,
            max_read_gap_ms: 0,
            rx_bytes: 0,
            reads: 0,
            polls: 0,
            last_poll_at: None,
            max_poll_gap_ms: 0,
            pending_polls: 0,
            max_pending_gap_ms: 0,
            last_pending_log_at: None,
        }
    }

    fn note_poll_at(&mut self, now: Instant) {
        self.polls += 1;
        if let Some(last) = self.last_poll_at {
            self.max_poll_gap_ms = self
                .max_poll_gap_ms
                .max(now.saturating_duration_since(last).as_millis());
        }
        self.last_poll_at = Some(now);
    }

    fn note_read_at(&mut self, read_bytes: usize, now: Instant) -> TuicTcpStreamReadEvent {
        let first_rx_ms = if self.first_rx_ms.is_none() {
            let elapsed = now.saturating_duration_since(self.opened_at).as_millis();
            self.first_rx_ms = Some(elapsed);
            Some(elapsed)
        } else {
            None
        };
        let gap_ms = self
            .last_rx_at
            .map(|last| now.saturating_duration_since(last).as_millis());
        if let Some(gap) = gap_ms {
            self.max_read_gap_ms = self.max_read_gap_ms.max(gap);
        }
        self.last_rx_at = Some(now);
        self.reads += 1;
        self.rx_bytes += read_bytes as u64;

        TuicTcpStreamReadEvent {
            first_rx_ms,
            gap_ms,
            read_bytes,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
        }
    }

    fn note_pending_at(&mut self, now: Instant) -> Option<TuicTcpStreamPendingEvent> {
        self.pending_polls += 1;
        let gap_base = self.last_rx_at.unwrap_or(self.opened_at);
        let pending_gap_ms = now.saturating_duration_since(gap_base).as_millis();
        self.max_pending_gap_ms = self.max_pending_gap_ms.max(pending_gap_ms);

        if pending_gap_ms < TUIC_TCP_STREAM_PENDING_LOG_MS {
            return None;
        }
        let should_log = self
            .last_pending_log_at
            .map(|last| {
                now.saturating_duration_since(last).as_millis() >= TUIC_TCP_STREAM_PENDING_LOG_MS
            })
            .unwrap_or(true);
        if !should_log {
            return None;
        }
        self.last_pending_log_at = Some(now);
        Some(TuicTcpStreamPendingEvent {
            pending_gap_ms,
            pending_polls: self.pending_polls,
            polls: self.polls,
            max_poll_gap_ms: self.max_poll_gap_ms,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
        })
    }

    fn close_snapshot(&self) -> TuicTcpStreamCloseSnapshot {
        TuicTcpStreamCloseSnapshot {
            first_rx_ms: self.first_rx_ms.unwrap_or(0),
            max_read_gap_ms: self.max_read_gap_ms,
            rx_bytes: self.rx_bytes,
            reads: self.reads,
            pending_polls: self.pending_polls,
            max_pending_gap_ms: self.max_pending_gap_ms,
            polls: self.polls,
            max_poll_gap_ms: self.max_poll_gap_ms,
        }
    }
}

fn format_tuic_tcp_stream_first_rx_line(
    meta: &TuicTcpStreamDiagMeta,
    first_rx_ms: u128,
    read_bytes: usize,
    reads: u64,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-first-rx target={} conn={} id={} stream={} first_rx_ms={} read_bytes={} reads={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        first_rx_ms,
        read_bytes,
        reads
    )
}

fn format_tuic_tcp_stream_read_gap_line(
    meta: &TuicTcpStreamDiagMeta,
    gap_ms: u128,
    read_bytes: usize,
    reads: u64,
    rx_bytes: u64,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-read-gap target={} conn={} id={} stream={} gap_ms={} read_bytes={} reads={} rx_bytes={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        gap_ms,
        read_bytes,
        reads,
        rx_bytes
    )
}

fn format_tuic_tcp_stream_pending_line(
    meta: &TuicTcpStreamDiagMeta,
    pending_gap_ms: u128,
    pending_polls: u64,
    polls: u64,
    max_poll_gap_ms: u128,
    rx_bytes: u64,
    reads: u64,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-pending target={} conn={} id={} stream={} pending_gap_ms={} pending_polls={} polls={} max_poll_gap_ms={} rx_bytes={} reads={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        pending_gap_ms,
        pending_polls,
        polls,
        max_poll_gap_ms,
        rx_bytes,
        reads
    )
}

fn format_tuic_tcp_stream_close_line(
    meta: &TuicTcpStreamDiagMeta,
    snapshot: &TuicTcpStreamCloseSnapshot,
) -> String {
    format!(
        "🔎 tuic-tcp-stream-close target={} conn={} id={} stream={} first_rx_ms={} max_read_gap_ms={} rx_bytes={} reads={} pending_polls={} max_pending_gap_ms={} polls={} max_poll_gap_ms={}",
        meta.target,
        meta.conn_index,
        meta.stable_id,
        meta.stream_id,
        snapshot.first_rx_ms,
        snapshot.max_read_gap_ms,
        snapshot.rx_bytes,
        snapshot.reads,
        snapshot.pending_polls,
        snapshot.max_pending_gap_ms,
        snapshot.polls,
        snapshot.max_poll_gap_ms
    )
}

fn quic_stats_snapshot(conn: &Connection) -> QuicStatsSnapshot {
    let stats = conn.stats();
    QuicStatsSnapshot {
        rtt_ms: stats.path.rtt.as_millis(),
        cwnd: stats.path.cwnd,
        lost_packets: stats.path.lost_packets,
        sent_packets: stats.path.sent_packets,
        lost_bytes: stats.path.lost_bytes,
        congestion_events: stats.path.congestion_events,
        tx_data_blocked: stats.frame_tx.data_blocked,
        tx_stream_data_blocked: stats.frame_tx.stream_data_blocked,
        tx_streams_blocked_bidi: stats.frame_tx.streams_blocked_bidi,
        tx_streams_blocked_uni: stats.frame_tx.streams_blocked_uni,
        tx_max_data: stats.frame_tx.max_data,
        tx_max_stream_data: stats.frame_tx.max_stream_data,
        rx_data_blocked: stats.frame_rx.data_blocked,
        rx_stream_data_blocked: stats.frame_rx.stream_data_blocked,
        rx_max_data: stats.frame_rx.max_data,
        rx_max_stream_data: stats.frame_rx.max_stream_data,
        udp_tx_datagrams: stats.udp_tx.datagrams,
        udp_tx_bytes: stats.udp_tx.bytes,
        udp_rx_datagrams: stats.udp_rx.datagrams,
        udp_rx_bytes: stats.udp_rx.bytes,
        datagram_max: conn.max_datagram_size(),
        datagram_send_buffer_space: conn.datagram_send_buffer_space(),
    }
}

fn spawn_quic_stats_logger(
    conn: Connection,
    conn_index: usize,
    interval_secs: u64,
    mut stop_rx: watch::Receiver<bool>,
) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(interval_secs));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {
                    let stable_id = conn.stable_id();
                    if let Some(reason) = conn.close_reason() {
                        println!("📊 TUIC QUIC stats conn={conn_index} id={stable_id} closed={reason:?}");
                        break;
                    }
                    println!(
                        "{}",
                        format_quic_stats_line(conn_index, stable_id, quic_stats_snapshot(&conn))
                    );
                }
                changed = stop_rx.changed() => {
                    if changed.is_err() || *stop_rx.borrow() {
                        break;
                    }
                }
            }
        }
    });
}

/// 把任意可显示错误包成 ClientError（统一错误面）。
fn io_err<E: std::fmt::Display>(ctx: &str, e: E) -> ClientError {
    ClientError::from(std::io::Error::other(format!("{ctx}: {e}")))
}

/// 刀9（spec §2.3）：QUIC 重连握手超时。黑洞/不可达 server 的握手默认受 idle 约束可阻塞数十秒，
/// 5s 封顶让 failover 快路（连接死 + 重连失败）快速暴露；正常重连 1-RTT 远小于 5s，不会误伤高 RTT 链路。
const TUIC_RECONNECT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// 刀9（真出口 acceptance 修）：单条 TUIC TCP open（open_bi + write Connect）超时。黑洞连接上 write_all
/// 因 send 窗口满+无 ACK 会无限挂（连接尚未判死），5s 封顶让 failover 慢路收到「连接活但 open 失败」
/// 信号；正常 open 是本地操作远小于 5s，不误伤。**与 `TUIC_RECONNECT_TIMEOUT` 各自独立**（恰好都 5s，
/// 非耦合）；亦**不同于** spec §2.6 的「open_tcp 10s」——那指 **REALITY** open 的 H2 止血超时（reality_upstream.rs），
/// 此处是 TUIC open 的黑洞探测超时（acceptance 新加，spec 当时未有）。
const TUIC_OPEN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// TCP pool slot stale threshold. Some sing-box/TUIC paths can let otherwise healthy-looking
/// idle pool connections time out only when the next stream is opened. Reconnect idle TCP slots
/// before reuse; primary UDP/health connection keeps its existing behavior.
const TUIC_TCP_POOL_STALE_RECONNECT_SECS: u64 = 10;

/// TUIC 客户端上游：持有到 sing-box 的 QUIC 连接，每条 TCP 开一条 `Connect` 双向流。
/// 中文要点：连接断了按需重连+重认证(13a 最小实现;迁移/0-RTT 调优在 13c)。
pub struct TuicUpstream {
    endpoint: Endpoint,
    server: SocketAddr,
    sni: String,
    uuid: [u8; 16],
    password: String,
    /// 主连接是 index 0：UDP relay、heartbeat、health probe 都固定走它，避免 TCP pool 改变 UDP 语义。
    conns: Vec<Mutex<Connection>>,
    /// TCP `open_tcp` 的轮询游标。Relaxed 足够：只需要分散流，不承载同步语义。
    tcp_next: AtomicU64,
    /// Per-slot last TCP use, measured against `clock`. A stale TCP-only pool slot is reconnected
    /// before opening a new stream so the data relay does not inherit a just-timed-out connection.
    tcp_last_used_secs: Vec<AtomicU64>,
    /// Active or opening TCP relay count per pool slot. Stale reconnect is only safe when a slot is
    /// idle; closing an active QUIC connection would cut every stream multiplexed on that slot.
    tcp_active_streams: Vec<Arc<AtomicU64>>,
    /// 上行 UDP datagram 丢弃计数（连接不可用 / stream 兜底也失败）。可观测性，不影响 UDP 语义。
    udp_drops: AtomicU64,
    /// 上行走 uni-stream 兜底（超 datagram 上限）的次数。可观测性：判断 MTU 调优是否够、兜底是否热。
    udp_stream_fallbacks: AtomicU64,
    /// 上次 UDP 上行的秒数（`clock` 起点，0=从未）。`send_udp` 写、心跳读，决定是否按需保活。
    last_udp_activity: AtomicU64,
    /// 单调时钟：`send_udp`(写活跃时刻)与驱动任务(读 now)**同源**，避免双时钟漂移。
    clock: std::time::Instant,
    /// 重连是否尝试 0-RTT（来自 config；失败自愈回落 1-RTT）。
    zero_rtt: bool,
    /// QUIC 连接级 stats 日志周期；`None` 表示关闭。
    quic_stats_secs: Option<u64>,
    /// QUIC stats task 停止信号。`Drop` 时触发，避免诊断 task 持有 `Connection` clone 延长连接生命周期。
    quic_stats_stop: Option<watch::Sender<bool>>,
    /// UDP relay mode（刀3.5；来自 config）：`Quic` → 所有上行包走 uni-stream（首包即触发 server
    /// 下行镜像 stream，摆脱 datagram 天花板）；`Native` → datagram 主 + 超限 stream 兜底（刀3 行为）。
    udp_relay_mode: UdpRelayMode,
    /// 刀11：进程级数据面可观测性句柄（与 run_event_loop 共享同一 `Arc<Metrics>`）。`start_udp` spawn 的
    /// 下行/统计 task 经 `me.metrics` 写**下行** drop（`udp_drops_down`）+ datagram 背压上升沿
    /// （`datagram_pressure_events`）。上行 `udp_drops`/`udp_stream_fallbacks` 仍是上方既有字段（零回归）。
    metrics: Arc<Metrics>,
}

impl TuicUpstream {
    /// 建连 + 发 Authenticate（token 经 keying-material 导出，字节级对齐 sing-box）。
    pub async fn connect(
        cfg: &TuicClientConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ClientError> {
        // 刀3.5：从 config 解析 CC（未知值回落 Cubic + 告警，失败自愈不致命）。
        let (cc, fell_back) = quic::parse_cc(&cfg.congestion_control);
        if fell_back {
            println!(
                "⚠️ TUIC 未知 congestion_control={:?}，回落 Cubic（quinn 默认）",
                cfg.congestion_control
            );
        }
        // 刀3.5：解析 UDP relay mode（未知值回落 Native + 告警，与 CC 对称）。
        let udp_relay_mode = UdpRelayMode::parse(&cfg.udp_relay_mode).unwrap_or_else(|| {
            println!(
                "⚠️ TUIC 未知 udp_relay_mode={:?}，回落 native（datagram 主 + 超限 stream 兜底）",
                cfg.udp_relay_mode
            );
            UdpRelayMode::Native
        });
        let qcfg =
            quic::client_quic_config_alpn(&cfg.ca_path, vec![cfg.alpn.as_bytes().to_vec()], cc)
                .map_err(ClientError::InvalidTarget)?;
        let endpoint = quic::client_endpoint(qcfg).map_err(ClientError::InvalidTarget)?;
        let conn = Self::handshake(
            &endpoint,
            cfg.server,
            &cfg.sni,
            &cfg.uuid,
            &cfg.password,
            cfg.zero_rtt,
        )
        .await?;
        // 刀3：记录初始 datagram 发送上限（acceptance 据此读真实 datagram 天花板；PLPMTUD 探完会更大）。
        // 超此上限的上行包走 uni-stream 兜底；下行大包由 server 分片、本端重组。
        println!(
            "📏 TUIC datagram 初始上限 = {:?} 字节（超此走 uni-stream 兜底；PLPMTUD 将继续上探）",
            conn.max_datagram_size()
        );
        // 刀3.5：打实际生效的 CC + relay mode，供 acceptance 确认 BBR/quic 真装上（A/B 归因）。
        println!("🧭 TUIC 拥塞控制器={cc:?} | UDP relay mode={udp_relay_mode:?}");
        println!(
            "🪟 QUIC flow windows: bidi={} uni={} stream_rx={}B conn_rx={}B send={}B",
            quic::QUIC_MAX_CONCURRENT_BIDI_STREAMS,
            quic::QUIC_MAX_CONCURRENT_UNI_STREAMS,
            quic::QUIC_STREAM_RECEIVE_WINDOW_BYTES,
            quic::QUIC_RECEIVE_WINDOW_BYTES,
            quic::QUIC_SEND_WINDOW_BYTES
        );
        let tcp_pool = cfg.tcp_pool.clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL);
        let mut conns = Vec::with_capacity(tcp_pool);
        let quic_stats_stop = cfg.quic_stats_secs.map(|_| watch::channel(false).0);
        if let Some(secs) = cfg.quic_stats_secs {
            println!("🔬 TUIC QUIC stats 已启用：每 {secs}s 打印连接级 flow/congestion 指标");
            if let Some(stop) = &quic_stats_stop {
                spawn_quic_stats_logger(conn.clone(), 0, secs, stop.subscribe());
            }
        }
        conns.push(Mutex::new(conn));
        for index in 1..tcp_pool {
            let extra = Self::handshake(
                &endpoint,
                cfg.server,
                &cfg.sni,
                &cfg.uuid,
                &cfg.password,
                cfg.zero_rtt,
            )
            .await?;
            if let Some(secs) = cfg.quic_stats_secs
                && let Some(stop) = &quic_stats_stop
            {
                spawn_quic_stats_logger(extra.clone(), index, secs, stop.subscribe());
            }
            conns.push(Mutex::new(extra));
        }
        if tcp_pool > 1 {
            println!(
                "🧵 TUIC TCP connection pool={tcp_pool}（UDP/health 仍走 primary connection）"
            );
        }
        let tcp_last_used_secs = (0..tcp_pool).map(|_| AtomicU64::new(0)).collect();
        let tcp_active_streams = (0..tcp_pool).map(|_| Arc::new(AtomicU64::new(0))).collect();
        Ok(Self {
            endpoint,
            server: cfg.server,
            sni: cfg.sni.clone(),
            uuid: cfg.uuid,
            password: cfg.password.clone(),
            conns,
            tcp_next: AtomicU64::new(0),
            tcp_last_used_secs,
            tcp_active_streams,
            udp_drops: AtomicU64::new(0),
            udp_stream_fallbacks: AtomicU64::new(0),
            last_udp_activity: AtomicU64::new(0),
            clock: std::time::Instant::now(),
            zero_rtt: cfg.zero_rtt,
            quic_stats_secs: cfg.quic_stats_secs,
            quic_stats_stop,
            udp_relay_mode,
            metrics,
        })
    }

    /// 当前 UDP relay mode（可观测/测试）。
    pub fn udp_relay_mode(&self) -> UdpRelayMode {
        self.udp_relay_mode
    }

    /// 建连 + 认证。`zero_rtt` 时先试 0-RTT（early data）；任何原因失败（无 ticket / 服务端不支持 /
    /// early-exporter 认证不齐）都**自愈回落 1-RTT**，绝不卡死重连循环。
    /// 中文要点：首连必无 ticket → `into_0rtt` 必失败 → 走 1-RTT（与 13a 行为一致，零回归）。
    async fn handshake(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
        zero_rtt: bool,
    ) -> Result<Connection, ClientError> {
        if zero_rtt && let Some(conn) = Self::try_0rtt(endpoint, server, sni, uuid, password).await
        {
            println!("⚡ TUIC 0-RTT 重连成功（early data）");
            return Ok(conn);
        }
        // 关了 0-RTT，或 0-RTT 不可用（无 ticket/不支持/认证不齐）→ 回落 1-RTT。
        Self::handshake_1rtt(endpoint, server, sni, uuid, password).await
    }

    /// 尝试 0-RTT 握手 + early-data 认证；不可用返回 None（交由调用方回落 1-RTT）。
    async fn try_0rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Option<Connection> {
        let connecting = endpoint.connect(server, sni).ok()?;
        // into_0rtt 的 Err 返回原 Connecting（非 Error 类型），无 ticket/不支持时走这里。
        let (conn, _accepted) = match connecting.into_0rtt() {
            Ok(pair) => pair,
            Err(_connecting) => return None,
        };
        // early-exporter 认证：若与 sing-box 不齐则失败 → 记一行(便于 e2e 诊断)并 None 回落（不卡死）。
        if let Err(e) = Self::authenticate(&conn, uuid, password).await {
            println!(
                "⚠️ TUIC 0-RTT 认证失败(可能 early-exporter 与 sing-box 不齐)，回落 1-RTT: {e:?}"
            );
            return None;
        }
        Some(conn)
    }

    /// 普通 1-RTT 握手 + 认证（13a 行为）。
    async fn handshake_1rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<Connection, ClientError> {
        let conn = endpoint
            .connect(server, sni)
            .map_err(|e| io_err("tuic connect", e))?
            .await
            .map_err(|e| io_err("tuic handshake", e))?;
        Self::authenticate(&conn, uuid, password).await?;
        Ok(conn)
    }

    /// 在已建立(0-RTT 或 1-RTT)的连接上发 TUIC Authenticate（单向流）。
    /// token = export_keying_material(out=32, label=UUID(16), context=password) —— 字节级对齐 sing-box。
    async fn authenticate(
        conn: &Connection,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<(), ClientError> {
        let mut token = [0u8; 32];
        conn.export_keying_material(&mut token, uuid, password.as_bytes())
            .map_err(|_| ClientError::InvalidTarget("tuic keying-material export failed".into()))?;
        let mut uni = conn
            .open_uni()
            .await
            .map_err(|e| io_err("tuic open_uni", e))?;
        uni.write_all(&encode_authenticate(uuid, &token))
            .await
            .map_err(|e| io_err("tuic auth write", e))?;
        uni.finish()
            .await
            .map_err(|e| io_err("tuic auth finish", e))?;
        Ok(())
    }

    /// 取当前主连接的克隆；若已关闭则就地重连+重认证（13a 逻辑，UDP/health 共用）。
    /// 中文要点：主连接(index 0)是 UDP/health 的单一事实源；TCP pool 通过 `live_tcp_conn` 分散到其它连接。
    /// 刀9（spec §2.3）：重连握手封 **5s 超时**——黑洞/不可达 server 的 QUIC 握手默认受 idle(15s, quic.rs) 约束、
    /// 可阻塞数十秒；5s 封顶让「连接死 + 重连失败」这个 failover 快路强信号**快速暴露**（is_dead 仍为
    /// true，调用方 record_tuic_failure(dead) 即切 REALITY），同时也避免纯 TUIC open 任务久挂。
    async fn live_conn(&self) -> Result<Connection, ClientError> {
        self.live_conn_at(0).await
    }

    /// 取一条 TCP pool 连接的克隆；连接自身已关闭则只重连该槽位。
    async fn live_conn_at(&self, index: usize) -> Result<Connection, ClientError> {
        self.live_conn_at_with_reason(index, None).await
    }

    async fn live_conn_at_with_reason(
        &self,
        index: usize,
        reconnect_reason: Option<&'static str>,
    ) -> Result<Connection, ClientError> {
        let slot = self
            .conns
            .get(index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?;
        let mut guard = slot.lock().await;
        let closed = guard.close_reason().is_some();
        if closed || reconnect_reason.is_some() {
            if let Some(reason) = reconnect_reason {
                println!("🔁 tuic-tcp-pool-reconnect conn={index} reason={reason}");
                guard.close(VarInt::from_u32(0), reason.as_bytes());
            }
            let hs = Self::handshake(
                &self.endpoint,
                self.server,
                &self.sni,
                &self.uuid,
                &self.password,
                self.zero_rtt,
            );
            *guard = tokio::time::timeout(TUIC_RECONNECT_TIMEOUT, hs)
                .await
                .map_err(|_| {
                    io_err(
                        "tuic reconnect",
                        "5s 超时（黑洞/不可达；failover 据 is_dead 切备腿）",
                    )
                })??;
            if let Some(secs) = self.quic_stats_secs
                && let Some(stop) = &self.quic_stats_stop
            {
                spawn_quic_stats_logger(guard.clone(), index, secs, stop.subscribe());
            }
        }
        Ok(guard.clone())
    }

    /// TCP 专用连接选择：默认 pool=1 时等价旧行为；pool>1 时 round-robin 分散新流。
    async fn live_tcp_conn(&self) -> Result<(usize, Connection, TcpPoolSlotLease), ClientError> {
        let cursor = self.tcp_next.fetch_add(1, Ordering::Relaxed);
        let index = tcp_pool_index(self.conns.len(), cursor);
        let now_secs = self.clock.elapsed().as_secs();
        let last_used = self
            .tcp_last_used_secs
            .get(index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?
            .load(Ordering::Relaxed);
        let active = self
            .tcp_active_streams
            .get(index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?
            .clone();
        let (lease, idle_exclusive) = TcpPoolSlotLease::reserve(active);
        let reconnect_reason =
            tcp_pool_stale_reconnect_reason(index, now_secs, last_used, idle_exclusive);
        let conn = self
            .live_conn_at_with_reason(index, reconnect_reason)
            .await?;
        if let Some(last) = self.tcp_last_used_secs.get(index) {
            last.store(now_secs, Ordering::Relaxed);
        }
        Ok((index, conn, lease))
    }

    /// 取当前活连接克隆——**非阻塞、不重连**（刀9，ADR-0011 §3b）。锁被占（后台 start_udp 正在重连，持锁
    /// 数秒）→ `None`，**绝不 await 锁**（否则在主循环 inline 的 send_udp 会被 stall）。连接已死 → `None`。
    /// 重连交给后台 `start_udp` 自愈循环（背景退避重连），与主循环解耦。
    fn current_conn(&self) -> Option<Connection> {
        let guard = self.conns.first()?.try_lock().ok()?;
        if guard.close_reason().is_some() {
            None
        } else {
            Some(guard.clone())
        }
    }

    /// 发一条**已编码**的 TUIC Packet（刀3：datagram 主路径 + uni-stream 兜底）。
    /// 中文要点：先按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native datagram，
    /// 超上限/不可用走 **per-packet uni-stream 兜底**（持续大流量直播不丢包）。datagram 真发遇
    /// `TooLarge`（MTU 竞态收缩）→ 二次 stream 兜底。仅**真失败**才丢弃计数（UDP 语义，不阻塞调用方除重连）。
    pub async fn send_udp(&self, datagram: Vec<u8>) {
        // 记录 UDP 活跃时刻：驱动任务据此「仅活跃时发」Heartbeat（纯 TCP 不发，省流量/电量）。
        self.last_udp_activity
            .store(self.clock.elapsed().as_secs(), Ordering::Relaxed);
        // 刀9（真出口 acceptance 修，ADR-0011 §3b）：**绝不在此 inline 重连**——send_udp 在主循环 inline
        // await（handle_tuic_udp_uplink），黑洞期每个 UDP 包触发 5s 重连会 stall 整个事件循环、饿死 DNS 劫持
        // 等所有主循环分支。改用 `current_conn`（try_lock 非阻塞 + 不重连）：连接活就发；锁被占（后台 start_udp
        // 正在重连）或已死 → 快速静默丢（热路径勿刷屏，udp_drops 计数即观测）。重连由后台 start_udp 自愈循环负责。
        let conn = match self.current_conn() {
            Some(c) => c,
            None => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };
        // Bytes：datagram 路径下 `clone()` 为 O(1)（Arc 引用计数），TooLarge 竞态二次兜底不深拷贝。
        let bytes = bytes::Bytes::from(datagram);
        // 按 config 的 relay mode 分流：Quic→恒 uni-stream（首包触发下行镜像 stream）；Native→size-based。
        match udp_send_plan(self.udp_relay_mode, conn.max_datagram_size(), bytes.len()) {
            UdpSend::Datagram => match conn.send_datagram(bytes.clone()) {
                Ok(()) => {}
                Err(quinn::SendDatagramError::TooLarge) => {
                    // 分流时还装得下、真发时 MTU 已收缩 → 二次 stream **兜底**（计 fallback），不丢。
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                    self.send_udp_via_stream(&conn, bytes).await;
                }
                Err(e) => {
                    self.udp_drops.fetch_add(1, Ordering::Relaxed);
                    println!("⚠️ TUIC UDP↑ 发送失败（连接将自愈），丢弃: {e:?}");
                }
            },
            UdpSend::Stream => {
                // Native 走到这里=超 datagram 上限的**兜底**（计数，acceptance 判 MTU 调优是否够）；
                // Quic 模式 stream 是**主发路径**（非兜底，不计 fallback，否则 `📊 stream 兜底` 会被
                // 误读为 MTU 失败——量级 = 全部包）。主发量看 path.sent_packets。
                if self.udp_relay_mode == UdpRelayMode::Native {
                    self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
                }
                self.send_udp_via_stream(&conn, bytes).await;
            }
        }
    }

    /// 在 uni-stream 上发一条完整 TUIC Packet：开单向流 → 写整条已编码 Packet → finish。
    /// 中文要点：TUIC quic-relay-mode 即「一条 uni-stream 承载一个完整 Packet 命令」（`FRAG_TOTAL=1`），
    /// 字节与 datagram 模式**完全一致**（复用同一 `encode_packet` 产物）。任一步失败→丢弃计数（UDP 自愈重发）。
    /// fallback 计数由调用方负责（仅 Native 兜底场景计），本函数只管 I/O + 真失败丢弃。
    async fn send_udp_via_stream(&self, conn: &Connection, datagram: bytes::Bytes) {
        if let Err(e) = Self::write_uni_packet(conn, &datagram).await {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
            println!("⚠️ TUIC UDP↑ uni-stream 发送失败（连接将自愈），丢弃: {e:?}");
        }
    }

    /// 在连接上开 uni-stream 写一个 Packet 并 finish（抽出便于错误归一）。
    async fn write_uni_packet(conn: &Connection, packet: &[u8]) -> Result<(), ClientError> {
        let mut uni = conn
            .open_uni()
            .await
            .map_err(|e| io_err("udp open_uni", e))?;
        uni.write_all(packet)
            .await
            .map_err(|e| io_err("udp uni write", e))?;
        uni.finish()
            .await
            .map_err(|e| io_err("udp uni finish", e))?;
        Ok(())
    }

    /// 累计丢弃的上行 UDP datagram 数（可观测性）。
    pub fn udp_drop_count(&self) -> u64 {
        self.udp_drops.load(Ordering::Relaxed)
    }

    /// 累计走 uni-stream 兜底的上行包数（可观测性：MTU 调优是否足够 / 兜底是否过热）。
    pub fn udp_stream_fallback_count(&self) -> u64 {
        self.udp_stream_fallbacks.load(Ordering::Relaxed)
    }

    /// 启动 UDP 驱动后台任务，返回**下行 datagram 接收端**给主循环 select。
    /// 任务职责：① 下行泵——`read_datagram`（native）+ `accept_uni`（quic-relay-mode / 大包）→ channel
    /// （主循环 `decode_packet_meta` + 重组后注入 TUN）；② 周期 Heartbeat 维持连接。连接断开则确定性退避后
    /// 经 `live_conn` 重连，泵与心跳自然恢复（等价于"重连后重启泵/心跳"）。
    /// 中文要点：单自愈循环，避免多任务各自重连产生竞态；下行用 `send().await` 施加背压、不丢 DNS 响应。
    /// uni-stream 读取有界派生（`Semaphore`），超并发上限丢弃该 stream（UDP 自愈），防 flood 无界 spawn。
    pub fn start_udp(self: &Arc<Self>) -> mpsc::Receiver<Vec<u8>> {
        let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(TUIC_DOWNLINK_CAPACITY);
        let me = Arc::clone(self);
        // 跨重连共享：限制下行 uni-stream 的并发读取数。
        let stream_sem = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_DOWNLINK_STREAMS));
        tokio::spawn(async move {
            let mut attempt: u32 = 0;
            loop {
                let conn = match me.live_conn().await {
                    Ok(c) => {
                        attempt = 0;
                        println!("🌊 TUIC UDP 驱动已就绪（datagram 泵 + heartbeat）");
                        c
                    }
                    Err(e) => {
                        println!("TUIC UDP 驱动重连失败: {e:?}");
                        tokio::time::sleep(udp_reconnect_backoff(attempt)).await;
                        attempt = attempt.saturating_add(1);
                        continue;
                    }
                };
                let mut hb = tokio::time::interval(Duration::from_secs(TUIC_HEARTBEAT_SECS));
                hb.tick().await; // 第一拍立即返回，跳过（避免连上瞬间立刻发心跳）。
                let mut stats = tokio::time::interval(Duration::from_secs(UDP_STATS_LOG_SECS));
                stats.tick().await; // 跳过第一拍（连上瞬间不打统计）。
                // 刀11：datagram 背压上升沿 latch（单 task 局部、无需原子；每重连复位，新连接从未背压起算）。
                let mut prev_pressured = false;
                loop {
                    tokio::select! {
                        dg = conn.read_datagram() => {
                            match dg {
                                Ok(bytes) => {
                                    if downlink_tx.send(bytes.to_vec()).await.is_err() {
                                        return; // 主循环已退出 → 结束任务
                                    }
                                }
                                Err(_) => break, // 连接断 → 外层 live_conn 重连
                            }
                        }
                        // quic-relay-mode / 超 datagram 上限的下行包：经 uni-stream 来。读满整条 Packet
                        // → 同一下行 channel（与 datagram 路径同构，主循环统一 decode + 重组）。
                        stream = conn.accept_uni() => {
                            match stream {
                                Ok(recv) => {
                                    // 有界派生：拿到 permit 才读；拿不到（达并发上限）→ 丢弃该 stream（UDP 自愈）。
                                    if let Ok(permit) = Arc::clone(&stream_sem).try_acquire_owned() {
                                        let tx = downlink_tx.clone();
                                        let m = Arc::clone(&me.metrics);
                                        tokio::spawn(async move {
                                            let _permit = permit; // 持有到读完，限并发
                                            match read_uni_packet(recv).await {
                                                Some(pkt) => {
                                                    let _ = tx.send(pkt).await;
                                                }
                                                // 刀11：下行 uni-stream 解码/读失败（超长/reset/空）→ 静默丢，计下行 drop。
                                                None => m.inc_udp_drops_down(),
                                            }
                                        });
                                    } else {
                                        // 刀11：下行并发达上限（信号量耗尽）→ 丢弃该 stream（UDP 自愈），计下行 drop。
                                        me.metrics.inc_udp_drops_down();
                                    }
                                }
                                Err(_) => break, // 连接断 → 外层 live_conn 重连
                            }
                        }
                        _ = stats.tick() => {
                            // 周期性观测（刀3.5）：UDP 最近活跃 或 有兜底/丢弃 才打（空闲静默，不刷屏）。
                            // 中文要点：native datagram acceptance 下 fb/drops 常为 0，但仍需看 cwnd/rtt/丢包，
                            // 故以「最近 UDP 活跃」为主闸门——量化 datagram 天花板成因（CC vs 无背压）。
                            let fb = me.udp_stream_fallbacks.load(Ordering::Relaxed);
                            let drops = me.udp_drops.load(Ordering::Relaxed);
                            let now = me.clock.elapsed().as_secs();
                            let last = me.last_udp_activity.load(Ordering::Relaxed);
                            let udp_active = should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS);
                            let max_dg = conn.max_datagram_size();
                            let space = conn.datagram_send_buffer_space();
                            // datagram 背压代理信号：剩余 < 1 个 datagram 上限 → 即将丢最老（静默丢盲点）。
                            // 仅 Native 模式有意义（Quic 模式 relay 不走 datagram，背压无关，避免假警报）。
                            let pressured = me.udp_relay_mode == UdpRelayMode::Native
                                && is_datagram_pressured(
                                    space,
                                    max_dg.unwrap_or(quic::QUIC_INITIAL_MTU as usize),
                                );
                            // 刀11：**每 tick 都更新 latch**（独立于打印门控，否则空闲期 latch 不复位）。
                            // 只在 false→true 上升沿计一次「集」，避免一段持续背压每 tick 重复计数。
                            if note_pressure_edge(pressured, &mut prev_pressured) {
                                me.metrics.inc_datagram_pressure_events();
                            }
                            if udp_active || fb > 0 || drops > 0 {
                                let path = conn.stats().path;
                                let line = format_udp_stats(
                                    max_dg, fb, drops,
                                    path.rtt.as_millis(), path.cwnd,
                                    path.lost_packets, path.sent_packets, space,
                                );
                                if pressured {
                                    println!("{line} ⚠️背压(datagram 缓冲将丢最老)");
                                } else {
                                    println!("{line}");
                                }
                            }
                        }
                        _ = hb.tick() => {
                            // 仅在「最近有 UDP 上行」时发心跳；纯 TCP 会话由 QUIC keep-alive 保活，不发。
                            let now = me.clock.elapsed().as_secs();
                            let last = me.last_udp_activity.load(Ordering::Relaxed);
                            if should_send_heartbeat(last, now, TUIC_HB_IDLE_WINDOW_SECS)
                                && conn.send_datagram(encode_heartbeat().into()).is_err()
                            {
                                break; // 连接断 → 外层 live_conn 重连
                            }
                        }
                    }
                }
                println!("🔌 TUIC UDP 驱动连接断开，准备重连");
                tokio::time::sleep(udp_reconnect_backoff(attempt)).await;
                attempt = attempt.saturating_add(1);
            }
        });
        downlink_rx
    }
}

/// 读满一条下行 uni-stream（一个完整 TUIC Packet 命令），上限 `MAX_TUIC_PACKET_BYTES`。
/// 中文要点：流过大/被 reset/读错 → None（丢弃该包，UDP 自愈）。空流（finish 无数据）→ None。
async fn read_uni_packet(mut recv: quinn::RecvStream) -> Option<Vec<u8>> {
    match recv.read_to_end(MAX_TUIC_PACKET_BYTES).await {
        Ok(buf) if !buf.is_empty() => Some(buf),
        _ => None,
    }
}

/// 是否该发 TUIC Heartbeat：仅当**最近有 UDP 上行活动**(距上次 ≤ 活跃窗口)。
/// `last_activity=0` 表示从未发过 UDP → 不发(纯 TCP 会话靠 QUIC keep-alive 保活)。
/// 中文要点：纯函数,`now`/`last` 同源单调秒数,便于单测;边界 `now-last==window` 取发(`<=`)。
fn should_send_heartbeat(last_activity: u64, now: u64, idle_window: u64) -> bool {
    last_activity != 0 && now.saturating_sub(last_activity) <= idle_window
}

/// UDP 驱动重连退避：确定性指数退避（`base * 2^attempt`，封顶 `CAP`）。
/// 中文要点：UDP 无连接状态、重连后下个 datagram 即自愈，对重连节奏不敏感，故不引入 jitter。
fn udp_reconnect_backoff(attempt: u32) -> Duration {
    let exp = UDP_RECONNECT_BASE_MS.saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX));
    Duration::from_millis(exp.min(UDP_RECONNECT_CAP_MS))
}

#[async_trait::async_trait]
impl ProxyUpstream for TuicUpstream {
    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        // 取 TCP pool 活连接，断了只重连该槽位；pool=1 时等价旧行为。
        let (conn_index, conn, lease) = self.live_tcp_conn().await?;
        // 刀9（真出口 acceptance 修）：open_bi + write Connect 在**黑洞连接**上会 hang——连接尚未被
        // 判死（close_reason 仍 None，因 keepalive/非对称封锁架空 idle 检测），但 QUIC send 窗口满、
        // 收不到 ACK → write_all 无限阻塞，failover 快/慢路都收不到信号。封 5s 超时让黑洞 open **快速失败**
        // → FailoverUpstream 据「连接活但 open 超时」走 **慢路计数**（并发 open 下 ~5s 累计 3 次即切 REALITY，
        // 不再死等 close_reason）。正常 open（open_bi + 写小 Connect 头）是本地操作、远小于 5s，不误伤。
        let open = async {
            let (mut send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| io_err("tuic open_bi", e))?;
            let stable_id = conn.stable_id();
            let stream_id = recv.id().0;
            send.write_all(&encode_connect(target))
                .await
                .map_err(|e| io_err("tuic connect write", e))?;
            let tcp_stream_diag = if tcp_diag_enabled() {
                println!(
                    "{}",
                    format_tuic_tcp_open_line(target, conn_index, stable_id)
                );
                Some(TuicTcpStreamDiag::new(
                    TuicTcpStreamDiagMeta::new(target, conn_index, stable_id, stream_id),
                    Instant::now(),
                ))
            } else {
                None
            };
            // 把双向流的收/发两半合成一条 AsyncRead+AsyncWrite，喂给现有双向泵。
            Ok::<RelayStream, ClientError>(Box::new(TrackedRelayStream::new(
                tokio::io::join(recv, send),
                lease,
                tcp_stream_diag,
            )))
        };
        tokio::time::timeout(TUIC_OPEN_TIMEOUT, open)
            .await
            .map_err(|_| {
                io_err(
                    "tuic open_tcp",
                    "5s 超时（黑洞/send 窗口满无 ACK；failover 慢路据此累计切备腿）",
                )
            })?
    }

    /// 刀14d：TUIC `open_tcp` 通常很快，但它仍可能 await QUIC reconnect、`open_bi` 或 Connect write
    /// flow-control up to `TUIC_OPEN_TIMEOUT`. 在单任务主循环里 inline await 会让一条慢 open 暂停
    /// 下行 flush、timer、DNS 和 reap；所以走已有 async-open 状态机。
    fn open_is_cheap(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for TuicUpstream {
    // 委托既有 inherent `send_udp`（inherent 优先解析，不会递归）。
    async fn send_udp(&self, datagram: Vec<u8>) {
        TuicUpstream::send_udp(self, datagram).await
    }

    // 刀11：上行计数仍住 TuicUpstream（零回归）→ 经既有 inherent 访问器暴露给 snapshot。
    fn udp_drops_up(&self) -> u64 {
        self.udp_drop_count()
    }
    fn udp_stream_fallbacks(&self) -> u64 {
        self.udp_stream_fallback_count()
    }
}

impl Drop for TuicUpstream {
    fn drop(&mut self) {
        if let Some(stop) = &self.quic_stats_stop {
            let _ = stop.send(true);
        }
    }
}

/// 刀9 F1：failover 健康探测面。`probe` 主动探活（live_conn=QUIC 握手+TUIC 认证，非浅探）；
/// `is_dead` 区分 down 快路（黑洞，连接被 idle/keepalive 打死）/慢路（流失败）。
#[async_trait::async_trait]
impl crate::failover::HealthProbe for TuicUpstream {
    async fn probe(&self) -> bool {
        self.live_conn().await.is_ok()
    }
    async fn is_dead(&self) -> bool {
        self.conns
            .first()
            .expect("TuicUpstream always has a primary connection")
            .lock()
            .await
            .close_reason()
            .is_some()
    }
    /// 累计收到的 UDP datagram 数（quinn `stats().udp_rx.datagrams`）——黑洞存活信标：健康连接每 ~5s
    /// 有 keepalive ACK 进来→单调增；黑洞连 ACK 都收不到→停滞。down 探测据此判黑洞（不被乐观开流/keepalive 架空）。
    /// **非阻塞**（try_lock，同 current_conn）：锁被占（后台 start_udp 正在重连，持锁≤5s）→ `None`，**绝不 await
    /// 锁**——否则健康探针任务被卡 5s、把锁等待当成网络停滞、检测计时偏斜（review finding）。调用方对 None
    /// 跳过本次观察：连接正在重连本就不健康，停滞计时靠 `now` 累积、不重置，下次能读到（仍停滞）即判黑洞。
    async fn rx_datagrams(&self) -> Option<u64> {
        Some(
            self.conns
                .first()?
                .try_lock()
                .ok()?
                .stats()
                .udp_rx
                .datagrams,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::TargetAddr;

    #[test]
    fn address_ipv4() {
        let a = encode_address(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(a, vec![0x01, 1, 2, 3, 4, 0x01, 0xBB]); // ATYP=IPv4, port 443
    }

    #[test]
    fn address_domain() {
        let a = encode_address(&TargetAddr::DomainPort {
            host: "ab.com".into(),
            port: 443,
        });
        assert_eq!(
            a,
            vec![0x00, 6, b'a', b'b', b'.', b'c', b'o', b'm', 0x01, 0xBB]
        );
    }

    #[test]
    fn address_ipv6() {
        let a = encode_address(&TargetAddr::IpPort("[::1]:53".parse().unwrap()));
        assert_eq!(a[0], 0x02);
        assert_eq!(a.len(), 1 + 16 + 2);
        assert_eq!(&a[17..19], &[0x00, 0x35]); // port 53
    }

    #[test]
    fn authenticate_layout() {
        let uuid = [0xABu8; 16];
        let token = [0xCDu8; 32];
        let c = encode_authenticate(&uuid, &token);
        assert_eq!(c.len(), 2 + 16 + 32);
        assert_eq!(&c[..2], &[0x05, 0x00]);
        assert_eq!(&c[2..18], &uuid);
        assert_eq!(&c[18..50], &token);
    }

    #[test]
    fn connect_prefixes_header() {
        let c = encode_connect(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
        assert_eq!(&c[..2], &[0x05, 0x01]);
        assert_eq!(&c[2..], &[0x01, 1, 2, 3, 4, 0x01, 0xBB]);
    }

    #[test]
    fn domain_over_255_truncated_safely() {
        let host = "a".repeat(300);
        let a = encode_address(&TargetAddr::DomainPort { host, port: 80 });
        assert_eq!(a[0], 0x00);
        assert_eq!(a[1], 255); // length byte capped
        assert_eq!(a.len(), 1 + 1 + 255 + 2);
    }

    const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

    #[test]
    fn config_valid_with_defaults() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .expect("valid");
        assert_eq!(c.server, "1.2.3.4:8443".parse().unwrap());
        assert_eq!(c.uuid[0], 0x55);
        assert_eq!(c.alpn, "h3"); // default
        assert_eq!(c.congestion_control, "cubic"); // 刀3.5 实测裁决：datagram 路径 Cubic 优于 BBR
        assert_eq!(c.udp_relay_mode, "native");
        assert_eq!(c.tcp_pool, 2);
        assert_eq!(c.quic_stats_secs, None);
        assert!(
            !c.zero_rtt,
            "0-RTT 默认关（quinn 0.10 在 0-RTT 不支持 keying-material 导出）"
        );
    }

    #[test]
    fn env_overrides_cc_and_udp_mode() {
        // 有值覆盖默认（A/B 切换：MINI_VPN_TUIC_CC=cubic / MINI_VPN_TUIC_UDP_MODE=quic）。
        assert_eq!(override_field("bbr".into(), Some("cubic".into())), "cubic");
        assert_eq!(override_field("native".into(), Some("quic".into())), "quic");
        // 无值 / 空白 → 保留默认。
        assert_eq!(override_field("bbr".into(), None), "bbr");
        assert_eq!(
            override_field("native".into(), Some("   ".into())),
            "native"
        );
    }

    #[test]
    fn tcp_pool_parse_defaults_and_clamps() {
        assert_eq!(parse_tcp_pool(None), 2);
        assert_eq!(parse_tcp_pool(Some("")), 2);
        assert_eq!(parse_tcp_pool(Some("0")), 1);
        assert_eq!(parse_tcp_pool(Some("nope")), 2);
        assert_eq!(parse_tcp_pool(Some("1")), 1);
        assert_eq!(parse_tcp_pool(Some("4")), 4);
        assert_eq!(parse_tcp_pool(Some("999")), MAX_TUIC_TCP_POOL);
    }

    #[test]
    fn quic_stats_secs_follows_tcp_diag_or_explicit_override() {
        assert_eq!(parse_quic_stats_secs(None, None, Some("5")), None);
        assert_eq!(parse_quic_stats_secs(None, Some("1"), Some("5")), Some(5));
        assert_eq!(
            parse_quic_stats_secs(None, Some("true"), Some("nope")),
            Some(DEFAULT_TUIC_QUIC_STATS_SECS)
        );
        assert_eq!(
            parse_quic_stats_secs(Some("7"), Some("0"), Some("5")),
            Some(7)
        );
        assert_eq!(parse_quic_stats_secs(Some("0"), Some("1"), Some("5")), None);
        assert_eq!(
            parse_quic_stats_secs(Some("nope"), Some("1"), Some("5")),
            None
        );
    }

    #[test]
    fn format_quic_stats_line_includes_flow_and_congestion_signals() {
        let line = format_quic_stats_line(
            2,
            99,
            QuicStatsSnapshot {
                rtt_ms: 181,
                cwnd: 65_535,
                lost_packets: 3,
                sent_packets: 100,
                lost_bytes: 4096,
                congestion_events: 2,
                tx_data_blocked: 4,
                tx_stream_data_blocked: 5,
                tx_streams_blocked_bidi: 6,
                tx_streams_blocked_uni: 7,
                tx_max_data: 8,
                tx_max_stream_data: 9,
                rx_data_blocked: 10,
                rx_stream_data_blocked: 11,
                rx_max_data: 12,
                rx_max_stream_data: 13,
                udp_tx_datagrams: 14,
                udp_tx_bytes: 15,
                udp_rx_datagrams: 16,
                udp_rx_bytes: 17,
                datagram_max: Some(1375),
                datagram_send_buffer_space: 18,
            },
        );
        assert!(line.contains("conn=2"), "{line}");
        assert!(line.contains("id=99"), "{line}");
        assert!(line.contains("rtt=181ms"), "{line}");
        assert!(line.contains("cwnd=65535"), "{line}");
        assert!(line.contains("lost=3/100"), "{line}");
        assert!(line.contains("congestion_events=2"), "{line}");
        assert!(line.contains("tx_blocked(data=4,stream=5"), "{line}");
        assert!(line.contains("rx_blocked(data=10,stream=11"), "{line}");
        assert!(
            line.contains("rx_window(max_data=12,max_stream_data=13"),
            "{line}"
        );
        assert!(line.contains("dg_max=Some(1375)"), "{line}");
        assert!(line.contains("dg_space=18B"), "{line}");
    }

    #[test]
    fn format_tuic_tcp_open_line_includes_target_pool_and_id() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let line = format_tuic_tcp_open_line(&target, 3, 42);

        assert!(line.contains("tuic-open-tcp"), "{line}");
        assert!(line.contains("target=1.2.3.4:5201"), "{line}");
        assert!(line.contains("conn=3"), "{line}");
        assert!(line.contains("id=42"), "{line}");
    }

    #[test]
    fn format_tuic_tcp_stream_diag_lines_include_first_rx_and_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let first = format_tuic_tcp_stream_first_rx_line(&meta, 20_500, 35_244, 1);
        let gap = format_tuic_tcp_stream_read_gap_line(&meta, 15_000, 39_884, 2, 75_128);
        let pending =
            format_tuic_tcp_stream_pending_line(&meta, 12_000, 24, 31, 5_000, 75_128, 2);
        let close_snapshot = TuicTcpStreamCloseSnapshot {
            first_rx_ms: 20_500,
            max_read_gap_ms: 15_000,
            rx_bytes: 109_304,
            reads: 3,
            pending_polls: 24,
            max_pending_gap_ms: 12_000,
            polls: 31,
            max_poll_gap_ms: 5_000,
        };
        let close = format_tuic_tcp_stream_close_line(&meta, &close_snapshot);

        assert!(first.contains("tuic-tcp-stream-first-rx"), "{first}");
        assert!(first.contains("target=1.2.3.4:5201"), "{first}");
        assert!(first.contains("conn=3"), "{first}");
        assert!(first.contains("id=42"), "{first}");
        assert!(first.contains("stream=8"), "{first}");
        assert!(first.contains("first_rx_ms=20500"), "{first}");
        assert!(first.contains("read_bytes=35244"), "{first}");
        assert!(first.contains("reads=1"), "{first}");

        assert!(gap.contains("tuic-tcp-stream-read-gap"), "{gap}");
        assert!(gap.contains("gap_ms=15000"), "{gap}");
        assert!(gap.contains("read_bytes=39884"), "{gap}");
        assert!(gap.contains("rx_bytes=75128"), "{gap}");

        assert!(pending.contains("tuic-tcp-stream-pending"), "{pending}");
        assert!(pending.contains("pending_gap_ms=12000"), "{pending}");
        assert!(pending.contains("pending_polls=24"), "{pending}");
        assert!(pending.contains("polls=31"), "{pending}");
        assert!(pending.contains("max_poll_gap_ms=5000"), "{pending}");
        assert!(pending.contains("rx_bytes=75128"), "{pending}");
        assert!(pending.contains("reads=2"), "{pending}");

        assert!(close.contains("tuic-tcp-stream-close"), "{close}");
        assert!(close.contains("first_rx_ms=20500"), "{close}");
        assert!(close.contains("max_read_gap_ms=15000"), "{close}");
        assert!(close.contains("rx_bytes=109304"), "{close}");
        assert!(close.contains("reads=3"), "{close}");
        assert!(close.contains("pending_polls=24"), "{close}");
        assert!(close.contains("max_pending_gap_ms=12000"), "{close}");
        assert!(close.contains("polls=31"), "{close}");
        assert!(close.contains("max_poll_gap_ms=5000"), "{close}");
    }

    #[test]
    fn tuic_tcp_stream_diag_tracks_first_rx_and_read_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_poll_at(start + std::time::Duration::from_millis(20_500));
        let first = diag.note_read_at(35_244, start + std::time::Duration::from_millis(20_500));
        assert_eq!(first.first_rx_ms, Some(20_500));
        assert_eq!(first.gap_ms, None);
        assert_eq!(first.rx_bytes, 35_244);
        assert_eq!(first.reads, 1);

        diag.note_poll_at(start + std::time::Duration::from_millis(35_500));
        let second = diag.note_read_at(39_884, start + std::time::Duration::from_millis(35_500));
        assert_eq!(second.first_rx_ms, None);
        assert_eq!(second.gap_ms, Some(15_000));
        assert_eq!(second.rx_bytes, 75_128);
        assert_eq!(second.reads, 2);

        let close = diag.close_snapshot();
        assert_eq!(close.first_rx_ms, 20_500);
        assert_eq!(close.max_read_gap_ms, 15_000);
        assert_eq!(close.rx_bytes, 75_128);
        assert_eq!(close.reads, 2);
        assert_eq!(close.polls, 2);
        assert_eq!(close.max_poll_gap_ms, 15_000);
    }

    #[test]
    fn tuic_tcp_stream_diag_tracks_rate_limited_pending_gaps() {
        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let start = std::time::Instant::now();
        let mut diag = TuicTcpStreamDiag::new(meta, start);

        diag.note_poll_at(start + std::time::Duration::from_millis(999));
        assert_eq!(
            diag.note_pending_at(start + std::time::Duration::from_millis(999)),
            None
        );
        diag.note_poll_at(start + std::time::Duration::from_millis(1_000));
        let first = diag
            .note_pending_at(start + std::time::Duration::from_millis(1_000))
            .expect("first threshold-crossing pending poll logs");
        assert_eq!(first.pending_gap_ms, 1_000);
        assert_eq!(first.pending_polls, 2);
        assert_eq!(first.polls, 2);
        assert_eq!(first.max_poll_gap_ms, 1);
        assert_eq!(first.rx_bytes, 0);
        assert_eq!(first.reads, 0);
        diag.note_poll_at(start + std::time::Duration::from_millis(1_500));
        assert_eq!(
            diag.note_pending_at(start + std::time::Duration::from_millis(1_500)),
            None,
            "pending logs are rate limited"
        );
        diag.note_poll_at(start + std::time::Duration::from_millis(2_100));
        let second = diag
            .note_pending_at(start + std::time::Duration::from_millis(2_100))
            .expect("second pending log after the log interval");
        assert_eq!(second.pending_gap_ms, 2_100);
        assert_eq!(second.pending_polls, 4);
        assert_eq!(second.polls, 4);
        assert_eq!(second.max_poll_gap_ms, 600);

        diag.note_poll_at(start + std::time::Duration::from_millis(2_200));
        let read = diag.note_read_at(128, start + std::time::Duration::from_millis(2_200));
        assert_eq!(read.rx_bytes, 128);
        diag.note_poll_at(start + std::time::Duration::from_millis(3_300));
        let after_read = diag
            .note_pending_at(start + std::time::Duration::from_millis(3_300))
            .expect("pending gap is measured from the latest data read");
        assert_eq!(after_read.pending_gap_ms, 1_100);
        assert_eq!(after_read.polls, 6);
        assert_eq!(after_read.max_poll_gap_ms, 1_100);
        assert_eq!(after_read.rx_bytes, 128);
        assert_eq!(after_read.reads, 1);

        let close = diag.close_snapshot();
        assert_eq!(close.pending_polls, 5);
        assert_eq!(close.max_pending_gap_ms, 2_100);
        assert_eq!(close.polls, 6);
        assert_eq!(close.max_poll_gap_ms, 1_100);
    }

    #[tokio::test]
    async fn tracked_relay_stream_records_nonempty_reads() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
        let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
        let active = Arc::new(AtomicU64::new(0));
        let (lease, _) = TcpPoolSlotLease::reserve(active);
        let (mut writer, inner) = tokio::io::duplex(64);
        let mut stream = TrackedRelayStream::new(
            inner,
            lease,
            Some(TuicTcpStreamDiag::new(meta, std::time::Instant::now())),
        );

        writer.write_all(b"abc").await.unwrap();

        let mut buf = [0u8; 3];
        let read = stream.read(&mut buf).await.unwrap();

        assert_eq!(read, 3);
        assert_eq!(&buf, b"abc");
        let snapshot = stream.tcp_diag.as_ref().unwrap().close_snapshot();
        assert_eq!(snapshot.rx_bytes, 3);
        assert_eq!(snapshot.reads, 1);
        assert!(
            snapshot.polls >= 1,
            "stream read should poll at least once: {snapshot:?}"
        );
        assert!(
            snapshot.first_rx_ms < 1_000,
            "first_rx_ms should be immediate in the in-memory stream: {snapshot:?}"
        );
    }

    #[test]
    fn tcp_pool_round_robin_selection() {
        assert_eq!(tcp_pool_index(1, 42), 0);
        assert_eq!(tcp_pool_index(3, 0), 0);
        assert_eq!(tcp_pool_index(3, 1), 1);
        assert_eq!(tcp_pool_index(3, 2), 2);
        assert_eq!(tcp_pool_index(3, 3), 0);
    }

    #[test]
    fn tcp_pool_slot_stale_after_threshold() {
        assert!(!tcp_pool_slot_stale(9, 0));
        assert!(tcp_pool_slot_stale(10, 0));
        assert!(tcp_pool_slot_stale(25, 15));
        assert!(!tcp_pool_slot_stale(24, 15));
        assert!(
            !tcp_pool_slot_stale(3, 15),
            "clock skew/saturation must not mark stale"
        );
    }

    #[test]
    fn tcp_pool_stale_reconnect_skips_primary_connection() {
        assert_eq!(tcp_pool_stale_reconnect_reason(0, 100, 0, true), None);
        assert_eq!(
            tcp_pool_stale_reconnect_reason(1, 100, 90, true),
            Some("stale_tcp_pool_slot")
        );
        assert_eq!(tcp_pool_stale_reconnect_reason(1, 99, 90, true), None);
        assert_eq!(tcp_pool_stale_reconnect_reason(1, 100, 90, false), None);
    }

    #[test]
    fn tcp_pool_slot_lease_reserves_one_idle_owner() {
        let active = Arc::new(AtomicU64::new(0));
        {
            let (_first, first_idle) = TcpPoolSlotLease::reserve(active.clone());
            assert!(first_idle);
            assert_eq!(active.load(Ordering::Relaxed), 1);
            {
                let (_second, second_idle) = TcpPoolSlotLease::reserve(active.clone());
                assert!(!second_idle);
                assert_eq!(active.load(Ordering::Relaxed), 2);
            }
            assert_eq!(active.load(Ordering::Relaxed), 1);
        }
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn zero_rtt_defaults_off_and_parses_on() {
        assert!(!parse_zero_rtt(None)); // 默认关（quinn 0.10 限制）
        assert!(parse_zero_rtt(Some("true")));
        assert!(parse_zero_rtt(Some("1")));
        assert!(parse_zero_rtt(Some("on")));
        assert!(parse_zero_rtt(Some("YES")));
        // 其它一律关。
        assert!(!parse_zero_rtt(Some("false")));
        assert!(!parse_zero_rtt(Some("0")));
        assert!(!parse_zero_rtt(Some("maybe")));
    }

    #[test]
    fn config_requires_core_fields() {
        let bad = |s, u, p| TuicClientConfig::from_sources(s, u, p, None, None, None).is_err();
        assert!(bad(None, Some(UUID), Some("p"))); // no server
        assert!(bad(Some("1.2.3.4:8443"), None, Some("p"))); // no uuid
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), None)); // no password
        assert!(bad(Some("1.2.3.4:8443"), Some(UUID), Some(""))); // empty password
    }

    #[test]
    fn config_rejects_bad_server_and_uuid() {
        assert!(
            TuicClientConfig::from_sources(Some("nope"), Some(UUID), Some("p"), None, None, None)
                .is_err()
        );
        assert!(
            TuicClientConfig::from_sources(
                Some("1.2.3.4:8443"),
                Some("not-a-uuid"),
                Some("p"),
                None,
                None,
                None
            )
            .is_err()
        );
    }

    #[test]
    fn packet_ipv4_layout() {
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        assert_eq!(&p[..2], &[0x05, 0x02]); // ver + Packet
        assert_eq!(&p[2..4], &[0x00, 0x07]); // assoc-id
        assert_eq!(&p[4..6], &[0x00, 0x00]); // pkt-id
        assert_eq!(p[6], 1); // frag total
        assert_eq!(p[7], 0); // frag id
        assert_eq!(&p[8..10], &[0x00, 0x02]); // size = 2
        assert_eq!(&p[10..15], &[0x01, 1, 2, 3, 4]); // atyp ipv4 + ip
        assert_eq!(&p[15..17], &[0x00, 0x35]); // port 53
        assert_eq!(&p[17..], b"hi");
    }

    #[test]
    fn packet_domain_roundtrips_assoc_and_data() {
        let p = encode_packet(
            9,
            &TargetAddr::DomainPort {
                host: "a.com".into(),
                port: 443,
            },
            b"q",
        );
        let (assoc, data) = decode_packet(&p).unwrap();
        assert_eq!(assoc, 9);
        assert_eq!(data, b"q");
    }

    #[test]
    fn packet_decode_rejects_truncated() {
        assert!(decode_packet(&[0u8; 5]).is_none());
        // size says 0, atyp domain len 200 overruns:
        assert!(decode_packet(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 0, 0x00, 200]).is_none());
    }

    #[test]
    fn datagram_pressure_signal() {
        // 出向 datagram 缓冲剩余 < 1 MTU → 视为正在背压（quinn 会静默丢最老）。
        assert!(is_datagram_pressured(0, 1200));
        assert!(is_datagram_pressured(1199, 1200));
        assert!(!is_datagram_pressured(1200, 1200)); // 边界：刚好 1 MTU 不算压力
        assert!(!is_datagram_pressured(100_000, 1200));
    }

    #[test]
    fn format_udp_stats_includes_key_metrics() {
        let s = format_udp_stats(Some(1332), 7, 2, 180, 65535, 3, 1000, 4096);
        assert!(s.contains("stream 兜底=7"), "{s}");
        assert!(s.contains("丢弃=2"), "{s}");
        assert!(s.contains("RTT=180ms"), "{s}");
        assert!(s.contains("cwnd=65535"), "{s}");
        assert!(s.contains("3/1000"), "{s}"); // lost/sent
        assert!(s.contains("4096"), "{s}"); // send_buffer 余
    }

    #[test]
    fn udp_send_plan_native_is_size_based() {
        use UdpRelayMode::Native;
        use UdpSend::*;
        // Native：装得下 → datagram 快路径（含边界 ==max）——保刀3 现行语义，零回归。
        assert_eq!(udp_send_plan(Native, Some(1242), 1000), Datagram);
        assert_eq!(udp_send_plan(Native, Some(1242), 1242), Datagram);
        // 超上限 → stream 兜底。
        assert_eq!(udp_send_plan(Native, Some(1242), 1243), Stream);
        // datagram 不可用（对端不支持/未协商）→ stream。
        assert_eq!(udp_send_plan(Native, None, 100), Stream);
    }

    #[test]
    fn udp_send_plan_quic_is_always_stream() {
        use UdpRelayMode::Quic;
        use UdpSend::*;
        // Quic：无论装不装得下、datagram 是否可用，首包起恒走 uni-stream
        // （触发 TUIC server 镜像下行也走 stream，摆脱 datagram 天花板）。
        assert_eq!(udp_send_plan(Quic, Some(1242), 1000), Stream);
        assert_eq!(udp_send_plan(Quic, Some(1242), 1242), Stream);
        assert_eq!(udp_send_plan(Quic, Some(1242), 1243), Stream);
        assert_eq!(udp_send_plan(Quic, None, 100), Stream);
    }

    #[test]
    fn udp_relay_mode_parses() {
        assert_eq!(UdpRelayMode::parse("native"), Some(UdpRelayMode::Native));
        assert_eq!(UdpRelayMode::parse("quic"), Some(UdpRelayMode::Quic));
        assert_eq!(UdpRelayMode::parse("QUIC"), Some(UdpRelayMode::Quic));
        assert_eq!(UdpRelayMode::parse("nope"), None);
    }

    #[test]
    fn heartbeat_layout() {
        assert_eq!(encode_heartbeat(), vec![0x05, 0x04]);
    }

    fn meta(assoc: u16, pkt: u16, ftot: u8, fid: u8, data: &[u8]) -> PacketMeta<'_> {
        PacketMeta {
            assoc_id: assoc,
            pkt_id: pkt,
            frag_total: ftot,
            frag_id: fid,
            data,
        }
    }

    #[test]
    fn reassemble_single_fragment_passthrough() {
        let mut r = FragReassembler::new();
        // FRAG_TOTAL=1 立即返回整 payload，且不入表（无残留）。
        assert_eq!(
            r.accept(&meta(1, 0, 1, 0, b"hello"), 0),
            Some(b"hello".to_vec())
        );
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_two_fragments_in_order() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(
            r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
            Some(b"ABCD".to_vec())
        );
        assert_eq!(r.pending_len(), 0); // 集齐后清出
    }

    #[test]
    fn reassemble_two_fragments_out_of_order() {
        let mut r = FragReassembler::new();
        // 先到 frag_id=1，再到 0 → 仍按 frag_id 序拼接。
        assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), None);
        assert_eq!(
            r.accept(&meta(5, 9, 2, 0, b"AB"), 0),
            Some(b"ABCD".to_vec())
        );
    }

    #[test]
    fn reassemble_duplicate_fragment_last_writer_wins() {
        let mut r = FragReassembler::new();
        // 重复 frag_id last-writer-wins：第二个 frag_id=0 覆盖第一个（received 不重复计数），
        // 既保跨重连残片被新 frag 顶替，又不破坏完成判定。
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"XX"), 0), None); // 覆盖 slot[0]
        assert_eq!(
            r.accept(&meta(5, 9, 2, 1, b"CD"), 0),
            Some(b"XXCD".to_vec())
        ); // 用较新的
    }

    #[test]
    fn reassemble_incomplete_swept_by_ttl() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 3, 0, b"AB"), 0), None);
        assert_eq!(r.pending_len(), 1);
        r.sweep(11, 10); // first_seen=0，超 TTL=10 → 清
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_rejects_bad_frag_id() {
        let mut r = FragReassembler::new();
        // frag_id >= frag_total → 丢弃，不入表（防越界）。
        assert_eq!(r.accept(&meta(5, 9, 2, 2, b"X"), 0), None);
        assert_eq!(r.pending_len(), 0);
        // frag_total=0 也无效。
        assert_eq!(r.accept(&meta(5, 9, 0, 0, b"X"), 0), None);
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_cap_evicts_oldest() {
        let mut r = FragReassembler::with_cap(1);
        r.accept(&meta(1, 1, 2, 0, b"A"), 0); // partial #1，first_seen=0
        r.accept(&meta(2, 2, 2, 0, b"B"), 5); // 新 key 触发 evict 最老 → 仍 ≤cap
        assert_eq!(r.pending_len(), 1);
    }

    #[test]
    fn packet_meta_single_fragment() {
        // encode_packet 产出的单帧（FRAG_TOTAL=1）→ meta 各字段就位，data 即整 payload。
        let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
        let m = decode_packet_meta(&p).expect("meta");
        assert_eq!(m.assoc_id, 7);
        assert_eq!(m.pkt_id, 0);
        assert_eq!(m.frag_total, 1);
        assert_eq!(m.frag_id, 0);
        assert_eq!(m.data, b"hi");
    }

    #[test]
    fn packet_meta_non_first_fragment_skips_none_addr() {
        // 非首分片：ADDR=ATYP_NONE(0xff，跳 1 字节)，FRAG_ID=1，SIZE=本分片 chunk 长。
        // [ver type assoc(2)=9 pkt(2)=2 ftot=3 fid=1 size(2)=3 ATYP_NONE 'a' 'b' 'c']
        let buf = [
            0x05, 0x02, 0x00, 0x09, 0x00, 0x02, 0x03, 0x01, 0x00, 0x03, 0xff, b'a', b'b', b'c',
        ];
        let m = decode_packet_meta(&buf).expect("meta");
        assert_eq!(m.assoc_id, 9);
        assert_eq!(m.pkt_id, 2);
        assert_eq!(m.frag_total, 3);
        assert_eq!(m.frag_id, 1);
        assert_eq!(m.data, b"abc");
    }

    #[test]
    fn packet_meta_rejects_truncated() {
        assert!(decode_packet_meta(&[0u8; 5]).is_none());
        // size 说 200 但 buffer 不够 → None（不 panic）。
        assert!(
            decode_packet_meta(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 200, 0x01, 1, 2, 3, 4]).is_none()
        );
    }

    #[test]
    fn decode_packet_delegates_to_meta() {
        // decode_packet 仍取 (assoc, data)，与 meta 一致（薄包装零回归）。
        let p = encode_packet(3, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"xy");
        assert_eq!(decode_packet(&p), Some((3, &b"xy"[..])));
    }

    #[test]
    fn heartbeat_only_while_udp_active() {
        let w = 60;
        // 从未发过 UDP(last=0)→ 不发,纯 TCP 会话靠 QUIC keepalive 保活。
        assert!(!should_send_heartbeat(0, 100, w));
        // 活跃窗口内(含同刻与边界 ==window)→ 发。
        assert!(should_send_heartbeat(100, 100, w));
        assert!(should_send_heartbeat(100, 130, w));
        assert!(should_send_heartbeat(100, 160, w)); // 边界 now-last==window
        // 超出活跃窗口 → 停发(此时也无活跃 flow 需要保活)。
        assert!(!should_send_heartbeat(100, 161, w));
    }

    fn tuple(p: u16) -> FourTuple {
        FourTuple {
            src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
            src_port: p,
            dst_ip: std::net::Ipv4Addr::new(198, 18, 0, 5),
            dst_port: 443,
        }
    }

    #[test]
    fn assoc_intern_stable_and_unique() {
        let mut t = AssocTable::new();
        let a = t.intern(tuple(1000));
        assert_eq!(a, t.intern(tuple(1000)));
        assert_ne!(a, t.intern(tuple(1001)));
    }

    #[test]
    fn assoc_intern_skips_live_id_on_wraparound() {
        // u16 回绕后 next_id 可能落到仍在册的 id 上：intern 必须跳过，绝不覆盖活跃 flow。
        let mut t = AssocTable::with_cap(8);
        let keep = t.intern(tuple(1)); // 一条长寿命 flow 占住 id=keep
        t.next_id = keep; // 把分配游标强行推回 keep（模拟回绕撞上活跃 id）
        let other = t.intern(tuple(2));
        assert_ne!(other, keep, "intern 覆盖了仍在册的 flow");
        assert!(t.resolve(keep).is_some(), "长寿命 flow 被覆盖丢失");
        assert!(t.resolve(other).is_some());
        // 两条 flow 各自映射独立，未发生 tuple_to_id 泄漏/串号。
        assert_eq!(t.intern(tuple(1)), keep);
        assert_eq!(t.intern(tuple(2)), other);
    }

    #[test]
    fn assoc_contains_tracks_membership() {
        let mut t = AssocTable::new();
        assert!(!t.contains(&tuple(1000)));
        t.intern(tuple(1000));
        assert!(t.contains(&tuple(1000)));
        assert!(!t.contains(&tuple(1001)));
    }

    #[test]
    fn assoc_resolve_endpoints_and_sweep() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        let e = t.resolve(id).expect("entry");
        assert_eq!(
            e.app_endpoint(),
            (std::net::Ipv4Addr::new(10, 0, 0, 1), 1000)
        );
        assert_eq!(
            e.target_src(),
            (std::net::Ipv4Addr::new(198, 18, 0, 5), 443)
        );
        t.sweep(61, 60);
        assert!(t.resolve(id).is_none());
    }

    /// 刀2：sweep 回收带 fake-IP 的 assoc 时，直接返回该 fake-IP（review #8：自包含返回）。
    #[test]
    fn assoc_sweep_reclaims_fake_ip() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        t.set_fake_ip(id, Ipv4Addr::new(198, 18, 0, 5));
        t.touch(id, 0);
        // 未过期：不回收，返回空。
        assert!(t.sweep(30, 60).is_empty());
        // 过期：回收，返回含该 fake-IP。
        assert_eq!(t.sweep(61, 60), vec![Ipv4Addr::new(198, 18, 0, 5)]);
    }

    /// 刀2：LRU 驱逐带 fake-IP 的 assoc 时也累积 reclaimed（intern 到 cap 触发 evict）。
    #[test]
    fn assoc_evict_reclaims_fake_ip() {
        let mut t = AssocTable::with_cap(1);
        let id1 = t.intern(tuple(1));
        t.set_fake_ip(id1, Ipv4Addr::new(198, 18, 0, 9));
        // 再 intern 一条 → cap=1 触发 evict id1。
        let _id2 = t.intern(tuple(2));
        assert_eq!(
            t.take_reclaimed_fake_ips(),
            vec![Ipv4Addr::new(198, 18, 0, 9)]
        );
    }

    #[test]
    fn assoc_touch_keeps_alive() {
        let mut t = AssocTable::new();
        let id = t.intern(tuple(1000));
        t.touch(id, 50);
        t.sweep(100, 60);
        assert!(t.resolve(id).is_some());
    }

    #[test]
    fn assoc_lru_evicts_at_cap() {
        let mut t = AssocTable::with_cap(2);
        let a = t.intern(tuple(1));
        let _b = t.intern(tuple(2));
        let _c = t.intern(tuple(3));
        assert!(t.resolve(a).is_none());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn quic_config_builds_with_h3_alpn() {
        // TUIC 上游的 TLS 配置(自定义 ALPN)能构建 —— connect 的真验证在互通 e2e(Task 6)。
        assert!(
            crate::quic::client_quic_config_alpn(
                "certs/dev/ca-cert.pem",
                vec![b"h3".to_vec()],
                crate::quic::CcChoice::Bbr,
            )
            .is_ok()
        );
    }

    #[test]
    fn config_debug_redacts_credentials() {
        let c = TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some(UUID),
            Some("secret123"),
            None,
            None,
            None,
        )
        .unwrap();
        let s = format!("{c:?}");
        assert!(!s.contains("secret123"), "password leaked: {s}");
        assert!(!s.contains("550e8400"), "uuid leaked: {s}");
        assert!(s.contains("redacted"));
    }
}
