//! TUIC v5 client (Stage 13a) — command codec + client upstream.
//!
//! 中文要点：实现成熟的 TUIC v5 协议(出口对接 sing-box,client-only,见 ADR-0004)。
//! 本文件先落「命令编码」纯函数(TDD 主战场),字节布局**严格按 TUIC v5 规范**,与 sing-box 字节级互通。
//! 线格式参考见 docs/tech/2026-06-08-stage-13a-tuic-tcp-connect-plan.md。

use crate::quic;
use crate::shared::{ClientError, TargetAddr};
use crate::udp_relay::{FlowEntry, FourTuple, MAX_UDP_FLOWS};
use crate::upstream::{DatagramUpstream, ProxyUpstream, RelayStream};
use quinn::{Connection, Endpoint};
use std::collections::HashMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::mpsc;

/// 默认 ALPN：TUIC over QUIC 常用 `h3`，必须与 sing-box `tls.alpn` 一致。
const DEFAULT_TUIC_ALPN: &str = "h3";
const DEFAULT_TUIC_SNI: &str = "localhost";
const DEFAULT_TUIC_CA_PATH: &str = "cert.pem";
const DEFAULT_TUIC_CC: &str = "bbr";
const DEFAULT_TUIC_UDP_MODE: &str = "native";

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
        Ok(cfg)
    }
}

/// 解析 `MINI_VPN_TUIC_ZERO_RTT`：**默认关**；显式 `true`/`1`/`on`/`yes`（大小写无关）开。
/// 中文要点：quinn 0.10 / rustls 0.21 在 0-RTT(握手未完成)阶段**不支持 `export_keying_material`**，
/// 而 TUIC token 依赖它 → 0-RTT 认证必失败、自愈回落 1-RTT（实测 2026-06-11，见 13c 验收）。默认开纯属
/// 每次重连白跑一次握手，故默认关；保留开关供未来 quinn 支持 0-RTT keying-material 后启用。
fn parse_zero_rtt(s: Option<&str>) -> bool {
    matches!(
        s.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("true") | Some("1") | Some("on") | Some("yes")
    )
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

/// 解码下行 `Packet`,只取 `(assoc_id, data)`(跳过 ADDR)。越界/地址类型未知返回 None。
pub fn decode_packet(buf: &[u8]) -> Option<(u16, &[u8])> {
    // 固定前缀 10 字节:ver type assoc(2) pkt(2) ftot fid size(2)。
    if buf.len() < 10 {
        return None;
    }
    let assoc = u16::from_be_bytes([buf[2], buf[3]]);
    let size = u16::from_be_bytes([buf[8], buf[9]]) as usize;
    let addr_len = address_len(buf, 10)?;
    let data_start = 10 + addr_len;
    let data_end = data_start.checked_add(size)?;
    if buf.len() < data_end {
        return None;
    }
    Some((assoc, &buf[data_start..data_end]))
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

/// 把任意可显示错误包成 ClientError（统一错误面）。
fn io_err<E: std::fmt::Display>(ctx: &str, e: E) -> ClientError {
    ClientError::from(std::io::Error::other(format!("{ctx}: {e}")))
}

/// TUIC 客户端上游：持有一条到 sing-box 的 QUIC 连接，每条 TCP 开一条 `Connect` 双向流。
/// 中文要点：连接断了按需重连+重认证(13a 最小实现;迁移/0-RTT 调优在 13c)。
pub struct TuicUpstream {
    endpoint: Endpoint,
    server: SocketAddr,
    sni: String,
    uuid: [u8; 16],
    password: String,
    conn: Mutex<Connection>,
    /// 上行 UDP datagram 丢弃计数（TooLarge / 连接不可用）。可观测性，不影响 UDP 语义。
    udp_drops: AtomicU64,
    /// 上次 UDP 上行的秒数（`clock` 起点，0=从未）。`send_udp` 写、心跳读，决定是否按需保活。
    last_udp_activity: AtomicU64,
    /// 单调时钟：`send_udp`(写活跃时刻)与驱动任务(读 now)**同源**，避免双时钟漂移。
    clock: std::time::Instant,
    /// 重连是否尝试 0-RTT（来自 config；失败自愈回落 1-RTT）。
    zero_rtt: bool,
}

impl TuicUpstream {
    /// 建连 + 发 Authenticate（token 经 keying-material 导出，字节级对齐 sing-box）。
    pub async fn connect(cfg: &TuicClientConfig) -> Result<Self, ClientError> {
        let qcfg = quic::client_quic_config_alpn(&cfg.ca_path, vec![cfg.alpn.as_bytes().to_vec()])
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
        Ok(Self {
            endpoint,
            server: cfg.server,
            sni: cfg.sni.clone(),
            uuid: cfg.uuid,
            password: cfg.password.clone(),
            conn: Mutex::new(conn),
            udp_drops: AtomicU64::new(0),
            last_udp_activity: AtomicU64::new(0),
            clock: std::time::Instant::now(),
            zero_rtt: cfg.zero_rtt,
        })
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
        if zero_rtt
            && let Some(conn) = Self::try_0rtt(endpoint, server, sni, uuid, password).await
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
            println!("⚠️ TUIC 0-RTT 认证失败(可能 early-exporter 与 sing-box 不齐)，回落 1-RTT: {e:?}");
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
        let mut uni = conn.open_uni().await.map_err(|e| io_err("tuic open_uni", e))?;
        uni.write_all(&encode_authenticate(uuid, &token))
            .await
            .map_err(|e| io_err("tuic auth write", e))?;
        uni.finish().await.map_err(|e| io_err("tuic auth finish", e))?;
        Ok(())
    }

    /// 取当前活连接的克隆；若已关闭则就地重连+重认证（13a 逻辑，TCP/UDP 共用）。
    /// 中文要点：单一事实源——TCP `open_tcp` 与 UDP `send_udp`/驱动任务都经此取连接，
    /// 由 `conn` 互斥锁串行化重连，避免并发双连接。
    async fn live_conn(&self) -> Result<Connection, ClientError> {
        let mut guard = self.conn.lock().await;
        if guard.close_reason().is_some() {
            *guard = Self::handshake(
                &self.endpoint,
                self.server,
                &self.sni,
                &self.uuid,
                &self.password,
                self.zero_rtt,
            )
            .await?;
        }
        Ok(guard.clone())
    }

    /// 发一条**已编码**的 TUIC Packet（native datagram）。
    /// 中文要点：TooLarge / 连接不可用 → 丢弃并计数（UDP 语义，绝不阻塞调用方除重连外）。
    /// native 模式不分片，超 QUIC datagram 上限的包直接丢（quic-stream 兜底留待后续）。
    pub async fn send_udp(&self, datagram: Vec<u8>) {
        // 记录 UDP 活跃时刻：驱动任务据此「仅活跃时发」Heartbeat（纯 TCP 不发，省流量/电量）。
        self.last_udp_activity
            .store(self.clock.elapsed().as_secs(), Ordering::Relaxed);
        let conn = match self.live_conn().await {
            Ok(c) => c,
            Err(e) => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                println!("⚠️ TUIC UDP↑ 无可用连接，丢弃: {e:?}");
                return;
            }
        };
        match conn.send_datagram(datagram.into()) {
            Ok(()) => {}
            Err(quinn::SendDatagramError::TooLarge) => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                println!("⚠️ TUIC UDP↑ datagram 超过 QUIC 上限，丢弃");
            }
            Err(e) => {
                self.udp_drops.fetch_add(1, Ordering::Relaxed);
                println!("⚠️ TUIC UDP↑ 发送失败（连接将自愈），丢弃: {e:?}");
            }
        }
    }

    /// 累计丢弃的上行 UDP datagram 数（可观测性）。
    pub fn udp_drop_count(&self) -> u64 {
        self.udp_drops.load(Ordering::Relaxed)
    }

    /// 启动 UDP 驱动后台任务，返回**下行 datagram 接收端**给主循环 select。
    /// 任务职责：① 下行泵——`read_datagram` → channel（主循环 `decode_packet` 后注入 TUN）；
    /// ② 周期 Heartbeat 维持连接。连接断开则确定性退避后经 `live_conn` 重连，泵与心跳自然恢复
    /// （等价于"重连后重启泵/心跳"）。
    /// 中文要点：单自愈循环，避免多任务各自重连产生竞态；下行用 `send().await` 施加背压、不丢 DNS 响应。
    pub fn start_udp(self: &Arc<Self>) -> mpsc::Receiver<Vec<u8>> {
        let (downlink_tx, downlink_rx) = mpsc::channel::<Vec<u8>>(TUIC_DOWNLINK_CAPACITY);
        let me = Arc::clone(self);
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
        // 取活连接，断了就地重连+重认证（与 UDP 共用 live_conn，单一事实源）。
        let conn = self.live_conn().await?;
        let (mut send, recv) = conn.open_bi().await.map_err(|e| io_err("tuic open_bi", e))?;
        send.write_all(&encode_connect(target))
            .await
            .map_err(|e| io_err("tuic connect write", e))?;
        // 把双向流的收/发两半合成一条 AsyncRead+AsyncWrite，喂给现有双向泵。
        Ok(Box::new(tokio::io::join(recv, send)))
    }
}

#[async_trait::async_trait]
impl DatagramUpstream for TuicUpstream {
    // 委托既有 inherent `send_udp`（inherent 优先解析，不会递归）。
    async fn send_udp(&self, datagram: Vec<u8>) {
        TuicUpstream::send_udp(self, datagram).await
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
        assert_eq!(c.congestion_control, "bbr");
        assert_eq!(c.udp_relay_mode, "native");
        assert!(!c.zero_rtt, "0-RTT 默认关（quinn 0.10 在 0-RTT 不支持 keying-material 导出）");
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
    fn heartbeat_layout() {
        assert_eq!(encode_heartbeat(), vec![0x05, 0x04]);
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
        assert_eq!(e.app_endpoint(), (std::net::Ipv4Addr::new(10, 0, 0, 1), 1000));
        assert_eq!(e.target_src(), (std::net::Ipv4Addr::new(198, 18, 0, 5), 443));
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
            crate::quic::client_quic_config_alpn("certs/dev/ca-cert.pem", vec![b"h3".to_vec()])
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
