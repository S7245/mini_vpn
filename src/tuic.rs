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
/// 一个 TUIC Packet 命令的字节上限（读 uni-stream 的 `read_to_end` size_limit，防恶意/异常无界流）。
/// UDP 载荷最大 65507 + TUIC 头/地址 ≈ 65537 → 取 66KiB 兜头。
const MAX_TUIC_PACKET_BYTES: usize = 66 * 1024;
/// 下行 uni-stream 并发读取上限：超额直接丢弃该 stream（reset），防 flood 下无界派生任务。
/// quic-relay-mode 下每包一条 uni-stream；256 并发够吸收突发，又不至失控。
const MAX_CONCURRENT_DOWNLINK_STREAMS: usize = 256;
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
struct FragPartial {
    frag_total: u8,
    frags: Vec<Option<Vec<u8>>>, // 下标 = frag_id
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
    /// 重复 frag_id 幂等（保留首个）；新 key 到 cap 触发 LRU（按 first_seen）驱逐最老未完成包。
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
        let stale = self
            .partials
            .get(&key)
            .is_some_and(|p| p.frag_total != m.frag_total);
        if stale {
            self.partials.remove(&key);
        }
        if !self.partials.contains_key(&key) {
            self.evict_if_full();
            self.partials.insert(
                key,
                FragPartial {
                    frag_total: m.frag_total,
                    frags: (0..frag_total).map(|_| None).collect(),
                    received: 0,
                    first_seen: now,
                },
            );
        }
        let p = self.partials.get_mut(&key).expect("just inserted");
        let slot = &mut p.frags[m.frag_id as usize];
        if slot.is_some() {
            return None; // 重复 frag，幂等忽略
        }
        *slot = Some(m.data.to_vec());
        p.received += 1;
        if p.received < p.frag_total as usize {
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

/// 纯决策：给定当前 datagram 发送上限（`conn.max_datagram_size()`）与待发字节数，选传输。
/// 中文要点：装得下走 datagram（含边界 `len==max`）；超上限或 datagram 不可用（`None`）→ stream 兜底。
/// 主动分流（先查 max_datagram_size），避免 `send_datagram` 返回 `TooLarge` 的往返。
pub fn udp_send_plan(max_datagram: Option<usize>, len: usize) -> UdpSend {
    match max_datagram {
        Some(max) if len <= max => UdpSend::Datagram,
        _ => UdpSend::Stream,
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
            udp_stream_fallbacks: AtomicU64::new(0),
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

    /// 发一条**已编码**的 TUIC Packet（刀3：datagram 主路径 + uni-stream 兜底）。
    /// 中文要点：先按 `udp_send_plan(max_datagram_size, len)` 主动分流——装得下走 native datagram，
    /// 超上限/不可用走 **per-packet uni-stream 兜底**（持续大流量直播不丢包）。datagram 真发遇
    /// `TooLarge`（MTU 竞态收缩）→ 二次 stream 兜底。仅**真失败**才丢弃计数（UDP 语义，不阻塞调用方除重连）。
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
        // Bytes：datagram 路径下 `clone()` 为 O(1)（Arc 引用计数），TooLarge 竞态二次兜底不深拷贝。
        let bytes = bytes::Bytes::from(datagram);
        match udp_send_plan(conn.max_datagram_size(), bytes.len()) {
            UdpSend::Datagram => match conn.send_datagram(bytes.clone()) {
                Ok(()) => {}
                Err(quinn::SendDatagramError::TooLarge) => {
                    // 分流时还装得下、真发时 MTU 已收缩 → 二次 stream 兜底，不丢。
                    self.send_udp_via_stream(&conn, bytes).await;
                }
                Err(e) => {
                    self.udp_drops.fetch_add(1, Ordering::Relaxed);
                    println!("⚠️ TUIC UDP↑ 发送失败（连接将自愈），丢弃: {e:?}");
                }
            },
            UdpSend::Stream => self.send_udp_via_stream(&conn, bytes).await,
        }
    }

    /// uni-stream 兜底：开单向流 → 写整条已编码 Packet → finish。
    /// 中文要点：TUIC quic-relay-mode 即「一条 uni-stream 承载一个完整 Packet 命令」（`FRAG_TOTAL=1`），
    /// 字节与 datagram 模式**完全一致**（复用同一 `encode_packet` 产物）。任一步失败→丢弃计数（UDP 自愈重发）。
    async fn send_udp_via_stream(&self, conn: &Connection, datagram: bytes::Bytes) {
        self.udp_stream_fallbacks.fetch_add(1, Ordering::Relaxed);
        if let Err(e) = Self::write_uni_packet(conn, &datagram).await {
            self.udp_drops.fetch_add(1, Ordering::Relaxed);
            println!("⚠️ TUIC UDP↑ stream 兜底失败（连接将自愈），丢弃: {e:?}");
        }
    }

    /// 在连接上开 uni-stream 写一个 Packet 并 finish（抽出便于错误归一）。
    async fn write_uni_packet(conn: &Connection, packet: &[u8]) -> Result<(), ClientError> {
        let mut uni = conn.open_uni().await.map_err(|e| io_err("udp open_uni", e))?;
        uni.write_all(packet)
            .await
            .map_err(|e| io_err("udp uni write", e))?;
        uni.finish().await.map_err(|e| io_err("udp uni finish", e))?;
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
                                        tokio::spawn(async move {
                                            let _permit = permit; // 持有到读完，限并发
                                            if let Some(pkt) = read_uni_packet(recv).await {
                                                let _ = tx.send(pkt).await;
                                            }
                                        });
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
    fn udp_send_plan_picks_transport() {
        use UdpSend::*;
        // 装得下 → datagram 快路径（含边界 ==max）。
        assert_eq!(udp_send_plan(Some(1242), 1000), Datagram);
        assert_eq!(udp_send_plan(Some(1242), 1242), Datagram);
        // 超上限 → stream 兜底。
        assert_eq!(udp_send_plan(Some(1242), 1243), Stream);
        // datagram 不可用（对端不支持/未协商）→ stream。
        assert_eq!(udp_send_plan(None, 100), Stream);
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
        assert_eq!(r.accept(&meta(1, 0, 1, 0, b"hello"), 0), Some(b"hello".to_vec()));
        assert_eq!(r.pending_len(), 0);
    }

    #[test]
    fn reassemble_two_fragments_in_order() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), Some(b"ABCD".to_vec()));
        assert_eq!(r.pending_len(), 0); // 集齐后清出
    }

    #[test]
    fn reassemble_two_fragments_out_of_order() {
        let mut r = FragReassembler::new();
        // 先到 frag_id=1，再到 0 → 仍按 frag_id 序拼接。
        assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), None);
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), Some(b"ABCD".to_vec()));
    }

    #[test]
    fn reassemble_duplicate_fragment_idempotent() {
        let mut r = FragReassembler::new();
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"AB"), 0), None);
        assert_eq!(r.accept(&meta(5, 9, 2, 0, b"XX"), 0), None); // 重复 frag_id=0，忽略
        assert_eq!(r.accept(&meta(5, 9, 2, 1, b"CD"), 0), Some(b"ABCD".to_vec())); // 仍用首个
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
        assert!(decode_packet_meta(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 200, 0x01, 1, 2, 3, 4]).is_none());
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
