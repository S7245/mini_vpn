//! UDP relay over the QUIC datagram data plane (Stage 12).
//!
//! 中文要点：本模块是 UDP relay 的纯逻辑收口——QUIC datagram 的线格式编解码、flow 表、
//! 以及裸 IP/UDP 包的造/解。放在 lib crate 里，让 server/client 两个 binary 模块和
//! `tests/` 集成测试都能复用。承重决策见 docs/adr/0003 与 stage-12 spec。

use crate::shared::TargetAddr;
use std::collections::HashMap;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use quinn::Connection;
use tokio::net::UdpSocket;

/// ATYP=1：4 字节 IPv4 地址。
const ATYP_IPV4: u8 = 1;
/// ATYP=3：`[len:u8][域名 len 字节]`。
const ATYP_DOMAIN: u8 = 3;
/// ATYP=4：16 字节 IPv6 地址（TUN 路径目前只产 IPv4，这里为健壮性兜底支持）。
const ATYP_IPV6: u8 = 4;

/// 编码一个上行 datagram：`[flow-id:u32][ATYP][ADDR][PORT:u16][payload...]`（大端）。
/// 中文要点：target 每包内联，服务端逐包无状态解析，无建流握手。
pub fn encode_uplink(flow_id: u32, target: &TargetAddr, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(11 + payload.len());
    v.extend_from_slice(&flow_id.to_be_bytes());
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
            // 域名长度用 u8 承载；DNS 域名上限 253，必然装得下，这里 saturating 兜底。
            let host_bytes = host.as_bytes();
            let len = host_bytes.len().min(u8::MAX as usize);
            v.push(ATYP_DOMAIN);
            v.push(len as u8);
            v.extend_from_slice(&host_bytes[..len]);
            v.extend_from_slice(&port.to_be_bytes());
        }
    }
    v.extend_from_slice(payload);
    v
}

/// 解码上行 datagram，返回 `(flow-id, target, payload)`。
/// 中文要点：任何越界 / 非法 UTF-8 / 未知 ATYP 一律返回 None，绝不 panic。
pub fn decode_uplink(buf: &[u8]) -> Option<(u32, TargetAddr, &[u8])> {
    // 至少 4 字节 flow-id + 1 字节 ATYP。
    if buf.len() < 5 {
        return None;
    }
    let flow_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    let atyp = buf[4];
    let mut pos = 5usize;
    let target = match atyp {
        ATYP_IPV4 => {
            // 4 addr + 2 port
            if buf.len() < pos + 6 {
                return None;
            }
            let ip = Ipv4Addr::new(buf[pos], buf[pos + 1], buf[pos + 2], buf[pos + 3]);
            let port = u16::from_be_bytes([buf[pos + 4], buf[pos + 5]]);
            pos += 6;
            TargetAddr::IpPort(SocketAddr::from((ip, port)))
        }
        ATYP_DOMAIN => {
            if buf.len() < pos + 1 {
                return None;
            }
            let dlen = buf[pos] as usize;
            pos += 1;
            // 域名 dlen 字节 + 2 字节端口
            if buf.len() < pos + dlen + 2 {
                return None;
            }
            let host = std::str::from_utf8(&buf[pos..pos + dlen]).ok()?.to_string();
            let port = u16::from_be_bytes([buf[pos + dlen], buf[pos + dlen + 1]]);
            pos += dlen + 2;
            TargetAddr::DomainPort { host, port }
        }
        ATYP_IPV6 => {
            // 16 addr + 2 port
            if buf.len() < pos + 18 {
                return None;
            }
            let octets: [u8; 16] = buf[pos..pos + 16].try_into().ok()?;
            let ip = Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([buf[pos + 16], buf[pos + 17]]);
            pos += 18;
            TargetAddr::IpPort(SocketAddr::from((ip, port)))
        }
        _ => return None,
    };
    Some((flow_id, target, &buf[pos..]))
}

/// 编码一个下行 datagram：`[flow-id:u32][payload...]`。
pub fn encode_downlink(flow_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(4 + payload.len());
    v.extend_from_slice(&flow_id.to_be_bytes());
    v.extend_from_slice(payload);
    v
}

/// 解码下行 datagram，返回 `(flow-id, payload)`。越界返回 None。
pub fn decode_downlink(buf: &[u8]) -> Option<(u32, &[u8])> {
    if buf.len() < 4 {
        return None;
    }
    let flow_id = u32::from_be_bytes(buf[0..4].try_into().ok()?);
    Some((flow_id, &buf[4..]))
}

/// 单客户端最多并发的 UDP flow 数；到顶 LRU 驱逐（见 spec / 系统稳定优先）。
pub const MAX_UDP_FLOWS: usize = 1024;

/// UDP flow 空闲回收阈值（秒）。client 与 server 各自独立计时、自愈兜底（见 stage-12 spec）。
/// 中文要点：收口在此（sweep / expired_flow_ids 都在本模块），避免两端常量漂移。
pub const UDP_FLOW_IDLE_SECS: u64 = 60;

/// 一条 UDP flow 的四元组身份：`(srcIP:srcPort, dstIP:dstPort)`（IPv4，TUN 路径）。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FourTuple {
    pub src_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_ip: Ipv4Addr,
    pub dst_port: u16,
}

/// flow 表里一条 flow 的运行态。
#[derive(Clone, Copy, Debug)]
pub struct FlowEntry {
    pub tuple: FourTuple,
    /// 最近活动时间（秒，由调用方注入，便于单测；不在内部读时钟）。
    pub last_activity: u64,
}

impl FlowEntry {
    /// 下行包的目的：app 当初的源端点。
    pub fn app_endpoint(&self) -> (Ipv4Addr, u16) {
        (self.tuple.src_ip, self.tuple.src_port)
    }
    /// 下行包的源：app 当初发往的目的（fake-IP/target），让 app 认得是对端回包。
    pub fn target_src(&self) -> (Ipv4Addr, u16) {
        (self.tuple.dst_ip, self.tuple.dst_port)
    }
}

/// client 侧 flow 表：四元组 ↔ flow-id 双向 + 空闲回收 + LRU 上限。
/// 中文要点：主循环独占、无锁；`now` 由调用方注入（不在内部读时钟，便于单测，呼应
/// `backoff_delay` 的随机注入做法）。
#[derive(Debug)]
pub struct FlowTable {
    tuple_to_id: HashMap<FourTuple, u32>,
    id_to_entry: HashMap<u32, FlowEntry>,
    next_id: u32,
    cap: usize,
}

impl Default for FlowTable {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowTable {
    pub fn new() -> Self {
        Self::with_cap(MAX_UDP_FLOWS)
    }

    pub fn with_cap(cap: usize) -> Self {
        Self {
            tuple_to_id: HashMap::new(),
            id_to_entry: HashMap::new(),
            next_id: 1,
            cap: cap.max(1),
        }
    }

    /// 该四元组是否已在册（用于「仅在新 flow 时打日志」，避免每包刷屏）。
    pub fn contains(&self, tuple: &FourTuple) -> bool {
        self.tuple_to_id.contains_key(tuple)
    }

    /// 查/铸 flow-id：四元组已在册稳定返回；否则铸新 id（到顶先 LRU 驱逐）。
    pub fn intern(&mut self, tuple: FourTuple) -> u32 {
        if let Some(&id) = self.tuple_to_id.get(&tuple) {
            return id;
        }
        if self.id_to_entry.len() >= self.cap {
            self.evict_lru();
        }
        let id = self.next_id;
        // 单调递增、绝不复用，避免回收后的迟到 datagram 串到新 flow。
        self.next_id = self.next_id.wrapping_add(1);
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

    pub fn resolve(&self, flow_id: u32) -> Option<&FlowEntry> {
        self.id_to_entry.get(&flow_id)
    }

    pub fn touch(&mut self, flow_id: u32, now: u64) {
        if let Some(e) = self.id_to_entry.get_mut(&flow_id) {
            e.last_activity = now;
        }
    }

    /// 回收空闲超过 `idle_secs` 的 flow（双向删表）。
    pub fn sweep(&mut self, now: u64, idle_secs: u64) {
        let expired: Vec<(u32, FourTuple)> = self
            .id_to_entry
            .iter()
            .filter(|(_, e)| now.saturating_sub(e.last_activity) > idle_secs)
            .map(|(id, e)| (*id, e.tuple))
            .collect();
        for (id, tuple) in expired {
            self.id_to_entry.remove(&id);
            self.tuple_to_id.remove(&tuple);
        }
    }

    /// 驱逐最久未活动的一条（last_activity 最小，同值取最小 flow-id = 最早插入）。
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
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.id_to_entry.len()
    }
}

/// 解析出来的入站 UDP 报文要点（裸包路径用）。
#[derive(Debug, PartialEq, Eq)]
pub struct UdpInbound<'a> {
    pub src_ip: Ipv4Addr,
    pub src_port: u16,
    pub dst_ip: Ipv4Addr,
    pub dst_port: u16,
    pub payload: &'a [u8],
}

/// 解析一个裸 IPv4/UDP 包。非 IPv4 / 非 UDP / 解析失败一律 None（绝不 panic）。
pub fn parse_inbound_udp(pkt: &[u8]) -> Option<UdpInbound<'_>> {
    let headers = etherparse::PacketHeaders::from_ip_slice(pkt).ok()?;
    let etherparse::IpHeader::Version4(ipv4, _) = headers.ip? else {
        return None;
    };
    let etherparse::TransportHeader::Udp(udp) = headers.transport? else {
        return None;
    };
    Some(UdpInbound {
        src_ip: Ipv4Addr::from(ipv4.source),
        src_port: udp.source_port,
        dst_ip: Ipv4Addr::from(ipv4.destination),
        dst_port: udp.destination_port,
        payload: headers.payload,
    })
}

/// 构造一个裸 IPv4/UDP 包（含正确的 IPv4 + UDP 校验和），用于下行注入 TUN。
pub fn build_udp_ip_packet(src: (Ipv4Addr, u16), dst: (Ipv4Addr, u16), payload: &[u8]) -> Vec<u8> {
    let builder =
        etherparse::PacketBuilder::ipv4(src.0.octets(), dst.0.octets(), 64).udp(src.1, dst.1);
    let mut buf = Vec::with_capacity(20 + 8 + payload.len());
    // etherparse 在 write 时计算 IPv4 + UDP 校验和；写入 Vec 不会失败。
    builder
        .write(&mut buf, payload)
        .expect("build_udp_ip_packet: write to Vec is infallible");
    buf
}

// ===================== 服务端：QUIC datagram → 出口 UDP relay =====================

/// 从「flow_id → last_activity(秒)」里挑出空闲超过 `idle_secs` 的 flow（服务端会话回收）。
/// 中文要点：纯函数，便于单测回收策略；async 侧用它驱动 socket 关闭。
pub fn expired_flow_ids<I>(entries: I, now: u64, idle_secs: u64) -> Vec<u32>
where
    I: IntoIterator<Item = (u32, u64)>,
{
    entries
        .into_iter()
        .filter(|(_, last)| now.saturating_sub(*last) > idle_secs)
        .map(|(id, _)| id)
        .collect()
}

/// 一条出口 UDP 会话：socket + 回程 task + 最近活动时间。
struct ServerSession {
    socket: Arc<UdpSocket>,
    recv_task: tokio::task::JoinHandle<()>,
    last_activity: u64,
}

/// 把 relay target 解析成可发的 `SocketAddr`（DomainPort 用服务端干净 DNS）。
async fn resolve_target_addr(target: &TargetAddr) -> Option<SocketAddr> {
    match target {
        TargetAddr::IpPort(addr) => Some(*addr),
        TargetAddr::DomainPort { host, port } => {
            tokio::net::lookup_host((host.as_str(), *port))
                .await
                .ok()?
                .next()
        }
    }
}

/// 为一个 flow 起回程 task：从（已 connect 的）出口 socket 收包 → 打 flow-id → 回客户端。
/// 中文要点：socket 已 connect 到 target，`recv` 只收该对端的包 —— 杜绝任意主机伪造回程
/// （off-path UDP 欺骗，对 DNS-over-UDP 尤其重要）。
fn spawn_downlink(conn: Connection, flow_id: u32, socket: Arc<UdpSocket>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut buf = vec![0u8; 65_535];
        loop {
            match socket.recv(&mut buf).await {
                Ok(n) => {
                    let dg = encode_downlink(flow_id, &buf[..n]);
                    match conn.send_datagram(dg.into()) {
                        Ok(()) => {}
                        // 超限 → 丢弃（UDP 语义），但记一笔，绝不静默。
                        Err(quinn::SendDatagramError::TooLarge) => {
                            println!("⚠️ UDP 下行 datagram 超过 QUIC 上限，丢弃 (flow {flow_id})");
                        }
                        // 连接断/被禁/不支持 → 退出。
                        Err(_) => break,
                    }
                }
                Err(_) => break,
            }
        }
    })
}

/// 服务端：在一条 QUIC 连接上中继 UDP datagram。每个 flow-id 配一个出口 UDP socket，
/// 空闲超过 `idle_secs` 回收。中文要点：朴素「每流一 socket」，池化/端口抗压留平台 stage。
pub async fn serve_quic_connection(conn: Connection, idle_secs: u64) {
    let mut sessions: HashMap<u32, ServerSession> = HashMap::new();
    let mut sweep = tokio::time::interval(std::time::Duration::from_secs(1));
    let start = std::time::Instant::now();

    loop {
        tokio::select! {
            dg = conn.read_datagram() => {
                let bytes = match dg {
                    Ok(b) => b,
                    Err(_) => break, // 连接断开
                };
                let Some((flow_id, target, payload)) = decode_uplink(&bytes) else {
                    continue;
                };
                let now = start.elapsed().as_secs();
                // 取/建会话；首包才解析一次 target、按地址族绑定并 connect（此后整条 flow 复用）。
                // 中文要点：解析移出每包热路径（避免 HOL + 重复 DNS）；connect 让收发只认该对端。
                let socket = match sessions.get_mut(&flow_id) {
                    Some(s) => {
                        s.last_activity = now;
                        s.socket.clone()
                    }
                    None => {
                        let Some(dst) = resolve_target_addr(&target).await else {
                            println!(
                                "❌ UDP relay 无法解析 target {} (flow {flow_id})",
                                target.to_wire_string()
                            );
                            continue;
                        };
                        println!(
                            "📨 UDP relay flow={flow_id} {} → {dst}",
                            target.to_wire_string()
                        );
                        let bind: SocketAddr = if dst.is_ipv4() {
                            (std::net::Ipv4Addr::UNSPECIFIED, 0).into()
                        } else {
                            (std::net::Ipv6Addr::UNSPECIFIED, 0).into()
                        };
                        let socket = match UdpSocket::bind(bind).await {
                            Ok(s) => s,
                            Err(_) => continue,
                        };
                        if socket.connect(dst).await.is_err() {
                            continue;
                        }
                        let socket = Arc::new(socket);
                        let recv_task = spawn_downlink(conn.clone(), flow_id, socket.clone());
                        sessions.insert(
                            flow_id,
                            ServerSession { socket: socket.clone(), recv_task, last_activity: now },
                        );
                        socket
                    }
                };
                let _ = socket.send(payload).await;
            }
            _ = sweep.tick() => {
                let now = start.elapsed().as_secs();
                let expired = expired_flow_ids(
                    sessions.iter().map(|(k, s)| (*k, s.last_activity)),
                    now,
                    idle_secs,
                );
                for id in expired {
                    if let Some(s) = sessions.remove(&id) {
                        s.recv_task.abort();
                    }
                }
            }
        }
    }
    // 连接结束：清理所有回程 task。
    for (_, s) in sessions {
        s.recv_task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shared::TargetAddr;

    #[test]
    fn uplink_roundtrips_ipv4_target() {
        let t = TargetAddr::IpPort("1.2.3.4:443".parse().unwrap());
        let buf = encode_uplink(7, &t, b"hello");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 7);
        assert_eq!(target, t);
        assert_eq!(payload, b"hello");
    }

    #[test]
    fn uplink_roundtrips_domain_target() {
        let t = TargetAddr::DomainPort {
            host: "facebook.com".into(),
            port: 443,
        };
        let buf = encode_uplink(9, &t, b"q");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 9);
        assert_eq!(target, t);
        assert_eq!(payload, &b"q"[..]);
    }

    #[test]
    fn uplink_roundtrips_ipv6_target() {
        let t = TargetAddr::IpPort("[2001:db8::1]:53".parse().unwrap());
        let buf = encode_uplink(1, &t, b"x");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 1);
        assert_eq!(target, t);
        assert_eq!(payload, &b"x"[..]);
    }

    #[test]
    fn uplink_empty_payload_is_ok() {
        let t = TargetAddr::IpPort("9.9.9.9:443".parse().unwrap());
        let buf = encode_uplink(3, &t, b"");
        let (fid, target, payload) = decode_uplink(&buf).unwrap();
        assert_eq!(fid, 3);
        assert_eq!(target, t);
        assert!(payload.is_empty());
    }

    #[test]
    fn downlink_roundtrips() {
        let buf = encode_downlink(42, b"resp");
        assert_eq!(decode_downlink(&buf).unwrap(), (42, &b"resp"[..]));
    }

    #[test]
    fn decode_rejects_truncated_without_panic() {
        assert!(decode_uplink(&[0u8; 3]).is_none()); // < flow-id+atyp
        assert!(decode_uplink(&[0, 0, 0, 1, ATYP_IPV4, 1, 2]).is_none()); // ipv4 addr/port short
        assert!(decode_uplink(&[0, 0, 0, 1, ATYP_DOMAIN, 200, b'a']).is_none()); // domain len overruns
        assert!(decode_uplink(&[0, 0, 0, 1, 99]).is_none()); // unknown ATYP
        assert!(decode_downlink(&[0u8; 2]).is_none());
    }

    fn tuple(p: u16) -> FourTuple {
        FourTuple {
            src_ip: Ipv4Addr::new(10, 0, 0, 1),
            src_port: p,
            dst_ip: Ipv4Addr::new(198, 18, 0, 5),
            dst_port: 443,
        }
    }

    #[test]
    fn intern_is_stable_per_tuple_and_unique_across() {
        let mut t = FlowTable::new();
        let a = t.intern(tuple(1000));
        assert_eq!(a, t.intern(tuple(1000)), "same tuple reuses flow-id");
        assert_ne!(a, t.intern(tuple(1001)), "different tuple new flow-id");
    }

    #[test]
    fn resolve_returns_entry_and_endpoints() {
        let mut t = FlowTable::new();
        let id = t.intern(tuple(1000));
        let e = t.resolve(id).expect("entry present");
        assert_eq!(e.app_endpoint(), (Ipv4Addr::new(10, 0, 0, 1), 1000));
        assert_eq!(e.target_src(), (Ipv4Addr::new(198, 18, 0, 5), 443));
    }

    #[test]
    fn sweep_reclaims_idle_flows() {
        let mut t = FlowTable::new();
        let id = t.intern(tuple(1000)); // last_activity = 0
        assert!(t.resolve(id).is_some());
        t.sweep(61, 60); // 61 - 0 > 60
        assert!(t.resolve(id).is_none(), "idle > 60s reclaimed");
        // 反向表也清掉：同四元组应铸新 id。
        assert_ne!(t.intern(tuple(1000)), id);
    }

    #[test]
    fn touch_keeps_flow_alive() {
        let mut t = FlowTable::new();
        let id = t.intern(tuple(1000)); // t=0
        t.touch(id, 50);
        t.sweep(100, 60); // 100 - 50 < 60
        assert!(t.resolve(id).is_some());
    }

    #[test]
    fn lru_evicts_oldest_beyond_cap() {
        let mut t = FlowTable::with_cap(2);
        let a = t.intern(tuple(1));
        let _b = t.intern(tuple(2));
        let _c = t.intern(tuple(3)); // evicts oldest (a)
        assert!(t.resolve(a).is_none());
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn build_then_parse_roundtrips_with_checksums() {
        let src = (Ipv4Addr::new(198, 18, 0, 5), 443);
        let dst = (Ipv4Addr::new(10, 0, 0, 1), 51000);
        let pkt = build_udp_ip_packet(src, dst, b"payload");
        let got = parse_inbound_udp(&pkt).expect("parses back");
        assert_eq!(got.src_ip, src.0);
        assert_eq!(got.src_port, src.1);
        assert_eq!(got.dst_ip, dst.0);
        assert_eq!(got.dst_port, dst.1);
        assert_eq!(got.payload, b"payload");
    }

    #[test]
    fn build_empty_payload_roundtrips() {
        let src = (Ipv4Addr::new(1, 1, 1, 1), 53);
        let dst = (Ipv4Addr::new(10, 0, 0, 1), 40000);
        let pkt = build_udp_ip_packet(src, dst, b"");
        let got = parse_inbound_udp(&pkt).expect("parses back");
        assert!(got.payload.is_empty());
        assert_eq!(got.dst_port, 40000);
    }

    #[test]
    fn parse_rejects_garbage_and_tcp() {
        assert!(parse_inbound_udp(&[0u8; 4]).is_none());
        // 一个 IPv4+TCP 包能解析出 IP/Transport，但不是 UDP → None。
        let mut tcp = Vec::new();
        etherparse::PacketBuilder::ipv4([10, 0, 0, 1], [1, 1, 1, 1], 64)
            .tcp(50000, 443, 0, 1024)
            .write(&mut tcp, &[])
            .unwrap();
        assert!(parse_inbound_udp(&tcp).is_none());
    }

    #[test]
    fn expired_flow_ids_selects_only_idle() {
        // flow 1 last_seen=0, flow 2 last_seen=50；now=61, idle=60 → 只有 1 过期。
        let entries = vec![(1u32, 0u64), (2u32, 50u64)];
        let mut got = expired_flow_ids(entries, 61, 60);
        got.sort_unstable();
        assert_eq!(got, vec![1]);
    }

    #[test]
    fn expired_flow_ids_none_when_all_fresh() {
        let entries = vec![(1u32, 30u64), (2u32, 40u64)];
        assert!(expired_flow_ids(entries, 50, 60).is_empty());
    }
}
