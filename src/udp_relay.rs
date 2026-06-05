//! UDP relay over the QUIC datagram data plane (Stage 12).
//!
//! 中文要点：本模块是 UDP relay 的纯逻辑收口——QUIC datagram 的线格式编解码、flow 表、
//! 以及裸 IP/UDP 包的造/解。放在 lib crate 里，让 server/client 两个 binary 模块和
//! `tests/` 集成测试都能复用。承重决策见 docs/adr/0003 与 stage-12 spec。

use crate::shared::TargetAddr;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

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
    tuple_to_id: std::collections::HashMap<FourTuple, u32>,
    id_to_entry: std::collections::HashMap<u32, FlowEntry>,
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
            tuple_to_id: std::collections::HashMap::new(),
            id_to_entry: std::collections::HashMap::new(),
            next_id: 1,
            cap: cap.max(1),
        }
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
}
