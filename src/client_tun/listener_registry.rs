//! Bounded per-port smoltcp listener registry.
//!
//! This module owns listener startup/configuration, TCP raw-buffer inputs, and listener-slot
//! allocation/registration, and borrowed inbound dirty hints. Flow lifecycle, relay custody,
//! and TUN scheduling remain with its parent. Slot counts stay bounded by the port and global caps;
//! Legacy dirty hints read actual tuples; F uses validated Interface input effects.
//! Neither path owns a second connection index. `SocketCtx`
//! registration happens atomically with each socket allocation. Entry points are
//! `TunListenerConfig`, `ListenerRegistry`, and the listener socket builders.
//! Capacity tests live in `tests/compatibility_listener_capacity.rs`; real tuple tests in
//! `listener_registry/tests/inbound.rs`. `prepare_inbound_tcp` preserves prior dirty work.

use super::{
    ClientError, DEFAULT_TUN_POOL_SIZE, MAX_TCP_SOCKET_BUFFER_BYTES, MIN_TCP_SOCKET_BUFFER_BYTES,
    SocketCtx, TCP_SOCKET_BUFFER_SIZE, trace_enabled,
};
use smoltcp::iface::{SocketHandle, SocketSet};
use smoltcp::socket::tcp::{
    Socket as TcpSocket, SocketBuffer as TcpSocketBuffer, State as TcpState,
};
use smoltcp::wire::{IpEndpoint, Ipv4Address};
use std::collections::{HashMap, HashSet};

#[cfg(test)]
pub(super) use tests::inspect_inbound_tcp;

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
pub struct TunListenerConfig {
    /// Per-port pool size: number of smoltcp listener slots created for each
    /// intercepted destination port.
    /// 中文要点：Stage 9 起 pool 按"每端口"算，决定单个端口能并发承接多少条连接。
    pub pool_size: usize,
}

impl TunListenerConfig {
    /// Build listener config from optional string sources.
    /// 中文要点：Stage 9 起本地不再固定监听端口，端口由 SYN inspector 按需注册，
    /// 这里只保留 `pool_size` 一个旋钮。
    pub(super) fn from_sources(pool_size: Option<&str>) -> Result<Self, ClientError> {
        let pool_size = match pool_size {
            Some(value) => value
                .parse::<usize>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid pool size: {value}")))?,
            None => DEFAULT_TUN_POOL_SIZE,
        };

        if pool_size == 0 {
            return Err(ClientError::InvalidTarget(
                "invalid pool size: must be at least 1".to_string(),
            ));
        }

        Ok(Self { pool_size })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TcpSocketBufferConfig {
    pub(super) rx_bytes: usize,
    pub(super) tx_bytes: usize,
    pub(super) receive_window_limit: Option<usize>,
}

impl Default for TcpSocketBufferConfig {
    fn default() -> Self {
        Self {
            rx_bytes: TCP_SOCKET_BUFFER_SIZE,
            tx_bytes: TCP_SOCKET_BUFFER_SIZE,
            receive_window_limit: None,
        }
    }
}

pub(super) fn parse_tcp_socket_buffer_bytes(s: Option<&str>, default: usize) -> usize {
    s.and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|value| (MIN_TCP_SOCKET_BUFFER_BYTES..=MAX_TCP_SOCKET_BUFFER_BYTES).contains(value))
        .unwrap_or(default)
}

pub(super) fn parse_tcp_socket_buffer_config(
    rx: Option<&str>,
    tx: Option<&str>,
) -> TcpSocketBufferConfig {
    let default = TcpSocketBufferConfig::default();
    TcpSocketBufferConfig {
        rx_bytes: parse_tcp_socket_buffer_bytes(rx, default.rx_bytes),
        tx_bytes: parse_tcp_socket_buffer_bytes(tx, default.tx_bytes),
        receive_window_limit: None,
    }
}

/// Private listener-capacity profile selection at the startup admission seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ListenerCapacityProfile {
    Legacy,
    F4096,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ListenerCapacityRejection {
    RxOverBudget,
    TxOverBudget,
    CombinedOverBudget,
}

impl ListenerCapacityProfile {
    /// Validate the effective raw listener buffers and select this profile's cap.
    pub(super) fn startup_max_total(
        self,
        socket_buffers: TcpSocketBufferConfig,
    ) -> Result<usize, ListenerCapacityRejection> {
        match self {
            Self::Legacy => Ok(MAX_TOTAL_LISTENERS),
            Self::F4096 => {
                const F4096_MAX_BUFFER_BYTES: usize = 65_535;
                const F4096_MAX_COMBINED_BUFFER_BYTES: usize = 131_070;

                if socket_buffers.rx_bytes > F4096_MAX_BUFFER_BYTES {
                    return Err(ListenerCapacityRejection::RxOverBudget);
                }
                if socket_buffers.tx_bytes > F4096_MAX_BUFFER_BYTES {
                    return Err(ListenerCapacityRejection::TxOverBudget);
                }
                if socket_buffers
                    .rx_bytes
                    .checked_add(socket_buffers.tx_bytes)
                    .is_none_or(|total| total > F4096_MAX_COMBINED_BUFFER_BYTES)
                {
                    return Err(ListenerCapacityRejection::CombinedOverBudget);
                }
                Ok(4352)
            }
        }
    }
}

/// One listener socket's binding parameters.
/// 中文要点：Stage 9 起 pool_size 已上移到 `ListenerRegistry`，这里只剩端口。
#[derive(Debug, Clone, Copy)]
pub(super) struct ListenerSpec {
    /// Local TCP port intercepted on the TUN-side smoltcp stack.
    pub(super) local_port: u16,
}

/// Hard cap on the number of distinct destination ports we will intercept.
/// 中文要点：防止 SYN flood 下 socket / 缓冲区无限增长，到顶就拒新端口。
pub(super) const MAX_INTERCEPTED_PORTS: usize = 64;

/// 全局 listener socket 总数上限（#2 弹性扩容的兜底）。
/// 中文要点：放开了「每端口 pool_size 固定上限」后，仍需一个全局闸防 SYN flood 把内存撑爆。
/// 4096 槽 × 128KB ≈ 512MB 上界；实际按需扩容远小于此。
pub(super) const MAX_TOTAL_LISTENERS: usize = 4096;

/// SYN 命中时为该端口保证的空闲 listening 槽数（#2 弹性扩容触发阈值）。
/// 中文要点：每个新 SYN 到来前确保该端口恒有 ≥2 个空闲 listening 槽，吸收突发并发，
/// 避免「所有槽都 Relaying 时新 SYN 无 socket 可握手 → SYN 退避重传 stall」。
pub(super) const MIN_SPARE_LISTENERS: usize = 2;

/// Failure mode for `ListenerRegistry::ensure_port`.
/// 中文要点：到顶时优雅拒绝，不能 panic，已注册端口的 socket 不受影响。
#[derive(Debug, PartialEq, Eq)]
pub(super) enum RegistryError {
    Capped,
}

/// Dynamic per-port listener pools.
/// 中文要点：一个目的端口对应一组（`pool_size` 个）smoltcp 监听 socket，
/// 由 SYN inspector 按需创建；主循环遍历所有 handle 处理首包。
#[derive(Debug)]
pub(super) struct ListenerRegistry {
    ports: HashMap<u16, Vec<SocketHandle>>,
    pool_size: usize,
    socket_buffers: TcpSocketBufferConfig,
    /// 全局 listener socket 总数上限（#2 弹性扩容兜底；默认 [`MAX_TOTAL_LISTENERS`]）。
    max_total: usize,
    /// 当前 listener socket 总数（review #6：O(1) 计数器，避免每 SYN 在扩容循环里 O(端口) 求和）。
    /// 中文要点：槽只增不删（rearm/reap 复用回 Listen，从不从 ports 移除），故计数器随建槽单调累加。
    total: usize,
}

impl ListenerRegistry {
    #[cfg(test)]
    pub(super) fn new(pool_size: usize) -> Self {
        Self::with_socket_buffers(pool_size, TcpSocketBufferConfig::default())
    }

    pub(super) fn with_socket_buffers(
        pool_size: usize,
        socket_buffers: TcpSocketBufferConfig,
    ) -> Self {
        Self::with_socket_buffers_and_max_total(pool_size, socket_buffers, MAX_TOTAL_LISTENERS)
    }

    pub(super) fn with_socket_buffers_and_max_total(
        pool_size: usize,
        socket_buffers: TcpSocketBufferConfig,
        max_total: usize,
    ) -> Self {
        Self {
            ports: HashMap::new(),
            pool_size,
            socket_buffers,
            max_total,
            total: 0,
        }
    }

    #[cfg(test)]
    pub(super) fn with_max_total(pool_size: usize, max_total: usize) -> Self {
        Self::with_socket_buffers_and_max_total(
            pool_size,
            TcpSocketBufferConfig::default(),
            max_total,
        )
    }

    /// 当前所有端口的 listener socket 总数（O(1)，维护计数器）。
    pub(super) fn total_handles(&self) -> usize {
        self.total
    }

    fn checked_total_after(&self, additional: usize) -> Result<usize, RegistryError> {
        self.total
            .checked_add(additional)
            .filter(|total| *total <= self.max_total)
            .ok_or(RegistryError::Capped)
    }

    #[cfg(test)]
    pub(super) fn port_count(&self) -> usize {
        self.ports.len()
    }

    /// Idempotently ensure a listener pool exists for `port`.
    /// 中文要点：端口在册时不重复建；首次建则一次性创建 `pool_size` 个监听槽位
    /// 并同步登记 `SocketCtx`，到顶返回 `Capped`。
    pub(super) fn ensure_port(
        &mut self,
        port: u16,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if self.ports.contains_key(&port) {
            return Ok(());
        }
        if self.ports.len() >= MAX_INTERCEPTED_PORTS {
            return Err(RegistryError::Capped);
        }
        let total = self.checked_total_after(self.pool_size)?;
        let spec = ListenerSpec { local_port: port };
        let mut handles = Vec::with_capacity(self.pool_size);
        for _ in 0..self.pool_size {
            let h = sockets.add(build_listener_socket_with_buffers(
                &spec,
                self.socket_buffers,
            ));
            socket_ctxs.insert(h, SocketCtx::new(port));
            handles.push(h);
        }
        self.total = total;
        trace_log!(
            "🆕 listener pool created for port {port} (pool_size={})",
            self.pool_size
        );
        self.ports.insert(port, handles);
        Ok(())
    }

    /// Iterate every smoltcp handle across all currently intercepted ports.
    /// 中文要点：脏集合驱动后热路径不再用它；仅低频「死槽回收」(reap_dead_slots) + 测试遍历。
    pub(super) fn all_handles(&self) -> impl Iterator<Item = SocketHandle> + '_ {
        self.ports.values().flatten().copied()
    }

    /// All listener handles registered for `port`（未注册端口返回空 slice）。
    /// Inbound hints first restrict candidates to this port, then select actual
    /// socket tuples and Listen slots without scanning other port pools.
    pub(super) fn handles_for_port(&self, port: u16) -> &[SocketHandle] {
        self.ports.get(&port).map(Vec::as_slice).unwrap_or(&[])
    }

    /// #2 弹性扩容：保证 `port` 当前至少有 `min_spare` 个 Listening 槽，不足则按需补建。
    ///
    /// 中文要点：这是放开「每端口 pool_size 固定上限」的核心——热门端口（如 :443）突发时，
    /// 已有槽都进了 Relaying，新 SYN 没有 listening socket 可握手就会 stall。每个 SYN 到来前
    /// 补足空闲槽即可吸收突发。rearm 回 Listening 的旧槽计入空闲、优先复用，不无限增长；
    /// 全局 `max_total` 兜底防 SYN flood，到顶返回 `Capped`（退回旧行为，不 panic）。
    /// 未注册端口（无 SYN 命中过）→ no-op，建池仍由 `ensure_port` 负责。
    pub(super) fn ensure_spare_listeners(
        &mut self,
        port: u16,
        min_spare: usize,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> Result<(), RegistryError> {
        if !self.ports.contains_key(&port) {
            return Ok(());
        }
        // 「空闲」必须看 smoltcp socket 的真实状态（accept 后立即离开 Listen），不能看
        // SocketCtx.state——后者直到 process 首包才更新，SYN 刚被 accept 时仍是 Listening，
        // 计数虚高会导致永不补建（实测单端口仍 stall）。
        let listening = self.ports[&port]
            .iter()
            .filter(|h| sockets.get::<TcpSocket>(**h).state() == TcpState::Listen)
            .count();
        if listening >= min_spare {
            return Ok(());
        }
        let additional = min_spare - listening;
        let total = self.checked_total_after(additional)?;
        let spec = ListenerSpec { local_port: port };
        for _ in 0..additional {
            let h = sockets.add(build_listener_socket_with_buffers(
                &spec,
                self.socket_buffers,
            ));
            socket_ctxs.insert(h, SocketCtx::new(port));
            self.ports.get_mut(&port).unwrap().push(h);
        }
        self.total = total;
        Ok(())
    }

    fn prepare_listener_capacity(
        &mut self,
        port: u16,
        is_clean_syn: bool,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) {
        if is_clean_syn {
            if let Err(e) = self.ensure_port(port, sockets, socket_ctxs) {
                println!(
                    "⚠️ intercepted port cap reached, drop SYN to port {port}: {:?}",
                    e
                );
            }
            if let Err(e) =
                self.ensure_spare_listeners(port, MIN_SPARE_LISTENERS, sockets, socket_ctxs)
            {
                println!(
                    "⚠️ global listener cap reached, 端口 {port} 无法弹性扩容: {:?}",
                    e
                );
            }
        }
    }

    /// Prepare capacity only. Actual validated input effects own F dirty selection.
    pub(super) fn prepare_inbound_tcp_capacity(
        &mut self,
        packet: Option<&[u8]>,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
    ) -> InboundTcpCoverage {
        let Some(inbound) = packet.and_then(parse_inbound_tcp) else {
            return InboundTcpCoverage::FullFallback;
        };
        self.prepare_listener_capacity(inbound.port, inbound.is_clean_syn, sockets, socket_ctxs);
        if inbound.tuple.is_some() {
            InboundTcpCoverage::CompleteTcp
        } else {
            InboundTcpCoverage::FullFallback
        }
    }

    pub(super) fn prepare_inbound_tcp(
        &mut self,
        packet: Option<&[u8]>,
        sockets: &mut SocketSet<'static>,
        socket_ctxs: &mut HashMap<SocketHandle, SocketCtx>,
        dirty: &mut HashSet<SocketHandle>,
    ) -> InboundTcpCoverage {
        let mut covered = false;
        // Parse once before iface.poll. Hints cannot replace smoltcp validation.
        if let Some(buf) = packet
            && let Some(InboundTcp {
                port,
                is_clean_syn,
                tuple,
                ..
            }) = parse_inbound_tcp(buf)
        {
            self.prepare_listener_capacity(port, is_clean_syn, sockets, socket_ctxs);
            for &h in self.handles_for_port(port) {
                let selected = tuple.is_none_or(|(local, remote)| {
                    let socket = sockets.get::<TcpSocket>(h);
                    // Several SYNs may stage before poll; any Listen slot can accept.
                    // TimeWait and retained closed tuples also need their actual hint.
                    socket.state() == TcpState::Listen
                        || (socket.local_endpoint() == Some(local)
                            && socket.remote_endpoint() == Some(remote))
                });
                if selected {
                    dirty.insert(h);
                    covered |= tuple.is_some();
                }
            }
        }
        if covered {
            InboundTcpCoverage::CompleteTcp
        } else {
            InboundTcpCoverage::FullFallback
        }
    }
}

/// Allocate a fresh smoltcp TCP listener socket for one pool slot.
/// 中文要点：每次调用都创建一间独立房间，并立即挂上 listen 牌子。
#[cfg(test)]
pub(super) fn build_listener_socket(spec: &ListenerSpec) -> TcpSocket<'static> {
    build_listener_socket_with_buffers(spec, TcpSocketBufferConfig::default())
}

pub(super) fn build_listener_socket_with_buffers(
    spec: &ListenerSpec,
    buffers: TcpSocketBufferConfig,
) -> TcpSocket<'static> {
    let tcp_rx_buffer = TcpSocketBuffer::new(vec![0; buffers.rx_bytes]);
    let tcp_tx_buffer = TcpSocketBuffer::new(vec![0; buffers.tx_bytes]);
    let mut tcp_socket = TcpSocket::new(tcp_rx_buffer, tcp_tx_buffer);
    tcp_socket.set_receive_window_limit(buffers.receive_window_limit);
    configure_local_tcp_socket(&mut tcp_socket);
    tcp_socket.listen(spec.local_port).unwrap();
    tcp_socket
}

pub(super) fn configure_local_tcp_socket(socket: &mut TcpSocket<'_>) {
    socket.set_nagle_enabled(false);
    socket.set_ack_delay(None);
}

/// Coverage of this parsed packet only; prior staged input needs a separate proof.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum InboundTcpCoverage {
    CompleteTcp,
    FullFallback,
}

struct InboundTcp {
    port: u16,
    is_clean_syn: bool,
    tuple: Option<(IpEndpoint, IpEndpoint)>,
}

// IPv6 and fragmented IPv4 retain the old parsed-port fallback; this is not
// protocol support or packet acceptance. Non-TCP/parse failure remains inert.
fn parse_inbound_tcp(packet: &[u8]) -> Option<InboundTcp> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    let tuple = match parsed.ip? {
        etherparse::IpHeader::Version4(ip, _) if !ip.is_fragmenting_payload() => Some((
            IpEndpoint::new(
                Ipv4Address::from_bytes(&ip.destination).into(),
                tcp.destination_port,
            ),
            IpEndpoint::new(Ipv4Address::from_bytes(&ip.source).into(), tcp.source_port),
        )),
        _ => None,
    };
    Some(InboundTcp {
        port: tcp.destination_port,
        is_clean_syn: tcp.syn && !tcp.ack,
        tuple,
    })
}

#[cfg(test)]
mod tests;
