use super::*;
use smoltcp::iface::{Config, Interface, SocketHandle, SocketSet};
use smoltcp::socket::{tcp, udp};
use smoltcp::wire::{HardwareAddress, IpCidr, Ipv4Address};

mod native;
pub(super) use native::{hard_q_refusal, PublicCa, ServiceProfile};
#[path = "../../../../../tests/tuic_service/observe_window.rs"]
mod observe_window;
pub(super) use observe_window::{progress_window, window, ProgressElapsed, WindowElapsed};

// Explicit qualified Hard carrier for lifecycle fault fixtures. Legacy config()
// stays unchanged; policy refusal tests still supply their own rejected inputs.
pub(super) fn native_policy() -> Arc<crate::upstream::RelayFamilyPolicy> {
    Arc::new(crate::upstream::RelayFamilyPolicy::from_sources(crate::upstream::RelayFamilyPolicySources {
        tuic_native_chunk: Some("1"), ..Default::default()
    }))
}

pub(super) fn config(address: std::net::SocketAddr) -> TuicClientConfig {
    let mut cfg = TuicClientConfig::from_sources(
        Some(&address.to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some("certs/dev/ca-cert.pem"),
        Some("h3"),
    )
    .unwrap();
    cfg.tcp_pool = 1;
    cfg.quic_stats_secs = Some(1);
    cfg.with_transport_shape(native_shape())
}

/// A2 Route 2 test shape: small honest windows so the protocol tier fits the
/// Service tests' budgets. Production windows are unchanged.
pub(super) fn native_shape() -> crate::quic::honest_bound::TransportShape {
    crate::quic::honest_bound::TransportShape {
        bidi_streams: 8,
        uni_streams: 64,
        stream_window: 256 * 1024,
        connection_window: 1024 * 1024,
        send_window: 1024 * 1024,
        datagram_rx: 64 * 1024,
        datagram_tx: 64 * 1024,
        crypto_buffer: 16 * 1024,
    }
}

/// Protocol-tier reserve the Service derives for `config()` (one connection,
/// two controlled-rebuild generations), computed by the production functions.
pub(super) fn native_reserve() -> usize {
    native_reserve_for(&native_shape())
}

pub(super) fn native_reserve_for(shape: &crate::quic::honest_bound::TransportShape) -> usize {
    use crate::quic::honest_bound::{node_bound, protocol_reserve, MeasuredFactors};
    let node = node_bound(shape, &MeasuredFactors::A2_MEASURED, 1).unwrap();
    usize::try_from(protocol_reserve(node, 2).unwrap()).unwrap()
}

/// Large-window shape for tests whose peer legitimately sends multi-MiB ahead
/// (wire gap past the reassembly cap, or bulk transfer under a fixed deadline).
pub(super) fn native_large_shape() -> crate::quic::honest_bound::TransportShape {
    crate::quic::honest_bound::TransportShape {
        stream_window: 8 * 1024 * 1024,
        connection_window: 8 * 1024 * 1024,
        ..native_shape()
    }
}

/// Hard budget whose app tier equals `app`: the historical app-side budget
/// keeps its exact meaning and the Node gets its own protocol tier.
pub(super) fn native_hard(app: usize) -> usize {
    app + native_reserve()
}

pub(super) async fn auth(peer: &quinn::Connection, cfg: &TuicClientConfig) {
    let mut recv = peer.accept_uni().await.unwrap();
    let actual = recv.read_to_end(64).await.unwrap();
    let mut token = [0; 32];
    peer.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes())
        .unwrap();
    assert_eq!(actual, encode_authenticate(&cfg.uuid, &token));
}

pub(super) async fn capture<F: std::future::Future>(work: F) -> std::thread::Result<F::Output> {
    let mut work = Box::pin(work);
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(std::task::Poll::Pending) => std::task::Poll::Pending,
            Ok(std::task::Poll::Ready(value)) => std::task::Poll::Ready(Ok(value)),
            Err(panic) => std::task::Poll::Ready(Err(panic)),
        }
    })
    .await
}

/// Copy the original first-error identity while its sole ledger guard is held.
/// The returned values can be compared after Root shutdown without borrowing
/// a temporary guard or replacing the original error object.
pub(super) fn first_failure_identity(
    owner: &crate::client_tun::compatibility_runtime::SessionControl,
) -> Option<(usize, String, Vec<u8>)> {
    let ledger = owner.ledger();
    let first = ledger.first_failure.as_ref()?;
    Some((
        first as *const _ as usize,
        format!("{:?}", first.facts()),
        first.text_bytes().to_vec(),
    ))
}

/// Observe the original parent stop clock after a scenario has already failed.
/// A missing deadline or charged Q is evidence, never a second test panic.
pub(super) async fn observe_failed_parent_stop<F: std::future::Future>(
    run: F,
    original_deadline: Option<tokio::time::Instant>,
) -> Option<F::Output> {
    match original_deadline {
        Some(deadline) => tokio::time::timeout_at(deadline, run).await.ok(),
        None => None,
    }
}

/// Shared Native failure observation: retain the first scenario failure while
/// reporting the original queue and parent phase after the same stop deadline.
pub(super) async fn observe_failed_native_stop<F: std::future::Future>(
    run: F,
    handle: &Handle,
    original_deadline: tokio::time::Instant,
    queue: Option<&crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue>,
) {
    let completed = observe_failed_parent_stop(run, Some(original_deadline))
        .await
        .is_some();
    eprintln!(
        "failed Native parent stop: completed={completed} phase={:?} queue_charged={:?}",
        handle.phase(),
        queue.map(|queue| !queue.physical_zero()),
    );
}

pub(super) struct Apps {
    device: LoopbackTunDevice,
    pub(super) iface: Interface,
    pub(super) sockets: SocketSet<'static>,
    pub(super) target: std::net::Ipv4Addr,
    pub(super) tcp: SocketHandle,
    pub(super) second_tcp: Option<SocketHandle>,
    pub(super) udp: SocketHandle,
    pub(super) sent_tcp: bool,
    pub(super) tcp_payload: &'static [u8],
    pub(super) sent_udp: bool,
    pub(super) tcp_reply: Vec<u8>,
    pub(super) second_reply: Vec<u8>,
    second_sent: bool,
    pub(super) udp_reply: Vec<u8>,
}

impl Apps {
    pub(super) fn restart(&mut self) {
        let socket = self.sockets.get_mut::<tcp::Socket>(self.tcp);
        socket.abort();
        socket
            .connect(
                self.iface.context(),
                (Ipv4Address::from(self.target), 10000),
                40002,
            )
            .unwrap();
        self.sent_tcp = false;
        self.sent_udp = false;
        self.tcp_reply.clear();
        self.udp_reply.clear();
    }

    pub(super) fn new(inbound: PacketLink, outbound: PacketLink) -> Self {
        Self::with_target(inbound, outbound, std::net::Ipv4Addr::new(192, 0, 2, 10))
    }
    pub(super) fn with_target(
        inbound: PacketLink,
        outbound: PacketLink,
        target: std::net::Ipv4Addr,
    ) -> Self {
        Self::from_device_with_target(LoopbackTunDevice::new(inbound, outbound), target)
    }

    pub(super) fn from_device_with_target(
        mut device: LoopbackTunDevice,
        target: std::net::Ipv4Addr,
    ) -> Self {
        let mut iface = Interface::new(
            Config::new(HardwareAddress::Ip),
            &mut device,
            smoltcp::time::Instant::now(),
        );
        let local = Ipv4Address::new(10, 0, 0, 1);
        iface.update_ip_addrs(|a| a.push(IpCidr::new(local.into(), 24)).unwrap());
        iface.routes_mut().add_default_ipv4_route(local).unwrap();
        let mut sockets = SocketSet::new(vec![]);
        let mut tcp = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        tcp.connect(iface.context(), (Ipv4Address::from(target), 10000), 40000)
            .unwrap();
        let tcp = sockets.add(tcp);
        let mut udp = udp::Socket::new(
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
            udp::PacketBuffer::new(vec![udp::PacketMetadata::EMPTY; 4], vec![0; 4096]),
        );
        udp.bind(40001).unwrap();
        let udp = sockets.add(udp);
        Self {
            device,
            iface,
            sockets,
            target,
            tcp,
            second_tcp: None,
            udp,
            sent_tcp: false,
            tcp_payload: b"service-tcp",
            sent_udp: false,
            tcp_reply: vec![],
            second_reply: vec![],
            second_sent: false,
            udp_reply: vec![],
        }
    }

    pub(super) fn enable_second_tcp(&mut self) {
        assert!(self.second_tcp.is_none());
        let mut tcp = tcp::Socket::new(
            tcp::SocketBuffer::new(vec![0; 4096]),
            tcp::SocketBuffer::new(vec![0; 4096]),
        );
        tcp.connect(
            self.iface.context(),
            (Ipv4Address::from(self.target), 10000),
            40004,
        )
        .unwrap();
        self.second_tcp = Some(self.sockets.add(tcp));
    }

    pub(super) async fn turn(&mut self) {
        self.turn_with_tcp_read_limit(usize::MAX).await;
    }

    pub(super) async fn turn_with_tcp_read_limit(&mut self, mut remaining: usize) {
        while self.device.try_recv_rx().unwrap() {
            assert!(self.device.stage_rx_for_iface());
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        let tcp = self.sockets.get_mut::<tcp::Socket>(self.tcp);
        if tcp.can_send() && !self.sent_tcp {
            tcp.send_slice(self.tcp_payload).unwrap();
            self.sent_tcp = true;
        }
        while tcp.can_recv() && remaining > 0 {
            let mut b = [0; 128];
            let limit = remaining.min(b.len());
            let n = tcp.recv_slice(&mut b[..limit]).unwrap();
            remaining -= n;
            self.tcp_reply.extend_from_slice(&b[..n]);
        }
        if let Some(handle) = self.second_tcp {
            let tcp = self.sockets.get_mut::<tcp::Socket>(handle);
            if tcp.can_send() && !self.second_sent {
                tcp.send_slice(b"service-two").unwrap();
                self.second_sent = true;
            }
            while tcp.can_recv() {
                let mut b = [0; 128];
                let n = tcp.recv_slice(&mut b).unwrap();
                self.second_reply.extend_from_slice(&b[..n]);
            }
        }
        let udp = self.sockets.get_mut::<udp::Socket>(self.udp);
        if !self.sent_udp {
            udp.send_slice(b"service-udp", (Ipv4Address::from(self.target), 10001))
                .unwrap();
            self.sent_udp = true;
        }
        while udp.can_recv() {
            let (b, _) = udp.recv().unwrap();
            self.udp_reply.extend_from_slice(b);
        }
        self.iface.poll(
            smoltcp::time::Instant::now(),
            &mut self.device,
            &mut self.sockets,
        );
        self.device.flush_tx().await.unwrap();
        // A ready-only caller must still poll the original Service and timer.
        tokio::task::yield_now().await;
    }
}
