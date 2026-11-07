//! Actual loopback QUIC with the original peer's outbound IO held at WouldBlock.
//! No successful send is fabricated: releasing the gate wakes the real socket pollers.
use super::*;
use crate::tuic::{NodeHost, NodeStartup};
use quinn::{AsyncUdpSocket, Runtime, UdpPoller};
use std::{
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    task::{Context, Poll},
};
#[derive(Debug)]
struct Hold {
    socket: Arc<dyn AsyncUdpSocket>,
    held: AtomicBool,
    blocked: AtomicUsize,
    blocked_ready: AtomicUsize,
    next: AtomicUsize,
    wakes: [std::sync::Mutex<Option<std::task::Waker>>; 4],
}
impl Hold {
    fn release(&self) {
        self.held.store(false, Ordering::SeqCst);
        for wake in &self.wakes {
            let original = wake.lock().unwrap().take();
            if let Some(original) = original {
                original.wake();
            }
        }
    }
}
#[derive(Debug)]
struct Writable {
    socket: Arc<Hold>,
    original: Pin<Box<dyn UdpPoller>>,
    slot: usize,
}
impl UdpPoller for Writable {
    fn poll_writable(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        let previous = self.socket.wakes[self.slot]
            .lock()
            .unwrap()
            .replace(cx.waker().clone());
        drop(previous);
        if self.socket.held.load(Ordering::SeqCst) {
            self.socket.blocked_ready.fetch_add(1, Ordering::SeqCst);
            return Poll::Pending;
        }
        self.original.as_mut().poll_writable(cx)
    }
}
impl AsyncUdpSocket for Hold {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
        let slot = self.next.fetch_add(1, Ordering::SeqCst);
        assert!(
            slot < self.wakes.len(),
            "one actual peer/endpoint has bounded socket pollers"
        );
        Box::pin(Writable {
            original: self.socket.clone().create_io_poller(),
            socket: self,
            slot,
        })
    }
    fn try_send(&self, transmit: &quinn::udp::Transmit) -> std::io::Result<()> {
        if self.held.load(Ordering::SeqCst) {
            self.blocked.fetch_add(1, Ordering::SeqCst);
            return Err(std::io::ErrorKind::WouldBlock.into());
        }
        self.socket.try_send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut Context<'_>,
        bufs: &mut [std::io::IoSliceMut<'_>],
        meta: &mut [quinn::udp::RecvMeta],
    ) -> Poll<std::io::Result<usize>> {
        self.socket.poll_recv(cx, bufs, meta)
    }
    fn local_addr(&self) -> std::io::Result<std::net::SocketAddr> {
        self.socket.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.socket.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.socket.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.socket.may_fragment()
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_uplink_held_ack_keeps_table_at_original_deadline() {
    let identity = PublicCa::new();
    let socket = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let held = Arc::new(Hold {
        socket: quinn::TokioRuntime.wrap_udp_socket(socket).unwrap(),
        held: AtomicBool::new(false),
        blocked: AtomicUsize::new(0),
        blocked_ready: AtomicUsize::new(0),
        next: AtomicUsize::new(0),
        wakes: std::array::from_fn(|_| std::sync::Mutex::new(None)),
    });
    let server = quinn::Endpoint::new_with_abstract_socket(
        quinn::EndpointConfig::default(),
        Some(identity.server_config()),
        held.clone(),
        Arc::new(quinn::TokioRuntime),
    )
    .unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    cfg.udp_relay_mode = "quic".into();
    assert_eq!(cfg.tcp_pool, 1);
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    let observer = host.native_observer_for_test();
    let (started, peer) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
        tokio::join!(host.start(), async {
            let peer = server.accept().await.unwrap().await.unwrap();
            auth(&peer, &cfg).await;
            peer
        })
    })
    .await
    .unwrap();
    assert_eq!(started, NodeStartup::Ready);
    let data = host.data().unwrap();
    let result = capture(async {
        held.held.store(true, Ordering::SeqCst);
        let packet = crate::tuic::encode_packet(
            7,
            &crate::shared::TargetAddr::IpPort("127.0.0.1:9000".parse().unwrap()),
            &[42; 128],
        );
        data.send_udp(packet.clone()).await;
        let actual = tokio::time::timeout(Duration::from_secs(2), async {
            peer.accept_uni()
                .await
                .unwrap()
                .read_to_end(65535)
                .await
                .unwrap()
        })
        .await
        .unwrap();
        assert_eq!(actual, packet);
        tokio::time::pause();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
        host.uplink_deadline_for_test(deadline);
        let mut retirement = std::pin::pin!(host.retire_live_uplink_for_test());
        assert!(
            std::future::poll_fn(|cx| Poll::Ready(retirement.as_mut().poll(cx)))
                .await
                .is_pending()
        );
        let before = observer.uplink_inventory_for_test().unwrap();
        assert!(before.2 && before.3 == 1 && !budget.quarantined());
        tokio::time::advance(Duration::from_secs(2)).await;
        let _ = std::future::poll_fn(|cx| Poll::Ready(retirement.as_mut().poll(cx))).await;
        assert!(
            held.blocked.load(Ordering::SeqCst) + held.blocked_ready.load(Ordering::SeqCst) > 0
        );
        assert!(
            observer.facts().2 && budget.quarantined(),
            "actual ACK debt cannot wait past the original deadline without quiet Q"
        );
        assert_eq!(observer.uplink_inventory_for_test(), Some(before));
        assert!(budget.used() >= before.1);
        assert_eq!(observer.uplink_deadline_for_test(), Some(deadline));
        assert!(observer.facts().0.is_none());
    })
    .await;
    tokio::time::resume();
    held.release();
    drop((data, host, observer));
    peer.close(0u32.into(), b"held ACK complete");
    server.close(0u32.into(), b"held ACK complete");
    tokio::time::timeout(Duration::from_secs(3), server.wait_idle())
        .await
        .unwrap();
    result.unwrap_or_else(|p| std::panic::resume_unwind(p));
}
