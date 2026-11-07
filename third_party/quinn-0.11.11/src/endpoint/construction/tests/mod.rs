use super::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Weak,
};
use std::time::Duration;
use tokio::time::timeout;

#[derive(Debug)]
struct SnapshotSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    owner: Mutex<Option<Weak<EndpointInner>>>,
    calls: AtomicUsize,
    locked: AtomicBool,
    proto_before: AtomicUsize,
    host_before: AtomicUsize,
    action: AtomicUsize,
    endpoint_hook: Mutex<Option<Endpoint>>,
    payload: Arc<()>,
}
impl AsyncUdpSocket for SnapshotSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn crate::UdpPoller>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if let Some(owner) = self.owner.lock().unwrap().as_ref().and_then(Weak::upgrade) {
            match owner.state.try_lock() {
                Ok(state) => {
                    self.proto_before
                        .store(state.inner.open_connections(), Ordering::Relaxed);
                    self.host_before.store(
                        state.recv_state.connections.catalog.len(),
                        Ordering::Relaxed,
                    );
                }
                Err(_) => self.locked.store(true, Ordering::Relaxed),
            }
        }
        match self.action.load(Ordering::Relaxed) {
            1 => self
                .endpoint_hook
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .close(9u32.into(), b"stop while preparing"),
            2 => std::panic::panic_any(self.payload.clone()),
            3 => self
                .endpoint_hook
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                .unwrap(),
            _ => {}
        }
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, transmit: &crate::udp::Transmit<'_>) -> io::Result<()> {
        self.inner.try_send(transmit)
    }
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [crate::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        self.inner.poll_recv(cx, bufs, meta)
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn max_transmit_segments(&self) -> usize {
        self.inner.max_transmit_segments()
    }
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }
}

#[cfg(any(feature = "rustls-ring", feature = "rustls-aws-lc-rs"))]
#[tokio::test(flavor = "current_thread")]
async fn connection_poller_is_prepared_without_root_lock_or_published_registration() {
    let runtime = crate::default_runtime().unwrap();
    let (socket, mut endpoint, budget, remote) = fixture(runtime);
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let observed = (
        socket.calls.load(Ordering::Relaxed),
        socket.locked.load(Ordering::Relaxed),
        socket.proto_before.load(Ordering::Relaxed),
        socket.host_before.load(Ordering::Relaxed),
    );
    let drain = connecting.take_drain_handle().unwrap();
    drain.abandon();
    drop(connecting);
    assert!(timeout(Duration::from_secs(2), drain.drained())
        .await
        .unwrap());
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(
        observed,
        (1, false, 0, 0),
        "external poller preparation must precede both registries and run outside Root lock"
    );
}

fn fixture(
    runtime: Arc<dyn Runtime>,
) -> (
    Arc<SnapshotSocket>,
    Endpoint,
    bytes::BackingBudget,
    std::net::UdpSocket,
) {
    let socket = Arc::new(SnapshotSocket {
        inner: runtime
            .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap(),
        owner: Mutex::new(None),
        calls: AtomicUsize::new(0),
        locked: AtomicBool::new(false),
        proto_before: AtomicUsize::new(usize::MAX),
        host_before: AtomicUsize::new(usize::MAX),
        action: AtomicUsize::new(0),
        endpoint_hook: Mutex::new(None),
        payload: Arc::new(()),
    });
    let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
    let mut endpoint = Endpoint::new_with_metered_abstract_socket(
        EndpointConfig::default(),
        None,
        socket.clone(),
        runtime,
        BackingMeter::with_budget(&budget),
    )
    .unwrap();
    *socket.owner.lock().unwrap() = Some(endpoint.inner.self_weak.clone());
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    endpoint
        .set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots)).unwrap());
    let remote = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    (socket, endpoint, budget, remote)
}

mod lifecycle;
mod previous_socket;

#[cfg(any(feature = "rustls-ring", feature = "rustls-aws-lc-rs"))]
mod fanout;
mod response;

#[cfg(any(feature = "rustls-ring", feature = "rustls-aws-lc-rs"))]
mod rejection;
