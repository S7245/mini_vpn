//! The actual receive preparation must fund its single serial response buffer.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn native_receive_preparation_prepays_response_scratch_layout() {
    let runtime = crate::default_runtime().unwrap();
    let socket = runtime
        .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let catalog = Catalog::prepare(Some(&meter)).unwrap();
    let catalog_layout = budget.used();
    drop(catalog);
    assert_eq!(budget.used(), 0);
    let prepared =
        RecvState::prepare_buffer(&EndpointConfig::default(), &*socket, Some(&meter), true)
            .unwrap();
    let slot = prepared.buf.len() / BATCH_SIZE;
    let required = prepared.buf.len() + catalog_layout + slot - 2;
    let actual = budget.used();
    drop(prepared);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert_eq!(
        actual, required,
        "receive preparation must prepay the actual response scratch before driver publication"
    );
}

struct PausedRuntime {
    inner: Arc<dyn Runtime>,
    driver: Mutex<Option<Pin<Box<dyn Future<Output = ()> + Send>>>>,
}
impl std::fmt::Debug for PausedRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PausedRuntime")
    }
}
impl Runtime for PausedRuntime {
    fn new_timer(&self, at: Instant) -> Pin<Box<dyn crate::AsyncTimer>> {
        self.inner.new_timer(at)
    }
    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        self.inner.wrap_udp_socket(socket)
    }
    fn now(&self) -> Instant {
        self.inner.now()
    }
    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        assert!(self.driver.lock().unwrap().replace(future).is_none());
    }
}
#[derive(Debug)]
struct ResponseSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    action: usize,
    attempts: AtomicUsize,
    first_pointer: AtomicUsize,
    reused: AtomicBool,
    payload: Arc<()>,
    inputs: Mutex<VecDeque<proto::ConnectionId>>,
    responses: Mutex<Vec<(proto::ConnectionId, [u8; 16])>>,
}
impl AsyncUdpSocket for ResponseSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn crate::UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }
    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [crate::udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let result = self.inner.poll_recv(cx, bufs, meta);
        if let Poll::Ready(Ok(count)) = &result {
            for (buf, meta) in bufs.iter().zip(meta.iter()).take(*count) {
                let (decoded, _) = proto::PartialDecode::new(
                    BytesMut::from(&buf[..meta.len]),
                    &proto::FixedLengthConnectionIdParser::new(8),
                    &[1],
                    true,
                )
                .unwrap();
                self.inputs.lock().unwrap().push_back(*decoded.dst_cid());
            }
        }
        result
    }
    fn try_send(&self, transmit: &crate::udp::Transmit<'_>) -> io::Result<()> {
        assert!(transmit.contents.len() >= 21 && transmit.contents.len() < 1200);
        assert_eq!(transmit.contents[0] & 0xc0, 0x40);
        let token = transmit.contents[transmit.contents.len() - 16..]
            .try_into()
            .unwrap();
        let cid = self.inputs.lock().unwrap().pop_front().unwrap();
        self.responses.lock().unwrap().push((cid, token));
        let pointer = transmit.contents.as_ptr() as usize;
        if self.attempts.fetch_add(1, Ordering::AcqRel) == 0 {
            self.first_pointer.store(pointer, Ordering::Release);
        } else {
            self.reused.store(
                pointer == self.first_pointer.load(Ordering::Acquire),
                Ordering::Release,
            );
        }
        match self.action {
            1 => Err(io::Error::from(io::ErrorKind::WouldBlock)),
            2 => std::panic::panic_any(self.payload.clone()),
            _ => self.inner.try_send(transmit),
        }
    }
    fn max_receive_segments(&self) -> usize {
        1
    }
}

#[tokio::test(flavor = "current_thread")]
async fn real_initial_reset_uses_one_paid_scratch_and_send_panic_quarantines_without_relocking() {
    for (pacing, action) in [(false, 0), (true, 1), (false, 2), (true, 2)] {
        let runtime = Arc::new(PausedRuntime {
            inner: crate::default_runtime().unwrap(),
            driver: Mutex::new(None),
        });
        let socket = Arc::new(ResponseSocket {
            inner: runtime
                .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                .unwrap(),
            action,
            attempts: AtomicUsize::new(0),
            first_pointer: AtomicUsize::new(0),
            reused: AtomicBool::new(false),
            payload: Arc::new(()),
            inputs: Mutex::new(VecDeque::new()),
            responses: Mutex::new(Vec::new()),
        });
        let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
        let mut config = EndpointConfig::default();
        config.min_reset_interval(Duration::ZERO);
        if pacing {
            config.endpoint_pacing_service(Some(
                proto::EndpointPacingServiceConfig::endpoint_window_v1(),
            ));
        }
        let mut endpoint = Endpoint::new_with_metered_abstract_socket(
            config.clone(),
            None,
            socket.clone(),
            runtime.clone(),
            BackingMeter::with_budget(&budget),
        )
        .unwrap();
        let baseline = budget.used();
        let copy_layout = {
            let root = endpoint.inner.state.lock().unwrap();
            BytesMut::metered_copy_layout_for_len(root.recv_state.recv_buf.len() / BATCH_SIZE)
                .unwrap()
        };
        let initial_drain = endpoint.take_driver_drain_handle().unwrap();
        let initial_future = runtime.driver.lock().unwrap().take().unwrap();
        runtime.inner.spawn(initial_future);
        drop(endpoint);
        assert!(timeout(Duration::from_secs(2), initial_drain.drained())
            .await
            .unwrap());
        assert_eq!(budget.used(), 0);
        // The positive wire path has exactly all cold layouts plus one legal copy.
        let budget = bytes::BackingBudget::new(baseline + copy_layout).unwrap();
        let mut endpoint = Endpoint::new_with_metered_abstract_socket(
            config,
            None,
            socket.clone(),
            runtime.clone(),
            BackingMeter::with_budget(&budget),
        )
        .unwrap();
        assert_eq!(budget.used(), baseline);
        let mut peer = Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.cert.der().clone()).unwrap();
        peer.set_default_client_config(
            ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
        );
        let mut connecting = Vec::new();
        let mut original = false;
        let mut immediate_q = false;
        for iteration in 0..if action == 2 { 1 } else { 2 } {
            connecting.push(
                peer.connect(socket.local_addr().unwrap(), "localhost")
                    .unwrap(),
            );
            timeout(Duration::from_secs(2), async {
                loop {
                    // Hold the actual Root guard outside catch_unwind: it neither
                    // poisons nor reacquires, so only the receive scope can set Q.
                    let mut root = endpoint.inner.state.lock().unwrap();
                    let mut cx = Context::from_waker(std::task::Waker::noop());
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        root.drive_recv(&mut cx, runtime.now())
                    }));
                    immediate_q = budget.quarantined();
                    drop(root);
                    if let Err(payload) = result {
                        original = payload
                            .downcast_ref::<Arc<()>>()
                            .is_some_and(|p| Arc::ptr_eq(p, &socket.payload));
                        break;
                    }
                    if socket.attempts.load(Ordering::Acquire) > iteration {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
        }
        if action != 2 {
            assert_eq!(budget.used(), baseline);
            assert!(!immediate_q);
            assert!(socket.reused.load(Ordering::Acquire));
            let responses = socket.responses.lock().unwrap();
            assert_eq!(responses.len(), 2);
            assert_ne!(
                responses[0].0, responses[1].0,
                "distinct real Initial DCIDs"
            );
            assert_ne!(
                responses[0].1, responses[1].1,
                "second reset must contain its new DCID token, not stale scratch bytes"
            );
            if pacing {
                let snapshot = endpoint.endpoint_pacing_snapshot().unwrap();
                assert_eq!(snapshot.outstanding_bytes, 0);
                assert_eq!(snapshot.stateless_responses_dropped, 2);
            }
        }
        let drain = endpoint.take_driver_drain_handle().unwrap();
        let future = runtime.driver.lock().unwrap().take().unwrap();
        runtime.inner.spawn(future);
        drop((connecting, peer, endpoint, socket));
        let clean = timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap();
        if action == 2 {
            assert!(original && immediate_q, "actual send panic must retain first and Q before any Root recovery; pacing={pacing}");
            assert!(!clean);
            assert_eq!(budget.used(), 1);
        } else {
            assert!(clean);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_cold_response_full_rejects_before_spawn_and_cancel_releases_physical_layout() {
    let runtime = Arc::new(PausedRuntime {
        inner: crate::default_runtime().unwrap(),
        driver: Mutex::new(None),
    });
    let socket = runtime
        .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let budget = bytes::BackingBudget::new(4 * 1024 * 1024).unwrap();
    let mut endpoint = Endpoint::new_with_metered_abstract_socket(
        EndpointConfig::default(),
        None,
        socket.clone(),
        runtime.clone(),
        BackingMeter::with_budget(&budget),
    )
    .unwrap();
    let exact_cold = budget.used();
    let drain = endpoint.take_driver_drain_handle().unwrap();
    let future = runtime.driver.lock().unwrap().take().unwrap();
    runtime.inner.spawn(future);
    drop(endpoint);
    assert!(timeout(Duration::from_secs(2), drain.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 0);

    let full = bytes::BackingBudget::new(exact_cold).unwrap();
    let rejected = Endpoint::new_with_metered_abstract_socket(
        EndpointConfig::default(),
        None,
        socket,
        runtime.clone(),
        BackingMeter::with_budget(&full),
    );
    let was_rejected = rejected.is_err();
    if let Ok(mut accepted) = rejected {
        let drain = accepted.take_driver_drain_handle().unwrap();
        let future = runtime.driver.lock().unwrap().take().unwrap();
        runtime.inner.spawn(future);
        drop(accepted);
        assert!(timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap());
    }
    assert!(
        runtime.driver.lock().unwrap().is_none(),
        "no driver before cold admission"
    );
    assert_eq!(full.used(), 0);
    assert!(!full.quarantined());
    assert!(
        was_rejected,
        "all cold layouts consuming the budget leave no legal receive copy; reject before spawn"
    );
}
