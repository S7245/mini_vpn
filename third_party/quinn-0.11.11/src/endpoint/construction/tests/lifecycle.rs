use super::*;

#[tokio::test(flavor = "current_thread")]
async fn stop_and_socket_version_change_cancel_private_instances_before_publication() {
    for action in [1, 3] {
        let (socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
        *socket.endpoint_hook.lock().unwrap() = Some(endpoint.clone());
        socket.action.store(action, Ordering::Relaxed);
        let error = endpoint
            .connect(remote.local_addr().unwrap(), "localhost")
            .unwrap_err();
        if action == 1 {
            assert!(matches!(error, ConnectError::EndpointStopping));
        } else {
            assert!(matches!(error, ConnectError::EventCapacityUnavailable));
        }
        {
            let state = endpoint.inner.state.lock().unwrap();
            assert_eq!(state.inner.open_connections(), 0);
            assert_eq!(state.recv_state.connections.catalog.occupied_len(), 0);
            assert!(state.recv_state.connections.catalog.matches(&state.inner));
        }
        assert!(!budget.quarantined());
        let driver = endpoint.take_driver_drain_handle().unwrap();
        drop(endpoint);
        assert!(timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap());
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn opaque_poller_panic_preserves_payload_and_quarantines_unpublished_root_without_poison() {
    let (socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    socket.action.store(2, Ordering::Relaxed);
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = endpoint.connect(remote.local_addr().unwrap(), "localhost");
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        &panic.downcast::<Arc<()>>().unwrap(),
        &socket.payload
    ));
    assert!(budget.quarantined());
    {
        let state = endpoint.inner.state.lock().unwrap();
        assert_eq!(state.inner.open_connections(), 0);
        assert_eq!(state.recv_state.connections.catalog.len(), 0);
        assert_eq!(state.recv_state.connections.catalog.occupied_len(), 1);
    }
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(!timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 1);
}

#[derive(Debug)]
struct SpawnRuntime {
    inner: Arc<dyn Runtime>,
    mode: AtomicUsize,
    owner: Mutex<Option<Weak<EndpointInner>>>,
    observed: AtomicUsize,
    payload: Arc<()>,
}
impl Runtime for SpawnRuntime {
    fn new_timer(&self, at: Instant) -> Pin<Box<dyn crate::AsyncTimer>> {
        self.inner.new_timer(at)
    }
    fn wrap_udp_socket(&self, socket: std::net::UdpSocket) -> io::Result<Arc<dyn AsyncUdpSocket>> {
        self.inner.wrap_udp_socket(socket)
    }
    fn now(&self) -> Instant {
        self.inner.now()
    }
    fn spawn(&self, mut future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        match self.mode.load(Ordering::Relaxed) {
            1 => {
                let owner = self
                    .owner
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .upgrade()
                    .unwrap();
                let root = owner.state.try_lock().expect("spawn outside Root lock");
                assert_eq!(root.inner.open_connections(), 1);
                assert_eq!(root.recv_state.connections.catalog.len(), 1);
                assert!(root.recv_state.connections.catalog.matches(&root.inner));
                drop(root);
                self.observed.fetch_add(1, Ordering::Relaxed);
                let mut cx = Context::from_waker(std::task::Waker::noop());
                if future.as_mut().poll(&mut cx).is_ready() {
                    return;
                }
            }
            2 => {
                drop(future);
                return;
            }
            3 => std::panic::panic_any(self.payload.clone()),
            _ => {}
        }
        self.inner.spawn(future);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn inline_first_poll_sees_both_live_owners_and_real_driver_drains() {
    let runtime = Arc::new(SpawnRuntime {
        inner: crate::default_runtime().unwrap(),
        mode: AtomicUsize::new(0),
        owner: Mutex::new(None),
        observed: AtomicUsize::new(0),
        payload: Arc::new(()),
    });
    let (_socket, mut endpoint, budget, remote) = fixture(runtime.clone());
    *runtime.owner.lock().unwrap() = Some(endpoint.inner.self_weak.clone());
    runtime.mode.store(1, Ordering::Relaxed);
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    assert_eq!(runtime.observed.load(Ordering::Relaxed), 1);
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
}

#[tokio::test(flavor = "current_thread")]
async fn spawn_panic_and_pre_first_poll_drop_cannot_mint_driver_completion() {
    for mode in [2, 3] {
        let runtime = Arc::new(SpawnRuntime {
            inner: crate::default_runtime().unwrap(),
            mode: AtomicUsize::new(0),
            owner: Mutex::new(None),
            observed: AtomicUsize::new(0),
            payload: Arc::new(()),
        });
        let (_socket, mut endpoint, budget, remote) = fixture(runtime.clone());
        runtime.mode.store(mode, Ordering::Relaxed);
        if mode == 2 {
            let mut connecting = endpoint
                .connect(remote.local_addr().unwrap(), "localhost")
                .unwrap();
            assert!(
                budget.quarantined(),
                "future release guard exists before first poll"
            );
            let drain = connecting.take_drain_handle().unwrap();
            drop(connecting);
            assert!(!timeout(Duration::from_secs(2), drain.drained())
                .await
                .unwrap());
        } else {
            let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = endpoint.connect(remote.local_addr().unwrap(), "localhost");
            }))
            .unwrap_err();
            assert!(Arc::ptr_eq(
                &panic.downcast::<Arc<()>>().unwrap(),
                &runtime.payload
            ));
            assert!(budget.quarantined());
        }
        let driver = endpoint.take_driver_drain_handle().unwrap();
        drop(endpoint);
        // The Endpoint may normally dispose a real published Drained event,
        // but the connection driver completion remains absent and Q stays sticky.
        let _ = timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap();
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn opaque_tls_constructor_panic_poison_keeps_original_payload_and_fails_driver_closed() {
    struct PanicTls(Arc<()>);
    impl proto::crypto::ClientConfig for PanicTls {
        fn start_session(
            self: Arc<Self>,
            _: u32,
            _: &str,
            _: &proto::transport_parameters::TransportParameters,
        ) -> Result<Box<dyn proto::crypto::Session>, ConnectError> {
            std::panic::panic_any(self.0.clone());
        }
    }
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let payload = Arc::new(());
    let config = ClientConfig::new(Arc::new(PanicTls(payload.clone())));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = endpoint.connect_with(config, remote.local_addr().unwrap(), "localhost");
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(&panic.downcast::<Arc<()>>().unwrap(), &payload));
    assert!(budget.quarantined());
    assert!(matches!(
        endpoint.connect(remote.local_addr().unwrap(), "localhost"),
        Err(ConnectError::EndpointStopping)
    ));
    assert!(
        !timeout(Duration::from_secs(2), endpoint.wait_idle_or_driver_lost())
            .await
            .unwrap()
    );
    endpoint.set_server_config(None);
    endpoint.close(0u32.into(), b"cleanup");
    let _ = endpoint.stats();
    assert_eq!(endpoint.open_connections(), 0);
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(!timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 1);
}
