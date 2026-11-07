//! Actual published receiver interruption during Root fanout publication.
use super::*;
struct PanicWake(Arc<()>);
impl std::task::Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        std::panic::panic_any(self.0.clone());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn rebind_original_receiver_wake_panic_is_immediate_charged_quarantine() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let payload = Arc::new(());
    let waker = std::task::Waker::from(Arc::new(PanicWake(payload.clone())));
    assert!(first.register_forward_waker_for_probe(&waker));
    assert!(second.register_forward_waker_for_probe(&waker));
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        endpoint.rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
    }));
    // Observe before reacquiring Root: poison recovery must not be what first
    // records an interruption that has already changed socket/queued controls.
    let immediate_q = budget.quarantined();
    let original = interrupted.err().is_some_and(|p| {
        p.downcast_ref::<Arc<()>>()
            .is_some_and(|p| Arc::ptr_eq(p, &payload))
    });
    let driver = endpoint.take_driver_drain_handle().unwrap();
    // Remove only this test's injected panic callback before normal cleanup.
    assert!(!first.register_forward_waker_for_probe(std::task::Waker::noop()));
    second.register_forward_waker_for_probe(std::task::Waker::noop());
    drop((first, second, endpoint));
    let drained = timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap();
    assert!(original);
    assert!(!drained);
    assert!(budget.quarantined());
    assert_eq!(budget.used(), 1);
    assert!(
        immediate_q,
        "a Root fanout interruption must record Q before a later observer recovers the lock"
    );
}

async fn dispose(
    mut endpoint: Endpoint,
    connections: Vec<Connecting>,
    clean: bool,
    budget: bytes::BackingBudget,
) {
    for mut connection in connections {
        if clean {
            let drain = connection.take_drain_handle().unwrap();
            drain.abandon();
            drop(connection);
            assert!(timeout(Duration::from_secs(2), drain.drained())
                .await
                .unwrap());
        } else {
            drop(connection);
        }
    }
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert_eq!(
        timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap(),
        clean
    );
    assert_eq!(budget.used(), if clean { 0 } else { 1 });
    assert_eq!(budget.quarantined(), !clean);
}

#[tokio::test(flavor = "current_thread")]
async fn close_original_receiver_wake_panic_preserves_original_payload_and_charged_q() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let payload = Arc::new(());
    let waker = std::task::Waker::from(Arc::new(PanicWake(payload.clone())));
    assert!(first.register_forward_waker_for_probe(&waker));
    assert!(second.register_forward_waker_for_probe(&waker));
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        endpoint.close(7u32.into(), b"original reason")
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        panic.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(budget.quarantined());
    assert!(!first.register_forward_waker_for_probe(std::task::Waker::noop()));
    assert!(!second.register_forward_waker_for_probe(std::task::Waker::noop()));
    {
        let state = endpoint.inner.lock_state();
        let (code, reason) = state.recv_state.connections.close.as_ref().unwrap();
        assert_eq!(*code, VarInt::from_u32(7));
        assert_eq!(&reason[..], b"original reason");
    }
    dispose(endpoint, vec![first, second], false, budget).await;
}

struct RootWake {
    root: Weak<EndpointInner>,
    unlocked: AtomicBool,
    calls: AtomicUsize,
}
impl std::task::Wake for RootWake {
    fn wake(self: Arc<Self>) {
        let owner = self.root.upgrade().unwrap();
        self.unlocked
            .store(owner.state.try_lock().is_ok(), Ordering::Release);
        self.calls.fetch_add(1, Ordering::Release);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn second_rebind_full_cancels_first_original_ticket_after_root_unlock() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let wake = Arc::new(RootWake {
        root: endpoint.inner.self_weak.clone(),
        unlocked: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    assert!(first.register_forward_waker_for_probe(&std::task::Waker::from(wake.clone())));
    let held = endpoint
        .inner
        .lock_state()
        .recv_state
        .connections
        .catalog
        .values()
        .nth(1)
        .unwrap()
        .reserve_control(ControlKind::Rebind)
        .unwrap();
    let before = endpoint.local_addr().unwrap();
    assert_eq!(
        endpoint
            .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(endpoint.local_addr().unwrap(), before);
    assert!(wake.unlocked.load(Ordering::Acquire));
    assert_eq!(wake.calls.load(Ordering::Acquire), 1);
    assert!(first.register_forward_waker_for_probe(std::task::Waker::noop()));
    assert!(second.register_forward_waker_for_probe(std::task::Waker::noop()));
    {
        let state = endpoint.inner.lock_state();
        assert_eq!(state.stats.socket_rebinds, 0);
        assert!(state.prev_socket.is_none());
        assert!(state.registration_inventory_valid());
        assert!(state
            .recv_state
            .connections
            .catalog
            .values()
            .next()
            .unwrap()
            .reserve_control(ControlKind::Rebind)
            .is_ok());
    }
    drop(held);
    dispose(endpoint, vec![first, second], true, budget).await;
}

#[tokio::test(flavor = "current_thread")]
async fn second_close_full_has_no_close_marker_or_partial_delivery() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let held = endpoint
        .inner
        .lock_state()
        .recv_state
        .connections
        .catalog
        .values()
        .nth(1)
        .unwrap()
        .reserve_control(ControlKind::Close)
        .unwrap();
    endpoint.close(7u32.into(), b"no partial close");
    assert!(budget.quarantined());
    assert!(endpoint
        .inner
        .lock_state()
        .recv_state
        .connections
        .close
        .is_none());
    assert!(first.forward_queue_empty_for_probe());
    assert!(second.forward_queue_empty_for_probe());
    drop(held);
    dispose(endpoint, vec![first, second], false, budget).await;
}

#[tokio::test(flavor = "current_thread")]
async fn close_reason_full_does_not_replace_reason_with_empty_success() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let conn = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let meter = endpoint
        .inner
        .lock_state()
        .recv_state
        .backing_meter
        .clone()
        .unwrap();
    let rest = meter
        .reserve_cold_layout(budget.limit() - budget.used())
        .unwrap()
        .unwrap();
    endpoint.close(7u32.into(), b"must retain this rejection");
    assert!(budget.quarantined());
    assert!(endpoint
        .inner
        .lock_state()
        .recv_state
        .connections
        .close
        .is_none());
    assert!(conn.forward_queue_empty_for_probe());
    drop(rest);
    dispose(endpoint, vec![conn], false, budget).await;
}

#[derive(Debug)]
struct DropSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    root: Weak<EndpointInner>,
    unlocked: Arc<AtomicBool>,
    payload: Arc<()>,
}
impl AsyncUdpSocket for DropSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn crate::UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, tx: &crate::udp::Transmit<'_>) -> io::Result<()> {
        self.inner.try_send(tx)
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
    fn max_receive_segments(&self) -> usize {
        self.inner.max_receive_segments()
    }
}
impl Drop for DropSocket {
    fn drop(&mut self) {
        let root = self.root.upgrade().unwrap();
        self.unlocked
            .store(root.state.try_lock().is_ok(), Ordering::Release);
        std::panic::panic_any(self.payload.clone());
    }
}
#[tokio::test(flavor = "current_thread")]
async fn displaced_socket_drop_runs_outside_root_and_preserves_panic_q() {
    let runtime = crate::default_runtime().unwrap();
    let (_socket, endpoint, budget, _remote) = fixture(runtime.clone());
    let unlocked = Arc::new(AtomicBool::new(false));
    let payload = Arc::new(());
    endpoint
        .rebind_abstract(Arc::new(DropSocket {
            inner: runtime
                .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                .unwrap(),
            root: endpoint.inner.self_weak.clone(),
            unlocked: unlocked.clone(),
            payload: payload.clone(),
        }))
        .unwrap();
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        endpoint.rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
    }))
    .unwrap_err();
    assert!(Arc::ptr_eq(
        panic.downcast_ref::<Arc<()>>().unwrap(),
        &payload
    ));
    assert!(unlocked.load(Ordering::Acquire));
    assert!(budget.quarantined());
    assert_eq!(endpoint.inner.lock_state().stats.socket_rebinds, 2);
    dispose(endpoint, vec![], false, budget).await;
}

#[tokio::test(flavor = "current_thread")]
async fn native_pending_reverse_is_rejected_before_take_or_auth_settlement() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let conn = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let (valid, still_pending) = {
        let mut state = endpoint.inner.lock_state();
        let entry = state
            .recv_state
            .connections
            .catalog
            .entries()
            .next()
            .unwrap();
        let message = (
            entry.handle,
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(entry.key, 0),
        );
        state.pending_reverse_event = Some(message);
        let valid = state.registration_inventory_valid();
        state.handle_events(
            &mut Context::from_waker(std::task::Waker::noop()),
            &endpoint.inner.shared,
        );
        (valid, state.pending_reverse_event.is_some())
    };
    let immediate_q = budget.quarantined();
    // Keep red evidence from turning teardown into a different destructor failure.
    endpoint
        .inner
        .lock_state()
        .recv_state
        .backing_meter
        .as_ref()
        .unwrap()
        .quarantine();
    dispose(endpoint, vec![conn], false, budget).await;
    assert!(
        !valid,
        "Native ordinary reverse events must never enter Legacy pending custody"
    );
    assert!(
        still_pending,
        "the invalid inventory must be rejected before take"
    );
    assert!(immediate_q);
}

#[tokio::test(flavor = "current_thread")]
async fn close_rejection_wakes_parked_driver_without_waiting_for_endpoint_drop() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let conn = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    tokio::task::yield_now().await;
    let driver = endpoint.take_driver_drain_handle().unwrap();
    let meter = endpoint
        .inner
        .lock_state()
        .recv_state
        .backing_meter
        .clone()
        .unwrap();
    let rest = meter
        .reserve_cold_layout(budget.limit() - budget.used())
        .unwrap()
        .unwrap();
    endpoint.close(7u32.into(), b"refused before publish");
    assert!(!timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert!(endpoint
        .inner
        .lock_state()
        .recv_state
        .connections
        .close
        .is_none());
    drop((rest, conn, endpoint));
    tokio::task::yield_now().await;
    assert_eq!(budget.used(), 1);
    assert!(budget.quarantined());
}

#[tokio::test(flavor = "current_thread")]
async fn actual_prepared_fanout_rejects_missing_ticket_and_wrong_event_kind_before_root() {
    for wrong_kind in [false, true] {
        let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
        let first = endpoint
            .connect(remote.local_addr().unwrap(), "localhost")
            .unwrap();
        let second = endpoint
            .connect(remote.local_addr().unwrap(), "localhost")
            .unwrap();
        let mut batch = super::super::super::fanout::Fanout::new();
        let (rejected, marker) = {
            let mut state = endpoint.inner.lock_state();
            batch.meter = state.recv_state.backing_meter.clone();
            batch
                .prepare(&state, ControlKind::Close, || ConnectionEvent::Close {
                    error_code: 9u32.into(),
                    reason: Bytes::new(),
                })
                .unwrap();
            if wrong_kind {
                batch.replace_first_kind_for_probe(ConnectionEvent::Rebind(state.socket.clone()));
            } else {
                batch.omit_first_target_for_probe();
            }
            let rejected = batch
                .apply(&mut state, ControlKind::Close, |state, _| {
                    state.recv_state.connections.close = Some((9u32.into(), Bytes::new()));
                    [None, None]
                })
                .is_err();
            (rejected, state.recv_state.connections.close.is_some())
        };
        drop(batch);
        let empty = first.forward_queue_empty_for_probe()
            && second.forward_queue_empty_for_probe();
        let immediate_q = budget.quarantined();
        endpoint
            .inner
            .lock_state()
            .recv_state
            .backing_meter
            .as_ref()
            .unwrap()
            .quarantine();
        dispose(endpoint, vec![first, second], false, budget).await;
        assert!(rejected && !marker && empty && immediate_q, "missing/wrong-kind physical inventory must reject and Q before Root or any slot commit");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_deferred_auth_cannot_settle_previous_socket_with_another_entry_path() {
    let (_socket, endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let conn = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let retained = {
        let mut state = endpoint.inner.lock_state();
        let entry = state
            .recv_state
            .connections
            .catalog
            .entries()
            .next()
            .unwrap();
        state
            .recv_state
            .connections
            .sender
            .send((
                entry.handle,
                ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(entry.key, 1),
            ))
            .unwrap();
        state.handle_events(
            &mut Context::from_waker(std::task::Waker::noop()),
            &endpoint.inner.shared,
        );
        state.prev_socket.is_some()
            && state.recv_state.connections.catalog.has_previous_pending()
            && state.stats.current_socket_rx_rebind_generation == 0
    };
    let immediate_q = budget.quarantined();
    endpoint
        .inner
        .lock_state()
        .recv_state
        .backing_meter
        .as_ref()
        .unwrap()
        .quarantine();
    dispose(endpoint, vec![conn], false, budget).await;
    assert!(retained && immediate_q);
}

#[derive(Debug)]
struct CandidatePanicSocket {
    inner: Arc<dyn AsyncUdpSocket>,
    root: Weak<EndpointInner>,
    original: Arc<()>,
    secondary: Arc<()>,
    dropped_unlocked: Arc<AtomicBool>,
    fail_address: bool,
}
impl AsyncUdpSocket for CandidatePanicSocket {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn crate::UdpPoller>> {
        self.inner.clone().create_io_poller()
    }
    fn try_send(&self, tx: &crate::udp::Transmit<'_>) -> io::Result<()> {
        self.inner.try_send(tx)
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
        if self.fail_address {
            std::panic::panic_any(self.original.clone());
        }
        self.inner.local_addr()
    }
    fn max_receive_segments(&self) -> usize {
        std::panic::panic_any(self.original.clone());
    }
}
impl Drop for CandidatePanicSocket {
    fn drop(&mut self) {
        let root = self.root.upgrade().unwrap();
        self.dropped_unlocked.store(
            !matches!(
                root.state.try_lock(),
                Err(std::sync::TryLockError::WouldBlock)
            ),
            Ordering::Release,
        );
        std::panic::panic_any(self.secondary.clone());
    }
}
#[tokio::test(flavor = "current_thread")]
async fn candidate_query_and_drop_double_panic_retains_first_payload_without_sigabrt() {
    for fail_address in [false, true] {
        let runtime = crate::default_runtime().unwrap();
        let (_socket, endpoint, budget, _remote) = fixture(runtime.clone());
        let original = Arc::new(());
        let dropped_unlocked = Arc::new(AtomicBool::new(false));
        let before = endpoint.local_addr().unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            endpoint.rebind_abstract(Arc::new(CandidatePanicSocket {
                inner: runtime
                    .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                    .unwrap(),
                root: endpoint.inner.self_weak.clone(),
                original: original.clone(),
                secondary: Arc::new(()),
                dropped_unlocked: dropped_unlocked.clone(),
                fail_address,
            }))
        }))
        .unwrap_err();
        let immediate_q = budget.quarantined();
        assert!(Arc::ptr_eq(
            panic.downcast_ref::<Arc<()>>().unwrap(),
            &original
        ));
        assert!(immediate_q);
        assert!(dropped_unlocked.load(Ordering::Acquire));
        assert_eq!(endpoint.local_addr().unwrap(), before);
        assert_eq!(endpoint.inner.lock_state().stats.socket_rebinds, 0);
        dispose(endpoint, vec![], false, budget).await;
    }
}
