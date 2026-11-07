//! Actual hard client Rebind must not populate an uncharged legacy set.
use super::*;

impl State {
    fn legacy_previous_socket_capacity(&self) -> usize {
        match &self.prev_socket_connections {
            PreviousSocketInventory::Native => 0,
            PreviousSocketInventory::Legacy(pending) => pending.pending.capacity(),
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn native_previous_socket_pending_has_no_retained_legacy_heap() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let mut second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let first_drain = first.take_drain_handle().unwrap();
    let second_drain = second.take_drain_handle().unwrap();
    endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let (retained, legacy_capacity) = {
        let state = endpoint.inner.lock_state();
        (
            state.prev_socket.is_some(),
            state.legacy_previous_socket_capacity(),
        )
    };
    first_drain.abandon();
    second_drain.abandon();
    drop(first);
    drop(second);
    assert!(timeout(Duration::from_secs(2), first_drain.drained())
        .await
        .unwrap());
    assert!(timeout(Duration::from_secs(2), second_drain.drained())
        .await
        .unwrap());
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert!(timeout(Duration::from_secs(2), driver.drained())
        .await
        .unwrap());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(
        retained,
        "two actual live registrations retain their real previous socket"
    );
    assert_eq!(
        legacy_capacity, 0,
        "Native pending ownership must be in the precharged Host catalog"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn rebind_control_pressure_keeps_socket_generation_and_pending_inventory_unchanged() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let drain = connecting.take_drain_handle().unwrap();
    let held = {
        let state = endpoint.inner.lock_state();
        let held = state
            .recv_state
            .connections
            .catalog
            .values()
            .next()
            .unwrap()
            .reserve_control(ControlKind::Rebind)
            .unwrap();
        held
    };
    let before = endpoint.local_addr().unwrap();
    let error = endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    assert_eq!(endpoint.local_addr().unwrap(), before);
    {
        let state = endpoint.inner.lock_state();
        assert_eq!(state.stats.socket_rebinds, 0);
        assert!(state.prev_socket.is_none());
        assert!(state.recv_state.connections.catalog.matches(&state.inner));
    }
    drop(held);
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

fn actual_keys(endpoint: &Endpoint) -> Vec<proto::RegistrationKey> {
    let keys = std::cell::RefCell::new(Vec::new());
    let state = endpoint.inner.lock_state();
    assert!(state.inner.registration_catalog_matches(|_, live, _| {
        if let Some(key) = live {
            keys.borrow_mut().push(key);
        }
        true
    }));
    keys.into_inner()
}

#[tokio::test(flavor = "current_thread")]
async fn stamped_recovery_and_actual_slot_reuse_never_release_another_pending_owner() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let mut second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let first_drain = first.take_drain_handle().unwrap();
    let second_drain = second.take_drain_handle().unwrap();
    let keys = actual_keys(&endpoint);
    let (old, other) = (keys[0], keys[1]);
    endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    // Explicit caller/misroute faults, not a claimed naturally authenticated wire event.
    endpoint
        .inner
        .settle_connection_event(
            other.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(old), 1),
        )
        .unwrap();
    endpoint
        .inner
        .settle_connection_event(
            other.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(other), 2),
        )
        .unwrap();
    {
        let state = endpoint.inner.lock_state();
        assert!(state.prev_socket.is_some());
        assert_eq!(state.stats.current_socket_rx_rebind_generation, 0);
        assert!(state.registration_inventory_valid());
    }
    first_drain.abandon();
    drop(first);
    assert!(timeout(Duration::from_secs(2), first_drain.drained())
        .await
        .unwrap());
    let mut successor = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let successor_drain = successor.take_drain_handle().unwrap();
    let successor_key = actual_keys(&endpoint)
        .into_iter()
        .find(|key| *key != other)
        .unwrap();
    assert_eq!(successor_key.handle(), old.handle());
    assert_ne!(successor_key, old);
    endpoint
        .inner
        .settle_connection_event(
            successor_key.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(old), 1),
        )
        .unwrap();
    endpoint
        .inner
        .settle_connection_event(
            successor_key.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(successor_key), 1),
        )
        .unwrap();
    {
        let state = endpoint.inner.lock_state();
        assert!(
            state.prev_socket.is_some(),
            "a post-rebind successor does not own the other pending entry"
        );
        assert!(state.registration_inventory_valid());
    }
    endpoint
        .inner
        .settle_connection_event(
            other.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(other), 1),
        )
        .unwrap();
    {
        let state = endpoint.inner.lock_state();
        assert!(state.prev_socket.is_none());
        assert!(state.registration_inventory_valid());
    }
    second_drain.abandon();
    successor_drain.abandon();
    drop(second);
    drop(successor);
    assert!(timeout(Duration::from_secs(2), second_drain.drained())
        .await
        .unwrap());
    assert!(timeout(Duration::from_secs(2), successor_drain.drained())
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
async fn generation_exhaustion_rejects_before_socket_or_fanout_slot_changes() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut connecting = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let drain = connecting.take_drain_handle().unwrap();
    let before = endpoint.local_addr().unwrap();
    endpoint.inner.lock_state().stats.socket_rebinds = u64::MAX;
    assert!(endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .is_err());
    assert_eq!(endpoint.local_addr().unwrap(), before);
    {
        let mut state = endpoint.inner.lock_state();
        assert_eq!(state.stats.socket_rebinds, u64::MAX);
        assert!(state.prev_socket.is_none());
        let permit = state
            .recv_state
            .connections
            .catalog
            .values()
            .next()
            .unwrap()
            .reserve_control(ControlKind::Rebind)
            .unwrap();
        drop(permit);
        state.stats.socket_rebinds = 0; // restore only the explicit counter-boundary fixture
    }
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
async fn both_directions_of_previous_socket_inventory_reject_control_and_keep_charged_quarantine() {
    for remove_pending_socket in [false, true] {
        let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
        let mut connecting = endpoint
            .connect(remote.local_addr().unwrap(), "localhost")
            .unwrap();
        let drain = connecting.take_drain_handle().unwrap();
        let key = actual_keys(&endpoint)[0];
        if remove_pending_socket {
            endpoint
                .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                .unwrap();
            endpoint.inner.lock_state().prev_socket = None;
        } else {
            let mut state = endpoint.inner.lock_state();
            state.prev_socket = Some(state.socket.clone());
        }
        assert!(endpoint
            .inner
            .settle_connection_event(
                key.handle(),
                ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(key), 1)
            )
            .is_err());
        assert!(budget.quarantined());
        drain.abandon();
        drop(connecting);
        assert!(!timeout(Duration::from_secs(2), drain.drained())
            .await
            .unwrap());
        let driver = endpoint.take_driver_drain_handle().unwrap();
        drop(endpoint);
        assert!(!timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap());
        assert_eq!(budget.used(), 1);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn original_sender_cannot_authorize_other_live_socket_recovery() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let mut second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let first_drain = first.take_drain_handle().unwrap();
    let second_drain = second.take_drain_handle().unwrap();
    let keys = actual_keys(&endpoint);
    let other = keys[1];
    endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    assert_eq!(
        endpoint
            .inner
            .lock_state()
            .recv_state
            .connections
            .catalog
            .previous_pending_count_for_probe(),
        2
    );
    // The real first Connection's sender carries a fully valid target-B identity.
    let rejected = first
        .send_endpoint_event_for_probe(
            other.handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(other), 1),
        )
        .is_err();
    let observed = {
        let state = endpoint.inner.lock_state();
        (
            rejected,
            budget.quarantined(),
            state
                .recv_state
                .connections
                .catalog
                .previous_pending_count_for_probe(),
            state.prev_socket.is_some(),
            state.stats.current_socket_rx_rebind_generation,
        )
    };
    let quarantined = budget.quarantined();
    first_drain.abandon();
    second_drain.abandon();
    drop(first);
    drop(second);
    assert_eq!(
        timeout(Duration::from_secs(2), first_drain.drained())
            .await
            .unwrap(),
        !quarantined
    );
    assert_eq!(
        timeout(Duration::from_secs(2), second_drain.drained())
            .await
            .unwrap(),
        !quarantined
    );
    let driver = endpoint.take_driver_drain_handle().unwrap();
    drop(endpoint);
    assert_eq!(
        timeout(Duration::from_secs(2), driver.drained())
            .await
            .unwrap(),
        !quarantined
    );
    assert_eq!(budget.used(), usize::from(quarantined));
    assert_eq!(
        observed,
        (true, true, 2, true, 0),
        "source authority cannot be borrowed from a different live target"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn each_original_bound_sender_completes_only_its_own_previous_socket_pending() {
    let (_socket, mut endpoint, budget, remote) = fixture(crate::default_runtime().unwrap());
    let mut first = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let mut second = endpoint
        .connect(remote.local_addr().unwrap(), "localhost")
        .unwrap();
    let first_drain = first.take_drain_handle().unwrap();
    let second_drain = second.take_drain_handle().unwrap();
    let keys = actual_keys(&endpoint);
    endpoint
        .rebind(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    first
        .send_endpoint_event_for_probe(
            keys[0].handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(keys[0]), 1),
        )
        .unwrap();
    assert_eq!(
        endpoint
            .inner
            .lock_state()
            .recv_state
            .connections
            .catalog
            .previous_pending_count_for_probe(),
        1
    );
    assert!(endpoint.inner.lock_state().prev_socket.is_some());
    second
        .send_endpoint_event_for_probe(
            keys[1].handle(),
            ConnectionToEndpointEvent::AuthenticatedCurrentSocketRx(Some(keys[1]), 1),
        )
        .unwrap();
    assert_eq!(
        endpoint
            .inner
            .lock_state()
            .recv_state
            .connections
            .catalog
            .previous_pending_count_for_probe(),
        0
    );
    assert!(endpoint.inner.lock_state().prev_socket.is_none());
    first_drain.abandon();
    second_drain.abandon();
    drop(first);
    drop(second);
    assert!(timeout(Duration::from_secs(2), first_drain.drained())
        .await
        .unwrap());
    assert!(timeout(Duration::from_secs(2), second_drain.drained())
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
