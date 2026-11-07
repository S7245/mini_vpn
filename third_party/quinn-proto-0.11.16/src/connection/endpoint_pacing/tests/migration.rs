//! Calls the real Connection migration boundary with a full old-lease inventory.
use super::*;
use crate::{ClientConfig, Endpoint, EndpointConfig};

fn fixture() -> (
    bytes::BackingBudget,
    Endpoint,
    ConnectionHandle,
    crate::Connection,
    Instant,
) {
    let mut config = EndpointConfig::default();
    config.receive_datagram_limit(1472).unwrap();
    let capacity = crate::connection::ColdResources::layout_bytes(&config, None).unwrap()
        + Endpoint::native_registration_layout_bytes()
        + Endpoint::native_pacing_layout_bytes()
        + 64 * 1024;
    let budget = bytes::BackingBudget::new(capacity).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, Some([0x41; 32])).unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    let now = Instant::now();
    let (handle, conn) = endpoint
        .connect(now, client, "127.0.0.1:4433".parse().unwrap(), "localhost")
        .unwrap();
    (budget, endpoint, handle, conn, now)
}

#[test]
fn real_connection_full_migration_preserves_path_key_timer_and_old_lease() {
    let (budget, mut endpoint, handle, mut conn, now) = fixture();
    let pacing = conn.endpoint_pacing.as_ref().unwrap();
    let service = pacing.service.clone();
    let old_key = pacing.key;
    let mut leases = Vec::new();
    leases.push(service.reserve(old_key, now, 1).unwrap());
    for i in 1..19 {
        let key = EndpointPacingConnectionKey::for_registration(ConnectionHandle(i), 1, 0);
        assert!(service.attach_connection(key));
        leases.push(service.reserve(key, now, 1).unwrap());
        service.detach_connection(key, now).unwrap();
    }
    let deadline = now + Duration::from_secs(3);
    conn.timers
        .set(crate::connection::Timer::PathValidation, deadline);
    assert!(matches!(
        service.poll_reserve(old_key, PacingTrafficClass::Bulk, deadline, 1, None, now),
        PollReservation::Blocked { .. }
    ));
    let before = (
        conn.path.remote,
        conn.path_counter,
        conn.timers.get(crate::connection::Timer::PathValidation),
    );
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        conn.migrate(now, "127.0.0.1:4434".parse().unwrap())
    }));
    let unchanged = before
        == (
            conn.path.remote,
            conn.path_counter,
            conn.timers.get(crate::connection::Timer::PathValidation),
        )
        && conn.endpoint_pacing.as_ref().unwrap().key == old_key
        && service.snapshot().migrations == 0
        && service.snapshot().live_reservation_bytes == 19;
    assert_eq!(
        service
            .lock_state()
            .connections
            .get(&old_key)
            .unwrap()
            .waiter
            .as_ref()
            .unwrap()
            .path_ready_at,
        deadline
    );
    drop(leases.pop());
    let remote = "127.0.0.1:4434".parse().unwrap();
    conn.migrate(now, remote).unwrap();
    assert_eq!(conn.path.remote, remote);
    assert_eq!(conn.path_counter, before.1 + 1);
    assert_eq!(
        conn.endpoint_pacing.as_ref().unwrap().key,
        old_key.with_path(before.1 + 1)
    );
    assert!(conn
        .timers
        .get(crate::connection::Timer::PathValidation)
        .is_some());
    drop(leases);
    conn.abandon();
    endpoint.handle_event(handle, conn.take_drained_event().unwrap());
    drop((conn, endpoint, service));
    assert!(
        caught.unwrap().is_err(),
        "known Full must be typed rejection, not a debug panic"
    );
    assert!(
        unchanged,
        "Full must precede all Path/key/timer and old-lease mutations"
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[derive(Clone)]
struct DropController {
    armed: Arc<std::sync::atomic::AtomicBool>,
    payload: Arc<()>,
}
impl crate::congestion::Controller for DropController {
    fn on_congestion_event(&mut self, _: Instant, _: Instant, _: bool, _: u64) {}
    fn on_mtu_update(&mut self, _: u16) {}
    fn window(&self) -> u64 {
        12000
    }
    fn initial_window(&self) -> u64 {
        12000
    }
    fn clone_box(&self) -> Box<dyn crate::congestion::Controller> {
        Box::new(self.clone())
    }
    fn into_any(self: Box<Self>) -> Box<dyn std::any::Any> {
        self
    }
}
impl Drop for DropController {
    fn drop(&mut self) {
        if self.armed.load(Ordering::Acquire) {
            std::panic::panic_any(self.payload.clone());
        }
    }
}

#[test]
fn real_migration_displaced_path_drop_follows_new_deadline_and_preserves_first() {
    let (budget, endpoint, _handle, mut conn, now) = fixture();
    let a = DropController {
        armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        payload: Arc::new(()),
    };
    let b = DropController {
        armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        payload: Arc::new(()),
    };
    conn.path.congestion = Box::new(a.clone());
    conn.migrate(now, "127.0.0.1:4434".parse().unwrap())
        .unwrap();
    conn.path.congestion = Box::new(b.clone());
    // Simulate actual PATH_RESPONSE validation while retaining the previous path.
    conn.path.challenge = None;
    let later = now + Duration::from_secs(1);
    let expected_deadline = later + 3 * conn.pto(crate::packet::SpaceId::Data);
    a.armed.store(true, Ordering::Release);
    b.armed.store(true, Ordering::Release);
    let remote = "127.0.0.1:4435".parse().unwrap();
    let caught =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| conn.migrate(later, remote)))
            .unwrap_err();
    a.armed.store(false, Ordering::Release);
    b.armed.store(false, Ordering::Release);
    assert!(Arc::ptr_eq(
        caught.downcast_ref::<Arc<()>>().unwrap(),
        &a.payload
    ));
    assert!(budget.quarantined());
    assert_eq!(conn.path.remote, remote);
    assert_eq!(conn.path_counter, 2);
    assert_eq!(
        conn.endpoint_pacing.as_ref().unwrap().key.path_generation,
        2
    );
    assert_eq!(
        conn.timers.get(crate::connection::Timer::PathValidation),
        Some(expected_deadline)
    );
    drop((conn, endpoint, a, b));
    assert_eq!(budget.used(), 1);
}
