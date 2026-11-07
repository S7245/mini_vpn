use super::super::{EndpointPacingService, EndpointPacingServiceConfig};
use super::*;

#[test]
fn reused_slot_and_old_reservation_keep_independent_registration_and_path() {
    let service = EndpointPacingService::new(EndpointPacingServiceConfig::endpoint_window_v1());
    let now = crate::Instant::now();
    let first = EndpointPacingConnectionKey::for_registration(ConnectionHandle(0), 1, 0);
    assert!(service.attach_connection(first));
    let held = service.reserve(first, now, 1280).unwrap();
    let moved = first.with_path(1);
    assert_ne!(first, moved);
    service.migrate_connection(first, moved).unwrap();
    assert_eq!(service.snapshot().connection_records, 2);
    service.detach_connection(moved, now).unwrap();
    let successor = EndpointPacingConnectionKey::for_registration(ConnectionHandle(0), 2, 0);
    assert_ne!(successor, first);
    assert_ne!(successor.with_path(1), moved);
    assert!(service.attach_connection(successor));
    let new_held = service.reserve(successor, now, 640).unwrap();
    assert_eq!(
        service
            .connection_snapshot(successor)
            .unwrap()
            .live_reservation_bytes,
        640
    );
    drop(held);
    assert_eq!(service.snapshot().connection_records, 1);
    assert_eq!(
        service
            .connection_snapshot(successor)
            .unwrap()
            .live_reservation_bytes,
        640
    );
    drop(new_held);
    service.detach_connection(successor, now).unwrap();
    let final_state = service.snapshot();
    assert_eq!(final_state.connection_records, 0);
    assert_eq!(final_state.live_reservation_bytes, 0);
    assert_eq!(final_state.outstanding_bytes, 0);
}
