//! Actual unpublished Native Connection owners must reserve capacity and prove disposal.
use super::*;

fn fixture() -> (Endpoint, bytes::BackingBudget) {
    let budget = cold_fixture_budget(20);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    (
        Endpoint::try_new(Arc::new(config), None, true, Some([0x39; 32])).unwrap(),
        budget,
    )
}
fn prepare(endpoint: &mut Endpoint) -> (crate::PreparedConnect, Connection) {
    endpoint
        .prepare_connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost",
        )
        .unwrap()
}

#[test]
fn unpublished_nineteen_reservations_are_unroutable_and_cancel_after_actual_disposal() {
    let (mut endpoint, budget) = fixture();
    let baseline = budget.used();
    let mut owners = Vec::new();
    for _ in 0..19 {
        let owner = prepare(&mut endpoint);
        assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (0, 0));
        assert!(!endpoint.registration_matches(owner.0.key().handle(), Some(owner.0.key())));
        owners.push(owner);
    }
    let full = budget.used();
    assert!(matches!(
        endpoint.prepare_connect(
            Instant::now(),
            client_config(),
            "[::1]:4433".parse().unwrap(),
            "localhost"
        ),
        Err(ConnectError::EventCapacityUnavailable)
    ));
    assert_eq!(budget.used(), full);
    // Publish one real reserved Connection, then process its genuine terminal event.
    let (mut ticket, mut conn) = owners.pop().unwrap();
    let ch = endpoint.publish_connect(&mut ticket, &conn).unwrap();
    assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (1, 1));
    assert!(endpoint.registration_matches(ch, conn.registration_key()));
    conn.abandon();
    let event = conn.take_drained_event().unwrap();
    endpoint.handle_event(ch, event);
    drop(conn);
    drop(ticket);
    for (ticket, conn) in owners {
        drop(conn);
        assert!(endpoint.cancel_connect(ticket));
    }
    assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (0, 0));
    assert_eq!(budget.used(), baseline);
    // An actually disposed and cancelled slot is reusable with a distinct key.
    let (ticket, conn) = prepare(&mut endpoint);
    drop(conn);
    assert!(endpoint.cancel_connect(ticket));
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn premature_cancel_cannot_substitute_a_host_marker_for_actual_disposal() {
    let (mut endpoint, budget) = fixture();
    let (ticket, conn) = prepare(&mut endpoint);
    assert!(!endpoint.cancel_connect(ticket));
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
    drop(conn);
    drop(endpoint);
    assert_eq!(budget.used(), 1);
}

#[test]
fn lost_ticket_and_root_loss_quarantine_immediately_while_connection_is_alive() {
    for lose_root in [false, true] {
        let (mut endpoint, budget) = fixture();
        let (ticket, conn) = prepare(&mut endpoint);
        if lose_root {
            drop(endpoint);
            assert!(budget.quarantined());
            drop(ticket);
        } else {
            drop(ticket);
            assert!(budget.quarantined());
            drop(endpoint);
        }
        assert!(budget.used() > 1);
        drop(conn);
        assert_eq!(budget.used(), 1);
    }
}

#[test]
fn foreign_endpoint_cannot_publish_reserved_identity() {
    let (mut owner, budget) = fixture();
    let (mut other, other_budget) = fixture();
    let (mut ticket, conn) = prepare(&mut owner);
    assert!(other.publish_connect(&mut ticket, &conn).is_err());
    assert_eq!(other.open_connections(), 0);
    assert_eq!(owner.open_connections(), 0);
    drop(conn);
    assert!(owner.cancel_connect(ticket));
    drop(owner);
    drop(other);
    assert_eq!(budget.used(), 0);
    assert_eq!(other_budget.used(), 0);
    assert!(!budget.quarantined());
    assert!(!other_budget.quarantined());
}

#[test]
fn paced_unpublished_disposal_and_constructor_rejection_leave_no_attachment() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
    for _ in 0..3 {
        let (ticket, conn) = prepare(&mut endpoint);
        assert_eq!(
            endpoint
                .endpoint_pacing_snapshot()
                .unwrap()
                .connection_records,
            1
        );
        drop(conn);
        assert!(endpoint.cancel_connect(ticket));
        assert_eq!(
            endpoint
                .endpoint_pacing_snapshot()
                .unwrap()
                .connection_records,
            0
        );
    }
    struct Reject;
    impl crate::TokenStore for Reject {
        fn insert(&self, _: &str, _: bytes::Bytes) {
            unreachable!()
        }
        fn take(&self, _: &str) -> Option<bytes::Bytes> {
            unreachable!()
        }
    }
    let baseline = budget.used();
    for _ in 0..3 {
        let mut client = client_config();
        client.token_store(Arc::new(Reject));
        assert!(matches!(
            endpoint.prepare_connect(
                Instant::now(),
                client,
                "[::1]:4433".parse().unwrap(),
                "localhost"
            ),
            Err(ConnectError::CryptoOutput(_))
        ));
        assert_eq!(
            endpoint
                .endpoint_pacing_snapshot()
                .unwrap()
                .connection_records,
            0
        );
        assert_eq!(budget.used(), baseline);
    }
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn cancelled_sender_cannot_borrow_successor_settlement_watermark() {
    let (mut endpoint, budget) = fixture();
    let observer = endpoint.registration_observer().unwrap();
    let (ticket, conn) = prepare(&mut endpoint);
    let old = ticket.key();
    let bound = observer.bind(old);
    drop(conn);
    assert!(endpoint.cancel_connect(ticket));
    let (mut ticket, mut conn) = prepare(&mut endpoint);
    let current = ticket.key();
    assert_eq!(current.handle(), old.handle());
    assert_ne!(current, old);
    endpoint.publish_connect(&mut ticket, &conn).unwrap();
    let published = observer.bind(current);
    conn.abandon();
    endpoint.handle_event(current.handle(), conn.take_drained_event().unwrap());
    assert!(published.was_settled(current.handle(), Some(current)));
    assert!(
        !bound.was_settled(old.handle(), Some(old)),
        "cancelled generation never acquired publication authority"
    );
    let late = observer.bind(old);
    assert!(
        !late.was_settled(old.handle(), Some(old)),
        "late bind cannot fill a cancellation gap"
    );
    drop(ticket);
    drop(conn);
    drop(observer);
    drop(published);
    drop(bound);
    drop(late);
    drop(endpoint);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn closed_private_connection_is_not_publishable_even_while_physically_alive() {
    for drained in [false, true] {
        let (mut endpoint, budget) = fixture();
        let baseline = budget.used();
        let (mut ticket, mut conn) = prepare(&mut endpoint);
        if drained {
            conn.abandon();
        } else {
            conn.close(
                Instant::now(),
                7u32.into(),
                bytes::Bytes::from_static(b"private cancellation"),
            );
        }
        assert!(endpoint.publish_connect(&mut ticket, &conn).is_err());
        assert_eq!((endpoint.open_connections(), endpoint.known_cids()), (0, 0));
        drop(conn);
        assert!(endpoint.cancel_connect(ticket));
        assert_eq!(budget.used(), baseline);
        drop(endpoint);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}

#[test]
fn poisoned_pacing_authority_cannot_sign_actual_connection_disposal() {
    let budget = cold_fixture_budget(2);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    config.endpoint_pacing_service(Some(EndpointPacingServiceConfig::endpoint_window_v1()));
    let mut endpoint = Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
    let (ticket, conn) = prepare(&mut endpoint);
    endpoint.poison_prepared_pacing_for_probe();
    drop(conn);
    assert!(budget.quarantined());
    assert!(!endpoint.cancel_connect(ticket));
    drop(endpoint);
    assert_eq!(budget.used(), 1);
}
