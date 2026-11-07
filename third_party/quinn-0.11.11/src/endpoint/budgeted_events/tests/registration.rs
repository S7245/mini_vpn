//! Explicit caller-misroute fault through the original terminal sender.
use super::*;

#[test]
fn terminal_sender_binds_original_slot_generation_and_handle() {
    let budget = BackingBudget::new(8 * 1024 * 1024).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let mut config = proto::EndpointConfig::default();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(meter.clone());
    let mut endpoint = proto::Endpoint::try_new(Arc::new(config), None, true, None).unwrap();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let client = proto::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
    let now = crate::Instant::now();
    let remote = "127.0.0.1:4433".parse().unwrap();
    let (old_handle, mut old) = endpoint
        .connect(now, client.clone(), remote, "localhost")
        .unwrap();
    let old_key = old.registration_key().unwrap();
    let observer = endpoint.registration_observer().unwrap();
    let old_bound = observer.bind(old_key);
    old.abandon();
    endpoint.handle_event(old_handle, old.take_drained_event().unwrap());
    assert!(old_bound.was_settled(old_handle, Some(old_key)));
    let (handle, mut successor) = endpoint.connect(now, client, remote, "localhost").unwrap();
    let key = successor.registration_key().unwrap();
    assert_eq!(handle, old_handle);
    assert_ne!(key, old_key);
    let current_bound = observer.bind(key);
    assert!(
        !current_bound.was_settled(handle, Some(old_key)),
        "another generation's settlement cannot authorize this sender"
    );
    assert!(!current_bound.matches(proto::ConnectionHandle(handle.0 + 1), Some(key)));
    assert!(!old_bound.matches(handle, Some(key)));
    let (terminal, mut receiver) = terminal_channel(0, &meter).unwrap();
    let (tx, mut deferred) = mpsc::unbounded_channel();
    let mut sender = EndpointEventSender::new(
        tx,
        Some(std::sync::Weak::new()),
        Some(meter),
        Some(terminal),
    );
    sender.bind_registration(old_bound);
    successor.abandon();
    let genuine_new_event = successor.take_drained_event().unwrap();
    let (ack, mut receipt) = tokio::sync::oneshot::channel();
    assert!(sender
        .send((
            handle,
            crate::ConnectionToEndpointEvent::Proto {
                event: genuine_new_event,
                endpoint_released: Some(ack)
            }
        ))
        .is_err());
    assert_eq!(
        receipt.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Closed)
    );
    assert!(deferred.try_recv().is_err());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(receiver.is_empty(), "Q cannot conceal a partially published event");
    assert!(matches!(receiver.poll_recv(&mut cx), Poll::Ready(None)));
    assert_eq!(
        endpoint.open_connections(),
        1,
        "misrouted caller cannot remove successor's Proto registration"
    );
    assert!(budget.quarantined());
    drop(sender);
    drop(receiver);
    drop(endpoint);
    drop(successor);
    drop(old);
    drop(observer);
    drop(current_bound);
    assert_eq!(budget.used(), 1);
}
