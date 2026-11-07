//! Real encrypted NEW_TOKEN, subsequent Initial and original terminal owners.
use super::*;

fn pair() -> (Pair, bytes::BackingBudget, bytes::BackingMeter) {
    let budget = cold_fixture_budget(4);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let mut config = admission_endpoint_config();
    config.receive_datagram_limit(1472).unwrap();
    config.receive_backing_meter(meter.clone());
    (Pair::new(Arc::new(config), server_config()), budget, meter)
}

#[test]
fn encrypted_default_cache_moves_original_backing_to_successor_initial_and_last_ref() {
    let (mut pair, budget, meter) = pair();
    let config = client_config(); // actual default shared TokenMemoryCache(256, 2)
    let store = config.token_store.clone();
    let first = pair.begin_connect(config.clone());
    assert!(!pair.drive_bounded());
    let peer = pair.server.assert_accept();
    pair.finish_connect(first, peer);
    assert!(pair.client_conn_mut(first).stats().frame_rx.new_token > 0);
    let token = store.take_metered("localhost", &meter).unwrap().unwrap();
    assert!(token.backing_meter().unwrap().same_authority(&meter));
    let ptr = token.as_ptr();
    let bytes = token.to_vec();
    // Keep only this original, genuinely received token for the next Initial.
    while store.take_metered("localhost", &meter).unwrap().is_some() {}
    store.insert_metered("localhost", token, &meter).unwrap();
    control_records::stop(&mut pair, &[first]);
    assert!(
        budget.used() > 0,
        "Connection retirement cannot refund shared cache"
    );
    let validated = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = validated.clone();
    pair.server.handle_incoming = Box::new(move |incoming| {
        observed.store(
            incoming.remote_address_validated(),
            std::sync::atomic::Ordering::SeqCst,
        );
        IncomingConnectionBehavior::Accept
    });
    let successor = pair.begin_connect(config.clone());
    let original = pair
        .client_conn_mut(successor)
        .validation_token_for_cache_probe()
        .unwrap();
    assert_eq!(
        original.as_ptr(),
        ptr,
        "constructor must move the original backing"
    );
    assert_eq!(original.as_ref(), bytes.as_slice());
    assert!(
        store.take_metered("localhost", &meter).unwrap().is_none(),
        "one-time take"
    );
    assert!(!pair.drive_bounded());
    let peer = pair.server.assert_accept();
    pair.finish_connect(successor, peer);
    assert!(
        validated.load(std::sync::atomic::Ordering::SeqCst),
        "peer validates actual Initial token"
    );
    control_records::stop(&mut pair, &[successor]);
    drop(pair);
    assert!(
        budget.used() > 0,
        "shared config still owns real cache layouts"
    );
    drop(config);
    assert!(
        budget.used() > 0,
        "explicit final store Arc still owns cache"
    );
    drop(store);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

// Transparent test wrapper around the exact store from ClientConfig::new.
// Only the instant of budget pressure is injected. No result is fabricated.
struct FullOnInsert {
    inner: Arc<dyn TokenStore>,
    budget: bytes::BackingBudget,
    held: std::sync::Mutex<Option<bytes::BackingBudgetReservation>>,
    calls: std::sync::atomic::AtomicUsize,
}
impl TokenStore for FullOnInsert {
    fn insert(&self, name: &str, token: Bytes) {
        self.inner.insert(name, token);
    }
    fn take(&self, name: &str) -> Option<Bytes> {
        self.inner.take(name)
    }
    fn take_metered(
        &self,
        name: &str,
        meter: &bytes::BackingMeter,
    ) -> Result<Option<Bytes>, crate::TokenStoreError> {
        self.inner.take_metered(name, meter)
    }
    fn insert_metered(
        &self,
        name: &str,
        token: Bytes,
        meter: &bytes::BackingMeter,
    ) -> Result<(), crate::TokenStoreError> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let mut held = self.held.lock().unwrap();
        assert!(
            held.is_none(),
            "only the first real token insertion is targeted"
        );
        *held = Some(
            self.budget
                .try_reserve(self.budget.limit() - self.budget.used())
                .unwrap(),
        );
        drop(held);
        self.inner.insert_metered(name, token, meter)
    }
}

#[test]
fn encrypted_new_token_full_keeps_cache_empty_and_original_first_close_clock() {
    let (mut pair, budget, meter) = pair();
    let mut config = client_config();
    let store = config.token_store.clone();
    let wrapper = Arc::new(FullOnInsert {
        inner: store.clone(),
        budget: budget.clone(),
        held: std::sync::Mutex::new(None),
        calls: Default::default(),
    });
    config.token_store(wrapper.clone());
    let client = pair.begin_connect(config.clone());
    pair.drive_client();
    pair.drive_server();
    pair.drive_client();
    pair.drive_server();
    let peer = pair.server.assert_accept();
    assert!(pair.server_conn_mut(peer).stats().frame_tx.new_token > 0);
    assert_eq!(pair.client_conn_mut(client).stats().frame_rx.new_token, 0);
    let now = pair.time;
    pair.client.drive_incoming(now, pair.server.addr);
    pair.client.process_connection_events(client);
    assert_eq!(wrapper.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    let conn = pair.client_conn_mut(client);
    assert!(
        conn.stats().frame_rx.new_token > 0,
        "real encrypted NEW_TOKEN reached cache"
    );
    let first = conn.retained_error().unwrap();
    assert!(matches!(&first, ConnectionError::TransportError(error)
        if error.code == TransportErrorCode::INTERNAL_ERROR
            && error.reason == "token cache admission refused"));
    // ACKs in this same genuine handshake-tail flight may update RTT before
    // NEW_TOKEN. Use that original state and caller clock, never a new timeout.
    let deadline = conn.original_close_deadline_for_stream_probe(now);
    assert_eq!(conn.poll_timeout(), Some(deadline));
    conn.close(now, 77u32.into(), Bytes::new());
    assert_eq!(conn.retained_error(), Some(first));
    assert_eq!(conn.poll_timeout(), Some(deadline));
    assert!(store.take_metered("localhost", &meter).unwrap().is_none());
    assert!(!budget.quarantined(), "known Full is not dirty inventory");
    drop(wrapper.held.lock().unwrap().take());
    assert!(!pair.drive_bounded());
    assert!(
        matches!(pair.server_conn_mut(peer).retained_error(),
        Some(ConnectionError::ConnectionClosed(error))
            if error.error_code == TransportErrorCode::INTERNAL_ERROR),
        "peer must decrypt original transport Close"
    );
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.server.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(pair.server.endpoint.known_cids(), 0);
    drop(pair);
    drop(config);
    drop(wrapper);
    drop(store);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn unsupported_hard_store_refuses_before_any_connection_route_is_published() {
    let (mut pair, budget, _) = pair();
    let mut config = client_config();
    config.token_store(Arc::new(crate::NoneTokenStore));
    let before = budget.used();
    let result = pair
        .client
        .endpoint
        .connect(pair.time, config, pair.server.addr, "localhost");
    assert!(matches!(result, Err(ConnectError::CryptoOutput(error))
        if error.code == TransportErrorCode::INTERNAL_ERROR
            && error.reason == "token cache take refused"));
    assert_eq!(pair.client.endpoint.open_connections(), 0);
    assert_eq!(pair.client.endpoint.known_cids(), 0);
    assert_eq!(budget.used(), before);
    assert!(!budget.quarantined());
}
