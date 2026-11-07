//! Original encrypted RESET traffic at the sent-credit/GSO common gate.
use super::*;
use crate::congestion::{Controller, ControllerFactory};

use std::{any::Any, sync::Mutex};

#[derive(Clone)]
pub(super) struct Receipts(pub(super) Arc<Mutex<Vec<(u64, u64)>>>);
struct Observed {
    inner: Box<dyn Controller>,
    receipts: Receipts,
}
impl ControllerFactory for Receipts {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(Observed {
            inner: Arc::new(crate::congestion::CubicConfig::default()).build(now, mtu),
            receipts: self.as_ref().clone(),
        })
    }
}
impl Controller for Observed {
    fn on_sent(&mut self, now: Instant, bytes: u64, pn: u64) {
        self.receipts.0.lock().unwrap().push((bytes, pn));
        self.inner.on_sent(now, bytes, pn);
    }
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        limited: bool,
        rtt: &crate::connection::RttEstimator,
    ) {
        self.inner.on_ack(now, sent, bytes, limited, rtt);
    }
    fn on_end_acks(&mut self, now: Instant, flight: u64, limited: bool, largest: Option<u64>) {
        self.inner.on_end_acks(now, flight, limited, largest);
    }
    fn on_congestion_event(&mut self, now: Instant, sent: Instant, persistent: bool, lost: u64) {
        self.inner.on_congestion_event(now, sent, persistent, lost);
    }
    fn on_mtu_update(&mut self, mtu: u16) {
        self.inner.on_mtu_update(mtu);
    }
    fn window(&self) -> u64 {
        self.inner.window()
    }
    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            receipts: self.receipts.clone(),
        })
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

#[test]
fn history_capacity_does_not_fail_at_completed_gso_batch_boundary() {
    real_gso_boundary(8, false);
}
#[test]
fn history_capacity_preserves_encoded_gso_prefix_and_congestion_receipt() {
    real_gso_boundary(16, true);
}
fn real_gso_boundary(max_datagrams: usize, terminal: bool) {
    let budget = bytes::BackingBudget::new(2 * 1024 * 1024).unwrap();
    let mut endpoint = admission_endpoint_config();
    endpoint.receive_backing_meter(bytes::BackingMeter::with_budget(&budget));
    endpoint.receive_datagram_limit(1472).unwrap();
    endpoint.receive_sent_history_limits(4, 4).unwrap();
    // This fixture exhausts packet history, after admitting its 4096 controls.
    endpoint.receive_control_record_limit(4096).unwrap();
    let mut server = server_config();
    Arc::get_mut(&mut server.transport)
        .unwrap()
        .max_concurrent_uni_streams(4096u32.into())
        .mtu_discovery_config(None);
    let receipts = Receipts(Arc::new(Mutex::new(Vec::new())));
    let mut client = client_config_with_deterministic_pns();
    Arc::get_mut(&mut client.transport)
        .unwrap()
        .mtu_discovery_config(None)
        .pad_to_mtu(true)
        .congestion_controller_factory(Arc::new(receipts.clone()));
    let mut pair = Pair::new(Arc::new(endpoint), server);
    let (ch, peer) = pair.connect_with(client);
    for _ in 0..4096 {
        let stream = pair.client_conn_mut(ch).streams().open(Dir::Uni).unwrap();
        pair.client_send(ch, stream)
            .reset(VarInt::from_u32(42))
            .unwrap();
    }
    receipts.0.lock().unwrap().clear();
    let (pn_before, occupied_before) = pair.client_conn_mut(ch).sent_history_data_state_for_probe();
    assert_eq!(occupied_before, 0);
    let mut wire = Vec::new();
    let now = pair.time + Duration::from_millis(100);
    let transmit = pair
        .client_conn_mut(ch)
        .poll_transmit(now, max_datagrams, &mut wire)
        .unwrap();
    let segment = transmit.segment_size.unwrap_or(transmit.size);
    let segments = transmit.size.div_ceil(segment);
    let (pn_after, occupied_after) = pair.client_conn_mut(ch).sent_history_data_state_for_probe();
    assert_eq!(
        segments, 8,
        "original pad-to-MTU profile fills eight real segments"
    );
    assert_eq!(pn_after - pn_before, 8);
    assert_eq!(occupied_after, 8);
    assert_eq!(
        *receipts.0.lock().unwrap(),
        [(transmit.size as u64, pn_after - 1)]
    );
    assert_eq!(
        pair.client_conn_mut(ch).retained_error().is_some(),
        terminal
    );
    if terminal {
        let (space, before, after, occupied) = pair
            .client_conn_mut(ch)
            .sent_history_rejection_for_probe()
            .unwrap();
        assert_eq!(space, crate::packet::SpaceId::Data);
        assert_eq!((before, after, occupied), (pn_after, pn_after, 8));
    }
    // Endpoint routing queues ConnectionEvent; only the original connection
    // receive phase decrypts/applies it. Observe after both common phases.
    let before = pair.server_conn_mut(peer).stats();
    for bytes in wire[..transmit.size].chunks(segment) {
        pair.server
            .inbound
            .push_back((now, None, BytesMut::from(bytes)));
    }
    pair.server.drive_incoming(now, pair.client.addr);
    pair.server.process_connection_events(peer);
    let after = pair.server_conn_mut(peer).stats();
    assert_eq!(after.udp_rx.datagrams - before.udp_rx.datagrams, 8);
    assert_eq!(
        after.udp_rx.bytes - before.udp_rx.bytes,
        transmit.size as u64
    );
    assert!(
        after.frame_rx.reset_stream > before.frame_rx.reset_stream,
        "returned ciphertext must be applied by the original encrypted reader"
    );
    eprintln!("history-gso: max={max_datagrams} segments={segments} bytes={} pn={pn_before}..{pn_after} occupied={occupied_after} cc_receipts=1 terminal={terminal}",transmit.size);
    assert!(!budget.quarantined());
    control_records::stop(&mut pair, &[ch]);
    drop(pair);
    assert_eq!(budget.used(), 0);
}
