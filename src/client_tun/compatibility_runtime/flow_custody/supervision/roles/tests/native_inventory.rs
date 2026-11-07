//! Cold outcome corruption probes use the original reader settlement, no actors.
use super::*;
impl Cold {
    pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles::native) fn producer_payload_matches(&self, expected: &Arc<usize>) -> bool {
        self.callbacks.as_ref().is_some_and(|stock| stock.source_payload_matches(Field::Notification, expected))
    }
}

struct ColdOutcome { outcome: Outcome, _cold: Option<Cold>, _sink: Option<Arc<cancellation::Facts>> }
impl std::ops::Deref for ColdOutcome { type Target = Outcome; fn deref(&self) -> &Outcome { &self.outcome } }
impl std::ops::DerefMut for ColdOutcome { fn deref_mut(&mut self) -> &mut Outcome { &mut self.outcome } }
async fn cold(meter: Option<bytes::BackingMeter>) -> ColdOutcome {
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
    ));
    let shared = Shared::configured_metered(
        TcpRelayEngine::ContinuousPump,
        DownlinkBackpressureConfig::default(),
        meter.clone(),
    )
    .unwrap();
    let (_credit, credit) = watch::channel(RelayReadCredit::default());
    let (signals, _signals) = mpsc::channel(1);
    let (_stop, stop) = oneshot::channel();
    let (start, gate) = oneshot::channel();
    drop(start);
    let mut source = None;
    let mut sink = None;
    let mut failures = None;
    if let Some(meter) = &meter {
        use crate::shared::errors::*;
        let facts = Arc::new(cancellation::Facts::default());
        let owner: Arc<dyn QuietFailureSink> = facts.clone();
        let clock = Arc::new(FailureOrder::default());
        clock.bind(Arc::downgrade(&owner), FailureOrigin { phase: FailurePhase::TcpOpen, resource: FailureResource::Session, resource_id: 0, attempt_id: None }, Some(meter.clone())).unwrap();
        assert!(shared.bind_failures(clock.clone()));
        source = Some(Cold::prepare(meter, clock.clone()).unwrap());
        sink = Some(facts); failures = Some(clock);
    }
    let lease = source.as_mut().and_then(Cold::take_lease);
    let outcome = run(
        Request {
            producer: None,
            backing_meter: meter,
            handle,
            epoch: 7,
            reader: None.into(),
            shared,
            credit: Some(credit).into(),
            signals: Some(signals).into(),
            local_finish: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stop: Some(stop).into(),
            service_floor: false,
        },
        gate,
        failures, lease,
    ).await;
    ColdOutcome { outcome, _cold: source, _sink: sink }
}

#[tokio::test]
async fn cold_reader_receipt_checks_empty_authority_and_real_pending_identity() {
    for fault in 0..5 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let mut outcome = cold(Some(meter.clone())).await;
        let expected_cold = outcome
            .state
            .request
            .shared
            .queue
            .bytes_queue()
            .unwrap()
            .native_waiter_layout_for_test()
            .unwrap() + SourceCallbacks::layout_bytes() + crate::shared::errors::FailureOrder::layout_bytes();
        assert_eq!(budget.used(), expected_cold);
        assert!(!budget.quarantined());
        let payload = meter.snapshot();
        assert_eq!(
            (
                payload.allocations,
                payload.payload_bytes,
                payload.physical_bytes
            ),
            (0, 0, 0)
        );
        assert_eq!(outcome.state.assembler.buffered_bytes(), 0);
        assert!(outcome.state.pending.is_none() && outcome.state.rejected.is_none());
        *outcome.state.failure = Some(std::io::Error::from_raw_os_error(17));
        outcome.state.error_order = Some(19);
        match fault {
            0 => outcome.state.assembler = NativeTcpChunkAssembler::default(),
            1 => {
                outcome.state.pending = Some(OwnedPayload::Legacy(b"same".to_vec()));
                outcome.state.received_bytes = 4;
            }
            2 => {
                let foreign = bytes::BackingMeter::new();
                outcome.state.pending = Some(OwnedPayload::Native(
                    (bytes::Bytes::copy_from_slice_metered(b"same", &foreign).unwrap()).into(),
                ));
                outcome.state.received_bytes = 4;
            }
            3 => {
                outcome.state.rejected = Some(crate::upstream::NativeTcpChunk {
                    offset: 0,
                    bytes: bytes::Bytes::from_static(b"same"),
                });
                outcome.state.received_bytes = 4;
            }
            _ => (),
        }
        assert!(
            outcome
                .discard_local(if fault == 4 { None } else { Some(&meter) })
                .is_none()
        );
        assert!(outcome.quarantined());
        assert_eq!(outcome.error_order(), Some(19));
        assert_eq!(outcome.take_error().unwrap().raw_os_error(), Some(17));
        drop(outcome);
        assert!(budget.quarantined());
        assert_eq!(
            budget.used(),
            expected_cold,
            "Q retains the actual complete callback stock; no sentinel-only refund"
        );
        let payload = meter.snapshot();
        assert_eq!(
            (
                payload.allocations,
                payload.payload_bytes,
                payload.physical_bytes
            ),
            (0, 0, 0)
        );
    }
    let mut pristine = cold(None).await;
    assert!(pristine.discard_local(None).is_some());
    assert!(!pristine.quarantined());
}

impl Roles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_terminal_for_test(
        &self,
    ) -> Option<(&'static str, &'static str)> {
        self.shared.retained_terminal_for_test()
    }
}

impl Roles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_permits_for_test(
        &self,
        identity: &crate::tcp_downlink_pump::PermitQueueIdentity,
    ) -> Option<(SocketHandle, u64, usize, usize, usize)> {
        let Role::Joined(dispatcher) = &self.dispatcher else {
            return None;
        };
        dispatcher.retained_permits_for_test(identity)
    }
}

#[path = "native_inventory/cancellation.rs"]
mod cancellation;
#[path = "native_inventory/capacity.rs"]
mod capacity;

#[path = "native_inventory/stock_credit.rs"]
mod stock_credit;

#[path = "native_inventory/gap_pressure.rs"]
mod gap_pressure;

// Passive diagnostics of original fields; never issues a receipt or polls work.
impl super::super::super::FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn native_retirement_for_test(&self) -> Option<String> {
        let r = self.native.as_ref()?;
        let monitor = match &r.monitor { Role::Joined(o) => Some(o.drained), _ => None };
        let dispatcher = match (&r.dispatcher, &r.reader) {
            (Role::Joined(d), Role::Joined(s)) => Some(d.audit(s.queued(), s.closed_sent())),
            _ => None,
        };
        Some(format!("started={} stopping={} stop_notified={} producers={:?} JOIN={} receipts(writer,reader,pump)={:?} source_cold={} monitor={monitor:?} dispatcher={dispatcher:?} data_disposed={} stock={:?} stock_retiring={} writer_callbacks={} uplink_observer={} Q={}",
            r.started, r.stopping, r.stop_notified, r.producers, r.join_count(),
            (r.writer_receipt.is_some(), r.reader_receipt.is_some(), r.pump_receipt.is_some()),
            r.source.is_some(), r.data_disposed(), r.stock.as_ref().map(|s| (s.done(), s.failed())),
            r.stock_retiring, r.writer_callbacks.is_some(), r.uplink_observer.is_some(), r.quarantined()))
    }
}
