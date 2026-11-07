use super::*;

#[tokio::test]
async fn original_native_reader_metadata_full_retains_input_and_settles_known_refusal() {
    let budget = crate::client_tun::compatibility_runtime::flow_custody::supervision::roles::native::stock_tests::budget_with_old_headroom(65536);
    let meter = bytes::BackingMeter::with_budget(&budget);
    let chunk = NativeTcpChunk {
        offset: 0,
        bytes: bytes::Bytes::copy_from_slice_metered(b"abcd", &meter).unwrap(),
    };
    let polls = Arc::new(AtomicUsize::new(0));
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let (clock, _owner) = cold_clock(&meter);
    let mut roles = FlowRoles::new_native_with_clock(
        NativeLaunchRequest {
            uplink_observer: None,
            handle,
            epoch: 7,
            native: NativeTcpRelayStream::new(
                Box::new(GapPendingSourceReader {
                    chunk: Some(chunk),
                    polls: polls.clone(),
                    meter: meter.clone(),
                    source_bytes: 0,
                }),
                Box::new(Stream {
                    probe: Arc::new(Probe::default()),
                    fail: false,
                }),
            ),
            engine: NativeTcpRelayEngine::ChunkPump,
            read_floor: false,
            rx,
            back_tx,
            read_credit_rx,
            downlink_backpressure: DownlinkBackpressureConfig::default(),
        },
        clock,
    )
    .ok()
    .unwrap();
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().unwrap());
    assert!(roles.commit_native());
    let original_allocations = meter.snapshot().allocations;
    let held = budget.try_reserve(budget.limit() - budget.used()).unwrap();
    roles.start();
    until(&mut roles, false, |r| {
        r.native_reader_received_bytes() == Some(0)
    })
    .await;
    assert_eq!(polls.load(Ordering::SeqCst), 0, "known metadata Full refuses before taking original input");
    assert!(!roles.quarantined());
    assert!(back.try_recv().is_err(), "no prefix can escape known Full");
    assert_eq!(
        meter.snapshot().allocations,
        original_allocations,
        "original unread input and protected cold stock remain custodied"
    );
    drop(tx);
    roles.stop();
    until(&mut roles, true, |roles| roles.native_reader_receipt_present() == Some(true)).await;
    assert!(!roles.settled(), "original capacity IO retains Source custody");
    assert_eq!(roles.join_count(), 4);
    assert_eq!(roles.native_reader_receipt_present(), Some(true));
    assert_eq!(roles.native_direct_source_received_bytes(), Some(0));
    let Some(super::super::super::super::roles::OriginalFailure::ReportedIo(error)) =
        roles.take_original_failure()
    else {
        panic!("original capacity IO must remain available");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::WouldBlock);
    assert_eq!(
        _owner
            .ledger()
            .first_failure
            .as_ref()
            .unwrap()
            .facts()
            .io_kind,
        Some(std::io::ErrorKind::WouldBlock)
    );
    assert_eq!(
        _owner.ledger().additional_failures,
        0,
        "JOIN must not report original capacity error twice"
    );
    assert!(roles.take_original_failure().is_none());
    drop(error);
    until(&mut roles, true, FlowRoles::settled).await;
    assert!(!budget.quarantined());
    drop((held, roles, back));
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn hard_carrier_rejected_by_legacy_runs_through_original_saved_roles() {
    for engine in [
        NativeTcpRelayEngine::ChunkPump,
        NativeTcpRelayEngine::PermitPump,
    ] {
        let budget = crate::client_tun::compatibility_runtime::flow_custody::supervision::roles::native::stock_tests::budget_with_old_headroom(65536);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let native = NativeTcpRelayStream::new(
            Box::new(OutOfOrderSourceReader {
                chunks: [NativeTcpChunk {
                    offset: 0,
                    bytes: bytes::Bytes::copy_from_slice_metered(b"original", &meter).unwrap(),
                }]
                .into_iter()
                .collect(),
                meter: meter.clone(),
                source_bytes: 0,
            }),
            Box::new(Stream {
                probe: Arc::new(Probe::default()),
                fail: false,
            }),
        );
        let native = native
            .try_start_legacy()
            .err()
            .expect("same original carrier");
        assert_eq!(native.reader.source_read_bytes(), Some(0));
        let mut sockets = SocketSet::new(Vec::new());
        let handle = sockets.add(Socket::new(
            SocketBuffer::new(vec![0; 8]),
            SocketBuffer::new(vec![0; 8]),
        ));
        let (tx, rx) = crate::client_tun::uplink::channel(2);
        let (back_tx, mut back) = crate::client_tun::relay_channel::channel(2);
        let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
        let (clock, _owner) = cold_clock(&meter);
        let mut roles = FlowRoles::new_native_with_clock(
            NativeLaunchRequest {
            uplink_observer: None,
                handle,
                epoch: 7,
                native,
                engine,
                read_floor: false,
                rx,
                back_tx,
                read_credit_rx,
                downlink_backpressure: DownlinkBackpressureConfig::default(),
            },
            clock,
        )
        .ok()
        .unwrap();
        let mut window = crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new();
        roles.prepare(window.take_reader().unwrap());
        assert!(roles.commit_native());
        roles.start();
        until(&mut roles, false, |r| {
            r.native_reader_received_bytes() == Some(8)
        })
        .await;
        loop {
            let (_, event) = tokio::time::timeout(std::time::Duration::from_secs(1), back.recv())
                .await
                .unwrap()
                .unwrap();
            if let RelayEvent::Data {
                bytes,
                downstream_permit,
                ..
            } = event
            {
                let crate::tcp_downlink_pump::OwnedPayload::Native(ref backing) = bytes else {
                    panic!("typed output")
                };
                assert_eq!(&backing[..], b"original");
                assert!(backing.backing_meter().unwrap().same_authority(&meter));
                window.admit(bytes.len(), 0, bytes.len());
                window.publish();
                drop((bytes, downstream_permit));
                break;
            }
        }
        drop(tx);
        roles.stop();
        until(&mut roles, true, FlowRoles::settled).await;
        assert_eq!(roles.join_count(), 4);
        assert_eq!(roles.native_reader_receipt_present(), Some(true));
        assert!(roles.take_original_failure().is_none());
        assert!(!roles.quarantined() && !budget.quarantined());
        drop((roles, back));
        assert_eq!(budget.used(), 0);
    }
}

pub(super) fn cold_clock(
    meter: &bytes::BackingMeter,
) -> (
    crate::shared::errors::FailureClock,
    Arc<crate::client_tun::compatibility_runtime::SessionControl>,
) {
    use crate::{
        client_tun::compatibility_runtime::SessionControl,
        shared::errors::{FailureOrder, FailurePhase, QuietFailureSink},
    };
    let mut owner = SessionControl::default();
    owner.app_meter = Some(meter.clone());
    let owner = Arc::new(owner);
    let sink: Arc<dyn QuietFailureSink> = owner.clone();
    let clock = Arc::new(FailureOrder::default());
    clock
        .bind(
            Arc::downgrade(&sink),
            SessionControl::origin(FailurePhase::TcpOpen),
            Some(meter.clone()),
        )
        .unwrap();
    (clock, owner)
}
