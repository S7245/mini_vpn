//! A2 Route 2 tier tracer. Native roles receive the Node's protocol-tier
//! meter from the Quinn recv stream, but every App-side downlink inventory
//! they fund (queue/stock, Source and writer cold layouts, output slabs) must
//! admit at the App ceiling `limit - protocol_reserve`, never inside the
//! protocol reserve that keeps honest Quinn receive away from Full.
use super::*;

const LIMIT: usize = 4 << 20;
const RESERVE: usize = 2 << 20;

fn launch(meter: &bytes::BackingMeter) -> (NativeLaunchRequest, impl Sized) {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let request = NativeLaunchRequest {
        uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(GapPendingSourceReader {
                chunk: None,
                polls: Arc::new(AtomicUsize::new(0)),
                meter: meter.clone(),
                source_bytes: 0,
            }),
            Box::new(Stream { probe: Arc::new(Probe::default()), fail: false }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    };
    (request, tx)
}

#[tokio::test]
async fn native_roles_admit_app_inventory_at_app_ceiling_not_protocol_reserve() {
    let budget = bytes::BackingBudget::with_protocol_reserve(LIMIT, RESERVE).unwrap();
    let node = bytes::BackingMeter::with_protocol_budget(&budget);
    let (clock, _owner) = super::output_capacity::cold_clock(&node);
    let app_ceiling = LIMIT - RESERVE;
    // The App tier is exactly exhausted; only the protocol reserve is free.
    let held = budget.try_reserve(app_ceiling - budget.used()).unwrap();
    assert_eq!(budget.used(), app_ceiling);
    let (request, tx) = launch(&node);
    let refused = FlowRoles::new_native_probed(request, clock.clone(), Some(node.clone()));
    let used = budget.used();
    let error = match refused {
        Ok(roles) => {
            drop(roles);
            panic!("Native App inventory entered the protocol reserve: used={used} ceiling={app_ceiling}");
        }
        Err((_request, error)) => error,
    };
    assert_eq!(error, bytes::MeteredCopyError::BudgetFull);
    assert_eq!(used, app_ceiling, "known App refusal must not consume protocol reserve");
    assert!(!budget.quarantined());
    drop((held, tx));
    // Control: with App headroom the same request is admitted, and the roles
    // retain the Node's exact authority for Quinn chunk identity checks.
    let (request, tx) = launch(&node);
    let roles = FlowRoles::new_native_probed(request, clock, Some(node.clone()))
        .ok()
        .expect("App headroom admits the original Native roles");
    let meter = roles.native_backing_meter().expect("budgeted Native roles meter").clone();
    assert!(meter.same_authority(&node));
    assert!(meter.same_budget(&node));
    assert!(budget.used() <= app_ceiling);
    drop((roles, tx, meter));
    assert!(!budget.quarantined());
}
