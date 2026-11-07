//! Original custodied queue constructor, source storage and real capabilities.
//! This component tracer does not claim F publication or public recovery.
use super::*;
use crate::tcp_downlink_pump::DownstreamPermitReleaseMode;

#[tokio::test]
async fn configured_capacity_modes_keep_original_bounds_and_permit_policy() {
    use crate::tcp_downlink_pump::ByteCustodyError;
    for high in [0, 7, 65_536] {
        let cfg = DownlinkBackpressureConfig {
            high_bytes: high,
            low_bytes: high / 2,
        };
        let shared = super::super::continuous::Shared::configured(
            TcpRelayEngine::OrderedEgressPermitPump,
            cfg,
        )
        .unwrap();
        let q = shared.queue.leased_queue().unwrap();
        let capacity = high.max(1);
        assert_eq!(q.snapshot_now().capacity_bytes, capacity);
        let mut pending = Some(vec![19; capacity + 1]);
        assert!(matches!(
            q.push_custodied(&mut pending).await,
            Err(ByteCustodyError::Rejected)
        ));
        assert_eq!(pending.as_ref().unwrap().len(), capacity + 1);
        assert_eq!(q.snapshot_now().owned_bytes(), 0);
        pending.as_mut().unwrap().truncate(capacity);
        q.push_custodied(&mut pending).await.unwrap();
        let mut held = None;
        q.recv_custodied(capacity, &mut held).await.unwrap();
        let cap = &mut held.as_mut().unwrap().permit;
        assert_eq!(
            cap.release_mode(),
            DownstreamPermitReleaseMode::OnEgressDrain
        );
        assert!(q.owns_permit(cap, capacity));
        q.dispose_custodied().unwrap().unwrap();
        assert!(!q.physical_zero());
        assert_eq!(cap.release(capacity), capacity);
        drop(held.take());
        assert!(q.physical_zero());
    }
    let shared = super::super::continuous::Shared::configured(
        TcpRelayEngine::PermitPump,
        DownlinkBackpressureConfig {
            high_bytes: 7,
            low_bytes: 2,
        },
    )
    .unwrap();
    let q = shared.queue.leased_queue().unwrap();
    assert_eq!(
        q.snapshot_now().capacity_bytes,
        CONTINUOUS_TCP_RELAY_QUEUE_BYTES
    );
    q.push_custodied(&mut Some(vec![7; 4])).await.unwrap();
    let mut held = None;
    q.recv_custodied(4, &mut held).await.unwrap();
    assert_eq!(
        held.as_ref().unwrap().permit.release_mode(),
        DownstreamPermitReleaseMode::OnAdmission
    );
    q.dispose_custodied().unwrap().unwrap();
    drop(held.take());
    assert!(q.physical_zero());
}

#[tokio::test]
async fn ordered_original_queue_retains_egress_mode_and_actual_external_lease() {
    let shared = super::super::continuous::Shared::new(TcpRelayEngine::OrderedEgressPermitPump);
    assert!(
        shared.is_some(),
        "Ordered needs the same original custodied queue authority"
    );
    let shared = shared.unwrap();
    let q = shared.queue.leased_queue().unwrap();
    assert_eq!(
        q.snapshot_now().capacity_bytes,
        native_tcp_permit_queue_capacity_bytes(DownlinkBackpressureConfig::default())
    );
    let mut pending = Some(vec![41; 4]);
    q.push_custodied(&mut pending).await.unwrap();
    assert!(pending.is_none());
    let mut held = None;
    assert!(q.recv_custodied(4, &mut held).await.unwrap());
    let cap = &mut held.as_mut().unwrap().permit;
    assert_eq!(
        cap.release_mode(),
        DownstreamPermitReleaseMode::OnEgressDrain
    );
    assert!(q.owns_permit(cap, 4));
    let receipt = q.dispose_custodied().unwrap().unwrap();
    assert_eq!(receipt.consumed, 4);
    assert_eq!(receipt.discarded_bytes, 0);
    assert_eq!(q.snapshot_now().leased_bytes, 4);
    assert!(
        !q.physical_zero(),
        "actual external cap is not local queue disposal"
    );
    assert_eq!(cap.release(2), 2);
    assert_eq!(q.snapshot_now().leased_bytes, 2);
    assert!(q.owns_permit(cap, 2));
    assert!(!q.physical_zero());
    assert_eq!(cap.release(2), 2);
    drop(held.take());
    assert!(q.physical_zero());
}
