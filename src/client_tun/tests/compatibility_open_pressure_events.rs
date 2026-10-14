//! Actual relay-event observations through the saved session and original loop.
//! The bridge is passive: it can enqueue one bounded event and read one value
//! snapshot, but cannot acquire loop resources or hold a sender alive.

use super::*;
use crate::client_tun::legacy_event_loop::relay_events::RelayEventTestKind;
use crate::client_tun::legacy_event_loop::{RelayEventTestBridge, RelayEventTestSnapshot};
use crate::client_tun::{RelayClose, RelayEvent};
use std::future::Future;
use std::sync::Arc;

#[derive(Clone, Copy, Debug)]
enum RejectedRelayEvent {
    Data,
    DataReady,
    AckDrainHint,
    Closed,
}

impl RejectedRelayEvent {
    const ALL: [Self; 4] = [
        Self::Data,
        Self::DataReady,
        Self::AckDrainHint,
        Self::Closed,
    ];

    fn kind(self) -> RelayEventTestKind {
        match self {
            Self::Data => RelayEventTestKind::Data,
            Self::DataReady => RelayEventTestKind::DataReady,
            Self::AckDrainHint => RelayEventTestKind::AckDrainHint,
            Self::Closed => RelayEventTestKind::Closed,
        }
    }

    fn event(self, epoch: u64) -> RelayEvent {
        match self {
            Self::Data => RelayEvent::Data {
                epoch,
                bytes: vec![0x5a],
                stream_service_window: Default::default(),
                downstream_permit: None,
            },
            Self::DataReady => RelayEvent::DataReady { epoch },
            Self::AckDrainHint => RelayEvent::AckDrainHint {
                epoch,
                gap_ms: 100,
                remote_to_global_rx_bytes: 64 * 1024,
            },
            Self::Closed => RelayEvent::Closed(RelayClose {
                epoch,
                direction: "test",
                reason: "stale",
            }),
        }
    }
}

async fn snapshot_for<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    bridge: &RelayEventTestBridge,
    after_generation: u64,
    handle: SocketHandle,
    epoch: u64,
    kind: RelayEventTestKind,
) -> Option<RelayEventTestSnapshot> {
    for _ in 0..128 {
        turn(session, apps, false).await;
        if let Some(snapshot) = bridge.snapshot()
            && snapshot.generation > after_generation
            && snapshot.handle == handle
            && snapshot.event_epoch == epoch
            && snapshot.kind == kind
        {
            return Some(snapshot);
        }
    }
    None
}

fn missing_context_handle() -> SocketHandle {
    let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
    let mut handle = SocketHandle::default();
    for _ in 0..=crate::client_tun::MAX_TOTAL_LISTENERS {
        handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
        ));
    }
    handle
}

fn assert_same_current_control(observed: RelayEventTestSnapshot, current: RelayEventTestSnapshot) {
    assert_eq!(observed.context, current.context);
    assert_eq!(observed.dirty, current.dirty);
    assert_eq!(observed.d4_deadline, current.d4_deadline);
    assert_eq!(observed.relay_gap_ack_drain, current.relay_gap_ack_drain);
}

async fn assert_rejected_event<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut RealApps,
    upstream: &PendingOpens,
    bridge: &RelayEventTestBridge,
    current_handle: SocketHandle,
    current_epoch: u64,
    target_handle: SocketHandle,
    target_epoch: u64,
    event: RejectedRelayEvent,
    missing_context: bool,
) {
    let current_before = bridge.snapshot().map_or(0, |snapshot| snapshot.generation);
    assert!(
        bridge
            .try_send(
                current_handle,
                RelayEvent::AckDrainHint {
                    epoch: current_epoch,
                    gap_ms: 100,
                    remote_to_global_rx_bytes: 64 * 1024,
                },
            )
            .is_accepted(),
        "the passive bridge must inject one current control without awaiting"
    );
    let current = snapshot_for(
        session,
        apps,
        bridge,
        current_before,
        current_handle,
        current_epoch,
        RelayEventTestKind::AckDrainHint,
    )
    .await
    .expect("current AckDrainHint disposition snapshot");
    assert!(!current.rejected, "current event must retain its branch");
    assert_eq!(
        current.context.map(|context| context.epoch),
        Some(current_epoch)
    );
    assert!(
        current.d4_deadline.is_some(),
        "current AckDrainHint arms D4"
    );
    let current_inventory = inventory(&session.control, upstream);

    assert!(
        bridge
            .try_send(target_handle, event.event(target_epoch))
            .is_accepted(),
        "the passive bridge must inject one rejected event without awaiting"
    );
    let rejected = snapshot_for(
        session,
        apps,
        bridge,
        current.generation,
        target_handle,
        target_epoch,
        event.kind(),
    )
    .await
    .expect("rejected relay-event disposition snapshot");
    assert!(
        rejected.rejected,
        "{event:?} must be rejected before its branch runs"
    );
    assert_eq!(inventory(&session.control, upstream), current_inventory);

    if !missing_context {
        assert_same_current_control(rejected, current);
        return;
    }

    assert_eq!(rejected.context, None, "opaque target has no SocketCtx");
    assert!(!rejected.dirty, "missing context cannot become dirty");
    assert_eq!(rejected.d4_deadline, None, "missing context cannot arm D4");
    assert_eq!(
        rejected.relay_gap_ack_drain, current.relay_gap_ack_drain,
        "missing context cannot change global relay-gap state"
    );

    let probe_epoch = current_epoch.wrapping_sub(1);
    assert!(
        bridge
            .try_send(current_handle, RelayEvent::DataReady { epoch: probe_epoch })
            .is_accepted(),
        "the passive bridge must inject one old-epoch control probe without awaiting"
    );
    let after = snapshot_for(
        session,
        apps,
        bridge,
        rejected.generation,
        current_handle,
        probe_epoch,
        RelayEventTestKind::DataReady,
    )
    .await
    .expect("post-missing current-control snapshot");
    assert!(after.rejected, "old-epoch probe must be rejected");
    assert_same_current_control(after, current);
    assert_eq!(inventory(&session.control, upstream), current_inventory);
}

#[tokio::test(flavor = "current_thread")]
async fn stale_or_missing_global_events_preserve_current_d4_and_custody() {
    let (mut session, mut apps, upstream, _downlink) = fixture_with_config(4096, |config| {
        config.d3_egress_actor = true;
        config.d4_stalled_read_service = true;
    });
    let captured = capture(async {
        assert!(ready(&mut session, &mut apps, &upstream).await);
        let bridge = session
            .control
            .relay_event_test_bridge()
            .expect("loop-local bridge must be discoverable after startup");
        let bridge_weak = Arc::downgrade(&bridge);
        let (handle, epoch) = session
            .control
            .opens()
            .slots
            .iter()
            .flatten()
            .find(|slot| matches!(slot.phase, OpenPhase::Worker(_)))
            .map(|slot| (slot.handle, slot.epoch))
            .expect("first real open worker");
        let stale_epoch = epoch.wrapping_sub(1);
        for event in RejectedRelayEvent::ALL {
            assert_rejected_event(
                &mut session,
                &mut apps,
                &upstream,
                &bridge,
                handle,
                epoch,
                handle,
                stale_epoch,
                event,
                false,
            )
            .await;
        }

        let missing_handle = missing_context_handle();
        for event in RejectedRelayEvent::ALL {
            assert_rejected_event(
                &mut session,
                &mut apps,
                &upstream,
                &bridge,
                handle,
                epoch,
                missing_handle,
                epoch,
                event,
                true,
            )
            .await;
        }
        (bridge, bridge_weak)
    })
    .await;

    let (bridge, bridge_weak) = match captured {
        Ok(bridges) => bridges,
        Err(panic) => {
            let cleanup = cleanup(&mut session, &upstream).await;
            assert!(!cleanup.timed_out);
            std::panic::resume_unwind(panic);
        }
    };
    let cleanup = cleanup(&mut session, &upstream).await;
    drop(bridge);
    assert!(!cleanup.timed_out);
    assert_eq!(cleanup.entered, cleanup.dropped);
    assert!(bridge_weak.upgrade().is_none());
    assert!(session.control.relay_event_test_bridge().is_none());
}
