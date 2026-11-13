//! N5-OBS (D241/D243) tracer: the read-only `OpenCustody::census` follows a
//! really admitted flow through a real close request, a Flow-Q row and a real
//! Session-Q source (stale close identity), never mutating the root.
use super::super::super::rearm::FlowAdmission;
use super::super::super::rearm::physical_flow::tests::admission::{NeverOpen, receiving_pair};
use super::super::*;
use crate::fake_ip::FakeIpPool;
use crate::shared::TargetAddr;

#[tokio::test]
async fn n5_obs_census_follows_real_flow_close_and_q_scopes() {
    let (_client, mut server) = receiving_pair().await;
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    assert_eq!(owner.opens().census(), FlowCensus::default(), "empty root");
    let (dispatch, _receiver) = OpenDispatch::channel(4, Some(owner.clone()));
    let (handle, mut ctx, mut pool) = (server.handle, SocketCtx::new(10000), FakeIpPool::new());
    dispatch.begin_turn().await;
    dispatch.admit_flow(
        Arc::new(NeverOpen::default()),
        FlowAdmission {
            handle,
            socket: server.sockets.get_mut::<TcpSocket>(handle),
            ctx: &mut ctx,
            fake_pool: &mut pool,
            fake_ip: None,
            target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
            now: 0,
        },
    );
    let (index, epoch, id) = owner
        .opens()
        .slots
        .iter()
        .enumerate()
        .find_map(|(i, slot)| {
            let slot = slot.as_ref()?;
            match &slot.phase {
                OpenPhase::Flow(flow) => Some((i, slot.epoch, flow.id)),
                _ => None,
            }
        })
        .expect("actual admitted flow");
    let admitted = owner.opens().census();
    assert_eq!(
        (admitted.live, admitted.closing, admitted.quarantined),
        (1, 0, 0),
        "{admitted:?}"
    );
    // Real close request with the row's own identity ⇒ Closing.
    assert!(dispatch.request_flow_close(handle, epoch, Some(id), None, None));
    let closing = owner.opens().census();
    assert_eq!(
        (closing.live, closing.closing, closing.flow_q_rows),
        (1, 1, 0),
        "{closing:?}"
    );
    // Flow-scope Q row: stage + root registration (as the soft-Q tracer does).
    {
        let mut opens = owner.opens();
        match &mut opens.slots.get_mut(index).unwrap().phase {
            OpenPhase::Flow(flow) => flow.stage = FlowStage::Quarantined,
            _ => panic!("actual Flow"),
        }
        assert!(opens.register_flow_q(index, &owner));
    }
    let flow_q = owner.opens().census();
    assert_eq!(
        (flow_q.closing, flow_q.quarantined, flow_q.flow_q_rows),
        (0, 1, 1),
        "{flow_q:?}"
    );
    assert!(!owner.ledger().session_quarantined());
    // Real Session-Q source: a stale close identity for the same row.
    assert!(dispatch.request_flow_close(handle, epoch.wrapping_add(1), Some(id), None, None));
    assert!(owner.ledger().session_quarantined());
    assert_eq!(
        owner.opens().census(),
        flow_q,
        "census never mutates the root"
    );
    // Leave the fixture's root as the soft-Q tracer does (row is Q-held).
    let row = owner.opens().slots.take(index);
    std::mem::forget(row);
}
