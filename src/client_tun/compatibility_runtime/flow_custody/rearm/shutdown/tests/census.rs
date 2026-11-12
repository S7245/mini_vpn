//! Census-only boundary shapes using an actually admitted smoltcp RX and checked
//! fake-IP lease. Sparse high-slot relocation is test setup, not proof of high
//! production admission. No provider is started, no receipt or join is fabricated.
use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::tests::admission::Endpoint;
use crate::client_tun::compatibility_runtime::flow_custody::transaction::{
    FlowIdentity, FlowStage, OpenPhase,
};
use crate::fake_ip::FakeIpLeaseReceipt;
use super::super::SHUTDOWN_CENSUS_CADENCE;

#[tokio::test]
async fn stale_nonzero_live_cannot_hide_empty_original_root() {
    let (owner, dispatch) = dispatch();
    let sockets = SocketSet::new(vec![]);
    let mut contexts = HashMap::new();
    let pool = FakeIpPool::new();
    owner.request_stop();
    owner.opens().live = 1;
    assert!(!dispatch.flow_shutdown_complete(&sockets, &mut contexts, &pool));
    assert!(owner.ledger().quarantined());
    assert_eq!(owner.opens().live, 1, "uncertain count stays charged");
}

#[tokio::test]
async fn nonzero_live_still_gets_periodic_full_shutdown_census() {
    let (owner, dispatch) = dispatch();
    owner.request_stop();
    owner.opens().live = 1;
    let now = tokio::time::Instant::now();
    assert!(dispatch.flow_shutdown_census_candidate(None, now));
    assert!(
        !dispatch
            .flow_shutdown_census_candidate(Some(now), now + SHUTDOWN_CENSUS_CADENCE - std::time::Duration::from_millis(1))
    );
    assert!(
        dispatch
            .flow_shutdown_census_candidate(Some(now), now + SHUTDOWN_CENSUS_CADENCE)
    );
}

struct Census {
    _client: Endpoint,
    server: Endpoint,
    owner: Arc<SessionControl>,
    dispatch: OpenDispatch,
    pool: FakeIpPool,
    contexts: HashMap<SocketHandle, SocketCtx>,
    upstream: Arc<NeverOpen>,
    id: FlowIdentity,
    lease: FakeIpLeaseReceipt,
    slot_deadline: tokio::time::Instant,
    open_deadline: std::time::Instant,
    stop_deadline: tokio::time::Instant,
}

impl Census {
    async fn new(high: bool) -> Self {
        let (client, mut server) = receiving_pair().await;
        let (owner, dispatch) = dispatch();
        let upstream = Arc::new(NeverOpen::default());
        let mut pool = FakeIpPool::new();
        let ip = pool.alloc("census.test", 0).unwrap();
        let mut contexts = HashMap::from([(server.handle, SocketCtx::new(10000))]);
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            upstream.clone(),
            FlowAdmission {
                handle: server.handle,
                socket: server.sockets.get_mut::<TcpSocket>(server.handle),
                ctx: contexts.get_mut(&server.handle).unwrap(),
                fake_pool: &mut pool,
                fake_ip: Some(ip),
                target: TargetAddr::DomainPort {
                    host: "census.test".into(),
                    port: 10000,
                },
                now: 0,
            },
        );
        let before = owner.flow_contracts().pop().unwrap();
        let audited = owner.opens().audit_flows();
        assert!(audited);
        let mirror = contexts[&server.handle].flow_mirror.as_ref().unwrap();
        let original_id = mirror.id;
        let lease = mirror.live_lease().unwrap().receipt();
        assert_eq!(original_id.index, 0);
        let id = if high {
            // Take the original non-Clone slot, preserving its escrow, upstream,
            // lease and all deadlines. Only both spatial index fields change.
            let slot = owner.opens().slots.take(0);
            let mut slot = slot.unwrap(); // No root mutex guard across assertions.
            let OpenPhase::Flow(flow) = &mut slot.phase else {
                panic!("actual admission row")
            };
            flow.id.index = 4223;
            let replaced = owner.opens().slots.put(4223, slot);
            assert!(matches!(replaced, Ok(None)));
            contexts
                .get_mut(&server.handle)
                .unwrap()
                .flow_mirror
                .as_mut()
                .unwrap()
                .id
                .index = 4223;
            FlowIdentity {
                index: 4223,
                ..original_id
            }
        } else {
            original_id
        };
        let after = owner.flow_contracts().pop().unwrap();
        let audited = owner.opens().audit_flows();
        assert!(audited);
        assert_eq!(after.id, id);
        assert_eq!(
            (after.slot_deadline, after.open_deadline),
            (before.slot_deadline, before.open_deadline)
        );
        assert_eq!(
            (id.handle, id.epoch, id.generation),
            (
                original_id.handle,
                original_id.epoch,
                original_id.generation
            )
        );
        assert_eq!(
            contexts[&server.handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .live_lease()
                .unwrap()
                .receipt(),
            lease
        );
        owner.request_stop();
        let stop_deadline = owner.ledger().shutdown_deadline;
        Self {
            _client: client,
            server,
            owner,
            dispatch,
            pool,
            contexts,
            upstream,
            id,
            lease,
            slot_deadline: before.slot_deadline,
            open_deadline: before.open_deadline,
            stop_deadline: stop_deadline.unwrap(),
        }
    }

    fn census(&mut self) -> bool {
        self.dispatch
            .flow_shutdown_complete(&self.server.sockets, &mut self.contexts, &self.pool)
    }

    fn assert_owned(&self, expected_id: FlowIdentity, quarantined: bool) {
        let actual = self.owner.flow_observation().unwrap();
        assert_eq!(
            (actual.live, actual.w, actual.f, actual.attempts),
            (1, 1, 0, 0)
        );
        assert_eq!(
            (actual.reserved, actual.escrow_bytes),
            (MAX_UPLINK_BUFFER, b"untouched-rx".len())
        );
        assert_eq!(
            (actual.published, actual.released, actual.joined),
            (0, 0, 0)
        );
        assert_eq!(self.upstream.0.load(Ordering::Acquire), 0);
        let facts = {
            let ledger = self.owner.ledger();
            (
                ledger.shutdown_deadline,
                ledger.quarantined(),
                ledger.first_failure.is_none(),
            )
        };
        assert_eq!(facts, (Some(self.stop_deadline), quarantined, true));
        let row = self.owner.flow_contracts().pop().unwrap();
        assert_eq!(row.id, expected_id);
        assert_eq!(
            (row.slot_deadline, row.open_deadline),
            (self.slot_deadline, self.open_deadline)
        );
        let original = {
            let opens = self.owner.opens();
            opens
                .slots
                .get(self.id.index)
                .and_then(Option::as_ref)
                .and_then(|slot| match &slot.phase {
                    OpenPhase::Flow(flow) => Some((
                        slot.handle,
                        slot.epoch,
                        flow.stage,
                        flow.escrow.to_vec(),
                        flow.fake_ip,
                        flow.physical,
                        flow.producer_stopped,
                        flow.attempt.is_some(),
                        flow.carrier.is_some(),
                        flow.roles.is_some(),
                    )),
                    _ => None,
                })
        };
        assert_eq!(
            original,
            Some((
                self.id.handle,
                self.id.epoch,
                if quarantined {
                    FlowStage::Quarantined
                } else {
                    FlowStage::Waiting
                },
                b"untouched-rx".to_vec(),
                Some(self.lease),
                true,
                false,
                false,
                false,
                false
            ))
        );
        assert_eq!(
            self.pool.resolve(self.lease.ip).as_deref(),
            Some("census.test")
        );
    }

    fn add_bare(&mut self, port: u16) -> SocketHandle {
        let mut socket = TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 32]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 32]),
        );
        socket.listen(port).unwrap();
        let handle = self.server.sockets.add(socket);
        assert!(self.contexts.insert(handle, SocketCtx::new(port)).is_none());
        handle
    }

    fn bare_facts(&self, handle: SocketHandle) -> (String, String) {
        (
            format!("{:?}", self.contexts[&handle]),
            format!("{:?}", self.server.sockets.get::<TcpSocket>(handle)),
        )
    }
}

#[tokio::test]
async fn sparse_high_waiting_slot_keeps_actual_lease_escrow_and_deadlines() {
    let mut f = Census::new(true).await;
    for _ in 0..3 {
        assert!(!f.census()); // The original live W is not a settlement certificate.
        f.assert_owned(f.id, false);
        let audited = f.owner.opens().audit_flows();
        assert!(audited);
        assert_eq!(
            f.contexts[&f.server.handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .live_lease()
                .unwrap()
                .receipt(),
            f.lease
        );
    }
}

#[tokio::test]
async fn missing_high_mirror_keeps_original_capability_and_charged_root() {
    let mut f = Census::new(true).await;
    let mirror = f
        .contexts
        .get_mut(&f.server.handle)
        .unwrap()
        .flow_mirror
        .take()
        .unwrap();
    assert!(!f.census());
    f.assert_owned(f.id, true);
    assert_eq!(mirror.live_lease().unwrap().receipt(), f.lease);
    let matches = mirror.matches_root(&f.owner.opens());
    assert!(matches);
}

#[tokio::test]
async fn orphan_high_context_quarantines_original_root_without_disposal() {
    let mut f = Census::new(true).await;
    let socket = f.server.sockets.remove(f.server.handle);
    assert!(!f.census());
    f.assert_owned(f.id, true);
    assert!(
        f.contexts[&f.server.handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .quarantined
    );
    assert!(matches!(socket, smoltcp::socket::Socket::Tcp(_)));
}

#[tokio::test]
async fn corrupt_inner_handle_never_changes_the_healthy_physical_peer() {
    let mut f = Census::new(true).await;
    let healthy = f.add_bare(10001);
    let before = f.bare_facts(healthy);
    let broken = FlowIdentity {
        handle: healthy,
        ..f.id
    };
    {
        let mut opens = f.owner.opens();
        if let OpenPhase::Flow(flow) = &mut opens.slots.get_mut(f.id.index).unwrap().phase {
            flow.id = broken;
        }
    }
    assert!(!f.census());
    f.assert_owned(broken, true);
    assert_eq!(f.bare_facts(healthy), before);
}

#[tokio::test]
async fn overflow_reverse_quarantine_reaches_previously_valid_high_root() {
    let mut f = Census::new(true).await;
    assert!(!f.census());
    f.assert_owned(f.id, false);
    for index in 0..4352 {
        f.add_bare(10001 + index as u16);
    }
    assert_eq!(f.server.sockets.iter().count(), 4353);
    assert_eq!(f.contexts.len(), 4353);
    let initial_mirror_q = f.contexts[&f.server.handle]
        .flow_mirror
        .as_ref()
        .unwrap()
        .quarantined;
    assert!(!initial_mirror_q);
    assert!(!f.census());
    f.assert_owned(f.id, true);
    assert!(
        f.contexts[&f.server.handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .quarantined
    );
}

#[tokio::test]
async fn mirror_index_overflow_fails_closed_without_indexing_or_erasing_root() {
    let mut f = Census::new(true).await;
    f.contexts
        .get_mut(&f.server.handle)
        .unwrap()
        .flow_mirror
        .as_mut()
        .unwrap()
        .id
        .index = usize::MAX;
    assert!(!f.census());
    f.assert_owned(f.id, true);
    assert!(
        f.contexts[&f.server.handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .quarantined
    );
}

#[tokio::test]
async fn wrong_slot_capacity_keeps_valid_flow_and_sticky_owner_quarantine() {
    for capacity in [4223, 4225] {
        let mut f = Census::new(false).await;
        f.assert_owned(f.id, false);
        let contracts = f.owner.flow_contracts();
        let physical = f.bare_facts(f.server.handle);
        if capacity == 4223 {
            let removed = f.owner.opens().slots.pop_row_for_test();
            assert!(matches!(removed, Some(None)));
        } else {
            f.owner.opens().slots.push_empty_row_for_test();
        }
        for _ in 0..2 {
            assert!(!f.census());
            assert_eq!(f.owner.flow_contracts(), contracts);
            assert_eq!(f.bare_facts(f.server.handle), physical);
            let actual = f.owner.flow_observation().unwrap();
            assert!(!actual.audit);
            assert_eq!(
                (actual.live, actual.w, actual.f, actual.attempts),
                (1, 1, 0, 0)
            );
            assert_eq!(
                (actual.reserved, actual.escrow_bytes),
                (MAX_UPLINK_BUFFER, b"untouched-rx".len())
            );
            assert_eq!(
                (actual.published, actual.released, actual.joined),
                (0, 0, 0)
            );
            let facts = {
                let ledger = f.owner.ledger();
                (
                    ledger.shutdown_deadline,
                    ledger.quarantined(),
                    ledger.first_failure.is_none(),
                )
            };
            assert_eq!(facts, (Some(f.stop_deadline), true, true));
            let root = {
                let opens = f.owner.opens();
                let slot = opens.slots[f.id.index].as_ref().unwrap();
                match &slot.phase {
                    OpenPhase::Flow(flow) => Some((
                        opens.slots.len(),
                        flow.stage,
                        flow.escrow.to_vec(),
                        flow.fake_ip,
                        flow.physical,
                    )),
                    _ => None,
                }
            };
            assert_eq!(
                root,
                Some((
                    capacity,
                    FlowStage::Waiting,
                    b"untouched-rx".to_vec(),
                    Some(f.lease),
                    true,
                ))
            );
            let mirror = f.contexts[&f.server.handle].flow_mirror.as_ref().unwrap();
            assert!(!mirror.quarantined);
            assert_eq!(mirror.live_lease().unwrap().receipt(), f.lease);
            assert_eq!(f.pool.resolve(f.lease.ip).as_deref(), Some("census.test"));
            assert_eq!(f.upstream.0.load(Ordering::Acquire), 0);
        }
    }
}

#[tokio::test]
async fn same_census_identity_faults_preserve_original_row_and_healthy_peer() {
    for case in 0..5 {
        let mut f = Census::new(true).await;
        let healthy = f.add_bare(10001);
        let healthy_before = f.bare_facts(healthy);
        let ctx = f.contexts.get_mut(&f.server.handle).unwrap();
        match case {
            0 => ctx.local_port = 0,
            1 => ctx.local_port = 10002,
            2 => ctx.conn_epoch += 1,
            3 => ctx.flow_mirror.as_mut().unwrap().id.generation += 1,
            4 => ctx.flow_mirror.as_mut().unwrap().quarantined = true,
            _ => unreachable!(),
        }
        let before = (ctx.local_port, ctx.conn_epoch, ctx.flow_mirror.as_ref().unwrap().id);
        for _ in 0..2 {
            assert!(!f.census(), "case {case}");
            f.assert_owned(f.id, true);
            let ctx = &f.contexts[&f.server.handle];
            assert_eq!((ctx.local_port, ctx.conn_epoch, ctx.flow_mirror.as_ref().unwrap().id), before);
            assert!(ctx.flow_mirror.as_ref().unwrap().quarantined);
            assert_eq!(ctx.flow_mirror.as_ref().unwrap().live_lease().unwrap().receipt(), f.lease);
            assert_eq!(f.bare_facts(healthy), healthy_before);
        }
    }
}

#[tokio::test]
async fn actual_socket_type_and_reverse_orphans_keep_all_original_custody() {
    use smoltcp::socket::udp::{PacketBuffer, PacketMetadata, Socket};
    for case in 0..4 {
        let mut f = Census::new(true).await;
        let healthy = f.add_bare(10001);
        let healthy_before = f.bare_facts(healthy);
        let rows = f.owner.flow_contracts();
        let mut retained_socket = None;
        let mut retained_context = None;
        if case <= 1 {
            if case == 0 { retained_socket = Some(f.server.sockets.remove(f.server.handle)); }
            let udp = Socket::new(
                PacketBuffer::new(vec![PacketMetadata::EMPTY], vec![0; 64]),
                PacketBuffer::new(vec![PacketMetadata::EMPTY], vec![0; 64]),
            );
            let handle = f.server.sockets.add(udp);
            if case == 0 { assert_eq!(handle, f.server.handle); }
        } else {
            let extra = f.add_bare(10002);
            if case == 2 { retained_context = f.contexts.remove(&extra); }
            else { retained_socket = Some(f.server.sockets.remove(extra)); }
        }
        for _ in 0..2 {
            assert!(!f.census(), "case {case}");
            assert!(f.owner.ledger().quarantined());
            assert_eq!(f.owner.ledger().shutdown_deadline, Some(f.stop_deadline));
            assert!(f.owner.ledger().first_failure.is_none());
            if case == 0 { f.assert_owned(f.id, true); }
            else { assert_eq!(f.owner.flow_contracts(), rows, "unrelated row retained, case {case}"); }
            let flow = f.owner.flow_observation().unwrap();
            assert_eq!((flow.live, flow.w, flow.f, flow.reserved, flow.escrow_bytes),
                (1, 1, 0, MAX_UPLINK_BUFFER, b"untouched-rx".len()));
            assert_eq!((flow.joined, flow.released, flow.attempts), (0, 0, 0));
            assert_eq!(f.contexts[&f.server.handle].flow_mirror.as_ref().unwrap().live_lease().unwrap().receipt(), f.lease);
            assert_eq!(f.bare_facts(healthy), healthy_before);
        }
        drop(retained_socket);
        drop(retained_context);
    }
}
