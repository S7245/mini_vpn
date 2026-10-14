//! Census-only faults with two actually admitted partial CONNECT donors.
//! Owns only test wiring; original pool/root/physical service retain authority.
//! A retains custody after restoration while distinct-IP B commits exactly once.
use super::*;

#[derive(Clone, Copy, Debug)]
enum Fault {
    Count,
    Lease,
}

async fn add_second(physical: &mut Physical, upstream: Arc<TuicUpstream>) -> SocketHandle {
    let server = physical
        .server
        .sockets
        .add(crate::client_tun::build_listener_socket(
            &crate::client_tun::ListenerSpec { local_port: 10001 },
        ));
    let mut socket = TcpSocket::new(
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
        smoltcp::socket::tcp::SocketBuffer::new(vec![0; 4096]),
    );
    socket
        .connect(
            physical.client.iface.context(),
            (smoltcp::wire::IpAddress::v4(10, 0, 0, 2), 10001),
            20001,
        )
        .unwrap();
    let client = physical.client.sockets.add(socket);
    tokio::time::timeout(Duration::from_secs(2), async {
        let mut sent = false;
        loop {
            physical.client.turn().await;
            if !sent && physical.client.sockets.get::<TcpSocket>(client).can_send() {
                assert_eq!(
                    physical
                        .client
                        .sockets
                        .get_mut::<TcpSocket>(client)
                        .send_slice(PAYLOAD)
                        .unwrap(),
                    PAYLOAD.len()
                );
                sent = true;
            }
            physical.server.turn().await;
            if physical.server.sockets.get::<TcpSocket>(server).can_recv() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    physical.contexts.insert(server, SocketCtx::new(10001));
    let ip = physical.pool.alloc("census-b.test", 0).unwrap();
    physical.dispatch.begin_turn().await;
    physical.dispatch.admit_flow(
        Arc::new(crate::owned_upstream::LegacyOwnedUpstream::new(upstream)),
        FlowAdmission {
            handle: server,
            socket: physical.server.sockets.get_mut::<TcpSocket>(server),
            ctx: physical.contexts.get_mut(&server).unwrap(),
            fake_pool: &mut physical.pool,
            fake_ip: Some(ip),
            target: target(),
            now: 0,
        },
    );
    client
}

fn ids(physical: &Physical) -> [FlowIdentity; 2] {
    let mut ids = physical
        .owner
        .opens()
        .slots
        .iter()
        .flatten()
        .map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                panic!("actual F")
            };
            flow.id
        })
        .collect::<Vec<_>>();
    ids.sort_by_key(|id| id.index);
    ids.try_into().unwrap()
}

fn phase(physical: &Physical, id: FlowIdentity) -> TcpOpenView {
    physical
        .owner
        .opens()
        .flow_mut(id)
        .unwrap()
        .attempt
        .as_ref()
        .unwrap()
        .view()
}

fn source(physical: &Physical, id: FlowIdentity) -> (usize, String) {
    let mut opens = physical.owner.opens();
    let error = opens
        .flow_mut(id)
        .unwrap()
        .attempt
        .as_ref()
        .unwrap()
        .failure()
        .unwrap();
    assert!(matches!(error, ClientError::Io(e) if e.kind() == std::io::ErrorKind::Interrupted));
    (error as *const ClientError as usize, error.to_string())
}

fn providers_retired(physical: &Physical) -> bool {
    physical.owner.opens().slots.iter().flatten().all(|slot| {
        let OpenPhase::Flow(flow) = &slot.phase else {
            return false;
        };
        flow.attempt.as_ref().is_some_and(|attempt| {
            let v = attempt.view();
            v.phase == TcpOpenPhase::Settled && !v.lease_held && !v.retirement_pending
        })
    })
}

// Poll the original root and service actual Closing events. Never set a stage,
// producer flag or provider state. Hold only the physical Rearming commit.
async fn retire_turn(physical: &mut Physical) {
    physical.dispatch.begin_turn().await;
    let event = std::future::poll_fn(|cx| {
        let mut next = std::pin::pin!(physical.dispatch.recv(&mut physical.receiver));
        Poll::Ready(next.as_mut().poll(cx))
    })
    .await;
    if let Poll::Ready(Some(OpenEvent::Flow(id))) = event {
        let stage = physical.owner.opens().flow_mut(id).unwrap().stage;
        if stage != FlowStage::Rearming {
            receipt::service(physical, id);
        }
    }
}

async fn retire(physical: &mut Physical) {
    physical.owner.request_stop();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !providers_retired(physical) {
            retire_turn(physical).await;
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn census(physical: &mut Physical) -> bool {
    physical.dispatch.flow_shutdown_complete(
        &physical.server.sockets,
        &mut physical.contexts,
        &physical.pool,
    )
}

async fn run(fault: Fault) {
    let ready = ReadyPeer::with_limits(4, 2).await;
    let setup = capture(Physical::new_with_fake_ip(ready.upstream.clone(), true)).await;
    let mut physical = match setup {
        Ok(physical) => physical,
        Err(panic) => {
            let _cleanup = capture(ready.close()).await;
            std::panic::resume_unwind(panic)
        }
    };
    let mut halves = Vec::new();
    let scenario = capture(async {
        let b_client = add_second(&mut physical, ready.upstream.clone()).await;
        let [a, b] = ids(&physical);
        assert_eq!(a.handle, physical.server.handle);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if [a, b].into_iter().all(|id| {
                    let v = phase(&physical, id);
                    v.phase == TcpOpenPhase::Connecting && v.connect_written == 4
                }) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let seen = physical.owner.flow_observation().unwrap();
        assert_eq!(
            (seen.live, seen.w, seen.f, seen.factory_started),
            (2, 2, 2, 2)
        );
        assert_eq!(
            (seen.published, seen.released, seen.joined, seen.running),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (seen.escrow_bytes, seen.reserved),
            (2 * PAYLOAD.len(), 2 * MAX_UPLINK_BUFFER)
        );
        assert!(seen.audit && !physical.owner.ledger().quarantined);
        let acquired = [a, b].map(|id| {
            physical.contexts[&id.handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .live_lease()
                .unwrap()
                .receipt()
        });
        assert_ne!(acquired[0].ip, acquired[1].ip);
        assert_eq!(physical.pool.usage(), (2, 2));

        physical.owner.request_stop();
        let deadline = physical.owner.ledger().shutdown_deadline;
        tokio::time::timeout(Duration::from_secs(2), async {
            for index in 0..2 {
                let (send, mut recv) = ready.peer.accept_bi().await.unwrap();
                assert_eq!(recv.id().index(), index);
                let mut prefix = [0; 4];
                recv.read_exact(&mut prefix).await.unwrap();
                assert_eq!(&prefix, &encode_connect(&target())[..4]);
                halves.push((send, recv));
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while [a, b]
                .into_iter()
                .any(|id| phase(&physical, id).phase != TcpOpenPhase::Retiring)
            {
                retire_turn(&mut physical).await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let sources = [source(&physical, a), source(&physical, b)];
        for pair in halves.drain(..) {
            tokio::time::timeout(Duration::from_secs(2), peer_retirement(pair))
                .await
                .unwrap();
        }
        retire(&mut physical).await;
        assert!(providers_retired(&physical));
        let flow_deadlines =
            [a, b].map(|id| physical.owner.opens().flow_mut(id).unwrap().stop_deadline);
        for id in [a, b] {
            assert_eq!(
                physical.owner.opens().flow_mut(id).unwrap().stage,
                FlowStage::Rearming
            );
            let ctx = &physical.contexts[&id.handle];
            assert_eq!(ctx.conn_epoch, id.epoch);
            assert_eq!(
                ctx.flow_mirror.as_ref().unwrap().commit_probe(),
                (false, false, false)
            );
        }
        assert!(!census(&mut physical));
        assert!(!physical.owner.ledger().quarantined);

        // Restore the actual capability/count before any original physical ACK.
        match fault {
            Fault::Count => {
                physical.pool.release(acquired[0].ip, 0);
                let completed = census(&mut physical);
                physical.pool.acquire(acquired[0].ip, 0);
                assert!(!completed);
            }
            Fault::Lease => {
                let lease = physical
                    .contexts
                    .get_mut(&a.handle)
                    .unwrap()
                    .flow_mirror
                    .as_mut()
                    .unwrap()
                    .replace_actual_lease(None)
                    .unwrap();
                let completed = census(&mut physical);
                assert!(
                    physical
                        .contexts
                        .get_mut(&a.handle)
                        .unwrap()
                        .flow_mirror
                        .as_mut()
                        .unwrap()
                        .replace_actual_lease(Some(lease))
                        .is_none()
                );
                assert!(!completed);
            }
        }
        assert!(physical.owner.ledger().quarantined);
        assert_eq!(physical.pool.usage(), (2, 2));
        let b_mirror = physical.contexts[&b.handle].flow_mirror.as_ref().unwrap();
        assert!(!b_mirror.quarantined);
        assert_eq!(b_mirror.commit_probe(), (false, false, false));
        assert_eq!(b_mirror.live_lease().unwrap().receipt(), acquired[1]);
        assert_eq!(
            physical.owner.opens().flow_mut(b).unwrap().stage,
            FlowStage::Rearming
        );

        // B goes through original reset, real poll/flush, listen and ACK.
        tokio::time::timeout(Duration::from_secs(2), async {
            while physical.owner.flow_observation().unwrap().released != 1 {
                receipt::service(&mut physical, b);
                physical.flush().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let b_failure = physical.owner.retained_provider_failure().unwrap();
        assert_eq!((b_failure.0, b_failure.2), sources[1]);
        assert_eq!(b_failure.1.phase, TcpOpenPhase::Settled);
        assert_eq!(physical.contexts[&b.handle].conn_epoch, b.epoch + 1);
        assert!(physical.contexts[&b.handle].flow_mirror.is_none());
        assert_eq!(
            physical.server.sockets.get::<TcpSocket>(b.handle).state(),
            smoltcp::socket::tcp::State::Listen
        );
        assert_eq!(
            physical.client.sockets.get::<TcpSocket>(b_client).state(),
            smoltcp::socket::tcp::State::Closed
        );
        assert_eq!(physical.pool.usage(), (2, 1));

        let retry = physical.owner.flow_observation().unwrap().retry;
        for _ in 0..3 {
            assert!(!receipt::service(&mut physical, a));
            physical.control().await;
            physical.flush().await;
            assert!(!census(&mut physical));
            let seen = physical.owner.flow_observation().unwrap();
            assert_eq!(
                (seen.live, seen.w, seen.f, seen.escrow_bytes, seen.reserved),
                (1, 1, 1, PAYLOAD.len(), MAX_UPLINK_BUFFER),
                "census {fault:?} donor must remain owned"
            );
            assert_eq!(
                (
                    seen.factory_started,
                    seen.published,
                    seen.released,
                    seen.joined,
                    seen.running
                ),
                (2, 0, 1, 0, 0)
            );
            assert_eq!(
                (seen.retry, seen.stopped_producers, seen.settled_providers),
                (retry, 1, 1)
            );
            assert!(seen.audit && physical.owner.ledger().quarantined);
            assert_eq!(source(&physical, a), sources[0]);
            assert_eq!(physical.owner.ledger().shutdown_deadline, deadline);
            let mut opens = physical.owner.opens();
            let flow = opens.flow_mut(a).unwrap();
            assert_eq!(flow.stage, FlowStage::Quarantined);
            assert_eq!(flow.stop_deadline, flow_deadlines[0]);
            assert_eq!(flow.escrow.as_slice(), PAYLOAD);
            drop(opens);
            let ctx = &physical.contexts[&a.handle];
            assert_eq!(ctx.conn_epoch, a.epoch);
            let mirror = ctx.flow_mirror.as_ref().unwrap();
            assert!(mirror.quarantined);
            assert_eq!(mirror.commit_probe(), (false, false, false));
            assert_eq!(mirror.live_lease().unwrap().receipt(), acquired[0]);
            assert!(
                physical
                    .pool
                    .validate_checked_leases(mirror.live_lease().into_iter())
            );
            assert_eq!(physical.pool.usage(), (2, 1));
        }
        assert!(providers_retired(&physical));
        ready.assert_current(2);
    })
    .await;
    drop(halves);
    // Drive real retirement on setup/scenario failure too, without waiting for
    // deliberately retained logical Q to become clean.
    let retirement = capture(retire(&mut physical)).await;
    drop(physical);
    let cleanup = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = retirement {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn census_count_fault_retains_a_and_releases_actual_b() {
    run(Fault::Count).await;
}

#[tokio::test]
async fn census_missing_lease_retains_a_and_releases_actual_b() {
    run(Fault::Lease).await;
}
