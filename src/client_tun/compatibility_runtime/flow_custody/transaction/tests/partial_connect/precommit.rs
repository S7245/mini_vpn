//! Real pre-commit failures after partial CONNECT and actual application RST.
//! The original provider retires before checked release/listen; no receipt or
//! error is injected. Restoring the pool/count/port cannot retry physical commit.
use super::*;
use crate::fake_ip::CheckedFakeIpError;

#[derive(Clone, Copy, Debug)]
enum Fault {
    WrongPool,
    CountMismatch,
    ListenPortZero,
}

async fn run(fault: Fault) {
    let ready = ReadyPeer::new().await;
    let setup = capture(Physical::new_with_fake_ip(ready.upstream.clone(), true)).await;
    let mut physical = match setup {
        Ok(physical) => physical,
        Err(panic) => {
            let _cleanup = capture(ready.close()).await;
            std::panic::resume_unwind(panic)
        }
    };
    let mut halves = None;
    let mut original_pool = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if view(&physical.owner)
                    .is_some_and(|v| v.phase == TcpOpenPhase::Connecting && v.connect_written == 4)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let id = held(&physical.owner);
        let acquired = physical
            .owner
            .opens()
            .flow_mut(id)
            .unwrap()
            .fake_ip
            .unwrap();
        halves = Some(ready.prefix().await);
        // Make the ORIGINAL socket Closed/noTuple through a real app RST before
        // root close starts. This reaches the reset's legitimate early-ready path.
        physical
            .client
            .sockets
            .get_mut::<TcpSocket>(physical.client.handle)
            .abort();
        physical.client.turn().await;
        physical.server.turn().await;
        let socket = physical.server.sockets.get::<TcpSocket>(id.handle);
        assert_eq!(socket.state(), smoltcp::socket::tcp::State::Closed);
        assert!(socket.local_endpoint().is_none() && socket.remote_endpoint().is_none());
        let mirror = physical.contexts[&id.handle].flow_mirror.as_ref().unwrap();
        assert_eq!(mirror.commit_probe(), (false, false, false));
        assert_eq!(physical.pool.usage(), (1, 1));
        match fault {
            Fault::WrongPool => {
                let mut other = FakeIpPool::new();
                assert_eq!(other.alloc("receipt.test", 0).unwrap(), acquired.ip);
                other.acquire(acquired.ip, 0);
                original_pool = Some(std::mem::replace(&mut physical.pool, other));
            }
            Fault::CountMismatch => physical.pool.release(acquired.ip, 0),
            Fault::ListenPortZero => physical.contexts.get_mut(&id.handle).unwrap().local_port = 0,
        }
        let lease = physical.contexts[&id.handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .live_lease()
            .unwrap();
        match fault {
            Fault::WrongPool => assert!(matches!(
                physical.pool.prepare_checked_release(lease, 0),
                Err(CheckedFakeIpError::WrongPool)
            )),
            Fault::CountMismatch => assert!(matches!(
                physical.pool.prepare_checked_release(lease, 0),
                Err(CheckedFakeIpError::CountMismatch)
            )),
            Fault::ListenPortZero => {
                assert!(physical.pool.prepare_checked_release(lease, 0).is_ok())
            }
        }
        physical.owner.request_stop();
        let shutdown_deadline = physical.owner.ledger().shutdown_deadline;
        tokio::time::timeout(Duration::from_secs(2), async {
            while view(&physical.owner).unwrap().phase != TcpOpenPhase::Retiring {
                physical.control().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let source = active_error(&physical.owner);
        let stop_deadline = physical.owner.opens().flow_mut(id).unwrap().stop_deadline;
        tokio::time::timeout(
            Duration::from_secs(2),
            peer_retirement(halves.take().unwrap()),
        )
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while !physical.contexts[&id.handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .quarantined
            {
                physical.control().await;
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(view(&physical.owner).unwrap().phase, TcpOpenPhase::Settled);
        assert_eq!(active_error(&physical.owner), source);
        let mirror = physical.contexts[&id.handle].flow_mirror.as_ref().unwrap();
        // Only the actual abort/configure/listen branch sets started; port 0
        // returns smoltcp ListenError::Unaddressable before any listen commit.
        assert_eq!(
            mirror.commit_probe(),
            (matches!(fault, Fault::ListenPortZero), false, false)
        );
        assert!(mirror.live_lease().is_some());
        match fault {
            Fault::WrongPool => {
                assert_eq!(physical.pool.usage(), (1, 1));
                physical.pool = original_pool.take().unwrap();
            }
            Fault::CountMismatch => {
                assert_eq!(physical.pool.usage(), (1, 0));
                physical.pool.acquire(acquired.ip, 0);
            }
            Fault::ListenPortZero => {
                physical.contexts.get_mut(&id.handle).unwrap().local_port = 10000
            }
        }
        let retry = physical.owner.flow_observation().unwrap().retry;
        for _ in 0..3 {
            assert!(!receipt::service(&mut physical, id));
            physical.control().await;
            physical.flush().await;
            let seen = physical.owner.flow_observation().unwrap();
            assert_eq!(
                (seen.live, seen.w, seen.f, seen.reserved, seen.escrow_bytes),
                (1, 1, 1, MAX_UPLINK_BUFFER, PAYLOAD.len())
            );
            assert_eq!(
                (
                    seen.published,
                    seen.released,
                    seen.running,
                    seen.joined,
                    seen.retry
                ),
                (0, 0, 0, 0, retry)
            );
            assert_eq!((seen.stopped_producers, seen.settled_providers), (1, 1));
            assert!(seen.audit && physical.owner.ledger().quarantined);
            assert_eq!(active_error(&physical.owner), source);
            assert_eq!(physical.owner.ledger().shutdown_deadline, shutdown_deadline);
            let opens = physical.owner.opens();
            let slot = opens.slots[id.index].as_ref().unwrap();
            let OpenPhase::Flow(flow) = &slot.phase else {
                panic!("original row")
            };
            assert_eq!(flow.id, id);
            assert_eq!(flow.stop_deadline, stop_deadline);
            assert_eq!(flow.stage, FlowStage::Quarantined);
            assert_eq!(flow.escrow.as_slice(), PAYLOAD);
            drop(opens);
            assert_eq!(physical.pool.usage(), (1, 1));
            let ctx = &physical.contexts[&id.handle];
            assert_eq!(ctx.conn_epoch, id.epoch);
            let mirror = ctx.flow_mirror.as_ref().unwrap();
            assert!(mirror.retained_receipt().is_none());
            assert!(
                physical
                    .pool
                    .validate_checked_leases(mirror.live_lease().into_iter())
            );
            assert_eq!(
                physical.server.sockets.get::<TcpSocket>(id.handle).state(),
                smoltcp::socket::tcp::State::Closed
            );
            assert!(!physical.dispatch.flow_shutdown_complete(
                &physical.server.sockets,
                &mut physical.contexts,
                &physical.pool
            ));
        }
        ready.assert_current(1);
    })
    .await;
    drop(halves.take());
    drop(physical);
    drop(original_pool);
    let cleanup = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn actual_wrong_pool_release_never_retries_after_original_pool_returns() {
    run(Fault::WrongPool).await;
}

#[tokio::test]
async fn actual_checked_count_failure_keeps_original_lease_after_count_is_restored() {
    run(Fault::CountMismatch).await;
}

#[tokio::test]
async fn actual_listen_unaddressable_keeps_prepared_release_uncommitted() {
    run(Fault::ListenPortZero).await;
}
