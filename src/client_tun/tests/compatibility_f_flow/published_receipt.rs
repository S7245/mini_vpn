//! Published DATA custody after real socket/provider/three-role settlement.
//! The original saved-loop pressure fixture owns all runtime components.
//! A held ACK retains only A; a real B stream remains healthy and finishes first.
use super::*;
use crate::client_tun::compatibility_runtime::SessionControl;

pub(super) struct ReceiptScenario {
    handle: SocketHandle,
    halves: Option<(quinn::SendStream, quinn::RecvStream)>,
    other_identity: (usize, SocketHandle, u64, u64),
}

async fn pulse<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut FlowRoleApps,
    handle: SocketHandle,
) {
    apps.turn().await;
    let socket = apps.sockets.get_mut::<Socket>(handle);
    if socket.state() == smoltcp::socket::tcp::State::CloseWait {
        socket.close();
    }
    tokio::select! {
        state = session.run() => panic!("saved driver ended before explicit stop: {state:?}"),
        _ = tokio::time::sleep(Duration::from_millis(1)) => {}
    }
}

async fn until<F: Future<Output = ()>>(
    session: &mut ClientTunSession<F>,
    apps: &mut FlowRoleApps,
    handle: SocketHandle,
    mut ready: impl FnMut(&mut FlowRoleApps, &SessionControl) -> bool,
) {
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            pulse(session, apps, handle).await;
            if ready(apps, &session.control) {
                break;
            }
        }
    })
    .await
    .expect("original published receipt / sibling transition");
}

async fn remote<F: Future<Output = ()>, G: Future>(
    session: &mut ClientTunSession<F>,
    apps: &mut FlowRoleApps,
    handle: SocketHandle,
    work: G,
) -> G::Output {
    let mut work = std::pin::pin!(work);
    tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            apps.turn().await;
            let socket = apps.sockets.get_mut::<Socket>(handle);
            if socket.state() == smoltcp::socket::tcp::State::CloseWait {
                socket.close();
            }
            tokio::select! {
                value = &mut work => return value,
                state = session.run() => panic!("saved driver ended before explicit stop: {state:?}"),
                _ = tokio::time::sleep(Duration::from_millis(1)) => {}
            }
        }
    }).await.expect("actual healthy peer transition")
}

impl ReceiptScenario {
    pub(super) async fn connect<F: Future<Output = ()>>(
        session: &mut ClientTunSession<F>,
        apps: &mut FlowRoleApps,
        peer: &quinn::Connection,
    ) -> Self {
        let mut socket = Socket::new(
            SocketBuffer::new(vec![0; 4096]),
            SocketBuffer::new(vec![0; 4096]),
        );
        socket
            .connect(
                apps.iface.context(),
                (IpAddress::v4(192, 0, 2, 11), 10000),
                40001,
            )
            .unwrap();
        let handle = apps.sockets.add(socket);
        until(session, apps, handle, |apps, _| {
            apps.sockets.get::<Socket>(handle).can_send()
        })
        .await;
        assert_eq!(
            apps.sockets
                .get_mut::<Socket>(handle)
                .send_slice(b"b-retained")
                .unwrap(),
            10
        );
        let halves = remote(session, apps, handle, async {
            let (send, mut recv) = peer.accept_bi().await.unwrap();
            assert_eq!(recv.id().index(), 1);
            let mut expected =
                encode_connect(&TargetAddr::IpPort("192.0.2.11:10000".parse().unwrap()));
            expected.extend_from_slice(b"b-retained");
            let mut bytes = vec![0; expected.len()];
            recv.read_exact(&mut bytes).await.unwrap();
            assert_eq!(bytes, expected);
            (send, recv)
        })
        .await;
        until(session, apps, handle, |_, owner| {
            owner.flow_observation().unwrap().published == 2
        })
        .await;
        let other = session
            .control
            .receipt_sibling()
            .expect("actual B published before A closes");
        Self {
            handle,
            halves: Some(halves),
            other_identity: (other.index, other.handle, other.epoch, other.generation),
        }
    }

    fn same_other(&self, owner: &SessionControl) {
        let other = owner
            .held_receipt()
            .unwrap()
            .other
            .expect("healthy B stays Published");
        assert_eq!(
            (other.index, other.handle, other.epoch, other.generation),
            self.other_identity
        );
    }

    async fn exchange<F: Future<Output = ()>>(
        &mut self,
        session: &mut ClientTunSession<F>,
        apps: &mut FlowRoleApps,
        bytes: [u8; 4],
    ) {
        self.same_other(&session.control);
        assert_eq!(
            apps.sockets
                .get_mut::<Socket>(self.handle)
                .send_slice(&bytes)
                .unwrap(),
            4
        );
        let (send, recv) = self.halves.as_mut().unwrap();
        remote(session, apps, self.handle, async {
            let mut got = [0; 4];
            recv.read_exact(&mut got).await.unwrap();
            assert_eq!(got, bytes);
            send.write_all(&bytes).await.unwrap();
        })
        .await;
        until(session, apps, self.handle, |apps, _| {
            apps.sockets.get::<Socket>(self.handle).recv_queue() == 4
        })
        .await;
        let socket = apps.sockets.get_mut::<Socket>(self.handle);
        let mut got = [0; 4];
        assert_eq!(socket.recv_slice(&mut got).unwrap(), 4);
        assert_eq!(got, bytes);
        assert_eq!(socket.state(), smoltcp::socket::tcp::State::Established);
        self.same_other(&session.control);
    }

    pub(super) async fn exercise<F: Future<Output = ()>>(
        &mut self,
        session: &mut ClientTunSession<F>,
        apps: &mut FlowRoleApps,
        case: CloseCase,
    ) {
        let wrong = matches!(case, CloseCase::ReceiptWrong | CloseCase::ReceiptWindow);
        until(session, apps, self.handle, |_, owner| {
            owner.held_receipt().is_some_and(|held| held.calls >= 3)
        })
        .await;
        let first = session.control.held_receipt().unwrap();
        let other = first.other.unwrap();
        assert_eq!(
            (other.index, other.handle, other.epoch, other.generation),
            self.other_identity
        );
        let (messages, bytes, transferred, discarded, pending_discard) = first.disposition;
        assert!(
            messages >= 2 && transferred > 0 && discarded > 0,
            "{first:?}"
        );
        assert_eq!(transferred + discarded, bytes);
        assert!(pending_discard > 0 && pending_discard <= transferred);
        for _ in 0..3 {
            // Dropping only this run observer leaves the original root/driver.
            pulse(session, apps, self.handle).await;
            let held = session.control.held_receipt().unwrap();
            assert_eq!((held.id, held.disposition), (first.id, first.disposition));
            assert!(held.row_held && held.rearming && !held.quarantined && held.original_matches);
            assert!(held.producer_stopped && held.provider_settled);
            assert_eq!(held.role_joins, 3);
            let seen = session.control.flow_observation().unwrap();
            assert!(seen.audit);
            assert_eq!(
                (seen.live, seen.w, seen.f, seen.reserved, seen.escrow_bytes),
                (2, 0, 2, 0, 0)
            );
            assert_eq!(
                (
                    seen.published,
                    seen.released,
                    seen.joined,
                    seen.running,
                    seen.retry
                ),
                (2, 0, 3, 3, 0)
            );
            self.same_other(&session.control);
        }
        self.exchange(session, apps, *b"one!").await;
        if wrong {
            if case == CloseCase::ReceiptWindow {
                session.control.fail_test_window(first.id.handle);
            } else {
                session.control.corrupt_held_receipt();
            }
            until(session, apps, self.handle, |_, owner| {
                owner.held_receipt().unwrap().quarantined
            })
            .await;
            let calls = session.control.held_receipt().unwrap().calls;
            session.control.release_held_receipt();
            for _ in 0..3 {
                pulse(session, apps, self.handle).await;
                let held = session.control.held_receipt().unwrap();
                assert_eq!(
                    (held.id, held.disposition, held.calls),
                    (first.id, first.disposition, calls)
                );
                assert!(held.row_held && held.quarantined && held.original_matches);
                assert!(session.control.ledger().quarantined);
                let seen = session.control.flow_observation().unwrap();
                assert_eq!(
                    (seen.live, seen.f, seen.released, seen.retry, seen.joined),
                    (2, 2, 0, 0, 3)
                );
            }
        } else {
            session.control.release_held_receipt();
            until(session, apps, self.handle, |_, owner| {
                owner.flow_observation().unwrap().released == 1
            })
            .await;
            for _ in 0..3 {
                pulse(session, apps, self.handle).await;
                let seen = session.control.flow_observation().unwrap();
                assert_eq!(
                    (seen.live, seen.f, seen.released, seen.retry, seen.joined),
                    (1, 1, 1, 1, 3)
                );
                assert_eq!(seen.last_data_disposition, Some(first.disposition));
                assert!(!session.control.held_receipt().unwrap().row_held);
                assert!(!session.control.ledger().quarantined);
            }
        }
        self.exchange(session, apps, *b"two!").await;
        let (mut send, mut recv) = self.halves.take().unwrap();
        send.finish().unwrap();
        let eof = remote(session, apps, self.handle, recv.read_to_end(1))
            .await
            .unwrap();
        assert!(eof.is_empty(), "B's exact uplink ends with real FIN");
        drop((send, recv));
        until(session, apps, self.handle, |apps, owner| {
            let seen = owner.flow_observation().unwrap();
            seen.joined == 6
                && seen.released == (if wrong { 1 } else { 2 })
                && apps.sockets.get::<Socket>(self.handle).state()
                    == smoltcp::socket::tcp::State::Closed
        })
        .await;
        let seen = session.control.flow_observation().unwrap();
        assert_eq!(
            (seen.w, seen.reserved, seen.escrow_bytes, seen.running),
            (0, 0, 0, 0)
        );
        assert_eq!(
            (seen.live, seen.f, seen.retry),
            if wrong { (1, 1, 1) } else { (0, 0, 2) }
        );
        assert_eq!(seen.factory_started, 2);
        let last = session.control.held_receipt().unwrap();
        assert!(last.other.is_none());
        if wrong {
            assert!(
                last.row_held && last.quarantined && last.provider_settled && last.original_matches
            );
            assert_eq!(last.role_joins, 3);
        }
        assert!(seen.audit);
        assert!(
            !session
                .control
                .device_settlement_observed
                .load(Ordering::SeqCst)
        );
    }
}

#[tokio::test]
async fn saved_published_data_holds_actual_receipt_across_cancelled_observers() {
    run_saved_f(CloseCase::ReceiptHold).await;
}

#[tokio::test]
async fn saved_published_receipt_disagreement_keeps_a_quarantined_and_b_healthy() {
    run_saved_f(CloseCase::ReceiptWrong).await;
}

#[tokio::test]
async fn saved_window_fault_rejects_original_retained_receipt_and_keeps_b_healthy() {
    run_saved_f(CloseCase::ReceiptWindow).await;
}
