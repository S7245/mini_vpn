use super::*;
use crate::shared::errors::{FailureClass, QuietFailureSink};
use std::task::{Context, Poll, Wake, Waker};
struct Hostile;
impl Wake for Hostile {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(11u8);
    }
}
impl Drop for Hostile {
    fn drop(&mut self) {
        std::panic::panic_any(22u8);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_recovery_callback_reports_only_original_facts() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut session = SessionControl::default();
    session.app_meter = Some(bytes::BackingMeter::with_budget(&budget));
    let owner = Arc::new(session);
    let parent: Arc<dyn QuietFailureSink> = owner.clone();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    host.bind_udp_parent(Arc::downgrade(&parent)).unwrap();
    let observer = host.native_observer_for_test();
    let mut peers = Vec::new();
    let mut observed = None;
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.start(), async {
                for _ in 0..cfg.tcp_pool {
                    let peer = server.accept().await.unwrap().await.unwrap();
                    auth(&peer, &cfg).await;
                    peers.push(peer);
                }
            })
        })
        .await
        .unwrap();
        assert_eq!(started, NodeStartup::Ready);
        let original = Waker::from(Arc::new(Hostile));
        let mut running = Box::pin(host.run());
        assert!(
            running
                .as_mut()
                .poll(&mut Context::from_waker(&original))
                .is_pending()
        );
        drop(original);
        tokio::time::timeout(Duration::from_secs(2), async {
            while owner.ledger().first_failure.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            running
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(Err(_))
        ));
        drop(running);
        let node = observer.facts();
        let ledger = owner.ledger();
        observed = Some((
            node.0.unwrap(),
            node.1,
            ledger.first_failure.as_ref().unwrap().facts().clone(),
            ledger.additional_failures,
        ));
    })
    .await;
    drop(host);
    drop(observer);
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop(server);
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    let (node, node_additional, session, session_additional) = observed.unwrap();
    assert_eq!(node, session);
    assert_eq!(node.class, FailureClass::Panic);
    assert_eq!(
        (node_additional, session_additional),
        (1, 1),
        "only actual original wake and Drop, no wrapper IO"
    );
    assert!(budget.quarantined());
    assert!(budget.used() > 0);
}

#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_recovery_refusal_is_quiet_in_session_before_later_io() {
    let identity = PublicCa::new();
    let mut server =
        quinn::Endpoint::server(identity.server_config(), "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut cfg = config(server.local_addr().unwrap());
    cfg.ca_path = identity.path.join("ca.pem").to_str().unwrap().to_owned();
    let budget = bytes::BackingBudget::new(32 << 20).unwrap();
    let mut session = SessionControl::default();
    session.app_meter = Some(bytes::BackingMeter::with_budget(&budget));
    let owner = Arc::new(session);
    let parent: Arc<dyn QuietFailureSink> = owner.clone();
    let mut host =
        NodeHost::prepare_with_budget(cfg.clone(), Arc::new(Metrics::default()), Some(&budget));
    host.bind_udp_parent(Arc::downgrade(&parent)).unwrap();
    let observer = host.native_observer_for_test();
    let waiting = host.native_weak_observer_for_test();
    let mut peers = Vec::new();
    let mut data = None;
    let mut source = None;
    let mut observed = None;
    let scenario = capture(async {
        let (started, ()) = crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(5), async {
            tokio::join!(host.start(), async { for _ in 0..cfg.tcp_pool { let peer = server.accept().await.unwrap().await.unwrap(); auth(&peer, &cfg).await; peers.push(peer); } })
        }).await.unwrap(); assert_eq!(started, NodeStartup::Ready);
        data = Some(host.data().unwrap()); source = Some(data.as_ref().unwrap().start_udp_source().unwrap());
        peers[0].close(71u32.into(), b"original primary loss");
        let mut running = Box::pin(host.run()); let mut refused = false;
        let clock = std::time::Instant::now();
        // D127: one refused + one unanswered handshake is a clean expiry of the
        // primary closed demand ⇒ Waiting (retryable), not a recorded failure.
        crate::client_tun::compatibility_runtime::tuic_service::tests::fixture::window(Duration::from_secs(15), async {
            while waiting.waiting().is_none_or(|s| s.waits == 0) {
                tokio::select! {
                    incoming = server.accept(), if !refused => { incoming.unwrap().refuse(); refused = true; },
                    _ = source.as_mut().unwrap().recv(&clock) => {},
                    _ = &mut running => break,
                    _ = tokio::time::sleep(Duration::from_millis(1)) => {},
                }
            }
        }).await.unwrap();
        drop(running); assert!(refused, "actual original recovery Initial must reach local endpoint");
        let sample = waiting.waiting().unwrap();
        assert!(!sample.first_failure && !sample.quarantined && sample.retries_held == 1, "{sample:?}");
        let node_first = observer.facts().0;
        observed = Some((node_first, owner.ledger().first_failure.as_ref().map(|f| f.facts().clone())));
        owner.record(crate::shared::errors::RetainedFailure::from_io(
            SessionControl::origin(crate::shared::errors::FailurePhase::SessionLoop), std::io::Error::from_raw_os_error(6)));
        drop(source.take()); drop(data.take());
        let later = owner.ledger().first_failure.as_ref().map(|f| f.facts().origin.phase.clone());
        assert_eq!(later, Some(crate::shared::errors::FailurePhase::SessionLoop), "later IO is the session first");
        assert_eq!(tokio::time::timeout(Duration::from_secs(15), host.drive_shutdown()).await.unwrap(), NodeShutdown::RunningResourcesSettled);
    }).await;
    drop((source, data, observer, waiting, host));
    for peer in peers {
        peer.close(0u32.into(), b"test complete");
    }
    server.close(0u32.into(), b"test complete");
    server.wait_idle().await;
    server.set_server_config(None);
    drop(server);
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    let (node, session) = observed.unwrap();
    assert_eq!(
        (node, session),
        (None, None),
        "clean UDP-primary recovery expiry is Waiting: quiet in Node and Session before later IO"
    );
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
