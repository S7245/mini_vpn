//! Actual Node source proof first observes its hostile caller after real JOIN.
use super::*;
use crate::shared::errors::{FailureClass, QuietFailureSink};
use std::task::{Context, Wake, Waker};
struct Hostile;
impl Wake for Hostile {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        std::panic::panic_any(31u8);
    }
}
impl Drop for Hostile {
    fn drop(&mut self) {
        std::panic::panic_any(32u8);
    }
}
#[tokio::test(flavor = "current_thread")]
async fn actual_hard_node_source_proof_keeps_original_callback_failure_without_wrapper_io() {
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
    let mut data = None;
    let mut source = None;
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
        data = Some(host.data().unwrap());
        source = Some(data.as_ref().unwrap().start_udp_source().unwrap());
        let mut shutdown = Box::pin(host.drive_shutdown());
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                assert!(
                    shutdown
                        .as_mut()
                        .poll(&mut Context::from_waker(Waker::noop()))
                        .is_pending()
                );
                if observer.udp_joined() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert!(owner.ledger().first_failure.is_none() && !budget.quarantined());
        let original = Waker::from(Arc::new(Hostile));
        assert!(
            shutdown
                .as_mut()
                .poll(&mut Context::from_waker(&original))
                .is_pending()
        );
        drop(original);
        drop(source.take()); // actual physical source retirement wakes proof
        drop(data.take());
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), &mut shutdown)
                .await
                .unwrap(),
            NodeShutdown::Quarantined
        );
        drop(shutdown);
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
    drop((source, data, host, observer));
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
        "only original source-proof wake and Drop"
    );
    assert!(budget.quarantined() && budget.used() > 0);
}
