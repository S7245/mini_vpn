//! N2 terminal dispositions around Waiting (D126 six dispositions, D143 F1/F3):
//! only a clean expiry (handshake failure/timeout, nothing uncertain) may park.
//! A re-probe's local precondition error, a configuration rejection met by a
//! re-probe, and Q while Waiting all stay terminal and never re-probe again.
use super::super::*;
use super::recovery::wait_closed;
use super::running::boot;
use super::startup::{config, finish, guarded, server};

/// Slot 1 of a pool=2 Node: two refused handshakes ⇒ clean expiry ⇒ parked.
pub(super) async fn park_slot_one(
    host: &mut NodeHost,
    server: &Endpoint,
    peers: &[Connection],
) -> (Arc<TuicUpstream>, recovery::Ticket) {
    let observer = host.native_weak_observer_for_test();
    let data = host.data().unwrap();
    peers[1].close(17u32.into(), b"black-holed auxiliary slot");
    wait_closed(&data, 1).await;
    let ticket = data
        .request_aux_recovery(1, data.tcp_pool_slots[1].state.lock().await)
        .unwrap();
    tokio::time::timeout(Duration::from_secs(6), async {
        tokio::select! {
            _ = async {
                for _ in 0..2 { server.accept().await.unwrap().refuse(); }
                while observer.waiting().unwrap().parked != 1 {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            } => {},
            result = host.run() => panic!("node ended: {result:?}"),
        }
    })
    .await
    .unwrap();
    let sample = observer.waiting().unwrap();
    assert!(!sample.first_failure && !sample.quarantined, "{sample:?}");
    (data, ticket)
}

/// Drive `run` (it must not end) until `done` holds or `limit` passes.
async fn run_until(
    host: &mut NodeHost,
    limit: Duration,
    done: impl Fn(WaitingSample) -> bool,
) -> WaitingSample {
    let observer = host.native_weak_observer_for_test();
    tokio::time::timeout(limit, async {
        tokio::select! {
            _ = async {
                while !done(observer.waiting().unwrap()) {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            } => {},
            result = host.run() => panic!("node ended: {result:?}"),
        }
    })
    .await
    .unwrap();
    observer.waiting().unwrap()
}

#[tokio::test(flavor = "current_thread")]
async fn reprobe_local_precondition_error_is_recorded_terminal_not_waiting() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let (data, first) = park_slot_one(&mut host, &server, &peers).await;
        // Waiting open: fails fast, but says nothing was recorded (D143 F12).
        let waiting = data.observe_aux_recovery(first).unwrap_err().to_string();
        assert!(
            !waiting.contains("retained") && waiting.contains("waiting"),
            "{waiting}"
        );
        let attempts = host.resources.state().attempts_started;
        // The shared predecessor capacity is held when the re-probe prepares:
        // a local precondition error, not handshake evidence (D143 F1).
        data.tcp_pool_draining_predecessor
            .store(true, Ordering::Release);
        let sample = run_until(&mut host, Duration::from_secs(4), |s| {
            s.reprobes == 1 && (s.retries_held == 0 || s.parked == 1)
        })
        .await;
        assert_eq!(
            (sample.retries_held, sample.waits, sample.first_failure),
            (0, 1, true),
            "re-probe precondition error must be recorded terminal: {sample:?}"
        );
        assert!(!sample.quarantined);
        data.tcp_pool_draining_predecessor
            .store(false, Ordering::Release);
        // Terminal: no further re-probe or handshake through the old schedule.
        assert!(
            tokio::time::timeout(Duration::from_millis(2_500), host.run())
                .await
                .is_err()
        );
        let after = host.native_weak_observer_for_test().waiting().unwrap();
        assert_eq!((after.reprobes, after.retries_held), (1, 0), "{after:?}");
        assert_eq!(host.resources.state().attempts_started, attempts);
        drop(data);
    })
    .await;
    finish(host, server, peers, result).await;
}

fn rejecting_server_config() -> quinn::ServerConfig {
    let (certs, key) = crate::tuic::tcp_fixture::test_server_identity();
    let mut crypto = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
    crypto.alpn_protocols = vec![b"x-n2-config-reject".to_vec()];
    quinn::ServerConfig::with_crypto(Arc::new(
        quinn::crypto::rustls::QuicServerConfig::try_from(crypto).unwrap(),
    ))
}

#[tokio::test(flavor = "current_thread")]
async fn reprobe_configuration_rejection_during_waiting_is_terminal() {
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let (data, _) = park_slot_one(&mut host, &server, &peers).await;
        let attempts = host.resources.state().attempts_started;
        // The real peer now rejects the unchanged client TLS configuration
        // (no common ALPN ⇒ TLS NoApplicationProtocol alert).
        server.set_server_config(Some(rejecting_server_config()));
        let observer = host.native_weak_observer_for_test();
        let owner = host.resources.clone();
        tokio::time::timeout(Duration::from_secs(6), async {
            tokio::select! {
                _ = async {
                    assert!(server.accept().await.unwrap().await.is_err());
                    while !owner.state().config_rejected
                        || observer.waiting().unwrap().retries_held != 0
                    {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => {},
                result = host.run() => panic!("node ended early: {result:?}"),
            }
        })
        .await
        .unwrap();
        let sample = observer.waiting().unwrap();
        assert_eq!((sample.reprobes, sample.waits), (1, 1), "{sample:?}");
        assert!(sample.first_failure && !sample.quarantined, "{sample:?}");
        assert_eq!(host.resources.state().attempts_started, attempts + 1);
        // Terminal: the old backoff never re-probes again (step 1 = 2s).
        assert!(
            tokio::time::timeout(Duration::from_millis(3_500), host.run())
                .await
                .is_err()
        );
        let after = observer.waiting().unwrap();
        assert_eq!((after.reprobes, after.retries_held), (1, 0), "{after:?}");
        assert_eq!(host.resources.state().attempts_started, attempts + 1);
        drop(data);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), host.drive_shutdown())
                .await
                .unwrap(),
            NodeShutdown::FailedSettled
        );
    })
    .await;
    finish(host, server, peers, result).await;
}

#[tokio::test(flavor = "current_thread")]
async fn quarantine_during_waiting_outage_is_terminal_without_retry() {
    // A2-Q (D126): the path is black-holed (slot parked) and Q is then set.
    // The Node ends terminally; no re-probe or handshake follows the schedule.
    let server = server(8);
    let mut cfg = config(server.local_addr().unwrap());
    cfg.tcp_pool = 2;
    let mut host = NodeHost::prepare(cfg.clone(), Arc::new(Metrics::default()));
    let mut peers = Vec::new();
    let result = guarded(async {
        boot(&mut host, &server, &cfg, &mut peers).await;
        let (data, first) = park_slot_one(&mut host, &server, &peers).await;
        let attempts = host.resources.state().attempts_started;
        host.resources.state().quarantined = true;
        host.resources.wake_recovery();
        let ended = tokio::time::timeout(Duration::from_millis(3_500), host.run()).await;
        assert!(
            matches!(ended, Ok(Err(_))),
            "Q while Waiting must end run: {ended:?}"
        );
        let sample = host.native_weak_observer_for_test().waiting().unwrap();
        assert_eq!(sample.reprobes, 0, "{sample:?}");
        assert_eq!(host.resources.state().attempts_started, attempts);
        let observed = data.observe_aux_recovery(first).unwrap_err().to_string();
        assert!(!observed.contains("waiting"), "{observed}");
        drop(data);
    })
    .await;
    finish(host, server, peers, result).await;
}
