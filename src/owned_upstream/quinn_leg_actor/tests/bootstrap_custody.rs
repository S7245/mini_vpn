//! Characterizes existing bootstrap admission, cancellation, and cleanup.
//!
//! Owns bootstrap characterization; shared fixtures stay in ancestors.
//! Five P2T bodies stay unchanged; P2A adds real input identity observations.
//! Entry: the parent's private `bootstrap_custody` route; all tests live here.

use super::*;

#[tokio::test(flavor = "current_thread")]
async fn server_bootstrap_parameter_aggregate_preserves_start_and_failure_identity() {
    use std::sync::atomic::Ordering::Acquire;

    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x93; 32]).expect("owner identity"),
        DevicePrincipal::new([0x94; 16]).expect("device identity"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot server config");
    let mut listener = QuinnLegActorListener::bind(config).await.expect("listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("raw peer config");
    let endpoint = super::build_client_endpoint(&client_config, limits).expect("raw endpoint");
    let clock = super::SessionMonotonicClock::start();

    for (fail_before_publication, leg) in [(true, LegId::B), (false, LegId::A)] {
        listener.fail_start_before_publication = fail_before_publication;
        let (connection, bootstrap) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                async {
                    endpoint
                        .connect(client_config.server_addr, &client_config.server_name)
                        .expect("connect raw peer")
                        .await
                        .expect("raw TLS completes")
                },
                listener.admit_bootstrap_for_test(leg, limits, peer_policy),
            )
        })
        .await
        .expect("admission bounded");
        let bootstrap = bootstrap.expect("admitted bootstrap");
        let reservation = std::sync::Arc::downgrade(
            &bootstrap
                .reservation
                .as_ref()
                .expect("original reservation")
                .slot,
        );
        assert_eq!(listener.registry.live.load(Acquire), 1);
        assert_eq!(listener.open_connections_for_test(), 1);
        assert!(matches!(
            listener.registry.try_reserve(),
            Err(QuinnLegActorError::ListenerRegistryFull)
        ));
        let (streams, outcome) = tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                async {
                    let exporter = super::export_binding(&connection, peer_policy)
                        .expect("same peer/exporter binding");
                    super::binding_confirmation(
                        &connection,
                        super::ActorRole::Client,
                        leg,
                        peer_policy,
                        &exporter,
                    )
                    .await
                    .expect("exact leg confirmation");
                    super::bootstrap_streams(&connection, super::ActorRole::Client, leg)
                        .await
                        .expect("exact leg streams")
                },
                bootstrap.start_with_clock(clock.clone()),
            )
        })
        .await
        .expect("start bounded");

        if fail_before_publication {
            assert!(matches!(outcome, Err(QuinnLegActorError::InvalidConfig)));
        } else {
            let mut actor = outcome.expect("same listener publishes the next actor");
            assert!(actor.session_clock_for_test().same_source(&clock));
            assert_eq!(actor.capacity, limits.capacity_witness());
            assert!(std::sync::Arc::ptr_eq(
                actor
                    .server_registration
                    .as_ref()
                    .expect("registration")
                    .slot
                    .as_ref()
                    .expect("slot"),
                &reservation
                    .upgrade()
                    .expect("original reservation remains live"),
            ));
            assert_eq!(listener.registry.live.load(Acquire), 1);
            actor.request_cancel();
            let joined = tokio::time::timeout(TEST_TIMEOUT, actor.join())
                .await
                .expect("actor join bounded")
                .expect("actor joins cleanly");
            assert_eq!((joined.owned_messages(), joined.owned_bytes()), (0, 0));
            assert!(joined.terminal_failure().is_none());
        }
        drop(streams);
        connection.close(CLOSE_CODE, b"parameter identity cleanup");
        drop(connection);
        tokio::time::timeout(TEST_TIMEOUT, async {
            while listener.open_connections_for_test() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("exact connection retirement bounded");
        assert!(reservation.upgrade().is_none());
        assert_eq!(listener.registry.live.load(Acquire), 0);
        assert_eq!(listener.registry.terminating_tasks.load(Acquire), 0);
        assert!(!listener.registry.unclean.load(Acquire));
    }
    endpoint.close(CLOSE_CODE, b"parameter identity endpoint cleanup");
    tokio::time::timeout(TEST_TIMEOUT, endpoint.wait_idle())
        .await
        .expect("raw endpoint idle bounded");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown bounded")
        .expect("original listener stays clean after failure and success");
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_start_caller_leaves_registered_bootstrap_cleanup_to_completion() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x73; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x74; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = ProductionQuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
        Duration::from_millis(500),
    )
    .expect("valid listener configuration with bounded bootstrap deadline");
    let mut listener = QuinnLegActorListener::bind(server_config)
        .await
        .expect("bind one-slot listener");
    let raw_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback raw-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid raw-client configuration");
    let raw_endpoint = super::build_client_endpoint(&raw_client_config, limits)
        .expect("build raw client endpoint");
    let (raw_connection, bootstrap) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            async {
                raw_endpoint
                    .connect(
                        raw_client_config.server_addr,
                        &raw_client_config.server_name,
                    )
                    .expect("begin raw TLS connection")
                    .await
                    .expect("raw peer completes TLS but withholds binding confirmation")
            },
            listener.admit_bootstrap_for_test(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("raw TLS admission is bounded");
    let bootstrap = bootstrap.expect("listener returns an owned bootstrap");
    let start_task = tokio::spawn(bootstrap.start());
    tokio::time::timeout(TEST_TIMEOUT, async {
        while listener
            .registry
            .terminating_tasks
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bootstrap inner task registers before caller cancellation");
    start_task.abort();
    let join_error = match start_task.await {
        Err(error) => error,
        Ok(Err(error)) => panic!("aborted start caller returned actor error: {error}"),
        Ok(Ok(_actor)) => panic!("aborted start caller returned an actor"),
    };
    assert!(join_error.is_cancelled());

    let blocked = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener observes in-flight cleanup immediately")
        .expect_err("registered bootstrap cleanup still owns the sole slot");
    assert!(matches!(
        blocked,
        QuinnLegActorError::ListenerShutdownBlocked { live: 1 }
    ));
    let exporter = super::export_binding(&raw_connection, peer_policy)
        .expect("cancelled caller's raw peer derives the exact exporter");
    super::binding_confirmation(
        &raw_connection,
        super::ActorRole::Client,
        LegId::A,
        peer_policy,
        &exporter,
    )
    .await
    .expect("raw peer completes confirmation after the start caller is cancelled");
    let unclaimed_streams =
        super::bootstrap_streams(&raw_connection, super::ActorRole::Client, LegId::A)
            .await
            .expect("raw peer completes streams after the start caller is cancelled");
    drop(unclaimed_streams);
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            if listener
                .registry
                .live
                .load(std::sync::atomic::Ordering::Acquire)
                == 0
                && listener
                    .registry
                    .terminating_tasks
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
                && listener.open_connections_for_test() == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("detached inner bootstrap performs exact bounded cleanup");
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire),
        "caller cancellation does not turn successful structured cleanup into actor loss",
    );

    raw_connection.close(CLOSE_CODE, b"cancelled start test cleanup");
    raw_endpoint.close(CLOSE_CODE, b"cancelled start test cleanup");
    tokio::time::timeout(TEST_TIMEOUT, raw_endpoint.wait_idle())
        .await
        .expect("cancelled-start raw endpoint cleanup is bounded");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("cancelled start cleanup leaves a clean listener");
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_slow_bootstrap_does_not_head_of_line_block_a_second_connection() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x77; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x78; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let server_config = ProductionQuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        2,
        Duration::from_secs(2),
    )
    .expect("valid two-slot listener configuration with bounded bootstrap deadline");
    let mut listener = QuinnLegActorListener::bind(server_config)
        .await
        .expect("bind two-slot listener");

    let raw_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback raw-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid raw-client configuration");
    let raw_endpoint = super::build_client_endpoint(&raw_client_config, limits)
        .expect("build raw client endpoint");
    let (raw_connection, slow_bootstrap) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            async {
                raw_endpoint
                    .connect(
                        raw_client_config.server_addr,
                        &raw_client_config.server_name,
                    )
                    .expect("begin slow raw TLS connection")
                    .await
                    .expect("slow raw peer completes only TLS")
            },
            listener.admit_bootstrap_for_test(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("slow Incoming admission is bounded");
    let slow_bootstrap = slow_bootstrap.expect("listener returns slow owned bootstrap");
    let mut bootstrap_nursery = tokio::task::JoinSet::new();
    bootstrap_nursery.spawn(slow_bootstrap.start());

    let healthy_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0"
            .parse()
            .expect("loopback healthy-client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid healthy-client configuration");
    let (client, server) = tokio::time::timeout(Duration::from_secs(1), async {
        tokio::join!(
            QuinnLegActorFactory::connect(healthy_client_config, LegId::B, limits, peer_policy,),
            async {
                listener
                    .admit_bootstrap_for_test(LegId::B, limits, peer_policy)
                    .await?
                    .start()
                    .await
            },
        )
    })
    .await
    .expect("healthy connection publishes before the slow bootstrap deadline");
    let mut client = client.expect("healthy client actor publishes");
    let mut server = server.expect("healthy server actor publishes");
    client.request_cancel();
    server.request_cancel();
    let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), server.join())
    })
    .await
    .expect("healthy actor cleanup is bounded");
    client_join.expect("healthy client joins");
    server_join.expect("healthy server joins");

    let slow_result = tokio::time::timeout(TEST_TIMEOUT, bootstrap_nursery.join_next())
        .await
        .expect("structured bootstrap nursery drains")
        .expect("slow bootstrap task remains registered")
        .expect("slow bootstrap task joins");
    assert!(matches!(
        slow_result,
        Err(QuinnLegActorError::BootstrapTimedOut)
    ));
    assert!(bootstrap_nursery.is_empty());
    raw_connection.close(CLOSE_CODE, b"slow bootstrap test cleanup");
    raw_endpoint.close(CLOSE_CODE, b"slow bootstrap test cleanup");
    tokio::time::timeout(TEST_TIMEOUT, raw_endpoint.wait_idle())
        .await
        .expect("slow raw endpoint cleanup is bounded");
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown is bounded")
        .expect("drained bootstraps and joined actors leave no detached ownership");
}

#[test]
fn abnormal_bootstrap_guard_poisons_before_releasing_its_live_slot() {
    let registry = std::sync::Arc::new(super::ListenerRegistry::new(1));
    let unwind = std::panic::catch_unwind(std::panic::AssertUnwindSafe({
        let registry = std::sync::Arc::clone(&registry);
        move || {
            let reservation = registry.try_reserve().expect("reserve sole bootstrap slot");
            let _termination = super::BootstrapTaskTerminationGuard::new(&reservation);
            drop(reservation);

            let concurrent_registry = std::sync::Arc::clone(&registry);
            let blocked = std::thread::spawn(move || {
                matches!(
                    concurrent_registry.try_reserve(),
                    Err(QuinnLegActorError::ListenerRegistryFull)
                )
            })
            .join()
            .expect("concurrent admission probe joins");
            assert!(
                blocked,
                "the termination guard must retain the last slot before unwind poisons it",
            );
            panic!("simulate an abnormal bootstrap task exit");
        }
    }));

    assert!(unwind.is_err());
    assert!(registry.unclean.load(std::sync::atomic::Ordering::Acquire));
    assert_eq!(
        registry.live.load(std::sync::atomic::Ordering::Acquire),
        0,
        "the slot is released only after sticky poison is published",
    );
    assert!(matches!(
        registry.try_reserve(),
        Err(QuinnLegActorError::ListenerShutdownUnclean)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn prepublication_start_failure_releases_reservation_without_poisoning_listener() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x83; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x84; 16]).expect("nonzero device principal"),
    );
    let server_config = QuinnLegActorServerConfig::new(
        "127.0.0.1:0".parse().expect("loopback listener address"),
        cert_dir.join("server-cert.pem"),
        cert_dir.join("server-key.pem"),
        1,
    )
    .expect("one-slot listener configuration is valid");
    let mut listener =
        tokio::time::timeout(TEST_TIMEOUT, QuinnLegActorListener::bind(server_config))
            .await
            .expect("listener bind is bounded")
            .expect("bind one-slot listener");
    listener.fail_start_before_publication = true;
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");

    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("injected prepublication failure is bounded");
    drop(client);
    assert!(matches!(server, Err(QuinnLegActorError::InvalidConfig)));
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );
    assert_eq!(
        listener
            .registry
            .terminating_tasks
            .load(std::sync::atomic::Ordering::Acquire),
        0,
    );
    assert!(
        !listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire),
        "a reservation that never published an actor must not poison the listener",
    );

    listener.fail_start_before_publication = false;
    let retry_client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0"
            .parse()
            .expect("retry client loopback address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid retry client configuration");
    let (retry_client, retry_server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(retry_client_config, LegId::A, limits, peer_policy,),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("same listener retry bootstrap is bounded");
    let mut retry_client = retry_client.expect("retry publishes client actor");
    let mut retry_server = retry_server.expect("retry publishes server actor");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        1
    );
    assert!(matches!(
        listener.registry.try_reserve(),
        Err(QuinnLegActorError::ListenerRegistryFull),
    ));

    retry_client.request_cancel();
    retry_server.request_cancel();
    let (client_join, server_join) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(retry_client.join(), retry_server.join())
    })
    .await
    .expect("retry actor cleanup joins are bounded");
    client_join.expect("retry client joins cleanly");
    server_join.expect("retry server joins cleanly");
    assert_eq!(
        listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire),
        0
    );

    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener shutdown after rejected start is bounded")
        .expect("prepublication failure leaves a clean listener");
}

#[tokio::test(flavor = "current_thread")]
async fn unclaimed_server_handoff_preserves_structural_failure_poison() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid bootstrap limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0x8d; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x8e; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid server configuration"),
    )
    .await
    .expect("bind listener");
    let client_config = QuinnLegActorClientConfig::new(
        "127.0.0.1:0".parse().expect("loopback client address"),
        listener.local_addr(),
        "localhost",
        cert_dir.join("ca-cert.pem"),
    )
    .expect("valid client configuration");
    let (client, server) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(
            QuinnLegActorFactory::connect(client_config, LegId::A, limits, peer_policy),
            listener.accept(LegId::A, limits, peer_policy),
        )
    })
    .await
    .expect("actor bootstrap is bounded");
    let mut client = client.expect("publish client actor");
    let server = server.expect("publish server actor");
    server.fail_root_after_children_for_test();
    drop(super::ServerBootstrapHandoff::new(
        tokio::runtime::Handle::current(),
        server,
    ));

    tokio::time::timeout(TEST_TIMEOUT, async {
        while listener
            .registry
            .live
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
            || listener
                .registry
                .terminating_tasks
                .load(std::sync::atomic::Ordering::Acquire)
                != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("unclaimed handoff cleanup reaches exact zero ownership");
    assert!(
        listener
            .registry
            .unclean
            .load(std::sync::atomic::Ordering::Acquire)
    );

    let _client_outcome = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("peer actor cleanup is bounded");
    drop(client);
    let shutdown_error = tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("listener cleanup is bounded")
        .expect_err("ignored handoff join verdict cannot erase sticky poison");
    assert!(matches!(
        shutdown_error,
        QuinnLegActorError::ListenerShutdownUnclean
    ));
}
