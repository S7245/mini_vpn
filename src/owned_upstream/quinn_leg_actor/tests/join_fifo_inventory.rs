use super::*;

#[tokio::test(flavor = "current_thread")]
async fn join_fails_closed_without_dropping_a_nonempty_actor_fifo() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xd1; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xd2; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid listener configuration"),
    )
    .await
    .expect("bind focused join listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    let frame = ordered_control(41);
    let expected_bytes = frame
        .encode(STANDBY_CONTROL_V1)
        .expect("focused control frame is encodable")
        .len();
    let _receipt = client
        .enqueue_ordered_leg_control(frame, STANDBY_CONTROL_V1)
        .expect("actor FIFO admits one focused residual frame");
    let before = client.actor_snapshot();
    assert_eq!(before.outbound_queue_messages(), 1);
    assert_eq!(before.outbound_queue_bytes(), expected_bytes);

    let first_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("focused residual join is bounded")
        .expect_err("a nonempty actor FIFO cannot mint a false-zero Joined witness");
    assert!(matches!(
        first_error,
        QuinnLegActorError::ResidualOwnership {
            messages: 1,
            bytes,
        } if bytes == expected_bytes
    ));
    assert_eq!(
        client.actor_snapshot(),
        before,
        "failed join must retain the exact underlying actor FIFO for audit and retry",
    );

    let second_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("focused residual rejoin is bounded")
        .expect_err("retained FIFO remains fail-closed on retry");
    assert!(matches!(
        second_error,
        QuinnLegActorError::ResidualOwnership {
            messages: 1,
            bytes,
        } if bytes == expected_bytes
    ));
    assert_eq!(client.actor_snapshot(), before);

    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("focused peer cleanup is bounded")
        .expect("the empty peer actor still joins cleanly");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop(client);
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("focused listener cleanup is bounded")
        .expect("dropping the retained failed owner releases the listener slot");
}

#[tokio::test(flavor = "current_thread")]
async fn join_fails_closed_on_an_ordered_delivery_wait_after_fifo_submission() {
    let limits = QuinnLegActorLimits::bootstrap_default().expect("valid actor limits");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xd3; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0xd4; 16]).expect("nonzero device principal"),
    );
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid listener configuration"),
    )
    .await
    .expect("bind ordered-wait join listener");
    let (mut client, mut server) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;

    let receipt = client
        .enqueue_ordered_leg_control(ordered_control(42), STANDBY_CONTROL_V1)
        .expect("actor FIFO admits one ordered residual frame");
    assert_eq!(
        client
            .try_flush(SimTime::ZERO)
            .expect("submit ordered frame"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    assert!(!receipt.was_delivered());

    let first_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("ordered-wait residual join is bounded")
        .expect_err("an ordered-delivery wait cannot mint a false-zero Joined witness");
    assert!(matches!(
        first_error,
        QuinnLegActorError::ResidualOwnership {
            messages: 1,
            bytes: 0,
        }
    ));
    let retained = client.actor_snapshot();
    assert_eq!(retained.outbound_queue_messages(), 0);
    assert_eq!(retained.outbound_queue_bytes(), 0);
    assert_eq!(retained.outbound_owned_messages(), 0);
    assert_eq!(retained.outbound_owned_bytes(), 0);
    assert!(
        !receipt.was_delivered(),
        "join must not manufacture peer delivery while retaining the exact wait",
    );

    let second_error = tokio::time::timeout(TEST_TIMEOUT, client.join())
        .await
        .expect("ordered-wait residual rejoin is bounded")
        .expect_err("the retained ordered wait remains fail-closed on retry");
    assert!(matches!(
        second_error,
        QuinnLegActorError::ResidualOwnership {
            messages: 1,
            bytes: 0,
        }
    ));

    let server_joined = tokio::time::timeout(TEST_TIMEOUT, server.join())
        .await
        .expect("ordered-wait peer cleanup is bounded")
        .expect("the empty peer actor still joins cleanly");
    assert_eq!(server_joined.owned_messages(), 0);
    assert_eq!(server_joined.owned_bytes(), 0);
    drop((client, receipt));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("ordered-wait listener cleanup is bounded")
        .expect("dropping the retained failed owner releases the listener slot");
}
