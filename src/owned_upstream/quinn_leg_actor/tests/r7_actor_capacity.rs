use super::*;
use crate::owned_upstream::quinn_leg_composition::QuinnLegCompositionGate;

#[test]
fn actor_capacity_witness_retains_the_exact_checked_envelope() {
    let limits = composition_actor_limits();
    let witness = limits.capacity_witness();

    let timestamp_bytes = 8usize
        .checked_mul(std::mem::size_of::<SimTime>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Option<SimTime>>()))
        .and_then(|bytes| {
            bytes.checked_add(
                2usize
                    .checked_mul(std::mem::size_of::<super::super::ActorTerminalStampWitness>())
                    .expect("two terminal-stamp handles fit usize"),
            )
        })
        .expect("the fixed timestamp envelope fits usize");
    let expected_bytes = [
        262_144usize,
        262_144,
        262_144,
        72,
        RECORD_HEADER_BYTES,
        MAX_ENCODED_DATA_FRAME_BYTES,
        RECORD_HEADER_BYTES,
        timestamp_bytes,
    ]
    .into_iter()
    .try_fold(0usize, |total, value| total.checked_add(value))
    .expect("the actor byte envelope fits usize");

    assert_eq!(witness.max_owned_messages(), 22);
    assert_eq!(witness.max_owned_bytes(), expected_bytes);
    #[cfg(target_pointer_width = "64")]
    assert_eq!(witness.max_owned_bytes(), 852_209);
    assert_eq!(witness.terminal_stamp_handles(), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn started_actor_exposes_the_installed_capacity_only_to_composition() {
    let limits = composition_actor_limits();
    let expected = limits.capacity_witness();
    let cert_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = AttachPeerPolicy::new(
        OwnerIdentity::new([0xa1; 32]).expect("nonzero capacity owner identity"),
        DevicePrincipal::new([0xa2; 16]).expect("nonzero capacity device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0"
                .parse()
                .expect("loopback capacity listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            1,
        )
        .expect("valid capacity listener configuration"),
    )
    .await
    .expect("bind capacity listener");
    let (mut client, mut owner) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let gate = QuinnLegCompositionGate::mint_for_test();

    assert_eq!(client.capacity_witness(&gate), expected);
    assert_eq!(owner.capacity_witness(&gate), expected);

    let (client_joined, owner_joined) = tokio::time::timeout(TEST_TIMEOUT, async {
        tokio::join!(client.join(), owner.join())
    })
    .await
    .expect("capacity actors join within the fixed timeout");
    assert_eq!(
        client_joined
            .expect("capacity client joins structurally")
            .owned_bytes(),
        0
    );
    assert_eq!(
        owner_joined
            .expect("capacity owner joins structurally")
            .owned_bytes(),
        0
    );
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("capacity listener cleanup is bounded")
        .expect("capacity listener releases its exact actor slot");
}
