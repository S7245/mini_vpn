use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use super::super::{
    ActorTerminalStampWitness, InboundPublicationTracker, QuinnLegActorEncodedInbound,
    QuinnLegActorFlushDisposition, Readiness, arrival_beats_or_meets_deadline,
    checked_actor_timestamp_bytes,
};
use super::{
    QuinnLegActorListener, QuinnLegActorServerConfig, RealUnregisteredStandbyFixture, TEST_TIMEOUT,
    actor_attach_credentials, actor_session_config, complete_initial_actor_attach,
    composition_actor_limits, connect_real_actor_pair,
    real_unregistered_standby_fixture_with_target_state, take_actor_inbound,
    wait_actor_ordered_delivery,
};
use crate::owned_upstream::leg::BoundInbound;
use crate::owned_upstream::quinn_client_root::{
    RootRecoveryTimerSlots, RootStandbyBRecoveryKey, RootStandbyBRecoveryWake,
};
use crate::owned_upstream::quinn_leg_composition::{
    QuinnClientSessionComposition, QuinnCompositionProgress, QuinnOwnedStandbyRegistrationDrive,
    QuinnOwnedStandbyRegistrationErrorKind, QuinnStandbyInboundDrive, QuinnStandbyLivenessConfig,
    QuinnStandbyProbeArmDrive, QuinnStandbyProbeDrive, QuinnStandbyProbeExpiryDrive,
    QuinnStandbyProbeExpiryError,
};
use crate::owned_upstream::session_clock::SessionMonotonicClock;
use crate::owned_upstream::standby::{OwnerStandbyAdmission, OwnerStandbySlot};
use crate::owned_upstream::two_leg::{
    EncodedDelivery, LegId, SimTime, WireDirection, WireLane, WireRoute,
};
use crate::resumable::{
    DevicePrincipal, LegControlFrame, LegControlRecord, OwnerIdentity, STANDBY_CONTROL_V1,
    StandbyNonce,
};

#[tokio::test(start_paused = true)]
async fn complete_read_admission_captures_time_inside_the_publication_reservation() {
    let clock = SessionMonotonicClock::start();
    let readiness = Readiness::new();
    let publications = Arc::new(InboundPublicationTracker::default());

    tokio::time::advance(Duration::from_nanos(9)).await;
    let reservation = publications
        .reserve_arrival(readiness.clone(), &clock)
        .expect("the same-clock arrival is representable");

    assert_eq!(reservation.arrived_at(), SimTime::from_nanos(9));
    assert_eq!(reservation.epoch(), 1);
    assert!(publications.in_flight_at_or_before(1));
    assert_eq!(readiness.current(), 0);

    drop(reservation);
    assert_eq!(readiness.current(), 1);
    assert!(!publications.has_in_flight());
}

#[tokio::test(start_paused = true)]
async fn equal_deadline_admission_after_frozen_high_water_still_suppresses_timer_commit() {
    let clock = SessionMonotonicClock::start();
    let readiness = Readiness::new();
    let publications = Arc::new(InboundPublicationTracker::default());

    tokio::time::advance(Duration::from_nanos(10)).await;
    let cutoff = publications.capture_high_water(&readiness);
    let reservation = publications
        .reserve_arrival(readiness.clone(), &clock)
        .expect("equal-deadline actor admission is representable");
    assert!(reservation.epoch() > cutoff);
    assert_eq!(reservation.arrived_at(), SimTime::from_nanos(10));

    let timer_token = String::from("exact timer authority");
    let result =
        publications.transact_unless_arrived_by(SimTime::from_nanos(10), timer_token, |_| {
            panic!("equal-deadline in-flight admission must suppress timer commit")
        });
    assert_eq!(result, Err(String::from("exact timer authority")));

    drop(reservation);
    assert!(!publications.has_in_flight());
}

#[tokio::test(start_paused = true)]
async fn post_deadline_in_flight_admission_cannot_extend_the_absolute_timer() {
    let clock = SessionMonotonicClock::start();
    let readiness = Readiness::new();
    let publications = Arc::new(InboundPublicationTracker::default());

    tokio::time::advance(Duration::from_nanos(11)).await;
    let reservation = publications
        .reserve_arrival(readiness.clone(), &clock)
        .expect("post-deadline actor admission is representable");
    let committed = publications
        .transact_unless_arrived_by(SimTime::from_nanos(10), 7_u8, |token| token + 1)
        .expect("post-deadline input does not suppress the timer");
    assert_eq!(committed, 8);

    drop(reservation);
}

#[test]
fn immutable_arrival_accepts_before_and_equal_but_rejects_after_deadline() {
    let deadline = SimTime::from_nanos(10);

    assert!(arrival_beats_or_meets_deadline(
        SimTime::from_nanos(9),
        deadline,
    ));
    assert!(arrival_beats_or_meets_deadline(deadline, deadline));
    assert!(!arrival_beats_or_meets_deadline(
        SimTime::from_nanos(11),
        deadline,
    ));
}

#[test]
fn actor_timestamp_metadata_is_checked_before_capacity_publication() {
    let expected = (3 + 4) * std::mem::size_of::<SimTime>()
        + std::mem::size_of::<Option<SimTime>>()
        + 2 * std::mem::size_of::<ActorTerminalStampWitness>();
    assert_eq!(checked_actor_timestamp_bytes(3), Some(expected),);
    assert_eq!(checked_actor_timestamp_bytes(usize::MAX), None);
}

#[test]
fn blocked_envelope_moves_the_same_arrival_without_recapture() {
    let route = WireRoute::new(LegId::A, WireDirection::OwnerToClient);
    let delivery = EncodedDelivery::from_memory_ingress(
        route,
        WireLane::Data,
        Bytes::from_static(b"arrival-stable"),
        7,
        0,
    )
    .expect("valid encoded test ingress");
    let first = QuinnLegActorEncodedInbound::mint(delivery, SimTime::from_nanos(23));
    let (delivery, arrived_at) = first.into_parts();

    let retry = QuinnLegActorEncodedInbound::restore_delivery(delivery, arrived_at);
    assert_eq!(retry.arrived_at(), SimTime::from_nanos(23));
    let (delivery, retry_arrived_at) = retry.into_parts();
    assert_eq!(retry_arrived_at, arrived_at);
    assert_eq!(delivery.route(), route);
    assert_eq!(delivery.send_ordinal(), 7);
    assert_eq!(delivery.into_bytes(), Bytes::from_static(b"arrival-stable"));
}

#[tokio::test(flavor = "current_thread")]
async fn composition_rejects_a_different_clock_source_before_standby_mutation() {
    let limits = composition_actor_limits();
    let cert_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let peer_policy = super::super::AttachPeerPolicy::new(
        OwnerIdentity::new([0x95; 32]).expect("nonzero owner identity"),
        DevicePrincipal::new([0x96; 16]).expect("nonzero device principal"),
    );
    let mut listener = QuinnLegActorListener::bind(
        QuinnLegActorServerConfig::new(
            "127.0.0.1:0".parse().expect("loopback listener address"),
            cert_dir.join("server-cert.pem"),
            cert_dir.join("server-key.pem"),
            2,
        )
        .expect("two-slot clock-domain listener configuration"),
    )
    .await
    .expect("bind clock-domain listener");
    let (mut client_a, mut server_a) =
        connect_real_actor_pair(&mut listener, LegId::A, limits, peer_policy).await;
    let (accepted_a, owner_bootstrap) =
        complete_initial_actor_attach(&mut client_a, &mut server_a).await;
    wait_actor_ordered_delivery(&mut server_a).await;
    let mut composition = QuinnClientSessionComposition::bootstrap_initial(
        actor_session_config(),
        accepted_a,
        client_a,
    )
    .expect("initial A installs its exact session clock");
    let before = composition.snapshot();

    // The compatibility factory mints a fresh endpoint clock, so this B is
    // physically valid but cannot enter A's endpoint session.
    let (client_b, mut server_b) =
        connect_real_actor_pair(&mut listener, LegId::B, limits, peer_policy).await;
    let failure = composition
        .begin_owned_standby_registration(
            client_b,
            StandbyNonce::new([0x97; 16]).expect("nonzero standby nonce"),
            &actor_attach_credentials(),
            QuinnStandbyLivenessConfig::new(Duration::from_millis(30), Duration::from_millis(200))
                .expect("bounded standby liveness"),
        )
        .expect_err("different clock-source B must be returned whole");
    assert_eq!(
        failure.kind(),
        QuinnOwnedStandbyRegistrationErrorKind::ClockSourceMismatch,
    );
    assert_eq!(composition.snapshot(), before);
    let mut client_b = failure.into_actor();
    client_b.request_cancel();
    server_b.request_cancel();

    let (client_a_joined, server_a_joined, client_b_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                client_b.join(),
                server_b.join(),
            )
        })
        .await
        .expect("clock-domain rejection cleanup is bounded");
    let client_a_joined = client_a_joined.expect("client A joins structurally");
    assert_eq!(client_a_joined.owned_messages(), 0);
    assert_eq!(client_a_joined.owned_bytes(), 0);
    for joined in [
        server_a_joined.expect("server A joins structurally"),
        client_b_joined.expect("returned client B joins structurally"),
        server_b_joined.expect("server B joins structurally"),
    ] {
        assert_eq!(joined.owned_messages(), 0);
        assert_eq!(joined.owned_bytes(), 0);
    }
    drop((composition, owner_bootstrap));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("clock-domain listener shutdown is bounded")
        .expect("all exact listener slots are released");
}

#[tokio::test(flavor = "current_thread")]
async fn queued_b_ack_before_deadline_beats_delayed_root_expiry_and_keeps_actor_arrival_time() {
    let RealUnregisteredStandbyFixture {
        mut listener,
        mut composition,
        mut server_a,
        client_b,
        mut server_b,
        tcp_factory,
        local_flows,
        peer_policy: _,
        owner_executor,
        owner_active_a,
    } = real_unregistered_standby_fixture_with_target_state(2, 1, true).await;
    let mut standby_slot = OwnerStandbySlot::new();
    let mut recovery_timers: RootRecoveryTimerSlots = RootRecoveryTimerSlots::new();
    let session_clock = composition.session_clock();
    let probe_timeout = Duration::from_millis(500);
    let freshness_window = Duration::from_millis(200);
    let registration_at = session_clock
        .now()
        .expect("the registered-B session clock remains representable");

    assert_eq!(
        composition
            .begin_owned_standby_registration(
                client_b,
                StandbyNonce::new([0x97; 16]).expect("nonzero standby nonce"),
                &actor_attach_credentials(),
                QuinnStandbyLivenessConfig::new(probe_timeout, freshness_window)
                    .expect("bounded B liveness policy"),
            )
            .expect("composition consumes the whole unregistered B actor"),
        QuinnOwnedStandbyRegistrationDrive::Queued,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(registration_at)
            .expect("composition-owned B registration flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let registration_inbound =
        tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
            .await
            .expect("B registration receive is bounded")
            .expect("server B remains live")
            .expect("server B receives the exact registration");
    let candidate = owner_executor
        .authenticate_owner_standby(
            &owner_active_a,
            server_b
                .io
                .as_ref()
                .expect("server B owns its LegIo")
                .established_leg(),
            take_actor_inbound(registration_inbound),
        )
        .expect("owner authenticates the composition-owned B registration");
    assert_eq!(
        standby_slot
            .admit(
                candidate,
                server_b.queue.as_mut().expect("server B owns its FIFO"),
            )
            .expect("owner B standby slot admits the candidate"),
        OwnerStandbyAdmission::Installed,
    );
    assert_eq!(
        server_b
            .try_flush(registration_at)
            .expect("B registration acceptance flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.try_receive_registered_standby() {
                QuinnStandbyInboundDrive::Idle => tokio::task::yield_now().await,
                QuinnStandbyInboundDrive::RegistrationAccepted => break,
                outcome => panic!("composition-owned B registration failed: {outcome:?}"),
            }
        }
    })
    .await
    .expect("composition validates the real B acceptance within the local bound");
    wait_actor_ordered_delivery(&mut server_b).await;

    let probe_started_at = session_clock
        .now()
        .expect("the pre-probe session clock remains representable");
    assert_eq!(
        composition
            .drive_registered_standby_probe(probe_started_at)
            .expect("registered B builds one exact probe"),
        QuinnStandbyProbeDrive::Queued,
    );
    assert_eq!(
        composition
            .try_flush_registered_standby(probe_started_at)
            .expect("registered B probe flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let inbound = tokio::time::timeout(TEST_TIMEOUT, server_b.recv_classified(STANDBY_CONTROL_V1))
        .await
        .expect("registered B probe receive is bounded")
        .expect("server B remains live")
        .expect("server B receives the exact probe");
    let BoundInbound::LegControl(bound) = take_actor_inbound(inbound) else {
        panic!("registered B probe was misclassified as session data");
    };
    let (_seal, probe) = bound.into_parts();
    let LegControlRecord::LegProbe {
        session_id,
        leg_epoch_nonce,
        probe_sequence,
    } = probe.record().clone()
    else {
        panic!("registered B liveness emitted a non-probe record");
    };
    let armed_at = session_clock
        .now()
        .expect("the B probe arm time remains representable");
    let (deadline, armed_key) = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            match composition.arm_registered_standby_probe(armed_at) {
                Ok(QuinnStandbyProbeArmDrive::NotDelivered) => tokio::task::yield_now().await,
                Ok(QuinnStandbyProbeArmDrive::Armed {
                    deadline,
                    recovery_key,
                }) => break (deadline, recovery_key),
                outcome => panic!("registered B probe did not arm exactly once: {outcome:?}"),
            }
        }
    })
    .await
    .expect("registered B probe arms within the local bound");
    assert_eq!(armed_key.generation(), probe.leg_generation());
    assert_eq!(armed_key.sequence(), probe_sequence);
    let installed_key = recovery_timers
        .try_install_standby_b_wake(RootStandbyBRecoveryWake::new(armed_key.clone(), deadline))
        .expect("root owns the exact physical-B wake after arm");
    assert_eq!(installed_key, armed_key);
    assert_eq!(
        deadline,
        armed_at
            .checked_add(probe_timeout)
            .expect("bounded exact B probe deadline"),
    );

    let exact_ack = LegControlFrame::try_new(
        probe.leg_generation(),
        LegControlRecord::LegProbeAck {
            session_id,
            leg_epoch_nonce,
            probe_sequence,
        },
        STANDBY_CONTROL_V1,
    )
    .expect("exact registered B probe ACK is valid");
    let before_ack_flush = session_clock
        .now()
        .expect("the pre-ACK shared time remains representable");
    assert!(before_ack_flush <= deadline);
    server_b
        .enqueue_ordered_leg_control(exact_ack, STANDBY_CONTROL_V1)
        .expect("server B queues the exact pre-deadline ACK");
    let progress_cursor = composition.progress_cursor();
    assert_eq!(
        server_b
            .try_flush(before_ack_flush)
            .expect("pre-deadline B ACK flush remains live"),
        QuinnLegActorFlushDisposition::Submitted,
    );
    let progress = tokio::time::timeout(
        TEST_TIMEOUT,
        composition.wait_for_actor_progress(progress_cursor),
    )
    .await
    .expect("pre-deadline B ACK wakes the composition")
    .expect("standby actor progress remains observable");
    assert!(matches!(progress, QuinnCompositionProgress::Standby { .. }));
    let queued_by = session_clock
        .now()
        .expect("the post-publication shared time remains representable");
    assert!(
        queued_by <= deadline,
        "the actor must fully publish the ACK before the absolute probe deadline",
    );

    let delayed_root_boundary = deadline
        .checked_add(freshness_window)
        .expect("bounded delayed root boundary");
    tokio::time::timeout(TEST_TIMEOUT, async {
        while session_clock
            .now()
            .expect("the delayed-root session clock remains representable")
            <= delayed_root_boundary
        {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("root observation is deliberately delayed beyond the probe deadline");
    let delayed_root_now = session_clock
        .now()
        .expect("the delayed root observation remains representable");
    let stale_equal_scalar_key =
        RootStandbyBRecoveryKey::mint_for_test(armed_key.generation(), armed_key.sequence());
    let before_stale_expiry = composition.snapshot();
    assert_eq!(
        composition.try_expire_registered_standby_probe(delayed_root_now, &stale_equal_scalar_key,),
        Err(QuinnStandbyProbeExpiryError::RecoveryKeyMismatch),
    );
    assert_eq!(composition.snapshot(), before_stale_expiry);
    assert!(
        recovery_timers.standby_b_wake_for_key(&armed_key).is_some(),
        "the stale equal-scalar key leaves the live physical wake untouched",
    );
    assert_eq!(
        composition
            .try_expire_registered_standby_probe(delayed_root_now, &armed_key)
            .expect("queued pre-deadline input arbitrates before expiry"),
        QuinnStandbyProbeExpiryDrive::InboundPending { deadline },
    );
    let QuinnStandbyInboundDrive::ProbeFresh {
        fresh_until,
        recovery_key: Some(recovery_key),
    } = composition.try_receive_registered_standby()
    else {
        panic!("the exact queued pre-deadline B ACK did not mint freshness");
    };
    assert_eq!(recovery_key, armed_key);
    let settled_wake = recovery_timers
        .take_standby_b_wake_by_exact_key(&recovery_key)
        .expect("ACK settles only its exact physical-B wake");
    assert_eq!(settled_wake.recovery_key(), &armed_key);
    assert!(recovery_timers.is_empty());
    let earliest_fresh_until = before_ack_flush
        .checked_add(freshness_window)
        .expect("bounded lower actor-arrival freshness limit");
    let latest_fresh_until = queued_by
        .checked_add(freshness_window)
        .expect("bounded upper actor-arrival freshness limit");
    assert!(fresh_until >= earliest_fresh_until);
    assert!(fresh_until <= latest_fresh_until);
    assert!(
        fresh_until < delayed_root_now,
        "freshness must be based on actor arrival, not delayed root service time",
    );

    wait_actor_ordered_delivery(&mut server_b).await;
    let (client_joined, server_a_joined, server_b_joined) =
        tokio::time::timeout(TEST_TIMEOUT, async {
            tokio::join!(
                composition.join_current_for_shutdown(),
                server_a.join(),
                server_b.join(),
            )
        })
        .await
        .expect("arrival-arbitration tracer actors join within the local bound");
    let client_joined = client_joined.expect("client A/B join cleanly");
    let server_a_joined = server_a_joined.expect("server A joins cleanly");
    let server_b_joined = server_b_joined.expect("server B joins cleanly");
    assert_eq!(client_joined.owned_messages(), 0);
    assert_eq!(client_joined.owned_bytes(), 0);
    assert_eq!(server_a_joined.owned_messages(), 0);
    assert_eq!(server_a_joined.owned_bytes(), 0);
    assert_eq!(server_b_joined.owned_messages(), 0);
    assert_eq!(server_b_joined.owned_bytes(), 0);
    drop((
        composition,
        tcp_factory,
        local_flows,
        owner_executor,
        owner_active_a,
        standby_slot,
    ));
    tokio::time::timeout(TEST_TIMEOUT, listener.shutdown())
        .await
        .expect("arrival-arbitration listener cleanup is bounded")
        .expect("arrival-arbitration tracer releases both actor slots");
}
