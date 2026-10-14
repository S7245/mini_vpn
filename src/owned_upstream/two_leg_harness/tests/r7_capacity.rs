use super::*;
use crate::resumable::{
    AttachProof, BitsPerSecond, DirectionalRates, FeatureSet, HintSequence, LegControlFrame,
    LegControlRecord, LegEpochNonce, ProbeSequence, ReplayCapacitySpec, ReplayStorageGeometry,
    SessionRole, StandbyNonce, StandbyProof, SwitchHintCause,
};

mod progress_scan;

fn byte_plan(live_limit: usize, historical_limit: usize) -> R7ByteLedgerPlan {
    R7ByteLedgerPlan::new(
        [live_limit; R7LiveByteDimension::COUNT],
        [historical_limit; R7HistoricalByteDimension::COUNT],
    )
    .expect("checked R7 byte ledger plan")
}

fn replay_storage(max_flows: usize) -> ReplayStoragePlan {
    let rates = DirectionalRates::symmetric(BitsPerSecond::new(100_000_000).unwrap());
    ReplayCapacitySpec::new(rates, rates, Duration::from_millis(500), Duration::ZERO)
        .unwrap()
        .derive()
        .unwrap()
        .storage_plan(ReplayStorageGeometry::new(62_500, max_flows).unwrap())
        .unwrap()
}

fn session_configs(
    storage: ReplayStoragePlan,
    max_terminal_tombstones: usize,
) -> (SessionConfig, SessionConfig) {
    let client = SessionConfig::from_storage_plan(
        SessionRole::Client,
        max_terminal_tombstones,
        storage,
        64 * 1024,
    )
    .unwrap();
    let owner = SessionConfig::from_storage_plan(
        SessionRole::Owner,
        max_terminal_tombstones,
        storage,
        64 * 1024,
    )
    .unwrap();
    (client, owner)
}

fn session_witness(
    storage: ReplayStoragePlan,
    max_terminal_tombstones: usize,
) -> R7SessionCapacityWitness {
    let (client, owner) = session_configs(storage, max_terminal_tombstones);
    R7SessionCapacityWitness::derive(storage, client, owner).unwrap()
}

fn manifest_byte_plan(
    storage: ReplayStoragePlan,
    other_live_limit: usize,
    historical_limit: usize,
) -> R7ByteLedgerPlan {
    let global = storage.global();
    let mut live = [other_live_limit; R7LiveByteDimension::COUNT];
    live[R7LiveByteDimension::Replay.index()] = global
        .client_to_target()
        .max_bytes()
        .checked_add(global.target_to_client().max_bytes())
        .unwrap();
    R7ByteLedgerPlan::new(live, [historical_limit; R7HistoricalByteDimension::COUNT]).unwrap()
}

const fn wire_faults(max_positive_accept_pieces: usize) -> R7WireFaultBounds {
    R7WireFaultBounds::new(max_positive_accept_pieces, 1, 1)
}

fn fixed_row_codec_len(kind: R7FixedWireKind) -> usize {
    let session_id = SessionId::new([0x71; 16]).unwrap();
    let attach_nonce = AttachNonce::new([0x72; 16]).unwrap();
    let standby_nonce = StandbyNonce::new([0x73; 16]).unwrap();
    let generation = LegGeneration::new(2).unwrap();
    let features = FeatureSet::STANDBY_CONTROL_V1;
    let session_frame = |record| {
        Frame::try_new(generation, record)
            .unwrap()
            .encode()
            .unwrap()
            .len()
    };
    let leg_control = |record| {
        LegControlFrame::try_new(generation, record, features)
            .unwrap()
            .encode(features)
            .unwrap()
            .len()
    };

    match kind {
        R7FixedWireKind::InitialAttach
        | R7FixedWireKind::ReplacementAttach
        | R7FixedWireKind::StatusFollowUpAttach => session_frame(Record::Attach {
            session_id,
            nonce: attach_nonce,
            min_version: SESSION_PROTOCOL_VERSION,
            max_version: SESSION_PROTOCOL_VERSION,
            offered_features: features,
            required_features: features,
            proof: AttachProof::new([0x74; 32]).unwrap(),
        }),
        R7FixedWireKind::InitialAttachAccepted | R7FixedWireKind::ReplacementAttachAccepted => {
            session_frame(Record::AttachAccepted {
                session_id,
                nonce: attach_nonce,
                selected_version: SESSION_PROTOCOL_VERSION,
                features,
            })
        }
        R7FixedWireKind::Open | R7FixedWireKind::ReplacementRecoveryOpen => {
            session_frame(Record::Open {
                flow_id: SessionFlowId::new(1).unwrap(),
                target: TargetAddr::DomainPort {
                    host: "x".repeat(253),
                    port: 443,
                },
            })
        }
        R7FixedWireKind::OpenResult | R7FixedWireKind::ReplacementRecoveryOpenResult => {
            session_frame(Record::OpenResult {
                flow_id: SessionFlowId::new(1).unwrap(),
                result: OpenResultCode::Opened,
            })
        }
        R7FixedWireKind::GenerationStatus => session_frame(Record::AttachGenerationStatus {
            session_id,
            requested_generation: generation,
            nonce: attach_nonce,
        }),
        R7FixedWireKind::FirstBApplicationAck
        | R7FixedWireKind::ReplacementRecoveryTombstone
        | R7FixedWireKind::ReplacementRecoveryAck => session_frame(Record::Ack {
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            next_accepted: ByteOffset::new(1),
            final_accepted: false,
        }),
        R7FixedWireKind::Terminal | R7FixedWireKind::ReplacementRecoveryClose => {
            session_frame(Record::Close {
                flow_id: SessionFlowId::new(1).unwrap(),
                direction: Direction::TargetToClient,
                final_offset: ByteOffset::new(1),
            })
        }
        R7FixedWireKind::StandbyRegistration => leg_control(LegControlRecord::StandbyRegister {
            session_id,
            standby_nonce,
            selected_version: SESSION_PROTOCOL_VERSION,
            features,
            proof: StandbyProof::new([0x75; 32]).unwrap(),
        }),
        R7FixedWireKind::StandbyRegistrationAccepted => {
            leg_control(LegControlRecord::StandbyAccepted {
                session_id,
                standby_nonce,
                selected_version: SESSION_PROTOCOL_VERSION,
                features,
            })
        }
        R7FixedWireKind::CurrentAProbe | R7FixedWireKind::StandbyBProbe => {
            leg_control(LegControlRecord::LegProbe {
                session_id,
                leg_epoch_nonce: LegEpochNonce::new([0x76; 16]).unwrap(),
                probe_sequence: ProbeSequence::new(1).unwrap(),
            })
        }
        R7FixedWireKind::CurrentAProbeAck | R7FixedWireKind::StandbyBProbeAck => {
            leg_control(LegControlRecord::LegProbeAck {
                session_id,
                leg_epoch_nonce: LegEpochNonce::new([0x76; 16]).unwrap(),
                probe_sequence: ProbeSequence::new(1).unwrap(),
            })
        }
        R7FixedWireKind::SwitchHint => leg_control(LegControlRecord::SwitchHint {
            session_id,
            standby_nonce,
            hint_sequence: HintSequence::new(1).unwrap(),
            flow_id: SessionFlowId::new(1).unwrap(),
            direction: Direction::TargetToClient,
            oldest_unacknowledged: ByteOffset::new(0),
            cause: SwitchHintCause::ReverseApplicationAckStall,
        }),
    }
}

#[test]
fn r7_manifest_rejects_unbounded_flow_or_probe_episode_domains_exactly() {
    let storage = replay_storage(1);
    let sessions = session_witness(storage, 1);
    let faults = wire_faults(2);
    let bytes = manifest_byte_plan(storage, 1, 1);
    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            sessions,
            R7ScenarioEpisodeBounds::new(1, 0, 1),
            faults,
            bytes,
        ),
        Err(R7CapacityError::ZeroFlowEpisodes)
    );
    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            sessions,
            R7ScenarioEpisodeBounds::new(1, 1, 0),
            faults,
            bytes,
        ),
        Err(R7CapacityError::ZeroProbeEpisodes)
    );
    let max_flow_episodes = usize::MAX / 2 + 1;
    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            sessions,
            R7ScenarioEpisodeBounds::new(1, max_flow_episodes, 1),
            faults,
            bytes,
        ),
        Err(R7CapacityError::FlowTerminalMessagesOverflow { max_flow_episodes })
    );
}

#[test]
fn r7_manifest_rejects_replay_byte_plan_smaller_than_storage_ownership() {
    let storage = replay_storage(1);

    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            session_witness(storage, 3),
            R7ScenarioEpisodeBounds::new(1, 1, 1),
            wire_faults(1),
            byte_plan(1, 1),
        ),
        Err(R7CapacityError::ReplayLiveByteCapacityTooSmall {
            required: 12_500_000,
            actual: 1,
        })
    );
}

#[test]
fn r7_session_capacity_witness_rejects_storage_or_tombstone_drift() {
    let storage = replay_storage(1);
    let (client, owner) = session_configs(storage, 3);
    let (_, owner_from_other_storage) = session_configs(replay_storage(2), 3);
    let (_, owner_with_other_tombstones) = session_configs(storage, 4);

    assert_eq!(
        R7SessionCapacityWitness::derive(storage, client, owner_from_other_storage),
        Err(R7CapacityError::SessionConfigNotDerivedFromReplayStorage {
            role: SessionRole::Owner,
        })
    );
    assert_eq!(
        R7SessionCapacityWitness::derive(storage, client, owner_with_other_tombstones),
        Err(R7CapacityError::SessionTerminalTombstoneCapacityMismatch {
            client: 3,
            owner: 4,
        })
    );
    assert!(R7SessionCapacityWitness::derive(storage, client, owner).is_ok());
}

#[test]
fn r7_replacement_recovery_non_data_wire_rows_are_explicit_and_additive() {
    let storage = replay_storage(1);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 3),
        R7ScenarioEpisodeBounds::new(3, 2, 3),
        wire_faults(2),
        manifest_byte_plan(storage, 1_024, 2_048),
    )
    .unwrap();

    for (kind, expected_messages) in [
        (R7FixedWireKind::ReplacementRecoveryTombstone, 6),
        (R7FixedWireKind::ReplacementRecoveryOpen, 1),
        (R7FixedWireKind::ReplacementRecoveryOpenResult, 1),
        (R7FixedWireKind::ReplacementRecoveryAck, 2),
        (R7FixedWireKind::ReplacementRecoveryClose, 2),
    ] {
        assert_eq!(
            manifest.wire.fixed.row(kind).max_messages,
            expected_messages
        );
        assert_eq!(
            manifest.wire.fixed.row(kind).encoded_bytes_per_message,
            fixed_row_codec_len(kind)
        );
    }
    assert_eq!(manifest.wire.fixed.max_messages(), 42);
    assert_eq!(manifest.wire.fixed.encoded_once_bytes(), 2_960);
    assert_eq!(manifest.wire.max_send_messages, 843);
    assert_eq!(manifest.wire.max_send_bytes, 25_032_998);
    assert_eq!(manifest.wire.max_delivery_messages, 1_686);
    assert_eq!(manifest.wire.max_delivery_bytes, 50_065_996);
}

#[test]
fn r7_no_oracle_manifest_locks_encoded_once_data_and_named_fixed_wire_rows() {
    let storage = replay_storage(1);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 3),
        R7ScenarioEpisodeBounds::new(3, 2, 3),
        wire_faults(2),
        manifest_byte_plan(storage, 1_024, 2_048),
    )
    .expect("checked necessary-only R7 capacity manifest");

    assert_eq!(manifest.sessions.replay_storage, storage);
    assert_eq!(
        manifest.sessions.replay_storage.horizon().effective(),
        Duration::from_millis(500)
    );
    assert_eq!(manifest.sessions.replay_storage.geometry().max_flows(), 1);
    assert_eq!(
        manifest
            .sessions
            .replay_storage
            .global()
            .client_to_target()
            .max_segments(),
        100
    );
    assert_eq!(manifest.sessions.max_terminal_tombstones, 3);
    assert_eq!(manifest.sessions.client.max_terminal_tombstones(), 3);
    assert_eq!(manifest.sessions.owner.max_terminal_tombstones(), 3);
    assert_eq!(manifest.episodes, R7ScenarioEpisodeBounds::new(3, 2, 3));
    assert_eq!(
        manifest.wire.client_to_target.data_application_bytes,
        6_250_000
    );
    assert_eq!(manifest.wire.client_to_target.data_logical_messages, 100);
    assert_eq!(manifest.wire.target_to_client.data_logical_messages, 100);
    assert_eq!(
        manifest
            .wire
            .client_to_target
            .coalesced_record_payload_bytes,
        62_500
    );
    assert_eq!(
        manifest.wire.client_to_target.data_tail_segment_allowance,
        1
    );
    assert_eq!(
        manifest.wire.client_to_target.data_encoded_once_bytes,
        6_250_000 + 37 * 100
    );
    assert_eq!(manifest.wire.data_encoded_once_messages, 200);
    assert_eq!(manifest.wire.data_encoded_once_bytes, 12_507_400);
    assert_eq!(manifest.wire.client_to_target.application_ack_messages, 201);
    assert_eq!(manifest.wire.target_to_client.application_ack_messages, 201);
    assert_eq!(manifest.wire.application_ack_messages, 402);
    assert_eq!(manifest.wire.application_ack_encoded_once_bytes, 15_276);

    for kind in R7FixedWireKind::ALL {
        let row = manifest.wire.fixed.row(kind);
        assert_eq!(row.kind, kind);
        assert_eq!(row.encoded_bytes_per_message, fixed_row_codec_len(kind));
        assert_eq!(
            row.max_messages,
            match kind {
                R7FixedWireKind::Open | R7FixedWireKind::OpenResult => 2,
                R7FixedWireKind::CurrentAProbe
                | R7FixedWireKind::CurrentAProbeAck
                | R7FixedWireKind::StandbyBProbe
                | R7FixedWireKind::StandbyBProbeAck => manifest.episodes.probe_rounds,
                R7FixedWireKind::Terminal => manifest.episodes.flow_lifecycles * 2,
                R7FixedWireKind::ReplacementRecoveryTombstone => 6,
                R7FixedWireKind::ReplacementRecoveryOpen
                | R7FixedWireKind::ReplacementRecoveryOpenResult => 1,
                R7FixedWireKind::ReplacementRecoveryAck
                | R7FixedWireKind::ReplacementRecoveryClose => 2,
                _ => 1,
            }
        );
    }
    assert_eq!(manifest.wire.fixed.max_messages(), 42);
    assert_eq!(manifest.wire.fixed.encoded_once_bytes(), 2_960);
    assert_eq!(manifest.wire.max_send_messages, 843);
    assert_eq!(manifest.wire.max_send_bytes, 25_032_998);
    assert_eq!(manifest.wire.max_delivery_messages, 1_686);
    assert_eq!(manifest.wire.max_delivery_bytes, 50_065_996);
    assert_eq!(manifest.wire.max_wire_delivery_events, 1_686);
    assert_eq!(manifest.wire.max_wire_delivery_owned_bytes, 50_065_996);
    for dimension in R7LiveByteDimension::ALL {
        assert_eq!(
            manifest.bytes.live_limit(dimension),
            if dimension == R7LiveByteDimension::Replay {
                12_500_000
            } else {
                1_024
            }
        );
    }
    for dimension in R7HistoricalByteDimension::ALL {
        assert_eq!(manifest.bytes.historical_limit(dimension), 2_048);
    }
    assert_eq!(manifest.required_replay_live_bytes, 12_500_000);
    assert_eq!(manifest.bytes.max_live_bytes(), 12_511_264);
    assert_eq!(manifest.bytes.max_historical_bytes(), 5 * 2_048);
    assert_eq!(
        manifest
            .slots
            .client_timers
            .limit(R7ClientTimerSlot::TerminalGrace),
        3
    );
    assert_eq!(
        manifest
            .slots
            .owner_timers
            .limit(R7OwnerTimerSlot::TerminalGrace),
        3
    );
    assert_eq!(
        manifest
            .slots
            .client_root
            .limit(R7ClientRootSlot::PendingAttach),
        1
    );
    assert_eq!(
        manifest
            .slots
            .client_root
            .limit(R7ClientRootSlot::AttachCandidateActor),
        1
    );
    assert_eq!(
        manifest
            .slots
            .owner_root
            .limit(R7OwnerRootSlot::ReverseProgressCursor),
        1
    );
    assert_eq!(
        manifest
            .slots
            .owner_root
            .limit(R7OwnerRootSlot::AttachCandidateActor),
        1
    );
}

#[test]
fn r7_flow_tombstone_scan_probe_and_additive_tail_bounds_do_not_borrow() {
    let storage = replay_storage(4);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 9),
        R7ScenarioEpisodeBounds::new(2, 3, 4),
        wire_faults(2),
        manifest_byte_plan(storage, 1, 1),
    )
    .expect("independent checked scenario bounds");

    assert_eq!(manifest.sessions.replay_storage.geometry().max_flows(), 4);
    let directional_storage = manifest.sessions.replay_storage.global().client_to_target();
    assert_eq!(directional_storage.max_bytes(), 6_250_000);
    assert_eq!(directional_storage.max_segments(), 103);
    assert_eq!(directional_storage.tail_segment_allowance(), 4);
    let one_tail_messages = (directional_storage.max_bytes() - 1)
        / manifest
            .sessions
            .replay_storage
            .geometry()
            .coalesced_record_payload_bytes()
        + 1;
    assert_eq!(one_tail_messages, 100);
    assert_eq!(
        manifest.wire.client_to_target.data_tail_segment_allowance,
        4
    );
    assert_eq!(manifest.wire.client_to_target.data_logical_messages, 103);
    assert_eq!(
        manifest.wire.client_to_target.data_logical_messages,
        one_tail_messages as u64 + 3
    );
    assert_eq!(
        manifest
            .slots
            .client_timers
            .limit(R7ClientTimerSlot::TerminalGrace),
        9
    );
    assert_eq!(
        manifest
            .slots
            .owner_timers
            .limit(R7OwnerTimerSlot::TerminalGrace),
        9
    );
    let mut client_timers = manifest.slots.client_timers.ledger();
    client_timers
        .try_admit(R7ClientTimerSlot::TerminalGrace, 9)
        .unwrap();
    let client_before = client_timers;
    assert_eq!(
        client_timers.try_admit(R7ClientTimerSlot::TerminalGrace, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7ClientTimerSlot::TerminalGrace,
            attempted: 10,
            limit: 9,
        })
    );
    assert_eq!(client_timers, client_before);
    let mut owner_timers = manifest.slots.owner_timers.ledger();
    owner_timers
        .try_admit(R7OwnerTimerSlot::TerminalGrace, 9)
        .unwrap();
    let owner_before = owner_timers;
    assert_eq!(
        owner_timers.try_admit(R7OwnerTimerSlot::TerminalGrace, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7OwnerTimerSlot::TerminalGrace,
            attempted: 10,
            limit: 9,
        })
    );
    assert_eq!(owner_timers, owner_before);
    assert_eq!(
        manifest.wire.fixed.row(R7FixedWireKind::Open).max_messages,
        3
    );
    assert_eq!(
        manifest
            .wire
            .fixed
            .row(R7FixedWireKind::OpenResult)
            .max_messages,
        3
    );
    assert_eq!(
        manifest
            .wire
            .fixed
            .row(R7FixedWireKind::Terminal)
            .max_messages,
        6
    );
    for kind in [
        R7FixedWireKind::CurrentAProbe,
        R7FixedWireKind::CurrentAProbeAck,
        R7FixedWireKind::StandbyBProbe,
        R7FixedWireKind::StandbyBProbeAck,
    ] {
        assert_eq!(manifest.wire.fixed.row(kind).max_messages, 4);
    }
}

#[test]
fn r7_fixed_wire_row_rejects_multiply_overflow_exactly() {
    let row = R7FixedWireRow::new(R7FixedWireKind::Terminal, usize::MAX, 2);

    assert_eq!(
        row.encoded_bytes(),
        Err(R7CapacityError::FixedWireRowBytesOverflow {
            kind: R7FixedWireKind::Terminal,
        })
    );

    assert_eq!(
        R7FixedWireCapacity::derive(
            1,
            usize::MAX - 12,
            1,
            R7ReplacementRecoveryWireCapacity::ZERO,
        ),
        Err(R7CapacityError::FixedWireMessagesOverflow)
    );
    let direction_entries = (usize::MAX - 1_220) / 37 + 1;
    assert_eq!(
        R7FixedWireCapacity::derive(
            1,
            direction_entries,
            1,
            R7ReplacementRecoveryWireCapacity::ZERO,
        ),
        Err(R7CapacityError::FixedWireBytesOverflow)
    );
}

#[test]
fn r7_application_ack_capacity_rejects_zero_and_overflow_exactly() {
    let storage = replay_storage(1);
    let fixed =
        R7FixedWireCapacity::derive(1, 2, 1, R7ReplacementRecoveryWireCapacity::ZERO).unwrap();
    assert_eq!(
        R7WireCapacity::derive(storage, wire_faults(0), fixed),
        Err(R7CapacityError::DataCapacity(
            CapacityError::ZeroPositiveAcceptancePieces,
        ))
    );

    let expected = if usize::BITS == 64 {
        R7CapacityError::ApplicationAckMessagesOverflow
    } else {
        R7CapacityError::WirePlatformCapacityOverflow
    };
    assert_eq!(
        R7WireCapacity::derive(storage, wire_faults(usize::MAX), fixed),
        Err(expected)
    );
    assert_eq!(
        R7WireCapacity::derive(storage, R7WireFaultBounds::new(2, 2, 1), fixed),
        Err(R7CapacityError::DataCapacity(
            CapacityError::TooManyReplayCopies,
        ))
    );
    assert_eq!(
        R7WireCapacity::derive(storage, R7WireFaultBounds::new(2, 1, 2), fixed),
        Err(R7CapacityError::DataCapacity(
            CapacityError::TooManyDuplicateCopies,
        ))
    );
}

#[test]
fn r7_named_slots_are_independent_and_reject_n_plus_one_exactly() {
    let capacities = R7PhysicalSlotCapacities::derive(6);
    for domain in R7ClientTimerSlot::ALL {
        assert_eq!(
            capacities.client_timers.limit(domain),
            if domain == R7ClientTimerSlot::TerminalGrace {
                6
            } else {
                1
            }
        );
    }
    for domain in R7OwnerTimerSlot::ALL {
        assert_eq!(
            capacities.owner_timers.limit(domain),
            if domain == R7OwnerTimerSlot::TerminalGrace {
                6
            } else {
                1
            }
        );
    }
    for domain in R7ClientRootSlot::ALL {
        assert_eq!(capacities.client_root.limit(domain), 1);
    }
    for domain in R7OwnerRootSlot::ALL {
        assert_eq!(capacities.owner_root.limit(domain), 1);
    }

    let mut client = capacities.client_root.ledger();
    client
        .try_admit(R7ClientRootSlot::PendingAttach, 1)
        .unwrap();
    let before = client;
    assert_eq!(
        client.try_admit(R7ClientRootSlot::PendingAttach, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7ClientRootSlot::PendingAttach,
            attempted: 2,
            limit: 1,
        })
    );
    assert_eq!(client, before);
    assert_eq!(client.live(R7ClientRootSlot::CurrentActor), 0);
    assert_eq!(
        client.release(R7ClientRootSlot::PendingAttach, 2),
        Err(R7SlotAdmissionError::ReleaseUnderflow {
            domain: R7ClientRootSlot::PendingAttach,
            current: 1,
            released: 2,
        })
    );
    assert_eq!(client, before);

    let mut client_timer = capacities.client_timers.ledger();
    client_timer
        .try_admit(R7ClientTimerSlot::StandbyBProbe, 1)
        .unwrap();
    assert_eq!(
        client_timer.try_admit(R7ClientTimerSlot::StandbyBProbe, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7ClientTimerSlot::StandbyBProbe,
            attempted: 2,
            limit: 1,
        })
    );
    assert_eq!(
        client_timer.live(R7ClientTimerSlot::LocalApplicationAckStall),
        0
    );

    let mut owner = capacities.owner_timers.ledger();
    owner
        .try_admit(R7OwnerTimerSlot::AggregateReverseStall, 1)
        .unwrap();
    assert_eq!(
        owner.try_admit(R7OwnerTimerSlot::AggregateReverseStall, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7OwnerTimerSlot::AggregateReverseStall,
            attempted: 2,
            limit: 1,
        })
    );
    assert_eq!(owner.live(R7OwnerTimerSlot::StandbyBProbe), 0);
    owner
        .release(R7OwnerTimerSlot::AggregateReverseStall, 1)
        .unwrap();
    assert!(owner.is_exact_zero());

    let mut owner_root = capacities.owner_root.ledger();
    owner_root
        .try_admit(R7OwnerRootSlot::OutputContinuation, 1)
        .unwrap();
    assert_eq!(
        owner_root.try_admit(R7OwnerRootSlot::OutputContinuation, 1),
        Err(R7SlotAdmissionError::CapacityExceeded {
            domain: R7OwnerRootSlot::OutputContinuation,
            attempted: 2,
            limit: 1,
        })
    );
    assert_eq!(owner_root.live(R7OwnerRootSlot::OutputPage), 0);
}

#[test]
fn r7_named_slot_counter_overflow_is_exact_and_transactional() {
    let capacity = R7NamedSlotCapacity::<R7ClientRootSlot, { R7ClientRootSlot::COUNT }>::new(
        [usize::MAX; R7ClientRootSlot::COUNT],
    );
    let mut ledger = capacity.ledger();
    ledger.try_admit(R7ClientRootSlot::Quarantine, 1).unwrap();
    let before = ledger;

    assert_eq!(
        ledger.try_admit(R7ClientRootSlot::Quarantine, usize::MAX),
        Err(R7SlotAdmissionError::CountOverflow {
            domain: R7ClientRootSlot::Quarantine,
            current: 1,
            additional: usize::MAX,
        })
    );
    assert_eq!(ledger, before);
}

#[test]
fn r7_live_byte_ledger_rejects_n_plus_one_and_overflow_without_mutation() {
    let mut live_limits = [0; R7LiveByteDimension::COUNT];
    live_limits[R7LiveByteDimension::ActorFifos.index()] = 3;
    let plan = R7ByteLedgerPlan::new(live_limits, [16; R7HistoricalByteDimension::COUNT])
        .expect("the exact per-domain capacities have a representable total");
    let mut ledger = plan.ledger();
    ledger
        .try_add_live(R7LiveByteDimension::ActorFifos, 3)
        .unwrap();
    let before_n_plus_one = ledger;
    assert_eq!(
        ledger.try_add_live(R7LiveByteDimension::ActorFifos, 1),
        Err(R7ByteLedgerError::CapacityExceeded {
            dimension: R7ByteDimension::Live(R7LiveByteDimension::ActorFifos),
            attempted: 4,
            limit: 3,
        })
    );
    assert_eq!(ledger, before_n_plus_one);
    assert_eq!(ledger.live_bytes(R7LiveByteDimension::PendingControl), 0);

    let mut overflow_limits = [0; R7LiveByteDimension::COUNT];
    overflow_limits[R7LiveByteDimension::Quarantine.index()] = usize::MAX;
    let mut overflow_ledger =
        R7ByteLedgerPlan::new(overflow_limits, [0; R7HistoricalByteDimension::COUNT])
            .unwrap()
            .ledger();
    overflow_ledger
        .try_add_live(R7LiveByteDimension::Quarantine, 1)
        .unwrap();
    let before_overflow = overflow_ledger;
    assert_eq!(
        overflow_ledger.try_add_live(R7LiveByteDimension::Quarantine, usize::MAX),
        Err(R7ByteLedgerError::CounterOverflow {
            dimension: R7ByteDimension::Live(R7LiveByteDimension::Quarantine),
            current: 1,
            additional: usize::MAX,
        })
    );
    assert_eq!(overflow_ledger, before_overflow);
}

#[test]
fn r7_history_is_sticky_settlement_evidence_but_not_live_ownership() {
    let plan = byte_plan(8, 8);
    let mut ledger = plan.ledger();
    ledger
        .try_add_live(R7LiveByteDimension::ShutdownResidual, 8)
        .unwrap();
    ledger
        .settle_live_to_history(
            R7LiveByteDimension::ShutdownResidual,
            R7HistoricalByteDimension::ShutdownDiscarded,
            8,
        )
        .unwrap();

    assert!(ledger.live_is_exact_zero());
    assert_eq!(ledger.live_total(), 0);
    assert_eq!(ledger.historical_total(), 8);
    assert_eq!(
        ledger.historical_bytes(R7HistoricalByteDimension::ShutdownDiscarded),
        8
    );

    ledger
        .try_add_live(R7LiveByteDimension::TerminalResidual, 1)
        .unwrap();
    let before = ledger;
    assert_eq!(
        ledger.settle_live_to_history(
            R7LiveByteDimension::TerminalResidual,
            R7HistoricalByteDimension::ShutdownDiscarded,
            1,
        ),
        Err(R7ByteLedgerError::CapacityExceeded {
            dimension: R7ByteDimension::Historical(R7HistoricalByteDimension::ShutdownDiscarded,),
            attempted: 9,
            limit: 8,
        })
    );
    assert_eq!(ledger, before);

    assert_eq!(
        ledger.settle_live_to_history(
            R7LiveByteDimension::TerminalResidual,
            R7HistoricalByteDimension::TerminalDiscarded,
            2,
        ),
        Err(R7ByteLedgerError::SettlementUnderflow {
            dimension: R7LiveByteDimension::TerminalResidual,
            current: 1,
            settled: 2,
        })
    );
    assert_eq!(ledger, before);
}

#[test]
fn r7_success_release_and_cancel_settlement_are_distinct_exact_zero_paths() {
    let plan = byte_plan(4, 4);
    let mut success = plan.ledger();
    success
        .try_add_live(R7LiveByteDimension::DriverPort, 4)
        .unwrap();
    success
        .release_live(R7LiveByteDimension::DriverPort, 4)
        .unwrap();
    assert!(success.live_is_exact_zero());
    assert_eq!(success.historical_total(), 0);

    let mut cancelled = plan.ledger();
    cancelled
        .try_add_live(R7LiveByteDimension::RootOutputFifo, 4)
        .unwrap();
    cancelled
        .settle_live_to_history(
            R7LiveByteDimension::RootOutputFifo,
            R7HistoricalByteDimension::CancelledDiscarded,
            4,
        )
        .unwrap();
    assert!(cancelled.live_is_exact_zero());
    assert_eq!(
        cancelled.historical_bytes(R7HistoricalByteDimension::CancelledDiscarded),
        4
    );
    assert_eq!(
        cancelled.historical_bytes(R7HistoricalByteDimension::ShutdownDiscarded),
        0
    );
}

#[test]
fn r7_byte_capacity_plan_rejects_each_total_overflow_exactly() {
    let mut live = [0; R7LiveByteDimension::COUNT];
    live[0] = usize::MAX;
    live[1] = 1;
    assert_eq!(
        R7ByteLedgerPlan::new(live, [0; R7HistoricalByteDimension::COUNT]),
        Err(R7CapacityError::LiveByteCapacityOverflow)
    );

    let mut historical = [0; R7HistoricalByteDimension::COUNT];
    historical[0] = usize::MAX;
    historical[1] = 1;
    assert_eq!(
        R7ByteLedgerPlan::new([0; R7LiveByteDimension::COUNT], historical),
        Err(R7CapacityError::HistoricalByteCapacityOverflow)
    );

    let mut live_limits = [0; R7LiveByteDimension::COUNT];
    live_limits[R7LiveByteDimension::Quarantine.index()] = usize::MAX;
    let mut historical_limits = [0; R7HistoricalByteDimension::COUNT];
    historical_limits[R7HistoricalByteDimension::FatalDiscarded.index()] = usize::MAX;
    let mut ledger = R7ByteLedgerPlan::new(live_limits, historical_limits)
        .unwrap()
        .ledger();
    ledger
        .try_add_live(R7LiveByteDimension::Quarantine, 1)
        .unwrap();
    ledger
        .settle_live_to_history(
            R7LiveByteDimension::Quarantine,
            R7HistoricalByteDimension::FatalDiscarded,
            1,
        )
        .unwrap();
    ledger
        .try_add_live(R7LiveByteDimension::Quarantine, usize::MAX)
        .unwrap();
    let before = ledger;
    assert_eq!(
        ledger.settle_live_to_history(
            R7LiveByteDimension::Quarantine,
            R7HistoricalByteDimension::FatalDiscarded,
            usize::MAX,
        ),
        Err(R7ByteLedgerError::CounterOverflow {
            dimension: R7ByteDimension::Historical(R7HistoricalByteDimension::FatalDiscarded,),
            current: 1,
            additional: usize::MAX,
        })
    );
    assert_eq!(ledger, before);
}
