use super::*;
use crate::shared::TargetAddr;
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode};
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

#[derive(Clone)]
struct TcpPoolInstallStateTransport {
    stable_id: usize,
    path_generation: u64,
    cwnd: u64,
    closed: bool,
}

impl TcpPoolTransport for TcpPoolInstallStateTransport {
    fn tcp_pool_stable_id(&self) -> usize {
        self.stable_id
    }

    fn tcp_pool_install_service_state(&self) -> Option<TcpPoolInstallServiceState> {
        Some(TcpPoolInstallServiceState {
            path_generation: self.path_generation,
            cwnd: self.cwnd,
            closed: self.closed,
        })
    }
}

async fn install_successor_with_current_service(
    current_path_generation: u64,
    current_cwnd: u64,
    closed: bool,
    proved_path_generation: u64,
    proved_final_cwnd: u64,
) -> Result<TcpPoolGenerationInstall, TcpPoolGenerationInstallError> {
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(
        1,
        TcpPoolInstallStateTransport {
            stable_id: 11,
            path_generation: 1,
            cwnd: 24_800,
            closed: false,
        },
        1,
        1,
        clock,
    );
    let predecessor_identity = TcpPoolTransportIdentity::new(11, 1);
    let predecessor_activity = slot.current_activity().await;
    let successor = TcpPoolGeneration::new_successor(
        TcpPoolInstallStateTransport {
            stable_id: 22,
            path_generation: current_path_generation,
            cwnd: current_cwnd,
            closed,
        },
        2,
        1,
        clock,
        proved_path_generation,
        26_424,
    )
    .unwrap();
    let proof = TcpPoolSuccessorInstallServiceProof::for_successor(&successor, proved_final_cwnd)
        .expect("the replay proof must remain above first-turn readiness");

    slot.install_successor_with(
        predecessor_identity,
        &predecessor_activity,
        successor,
        proof,
        Instant::now(),
        |_, _| true,
    )
    .await
}

fn tcp_pool_observations(path_service: &[TcpPoolPathService]) -> Vec<TcpPoolPathObservation> {
    path_service
        .iter()
        .copied()
        .enumerate()
        .map(|(index, path_service)| match path_service {
            TcpPoolPathService::Unknown => TcpPoolPathObservation::Unknown,
            TcpPoolPathService::Known { .. } => TcpPoolPathObservation::Known {
                identity: TcpPoolTransportIdentity::new(index, 1),
                path_service,
                black_holes_detected: 0,
                path_generation: None,
                successor_service_certificate: None,
            },
        })
        .collect()
}

fn endpoint_recovery_connection(
    stable_id: usize,
    black_holes_detected: u64,
    pressure: Option<TcpWritePressureSnapshot>,
) -> EndpointRecoveryConnectionSample {
    EndpointRecoveryConnectionSample {
        stable_id,
        tx_bytes: 100_000,
        rx_bytes: 10_000,
        rtt: Duration::from_millis(159),
        black_holes_detected,
        current_socket_rx_rebind_generation: 0,
        tcp_write_pressures: pressure.into_iter().collect(),
        tcp_ordered_read_progress: Vec::new(),
    }
}

fn endpoint_recovery_path_sample(
    connections: Vec<EndpointRecoveryConnectionSample>,
) -> EndpointRecoveryInput {
    EndpointRecoveryInput {
        active_tcp: 1,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: 0,
        connections,
    }
}

fn endpoint_recovery_pending(
    writer: u64,
    episode: u64,
    pending_for: Duration,
    ack_stalled_for: Duration,
    acknowledged_bytes: u64,
) -> TcpWritePressureSnapshot {
    TcpWritePressureSnapshot {
        writer,
        stream: writer.saturating_mul(2).saturating_add(1),
        episode,
        pending_for,
        ack_stalled_for,
        acknowledged_bytes,
    }
}

fn endpoint_recovery_ordered_progress(
    reader: u64,
    stream: u64,
    read_offset: u64,
    next_received_offset: Option<u64>,
    highest_received_offset: u64,
    buffered_bytes: usize,
) -> TcpOrderedReadProgressSnapshot {
    TcpOrderedReadProgressSnapshot {
        reader,
        stream,
        read_offset,
        next_received_offset,
        highest_received_offset,
        buffered_bytes,
        ordered_gap_bytes: next_received_offset
            .unwrap_or(read_offset)
            .saturating_sub(read_offset),
    }
}

#[test]
fn recovery_evidence_observes_persistent_ordered_gap_without_endpoint_action() {
    let started = Instant::now();
    let mut recovery = EndpointRecoveryState::default();
    let mut evidence = RecoveryEvidenceObserver::default();
    let sample = |read_offset, next_received_offset, highest_received_offset, buffered_bytes| {
        let mut connection = endpoint_recovery_connection(11, 0, None);
        connection.tcp_ordered_read_progress = vec![endpoint_recovery_ordered_progress(
            7,
            21,
            read_offset,
            next_received_offset,
            highest_received_offset,
            buffered_bytes,
        )];
        endpoint_recovery_path_sample(vec![connection])
    };

    let first_gap = sample(569_624_937, Some(569_626_217), 569_721_669, 96_732);
    assert!(evidence.observe(started, &first_gap).is_empty());
    assert_eq!(
        recovery.observe(started, &first_gap),
        EndpointRecoveryAction::None,
        "read-only evidence cannot authorize Endpoint mutation"
    );
    let growing_tail = sample(569_624_937, Some(569_626_217), 570_721_669, 1_096_732);
    let events = evidence.observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &growing_tail);
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0],
        RecoveryEvidence::OrderedGapObserved {
            stable_id: 11,
            reader: 7,
            stream: 21,
            episode: 1,
            read_offset: 569_624_937,
            next_received_offset: 569_626_217,
            initial_highest_received_offset: 569_721_669,
            current_highest_received_offset: 570_721_669,
            initial_buffered_bytes: 96_732,
            current_buffered_bytes: 1_096_732,
            ordered_gap_bytes: 1_280,
            observations: 2,
            tail_advanced: true,
            ..
        }
    ));
    assert_eq!(
        recovery.observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &growing_tail,),
        EndpointRecoveryAction::None,
        "even a persistent gap with a growing tail remains observation-only"
    );
    assert!(
        evidence
            .observe(
                started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2,
                &growing_tail,
            )
            .is_empty()
    );

    let progressed = sample(570_721_669, None, 570_721_669, 0);
    assert!(
        evidence
            .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 3, &progressed,)
            .is_empty()
    );

    let later_gap = sample(700_000_000, Some(700_001_280), 700_080_000, 80_000);
    assert!(
        evidence
            .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 4, &later_gap,)
            .is_empty(),
        "a later gap must establish fresh observation ownership"
    );
    assert!(matches!(
        evidence
            .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 5, &later_gap,)
            .as_slice(),
        [RecoveryEvidence::OrderedGapObserved { episode: 2, .. }]
    ));
}

#[test]
fn recovery_evidence_aggregates_writer_ack_progress_until_episode_end() {
    let started = Instant::now();
    let mut evidence = RecoveryEvidenceObserver::default();
    let sample = |pressure| {
        endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, pressure)])
    };

    let first = sample(Some(endpoint_recovery_pending(
        3,
        7,
        Duration::from_millis(250),
        Duration::from_millis(250),
        1_000,
    )));
    assert!(matches!(
        evidence.observe(started, &first).as_slice(),
        [RecoveryEvidence::TcpWritePressureStarted {
            stable_id: 11,
            writer: 3,
            stream: 7,
            episode: 7,
            acknowledged_bytes: 1_000,
            ..
        }]
    ));

    let progressed = sample(Some(endpoint_recovery_pending(
        3,
        7,
        Duration::from_millis(500),
        Duration::from_millis(50),
        65_000,
    )));
    assert!(
        evidence
            .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL, &progressed)
            .is_empty()
    );

    let ended = sample(None);
    assert!(matches!(
        evidence
            .observe(started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2, &ended)
            .as_slice(),
        [RecoveryEvidence::TcpWritePressureEnded {
            stable_id: 11,
            writer: 3,
            stream: 7,
            episode: 7,
            observations: 2,
            initial_acknowledged_bytes: 1_000,
            final_acknowledged_bytes: 65_000,
            ack_progress_observations: 1,
            max_pending_for,
            max_ack_stalled_for,
            ..
        }] if *max_pending_for == Duration::from_millis(500)
            && *max_ack_stalled_for == Duration::from_millis(250)
    ));
}

#[test]
fn recovery_evidence_does_not_merge_writer_identity_or_episode_replacement() {
    let started = Instant::now();
    let mut evidence = RecoveryEvidenceObserver::default();
    let input = |stable_id, episode, acknowledged_bytes| {
        endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
            stable_id,
            0,
            Some(endpoint_recovery_pending(
                3,
                episode,
                Duration::from_millis(250),
                Duration::from_millis(250),
                acknowledged_bytes,
            )),
        )])
    };

    assert_eq!(evidence.observe(started, &input(11, 7, 1_000)).len(), 1);
    let episode_events = evidence.observe(
        started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL,
        &input(11, 8, 2_000),
    );
    assert_eq!(episode_events.len(), 2);
    assert!(matches!(
        episode_events[0],
        RecoveryEvidence::TcpWritePressureEnded {
            stable_id: 11,
            episode: 7,
            observations: 1,
            ..
        }
    ));
    assert!(matches!(
        episode_events[1],
        RecoveryEvidence::TcpWritePressureStarted {
            stable_id: 11,
            episode: 8,
            acknowledged_bytes: 2_000,
            ..
        }
    ));

    let identity_events = evidence.observe(
        started + ENDPOINT_RECOVERY_SAMPLE_INTERVAL * 2,
        &input(12, 9, 3_000),
    );
    assert_eq!(identity_events.len(), 2);
    assert!(matches!(
        identity_events[0],
        RecoveryEvidence::TcpWritePressureEnded {
            stable_id: 11,
            episode: 8,
            observations: 1,
            ..
        }
    ));
    assert!(matches!(
        identity_events[1],
        RecoveryEvidence::TcpWritePressureStarted {
            stable_id: 12,
            episode: 9,
            acknowledged_bytes: 3_000,
            ..
        }
    ));
}

#[test]
fn endpoint_recovery_writer_ack_stall_is_unchanged_by_ordered_gap_evidence() {
    let started = Instant::now();
    let rtt = Duration::from_millis(159);
    let bound = endpoint_recovery_stall_bound(rtt);
    let mut state = EndpointRecoveryState::default();
    let mut connection = endpoint_recovery_connection(
        11,
        0,
        Some(endpoint_recovery_pending(3, 7, bound, bound, 0)),
    );
    connection.tcp_ordered_read_progress = vec![endpoint_recovery_ordered_progress(
        9,
        21,
        569_624_937,
        Some(569_626_217),
        569_721_669,
        96_732,
    )];
    let sample = endpoint_recovery_path_sample(vec![connection]);

    assert!(matches!(
        state.observe(started, &sample),
        EndpointRecoveryAction::Rebind {
            trigger: EndpointRecoveryTrigger::TcpWriteStall {
                stable_id: 11,
                writer: 3,
                episode: 7,
                ..
            },
            ..
        }
    ));
}

#[test]
fn endpoint_recovery_does_not_reset_an_ack_progressing_path_on_black_hole_advance() {
    let started = Instant::now();
    let rtt = Duration::from_millis(159);
    let bound = endpoint_recovery_stall_bound(rtt);
    let mut state = EndpointRecoveryState::default();

    let baseline = endpoint_recovery_path_sample(vec![
        endpoint_recovery_connection(11, 0, None),
        endpoint_recovery_connection(12, 0, None),
    ]);
    assert_eq!(
        state.observe(started, &baseline),
        EndpointRecoveryAction::None
    );

    let degraded = endpoint_recovery_path_sample(vec![
        endpoint_recovery_connection(
            11,
            1,
            Some(endpoint_recovery_pending(
                7,
                9,
                bound,
                Duration::from_millis(250),
                64 * 1024,
            )),
        ),
        endpoint_recovery_connection(
            12,
            0,
            Some(endpoint_recovery_pending(
                8,
                10,
                bound + Duration::from_secs(1),
                Duration::from_millis(250),
                128 * 1024,
            )),
        ),
    ]);
    let action = state.observe(started + bound, &degraded);
    assert_eq!(
        action,
        EndpointRecoveryAction::None,
        "continuing exact ACK progress must leave native QUIC recovery in control despite writer Pending and black-hole advance"
    );
}

#[test]
fn endpoint_recovery_repeated_black_hole_advance_does_not_override_ack_progress() {
    let started = Instant::now();
    let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
    let mut state = EndpointRecoveryState::default();

    assert_eq!(
        state.observe(
            started,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
        ),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(
            started + Duration::from_millis(250),
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 1, None)])
        ),
        EndpointRecoveryAction::None,
        "an idle black-hole increment must only refresh the anchor"
    );
    assert_eq!(
        state.observe(
            started + bound,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                11,
                1,
                Some(endpoint_recovery_pending(
                    7,
                    9,
                    bound,
                    Duration::from_millis(250),
                    65_536,
                )),
            )])
        ),
        EndpointRecoveryAction::None,
        "Pending and ACK progress without a same-window black-hole increment is ordinary backpressure"
    );
    assert_eq!(
        state.observe(
            started + bound + Duration::from_millis(250),
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                11,
                2,
                Some(endpoint_recovery_pending(
                    7,
                    9,
                    bound + Duration::from_millis(250),
                    Duration::from_millis(250),
                    131_072,
                )),
            )])
        ),
        EndpointRecoveryAction::None,
        "later black-hole movement still cannot override continuing exact ACK progress"
    );
}

#[test]
fn endpoint_recovery_counter_regression_is_not_recovery_authority() {
    let started = Instant::now();
    let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
    let mut state = EndpointRecoveryState::default();
    assert_eq!(
        state.observe(
            started,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 5, None)])
        ),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(
            started + bound,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
                11,
                4,
                Some(endpoint_recovery_pending(
                    7,
                    9,
                    bound,
                    Duration::from_millis(250),
                    65_536,
                )),
            )])
        ),
        EndpointRecoveryAction::None,
        "a regressed cumulative counter is unknown evidence and must fail closed"
    );
}

#[test]
fn endpoint_recovery_exact_ack_stall_rebinds() {
    let started = Instant::now();
    let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
    let mut state = EndpointRecoveryState::default();
    assert_eq!(
        state.observe(
            started,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
        ),
        EndpointRecoveryAction::None
    );
    let action = state.observe(
        started + bound,
        &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
            11,
            1,
            Some(endpoint_recovery_pending(7, 9, bound, bound, 0)),
        )]),
    );
    assert!(matches!(
        action,
        EndpointRecoveryAction::Rebind {
            trigger: EndpointRecoveryTrigger::TcpWriteStall { stable_id: 11, .. },
            ..
        }
    ));
}

#[test]
fn endpoint_recovery_does_not_overlap_rebind_recovery() {
    let started = Instant::now();
    let bound = endpoint_recovery_stall_bound(Duration::from_millis(159));
    let mut state = EndpointRecoveryState::default();
    assert_eq!(
        state.observe(
            started,
            &endpoint_recovery_path_sample(vec![endpoint_recovery_connection(11, 0, None)])
        ),
        EndpointRecoveryAction::None
    );
    let hard_stall = endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
        11,
        1,
        Some(endpoint_recovery_pending(7, 9, bound, bound, 0)),
    )]);
    assert!(matches!(
        state.observe(started + bound, &hard_stall),
        EndpointRecoveryAction::Rebind { .. }
    ));
    state.note_rebind_succeeded(started + bound, 1);

    let mut waiting_for_current_socket =
        endpoint_recovery_path_sample(vec![endpoint_recovery_connection(
            11,
            2,
            Some(endpoint_recovery_pending(
                7,
                9,
                bound + Duration::from_millis(250),
                bound + Duration::from_millis(250),
                0,
            )),
        )]);
    waiting_for_current_socket.current_socket_rx_rebind_generation = 1;
    assert_eq!(
        state.observe(
            started + bound + Duration::from_millis(250),
            &waiting_for_current_socket,
        ),
        EndpointRecoveryAction::None,
        "an in-flight Endpoint rebind owns recovery until every sampled connection authenticates the current socket"
    );
}

#[test]
fn endpoint_recovery_rebinds_on_continuous_tcp_write_stall_despite_rx_progress() {
    let started = Instant::now();
    let rtt = Duration::from_millis(159);
    let mut state = EndpointRecoveryState::default();
    let sample = |rx_bytes, pending_for| EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes: 100_000,
            rx_bytes,
            rtt,
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: vec![TcpWritePressureSnapshot {
                writer: 1,
                stream: 3,
                episode: 7,
                pending_for,
                ack_stalled_for: pending_for,
                acknowledged_bytes: 0,
            }],
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &sample(1_000, Duration::ZERO)),
        EndpointRecoveryAction::None
    );
    let bound = endpoint_recovery_stall_bound(rtt);
    let action = state.observe(started + bound, &sample(2_000, bound));
    assert!(
        matches!(
            action,
            EndpointRecoveryAction::Rebind {
                trigger: EndpointRecoveryTrigger::TcpWriteStall {
                    stable_id: 11,
                    episode: 7,
                    ..
                },
                ..
            }
        ),
        "continuous business-stream write pressure must survive unrelated/ACK RX: {action:?}"
    );
}

#[test]
fn endpoint_recovery_does_not_rebind_while_pending_stream_acknowledges_progress() {
    let started = Instant::now();
    let mut state = EndpointRecoveryState::default();
    let input = EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes: 100_000,
            rx_bytes: 2_000,
            rtt: Duration::from_millis(159),
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: vec![TcpWritePressureSnapshot {
                writer: 1,
                stream: 3,
                episode: 7,
                pending_for: Duration::from_secs(5),
                ack_stalled_for: Duration::from_millis(250),
                acknowledged_bytes: 64 * 1024,
            }],
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &input),
        EndpointRecoveryAction::None,
        "application backpressure with business-stream ACK progress is not a path black hole"
    );
}

#[test]
fn endpoint_recovery_one_rebind_covers_every_current_tcp_write_episode() {
    let started = Instant::now();
    let bound = Duration::from_secs(2);
    let mut state = EndpointRecoveryState::default();
    let sample = |socket_generation| EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: socket_generation,
        connections: vec![
            EndpointRecoveryConnectionSample {
                stable_id: 11,
                tx_bytes: 100_000,
                rx_bytes: 1_000 + socket_generation,
                rtt: Duration::from_millis(100),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: socket_generation,
                tcp_write_pressures: vec![TcpWritePressureSnapshot {
                    writer: 1,
                    stream: 3,
                    episode: 7,
                    pending_for: bound,
                    ack_stalled_for: bound,
                    acknowledged_bytes: 0,
                }],
                tcp_ordered_read_progress: Vec::new(),
            },
            EndpointRecoveryConnectionSample {
                stable_id: 12,
                tx_bytes: 200_000,
                rx_bytes: 2_000 + socket_generation,
                rtt: Duration::from_millis(150),
                black_holes_detected: 0,
                current_socket_rx_rebind_generation: socket_generation,
                tcp_write_pressures: vec![TcpWritePressureSnapshot {
                    writer: 2,
                    stream: 5,
                    episode: 9,
                    pending_for: bound,
                    ack_stalled_for: bound,
                    acknowledged_bytes: 0,
                }],
                tcp_ordered_read_progress: Vec::new(),
            },
        ],
    };

    let action = state.observe(started, &sample(0));
    assert!(matches!(action, EndpointRecoveryAction::Rebind { .. }));
    state.note_rebind_succeeded(started, 1);
    assert!(matches!(
        state.observe(started + Duration::from_millis(250), &sample(1)),
        EndpointRecoveryAction::Recovered { .. }
    ));
    assert_eq!(
        state.observe(started + Duration::from_millis(500), &sample(1)),
        EndpointRecoveryAction::None,
        "one Endpoint migration must cover every write episode already pending at rebind time"
    );

    let cleared = EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: 1,
        connections: sample(1)
            .connections
            .into_iter()
            .map(|mut connection| {
                connection.tcp_write_pressures.clear();
                connection
            })
            .collect(),
    };
    assert_eq!(
        state.observe(started + Duration::from_millis(750), &cleared),
        EndpointRecoveryAction::None
    );
    let mut next_episode = sample(1);
    next_episode.connections[0].tcp_write_pressures.clear();
    next_episode.connections[1].tcp_write_pressures = vec![TcpWritePressureSnapshot {
        writer: 3,
        stream: 7,
        episode: 10,
        pending_for: bound,
        ack_stalled_for: bound,
        acknowledged_bytes: 0,
    }];
    assert!(matches!(
        state.observe(started + Duration::from_secs(3), &next_episode),
        EndpointRecoveryAction::Rebind {
            generation: 2,
            trigger: EndpointRecoveryTrigger::TcpWriteStall {
                stable_id: 12,
                episode: 10,
                ..
            },
            ..
        }
    ));
}

#[test]
fn endpoint_recovery_one_rebind_covers_all_pending_writers_on_one_connection() {
    let started = Instant::now();
    let bound = Duration::from_secs(2);
    let mut state = EndpointRecoveryState::default();
    let sample = |socket_generation| EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: socket_generation,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes: 100_000,
            rx_bytes: 1_000 + socket_generation,
            rtt: Duration::from_millis(100),
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: socket_generation,
            tcp_write_pressures: vec![
                TcpWritePressureSnapshot {
                    writer: 1,
                    stream: 3,
                    episode: 7,
                    pending_for: bound,
                    ack_stalled_for: bound,
                    acknowledged_bytes: 0,
                },
                TcpWritePressureSnapshot {
                    writer: 2,
                    stream: 5,
                    episode: 8,
                    pending_for: bound,
                    ack_stalled_for: bound,
                    acknowledged_bytes: 0,
                },
            ],
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert!(matches!(
        state.observe(started, &sample(0)),
        EndpointRecoveryAction::Rebind { .. }
    ));
    state.note_rebind_succeeded(started, 1);
    assert!(matches!(
        state.observe(started + Duration::from_millis(250), &sample(1)),
        EndpointRecoveryAction::Recovered { .. }
    ));
    assert_eq!(
        state.observe(started + Duration::from_millis(500), &sample(1)),
        EndpointRecoveryAction::None,
        "one socket migration covers every writer sampled on that connection"
    );
}

#[test]
fn tcp_write_pressure_selects_the_oldest_per_stream_ack_stall() {
    let started = Instant::now();
    let pressure = Arc::new(TcpWritePressure::new(started));
    let mut first = pressure.writer(3);
    let mut second = pressure.writer(5);

    first.note_pending_at(started, 0);
    let first_snapshot = pressure
        .snapshot_at(started + Duration::from_secs(1))
        .expect("first pending writer starts an episode");
    assert_eq!(first_snapshot.episode, 1);
    assert_eq!(first_snapshot.pending_for, Duration::from_secs(1));

    second.note_pending_at(started + Duration::from_millis(500), 0);
    first.note_ready();
    assert_eq!(
        pressure
            .snapshot_at(started + Duration::from_secs(2))
            .expect("second writer still owns the shared episode"),
        TcpWritePressureSnapshot {
            writer: 2,
            stream: 5,
            episode: 2,
            pending_for: Duration::from_millis(1500),
            ack_stalled_for: Duration::from_millis(1500),
            acknowledged_bytes: 0,
        }
    );

    drop(second);
    assert_eq!(pressure.snapshot_at(started + Duration::from_secs(2)), None);

    let mut third = pressure.writer(7);
    third.note_pending_at(started + Duration::from_secs(3), 0);
    assert_eq!(
        pressure.snapshot_at(started + Duration::from_secs(4)),
        Some(TcpWritePressureSnapshot {
            writer: 3,
            stream: 7,
            episode: 3,
            pending_for: Duration::from_secs(1),
            ack_stalled_for: Duration::from_secs(1),
            acknowledged_bytes: 0,
        })
    );
}

#[test]
fn tcp_write_pressure_ack_progress_resets_stall_without_clearing_pending() {
    let started = Instant::now();
    let pressure = Arc::new(TcpWritePressure::new(started));
    let mut writer = pressure.writer(3);

    writer.note_pending_at(started, 0);
    writer.note_acknowledged_at(started + Duration::from_secs(2), 64 * 1024);

    assert_eq!(
        pressure
            .snapshot_at(started + Duration::from_millis(2250))
            .expect("the writer remains application-blocked")
            .ack_stalled_for,
        Duration::from_millis(250),
        "acknowledged business-stream bytes reset only the recovery-stall clock"
    );
}

#[tokio::test]
async fn tcp_write_pressure_adapter_tracks_async_write_pending_edges() {
    struct GateWriter {
        ready: Arc<AtomicBool>,
    }

    impl AsyncWrite for GateWriter {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            if self.ready.load(Ordering::Acquire) {
                Poll::Ready(Ok(buf.len()))
            } else {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    let pressure = Arc::new(TcpWritePressure::new(Instant::now()));
    let ready = Arc::new(AtomicBool::new(false));
    let mut writer = TcpWritePressureAdapter::new(
        GateWriter {
            ready: ready.clone(),
        },
        pressure.writer(3),
    );
    let write = tokio::spawn(async move { writer.write_all(b"pending").await });
    tokio::task::yield_now().await;
    assert!(
        pressure.snapshot_at(Instant::now()).is_some(),
        "a real AsyncWrite Pending edge must arm connection pressure"
    );

    ready.store(true, Ordering::Release);
    write.await.expect("writer task").expect("write completes");
    assert_eq!(pressure.snapshot_at(Instant::now()), None);
}

#[tokio::test]
async fn tcp_ordered_read_pressure_samples_live_reader_and_releases_on_drop() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let mut marker = [0u8; 1];
        recv.read_exact(&mut marker).await.unwrap();
        send.write_all(b"buffered ordered bytes").await.unwrap();
        send.finish().unwrap();
        let _ = release_rx.await;
    });

    let connection = client_endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();
    let (mut send, recv) = connection.open_bi().await.unwrap();
    let stream = recv.id().index();
    let pressure = Arc::new(TcpOrderedReadPressure::new());
    let reader = pressure.reader(stream, recv.progress_handle());
    send.write_all(&[0x5a]).await.unwrap();

    let snapshots = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let snapshots = pressure.snapshots();
            if !snapshots.is_empty() {
                break snapshots;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("live ordered reader progress");
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0].reader, 1);
    assert_eq!(snapshots[0].stream, stream);
    assert_eq!(snapshots[0].read_offset, 0);
    assert_eq!(snapshots[0].next_received_offset, Some(0));
    assert_eq!(snapshots[0].ordered_gap_bytes, 0);

    drop(reader);
    assert!(pressure.snapshots().is_empty());
    let _ = release_tx.send(());
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[test]
fn endpoint_recovery_silent_tcp_transport_tx_does_not_arm_generic_no_rx() {
    let started = Instant::now();
    let rtt = Duration::from_millis(171);
    let mut state = EndpointRecoveryState::default();
    let sample = |tx_bytes| EndpointRecoveryInput {
        active_tcp: 2,
        udp_active: false,
        last_udp_activity_secs: 0,
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes,
            rx_bytes: 1_000,
            rtt,
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: Vec::new(),
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &sample(10_000)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(started + Duration::from_millis(250), &sample(10_037)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.armed_at, None,
        "silent TCP ownership plus transport ACK/control TX is not application demand"
    );

    let bound = endpoint_recovery_stall_bound(rtt);
    assert_eq!(
        state.observe(
            started + bound + Duration::from_millis(250),
            &sample(10_074)
        ),
        EndpointRecoveryAction::None,
        "TCP-only transport TX without an exact writer stall must never rebind"
    );
}

#[test]
fn endpoint_recovery_rebinds_once_after_active_tx_without_rx() {
    let started = Instant::now();
    let rtt = Duration::from_millis(164);
    let mut state = EndpointRecoveryState::default();

    let sample = |tx_bytes| EndpointRecoveryInput {
        active_tcp: 0,
        udp_active: true,
        last_udp_activity_secs: 1,
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes,
            rx_bytes: 1_000,
            rtt,
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: Vec::new(),
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &sample(10_000)),
        EndpointRecoveryAction::None
    );
    let armed_at = started + Duration::from_millis(250);
    assert_eq!(
        state.observe(armed_at, &sample(20_000)),
        EndpointRecoveryAction::None
    );

    let bound = endpoint_recovery_stall_bound(rtt);
    assert_eq!(bound, Duration::from_secs(2));
    let first = state.observe(armed_at + bound, &sample(30_000));
    assert!(
        matches!(first, EndpointRecoveryAction::Rebind { generation: 1, .. }),
        "expected one endpoint rebind, got {first:?}"
    );
    assert_eq!(
        state.observe(armed_at + bound + Duration::from_secs(5), &sample(40_000)),
        EndpointRecoveryAction::None,
        "one continuous no-RX episode must never create a rebind loop"
    );
}

#[test]
fn endpoint_recovery_new_connection_clears_the_old_stall_episode() {
    let started = Instant::now();
    let mut state = EndpointRecoveryState::default();
    let sample = |stable_id, tx_bytes, rx_bytes| EndpointRecoveryInput {
        active_tcp: 0,
        udp_active: true,
        last_udp_activity_secs: 1,
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id,
            tx_bytes,
            rx_bytes,
            rtt: Duration::from_millis(100),
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: Vec::new(),
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &sample(11, 1_000, 1_000)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(
            started + Duration::from_millis(250),
            &sample(11, 2_000, 1_000),
        ),
        EndpointRecoveryAction::None
    );

    assert_eq!(
        state.observe(started + Duration::from_secs(3), &sample(12, 500, 500),),
        EndpointRecoveryAction::None,
        "a newly authenticated connection proves receive progress on the endpoint"
    );
    assert_eq!(
        state.observe(started + Duration::from_secs(6), &sample(12, 500, 500),),
        EndpointRecoveryAction::None,
        "the old connection's no-RX deadline must not survive replacement"
    );
}

#[test]
fn endpoint_recovery_requires_endpoint_wide_stall_and_rearms_on_rx() {
    let started = Instant::now();
    let mut state = EndpointRecoveryState::default();
    let sample = |active_tcp,
                  first_tx,
                  first_rx,
                  second_tx,
                  second_rx,
                  socket_rx_generation,
                  first_socket_rx_generation,
                  second_socket_rx_generation,
                  last_udp_activity_secs| {
        EndpointRecoveryInput {
            active_tcp,
            udp_active: true,
            last_udp_activity_secs,
            current_socket_rx_rebind_generation: socket_rx_generation,
            connections: vec![
                EndpointRecoveryConnectionSample {
                    stable_id: 11,
                    tx_bytes: first_tx,
                    rx_bytes: first_rx,
                    rtt: Duration::from_millis(100),
                    black_holes_detected: 0,
                    current_socket_rx_rebind_generation: first_socket_rx_generation,
                    tcp_write_pressures: Vec::new(),
                    tcp_ordered_read_progress: Vec::new(),
                },
                EndpointRecoveryConnectionSample {
                    stable_id: 12,
                    tx_bytes: second_tx,
                    rx_bytes: second_rx,
                    rtt: Duration::from_millis(150),
                    black_holes_detected: 0,
                    current_socket_rx_rebind_generation: second_socket_rx_generation,
                    tcp_write_pressures: Vec::new(),
                    tcp_ordered_read_progress: Vec::new(),
                },
            ],
        }
    };

    assert_eq!(
        state.observe(started, &sample(2, 100, 100, 100, 100, 0, 0, 0, 1)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(
            started + Duration::from_millis(250),
            &sample(2, 200, 100, 100, 100, 0, 0, 0, 1),
        ),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(
            started + Duration::from_secs(3),
            &sample(2, 300, 100, 100, 101, 0, 0, 0, 1),
        ),
        EndpointRecoveryAction::None,
        "RX on either connection proves that the shared endpoint is alive"
    );

    let second_episode = started + Duration::from_secs(4);
    assert_eq!(
        state.observe(second_episode, &sample(2, 400, 100, 100, 101, 0, 0, 0, 2),),
        EndpointRecoveryAction::None
    );
    let action = state.observe(
        second_episode + Duration::from_secs(2),
        &sample(2, 500, 100, 100, 101, 0, 0, 0, 2),
    );
    assert!(matches!(
        action,
        EndpointRecoveryAction::Rebind { generation: 1, .. }
    ));
    state.note_rebind_succeeded(second_episode + Duration::from_secs(2), 1);
    assert_eq!(
        state.observe(
            second_episode + Duration::from_millis(2_050),
            &sample(2, 500, 101, 100, 101, 0, 0, 0, 2),
        ),
        EndpointRecoveryAction::None,
        "a packet on Quinn's retained old socket must not complete rebind recovery"
    );
    assert_eq!(
        state.observe(
            second_episode + Duration::from_millis(2_100),
            &sample(2, 500, 101, 100, 101, 1, 1, 0, 2),
        ),
        EndpointRecoveryAction::None,
        "one current-socket connection cannot prove that the other pooled connection migrated"
    );
    assert_eq!(
        state.observe(
            second_episode + Duration::from_millis(2_150),
            &sample(2, 500, 101, 100, 101, 1, 1, 1, 2),
        ),
        EndpointRecoveryAction::Recovered {
            generation: 1,
            socket_generation: 1,
            recovery_time: Duration::from_millis(150),
            connection_count: 2,
        }
    );

    assert_eq!(
        endpoint_recovery_stall_bound(Duration::from_millis(1)),
        Duration::from_secs(2)
    );
    assert_eq!(
        endpoint_recovery_stall_bound(Duration::from_millis(500)),
        Duration::from_secs(4)
    );
    assert_eq!(
        endpoint_recovery_stall_bound(Duration::from_secs(2)),
        Duration::from_secs(7)
    );
}

#[test]
fn endpoint_recovery_does_not_arm_without_workload_or_tx_progress() {
    let started = Instant::now();
    let mut state = EndpointRecoveryState::default();
    let sample = |udp_active, tx_bytes| EndpointRecoveryInput {
        active_tcp: 0,
        udp_active,
        last_udp_activity_secs: u64::from(udp_active),
        current_socket_rx_rebind_generation: 0,
        connections: vec![EndpointRecoveryConnectionSample {
            stable_id: 11,
            tx_bytes,
            rx_bytes: 100,
            rtt: Duration::from_millis(100),
            black_holes_detected: 0,
            current_socket_rx_rebind_generation: 0,
            tcp_write_pressures: Vec::new(),
            tcp_ordered_read_progress: Vec::new(),
        }],
    };

    assert_eq!(
        state.observe(started, &sample(false, 100)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(started + Duration::from_secs(3), &sample(false, 200)),
        EndpointRecoveryAction::None
    );
    assert_eq!(
        state.observe(started + Duration::from_secs(6), &sample(true, 200)),
        EndpointRecoveryAction::None,
        "old background TX must not arm a later workload"
    );
    assert_eq!(
        state.observe(started + Duration::from_secs(9), &sample(true, 200)),
        EndpointRecoveryAction::None,
        "an active but transport-idle workload must not churn the socket"
    );
}

#[test]
fn address_ipv4() {
    let a = encode_address(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
    assert_eq!(a, vec![0x01, 1, 2, 3, 4, 0x01, 0xBB]); // ATYP=IPv4, port 443
}

#[test]
fn address_domain() {
    let a = encode_address(&TargetAddr::DomainPort {
        host: "ab.com".into(),
        port: 443,
    });
    assert_eq!(
        a,
        vec![0x00, 6, b'a', b'b', b'.', b'c', b'o', b'm', 0x01, 0xBB]
    );
}

#[test]
fn address_ipv6() {
    let a = encode_address(&TargetAddr::IpPort("[::1]:53".parse().unwrap()));
    assert_eq!(a[0], 0x02);
    assert_eq!(a.len(), 1 + 16 + 2);
    assert_eq!(&a[17..19], &[0x00, 0x35]); // port 53
}

#[test]
fn authenticate_layout() {
    let uuid = [0xABu8; 16];
    let token = [0xCDu8; 32];
    let c = encode_authenticate(&uuid, &token);
    assert_eq!(c.len(), 2 + 16 + 32);
    assert_eq!(&c[..2], &[0x05, 0x00]);
    assert_eq!(&c[2..18], &uuid);
    assert_eq!(&c[18..50], &token);
}

#[test]
fn connect_prefixes_header() {
    let c = encode_connect(&TargetAddr::IpPort("1.2.3.4:443".parse().unwrap()));
    assert_eq!(&c[..2], &[0x05, 0x01]);
    assert_eq!(&c[2..], &[0x01, 1, 2, 3, 4, 0x01, 0xBB]);
}

#[test]
fn domain_over_255_truncated_safely() {
    let host = "a".repeat(300);
    let a = encode_address(&TargetAddr::DomainPort { host, port: 80 });
    assert_eq!(a[0], 0x00);
    assert_eq!(a[1], 255); // length byte capped
    assert_eq!(a.len(), 1 + 1 + 255 + 2);
}

const UUID: &str = "550e8400-e29b-41d4-a716-446655440000";

#[test]
fn config_valid_with_defaults() {
    let c = TuicClientConfig::from_sources(
        Some("1.2.3.4:8443"),
        Some(UUID),
        Some("secret123"),
        None,
        None,
        None,
    )
    .expect("valid");
    assert_eq!(c.server, "1.2.3.4:8443".parse().unwrap());
    assert_eq!(c.uuid[0], 0x55);
    assert_eq!(c.alpn, "h3"); // default
    assert_eq!(c.congestion_control, "cubic"); // 刀3.5 实测裁决：datagram 路径 Cubic 优于 BBR
    assert_eq!(c.udp_relay_mode, "native");
    assert_eq!(c.gso_policy, "enabled");
    assert_eq!(c.udp_send_service, "quinn");
    assert_eq!(c.pacing_policy, "quinn");
    assert_eq!(c.tcp_pool, 2);
    assert_eq!(c.quic_stats_secs, None);
    assert!(
        !c.zero_rtt,
        "0-RTT 默认关（quinn 0.10 在 0-RTT 不支持 keying-material 导出）"
    );
}

#[test]
fn env_overrides_cc_and_udp_mode() {
    // 有值覆盖默认（A/B 切换：MINI_VPN_TUIC_CC=cubic / MINI_VPN_TUIC_UDP_MODE=quic）。
    assert_eq!(override_field("bbr".into(), Some("cubic".into())), "cubic");
    assert_eq!(override_field("native".into(), Some("quic".into())), "quic");
    // 无值 / 空白 → 保留默认。
    assert_eq!(override_field("bbr".into(), None), "bbr");
    assert_eq!(
        override_field("native".into(), Some("   ".into())),
        "native"
    );
}

#[test]
fn tcp_pool_parse_defaults_and_clamps() {
    assert_eq!(parse_tcp_pool(None), 2);
    assert_eq!(parse_tcp_pool(Some("")), 2);
    assert_eq!(parse_tcp_pool(Some("0")), 1);
    assert_eq!(parse_tcp_pool(Some("nope")), 2);
    assert_eq!(parse_tcp_pool(Some("1")), 1);
    assert_eq!(parse_tcp_pool(Some("2")), 2);
    assert_eq!(parse_tcp_pool(Some("4")), 4);
    assert_eq!(parse_tcp_pool(Some("999")), MAX_TUIC_TCP_POOL);
}

#[test]
fn quic_stats_secs_follows_tcp_diag_or_explicit_override() {
    assert_eq!(parse_quic_stats_secs(None, None, Some("5")), None);
    assert_eq!(parse_quic_stats_secs(None, Some("1"), Some("5")), Some(5));
    assert_eq!(
        parse_quic_stats_secs(None, Some("true"), Some("nope")),
        Some(DEFAULT_TUIC_QUIC_STATS_SECS)
    );
    assert_eq!(
        parse_quic_stats_secs(Some("7"), Some("0"), Some("5")),
        Some(7)
    );
    assert_eq!(parse_quic_stats_secs(Some("0"), Some("1"), Some("5")), None);
    assert_eq!(
        parse_quic_stats_secs(Some("nope"), Some("1"), Some("5")),
        None
    );
}

#[test]
fn tuic_tcp_relay_mode_defaults_to_ordered_join_and_gates_diagnostics() {
    assert_eq!(
        parse_tuic_tcp_relay_mode(None, None),
        TuicTcpRelayMode::OrderedJoin
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some(""), Some("")),
        TuicTcpRelayMode::OrderedJoin
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("0"), Some("0")),
        TuicTcpRelayMode::OrderedJoin
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("false"), Some("false")),
        TuicTcpRelayMode::OrderedJoin
    );

    assert_eq!(
        parse_tuic_tcp_relay_mode(None, Some("1")),
        TuicTcpRelayMode::OrderedChunk
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(None, Some("true")),
        TuicTcpRelayMode::OrderedChunk
    );

    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("1"), None),
        TuicTcpRelayMode::UnorderedReassembly
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("true"), None),
        TuicTcpRelayMode::UnorderedReassembly
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("on"), None),
        TuicTcpRelayMode::UnorderedReassembly
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("yes"), None),
        TuicTcpRelayMode::UnorderedReassembly
    );
    assert_eq!(
        parse_tuic_tcp_relay_mode(Some("1"), Some("1")),
        TuicTcpRelayMode::UnorderedReassembly
    );
    assert_eq!(
        TuicNativeOverride::NativeChunkPump.as_str(),
        "native_chunk_pump_diag"
    );
    assert_eq!(
        TuicNativeOverride::NativeOrderedPump.as_str(),
        "native_ordered_pump_diag"
    );
}

#[test]
fn d16_read_reservation_counts_before_remote_read() {
    let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
        512 * 1024,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    assert_eq!(queue.blocking_snapshot_for_test().reserved_bytes, 0);
}

struct PendingOnceOrderedSource {
    calls: Arc<AtomicU64>,
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    payload: bytes::Bytes,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for PendingOnceOrderedSource {
    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            self.entered.notify_one();
            self.release.notified().await;
            return Ok(Some(NativeTcpChunk {
                offset: 0,
                bytes: self.payload.clone(),
            }));
        }
        std::future::pending::<io::Result<Option<NativeTcpChunk>>>().await
    }
}

struct SequenceOrderedSource {
    calls: Arc<AtomicU64>,
    chunks: Vec<NativeTcpChunk>,
}

#[async_trait::async_trait]
impl TuicOrderedNativeChunkSource for SequenceOrderedSource {
    async fn read_ordered_native_chunk(
        &mut self,
        _max_len: usize,
    ) -> io::Result<Option<NativeTcpChunk>> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) as usize;
        if call < self.chunks.len() {
            Ok(Some(self.chunks[call].clone()))
        } else {
            Ok(None)
        }
    }
}

#[tokio::test]
async fn native_ordered_pump_holds_pending_read_until_ready() {
    let calls = Arc::new(AtomicU64::new(0));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let source = PendingOnceOrderedSource {
        calls: calls.clone(),
        entered: entered.clone(),
        release: release.clone(),
        payload: bytes::Bytes::from_static(b"ordered-pump"),
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let mut reader = TuicNativeOrderedPumpReader::spawn_from_source(source, lease, None, None);

    tokio::time::timeout(Duration::from_millis(200), entered.notified())
        .await
        .expect("read pump should enter the first source read");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "the pump must hold the pending read future instead of starting another read"
    );

    release.notify_one();
    let chunk = tokio::time::timeout(Duration::from_millis(200), async {
        std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 4096)).await
    })
    .await
    .expect("released ordered read should reach the native reader")
    .expect("pump read should succeed")
    .expect("released read should produce a chunk");
    assert_eq!(chunk.offset, 0);
    assert_eq!(&chunk.bytes[..], b"ordered-pump");

    drop(reader);
    assert_eq!(
        active.load(Ordering::SeqCst),
        0,
        "dropping the ordered pump reader must release its TCP pool lease"
    );
}

#[tokio::test]
async fn native_ordered_pump_reader_batches_ready_chunks_to_max_len() {
    let calls = Arc::new(AtomicU64::new(0));
    let source = SequenceOrderedSource {
        calls: calls.clone(),
        chunks: vec![
            NativeTcpChunk {
                offset: 0,
                bytes: bytes::Bytes::from_static(b"ab"),
            },
            NativeTcpChunk {
                offset: 2,
                bytes: bytes::Bytes::from_static(b"cdef"),
            },
            NativeTcpChunk {
                offset: 6,
                bytes: bytes::Bytes::from_static(b"gh"),
            },
        ],
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let mut reader = TuicNativeOrderedPumpReader::spawn_from_source(source, lease, None, None);

    tokio::time::timeout(Duration::from_millis(200), async {
        while calls.load(Ordering::SeqCst) < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("source should publish all ready chunks and EOF");

    let first = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 5))
        .await
        .expect("first batched read should succeed")
        .expect("first batched read should produce data");
    assert_eq!(first.offset, 0);
    assert_eq!(&first.bytes[..], b"abcde");

    let second = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
        .await
        .expect("second batched read should succeed")
        .expect("second batched read should produce stashed remainder");
    assert_eq!(second.offset, 5);
    assert_eq!(&second.bytes[..], b"fgh");

    let eof = std::future::poll_fn(|cx| reader.poll_read_chunk(cx, 16))
        .await
        .expect("EOF read should succeed");
    assert!(eof.is_none(), "EOF must be delivered after stashed bytes");

    drop(reader);
    assert_eq!(active.load(Ordering::SeqCst), 0);
}

struct RecordingDirectOrderedRecv {
    max_lens: Arc<std::sync::Mutex<Vec<usize>>>,
    chunk: Option<NativeTcpChunk>,
}

impl DirectOrderedNativeChunkRecv for RecordingDirectOrderedRecv {
    fn poll_read_ordered_native_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        self.max_lens.lock().unwrap().push(max_len);
        Poll::Ready(Ok(self.chunk.take()))
    }
}

struct PendingDirectOrderedRecv {
    starts: Arc<AtomicU64>,
    released: Arc<std::sync::atomic::AtomicBool>,
    in_flight: bool,
}

impl DirectOrderedNativeChunkRecv for PendingDirectOrderedRecv {
    fn poll_read_ordered_native_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        _max_len: usize,
    ) -> Poll<io::Result<Option<NativeTcpChunk>>> {
        if !self.in_flight {
            self.in_flight = true;
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
        if !self.released.load(Ordering::SeqCst) {
            return Poll::Pending;
        }
        self.in_flight = false;
        Poll::Ready(Ok(Some(NativeTcpChunk {
            offset: 0,
            bytes: bytes::Bytes::from_static(b"ready"),
        })))
    }
}

#[test]
fn d16_direct_ordered_reader_forwards_exact_max_len() {
    let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recv = RecordingDirectOrderedRecv {
        max_lens: max_lens.clone(),
        chunk: Some(NativeTcpChunk {
            offset: 0,
            bytes: bytes::Bytes::from_static(b"direct"),
        }),
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active.clone());
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let Poll::Ready(chunk) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
        panic!("recording direct reader should be ready");
    };
    let chunk = chunk
        .expect("recording direct reader should succeed")
        .expect("recording direct reader should return data");
    assert_eq!(&chunk.bytes[..], b"direct");
    assert_eq!(&*max_lens.lock().unwrap(), &[128 * 1024]);
}

#[test]
fn h10d16_single_gate_selects_direct_ordered_native_relay() {
    assert_eq!(
        select_tuic_native_relay_mode(true, false, false),
        Some(TuicNativeOverride::D16DirectOrdered)
    );
    assert_eq!(
        select_tuic_native_relay_mode(true, true, true),
        Some(TuicNativeOverride::D16DirectOrdered),
        "the approved D16 gate must not depend on or lose to older diagnostic flags"
    );
    assert_eq!(select_tuic_native_relay_mode(false, false, false), None);
}

#[test]
fn d16_direct_ordered_reader_never_returns_more_than_max_len() {
    let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recv = RecordingDirectOrderedRecv {
        max_lens: max_lens.clone(),
        chunk: Some(NativeTcpChunk {
            offset: 0,
            bytes: bytes::Bytes::from(vec![1; 16 * 1024 + 1]),
        }),
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let Poll::Ready(result) = reader.poll_read_chunk(&mut cx, 16 * 1024) else {
        panic!("oversized direct chunk should be rejected immediately");
    };
    let err = result.expect_err("direct reader must reject bytes above max_len");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert_eq!(&*max_lens.lock().unwrap(), &[16 * 1024]);
}

#[tokio::test]
async fn d16_direct_ordered_reader_keeps_one_pending_read_future() {
    let starts = Arc::new(AtomicU64::new(0));
    let released = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let recv = PendingDirectOrderedRecv {
        starts: starts.clone(),
        released: released.clone(),
        in_flight: false,
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(reader.poll_read_chunk(&mut cx, 128 * 1024).is_pending());
    assert!(reader.poll_read_chunk(&mut cx, 128 * 1024).is_pending());
    assert_eq!(
        starts.load(Ordering::SeqCst),
        1,
        "re-polling must not start a second concurrent ordered read"
    );

    released.store(true, Ordering::SeqCst);
    let Poll::Ready(chunk) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
        panic!("released direct read should complete");
    };
    assert_eq!(&chunk.unwrap().unwrap().bytes[..], b"ready");
    assert_eq!(starts.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn d16_real_quinn_reader_wakes_after_delayed_ordered_payload() {
    let cert_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("certs/dev");
    let mut server_cert_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-cert.pem")).unwrap());
    let server_certs = rustls_pemfile::certs(&mut server_cert_reader)
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut server_key_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("server-key.pem")).unwrap());
    let server_key = rustls_pemfile::private_key(&mut server_key_reader)
        .unwrap()
        .unwrap();
    let server_config = quinn::ServerConfig::with_single_cert(server_certs, server_key).unwrap();
    let server_endpoint = quinn::Endpoint::server(
        server_config,
        "127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap(),
    )
    .unwrap();
    let server_addr = server_endpoint.local_addr().unwrap();

    let mut ca_reader =
        std::io::BufReader::new(std::fs::File::open(cert_dir.join("ca-cert.pem")).unwrap());
    let mut roots = rustls::RootCertStore::empty();
    for cert in rustls_pemfile::certs(&mut ca_reader) {
        roots.add(cert.unwrap()).unwrap();
    }
    let mut client_endpoint =
        quinn::Endpoint::client("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap()).unwrap();
    client_endpoint.set_default_client_config(
        quinn::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap(),
    );

    let reader_polled = Arc::new(tokio::sync::Notify::new());
    let server_reader_polled = reader_polled.clone();
    let (client_read_tx, client_read_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let mut marker = [0u8; 1];
        recv.read_exact(&mut marker).await.unwrap();
        assert_eq!(marker, [0x5a]);
        server_reader_polled.notified().await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        send.write_all(b"delayed-quinn-payload").await.unwrap();
        send.finish().unwrap();
        let _ = client_read_rx.await;
    });

    let connection = client_endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();
    let (mut send, recv) = connection.open_bi().await.unwrap();
    send.write_all(&[0x5a]).await.unwrap();

    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::new(recv, lease, None, connection, None);
    let mut first_poll = true;
    let read = tokio::time::timeout(
        Duration::from_secs(1),
        std::future::poll_fn(|cx| {
            let poll = reader.poll_read_chunk(cx, 128 * 1024);
            if first_poll {
                first_poll = false;
                assert!(poll.is_pending(), "server has not released payload yet");
                reader_polled.notify_one();
            }
            poll
        }),
    )
    .await
    .expect("real Quinn readability must wake the pending D16 reader")
    .unwrap()
    .unwrap();

    assert_eq!(&read.bytes[..], b"delayed-quinn-payload");
    let _ = client_read_tx.send(());
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[tokio::test]
async fn d16_real_quinn_reader_sustains_ordered_progress() {
    let _capacity_guard = crate::test_support::local_capacity_test_guard().await;
    const PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
    const WRITE_BYTES: usize = 64 * 1024;

    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();

    let (client_read_tx, client_read_rx) = tokio::sync::oneshot::channel();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let (mut send, mut recv) = connection.accept_bi().await.unwrap();
        let mut marker = [0u8; 1];
        recv.read_exact(&mut marker).await.unwrap();
        assert_eq!(marker, [0x5b]);
        let payload = vec![0x6d; WRITE_BYTES];
        for _ in 0..PAYLOAD_BYTES / WRITE_BYTES {
            send.write_all(&payload).await.unwrap();
        }
        send.finish().unwrap();
        let _ = client_read_rx.await;
    });

    let connection = client_endpoint
        .connect(server_addr, "example.com")
        .unwrap()
        .await
        .unwrap();
    let (mut send, recv) = connection.open_bi().await.unwrap();
    send.write_all(&[0x5b]).await.unwrap();

    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::new(recv, lease, None, connection, None);
    let started = Instant::now();
    let mut last_progress = started;
    let mut max_progress_gap = Duration::ZERO;
    let mut received = 0usize;
    let mut reads = 0u64;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let chunk = std::future::poll_fn(|cx| {
                reader.poll_read_chunk(cx, TUIC_TCP_DIRECT_ORDERED_READ_MAX_BYTES)
            })
            .await
            .unwrap();
            let Some(chunk) = chunk else {
                break;
            };
            let now = Instant::now();
            max_progress_gap = max_progress_gap.max(now.saturating_duration_since(last_progress));
            last_progress = now;
            received = received.saturating_add(chunk.bytes.len());
            reads = reads.saturating_add(1);
        }
    })
    .await
    .expect("the direct ordered Quinn reader must not develop a multi-second service gap");
    let elapsed = started.elapsed();
    let receiver_mbps = received as f64 * 8.0 / elapsed.as_secs_f64() / 1_000_000.0;

    assert_eq!(received, PAYLOAD_BYTES);
    assert!(
        max_progress_gap < Duration::from_millis(250),
        "same-stream progress gap must stay sub-250ms: gap={max_progress_gap:?} reads={reads} rate={receiver_mbps:.1}M"
    );
    assert!(
        receiver_mbps >= 170.0,
        "same-stream direct reader must have plausible Gate B capacity: reads={reads} elapsed={elapsed:?} rate={receiver_mbps:.1}M"
    );
    let average_read_bytes = received as u64 / reads.max(1);
    assert!(
        average_read_bytes <= 4 * 1024,
        "the D16 reader must hand off cancellation-safe Quinn chunks without retaining a large application read buffer: reads={reads} average_read_bytes={average_read_bytes} rate={receiver_mbps:.1}M"
    );

    let _ = client_read_tx.send(());
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[test]
fn d16_direct_ordered_reader_rejects_offset_discontinuity() {
    let max_lens = Arc::new(std::sync::Mutex::new(Vec::new()));
    let recv = RecordingDirectOrderedRecv {
        max_lens,
        chunk: Some(NativeTcpChunk {
            offset: 7,
            bytes: bytes::Bytes::from_static(b"gap"),
        }),
    };
    let active = Arc::new(AtomicU64::new(0));
    let (lease, reserved) = TcpPoolSlotLease::reserve(active);
    assert!(reserved);
    let mut reader = TuicNativeOrderedReader::from_recv(recv, lease, None, None, None);
    let waker = std::task::Waker::noop();
    let mut cx = Context::from_waker(waker);

    let Poll::Ready(result) = reader.poll_read_chunk(&mut cx, 128 * 1024) else {
        panic!("offset discontinuity should be rejected immediately");
    };
    let err = result.expect_err("direct ordered reader must reject a gap");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    assert!(err.to_string().contains("expected=0 actual=7"), "{err}");
}

#[test]
fn native_ordered_chunk_batch_combines_contiguous_bytes_and_advances_offset() {
    let mut next_offset = 41;
    let first = combine_ordered_native_chunks(
        &mut next_offset,
        &[
            bytes::Bytes::from_static(b"ab"),
            bytes::Bytes::from_static(b"cde"),
        ],
    )
    .expect("nonempty chunk batch should combine");
    assert_eq!(first.offset, 41);
    assert_eq!(&first.bytes[..], b"abcde");
    assert_eq!(next_offset, 46);

    let second =
        combine_ordered_native_chunks(&mut next_offset, &[bytes::Bytes::from_static(b"fg")])
            .expect("single chunk batch should pass through");
    assert_eq!(second.offset, 46);
    assert_eq!(&second.bytes[..], b"fg");
    assert_eq!(next_offset, 48);

    assert!(combine_ordered_native_chunks(&mut next_offset, &[]).is_none());
    assert_eq!(next_offset, 48);
}

#[test]
fn format_quic_stats_line_includes_flow_and_congestion_signals() {
    let line = format_quic_stats_line(
        2,
        99,
        QuicStatsSnapshot {
            rtt_ms: 181,
            cwnd: 65_535,
            lost_packets: 3,
            sent_packets: 100,
            lost_bytes: 4096,
            congestion_events: 2,
            sent_plpmtud_probes: 19,
            lost_plpmtud_probes: 20,
            black_holes_detected: 21,
            tx_ack_frames: 22,
            gap_ack_reinforcements: 26,
            rx_ack_frames: 23,
            rx_stream_frames: 24,
            tx_data_blocked: 4,
            tx_stream_data_blocked: 5,
            tx_streams_blocked_bidi: 6,
            tx_streams_blocked_uni: 7,
            tx_max_data: 8,
            tx_max_stream_data: 9,
            rx_data_blocked: 10,
            rx_stream_data_blocked: 11,
            rx_max_data: 12,
            rx_max_stream_data: 13,
            udp_tx_datagrams: 14,
            udp_tx_bytes: 15,
            udp_rx_datagrams: 16,
            udp_rx_bytes: 17,
            datagram_max: Some(1375),
            datagram_send_buffer_space: 18,
            pacing_uncapped_capacity_bytes: 307_200,
            pacing_capacity_bytes: 76_800,
            pacing_tokens_bytes: 65_536,
            current_mtu: 1_375,
            pacing_mtu: 1_200,
            pacing_cap_active: true,
            pacing_delay_events: 25,
        },
    );
    assert!(line.contains("conn=2"), "{line}");
    assert!(line.contains("id=99"), "{line}");
    assert!(line.contains("rtt=181ms"), "{line}");
    assert!(line.contains("cwnd=65535"), "{line}");
    assert!(line.contains("lost=3/100"), "{line}");
    assert!(line.contains("congestion_events=2"), "{line}");
    assert!(
        line.contains("plpmtud(sent=19,lost=20,black_holes=21)"),
        "{line}"
    );
    assert!(line.contains("gap_ack_reinforcements=26"), "{line}");
    assert!(
        line.contains("frames(rx_stream=24,rx_ack=23,tx_ack=22,gap_ack_reinforcements=26)"),
        "{line}"
    );
    assert!(line.contains("tx_blocked(data=4,stream=5"), "{line}");
    assert!(line.contains("rx_blocked(data=10,stream=11"), "{line}");
    assert!(
        line.contains("rx_window(max_data=12,max_stream_data=13"),
        "{line}"
    );
    assert!(line.contains("dg_max=Some(1375)"), "{line}");
    assert!(line.contains("dg_space=18B"), "{line}");
    assert!(line.contains("current_mtu=1375"), "{line}");
    assert!(line.contains("pacing_mtu=1200"), "{line}");
    assert!(
            line.contains(
                "pacing(uncapped=307200,capacity=76800,tokens=65536,current_mtu=1375,pacing_mtu=1200,cap_active=true,delay_events=25)"
            ),
            "{line}"
        );
}

#[test]
fn format_udp_send_service_stats_line_includes_actual_service_timing() {
    let line = format_udp_send_service_stats_line(quic::QuicUdpSendServiceSnapshot {
        accepted_bytes: 512_000,
        accepted_datagrams: 400,
        accepted_bytes_per_sec: 25_600_000,
        accepted_datagrams_per_sec: 20_000,
        service_elapsed_ns: 20_000_000,
        gate_would_block: 3,
        batch_closes: 9,
        timer_blocked_polls: 10,
        cooldown_rearms: 8,
        total_rearm_lateness_ns: 4_000_000,
        max_rearm_lateness_ns: 1_500_000,
        wasted_datagrams: 7,
        inner_would_block: 2,
        inner_errors: 1,
        invalid_transmits: 0,
    });

    assert!(
        line.contains("service_rate=20000dg/s/25600000B/s"),
        "{line}"
    );
    assert!(line.contains("mean_payload=1280B"), "{line}");
    assert!(line.contains("batch_closes=9"), "{line}");
    assert!(line.contains("timer_blocked_polls=10"), "{line}");
    assert!(line.contains("mean_rearm_period_us=2500.000"), "{line}");
    assert!(
        line.contains("rearm_lateness_us(total=4000.000,mean=500.000,max=1500.000)"),
        "{line}"
    );
}

#[test]
fn format_tuic_tcp_pool_selection_line_identifies_forward_qualification_policy() {
    let line = format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
        conn_index: 1,
        stable_id: 42,
        active_before: 62,
        path_service: TcpPoolPathService::known(23_842, Duration::from_millis(163)),
        path_service_tiebreak: false,
        service_normalized_ordering: true,
        qualification: TcpPoolForwardQualification::Qualified,
        qualification_anchor: Some(0),
        black_holes_current: Some(0),
        qualification_override: true,
        all_degraded_fallback: false,
        candidates: &[TcpPoolAdmissionCandidate {
            index: 1,
            identity: Some(TcpPoolTransportIdentity::new(42, 7)),
            active_before: 62,
            path_service: TcpPoolPathService::known(23_842, Duration::from_millis(163)),
            qualification: TcpPoolForwardQualification::Qualified,
            qualification_anchor: Some(0),
            black_holes_current: Some(0),
            successor_service_readiness: TcpPoolSuccessorServiceReadiness::Ready,
            successor_service_current_path_generation: Some(5),
            successor_service_proved_path_generation: Some(5),
            successor_service_proved_cwnd_floor: Some(23_000),
            admitted: true,
        }],
        generation: 7,
        last_success_age_secs: Some(15),
        probe_result: "alive",
        reconnect_reason: Some("previous_open_failure"),
        replacement_installed: true,
    });

    assert!(line.contains("tuic-tcp-pool-selection"), "{line}");
    assert!(line.contains("conn=1 id=42"), "{line}");
    assert!(
            line.contains(
                "policy=busy_epoch_forward_qualification_then_successor_service_certificate_then_service_normalized_load_then_least_active_then_path_service"
            ),
            "{line}"
        );
    assert!(line.contains("active_before=62"), "{line}");
    assert!(line.contains("path_cwnd=23842"), "{line}");
    assert!(line.contains("path_rtt_us=163000"), "{line}");
    assert!(line.contains("service_normalized_ordering=true"), "{line}");
    assert!(line.contains("path_service_tiebreak=false"), "{line}");
    assert!(line.contains("qualification=qualified"), "{line}");
    assert!(line.contains("black_hole_anchor=0"), "{line}");
    assert!(line.contains("black_holes_current=0"), "{line}");
    assert!(line.contains("qualification_override=true"), "{line}");
    assert!(line.contains("all_degraded_fallback=false"), "{line}");
    assert!(
            line.contains("candidates=[conn1:id=42@7,active=62,qualification=qualified,anchor=0,current=0,service_certificate=ready,service_current_path_generation=5,service_proved_path_generation=5,service_proved_cwnd_floor=23000,admitted=true,cwnd=23842,rtt_us=163000]"),
            "{line}"
        );
    assert!(line.contains("generation=7"), "{line}");
    assert!(line.contains("replacement_installed=true"), "{line}");
    assert!(line.contains("last_success_age_secs=15"), "{line}");
    assert!(line.contains("probe_result=alive"), "{line}");
    assert!(
        line.contains("reconnect_reason=previous_open_failure"),
        "{line}"
    );
}

#[test]
fn format_tuic_tcp_open_line_includes_target_pool_and_id() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let line = format_tuic_tcp_open_line(&target, 3, 42, 8, 2, "ordered_join");

    assert!(line.contains("tuic-open-tcp"), "{line}");
    assert!(line.contains("target=1.2.3.4:5201"), "{line}");
    assert!(line.contains("conn=3"), "{line}");
    assert!(line.contains("id=42"), "{line}");
    assert!(line.contains("stream=8"), "{line}");
    assert!(line.contains("relay_mode=ordered_join"), "{line}");
    assert!(line.contains("startup_auth_attempts=2"), "{line}");
    assert!(!line.contains("handle="), "{line}");
    assert!(!line.contains("epoch="), "{line}");

    let diagnostic = format_tuic_tcp_open_line(&target, 3, 42, 8, 2, "ordered_chunk");
    assert!(
        diagnostic.contains("relay_mode=ordered_chunk"),
        "{diagnostic}"
    );

    let bridge = TcpRelayOpenDiag {
        handle: "SocketHandle(7)".to_string(),
        epoch: 41,
    };
    let bridged =
        format_tuic_tcp_open_line_with_diag(&target, 3, 42, 8, 2, "ordered_join", Some(&bridge));
    assert!(bridged.contains("handle=SocketHandle(7)"), "{bridged}");
    assert!(bridged.contains("epoch=41"), "{bridged}");
}

#[tokio::test]
async fn format_tuic_tcp_open_line_reads_task_local_relay_bridge() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let bridge = TcpRelayOpenDiag {
        handle: "SocketHandle(9)".to_string(),
        epoch: 17,
    };
    let line = crate::upstream::with_tcp_relay_open_diag(bridge, async {
        format_tuic_tcp_open_line(&target, 3, 42, 8, 2, "ordered_join")
    })
    .await;

    assert!(line.contains("handle=SocketHandle(9)"), "{line}");
    assert!(line.contains("epoch=17"), "{line}");
}

#[test]
fn format_tuic_tcp_stream_diag_lines_include_first_rx_and_gaps() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let first = format_tuic_tcp_stream_first_rx_line(&meta, 20_500, 35_244, 1);
    let gap = format_tuic_tcp_stream_read_gap_line(&meta, 15_000, 39_884, 2, 75_128);
    let pending = format_tuic_tcp_stream_pending_line(
        &meta,
        12_000,
        24,
        31,
        5_000,
        75_128,
        2,
        TuicTcpStreamPendingCause::NoTransportSample,
        None,
        5,
        4,
    );
    let close_snapshot = TuicTcpStreamCloseSnapshot {
        first_rx_ms: 20_500,
        max_read_gap_ms: 15_000,
        rx_bytes: 109_304,
        reads: 3,
        pending_polls: 24,
        max_pending_gap_ms: 12_000,
        polls: 31,
        max_poll_gap_ms: 5_000,
        self_wake_armed: 5,
        self_wake_fired: 4,
    };
    let close = format_tuic_tcp_stream_close_line(&meta, &close_snapshot);

    assert!(first.contains("tuic-tcp-stream-first-rx"), "{first}");
    assert!(first.contains("target=1.2.3.4:5201"), "{first}");
    assert!(first.contains("conn=3"), "{first}");
    assert!(first.contains("id=42"), "{first}");
    assert!(first.contains("stream=8"), "{first}");
    assert!(first.contains("first_rx_ms=20500"), "{first}");
    assert!(first.contains("read_bytes=35244"), "{first}");
    assert!(first.contains("reads=1"), "{first}");

    assert!(gap.contains("tuic-tcp-stream-read-gap"), "{gap}");
    assert!(gap.contains("gap_ms=15000"), "{gap}");
    assert!(gap.contains("read_bytes=39884"), "{gap}");
    assert!(gap.contains("rx_bytes=75128"), "{gap}");

    assert!(pending.contains("tuic-tcp-stream-pending"), "{pending}");
    assert!(pending.contains("pending_gap_ms=12000"), "{pending}");
    assert!(pending.contains("pending_polls=24"), "{pending}");
    assert!(pending.contains("polls=31"), "{pending}");
    assert!(pending.contains("max_poll_gap_ms=5000"), "{pending}");
    assert!(pending.contains("rx_bytes=75128"), "{pending}");
    assert!(pending.contains("reads=2"), "{pending}");
    assert!(pending.contains("self_wake_armed=5"), "{pending}");
    assert!(pending.contains("self_wake_fired=4"), "{pending}");

    assert!(close.contains("tuic-tcp-stream-close"), "{close}");
    assert!(close.contains("first_rx_ms=20500"), "{close}");
    assert!(close.contains("max_read_gap_ms=15000"), "{close}");
    assert!(close.contains("rx_bytes=109304"), "{close}");
    assert!(close.contains("reads=3"), "{close}");
    assert!(close.contains("pending_polls=24"), "{close}");
    assert!(close.contains("max_pending_gap_ms=12000"), "{close}");
    assert!(close.contains("polls=31"), "{close}");
    assert!(close.contains("max_poll_gap_ms=5000"), "{close}");
    assert!(close.contains("self_wake_armed=5"), "{close}");
    assert!(close.contains("self_wake_fired=4"), "{close}");
}

#[test]
fn format_tuic_tcp_stream_pending_line_includes_transport_delivery_counters() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let transport = TuicStreamTransportPending {
        sample: TuicStreamTransportSample {
            udp_rx_datagrams: 101,
            udp_rx_bytes: 200_000,
            rx_stream_frames: 37,
            rx_ack_frames: 41,
            tx_ack_frames: 43,
            sent_plpmtud_probes: 5,
            lost_plpmtud_probes: 2,
            black_holes_detected: 1,
        },
        since_last_read: TuicStreamTransportDelta {
            udp_rx_datagrams: 11,
            udp_rx_bytes: 35_000,
            rx_stream_frames: 7,
        },
        since_last_pending: TuicStreamTransportDelta {
            udp_rx_datagrams: 3,
            udp_rx_bytes: 9_000,
            rx_stream_frames: 2,
        },
    };
    let pending = format_tuic_tcp_stream_pending_line(
        &meta,
        12_000,
        24,
        31,
        250,
        75_128,
        2,
        TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
        Some(transport),
        13,
        12,
    );

    assert!(
        pending.contains("pending_cause=connection_fresh_stream_frames_pending"),
        "{pending}"
    );
    assert!(pending.contains("conn_udp_rx=101/200000B"), "{pending}");
    assert!(
        pending.contains("conn_udp_rx_since_read=11/35000B"),
        "{pending}"
    );
    assert!(
        pending.contains("conn_udp_rx_since_pending=3/9000B"),
        "{pending}"
    );
    assert!(pending.contains("conn_rx_stream_frames=37"), "{pending}");
    assert!(pending.contains("self_wake_armed=13"), "{pending}");
    assert!(pending.contains("self_wake_fired=12"), "{pending}");
    assert!(
        pending.contains("conn_rx_stream_frames_since_read=7"),
        "{pending}"
    );
    assert!(
        pending.contains("conn_rx_stream_frames_since_pending=2"),
        "{pending}"
    );
    assert!(
        pending.contains("conn_ack_frames(rx=41,tx=43)"),
        "{pending}"
    );
    assert!(
        pending.contains("conn_plpmtud(sent=5,lost=2,black_holes=1)"),
        "{pending}"
    );
}

#[test]
fn format_tuic_tcp_unordered_staging_line_includes_reassembly_pressure() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);

    let line = format_tuic_tcp_unordered_staging_line(
        &meta,
        "chunk",
        1024,
        4096,
        1200,
        3600,
        3,
        4800,
        7,
        8400,
        2,
        4096,
        3072,
        1,
        4 * 1024 * 1024,
    );

    assert!(line.contains("tuic-tcp-unordered-staging"), "{line}");
    assert!(line.contains("target=1.2.3.4:5201"), "{line}");
    assert!(line.contains("conn=3"), "{line}");
    assert!(line.contains("id=42"), "{line}");
    assert!(line.contains("stream=8"), "{line}");
    assert!(line.contains("reason=chunk"), "{line}");
    assert!(line.contains("next_offset=1024"), "{line}");
    assert!(line.contains("chunk_offset=4096"), "{line}");
    assert!(line.contains("chunk_bytes=1200"), "{line}");
    assert!(line.contains("gap_bytes=3072"), "{line}");
    assert!(line.contains("max_gap_bytes=4096"), "{line}");
    assert!(line.contains("buffered=3600B"), "{line}");
    assert!(line.contains("buffered_chunks=3"), "{line}");
    assert!(line.contains("max_buffered=4800B"), "{line}");
    assert!(line.contains("unordered_chunks=7"), "{line}");
    assert!(line.contains("unordered_bytes=8400"), "{line}");
    assert!(line.contains("out_of_order_chunks=2"), "{line}");
    assert!(line.contains("cap_hits=1"), "{line}");
    assert!(line.contains("cap=4194304B"), "{line}");
}

#[test]
fn tuic_tcp_stream_pending_cause_classifies_transport_freshness() {
    assert_eq!(
        classify_tuic_stream_pending_cause(None),
        TuicTcpStreamPendingCause::NoTransportSample
    );

    let no_connection_rx = TuicStreamTransportPending {
        sample: TuicStreamTransportSample {
            udp_rx_datagrams: 100,
            udp_rx_bytes: 200_000,
            rx_stream_frames: 30,
            rx_ack_frames: 40,
            tx_ack_frames: 50,
            sent_plpmtud_probes: 0,
            lost_plpmtud_probes: 0,
            black_holes_detected: 0,
        },
        since_last_read: TuicStreamTransportDelta::default(),
        since_last_pending: TuicStreamTransportDelta::default(),
    };
    assert_eq!(
        classify_tuic_stream_pending_cause(Some(no_connection_rx)),
        TuicTcpStreamPendingCause::NoConnectionRx
    );

    let connection_rx_no_stream_frames = TuicStreamTransportPending {
        since_last_read: TuicStreamTransportDelta {
            udp_rx_datagrams: 12,
            udp_rx_bytes: 14_000,
            rx_stream_frames: 0,
        },
        ..no_connection_rx
    };
    assert_eq!(
        classify_tuic_stream_pending_cause(Some(connection_rx_no_stream_frames)),
        TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames
    );

    let fresh_stream_frames_pending = TuicStreamTransportPending {
        since_last_read: TuicStreamTransportDelta {
            udp_rx_datagrams: 12,
            udp_rx_bytes: 14_000,
            rx_stream_frames: 3,
        },
        since_last_pending: TuicStreamTransportDelta {
            udp_rx_datagrams: 2,
            udp_rx_bytes: 4_000,
            rx_stream_frames: 1,
        },
        ..no_connection_rx
    };
    assert_eq!(
        classify_tuic_stream_pending_cause(Some(fresh_stream_frames_pending)),
        TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending
    );

    let stale_stream_frames_pending = TuicStreamTransportPending {
        since_last_read: TuicStreamTransportDelta {
            udp_rx_datagrams: 12,
            udp_rx_bytes: 14_000,
            rx_stream_frames: 3,
        },
        since_last_pending: TuicStreamTransportDelta {
            udp_rx_datagrams: 2,
            udp_rx_bytes: 4_000,
            rx_stream_frames: 0,
        },
        ..no_connection_rx
    };
    assert_eq!(
        classify_tuic_stream_pending_cause(Some(stale_stream_frames_pending)),
        TuicTcpStreamPendingCause::ConnectionStaleStreamFramesPending
    );
}

#[test]
fn tuic_stream_pending_self_wake_arms_for_active_connection_rx_after_deadline() {
    let now = Instant::now();
    let future = now + Duration::from_millis(10);
    let past = now - Duration::from_millis(1);

    assert!(should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
        None,
        now
    ));
    assert!(!should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
        Some(future),
        now
    ));
    assert!(should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionFreshStreamFramesPending,
        Some(past),
        now
    ));
    assert!(should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
        None,
        now
    ));
    assert!(!should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
        Some(future),
        now
    ));
    assert!(should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
        Some(past),
        now
    ));
    assert!(!should_arm_tuic_stream_pending_self_wake(
        TuicTcpStreamPendingCause::NoConnectionRx,
        Some(past),
        now
    ));
}

#[test]
fn tuic_stream_pending_self_wake_services_active_connection_rx() {
    let now = Instant::now();

    assert!(
        should_arm_tuic_stream_pending_self_wake(
            TuicTcpStreamPendingCause::ConnectionRxNoStreamFrames,
            None,
            now,
        ),
        "active QUIC receive without yet-deliverable stream frames should still wake pending reads"
    );
}

#[test]
fn tuic_tcp_stream_diag_tracks_first_rx_and_read_gaps() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let start = std::time::Instant::now();
    let mut diag = TuicTcpStreamDiag::new(meta, start);

    diag.note_poll_at(start + std::time::Duration::from_millis(20_500));
    let first = diag.note_read_at(
        35_244,
        start + std::time::Duration::from_millis(20_500),
        None,
    );
    assert_eq!(first.first_rx_ms, Some(20_500));
    assert_eq!(first.gap_ms, None);
    assert_eq!(first.rx_bytes, 35_244);
    assert_eq!(first.reads, 1);

    diag.note_poll_at(start + std::time::Duration::from_millis(35_500));
    let second = diag.note_read_at(
        39_884,
        start + std::time::Duration::from_millis(35_500),
        None,
    );
    assert_eq!(second.first_rx_ms, None);
    assert_eq!(second.gap_ms, Some(15_000));
    assert_eq!(second.rx_bytes, 75_128);
    assert_eq!(second.reads, 2);

    let close = diag.close_snapshot();
    assert_eq!(close.first_rx_ms, 20_500);
    assert_eq!(close.max_read_gap_ms, 15_000);
    assert_eq!(close.rx_bytes, 75_128);
    assert_eq!(close.reads, 2);
    assert_eq!(close.polls, 2);
    assert_eq!(close.max_poll_gap_ms, 15_000);
}

#[test]
fn tuic_tcp_stream_diag_tracks_rate_limited_pending_gaps() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let start = std::time::Instant::now();
    let mut diag = TuicTcpStreamDiag::new(meta, start);

    diag.note_poll_at(start + std::time::Duration::from_millis(999));
    assert_eq!(
        diag.note_pending_at(start + std::time::Duration::from_millis(999), None),
        None
    );
    diag.note_poll_at(start + std::time::Duration::from_millis(1_000));
    let first = diag
        .note_pending_at(start + std::time::Duration::from_millis(1_000), None)
        .expect("first threshold-crossing pending poll logs");
    assert_eq!(first.pending_gap_ms, 1_000);
    assert_eq!(first.pending_polls, 2);
    assert_eq!(first.polls, 2);
    assert_eq!(first.max_poll_gap_ms, 1);
    assert_eq!(first.rx_bytes, 0);
    assert_eq!(first.reads, 0);
    diag.note_poll_at(start + std::time::Duration::from_millis(1_500));
    assert_eq!(
        diag.note_pending_at(start + std::time::Duration::from_millis(1_500), None),
        None,
        "pending logs are rate limited"
    );
    diag.note_poll_at(start + std::time::Duration::from_millis(2_100));
    let second = diag
        .note_pending_at(start + std::time::Duration::from_millis(2_100), None)
        .expect("second pending log after the log interval");
    assert_eq!(second.pending_gap_ms, 2_100);
    assert_eq!(second.pending_polls, 4);
    assert_eq!(second.polls, 4);
    assert_eq!(second.max_poll_gap_ms, 600);

    diag.note_poll_at(start + std::time::Duration::from_millis(2_200));
    let read = diag.note_read_at(128, start + std::time::Duration::from_millis(2_200), None);
    assert_eq!(read.rx_bytes, 128);
    diag.note_poll_at(start + std::time::Duration::from_millis(3_300));
    let after_read = diag
        .note_pending_at(start + std::time::Duration::from_millis(3_300), None)
        .expect("pending gap is measured from the latest data read");
    assert_eq!(after_read.pending_gap_ms, 1_100);
    assert_eq!(after_read.polls, 6);
    assert_eq!(after_read.max_poll_gap_ms, 1_100);
    assert_eq!(after_read.rx_bytes, 128);
    assert_eq!(after_read.reads, 1);

    let close = diag.close_snapshot();
    assert_eq!(close.pending_polls, 5);
    assert_eq!(close.max_pending_gap_ms, 2_100);
    assert_eq!(close.polls, 6);
    assert_eq!(close.max_poll_gap_ms, 1_100);
}

#[test]
fn tuic_tcp_stream_diag_reports_transport_delta_since_last_read() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let start = std::time::Instant::now();
    let mut diag = TuicTcpStreamDiag::new(meta, start);

    diag.note_poll_at(start + std::time::Duration::from_millis(100));
    diag.note_read_at(
        128,
        start + std::time::Duration::from_millis(100),
        Some(TuicStreamTransportSample {
            udp_rx_datagrams: 10,
            udp_rx_bytes: 20_000,
            rx_stream_frames: 5,
            rx_ack_frames: 1,
            tx_ack_frames: 2,
            sent_plpmtud_probes: 3,
            lost_plpmtud_probes: 0,
            black_holes_detected: 0,
        }),
    );
    diag.note_poll_at(start + std::time::Duration::from_millis(1_200));
    let pending = diag
        .note_pending_at(
            start + std::time::Duration::from_millis(1_200),
            Some(TuicStreamTransportSample {
                udp_rx_datagrams: 17,
                udp_rx_bytes: 44_000,
                rx_stream_frames: 8,
                rx_ack_frames: 4,
                tx_ack_frames: 6,
                sent_plpmtud_probes: 4,
                lost_plpmtud_probes: 1,
                black_holes_detected: 1,
            }),
        )
        .expect("pending over threshold logs");
    let transport = pending.transport.expect("transport sample is attached");

    assert_eq!(transport.since_last_read.udp_rx_datagrams, 7);
    assert_eq!(transport.since_last_read.udp_rx_bytes, 24_000);
    assert_eq!(transport.since_last_read.rx_stream_frames, 3);
    assert_eq!(transport.since_last_pending.udp_rx_datagrams, 17);
    assert_eq!(transport.since_last_pending.udp_rx_bytes, 44_000);
    assert_eq!(transport.since_last_pending.rx_stream_frames, 8);
    assert_eq!(transport.sample.rx_ack_frames, 4);
    assert_eq!(transport.sample.tx_ack_frames, 6);
    assert_eq!(transport.sample.lost_plpmtud_probes, 1);
    assert_eq!(transport.sample.black_holes_detected, 1);
}

#[test]
fn tuic_tcp_stream_pending_event_reports_since_last_pending_delta() {
    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let start = std::time::Instant::now();
    let mut diag = TuicTcpStreamDiag::new(meta, start);

    diag.note_pending_at(
        start + std::time::Duration::from_millis(1_000),
        Some(TuicStreamTransportSample {
            udp_rx_datagrams: 10,
            udp_rx_bytes: 20_000,
            rx_stream_frames: 5,
            rx_ack_frames: 1,
            tx_ack_frames: 2,
            sent_plpmtud_probes: 0,
            lost_plpmtud_probes: 0,
            black_holes_detected: 0,
        }),
    )
    .expect("first pending log arms the pending baseline");

    let pending = diag
        .note_pending_at(
            start + std::time::Duration::from_millis(2_000),
            Some(TuicStreamTransportSample {
                udp_rx_datagrams: 12,
                udp_rx_bytes: 22_882,
                rx_stream_frames: 5,
                rx_ack_frames: 2,
                tx_ack_frames: 3,
                sent_plpmtud_probes: 0,
                lost_plpmtud_probes: 0,
                black_holes_detected: 0,
            }),
        )
        .expect("second pending log reports delta since previous pending log");
    let transport = pending.transport.expect("transport sample is attached");

    assert_eq!(transport.since_last_read.udp_rx_datagrams, 12);
    assert_eq!(transport.since_last_pending.udp_rx_datagrams, 2);
    assert_eq!(transport.since_last_pending.udp_rx_bytes, 2_882);
    assert_eq!(
        transport.since_last_pending.rx_stream_frames, 0,
        "connection stream frames may be stale even when the since-read cause remains stream-frame-pending"
    );
}

#[test]
fn ordered_quic_chunk_assembler_buffers_gap_then_drains_contiguously() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(3, bytes::Bytes::from_static(b"def"));
    assert_eq!(assembler.drain_into(&mut read_buf), 0);
    assert_eq!(read_buf.filled(), b"");

    assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_trims_already_delivered_prefix() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut first_out = [0u8; 3];
    let mut first = ReadBuf::new(&mut first_out);
    assembler.push_chunk(0, bytes::Bytes::from_static(b"abc"));
    assert_eq!(assembler.drain_into(&mut first), 3);

    let mut second_out = [0u8; 3];
    let mut second = ReadBuf::new(&mut second_out);
    assembler.push_chunk(1, bytes::Bytes::from_static(b"bcdef"));
    assert_eq!(assembler.drain_into(&mut second), 3);
    assert_eq!(second.filled(), b"def");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_keeps_longer_duplicate_at_same_offset() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));
    assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));

    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[test]
fn ordered_quic_chunk_assembler_replaces_short_duplicate_with_longer() {
    let mut assembler = OrderedQuicChunkAssembler::default();
    let mut out = [0u8; 6];
    let mut read_buf = ReadBuf::new(&mut out);

    assembler.push_chunk(0, bytes::Bytes::from_static(b"ab"));
    assembler.push_chunk(0, bytes::Bytes::from_static(b"abcdef"));

    assert_eq!(assembler.drain_into(&mut read_buf), 6);
    assert_eq!(read_buf.filled(), b"abcdef");
    assert_eq!(assembler.next_offset(), 6);
    assert_eq!(assembler.buffered_bytes(), 0);
}

#[derive(Debug)]
struct MockOrderedChunkRecv {
    chunks: std::collections::VecDeque<io::Result<Option<bytes::Bytes>>>,
    max_lens: Vec<usize>,
    poll_count: usize,
}

impl MockOrderedChunkRecv {
    fn new(chunks: impl IntoIterator<Item = io::Result<Option<bytes::Bytes>>>) -> Self {
        Self {
            chunks: chunks.into_iter().collect(),
            max_lens: Vec::new(),
            poll_count: 0,
        }
    }
}

impl OrderedChunkRecv for MockOrderedChunkRecv {
    fn poll_read_ordered_chunk(
        &mut self,
        _cx: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<io::Result<Option<bytes::Bytes>>> {
        self.poll_count += 1;
        self.max_lens.push(max_len);
        Poll::Ready(self.chunks.pop_front().unwrap_or(Ok(None)))
    }
}

#[tokio::test]
async fn ordered_relay_stream_reads_ordered_chunks_without_join() {
    use tokio::io::AsyncReadExt;

    let recv = MockOrderedChunkRecv::new([
        Ok(Some(bytes::Bytes::from_static(b"abc"))),
        Ok(Some(bytes::Bytes::from_static(b"def"))),
        Ok(None),
    ]);
    let (send, _peer) = tokio::io::duplex(64);
    let mut stream = TuicOrderedRelayStream::new(recv, send);

    let mut out = Vec::new();
    stream.read_to_end(&mut out).await.unwrap();

    assert_eq!(out, b"abcdef");
    assert_eq!(stream.recv.poll_count, 3);
}

#[tokio::test]
async fn ordered_relay_stream_keeps_remainder_without_repolling_recv() {
    use tokio::io::AsyncReadExt;

    let recv =
        MockOrderedChunkRecv::new([Ok(Some(bytes::Bytes::from_static(b"abcdef"))), Ok(None)]);
    let (send, _peer) = tokio::io::duplex(64);
    let mut stream = TuicOrderedRelayStream::new(recv, send);

    let mut first = [0u8; 3];
    let first_read = stream.read(&mut first).await.unwrap();
    assert_eq!(first_read, 3);
    assert_eq!(&first, b"abc");
    assert_eq!(stream.recv.max_lens, vec![3]);

    let mut second = [0u8; 3];
    let second_read = stream.read(&mut second).await.unwrap();
    assert_eq!(second_read, 3);
    assert_eq!(&second, b"def");
    assert_eq!(
        stream.recv.poll_count, 1,
        "remaining bytes from an oversized chunk must be served locally"
    );
}

#[tokio::test]
async fn ordered_relay_stream_delegates_writes_to_quic_send_half() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let recv = MockOrderedChunkRecv::new([Ok(None)]);
    let (send, mut peer) = tokio::io::duplex(64);
    let mut stream = TuicOrderedRelayStream::new(recv, send);

    stream.write_all(b"ping").await.unwrap();
    stream.flush().await.unwrap();

    let mut out = [0u8; 4];
    peer.read_exact(&mut out).await.unwrap();
    assert_eq!(&out, b"ping");
}

#[tokio::test]
async fn tracked_relay_stream_records_nonempty_reads() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let target = TargetAddr::parse("1.2.3.4:5201").unwrap();
    let meta = TuicTcpStreamDiagMeta::new(&target, 3, 42, 8);
    let active = Arc::new(AtomicU64::new(0));
    let (lease, _) = TcpPoolSlotLease::reserve(active);
    let (mut writer, inner) = tokio::io::duplex(64);
    let mut stream = TrackedRelayStream::new(
        inner,
        lease,
        Some(TuicTcpStreamDiag::new(meta, std::time::Instant::now())),
    );

    writer.write_all(b"abc").await.unwrap();

    let mut buf = [0u8; 3];
    let read = stream.read(&mut buf).await.unwrap();

    assert_eq!(read, 3);
    assert_eq!(&buf, b"abc");
    let snapshot = stream.tcp_diag.as_ref().unwrap().close_snapshot();
    assert_eq!(snapshot.rx_bytes, 3);
    assert_eq!(snapshot.reads, 1);
    assert!(
        snapshot.polls >= 1,
        "stream read should poll at least once: {snapshot:?}"
    );
    assert!(
        snapshot.first_rx_ms < 1_000,
        "first_rx_ms should be immediate in the in-memory stream: {snapshot:?}"
    );
}

#[test]
fn tcp_pool_phase_history_cannot_shift_the_next_idle_pair() {
    let selector = TcpPoolAdmission::new(2);

    {
        let prior = selector
            .try_reserve(&[])
            .expect("an idle pool must accept a historical one-flow phase");
        assert_eq!(prior.index, 0);
        assert_eq!(prior.active_before, 0);
    }

    let control = selector
        .try_reserve(&[])
        .expect("the next control flow must reserve the primary slot");
    let data = selector
        .try_reserve(&[])
        .expect("the live control reservation must steer data to auxiliary");

    assert_eq!((control.index, control.active_before), (0, 0));
    assert_eq!((data.index, data.active_before), (1, 0));
    drop((control, data));
    assert_eq!(selector.active_slots[0].load(Ordering::Relaxed), 0);
    assert_eq!(selector.active_slots[1].load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn tcp_pool_generation_lease_zero_is_isolated_from_successor() {
    let pool_active = Arc::new(AtomicU64::new(0));
    let predecessor_slot_active = Arc::new(AtomicU64::new(0));
    let successor_slot_active = Arc::new(AtomicU64::new(0));
    let predecessor = Arc::new(TcpPoolGenerationActivity::new());
    let successor = Arc::new(TcpPoolGenerationActivity::new());

    let predecessor_lease = TcpPoolSlotLease::reserve_generation(
        predecessor.clone(),
        predecessor_slot_active,
        pool_active.clone(),
    )
    .expect("the predecessor generation must accept its first lease");
    let successor_lease = TcpPoolSlotLease::reserve_generation(
        successor.clone(),
        successor_slot_active,
        pool_active.clone(),
    )
    .expect("the successor generation must have an independent lease counter");

    assert_eq!(predecessor.active(), 1);
    assert_eq!(successor.active(), 1);
    assert_eq!(pool_active.load(Ordering::Acquire), 2);

    let predecessor_zero = tokio::spawn({
        let predecessor = predecessor.clone();
        async move { predecessor.wait_for_zero().await }
    });
    tokio::task::yield_now().await;
    drop(predecessor_lease);
    tokio::time::timeout(Duration::from_secs(1), predecessor_zero)
        .await
        .expect("the exact predecessor zero transition must notify its drain waiter")
        .expect("the predecessor zero waiter must not panic");

    assert_eq!(predecessor.active(), 0);
    assert_eq!(successor.active(), 1);
    assert_eq!(pool_active.load(Ordering::Acquire), 1);

    drop(successor_lease);
    assert_eq!(successor.active(), 0);
    assert_eq!(pool_active.load(Ordering::Acquire), 0);
}

#[test]
fn tcp_pool_successor_generation_owns_its_exact_service_certificate() {
    let clock = Instant::now();
    let successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 24_800)
        .expect("a positive post-turn cwnd must create one exact certificate");

    assert_eq!(
        successor.successor_service_certificate,
        TcpPoolSuccessorServiceCertificate::new(TcpPoolTransportIdentity::new(22, 2), 7, 24_800,)
    );
    assert_eq!(successor.replacement_forward_service_floor, Some(24_800));
    assert!(
        TcpPoolGeneration::new_successor(23_u64, 3, 1, clock, 8, 0).is_none(),
        "zero service cannot manufacture a certificate"
    );
}

#[test]
fn tcp_pool_reconnected_generation_cannot_reuse_a_successor_certificate() {
    let clock = Instant::now();
    let mut successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 24_800)
        .expect("the replay starts with an installed successor proof");

    successor.note_reconnect_success();

    assert_eq!(
        successor.identity(),
        TcpPoolTransportIdentity::new(22, 3),
        "reconnect must advance the logical identity"
    );
    assert_eq!(successor.successor_service_certificate, None);
    assert_eq!(successor.replacement_forward_service_floor, None);
}

#[test]
fn tcp_pool_generation_recomputes_each_replacement_handoff_from_readiness() {
    let clock = Instant::now();
    let mut generation = TcpPoolGeneration::new_successor(11_u64, 2, 1, clock, 7, 26_424).unwrap();

    assert_eq!(
        generation.record_forward_service_floor(3_605_919),
        Some(3_605_919)
    );
    assert_eq!(
        generation.record_forward_service_floor(381_502),
        Some(381_502),
        "a failed historical handoff must not ratchet the next replacement attempt"
    );
    assert_eq!(generation.replacement_forward_service_floor, Some(381_502));
    assert_eq!(
        generation.record_forward_service_floor(17_360),
        Some(26_424)
    );
    assert_eq!(generation.record_forward_service_floor(0), None);
}

#[test]
fn tcp_pool_successor_forward_service_progress_is_dynamic_and_fail_closed() {
    assert_eq!(
        tcp_pool_successor_forward_service_progress(None, 12_000, 24_000),
        TcpPoolSuccessorForwardServiceProgress::Complete,
        "an uncertified predecessor preserves the existing one-turn contract"
    );
    assert_eq!(
        tcp_pool_successor_forward_service_progress(Some(41_301), 12_000, 26_424),
        TcpPoolSuccessorForwardServiceProgress::Continue
    );
    assert_eq!(
        tcp_pool_successor_forward_service_progress(Some(41_301), 26_424, 52_848),
        TcpPoolSuccessorForwardServiceProgress::Complete
    );
    assert_eq!(
        tcp_pool_successor_forward_service_progress(Some(41_301), 26_424, 26_424),
        TcpPoolSuccessorForwardServiceProgress::Stalled,
        "a successful turn without cwnd progress cannot become an unbounded retry"
    );
}

#[tokio::test]
async fn tcp_pool_generation_slot_installs_successor_without_resetting_predecessor() {
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
    let predecessor_activity = slot.current_activity().await;
    let slot_active = Arc::new(AtomicU64::new(0));
    let pool_active = Arc::new(AtomicU64::new(0));
    let predecessor_lease = TcpPoolSlotLease::reserve_generation(
        predecessor_activity.clone(),
        slot_active,
        pool_active,
    )
    .unwrap();
    let expected = TcpPoolTransportIdentity::new(11, 1);
    let successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 24_800)
        .expect("an installable successor must own exact service evidence");

    let installed = slot
        .install_successor(expected, &predecessor_activity, successor, Instant::now())
        .await
        .expect("the exact predecessor identity must install one successor");

    assert_eq!(installed.predecessor_identity, expected);
    assert_eq!(
        installed.successor_identity,
        TcpPoolTransportIdentity::new(22, 2)
    );
    assert_eq!(slot.current_transport().await, 22);
    assert_eq!(slot.draining_transport().await, Some(11));
    assert_eq!(predecessor_activity.active(), 1);
    assert!(
        slot.take_drained(expected).await.is_none(),
        "a predecessor with a live lease must remain untouched"
    );

    drop(predecessor_lease);
    predecessor_activity.wait_for_zero().await;
    assert_eq!(predecessor_activity.active(), 0);
    let drained = slot
        .take_drained(expected)
        .await
        .expect("only the exact zero predecessor may be reaped");
    assert_eq!(drained.generation.transport, 11);
    assert_eq!(slot.draining_transport().await, None);
}

#[tokio::test]
async fn tcp_pool_generation_install_rejects_primary_stale_and_second_predecessor() {
    let clock = Instant::now();
    let primary = TcpPoolGenerationSlot::new(0, 10_u64, 1, 1, clock);
    let primary_activity = primary.current_activity().await;
    let primary_result = primary
        .install_successor(
            TcpPoolTransportIdentity::new(10, 1),
            &primary_activity,
            TcpPoolGeneration::new_successor(20_u64, 2, 1, clock, 7, 24_800).unwrap(),
            Instant::now(),
        )
        .await;
    assert!(matches!(
        primary_result,
        Err(TcpPoolGenerationInstallError::Primary)
    ));
    assert_eq!(primary.current_transport().await, 10);

    let auxiliary = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
    let predecessor_activity = auxiliary.current_activity().await;
    let stale_result = auxiliary
        .install_successor(
            TcpPoolTransportIdentity::new(99, 1),
            &predecessor_activity,
            TcpPoolGeneration::new_successor(21_u64, 2, 1, clock, 7, 24_800).unwrap(),
            Instant::now(),
        )
        .await;
    assert!(matches!(
        stale_result,
        Err(TcpPoolGenerationInstallError::StalePredecessor)
    ));
    assert_eq!(auxiliary.current_transport().await, 11);
    assert_eq!(auxiliary.draining_transport().await, None);

    auxiliary
        .install_successor(
            TcpPoolTransportIdentity::new(11, 1),
            &predecessor_activity,
            TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 24_800).unwrap(),
            Instant::now(),
        )
        .await
        .unwrap();
    let successor_activity = auxiliary.current_activity().await;
    let second_result = auxiliary
        .install_successor(
            TcpPoolTransportIdentity::new(22, 2),
            &successor_activity,
            TcpPoolGeneration::new_successor(23_u64, 3, 1, clock, 7, 24_800).unwrap(),
            Instant::now(),
        )
        .await;
    assert!(matches!(
        second_result,
        Err(TcpPoolGenerationInstallError::PredecessorDraining)
    ));
    assert_eq!(auxiliary.current_transport().await, 22);
    assert_eq!(auxiliary.draining_transport().await, Some(11));
}

#[tokio::test]
async fn tcp_pool_replacement_trigger_reserves_only_the_installed_successor() {
    let clock = Instant::now();
    let slots = [
        TcpPoolGenerationSlot::new(0, 10_u64, 1, 1, clock),
        TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock),
    ];
    let activities = vec![
        slots[0].current_activity().await,
        slots[1].current_activity().await,
    ];
    let admission = TcpPoolAdmission::with_current_generation_activity(activities.clone());
    let anchored = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            5_140,
            Duration::from_millis(175),
            0,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(11, 1),
            77_246,
            Duration::from_millis(179),
            0,
        ),
    ];
    let primary = admission.try_reserve(&anchored).unwrap();
    let auxiliary = admission.try_reserve(&anchored).unwrap();
    drop((primary, auxiliary));
    admission.set_active_for_test(0, 14);
    admission.set_active_for_test(1, 6);
    let observed = [
        anchored[0],
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(11, 1),
            77_246,
            Duration::from_millis(179),
            16,
        ),
    ];
    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) =
        admission.try_decide(&observed, &[false, true]).unwrap()
    else {
        panic!("the replay must prepare an auxiliary successor");
    };
    let predecessor_activity = activities[1].clone();
    let successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 5, 24_800)
        .expect("the installed successor must own its successful service turn");
    let successor_activity = successor.activity.clone();
    let successor_install_service_proof =
        TcpPoolSuccessorInstallServiceProof::for_successor(&successor, 24_800).unwrap();
    let successor_observation = TcpPoolPathObservation::known_with_successor_service_certificate(
        successor.identity(),
        24_800,
        Duration::from_millis(175),
        0,
        5,
        5,
        24_800,
    );
    slots[1]
        .install_successor_with(
            TcpPoolTransportIdentity::new(11, 1),
            &predecessor_activity,
            successor,
            successor_install_service_proof,
            Instant::now(),
            |predecessor, successor| {
                admission.replace_current_generation_activity(1, predecessor, successor.clone())
            },
        )
        .await
        .unwrap();

    let selected = admission
        .reserve_replacement_successor(
            replacement,
            successor_activity.clone(),
            successor_observation,
        )
        .unwrap();

    assert_eq!((selected.index, selected.active_before), (1, 0));
    assert_eq!(
        selected
            .candidates
            .iter()
            .find(|candidate| candidate.index == 1)
            .expect("the installed successor must replace its predecessor candidate")
            .successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::Ready
    );
    assert_eq!(
        slots[1]
            .state
            .lock()
            .await
            .current
            .successor_service_certificate,
        TcpPoolSuccessorServiceCertificate::new(TcpPoolTransportIdentity::new(22, 2), 5, 24_800,)
    );
    assert_eq!(predecessor_activity.active(), 6);
    assert_eq!(successor_activity.active(), 1);
    assert_eq!(admission.active_total(), 21);
    drop(selected);
    assert_eq!(predecessor_activity.active(), 6);
    assert_eq!(successor_activity.active(), 0);
    assert_eq!(admission.active_total(), 20);
}

#[test]
fn tcp_pool_busy_epoch_black_hole_advancement_isolates_new_forward_opens() {
    let admission = TcpPoolAdmission::new(2);
    let anchored = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(0, 1),
            6_665,
            Duration::from_millis(176),
            0,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(1, 1),
            12_887,
            Duration::from_millis(176),
            0,
        ),
    ];

    let first = admission
        .try_reserve(&anchored)
        .expect("the first idle slot must open its qualification epoch");
    let second = admission
        .try_reserve(&anchored)
        .expect("the second idle slot must open its qualification epoch");
    assert_eq!((first.index, second.index), (0, 1));
    drop((first, second));

    admission.set_active_for_test(0, 8);
    admission.set_active_for_test(1, 6);
    let advanced = [
        anchored[0],
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(1, 1),
            12_887,
            Duration::from_millis(176),
            10,
        ),
    ];

    let selected = admission
        .try_reserve(&advanced)
        .expect("a qualified busy alternative must isolate the degraded slot");

    assert_eq!((selected.index, selected.active_before), (0, 8));
    assert_eq!(
        selected.qualification,
        TcpPoolForwardQualification::Qualified
    );
    assert!(selected.qualification_override);
    assert!(!selected.all_degraded_fallback);
}

#[test]
fn tcp_pool_qualified_lane_collapse_requests_auxiliary_generation_replacement() {
    let admission = TcpPoolAdmission::new(2);
    let anchored = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(0, 1),
            5_140,
            Duration::from_millis(175),
            0,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(1, 1),
            77_246,
            Duration::from_millis(179),
            0,
        ),
    ];
    let first = admission.try_reserve(&anchored).unwrap();
    let second = admission.try_reserve(&anchored).unwrap();
    drop((first, second));

    admission.set_active_for_test(0, 14);
    admission.set_active_for_test(1, 6);
    let observed = [
        anchored[0],
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(1, 1),
            77_246,
            Duration::from_millis(179),
            16,
        ),
    ];

    let decision = admission
        .try_decide(&observed, &[false, true])
        .expect("the degraded auxiliary generation must remain replaceable");

    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = decision else {
        panic!("qualified-lane collapse must request replacement, not reserve conn0");
    };
    assert_eq!(replacement.index, 1);
}

#[test]
fn tcp_pool_stale_successor_service_certificate_requests_auxiliary_replacement() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    admission.set_active_for_test(1, 0);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_micros(172_618),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            17_360,
            Duration::from_micros(172_869),
            0,
            0,
            0,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("the exact stale successor must retain bounded replacement authority");

    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = decision else {
        panic!("a 17,360B current window must not borrow a 24,800B successor proof");
    };
    assert_eq!(replacement.index, 1);
}

#[test]
fn tcp_pool_successor_at_its_proved_floor_does_not_churn_generation() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    admission.set_active_for_test(1, 0);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_micros(172_618),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            24_800,
            Duration::from_micros(172_869),
            0,
            0,
            0,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("a successor at its own proved floor must remain available");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("ordinary congestion history must not replace a currently proved successor");
    };
    assert_eq!(reservation.index, 1);
    assert_eq!(
        reservation.candidates[1].successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::Ready
    );
}

#[test]
fn tcp_pool_historical_handoff_proof_does_not_churn_ready_successor() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    let readiness_floor = 26_424;
    let current_cwnd = 381_502;
    let historical_handoff_proof = 3_605_919;
    let mut successor =
        TcpPoolGeneration::new_successor(11_u64, 2, 1, Instant::now(), 0, readiness_floor).unwrap();
    assert_eq!(
        successor.record_forward_service_floor(historical_handoff_proof),
        Some(historical_handoff_proof)
    );
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(164),
            2,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            successor.identity(),
            current_cwnd,
            Duration::from_millis(164),
            0,
            0,
            0,
            readiness_floor,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("native same-path congestion contraction must retain current availability");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("a historical multi-round handoff cannot authorize generation churn");
    };
    assert_eq!(reservation.index, 1);
    assert_eq!(
        reservation.candidates[1].successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::Ready
    );
}

#[test]
fn tcp_pool_successor_certificate_cannot_cross_a_path_generation() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            96_000,
            Duration::from_millis(173),
            0,
            4,
            3,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("a migrated successor must retain fresh replacement authority");

    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = decision else {
        panic!("a large cwnd cannot transfer an ACK proof across path generations");
    };
    assert_eq!(replacement.index, 1);
    assert_eq!(
        replacement.candidates[1].successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::StalePath
    );
}

#[test]
fn tcp_pool_successor_certificate_cannot_cross_a_transport_identity() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    let observed_identity = TcpPoolTransportIdentity::new(11, 3);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::Known {
            identity: observed_identity,
            path_service: TcpPoolPathService::known(96_000, Duration::from_millis(173)),
            black_holes_detected: 0,
            path_generation: Some(4),
            successor_service_certificate: TcpPoolSuccessorServiceCertificate::new(
                TcpPoolTransportIdentity::new(99, 3),
                4,
                24_800,
            ),
        },
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("a certificate from another transport must retain replacement authority");

    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = decision else {
        panic!("transport identity is part of the exact successor proof");
    };
    assert_eq!(replacement.index, 1);
    assert_eq!(
        replacement.candidates[1].successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::StaleIdentity
    );
}

#[test]
fn tcp_pool_missing_current_path_generation_preserves_unknown_availability() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    let identity = TcpPoolTransportIdentity::new(11, 2);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::Known {
            identity,
            path_service: TcpPoolPathService::known(17_360, Duration::from_millis(173)),
            black_holes_detected: 0,
            path_generation: None,
            successor_service_certificate: TcpPoolSuccessorServiceCertificate::new(
                identity, 0, 24_800,
            ),
        },
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("unavailable adapter evidence must not manufacture staleness");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("unknown current path generation cannot authorize replacement");
    };
    assert_eq!(reservation.index, 1);
    assert_eq!(
        reservation.candidates[1].successor_service_readiness,
        TcpPoolSuccessorServiceReadiness::Unknown
    );
}

#[test]
fn tcp_pool_all_stale_service_candidates_preserve_bounded_fallback() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    admission.set_active_for_test(1, 1);
    let observations = [
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(10, 2),
            17_360,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            18_000,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("all-stale evidence must preserve bounded availability");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("all-stale fallback cannot enter replacement churn");
    };
    assert_eq!(reservation.index, 1);
    assert!(reservation.candidates.iter().all(
            |candidate| candidate.admitted && candidate.successor_service_readiness.is_stale()
        ));
}

#[test]
fn tcp_pool_stale_successor_with_unavailable_replacement_uses_current_fallback() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            17_360,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, false])
        .expect("a draining predecessor must not make a current qualified lane unavailable");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("unavailable replacement authority must fall back to the admitted primary");
    };
    assert_eq!(reservation.index, 0);
    assert!(!reservation.candidates[1].admitted);
}

#[test]
fn tcp_pool_stale_busy_successor_without_exact_epoch_cannot_spin_replacement() {
    let admission = TcpPoolAdmission::new(2);
    admission.set_active_for_test(0, 2);
    admission.set_active_for_test(1, 1);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            17_360,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
    ];

    let decision = admission
        .try_decide(&observations, &[false, true])
        .expect("missing exact epoch evidence must retain a bounded current fallback");

    let TcpPoolAdmissionDecision::ReserveCurrent(reservation) = decision else {
        panic!("replacement cannot start without an exact qualification epoch");
    };
    assert_eq!(reservation.index, 0);
    assert_eq!(
        reservation.candidates[1].qualification,
        TcpPoolForwardQualification::Unknown
    );
    assert!(!reservation.candidates[1].admitted);
}

#[test]
fn tcp_pool_failed_auxiliary_replacement_falls_back_without_same_open_retry() {
    let admission = TcpPoolAdmission::new(3);
    let anchored = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(165),
            0,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(11, 1),
            980_000,
            Duration::from_millis(163),
            0,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(12, 1),
            960_000,
            Duration::from_millis(164),
            0,
        ),
    ];
    let first = admission.try_reserve(&anchored).unwrap();
    let second = admission.try_reserve(&anchored).unwrap();
    let third = admission.try_reserve(&anchored).unwrap();
    drop((first, second, third));
    admission.set_active_for_test(0, 2);
    admission.set_active_for_test(1, 2);
    admission.set_active_for_test(2, 2);

    let degraded = [
        anchored[0],
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(11, 1),
            980_000,
            Duration::from_millis(163),
            5,
        ),
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(12, 1),
            960_000,
            Duration::from_millis(164),
            3,
        ),
    ];
    let first_decision = admission
        .try_decide(&degraded, &[false, true, true])
        .expect("the degraded auxiliary generation must request replacement");
    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = first_decision else {
        panic!("the replay must reach auxiliary replacement");
    };
    assert_eq!(replacement.index, 1);
    drop(replacement);

    let fallback = admission
        .try_decide_excluding(&degraded, &[false, true, true], &[false, true, false], true)
        .expect("the same open must retain a qualified current-generation fallback");
    let TcpPoolAdmissionDecision::ReserveCurrent(fallback) = fallback else {
        panic!("the same open must not retry replacement");
    };
    assert_eq!(fallback.index, 0);
    assert_eq!(fallback.active_before, 2);
    assert_eq!(admission.active_total(), 7);
    assert!(matches!(
        admission.try_decide_excluding(
            &degraded,
            &[false, true, true],
            &[false, true, false],
            true,
        ),
        Err(TcpPoolReservationError::Busy)
    ));
    drop(fallback);
    assert_eq!(admission.active_total(), 6);

    let next_open = admission
        .try_decide(&degraded, &[false, true, true])
        .expect("a later independent open retains fresh replacement authority");
    let TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement) = next_open else {
        panic!("replacement suppression must remain attempt-local");
    };
    assert_eq!(replacement.index, 1);
}

#[test]
fn tcp_pool_failed_stale_service_replacement_cannot_start_a_second_maintenance_action() {
    let admission = TcpPoolAdmission::new(3);
    let observations = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 1),
            12_000,
            Duration::from_millis(173),
            0,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(11, 2),
            17_360,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
        TcpPoolPathObservation::known_with_successor_service_certificate(
            TcpPoolTransportIdentity::new(12, 2),
            18_000,
            Duration::from_millis(173),
            0,
            0,
            0,
            24_800,
        ),
    ];
    let primary_epoch = admission
        .try_reserve(&observations)
        .expect("the primary must establish its exact qualification epoch");
    assert_eq!(primary_epoch.index, 0);
    drop(primary_epoch);
    admission.set_active_for_test(0, 2);

    let first = admission
        .try_decide(&observations, &[false, true, true])
        .expect("one stale auxiliary may request replacement");
    let TcpPoolAdmissionDecision::ReplaceAuxiliary(first) = first else {
        panic!("the first stale auxiliary must own the only maintenance attempt");
    };
    assert_eq!(first.index, 1);
    drop(first);

    let fallback = admission
        .try_decide_excluding(
            &observations,
            &[false, true, true],
            &[false, true, false],
            true,
        )
        .expect("the same open must retain a current-generation fallback");
    let TcpPoolAdmissionDecision::ReserveCurrent(fallback) = fallback else {
        panic!("the same open must not replace a second stale auxiliary generation");
    };
    assert_eq!(fallback.index, 0);
}

#[test]
fn tcp_pool_active_zero_starts_a_fresh_forward_qualification_epoch() {
    let admission = TcpPoolAdmission::new(1);
    let identity = TcpPoolTransportIdentity::new(7, 3);
    let initial = [TcpPoolPathObservation::known(
        identity,
        10_000,
        Duration::from_millis(100),
        2,
    )];
    let first = admission.try_reserve(&initial).unwrap();
    assert_eq!(first.qualification, TcpPoolForwardQualification::Qualified);
    assert_eq!(first.qualification_anchor, Some(2));
    drop(first);

    admission.set_active_for_test(0, 1);
    let advanced = [TcpPoolPathObservation::known(
        identity,
        10_000,
        Duration::from_millis(100),
        5,
    )];
    let degraded = admission.try_reserve(&advanced).unwrap();
    assert_eq!(
        degraded.qualification,
        TcpPoolForwardQualification::Degraded
    );
    assert!(degraded.all_degraded_fallback);
    drop(degraded);

    admission.set_active_for_test(0, 0);
    let recovered = admission.try_reserve(&advanced).unwrap();
    assert_eq!(
        recovered.qualification,
        TcpPoolForwardQualification::Qualified
    );
    assert_eq!(recovered.qualification_anchor, Some(5));
    assert_eq!(recovered.black_holes_current, Some(5));
    assert!(!recovered.all_degraded_fallback);
}

#[test]
fn tcp_pool_idle_observation_does_not_commit_an_epoch_before_reservation() {
    let admission = TcpPoolAdmission::new(1);
    let identity = TcpPoolTransportIdentity::new(7, 3);
    let anchored = TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 0);
    let first = admission.try_reserve(&[anchored]).unwrap();
    assert_eq!(first.qualification_anchor, Some(0));

    let losing_idle_sample =
        TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 7);
    let sampled = admission.qualification_for(0, 0, losing_idle_sample);
    assert_eq!(
        sampled.qualification,
        TcpPoolForwardQualification::Qualified
    );

    let advanced = TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 1);
    let still_busy = admission.qualification_for(0, 1, advanced);
    assert_eq!(
        still_busy.qualification,
        TcpPoolForwardQualification::Degraded,
        "an idle observation that lost reservation ownership must not reset the busy epoch"
    );
}

#[test]
fn tcp_pool_all_degraded_fallback_preserves_bounded_least_active_ordering() {
    let admission = TcpPoolAdmission::new(2);
    let identities = [
        TcpPoolTransportIdentity::new(0, 1),
        TcpPoolTransportIdentity::new(1, 1),
    ];
    let anchored = identities.map(|identity| {
        TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 0)
    });
    let first = admission.try_reserve(&anchored).unwrap();
    let second = admission.try_reserve(&anchored).unwrap();
    drop((first, second));

    admission.set_active_for_test(0, 7);
    admission.set_active_for_test(1, 3);
    let degraded = [
        TcpPoolPathObservation::known(identities[0], 1_000_000, Duration::from_millis(1), 1),
        TcpPoolPathObservation::known(identities[1], 1, Duration::from_secs(1), 2),
    ];

    let selected = admission.try_reserve(&degraded).unwrap();

    assert_eq!((selected.index, selected.active_before), (1, 3));
    assert_eq!(
        selected.qualification,
        TcpPoolForwardQualification::Degraded
    );
    assert!(selected.all_degraded_fallback);
    assert!(!selected.qualification_override);
    drop(selected);

    admission.set_active_for_test(0, 6);
    admission.set_active_for_test(1, 6);
    let service_selected = admission.try_reserve(&degraded).unwrap();
    assert_eq!(
        (service_selected.index, service_selected.active_before),
        (0, 6)
    );
    assert!(service_selected.path_service_tiebreak);
    assert!(!service_selected.service_normalized_ordering);
    assert!(service_selected.all_degraded_fallback);
    drop(service_selected);

    let equal_service = identities.map(|identity| {
        TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 3)
    });
    let stable = admission.try_reserve(&equal_service).unwrap();
    assert_eq!((stable.index, stable.active_before), (0, 6));
    assert!(!stable.path_service_tiebreak);
    assert!(!stable.service_normalized_ordering);
    assert!(stable.all_degraded_fallback);
}

#[test]
fn tcp_pool_unknown_evidence_never_reuses_or_resets_a_busy_epoch() {
    let admission = TcpPoolAdmission::new(2);
    let identities = [
        TcpPoolTransportIdentity::new(10, 1),
        TcpPoolTransportIdentity::new(11, 1),
    ];
    let anchored = identities.map(|identity| {
        TcpPoolPathObservation::known(identity, 10_000, Duration::from_millis(100), 4)
    });
    let first = admission.try_reserve(&anchored).unwrap();
    let second = admission.try_reserve(&anchored).unwrap();
    drop((first, second));
    admission.set_active_for_test(0, 5);
    admission.set_active_for_test(1, 4);

    let missing_and_degraded = [
        TcpPoolPathObservation::Unknown,
        TcpPoolPathObservation::known(identities[1], 10_000, Duration::from_millis(100), 5),
    ];
    let missing = admission.try_reserve(&missing_and_degraded).unwrap();
    assert_eq!(missing.index, 0);
    assert_eq!(missing.qualification, TcpPoolForwardQualification::Unknown);
    assert!(missing.qualification_override);
    drop(missing);

    let regression_and_degraded = [
        TcpPoolPathObservation::known(identities[0], 10_000, Duration::from_millis(100), 3),
        missing_and_degraded[1],
    ];
    let regression = admission.try_reserve(&regression_and_degraded).unwrap();
    assert_eq!(regression.index, 0);
    assert_eq!(
        regression.qualification,
        TcpPoolForwardQualification::Unknown
    );
    assert_eq!(regression.qualification_anchor, Some(4));
    assert_eq!(regression.black_holes_current, Some(3));
    drop(regression);

    let new_identity_and_degraded = [
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(10, 2),
            10_000,
            Duration::from_millis(100),
            0,
        ),
        missing_and_degraded[1],
    ];
    let replaced = admission.try_reserve(&new_identity_and_degraded).unwrap();
    assert_eq!(replaced.index, 0);
    assert_eq!(replaced.qualification, TcpPoolForwardQualification::Unknown);
    assert_eq!(replaced.qualification_anchor, None);
    assert_eq!(replaced.black_holes_current, Some(0));
}

#[test]
fn tcp_pool_equal_busy_load_prefers_greater_path_service() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(0, 62);
    selector.set_active_for_test(1, 62);
    let path_service = [
        TcpPoolPathService::known(10_124, Duration::from_millis(163)),
        TcpPoolPathService::known(23_842, Duration::from_millis(163)),
    ];

    let selected = selector
        .try_reserve(&tcp_pool_observations(&path_service))
        .expect("an equal-load busy pool must use current path service");

    assert_eq!((selected.index, selected.active_before), (1, 62));
    assert!(selected.service_normalized_ordering);
    assert!(!selected.path_service_tiebreak);
}

#[test]
fn tcp_pool_path_service_compares_exact_ratio_and_rejects_invalid_samples() {
    let lower_window_better_service = TcpPoolPathService::known(10_000, Duration::from_millis(100));
    let higher_window_lower_service = TcpPoolPathService::known(15_000, Duration::from_millis(200));
    assert!(lower_window_better_service.has_greater_service_than(higher_window_lower_service));
    assert!(!higher_window_lower_service.has_greater_service_than(lower_window_better_service));

    let max_fast = TcpPoolPathService::known(u64::MAX, Duration::from_micros(1));
    let max_slow = TcpPoolPathService::known(u64::MAX, Duration::from_micros(2));
    assert!(max_fast.has_greater_service_than(max_slow));
    let maximum_normalized_load = TcpPoolPathService::Known {
        cwnd: 1,
        rtt_micros: u64::MAX,
    };
    let smaller_normalized_load = TcpPoolPathService::Known {
        cwnd: u64::MAX,
        rtt_micros: u64::MAX,
    };
    assert_eq!(
        maximum_normalized_load.normalized_load_cmp(u64::MAX, smaller_normalized_load, u64::MAX,),
        Some(std::cmp::Ordering::Greater),
        "three-factor comparison must remain exact above u128 cross-product capacity"
    );
    assert_eq!(
        TcpPoolPathService::Known {
            cwnd: 10,
            rtt_micros: 100,
        }
        .normalized_load_cmp(
            1,
            TcpPoolPathService::Known {
                cwnd: 20,
                rtt_micros: 100,
            },
            2,
        ),
        Some(std::cmp::Ordering::Equal)
    );
    assert_eq!(
        TcpPoolPathService::Unknown.normalized_load_cmp(1, max_fast, 1),
        None
    );
    assert_eq!(
        TcpPoolPathService::Known {
            cwnd: 0,
            rtt_micros: 1,
        }
        .normalized_load_cmp(1, max_fast, 1),
        None
    );
    assert_eq!(
        TcpPoolPathService::known(0, Duration::from_millis(1)),
        TcpPoolPathService::Unknown
    );
    assert_eq!(
        TcpPoolPathService::known(1, Duration::ZERO),
        TcpPoolPathService::Unknown
    );
}

#[test]
fn tcp_pool_positive_rational_comparison_matches_small_cross_products() {
    for left_numerator in 1..=16 {
        for left_denominator in 1..=16 {
            for right_numerator in 1..=16 {
                for right_denominator in 1..=16 {
                    assert_eq!(
                        compare_positive_rationals(
                            left_numerator,
                            left_denominator,
                            right_numerator,
                            right_denominator,
                        ),
                        (left_numerator * right_denominator)
                            .cmp(&(right_numerator * left_denominator)),
                    );
                }
            }
        }
    }
}

#[test]
fn tcp_pool_idle_pair_ignores_stale_path_service_history() {
    let selector = TcpPoolAdmission::new(2);
    let path_service = [
        TcpPoolPathService::known(10_124, Duration::from_millis(163)),
        TcpPoolPathService::known(23_842, Duration::from_millis(163)),
    ];

    let observations = tcp_pool_observations(&path_service);
    let control = selector
        .try_reserve(&observations)
        .expect("idle control must reserve the stable primary slot");
    let data = selector
        .try_reserve(&observations)
        .expect("the live control reservation must steer data to auxiliary");

    assert_eq!((control.index, control.active_before), (0, 0));
    assert_eq!((data.index, data.active_before), (1, 0));
    assert!(!control.path_service_tiebreak);
    assert!(!data.path_service_tiebreak);
    assert!(!control.service_normalized_ordering);
    assert!(!data.service_normalized_ordering);
}

#[test]
fn tcp_pool_service_normalized_admission_keeps_data_on_served_path_after_control() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(0, 2);
    selector.set_active_for_test(1, 2);
    let control_observations = [
        TcpPoolPathService::known(12_000, Duration::from_micros(164_215)),
        TcpPoolPathService::known(871_763, Duration::from_micros(163_889)),
    ];

    let control = selector
        .try_reserve(&tcp_pool_observations(&control_observations))
        .expect("equal-load control must select the currently served path");
    assert_eq!((control.index, control.active_before), (1, 2));
    assert!(control.service_normalized_ordering);
    assert!(!control.path_service_tiebreak);
    drop(control.preparation);

    let data_observations = [
        TcpPoolPathService::known(12_000, Duration::from_micros(164_215)),
        TcpPoolPathService::known(871_763, Duration::from_micros(163_853)),
    ];
    let data = selector
        .try_reserve(&tcp_pool_observations(&data_observations))
        .expect("data must compare ownership against current path service");

    assert_eq!((data.index, data.active_before), (1, 3));
    assert!(data.service_normalized_ordering);
}

#[test]
fn tcp_pool_equal_normalized_load_falls_back_to_lower_raw_ownership() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(0, 2);
    selector.set_active_for_test(1, 1);
    let equal_normalized_load = [
        TcpPoolPathService::known(20, Duration::from_micros(100)),
        TcpPoolPathService::known(10, Duration::from_micros(100)),
    ];

    let selected = selector
        .try_reserve(&tcp_pool_observations(&equal_normalized_load))
        .expect("an exact normalized tie must preserve deterministic fallback");

    assert_eq!((selected.index, selected.active_before), (1, 1));
    assert!(selected.service_normalized_ordering);
    assert!(!selected.path_service_tiebreak);
}

#[test]
fn tcp_pool_known_path_service_precedes_unknown_for_equal_busy_load() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(0, 4);
    selector.set_active_for_test(1, 4);
    let path_service = [
        TcpPoolPathService::Unknown,
        TcpPoolPathService::known(10_000, Duration::from_millis(100)),
    ];

    let selected = selector
        .try_reserve(&tcp_pool_observations(&path_service))
        .expect("known current service must win an equal busy-load tie");

    assert_eq!((selected.index, selected.active_before), (1, 4));
    assert!(selected.path_service_tiebreak);
    assert!(!selected.service_normalized_ordering);
}

#[test]
fn tcp_pool_equal_or_unknown_path_service_keeps_stable_index() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(0, 8);
    selector.set_active_for_test(1, 8);
    let equal_service = [
        TcpPoolPathService::known(10_000, Duration::from_millis(100)),
        TcpPoolPathService::known(20_000, Duration::from_millis(200)),
    ];

    let equal = selector
        .try_reserve(&tcp_pool_observations(&equal_service))
        .expect("equal path service must keep stable index ordering");
    assert_eq!((equal.index, equal.active_before), (0, 8));
    assert!(!equal.path_service_tiebreak);
    assert!(equal.service_normalized_ordering);
    drop(equal);

    let unknown = selector
        .try_reserve(&[])
        .expect("missing samples must keep stable index ordering");
    assert_eq!((unknown.index, unknown.active_before), (0, 8));
    assert!(!unknown.path_service_tiebreak);
    assert!(!unknown.service_normalized_ordering);
}

#[test]
fn tcp_pool_preparing_slot_cannot_be_overtaken_before_connection_clone() {
    let selector = TcpPoolAdmission::new(2);
    selector.set_active_for_test(1, 4);

    let first = selector
        .try_reserve(&[])
        .expect("the idle primary slot must be selected first");
    let second = selector
        .try_reserve(&[])
        .expect("the preparing primary must steer the next opener to auxiliary");

    assert_eq!((first.index, first.active_before), (0, 0));
    assert_eq!((second.index, second.active_before), (1, 4));
    selector.set_active_for_test(1, 1);
    drop((first, second));
    assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
    assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn tcp_pool_busy_waiter_wakes_after_preparation_releases() {
    let selector = Arc::new(TcpPoolAdmission::new(1));
    let first = selector
        .try_reserve(&[])
        .expect("the only idle slot must be reservable");
    let sample_calls = Arc::new(AtomicU64::new(0));
    let waiter = tokio::spawn({
        let selector = selector.clone();
        let sample_calls = sample_calls.clone();
        async move {
            selector
                .reserve(|| {
                    let sample = sample_calls.fetch_add(1, Ordering::Relaxed) + 1;
                    vec![TcpPoolPathObservation::known(
                        TcpPoolTransportIdentity::new(0, 1),
                        10_000,
                        Duration::from_millis(100),
                        sample,
                    )]
                })
                .await
        }
    });
    tokio::task::yield_now().await;
    assert!(
        !waiter.is_finished(),
        "the preparing slot must not be overtaken"
    );

    drop(first);
    let second = tokio::time::timeout(Duration::from_secs(1), waiter)
        .await
        .expect("preparation release must wake one blocked selector")
        .unwrap()
        .unwrap();
    assert_eq!((second.index, second.active_before), (0, 0));
    assert_eq!(second.qualification, TcpPoolForwardQualification::Qualified);
    assert_eq!(
        second.black_holes_current,
        Some(sample_calls.load(Ordering::Relaxed))
    );
    assert!(
        sample_calls.load(Ordering::Relaxed) >= 2,
        "a waiter must refresh path and qualification after preparation releases"
    );
    drop(second);
    assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
}

#[test]
fn tcp_pool_simultaneous_first_reservations_use_distinct_slots() {
    let selector = Arc::new(TcpPoolAdmission::new(2));
    let held = Arc::new(std::sync::Barrier::new(3));
    let (selected_tx, selected_rx) = std::sync::mpsc::channel();

    let workers = (0..2)
        .map(|_| {
            let selector = selector.clone();
            let held = held.clone();
            let selected_tx = selected_tx.clone();
            std::thread::spawn(move || {
                let reservation = selector
                    .try_reserve(&[])
                    .expect("an idle two-slot pool must accept both reservations");
                selected_tx.send(reservation.index).unwrap();
                held.wait();
                drop(reservation);
            })
        })
        .collect::<Vec<_>>();
    drop(selected_tx);

    let mut selected = vec![selected_rx.recv().unwrap(), selected_rx.recv().unwrap()];
    selected.sort_unstable();
    assert_eq!(selected, vec![0, 1]);
    assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 1);
    assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 1);

    held.wait();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(selector.active_slots[0].load(Ordering::Acquire), 0);
    assert_eq!(selector.active_slots[1].load(Ordering::Acquire), 0);
}

#[test]
fn tcp_pool_reservation_fails_closed_for_empty_or_saturated_slots() {
    let empty = TcpPoolAdmission::new(0);
    assert!(matches!(
        empty.try_reserve(&[]),
        Err(TcpPoolReservationError::Empty)
    ));

    let saturated = TcpPoolAdmission::new(2);
    saturated.set_active_for_test(0, u64::MAX);
    saturated.set_active_for_test(1, u64::MAX);
    assert!(matches!(
        saturated.try_reserve(&[]),
        Err(TcpPoolReservationError::Saturated)
    ));
    assert_eq!(saturated.active_slots[0].load(Ordering::Acquire), u64::MAX);
    assert_eq!(saturated.active_slots[1].load(Ordering::Acquire), u64::MAX);
}

#[test]
fn tcp_pool_slot_stale_after_threshold() {
    assert!(!tcp_pool_slot_stale(9, 0));
    assert!(tcp_pool_slot_stale(10, 0));
    assert!(tcp_pool_slot_stale(25, 15));
    assert!(!tcp_pool_slot_stale(24, 15));
    assert!(
        !tcp_pool_slot_stale(3, 15),
        "clock skew/saturation must not mark stale"
    );
}

#[test]
fn tcp_pool_idle_auxiliary_requires_liveness_probe_before_reconnect() {
    assert_eq!(
        tcp_pool_idle_action(0, 100, 90, true),
        TcpPoolIdleAction::Reuse,
        "primary UDP/health connection keeps its existing lifecycle"
    );
    assert_eq!(
        tcp_pool_idle_action(1, 99, 90, true),
        TcpPoolIdleAction::Reuse,
        "a recently used auxiliary slot is reused"
    );
    assert_eq!(
        tcp_pool_idle_action(1, 100, 90, false),
        TcpPoolIdleAction::Reuse,
        "an active shared slot must never be probed or recycled"
    );
    assert_eq!(
        tcp_pool_idle_action(1, 100, 90, true),
        TcpPoolIdleAction::Probe,
        "elapsed idle time requests evidence; it is not failure evidence"
    );
    assert!(
        !should_probe_tcp_pool_generation(1, 10_000, 0, true, true),
        "an authenticated successor must not be recycled as stale before its triggering open"
    );
    assert!(should_probe_tcp_pool_generation(1, 100, 90, true, false));
}

#[tokio::test]
async fn tcp_pool_liveness_probe_reuses_an_acked_quinn_connection() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        let heartbeat = connection.read_datagram().await.unwrap();
        assert_eq!(&heartbeat[..], &encode_heartbeat());
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let stable_id = connection.stable_id();
    let outcome = probe_tcp_pool_connection(&connection, Duration::from_secs(1)).await;

    assert!(matches!(outcome, TcpPoolProbeOutcome::Alive { .. }));
    assert_eq!(connection.stable_id(), stable_id);
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[tokio::test]
async fn tcp_pool_successor_service_turn_requires_an_exact_acked_quinn_flight() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        connection.closed().await;
    });

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let initial_cwnd = connection.stats().path.cwnd;
    let stats = connection.successor_service_turn().await.unwrap();

    assert_eq!(stats.target_bytes, initial_cwnd);
    assert!(stats.sent_bytes >= stats.target_bytes);
    assert_eq!(stats.acked_bytes, stats.sent_bytes);
    assert_eq!(stats.lost_bytes, 0);
    assert!(
        connection.stats().path.cwnd > initial_cwnd,
        "the deliberately full service turn must not be marked application-limited before ACKs"
    );

    client_endpoint.close(0u32.into(), b"test complete");
    server_task.await.unwrap();
}

#[tokio::test]
async fn tcp_pool_replacement_handoff_publishes_the_exact_current_service() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        connection.closed().await;
    });
    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(1, connection.clone(), 1, 1, clock);
    let identity = TcpPoolTransportIdentity::new(connection.stable_id(), 1);
    let old_floor = connection.stats().path.cwnd;
    {
        let mut state = slot.state.lock().await;
        assert_eq!(
            state.current.record_forward_service_floor(old_floor),
            Some(old_floor)
        );
    }

    connection.successor_service_turn().await.unwrap();
    let current_cwnd = connection.stats().path.cwnd;
    assert!(
        current_cwnd > old_floor,
        "the exact predecessor must grow beyond its stale installation floor"
    );

    let handoff = slot
        .prepare_replacement_current_service_handoff(identity)
        .await
        .unwrap();
    assert_eq!(handoff.observed_cwnd, current_cwnd);
    assert_eq!(handoff.required_cwnd_floor, current_cwnd);
    assert_eq!(
        slot.replacement_forward_service_floor(identity)
            .await
            .unwrap(),
        Some(current_cwnd)
    );

    connection.close(0u32.into(), b"test complete");
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[tokio::test]
async fn tcp_pool_replacement_handoff_rejects_a_stale_generation() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        connection.closed().await;
    });
    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let slot = TcpPoolGenerationSlot::new(1, connection.clone(), 7, 1, Instant::now());
    let stale_identity = TcpPoolTransportIdentity::new(connection.stable_id(), 6);

    assert_eq!(
        slot.prepare_replacement_current_service_handoff(stale_identity)
            .await,
        Err(TcpPoolReplacementCurrentServiceHandoffError::StalePredecessor)
    );
    assert_eq!(
        slot.state
            .lock()
            .await
            .current
            .replacement_forward_service_floor,
        None
    );

    connection.close(0u32.into(), b"test complete");
    server_task.await.unwrap();
    client_endpoint.close(0u32.into(), b"test complete");
}

#[tokio::test]
async fn tcp_pool_successor_install_rechecks_the_latest_owned_floor() {
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
    let predecessor_identity = TcpPoolTransportIdentity::new(11, 1);
    let predecessor_activity = slot.current_activity().await;
    {
        let mut state = slot.state.lock().await;
        assert_eq!(
            state.current.record_forward_service_floor(41_301),
            Some(41_301)
        );
    }
    let insufficient = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 26_424)
        .expect("the replay successor has a positive but insufficient proof");

    assert!(matches!(
        slot.install_successor(
            predecessor_identity,
            &predecessor_activity,
            insufficient,
            Instant::now(),
        )
        .await,
        Err(TcpPoolGenerationInstallError::InsufficientForwardService)
    ));
    assert_eq!(slot.current_transport().await, 11);
    assert_eq!(slot.draining_transport().await, None);

    let sufficient = TcpPoolGeneration::new_successor(23_u64, 2, 1, clock, 7, 52_848)
        .expect("a proof above the latest owned floor must remain installable");
    slot.install_successor(
        predecessor_identity,
        &predecessor_activity,
        sufficient,
        Instant::now(),
    )
    .await
    .expect("the install CAS must accept the latest sufficient proof");
    assert_eq!(slot.current_transport().await, 23);
}

#[tokio::test]
async fn tcp_pool_successor_install_separates_readiness_from_handoff_proof() {
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
    let predecessor_identity = TcpPoolTransportIdentity::new(11, 1);
    let predecessor_activity = slot.current_activity().await;
    let inherited_handoff_floor = 3_605_919;
    let readiness_floor = 26_424;
    {
        let mut state = slot.state.lock().await;
        assert_eq!(
            state
                .current
                .record_forward_service_floor(inherited_handoff_floor),
            Some(inherited_handoff_floor)
        );
    }
    let successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, readiness_floor)
        .expect("the first exact service turn must create readiness evidence");

    slot.install_successor_with_proved_service(
        predecessor_identity,
        &predecessor_activity,
        successor,
        inherited_handoff_floor,
        Instant::now(),
    )
    .await
    .expect("a final transaction proof may satisfy a larger inherited handoff");

    let state = slot.state.lock().await;
    assert_eq!(
        state.current.successor_service_certificate,
        TcpPoolSuccessorServiceCertificate::new(
            TcpPoolTransportIdentity::new(22, 2),
            7,
            readiness_floor,
        )
    );
    assert_eq!(
        state.current.replacement_forward_service_floor,
        Some(readiness_floor),
        "the historical multi-round handoff proof must not become a lifetime replacement floor"
    );
}

#[tokio::test]
async fn tcp_pool_successor_install_rejects_proof_from_another_path() {
    let clock = Instant::now();
    let slot = TcpPoolGenerationSlot::new(1, 11_u64, 1, 1, clock);
    let predecessor_identity = TcpPoolTransportIdentity::new(11, 1);
    let predecessor_activity = slot.current_activity().await;
    let successor = TcpPoolGeneration::new_successor(22_u64, 2, 1, clock, 7, 26_424).unwrap();
    let mismatched = TcpPoolSuccessorInstallServiceProof {
        successor_identity: successor.identity(),
        path_generation: 8,
        readiness_cwnd_floor: 26_424,
        final_cwnd: 52_848,
    };

    assert!(matches!(
        slot.install_successor_with(
            predecessor_identity,
            &predecessor_activity,
            successor,
            mismatched,
            Instant::now(),
            |_, _| true,
        )
        .await,
        Err(TcpPoolGenerationInstallError::InvalidSuccessor)
    ));
    assert_eq!(slot.current_transport().await, 11);
    assert_eq!(slot.draining_transport().await, None);
}

#[tokio::test]
async fn tcp_pool_successor_install_rejects_current_path_after_proof_changed() {
    assert!(matches!(
        install_successor_with_current_service(8, 52_848, false, 7, 52_848).await,
        Err(TcpPoolGenerationInstallError::InvalidSuccessor)
    ));
}

#[tokio::test]
async fn tcp_pool_successor_install_rejects_current_cwnd_regression_after_proof() {
    assert!(matches!(
        install_successor_with_current_service(7, 41_301, false, 7, 52_848).await,
        Err(TcpPoolGenerationInstallError::InsufficientForwardService)
    ));
}

#[tokio::test]
async fn tcp_pool_successor_install_rejects_connection_closed_after_proof() {
    assert!(matches!(
        install_successor_with_current_service(7, 52_848, true, 7, 52_848).await,
        Err(TcpPoolGenerationInstallError::InvalidSuccessor)
    ));
}

#[tokio::test]
async fn tcp_pool_successor_install_accepts_matching_current_service() {
    install_successor_with_current_service(7, 61_440, false, 7, 52_848)
        .await
        .expect("a live same-path successor above its exact proof remains installable");
}

#[test]
fn tcp_pool_successor_install_proof_binds_the_exact_forward_service_turn() {
    let successor =
        TcpPoolGeneration::new_successor(22_u64, 2, 1, Instant::now(), 7, 26_424).unwrap();
    let exact = TcpPoolSuccessorForwardServiceProof {
        required_cwnd_floor: Some(52_848),
        initial_cwnd: 12_000,
        readiness_cwnd_floor: 26_424,
        final_cwnd: 52_848,
        rounds: 2,
        target_bytes: 38_424,
        sent_bytes: 38_424,
        acked_bytes: 38_424,
        lost_bytes: 0,
        path_generation: 7,
    };

    assert!(
        TcpPoolSuccessorInstallServiceProof::from_forward_service(&successor, &exact).is_some()
    );
    assert!(
        TcpPoolSuccessorInstallServiceProof::from_forward_service(
            &successor,
            &TcpPoolSuccessorForwardServiceProof {
                path_generation: 8,
                ..exact
            },
        )
        .is_none()
    );
    assert!(
        TcpPoolSuccessorInstallServiceProof::from_forward_service(
            &successor,
            &TcpPoolSuccessorForwardServiceProof {
                readiness_cwnd_floor: 24_800,
                ..exact
            },
        )
        .is_none()
    );
}

#[tokio::test]
async fn tcp_pool_successor_inherits_a_dynamic_forward_service_floor() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        connection.closed().await;
    });

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let initial_cwnd = connection.stats().path.cwnd;
    let required_floor = initial_cwnd.saturating_mul(3);
    let proof = prove_tcp_pool_successor_forward_service(
        &connection,
        Some(required_floor),
        tokio::time::Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap();

    assert!(
        proof.rounds >= 2,
        "a floor above one slow-start turn must require sequential exact ownership: {proof:?}"
    );
    assert_eq!(proof.required_cwnd_floor, Some(required_floor));
    assert!(
        proof.readiness_cwnd_floor > initial_cwnd,
        "the first exact ACK-owned turn must establish a positive readiness floor: {proof:?}"
    );
    assert!(
        proof.readiness_cwnd_floor < proof.final_cwnd,
        "a multi-round handoff must not collapse its first-turn readiness floor into the final transaction proof: {proof:?}"
    );
    assert!(proof.final_cwnd >= required_floor);
    assert!(proof.sent_bytes >= proof.target_bytes);
    assert_eq!(proof.acked_bytes, proof.sent_bytes);
    assert_eq!(proof.lost_bytes, 0);
    assert_eq!(proof.path_generation, connection.current_path_generation());

    client_endpoint.close(0u32.into(), b"test complete");
    server_task.await.unwrap();
}

#[tokio::test]
async fn tcp_pool_successor_service_turn_has_exactly_one_owner() {
    let (server_endpoint, client_endpoint, server_addr) = d16_quinn_test_endpoints();
    let server_task = tokio::spawn(async move {
        let connection = server_endpoint.accept().await.unwrap().await.unwrap();
        connection.closed().await;
    });

    let connection = client_endpoint
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .unwrap();
    let (left, right) = tokio::join!(
        connection.successor_service_turn(),
        connection.successor_service_turn()
    );

    assert!(
        matches!(
            (&left, &right),
            (Ok(_), Err(quinn::SuccessorServiceTurnError::Busy))
                | (Err(quinn::SuccessorServiceTurnError::Busy), Ok(_))
        ),
        "exactly one concurrent caller must own the bounded service turn: left={left:?} right={right:?}"
    );

    client_endpoint.close(0u32.into(), b"test complete");
    server_task.await.unwrap();
}

#[test]
fn tcp_pool_probe_outcome_reconnects_only_without_liveness_evidence() {
    assert_eq!(
        tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::Alive {
            rx_before: 10,
            rx_after: 11,
        }),
        None
    );
    assert_eq!(
        tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::Closed),
        Some("transport_closed")
    );
    assert_eq!(
        tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::SendFailed),
        Some("liveness_probe_send_failed")
    );
    assert_eq!(
        tcp_pool_probe_reconnect_reason(TcpPoolProbeOutcome::TimedOut { rx_datagrams: 10 }),
        Some("liveness_probe_timeout")
    );
}

#[test]
fn tcp_pool_open_state_records_only_success_and_invalidates_failed_auxiliary() {
    let state = TcpPoolOpenState::new();
    state.note_open_failure(1);
    assert!(state.needs_reconnect(1));
    assert_eq!(state.last_success_age_secs(20), None);

    state.note_reconnect_success();
    assert!(!state.needs_reconnect(1));
    assert_eq!(state.generation(), 2);

    state.note_open_success(25);
    assert_eq!(state.last_success_age_secs(30), Some(5));

    state.note_open_failure(0);
    assert!(
        !state.needs_reconnect(0),
        "TCP opens cannot take authority away from primary UDP health"
    );
    assert_eq!(state.last_success_age_secs(30), Some(5));
}

#[test]
fn tcp_pool_aux_failure_degrades_after_primary_connection() {
    assert_eq!(
        tcp_pool_aux_failure_action(0),
        TcpPoolAuxFailureAction::FailStartup,
        "without an authenticated primary connection startup must still fail"
    );
    assert_eq!(
        tcp_pool_aux_failure_action(1),
        TcpPoolAuxFailureAction::ContinueWithEstablished,
        "auxiliary TCP pool auth failure should not kill a usable TUIC data plane"
    );
    assert_eq!(
        tcp_pool_aux_failure_action(2),
        TcpPoolAuxFailureAction::ContinueWithEstablished,
        "later auxiliary failures should keep the already established pool"
    );
}

#[test]
fn tcp_pool_aux_retry_delay_is_short_and_bounded() {
    assert_eq!(
        tcp_pool_aux_retry_delay(0),
        Some(Duration::from_millis(250))
    );
    assert_eq!(
        tcp_pool_aux_retry_delay(1),
        Some(Duration::from_millis(500))
    );
    assert_eq!(
        tcp_pool_aux_retry_delay(2),
        None,
        "the final configured attempt should surface the original failure"
    );
    assert_eq!(tcp_pool_aux_retry_delay(99), None);
}

#[test]
fn tcp_pool_slot_lease_reserves_one_idle_owner() {
    let active = Arc::new(AtomicU64::new(0));
    {
        let (_first, first_idle) = TcpPoolSlotLease::reserve(active.clone());
        assert!(first_idle);
        assert_eq!(active.load(Ordering::Relaxed), 1);
        {
            let (_second, second_idle) = TcpPoolSlotLease::reserve(active.clone());
            assert!(!second_idle);
            assert_eq!(active.load(Ordering::Relaxed), 2);
        }
        assert_eq!(active.load(Ordering::Relaxed), 1);
    }
    assert_eq!(active.load(Ordering::Relaxed), 0);
}

#[test]
fn tcp_pool_activity_is_published_only_on_transitions() {
    let mut previous = None;

    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), Some(0));
    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), None);
    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 4), Some(4));
    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 2), Some(2));
    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 2), None);
    assert_eq!(note_tcp_pool_activity_transition(&mut previous, 0), Some(0));
}

#[test]
fn zero_rtt_defaults_off_and_parses_on() {
    assert!(!parse_zero_rtt(None)); // 默认关（quinn 0.10 限制）
    assert!(parse_zero_rtt(Some("true")));
    assert!(parse_zero_rtt(Some("1")));
    assert!(parse_zero_rtt(Some("on")));
    assert!(parse_zero_rtt(Some("YES")));
    // 其它一律关。
    assert!(!parse_zero_rtt(Some("false")));
    assert!(!parse_zero_rtt(Some("0")));
    assert!(!parse_zero_rtt(Some("maybe")));
}

#[test]
fn config_requires_core_fields() {
    let bad = |s, u, p| TuicClientConfig::from_sources(s, u, p, None, None, None).is_err();
    assert!(bad(None, Some(UUID), Some("p"))); // no server
    assert!(bad(Some("1.2.3.4:8443"), None, Some("p"))); // no uuid
    assert!(bad(Some("1.2.3.4:8443"), Some(UUID), None)); // no password
    assert!(bad(Some("1.2.3.4:8443"), Some(UUID), Some(""))); // empty password
}

#[test]
fn config_rejects_bad_server_and_uuid() {
    assert!(
        TuicClientConfig::from_sources(Some("nope"), Some(UUID), Some("p"), None, None, None)
            .is_err()
    );
    assert!(
        TuicClientConfig::from_sources(
            Some("1.2.3.4:8443"),
            Some("not-a-uuid"),
            Some("p"),
            None,
            None,
            None
        )
        .is_err()
    );
}

#[test]
fn packet_ipv4_layout() {
    let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
    assert_eq!(&p[..2], &[0x05, 0x02]); // ver + Packet
    assert_eq!(&p[2..4], &[0x00, 0x07]); // assoc-id
    assert_eq!(&p[4..6], &[0x00, 0x00]); // pkt-id
    assert_eq!(p[6], 1); // frag total
    assert_eq!(p[7], 0); // frag id
    assert_eq!(&p[8..10], &[0x00, 0x02]); // size = 2
    assert_eq!(&p[10..15], &[0x01, 1, 2, 3, 4]); // atyp ipv4 + ip
    assert_eq!(&p[15..17], &[0x00, 0x35]); // port 53
    assert_eq!(&p[17..], b"hi");
}

#[test]
fn packet_domain_roundtrips_assoc_and_data() {
    let p = encode_packet(
        9,
        &TargetAddr::DomainPort {
            host: "a.com".into(),
            port: 443,
        },
        b"q",
    );
    let (assoc, data) = decode_packet(&p).unwrap();
    assert_eq!(assoc, 9);
    assert_eq!(data, b"q");
}

#[test]
fn packet_decode_rejects_truncated() {
    assert!(decode_packet(&[0u8; 5]).is_none());
    // size says 0, atyp domain len 200 overruns:
    assert!(decode_packet(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 0, 0x00, 200]).is_none());
}

#[test]
fn datagram_pressure_signal() {
    // 出向 datagram 缓冲剩余 < 1 MTU → 视为正在背压（quinn 会静默丢最老）。
    assert!(is_datagram_pressured(0, 1200));
    assert!(is_datagram_pressured(1199, 1200));
    assert!(!is_datagram_pressured(1200, 1200)); // 边界：刚好 1 MTU 不算压力
    assert!(!is_datagram_pressured(100_000, 1200));
}

#[test]
fn format_udp_stats_includes_key_metrics() {
    let s = format_udp_stats(Some(1332), 7, 2, 180, 65535, 3, 1000, 4096);
    assert!(s.contains("stream 兜底=7"), "{s}");
    assert!(s.contains("丢弃=2"), "{s}");
    assert!(s.contains("RTT=180ms"), "{s}");
    assert!(s.contains("cwnd=65535"), "{s}");
    assert!(s.contains("3/1000"), "{s}"); // lost/sent
    assert!(s.contains("4096"), "{s}"); // send_buffer 余
}

#[test]
fn udp_relay_mode_parses() {
    assert_eq!(UdpRelayMode::parse("native"), Some(UdpRelayMode::Native));
    assert_eq!(UdpRelayMode::parse("quic"), Some(UdpRelayMode::Quic));
    assert_eq!(UdpRelayMode::parse("QUIC"), Some(UdpRelayMode::Quic));
    assert_eq!(UdpRelayMode::parse("nope"), None);
}

#[test]
fn heartbeat_layout() {
    assert_eq!(encode_heartbeat(), vec![0x05, 0x04]);
}

#[test]
fn packet_meta_single_fragment() {
    // encode_packet 产出的单帧（FRAG_TOTAL=1）→ meta 各字段就位，data 即整 payload。
    let p = encode_packet(7, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"hi");
    let m = decode_packet_meta(&p).expect("meta");
    assert_eq!(m.assoc_id, 7);
    assert_eq!(m.pkt_id, 0);
    assert_eq!(m.frag_total, 1);
    assert_eq!(m.frag_id, 0);
    assert_eq!(m.data, b"hi");
}

#[test]
fn packet_meta_non_first_fragment_skips_none_addr() {
    // 非首分片：ADDR=ATYP_NONE(0xff，跳 1 字节)，FRAG_ID=1，SIZE=本分片 chunk 长。
    // [ver type assoc(2)=9 pkt(2)=2 ftot=3 fid=1 size(2)=3 ATYP_NONE 'a' 'b' 'c']
    let buf = [
        0x05, 0x02, 0x00, 0x09, 0x00, 0x02, 0x03, 0x01, 0x00, 0x03, 0xff, b'a', b'b', b'c',
    ];
    let m = decode_packet_meta(&buf).expect("meta");
    assert_eq!(m.assoc_id, 9);
    assert_eq!(m.pkt_id, 2);
    assert_eq!(m.frag_total, 3);
    assert_eq!(m.frag_id, 1);
    assert_eq!(m.data, b"abc");
}

#[test]
fn packet_meta_rejects_truncated() {
    assert!(decode_packet_meta(&[0u8; 5]).is_none());
    // size 说 200 但 buffer 不够 → None（不 panic）。
    assert!(
        decode_packet_meta(&[0x05, 0x02, 0, 7, 0, 0, 1, 0, 0, 200, 0x01, 1, 2, 3, 4]).is_none()
    );
}

#[test]
fn decode_packet_delegates_to_meta() {
    // decode_packet 仍取 (assoc, data)，与 meta 一致（薄包装零回归）。
    let p = encode_packet(3, &TargetAddr::IpPort("1.2.3.4:53".parse().unwrap()), b"xy");
    assert_eq!(decode_packet(&p), Some((3, &b"xy"[..])));
}

#[test]
fn heartbeat_only_while_udp_active() {
    let w = 60;
    // 从未发过 UDP(last=0)→ 不发,纯 TCP 会话靠 QUIC keepalive 保活。
    assert!(!should_send_heartbeat(0, 100, w));
    // 活跃窗口内(含同刻与边界 ==window)→ 发。
    assert!(should_send_heartbeat(100, 100, w));
    assert!(should_send_heartbeat(100, 130, w));
    assert!(should_send_heartbeat(100, 160, w)); // 边界 now-last==window
    // 超出活跃窗口 → 停发(此时也无活跃 flow 需要保活)。
    assert!(!should_send_heartbeat(100, 161, w));
}

fn tuple(p: u16) -> FourTuple {
    FourTuple {
        src_ip: std::net::Ipv4Addr::new(10, 0, 0, 1),
        src_port: p,
        dst_ip: std::net::Ipv4Addr::new(198, 18, 0, 5),
        dst_port: 443,
    }
}

#[test]
fn assoc_intern_stable_and_unique() {
    let mut t = AssocTable::new();
    let a = t.intern(tuple(1000));
    assert_eq!(a, t.intern(tuple(1000)));
    assert_ne!(a, t.intern(tuple(1001)));
}

#[test]
fn assoc_intern_skips_live_id_on_wraparound() {
    // u16 回绕后 next_id 可能落到仍在册的 id 上：intern 必须跳过，绝不覆盖活跃 flow。
    let mut t = AssocTable::with_cap(8);
    let keep = t.intern(tuple(1)); // 一条长寿命 flow 占住 id=keep
    t.next_id = keep; // 把分配游标强行推回 keep（模拟回绕撞上活跃 id）
    let other = t.intern(tuple(2));
    assert_ne!(other, keep, "intern 覆盖了仍在册的 flow");
    assert!(t.resolve(keep).is_some(), "长寿命 flow 被覆盖丢失");
    assert!(t.resolve(other).is_some());
    // 两条 flow 各自映射独立，未发生 tuple_to_id 泄漏/串号。
    assert_eq!(t.intern(tuple(1)), keep);
    assert_eq!(t.intern(tuple(2)), other);
}

#[test]
fn assoc_contains_tracks_membership() {
    let mut t = AssocTable::new();
    assert!(!t.contains(&tuple(1000)));
    t.intern(tuple(1000));
    assert!(t.contains(&tuple(1000)));
    assert!(!t.contains(&tuple(1001)));
}

#[test]
fn assoc_resolve_endpoints_and_sweep() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    let e = t.resolve(id).expect("entry");
    assert_eq!(
        e.app_endpoint(),
        (std::net::Ipv4Addr::new(10, 0, 0, 1), 1000)
    );
    assert_eq!(
        e.target_src(),
        (std::net::Ipv4Addr::new(198, 18, 0, 5), 443)
    );
    t.sweep(61, 60);
    assert!(t.resolve(id).is_none());
}

/// 刀2：sweep 回收带 fake-IP 的 assoc 时，直接返回该 fake-IP（review #8：自包含返回）。
#[test]
fn assoc_sweep_reclaims_fake_ip() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    t.set_fake_ip(id, Ipv4Addr::new(198, 18, 0, 5));
    t.touch(id, 0);
    // 未过期：不回收，返回空。
    assert!(t.sweep(30, 60).is_empty());
    // 过期：回收，返回含该 fake-IP。
    assert_eq!(t.sweep(61, 60), vec![Ipv4Addr::new(198, 18, 0, 5)]);
}

/// 刀2：LRU 驱逐带 fake-IP 的 assoc 时也累积 reclaimed（intern 到 cap 触发 evict）。
#[test]
fn assoc_evict_reclaims_fake_ip() {
    let mut t = AssocTable::with_cap(1);
    let id1 = t.intern(tuple(1));
    t.set_fake_ip(id1, Ipv4Addr::new(198, 18, 0, 9));
    // 再 intern 一条 → cap=1 触发 evict id1。
    let _id2 = t.intern(tuple(2));
    assert_eq!(
        t.take_reclaimed_fake_ips(),
        vec![Ipv4Addr::new(198, 18, 0, 9)]
    );
}

#[test]
fn assoc_touch_keeps_alive() {
    let mut t = AssocTable::new();
    let id = t.intern(tuple(1000));
    t.touch(id, 50);
    t.sweep(100, 60);
    assert!(t.resolve(id).is_some());
}

#[test]
fn assoc_lru_evicts_at_cap() {
    let mut t = AssocTable::with_cap(2);
    let a = t.intern(tuple(1));
    let _b = t.intern(tuple(2));
    let _c = t.intern(tuple(3));
    assert!(t.resolve(a).is_none());
    assert_eq!(t.len(), 2);
}

#[test]
fn quic_config_builds_with_h3_alpn() {
    // TUIC 上游的 TLS 配置(自定义 ALPN)能构建 —— connect 的真验证在互通 e2e(Task 6)。
    assert!(
        crate::quic::client_quic_config_alpn(
            "certs/dev/ca-cert.pem",
            vec![b"h3".to_vec()],
            crate::quic::CcChoice::Bbr,
            crate::quic::MtuPolicy::Default,
            crate::quic::QuicGsoPolicy::Enabled,
            crate::quic::QuicPacingPolicy::QuinnDefault,
        )
        .is_ok()
    );
}

#[test]
fn config_debug_redacts_credentials() {
    let c = TuicClientConfig::from_sources(
        Some("1.2.3.4:8443"),
        Some(UUID),
        Some("secret123"),
        None,
        None,
        None,
    )
    .unwrap();
    let s = format!("{c:?}");
    assert!(!s.contains("secret123"), "password leaked: {s}");
    assert!(!s.contains("550e8400"), "uuid leaked: {s}");
    assert!(s.contains("redacted"));
}
