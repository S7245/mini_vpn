//! Characterization and capacity tracers for the bounded owner progress scan.
//!
//! This test child owns scan-width/turn arithmetic only. It does not own wire,
//! byte, timer, physical-root, or controller/output settlement tests.

use super::*;

fn owner_scan_capacity(
    max_live_flows: usize,
    max_terminal_tombstones: usize,
    max_episodes: usize,
) -> Result<R7OwnerScanCapacity, R7CapacityError> {
    let progress_capacity = OwnerProgressCapacity::checked(max_live_flows, max_terminal_tombstones)
        .map_err(R7CapacityError::OwnerProgressCapacity)?;
    R7OwnerScanCapacity::derive(
        progress_capacity,
        R7ScenarioEpisodeBounds::new(max_episodes, 1, 1),
    )
}

#[test]
fn r7_owner_scan_capacity_uses_two_directions_ceiling_and_bounded_episodes() {
    let capacity = owner_scan_capacity(65, 0, 3).expect("checked R7 owner scan capacity");

    assert_eq!(owner_reverse_scan_turns(128), 4);
    assert_eq!(owner_reverse_scan_turns(129), 5);
    assert_eq!(capacity.direction_entries, 130);
    assert_eq!(capacity.turns_per_attempt, 5);
    assert_eq!(capacity.max_attempts, 3);
    assert_eq!(capacity.max_turns, 15);
}

#[test]
fn r7_owner_scan_capacity_rejects_zero_flow_or_episode_domains_exactly() {
    assert_eq!(
        owner_scan_capacity(0, 0, 1),
        Err(R7CapacityError::ZeroMaxFlows)
    );
    assert_eq!(
        owner_scan_capacity(1, 0, 0),
        Err(R7CapacityError::ZeroCompletedScanEpisodes)
    );
}

#[test]
fn r7_owner_scan_capacity_rejects_direction_and_episode_overflow_exactly() {
    assert_eq!(
        owner_scan_capacity(usize::MAX, 0, 1),
        Err(R7CapacityError::OwnerProgressCapacity(
            OwnerProgressCapacityError::DirectionalEntryCapacityOverflow,
        ))
    );

    let max_flows = usize::MAX / 2;
    let turns_per_attempt = owner_reverse_scan_turns(max_flows * 2);
    assert_eq!(
        owner_scan_capacity(max_flows, 0, usize::MAX),
        Err(R7CapacityError::ScanAttemptTurnsOverflow {
            turns_per_attempt,
            max_attempts: usize::MAX,
        })
    );
}

#[test]
fn r7_manifest_scan_capacity_uses_live_flow_baseline_characterization() {
    let storage = replay_storage(1);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 3),
        R7ScenarioEpisodeBounds::new(3, 2, 3),
        wire_faults(2),
        manifest_byte_plan(storage, 1_024, 2_048),
    )
    .expect("checked necessary-only R7 capacity manifest");

    assert_eq!(manifest.owner_scan.direction_entries, 8);
    assert_eq!(manifest.owner_scan.turns_per_attempt, 1);
    assert_eq!(manifest.owner_scan.max_turns, 3);
}

#[test]
fn r7_manifest_flow_and_terminal_bounds_do_not_borrow_characterization() {
    let storage = replay_storage(4);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 9),
        R7ScenarioEpisodeBounds::new(2, 3, 4),
        wire_faults(2),
        manifest_byte_plan(storage, 1, 1),
    )
    .expect("independent checked scenario bounds");

    assert_eq!(manifest.owner_scan.direction_entries, 26);
    assert_eq!(manifest.owner_scan.max_attempts, 2);
    assert_eq!(
        manifest.owner_scan.direction_entries - 8,
        manifest.sessions.max_terminal_tombstones * 2
    );
}

#[test]
fn r7_manifest_owner_scan_width_includes_terminal_grace_capacity() {
    let storage = replay_storage(16);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 1),
        R7ScenarioEpisodeBounds::new(3, 2, 3),
        wire_faults(2),
        manifest_byte_plan(storage, 1, 1),
    )
    .expect("checked necessary-only R7 capacity manifest");

    assert_eq!(manifest.owner_scan.direction_entries, 34);
    assert_eq!(manifest.owner_scan.turns_per_attempt, 2);
    assert_eq!(manifest.owner_scan.max_turns, 6);
}

#[test]
fn r7_manifest_rejects_tracked_flow_addition_overflow_exactly() {
    let storage = replay_storage(usize::MAX);

    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            session_witness(storage, 1),
            R7ScenarioEpisodeBounds::new(1, 1, 1),
            wire_faults(1),
            manifest_byte_plan(storage, 1, 1),
        ),
        Err(R7CapacityError::OwnerProgressCapacity(
            OwnerProgressCapacityError::TrackedFlowCapacityOverflow,
        ))
    );
}

#[test]
fn r7_manifest_scan_attempts_include_revision_invalidated_restarts() {
    let storage = replay_storage(16);
    let episodes = R7ScenarioEpisodeBounds::new(3, 2, 3).with_owner_scan_revision_resets(2);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 1),
        episodes,
        wire_faults(2),
        manifest_byte_plan(storage, 1, 1),
    )
    .expect("checked necessary-only R7 capacity manifest");

    assert_eq!(manifest.owner_scan.turns_per_attempt, 2);
    assert_eq!(manifest.owner_scan.max_attempts, 5);
    assert_eq!(manifest.owner_scan.max_turns, 10);
}

#[test]
fn r7_manifest_rejects_scan_attempt_addition_overflow_exactly() {
    let storage = replay_storage(1);

    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            session_witness(storage, 1),
            R7ScenarioEpisodeBounds::new(1, 1, 1).with_owner_scan_revision_resets(usize::MAX),
            wire_faults(1),
            manifest_byte_plan(storage, 1, 1),
        ),
        Err(R7CapacityError::ScanAttemptsOverflow)
    );
}

#[test]
fn r7_manifest_accepts_one_turn_exact_usize_max_scan_boundary() {
    let storage = replay_storage(1);
    let manifest = R7NoOracleCapacityManifest::derive(
        session_witness(storage, 1),
        R7ScenarioEpisodeBounds::new(1, 1, 1).with_owner_scan_revision_resets(usize::MAX - 1),
        wire_faults(1),
        manifest_byte_plan(storage, 1, 1),
    )
    .expect("exact usize::MAX one-turn scan capacity");

    assert_eq!(manifest.owner_scan.direction_entries, 4);
    assert_eq!(manifest.owner_scan.turns_per_attempt, 1);
    assert_eq!(manifest.owner_scan.max_attempts, usize::MAX);
    assert_eq!(manifest.owner_scan.max_turns, usize::MAX);
}

#[test]
fn r7_manifest_rejects_revision_resets_without_completed_scan() {
    let storage = replay_storage(1);

    assert_eq!(
        R7NoOracleCapacityManifest::derive(
            session_witness(storage, 1),
            R7ScenarioEpisodeBounds::new(0, 1, 1).with_owner_scan_revision_resets(1),
            wire_faults(1),
            manifest_byte_plan(storage, 1, 1),
        ),
        Err(R7CapacityError::ZeroCompletedScanEpisodes)
    );
}
