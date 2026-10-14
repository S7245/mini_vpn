use super::*;
use crate::resumable::LegGeneration;

mod progress_exact_zero;

fn empty_drain_inventory() -> OwnerOutputDrainInventory {
    OwnerOutputDrain::new(1, 1)
        .expect("one-slot output drain")
        .inventory()
        .expect("new drain has an exact empty inventory")
}

fn empty_lifecycle_inventory() -> OwnerOutputLifecycleInventory {
    OwnerOutputLifecyclePort::new().inventory()
}

fn root_inventory_from_sources(
    drain: Option<OwnerOutputDrainInventory>,
    drain_ledgers: u8,
    drain_audit_failed: bool,
    lifecycle: OwnerOutputLifecycleInventory,
    rejected_page: Option<OwnerOutputAdmissionInventory>,
) -> QuinnOwnerOutputRootInventory {
    QuinnOwnerOutputRootInventory::from_sources(
        drain,
        drain_ledgers,
        drain_audit_failed,
        0,
        0,
        0,
        0,
        true,
        false,
        lifecycle,
        rejected_page,
    )
}

fn live_output_parts(
    output: &mut QuinnOwnerOutputSubroot,
) -> (&mut OwnerOutputDrain, &mut QuinnOwnerOutputPhase) {
    let QuinnOwnerOutputLedger { drain, status } = &mut output.ledger;
    let QuinnOwnerOutputStatus::Live { phase } = status else {
        panic!("output subroot is not live");
    };
    (drain, phase)
}

#[test]
fn output_root_zero_requires_one_audited_drain_and_no_external_slots() {
    let empty = empty_drain_inventory();
    let lifecycle = empty_lifecycle_inventory();
    let inventory = root_inventory_from_sources(Some(empty), 1, false, lifecycle, None);

    assert!(inventory.is_output_exact_zero());
    assert_eq!(inventory.drain(), Some(empty));
    assert_eq!(inventory.drain_ledgers(), 1);
    assert!(!inventory.drain_audit_failed());

    assert!(
        !root_inventory_from_sources(Some(empty), 0, false, lifecycle, None).is_output_exact_zero()
    );
    assert!(
        !root_inventory_from_sources(Some(empty), 2, false, lifecycle, None).is_output_exact_zero()
    );
    assert!(!root_inventory_from_sources(None, 1, true, lifecycle, None).is_output_exact_zero());
}

#[test]
fn preflighted_empty_hard_terminal_stop_commits_fatal_infallibly() {
    let mut output = QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
    let audited = output
        .preflight_hard_terminal_exact_zero()
        .expect("a vacant output root is an exact-zero terminal source");

    assert!(output.commit_preflighted_hard_terminal_exact_zero(audited));
    assert!(matches!(
        output.stop(OwnerOutputDrainStopReason::Shutdown),
        QuinnOwnerOutputStopDrive::AlreadyStopped {
            reason: OwnerOutputDrainStopReason::Fatal,
            ..
        }
    ));
    assert!(output.inventory().is_output_exact_zero());
}

#[test]
fn lifecycle_and_rejected_page_are_part_of_the_output_root_witness() {
    let empty = empty_drain_inventory();
    let mut lifecycle_port = OwnerOutputLifecyclePort::new();
    lifecycle_port
        .try_install(OwnerOutputLifecycleAction::session_expired_for_test())
        .expect("empty lifecycle port accepts its one exact action");
    let lifecycle =
        root_inventory_from_sources(Some(empty), 1, false, lifecycle_port.inventory(), None);
    assert_eq!(lifecycle.installed_lifecycle_actions(), 1);
    assert!(!lifecycle.is_output_exact_zero());

    let mut drain = OwnerOutputDrain::new(1, 1).expect("one-slot output drain");
    let failure = drain
        .admit_outputs(
            vec![
                OwnerTargetOutput::LegActivated {
                    generation: LegGeneration::new(1).expect("nonzero generation"),
                },
                OwnerTargetOutput::SessionExpired,
            ],
            true,
        )
        .expect_err("two outputs cannot enter one slot");
    let rejected = failure.inventory();
    let inventory = root_inventory_from_sources(
        Some(empty),
        1,
        false,
        empty_lifecycle_inventory(),
        Some(rejected),
    );

    assert_eq!(inventory.rejected_page(), Some(rejected));
    assert_eq!(rejected.pending_outputs(), 2);
    assert!(rejected.page_open());
    assert!(rejected.continuation_needed());
    assert!(!inventory.is_output_exact_zero());
}

#[test]
fn audited_terminal_lifecycle_discard_closes_the_output_subroot() {
    let stopped = OwnerOutputDrain::new(1, 1)
        .expect("one-slot output drain")
        .stop(OwnerOutputDrainStopReason::Shutdown)
        .expect("empty drain has an exact stopped ledger");
    let mut lifecycle = OwnerOutputLifecyclePort::new();
    lifecycle
        .try_install(OwnerOutputLifecycleAction::session_expired_for_test())
        .expect("empty lifecycle port accepts the exact pending action");

    let before = root_inventory_from_sources(
        Some(stopped.inventory()),
        1,
        false,
        lifecycle.inventory(),
        None,
    );
    assert!(!before.is_output_exact_zero());

    assert_eq!(
        lifecycle.transact(
            OwnerOutputLifecycleSettlement::Discard {
                reason: stopped.reason(),
            },
            |transaction| {
                assert_eq!(
                    transaction.settlement(),
                    OwnerOutputLifecycleSettlement::Discard {
                        reason: OwnerOutputDrainStopReason::Shutdown,
                    },
                );
                transaction.discard()
            },
        ),
        OwnerOutputLifecycleDrive::Committed {
            settlement: OwnerOutputLifecycleSettlement::Discard {
                reason: OwnerOutputDrainStopReason::Shutdown,
            },
        },
    );
    let after = root_inventory_from_sources(
        Some(stopped.inventory()),
        1,
        false,
        lifecycle.inventory(),
        None,
    );
    assert!(after.is_output_exact_zero());
    assert_eq!(after.lifecycle().shutdown_discards(), 1);
}

#[test]
fn stop_consumes_only_a_completed_page_boundary_and_retains_a_continuation() {
    let mut completed_drain = OwnerOutputDrain::new(1, 1).expect("one-slot output drain");
    let mut completed_phase = QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(false));
    settle_output_page_boundary_for_stop(&mut completed_phase, &mut completed_drain)
        .expect("a completed page boundary has no residual work");
    assert_eq!(completed_phase, QuinnOwnerOutputPhase::Vacant);
    assert!(completed_drain.inventory().unwrap().is_exact_zero());

    let mut continued_drain = OwnerOutputDrain::new(1, 1).expect("one-slot output drain");
    let mut continued_phase = QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(true));
    settle_output_page_boundary_for_stop(&mut continued_phase, &mut continued_drain)
        .expect("the continuation marker fits its checked one-slot page");
    assert_eq!(continued_phase, QuinnOwnerOutputPhase::Page);
    let inventory = continued_drain.inventory().unwrap();
    assert!(inventory.page_open());
    assert!(inventory.continuation_needed());
    assert!(!inventory.is_exact_zero());
}

#[test]
fn output_phase_and_drain_are_a_bidirectional_exact_zero_invariant() {
    for phase in [
        QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(false)),
        QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(true)),
    ] {
        let mut output = QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
        let (_, installed) = live_output_parts(&mut output);
        *installed = phase;

        let inventory = output.inventory();
        assert!(inventory.phase_drain_consistent());
        assert_eq!(inventory.phase_markers(), 1);
        assert_eq!(
            inventory.continuation_markers(),
            u8::from(
                matches!(phase, QuinnOwnerOutputPhase::Drained(page) if page.needs_continue())
            ),
        );
        assert!(!inventory.is_output_exact_zero());
    }

    let vacant = QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
    assert!(vacant.inventory().phase_drain_consistent());
    assert!(vacant.inventory().is_output_exact_zero());

    let mut page = QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
    let (drain, phase) = live_output_parts(&mut page);
    drain
        .admit_outputs(Vec::new(), false)
        .expect("empty bounded page is a real drain marker");
    *phase = QuinnOwnerOutputPhase::Page;
    assert!(page.inventory().phase_drain_consistent());
    assert!(!page.inventory().is_output_exact_zero());

    let mut page_without_drain =
        QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
    let (_, phase) = live_output_parts(&mut page_without_drain);
    *phase = QuinnOwnerOutputPhase::Page;
    assert!(!page_without_drain.inventory().phase_drain_consistent());
    assert!(!page_without_drain.inventory().is_output_exact_zero());

    let mut drain_without_page =
        QuinnOwnerOutputSubroot::new(1, 1).expect("bounded output subroot");
    let (drain, _) = live_output_parts(&mut drain_without_page);
    drain
        .admit_outputs(Vec::new(), false)
        .expect("empty bounded page is a real drain marker");
    assert!(!drain_without_page.inventory().phase_drain_consistent());
    assert!(!drain_without_page.inventory().is_output_exact_zero());
}

#[test]
fn invalid_stop_preflight_enters_sticky_quarantine_without_minting_a_page() {
    let mut output = QuinnOwnerOutputSubroot::new(1, 1).expect("one-slot output subroot");
    let (drain, phase) = live_output_parts(&mut output);
    drain
        .admit_outputs(Vec::new(), true)
        .expect("the drain owns one continuation page");
    *phase = QuinnOwnerOutputPhase::Drained(OwnerOutputPageDrained::new(true));
    let before = output
        .inventory()
        .drain()
        .expect("the invalid live pair still has an auditable exact drain");

    assert!(matches!(
        output.stop(OwnerOutputDrainStopReason::Fatal),
        QuinnOwnerOutputStopDrive::CompositionInvariant { .. },
    ));
    let quarantined = output.inventory();
    assert!(quarantined.quarantined());
    assert!(quarantined.drain_audit_failed());
    assert_eq!(quarantined.drain(), Some(before));
    assert!(quarantined.rejected_page().is_none());
    assert!(!quarantined.is_output_exact_zero());
    assert!(matches!(
        output.ledger.status,
        QuinnOwnerOutputStatus::Quarantined {
            first_stop_reason: Some(OwnerOutputDrainStopReason::Fatal),
            ..
        }
    ));

    assert!(matches!(
        output.stop(OwnerOutputDrainStopReason::Shutdown),
        QuinnOwnerOutputStopDrive::CompositionInvariant { .. },
    ));
    assert!(matches!(
        output.ledger.status,
        QuinnOwnerOutputStatus::Quarantined {
            first_stop_reason: Some(OwnerOutputDrainStopReason::Fatal),
            ..
        }
    ));
    assert_eq!(output.inventory().drain(), Some(before));
}
