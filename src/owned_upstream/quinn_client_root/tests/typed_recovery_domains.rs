use super::*;
use crate::resumable::ProbeSequence;

struct CurrentAQueued {
    key: RootCurrentARecoveryKey,
    owner: Box<u8>,
}

impl RootCurrentARecoveryQueuedOwner for CurrentAQueued {
    fn current_a_recovery_key(&self) -> RootCurrentARecoveryKey {
        self.key
    }
}

struct CurrentADeadline {
    owner: Box<u8>,
}

struct StandbyBWake {
    key: RootStandbyBRecoveryKey,
    owner: Box<u8>,
}

impl RootStandbyBRecoveryWakeOwner for StandbyBWake {
    fn standby_b_recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.key
    }
}

fn generation(value: u64) -> LegGeneration {
    LegGeneration::new(value).expect("nonzero test generation")
}

fn sequence(value: u64) -> ProbeSequence {
    ProbeSequence::new(value).expect("nonzero test probe sequence")
}

#[test]
fn equal_scalar_b_correlations_from_distinct_incarnations_cannot_alias() {
    let mut slots = RootRecoveryTimerSlots::<CurrentAQueued, CurrentADeadline, StandbyBWake>::new();
    let installed_key = RootStandbyBRecoveryKey::mint_for_test(generation(7), sequence(11));
    let reincarnated_key = RootStandbyBRecoveryKey::mint_for_test(generation(7), sequence(11));
    let wake = StandbyBWake {
        key: installed_key.clone(),
        owner: Box::new(0xb7),
    };
    let owner = wake.owner.as_ref() as *const u8;

    slots
        .try_install_standby_b_wake(wake)
        .expect("the exact first-incarnation wake fits its named domain");

    assert!(matches!(
        slots.take_standby_b_wake_by_exact_key(&reincarnated_key),
        Err(RootStandbyBRecoveryWakeTakeFailureKind::KeyMismatch),
    ));
    let retained = slots
        .take_standby_b_wake_by_exact_key(&installed_key)
        .expect("the original physical incarnation still owns the wake");
    assert_eq!(retained.owner.as_ref() as *const u8, owner);
    assert!(slots.is_empty());
}

#[test]
fn equal_scalar_a_and_b_correlations_are_independent_named_domains() {
    let mut slots = RootRecoveryTimerSlots::<CurrentAQueued, CurrentADeadline, StandbyBWake>::new();
    let current_key = RootCurrentARecoveryKey {
        generation: generation(7),
        sequence: sequence(11),
    };
    let standby_key = RootStandbyBRecoveryKey::mint_for_test(generation(7), sequence(11));
    let current = CurrentAQueued {
        key: current_key,
        owner: Box::new(0xa1),
    };
    let current_owner = current.owner.as_ref() as *const u8;
    let standby = StandbyBWake {
        key: standby_key.clone(),
        owner: Box::new(0xb1),
    };
    let standby_owner = standby.owner.as_ref() as *const u8;

    slots
        .try_install_current_a_queued(current, Duration::from_millis(31))
        .expect("the one exact A scheduler owner fits its named domain");
    slots
        .try_install_standby_b_wake(standby)
        .expect("the one non-authoritative B wake fits its named domain");

    assert_eq!(slots.len(), ROOT_RECOVERY_TIMER_SLOTS);
    assert_eq!(slots.live_inventory().current_a_queued(), 1);
    assert_eq!(slots.live_inventory().standby_b_wakes(), 1);

    let current =
        slots.transact_current_a_scheduler_by_exact_key(current_key, |scheduler| match scheduler {
            RootRecoverySchedulerAuthority::Queued(queued) => {
                assert_eq!(queued.owner.as_ref() as *const u8, current_owner);
                Ok::<_, (RootRecoverySchedulerAuthority<_, _>, ())>(())
            }
            RootRecoverySchedulerAuthority::Armed(deadline) => {
                Err((RootRecoverySchedulerAuthority::Armed(deadline), ()))
            }
        });
    assert_eq!(current, Ok(()));
    assert_eq!(slots.len(), 1);

    let standby = slots
        .take_standby_b_wake_by_exact_key(&standby_key)
        .expect("the equal scalar B key addresses only the B wake domain");
    assert_eq!(standby.owner.as_ref() as *const u8, standby_owner);
    assert!(slots.is_empty());
}

#[test]
fn third_obligation_in_either_named_domain_is_returned_exactly() {
    let mut slots = RootRecoveryTimerSlots::<CurrentAQueued, CurrentADeadline, StandbyBWake>::new();
    slots
        .try_install_current_a_queued(
            CurrentAQueued {
                key: RootCurrentARecoveryKey {
                    generation: generation(8),
                    sequence: sequence(1),
                },
                owner: Box::new(0xa2),
            },
            Duration::from_millis(41),
        )
        .expect("the one current-A obligation fits");
    slots
        .try_install_standby_b_wake(StandbyBWake {
            key: RootStandbyBRecoveryKey::mint_for_test(generation(8), sequence(1)),
            owner: Box::new(0xb2),
        })
        .expect("the one standby-B wake fits independently");

    let extra_a = CurrentAQueued {
        key: RootCurrentARecoveryKey {
            generation: generation(8),
            sequence: sequence(2),
        },
        owner: Box::new(0xa3),
    };
    let extra_a_owner = extra_a.owner.as_ref() as *const u8;
    let extra_a = slots
        .try_install_current_a_queued(extra_a, Duration::from_millis(42))
        .expect_err("a second current-A obligation cannot borrow B capacity");
    assert_eq!(
        extra_a.kind(),
        RootRecoveryTimerInstallFailureKind::Capacity
    );
    let (extra_a, timeout) = extra_a.into_parts();
    assert_eq!(extra_a.owner.as_ref() as *const u8, extra_a_owner);
    assert_eq!(timeout, Duration::from_millis(42));

    let extra_b = StandbyBWake {
        key: RootStandbyBRecoveryKey::mint_for_test(generation(8), sequence(2)),
        owner: Box::new(0xb3),
    };
    let extra_b_owner = extra_b.owner.as_ref() as *const u8;
    let extra_b = slots
        .try_install_standby_b_wake(extra_b)
        .expect_err("a second standby-B wake cannot borrow A capacity");
    assert_eq!(
        extra_b.kind(),
        RootStandbyBRecoveryWakeInstallFailureKind::Capacity
    );
    let extra_b = extra_b.into_wake();
    assert_eq!(extra_b.owner.as_ref() as *const u8, extra_b_owner);

    assert_eq!(slots.len(), ROOT_RECOVERY_TIMER_SLOTS);
    assert_eq!(slots.live_inventory().current_a_queued(), 1);
    assert_eq!(slots.live_inventory().standby_b_wakes(), 1);
}
