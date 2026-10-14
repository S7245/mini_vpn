use super::*;
use crate::resumable::ProbeSequence;

struct SchedulerQueued {
    key: RootCurrentARecoveryKey,
    owner: Box<u8>,
}

impl RootCurrentARecoveryQueuedOwner for SchedulerQueued {
    fn current_a_recovery_key(&self) -> RootCurrentARecoveryKey {
        self.key
    }
}

struct SchedulerDeadline {
    owner: Box<u8>,
}

struct SchedulerStandbyWake {
    key: RootStandbyBRecoveryKey,
    owner: Box<u8>,
}

impl RootStandbyBRecoveryWakeOwner for SchedulerStandbyWake {
    fn standby_b_recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.key
    }
}

fn key(generation: u64, sequence: u64) -> RootCurrentARecoveryKey {
    RootCurrentARecoveryKey {
        generation: LegGeneration::new(generation).expect("nonzero generation"),
        sequence: ProbeSequence::new(sequence).expect("nonzero sequence"),
    }
}

fn standby_key(generation: u64, sequence: u64) -> RootStandbyBRecoveryKey {
    RootStandbyBRecoveryKey::mint_for_test(
        LegGeneration::new(generation).expect("nonzero generation"),
        ProbeSequence::new(sequence).expect("nonzero sequence"),
    )
}

fn install_queued<W>(
    slots: &mut RootRecoveryTimerSlots<SchedulerQueued, SchedulerDeadline, W>,
    key: RootCurrentARecoveryKey,
    marker: u8,
    timeout: Duration,
) -> *const u8 {
    let queued = SchedulerQueued {
        key,
        owner: Box::new(marker),
    };
    let owner = queued.owner.as_ref() as *const u8;
    slots
        .try_install_current_a_queued(queued, timeout)
        .expect("one exact scheduler slot fits");
    owner
}

#[test]
fn queued_and_armed_ack_transactions_each_consume_the_exact_correlated_owner() {
    let mut slots = RootRecoveryTimerSlots::<SchedulerQueued, SchedulerDeadline>::new();
    let queued_key = key(11, 1);
    let armed_key = key(11, 2);
    let queued_owner = install_queued(&mut slots, queued_key, 0x11, Duration::from_millis(31));

    let queued_result = slots
        .transact_current_a_scheduler_by_exact_key(queued_key, |scheduler| match scheduler {
            RootRecoverySchedulerAuthority::Queued(queued) => {
                assert_eq!(queued.owner.as_ref() as *const u8, queued_owner);
                Ok::<_, (RootRecoverySchedulerAuthority<_, _>, &'static str)>("queued")
            }
            RootRecoverySchedulerAuthority::Armed(deadline) => Err((
                RootRecoverySchedulerAuthority::Armed(deadline),
                "wrong phase",
            )),
        })
        .expect("the exact queued ACK transaction succeeds");
    assert_eq!(queued_result, "queued");
    assert!(slots.is_empty());

    let armed_owner = install_queued(&mut slots, armed_key, 0x12, Duration::from_millis(32));
    slots
        .try_arm_current_a_queued(armed_key, |queued, timeout| {
            assert_eq!(timeout, Duration::from_millis(32));
            Ok::<_, (SchedulerQueued, ())>(SchedulerDeadline {
                owner: queued.owner,
            })
        })
        .expect("the next sole current-A scheduler becomes armed");

    let armed_result = slots
        .transact_current_a_scheduler_by_exact_key(armed_key, |scheduler| match scheduler {
            RootRecoverySchedulerAuthority::Armed(deadline) => {
                assert_eq!(deadline.owner.as_ref() as *const u8, armed_owner);
                Ok::<_, (RootRecoverySchedulerAuthority<_, _>, &'static str)>("armed")
            }
            RootRecoverySchedulerAuthority::Queued(queued) => Err((
                RootRecoverySchedulerAuthority::Queued(queued),
                "wrong phase",
            )),
        })
        .expect("the exact armed ACK transaction succeeds");
    assert_eq!(armed_result, "armed");
    assert!(slots.is_empty());
}

#[test]
fn wrong_correlation_and_action_rejection_restore_exact_owner_and_adapter_metadata() {
    let mut slots = RootRecoveryTimerSlots::<SchedulerQueued, SchedulerDeadline>::new();
    let exact_key = key(12, 7);
    let owner = install_queued(&mut slots, exact_key, 0x27, Duration::from_millis(47));
    let mut wrong_action_called = false;
    let wrong = slots
        .transact_current_a_scheduler_by_exact_key(key(12, 8), |scheduler| {
            wrong_action_called = true;
            Err::<(), _>((scheduler, "must not run"))
        })
        .expect_err("a wrong ACK correlation cannot take another authority");
    assert_eq!(
        wrong,
        RootRecoverySchedulerTransactionError::Take(RootRecoveryTimerTakeFailureKind::KeyMismatch)
    );
    assert!(!wrong_action_called);

    let rejected = slots
        .transact_current_a_scheduler_by_exact_key(exact_key, |scheduler| {
            Err::<(), _>((scheduler, "controller rejected"))
        })
        .expect_err("controller rejection restores before returning");
    assert_eq!(
        rejected,
        RootRecoverySchedulerTransactionError::Action("controller rejected")
    );
    let retained = slots
        .take_current_a_by_exact_key(exact_key)
        .expect("the rejected exact scheduler remains in its original slot");
    match retained.authority() {
        RootRecoveryTimerAuthority::Queued {
            queued,
            timeout,
            publication_pending,
            ..
        } => {
            assert_eq!(queued.owner.as_ref() as *const u8, owner);
            assert_eq!(*timeout, Duration::from_millis(47));
            assert!(!publication_pending);
        }
        RootRecoveryTimerAuthority::Armed { .. } => panic!("queued phase changed on rejection"),
    }
}

#[test]
fn armed_action_rejection_restores_exact_deadline_timeout_and_publication_state() {
    let mut slots = RootRecoveryTimerSlots::<SchedulerQueued, SchedulerDeadline>::new();
    let exact_key = key(12, 9);
    let owner = install_queued(&mut slots, exact_key, 0x29, Duration::from_millis(49));
    slots
        .try_arm_current_a_queued(exact_key, |queued, _timeout| {
            Ok::<_, (SchedulerQueued, ())>(SchedulerDeadline {
                owner: queued.owner,
            })
        })
        .expect("the exact scheduler becomes armed");
    slots
        .set_current_a_publication_pending(exact_key, true)
        .expect("the armed scheduler records publication pressure");

    let rejected = slots
        .transact_current_a_scheduler_by_exact_key(exact_key, |scheduler| {
            Err::<(), _>((scheduler, "history overflow"))
        })
        .expect_err("an armed controller rejection restores before returning");
    assert_eq!(
        rejected,
        RootRecoverySchedulerTransactionError::Action("history overflow")
    );
    let retained = slots
        .take_current_a_by_exact_key(exact_key)
        .expect("the rejected deadline remains exact and addressable");
    match retained.authority() {
        RootRecoveryTimerAuthority::Armed {
            deadline,
            timeout,
            publication_pending,
            ..
        } => {
            assert_eq!(deadline.owner.as_ref() as *const u8, owner);
            assert_eq!(*timeout, Duration::from_millis(49));
            assert!(*publication_pending);
        }
        RootRecoveryTimerAuthority::Queued { .. } => panic!("armed phase changed on rejection"),
    }
}

#[test]
fn shutdown_handoff_drives_a_and_b_domains_independently_and_retains_rejection() {
    let mut slots =
        RootRecoveryTimerSlots::<SchedulerQueued, SchedulerDeadline, SchedulerStandbyWake>::new();
    let current_key = key(13, 1);
    let wake_key = standby_key(13, 1);
    let current_owner = install_queued(&mut slots, current_key, 0x31, Duration::from_millis(51));
    let wake = SchedulerStandbyWake {
        key: wake_key.clone(),
        owner: Box::new(0x32),
    };
    let wake_owner = wake.owner.as_ref() as *const u8;
    slots
        .try_install_standby_b_wake(wake)
        .expect("the independent B wake fits beside current A");

    let mut drain = slots.take_for_shutdown();
    let current_summary = drain.try_handoff_current_a(|scheduler| match &scheduler {
        RootRecoverySchedulerAuthority::Queued(queued) => {
            assert_eq!(queued.owner.as_ref() as *const u8, current_owner);
            Ok(())
        }
        RootRecoverySchedulerAuthority::Armed(_) => {
            panic!("the queued current-A phase changed before shutdown handoff")
        }
    });
    let wake_summary = drain.try_handoff_standby_b_wake(|wake| {
        assert_eq!(wake.owner.as_ref() as *const u8, wake_owner);
        Err(wake)
    });
    assert_eq!(
        current_summary,
        RootRecoveryTimerHandoffSummary {
            accepted: 1,
            rejected: 0,
        }
    );
    assert_eq!(
        wake_summary,
        RootRecoveryTimerHandoffSummary {
            accepted: 0,
            rejected: 1,
        }
    );
    assert_eq!(
        drain.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 0,
            armed: 0,
            publication_pending: 0,
            standby_b_wakes: 1,
        }
    );
    assert_eq!(
        drain
            .standby_b_wake_for_key(&wake_key)
            .expect("the rejected B wake remains in its named shutdown slot")
            .owner
            .as_ref() as *const u8,
        wake_owner,
    );
    let wake_retry = drain.try_handoff_standby_b_wake(|wake| {
        assert_eq!(wake.owner.as_ref() as *const u8, wake_owner);
        Ok(())
    });
    assert_eq!(
        wake_retry,
        RootRecoveryTimerHandoffSummary {
            accepted: 1,
            rejected: 0,
        }
    );
    assert_eq!(drain.len(), 0);
}

#[test]
fn joined_witness_counts_controller_and_root_live_ownership_but_not_history() {
    let clean = ClientRootJoined {
        controller_ownership: ClientControllerOwnershipLedger::default(),
        root_recovery_timers: RootRecoveryTimerLiveInventory::default(),
        ..ClientRootJoined::default()
    };
    assert!(clean.controller_ownership().live_is_zero());
    assert!(clean.root_recovery_timers().is_zero());
    assert!(clean.is_zero());

    let residual = ClientRootJoined {
        timers: 1,
        root_recovery_timers: RootRecoveryTimerLiveInventory {
            queued: 1,
            armed: 0,
            publication_pending: 0,
            standby_b_wakes: 0,
        },
        ..clean
    };
    assert_eq!(residual.root_recovery_timers().queued(), 1);
    assert!(!residual.is_zero());
}
