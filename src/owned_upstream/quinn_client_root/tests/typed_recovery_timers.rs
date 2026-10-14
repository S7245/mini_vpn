use super::*;
use crate::resumable::ProbeSequence;

struct FakeQueuedProbe {
    key: RootCurrentARecoveryKey,
    owner: Box<u8>,
}

impl RootCurrentARecoveryQueuedOwner for FakeQueuedProbe {
    fn current_a_recovery_key(&self) -> RootCurrentARecoveryKey {
        self.key
    }
}

struct FakeProbeDeadline {
    owner: Box<u8>,
}

fn timer_key(generation: u64, sequence: u64) -> RootCurrentARecoveryKey {
    RootCurrentARecoveryKey {
        generation: LegGeneration::new(generation).expect("nonzero test generation"),
        sequence: ProbeSequence::new(sequence).expect("nonzero test probe sequence"),
    }
}

#[test]
fn current_a_queued_install_is_one_slot_and_returns_each_exact_rejected_owner() {
    let mut slots = RootRecoveryTimerSlots::<FakeQueuedProbe, FakeProbeDeadline>::new();
    let first_key = timer_key(1, 1);
    let first = FakeQueuedProbe {
        key: first_key,
        owner: Box::new(0x11),
    };
    slots
        .try_install_current_a_queued(first, Duration::from_millis(10))
        .expect("the first exact queued probe fits");

    let duplicate = FakeQueuedProbe {
        key: first_key,
        owner: Box::new(0x22),
    };
    let duplicate_ptr = duplicate.owner.as_ref() as *const u8;
    let duplicate = slots
        .try_install_current_a_queued(duplicate, Duration::from_millis(20))
        .expect_err("a duplicate correlation key fails before slot mutation");
    assert_eq!(
        duplicate.kind(),
        RootRecoveryTimerInstallFailureKind::Duplicate,
    );
    let (duplicate, timeout) = duplicate.into_parts();
    assert_eq!(duplicate.owner.as_ref() as *const u8, duplicate_ptr);
    assert_eq!(timeout, Duration::from_millis(20));

    let over_capacity = FakeQueuedProbe {
        key: timer_key(1, 2),
        owner: Box::new(0x33),
    };
    let over_capacity_ptr = over_capacity.owner.as_ref() as *const u8;
    let over_capacity = slots
        .try_install_current_a_queued(over_capacity, Duration::from_millis(30))
        .expect_err("a second A owner cannot borrow the named B wake slot");
    assert_eq!(
        over_capacity.kind(),
        RootRecoveryTimerInstallFailureKind::Capacity,
    );
    let (over_capacity, timeout) = over_capacity.into_parts();
    assert_eq!(over_capacity.owner.as_ref() as *const u8, over_capacity_ptr,);
    assert_eq!(timeout, Duration::from_millis(30));

    assert_eq!(slots.capacity(), ROOT_RECOVERY_TIMER_SLOTS);
    assert_eq!(slots.len(), ROOT_CURRENT_A_RECOVERY_TIMER_SLOTS);
}

#[test]
fn queued_to_armed_is_an_in_place_exact_owner_transaction() {
    let mut slots = RootRecoveryTimerSlots::<FakeQueuedProbe, FakeProbeDeadline>::new();
    let key = timer_key(2, 7);
    let queued = FakeQueuedProbe {
        key,
        owner: Box::new(0x71),
    };
    let owner_ptr = queued.owner.as_ref() as *const u8;
    slots
        .try_install_current_a_queued(queued, Duration::from_millis(25))
        .expect("one exact queued owner fits");

    let mut wrong_key_called = false;
    let wrong_key = slots
        .try_arm_current_a_queued(timer_key(9, 9), |queued, _timeout| {
            wrong_key_called = true;
            Result::<FakeProbeDeadline, (FakeQueuedProbe, &'static str)>::Err((
                queued,
                "must not run",
            ))
        })
        .expect_err("an unrelated correlation cannot take the queued owner");
    assert_eq!(wrong_key.kind(), RootRecoveryTimerArmFailureKind::Missing,);
    assert!(!wrong_key_called);

    let retry = slots
        .try_arm_current_a_queued(key, |queued, timeout| {
            assert_eq!(queued.owner.as_ref() as *const u8, owner_ptr);
            assert_eq!(timeout, Duration::from_millis(25));
            Err((queued, "writer pending"))
        })
        .expect_err("retryable pressure restores the exact queued owner");
    assert_eq!(retry.kind(), RootRecoveryTimerArmFailureKind::Arm);
    assert_eq!(retry.into_error(), Some("writer pending"));
    assert_eq!(
        slots.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 1,
            armed: 0,
            publication_pending: 0,
            standby_b_wakes: 0,
        }
    );

    slots
        .try_arm_current_a_queued(key, |queued, timeout| {
            assert_eq!(queued.owner.as_ref() as *const u8, owner_ptr);
            assert_eq!(timeout, Duration::from_millis(25));
            Ok::<FakeProbeDeadline, (FakeQueuedProbe, &'static str)>(FakeProbeDeadline {
                owner: queued.owner,
            })
        })
        .expect("delivery completion mints one exact armed deadline in place");
    assert_eq!(
        slots.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 0,
            armed: 1,
            publication_pending: 0,
            standby_b_wakes: 0,
        }
    );

    let mut armed_phase_called = false;
    let armed_phase = slots
        .try_arm_current_a_queued(key, |queued, _timeout| {
            armed_phase_called = true;
            Result::<FakeProbeDeadline, (FakeQueuedProbe, &'static str)>::Err((
                queued,
                "must not run",
            ))
        })
        .expect_err("an armed slot cannot consume a second queued transition");
    assert_eq!(
        armed_phase.kind(),
        RootRecoveryTimerArmFailureKind::AlreadyArmed,
    );
    assert!(!armed_phase_called);
}

fn queued_owner_ptr(
    taken: &RootRecoveryTimerTaken<FakeQueuedProbe, FakeProbeDeadline>,
) -> *const u8 {
    match taken.authority() {
        RootRecoveryTimerAuthority::Queued { queued, .. } => queued.owner.as_ref() as *const u8,
        RootRecoveryTimerAuthority::Armed { .. } => {
            panic!("test expected queued scheduler ownership")
        }
    }
}

#[test]
fn exact_key_take_and_restore_never_clear_or_replace_an_owner_blindly() {
    let mut slots = RootRecoveryTimerSlots::<FakeQueuedProbe, FakeProbeDeadline>::new();
    let key = timer_key(3, 1);
    let queued = FakeQueuedProbe {
        key,
        owner: Box::new(0x31),
    };
    let owner_ptr = queued.owner.as_ref() as *const u8;
    slots
        .try_install_current_a_queued(queued, Duration::from_millis(15))
        .expect("one queued owner fits");

    assert_eq!(
        slots
            .take_current_a_by_exact_key(timer_key(3, 2))
            .expect_err("a wrong ACK correlation cannot take another owner"),
        RootRecoveryTimerTakeFailureKind::KeyMismatch,
    );
    assert_eq!(slots.len(), 1);

    let taken = slots
        .take_current_a_by_exact_key(key)
        .expect("the exact ACK correlation takes the scheduler owner");
    assert_eq!(taken.key(), key);
    assert_eq!(queued_owner_ptr(&taken), owner_ptr);
    assert!(slots.is_empty());
    let (slot_index, authority) = taken.into_parts();
    assert!(slot_index < ROOT_RECOVERY_TIMER_SLOTS);
    match &authority {
        RootRecoveryTimerAuthority::Queued {
            key: authority_key,
            queued,
            timeout,
            ..
        } => {
            assert_eq!(*authority_key, key);
            assert_eq!(queued.owner.as_ref() as *const u8, owner_ptr);
            assert_eq!(*timeout, Duration::from_millis(15));
        }
        RootRecoveryTimerAuthority::Armed { .. } => {
            panic!("test expected queued adapter metadata")
        }
    }
    let taken = RootRecoveryTimerTaken::from_parts(slot_index, authority);
    slots
        .restore_current_a_exact(taken)
        .expect("an unchanged slot restores its exact owner");

    let taken_again = slots
        .take_current_a_by_exact_key(key)
        .expect("the restored exact owner remains addressable");
    assert_eq!(queued_owner_ptr(&taken_again), owner_ptr);

    let replacement = FakeQueuedProbe {
        key,
        owner: Box::new(0x32),
    };
    let replacement_ptr = replacement.owner.as_ref() as *const u8;
    slots
        .try_install_current_a_queued(replacement, Duration::from_millis(16))
        .expect("the now-empty fixed slot admits a distinct exact owner");
    let restore = slots
        .restore_current_a_exact(taken_again)
        .expect_err("restore cannot overwrite a duplicate correlation owner");
    assert_eq!(
        restore.kind(),
        RootRecoveryTimerRestoreFailureKind::Duplicate,
    );
    let exact_rejected = restore.into_taken();
    assert_eq!(queued_owner_ptr(&exact_rejected), owner_ptr);

    let replacement = slots
        .take_current_a_by_exact_key(key)
        .expect("the admitted replacement was never overwritten");
    assert_eq!(queued_owner_ptr(&replacement), replacement_ptr);
}

#[test]
fn publication_pending_is_checked_armed_state_and_live_inventory() {
    let mut slots = RootRecoveryTimerSlots::<FakeQueuedProbe, FakeProbeDeadline>::new();
    let queued_key = timer_key(4, 1);
    slots
        .try_install_current_a_queued(
            FakeQueuedProbe {
                key: queued_key,
                owner: Box::new(0x41),
            },
            Duration::from_millis(12),
        )
        .expect("one queued owner fits");
    assert_eq!(
        slots.set_current_a_publication_pending(queued_key, true),
        Err(RootRecoveryTimerPublicationFailureKind::NotArmed),
    );
    assert_eq!(
        slots.set_current_a_publication_pending(timer_key(4, 2), true),
        Err(RootRecoveryTimerPublicationFailureKind::KeyMismatch),
    );
    assert_eq!(
        slots.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 1,
            armed: 0,
            publication_pending: 0,
            standby_b_wakes: 0,
        }
    );

    slots
        .try_arm_current_a_queued(queued_key, |queued, _timeout| {
            Ok::<FakeProbeDeadline, (FakeQueuedProbe, ())>(FakeProbeDeadline {
                owner: queued.owner,
            })
        })
        .expect("the queued owner becomes armed");
    slots
        .set_current_a_publication_pending(queued_key, true)
        .expect("the exact armed owner records publication pressure");
    assert_eq!(
        slots.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 0,
            armed: 1,
            publication_pending: 1,
            standby_b_wakes: 0,
        }
    );
    let taken = slots
        .take_current_a_by_exact_key(queued_key)
        .expect("the exact armed adapter owner is addressable");
    let (slot_index, authority) = taken.into_parts();
    let armed_owner_ptr = match &authority {
        RootRecoveryTimerAuthority::Armed {
            key,
            deadline,
            publication_pending,
            ..
        } => {
            assert_eq!(*key, queued_key);
            assert!(*publication_pending);
            deadline.owner.as_ref() as *const u8
        }
        RootRecoveryTimerAuthority::Queued { .. } => {
            panic!("test expected armed adapter metadata")
        }
    };
    let taken = RootRecoveryTimerTaken::from_parts(slot_index, authority);
    slots
        .restore_current_a_exact(taken)
        .expect("the armed adapter round trip restores the full authority");
    let restored = slots
        .take_current_a_by_exact_key(queued_key)
        .expect("the restored armed authority remains addressable");
    match restored.authority() {
        RootRecoveryTimerAuthority::Armed {
            deadline,
            publication_pending,
            ..
        } => {
            assert_eq!(deadline.owner.as_ref() as *const u8, armed_owner_ptr);
            assert!(*publication_pending);
        }
        RootRecoveryTimerAuthority::Queued { .. } => {
            panic!("test expected restored armed adapter metadata")
        }
    }
    slots
        .restore_current_a_exact(restored)
        .expect("the checked observer restores the exact armed authority");
    slots
        .set_current_a_publication_pending(queued_key, false)
        .expect("the same exact armed owner clears only its wake suppression flag");
    assert_eq!(slots.live_inventory().publication_pending, 0);
}

fn authority_owner_ptr(
    authority: &RootRecoveryTimerAuthority<FakeQueuedProbe, FakeProbeDeadline>,
) -> *const u8 {
    match authority {
        RootRecoveryTimerAuthority::Queued { queued, .. } => queued.owner.as_ref() as *const u8,
        RootRecoveryTimerAuthority::Armed { deadline, .. } => deadline.owner.as_ref() as *const u8,
    }
}

struct FakeStandbyWake {
    key: RootStandbyBRecoveryKey,
    owner: Box<u8>,
}

impl RootStandbyBRecoveryWakeOwner for FakeStandbyWake {
    fn standby_b_recovery_key(&self) -> &RootStandbyBRecoveryKey {
        &self.key
    }
}

fn standby_key(generation: u64, sequence: u64) -> RootStandbyBRecoveryKey {
    RootStandbyBRecoveryKey::mint_for_test(
        LegGeneration::new(generation).expect("nonzero test generation"),
        ProbeSequence::new(sequence).expect("nonzero test probe sequence"),
    )
}

#[test]
fn shutdown_drain_and_restore_preserve_both_complete_named_domains() {
    let mut slots =
        RootRecoveryTimerSlots::<FakeQueuedProbe, FakeProbeDeadline, FakeStandbyWake>::new();
    let armed_key = timer_key(5, 1);
    let wake_key = standby_key(5, 1);
    let armed = FakeQueuedProbe {
        key: armed_key,
        owner: Box::new(0x52),
    };
    let armed_ptr = armed.owner.as_ref() as *const u8;
    slots
        .try_install_current_a_queued(armed, Duration::from_millis(20))
        .expect("the current-A shutdown owner starts queued");
    slots
        .try_arm_current_a_queued(armed_key, |queued, _timeout| {
            Ok::<FakeProbeDeadline, (FakeQueuedProbe, ())>(FakeProbeDeadline {
                owner: queued.owner,
            })
        })
        .expect("second shutdown owner becomes armed");
    slots
        .set_current_a_publication_pending(armed_key, true)
        .expect("armed shutdown owner retains publication pressure");
    let standby = FakeStandbyWake {
        key: wake_key.clone(),
        owner: Box::new(0x5b),
    };
    let standby_ptr = standby.owner.as_ref() as *const u8;
    slots
        .try_install_standby_b_wake(standby)
        .expect("the non-authoritative B wake fits its named domain");

    let drain = slots.take_for_shutdown();
    assert!(slots.is_empty());
    assert_eq!(
        drain.live_inventory(),
        RootRecoveryTimerLiveInventory {
            queued: 0,
            armed: 1,
            publication_pending: 1,
            standby_b_wakes: 1,
        },
    );
    assert_eq!(
        authority_owner_ptr(
            drain
                .current_a_authority_for_key(armed_key)
                .expect("drain retains exact armed owner"),
        ),
        armed_ptr,
    );
    assert_eq!(
        drain
            .standby_b_wake_for_key(&wake_key)
            .expect("drain retains the exact B wake witness")
            .owner
            .as_ref() as *const u8,
        standby_ptr,
    );
    slots
        .restore_for_shutdown(drain)
        .expect("an untouched empty registry accepts both exact shutdown domains");

    let drain = slots.take_for_shutdown();
    slots
        .try_install_current_a_queued(
            FakeQueuedProbe {
                key: timer_key(5, 3),
                owner: Box::new(0x53),
            },
            Duration::from_millis(30),
        )
        .expect("the empty live A domain can independently become occupied");
    let restore = slots
        .restore_for_shutdown(drain)
        .expect_err("shutdown restore cannot overwrite newly live ownership");
    assert_eq!(
        restore.kind(),
        RootRecoveryTimerShutdownRestoreFailureKind::DestinationLive,
    );
    let exact_drain = restore.into_drain();
    assert_eq!(
        authority_owner_ptr(
            exact_drain
                .current_a_authority_for_key(armed_key)
                .expect("failed restore returns exact armed owner"),
        ),
        armed_ptr,
    );
    assert_eq!(
        exact_drain
            .standby_b_wake_for_key(&wake_key)
            .expect("failed restore returns the exact B wake witness")
            .owner
            .as_ref() as *const u8,
        standby_ptr,
    );
}
