//! Owning metadata requests through the actual reserved/bound slot.
//!
//! No TCP, Source, Target or terminal settlement is fabricated. These recipes
//! own only readiness metadata; failures retain the original record and fault.
use super::super::*;
use std::future::Future;
use std::task::Context;

fn fixture(source: bool) -> (ReadyIndex, OwnerReadyInstalled, OwnerReadyFlowAssociation) {
    let geometry = ReadyGeometry::checked(NonZeroUsize::new(1).unwrap()).unwrap();
    let mut index = ReadyIndex::cold(&geometry).unwrap();
    let id = SessionId::new([0x7b; 16]).unwrap();
    let association = if source {
        OwnerReadyFlowAssociation::Source(LocalFlow::for_adapter_test(
            id,
            SessionFlowId::new(1).unwrap(),
            crate::resumable::Direction::TargetToClient,
            1,
        ))
    } else {
        OwnerReadyFlowAssociation::NoSource {
            flow_id: SessionFlowId::new(1).unwrap(),
            phase: OwnerReadyNoSourcePhase::RetainedCompletion,
        }
    };
    let limits = OwnerReadySessionLimits::checked(2, 2, 1, 1).unwrap();
    let reservation = index.reserve_session(id, limits).unwrap();
    let prepared = reservation
        .prepare_flow_backing(
            2,
            &[OwnerReadyFlowImport {
                association,
                target_present: source,
                read_interest: source,
                write_interest: source,
                join_interest: source,
                completion: !source,
                pending_open: false,
                retiring: false,
            }],
        )
        .unwrap();
    let installed = reservation
        .bind_prepared(prepared)
        .unwrap_or_else(|_| panic!("actual same-domain metadata binding"));
    (index, installed, association)
}

#[derive(Debug, PartialEq, Eq)]
struct RecordSnapshot {
    retiring: bool,
    association: OwnerReadyFlowAssociation,
    generation: u64,
    due: usize,
    revisions: [u64; FLOW_KINDS],
    credit: u8,
    read_phase: u8,
    indexed: [bool; FLOW_KINDS],
}

fn snapshot(index: &ReadyIndex) -> RecordSnapshot {
    let state = index.shared.slots[0].lock().unwrap();
    let backing = match &state.occupancy {
        ReadyOccupancy::BoundInactive(_, backing) | ReadyOccupancy::BoundActive(_, backing) => {
            backing
        }
        _ => panic!("original binding retained"),
    };
    let (record, retiring) = match &backing.slots[0] {
        FlowReadySlot::Live(record) => (record, false),
        FlowReadySlot::Retiring(record) => (record, true),
        _ => panic!("original record retained, never temporary Free"),
    };
    RecordSnapshot {
        retiring,
        association: record.association,
        generation: record.generation,
        due: record.due,
        revisions: record.revisions,
        credit: record.credit as u8,
        read_phase: record.read_phase as u8,
        indexed: std::array::from_fn(|kind| backing.trees[kind].select(0).unwrap() == Some(0)),
    }
}

struct NotifyCount {
    count: AtomicUsize,
    shared: Arc<ReadyShared>,
}

impl Wake for NotifyCount {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        assert!(
            self.shared.slots[0].try_lock().is_ok(),
            "Notify outside exclusion"
        );
        self.count.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn inactive_live_retirement_commits_only_original_metadata() {
    let (mut index, mut installed, association) = fixture(true);
    let before = snapshot(&index);
    assert!(!before.retiring);
    let count = Arc::new(NotifyCount {
        count: AtomicUsize::new(0),
        shared: Arc::clone(&index.shared),
    });
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&index.shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    installed
        .binding
        .request_retained_flow_work(0, (association, 1, false), OwnerReadyCategory::Retirement)
        .unwrap();
    let after = snapshot(&index);
    assert!(after.retiring);
    assert_eq!(after.association, before.association);
    assert_eq!(after.generation, before.generation);
    assert_eq!(after.credit, before.credit);
    assert_eq!(after.read_phase, ReadPhase::Closed as u8);
    assert_eq!(after.due, 1 << 10);
    assert_eq!(after.indexed, std::array::from_fn(|kind| kind == 10));
    let mut expected = before.revisions;
    expected[10] += 1;
    assert_eq!(after.revisions, expected);
    assert_eq!(index.check_root(), Ok(()));
    assert_eq!(index.shared.epoch.load(Ordering::Acquire), 0);
    assert!(index.shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(index.shared.slots[0].publication.load(Ordering::Acquire), 0);
    assert_eq!(count.count.load(Ordering::SeqCst), 0);
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 1 << CONTINUATION_SOURCE)
        .unwrap();
    let claim = index.claim(0).unwrap();
    let turn = installed
        .binding
        .begin_publication()
        .unwrap_or_else(|_| panic!("same claim"));
    let selection = installed
        .binding
        .select_continuation(OwnerReadyCategory::Retirement.mask(), 0)
        .unwrap()
        .unwrap();
    assert_eq!(selection.candidate.unwrap().association, association);
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::Waiting,
        runnable: 0,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    index.finish(&claim, 0, None).unwrap();
}

#[test]
fn active_completion_retains_live_metadata_and_defers_notify_to_finish() {
    let (mut index, mut installed, association) = fixture(true);
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 1 << CONTINUATION_SOURCE)
        .unwrap();
    let claim = index.claim(0).unwrap();
    let turn = installed
        .binding
        .begin_publication()
        .unwrap_or_else(|_| panic!("same active claim"));
    let before = snapshot(&index);
    let count = Arc::new(NotifyCount {
        count: AtomicUsize::new(0),
        shared: Arc::clone(&index.shared),
    });
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&index.shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    installed
        .binding
        .request_retained_flow_work(0, (association, 1, false), OwnerReadyCategory::Completion)
        .unwrap();
    let after = snapshot(&index);
    let mut expected = before;
    expected.due |= 1 << 9;
    expected.revisions[9] += 1;
    expected.indexed[9] = true;
    assert_eq!(after, expected);
    assert_eq!(count.count.load(Ordering::SeqCst), 0);
    assert_eq!(
        index.shared.slots[0].publication.load(Ordering::Acquire),
        PUBLICATION_ENABLED | PUBLICATION_ACTIVE | PUBLICATION_PENDING
    );
    let selected = installed
        .binding
        .select_continuation(OwnerReadyCategory::Completion.mask(), 0)
        .unwrap()
        .unwrap();
    assert_eq!(selected.candidate.unwrap().association, association);
    let completed = SessionCommandVisit {
        work: OwnerReadyUnit::Waiting,
        runnable: 0,
        blocked: None,
    };
    assert!(turn.finish(&completed).is_ok());
    assert_eq!(count.count.load(Ordering::SeqCst), 1);
    index.finish(&claim, 0, None).unwrap();
    assert_eq!(index.check_root(), Ok(()));
}

#[test]
fn owning_rejections_preserve_the_exact_record_and_first_fault() {
    for case in [
        "stale-key",
        "zero-slot-generation",
        "zero-scheduling-generation",
        "abandoned",
        "session",
        "limits",
        "capacity",
        "free",
        "out-of-range",
        "association",
        "flow-generation",
        "retiring-shape",
        "category",
        "enabled",
        "root-indexed",
        "phase",
        "prior-fault",
    ] {
        let (index, mut installed, association) = fixture(true);
        let mut position = 0;
        let mut expected = (association, 1, false);
        let mut category = OwnerReadyCategory::Retirement;
        let fault = if case == "stale-key" {
            ReadyFault::Stale
        } else if case == "prior-fault" {
            ReadyFault::EpochExhausted
        } else {
            ReadyFault::Corrupt
        };
        match case {
            "stale-key" => installed.binding.custody.key.scheduling_generation += 1,
            "zero-slot-generation" => installed.binding.custody.key.slot_generation = 0,
            "zero-scheduling-generation" => installed.binding.custody.key.scheduling_generation = 0,
            "abandoned" => installed.binding.custody.abandon_cold = true,
            "session" => {
                installed.binding.custody.correlation.session_id =
                    SessionId::new([0x7c; 16]).unwrap()
            }
            "limits" => {
                installed.binding.custody.correlation.limits =
                    OwnerReadySessionLimits::checked(2, 2, 2, 1).unwrap()
            }
            "capacity" => {
                let mut state = index.shared.slots[0].lock().unwrap();
                let ReadyOccupancy::BoundInactive(_, backing) = &mut state.occupancy else {
                    panic!("inactive");
                };
                assert!(matches!(
                    backing.slots.pop(),
                    Some(FlowReadySlot::Free { .. })
                ));
            }
            "free" => position = 1,
            "out-of-range" => position = 2,
            "association" => {
                expected.0 = OwnerReadyFlowAssociation::NoSource {
                    flow_id: SessionFlowId::new(1).unwrap(),
                    phase: OwnerReadyNoSourcePhase::Retirement,
                }
            }
            "flow-generation" => expected.1 = 0,
            "retiring-shape" => expected.2 = true,
            "category" => category = OwnerReadyCategory::ReadReady,
            "enabled" => index.shared.slots[0]
                .publication
                .store(PUBLICATION_ENABLED, Ordering::Release),
            "root-indexed" => index.shared.trees[SESSION_CLASS].publish(0),
            "phase" => {
                index.shared.slots[0].lock().unwrap().processes[SESSION_CLASS].phase =
                    ReadyPhase::Idle
            }
            "prior-fault" => {
                assert_eq!(index.shared.failure.fail(fault), fault);
            }
            _ => unreachable!("finite test cases"),
        }
        let before = snapshot(&index);
        let count = Arc::new(NotifyCount {
            count: AtomicUsize::new(0),
            shared: Arc::clone(&index.shared),
        });
        let waker = Waker::from(Arc::clone(&count));
        let mut cx = Context::from_waker(&waker);
        let mut notification = Box::pin(Arc::clone(&index.shared.notify).notified_owned());
        assert!(notification.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            installed
                .binding
                .request_retained_flow_work(position, expected, category),
            Err(fault),
            "{case}"
        );
        assert_eq!(snapshot(&index), before, "{case}");
        assert_eq!(index.check_root(), Err(fault), "{case}");
        assert_ne!(index.shared.epoch.load(Ordering::Acquire), 0, "{case}");
        assert_ne!(
            index.shared.slots[0].publication.load(Ordering::Acquire) & PUBLICATION_PENDING,
            0
        );
        assert_eq!(
            count.count.load(Ordering::SeqCst),
            0,
            "owning fault is deferred: {case}"
        );
        assert_eq!(
            installed.binding.request_retained_flow_work(
                0,
                (association, 1, false),
                OwnerReadyCategory::Completion
            ),
            Err(fault),
            "first fault remains exact: {case}"
        );
        assert_eq!(snapshot(&index), before, "{case}");
    }
}

#[test]
fn revision_exhaustion_precedes_live_retirement_or_completion_commit() {
    for category in [
        OwnerReadyCategory::Completion,
        OwnerReadyCategory::Retirement,
    ] {
        for (active, exhausted) in [
            (false, "flow"),
            (true, "flow"),
            (true, "source"),
            (true, "process"),
        ] {
            let (mut index, mut installed, association) = fixture(true);
            let _interval = if active {
                installed
                    .binding
                    .activate(&mut index, 0, &installed.session, 1 << CONTINUATION_SOURCE)
                    .unwrap();
                let claim = index.claim(0).unwrap();
                let turn = installed
                    .binding
                    .begin_publication()
                    .unwrap_or_else(|_| panic!("same claim"));
                Some((claim, turn))
            } else {
                None
            };
            {
                let mut state = index.shared.slots[0].lock().unwrap();
                match exhausted {
                    "flow" => {
                        let backing = match &mut state.occupancy {
                            ReadyOccupancy::BoundInactive(_, backing)
                            | ReadyOccupancy::BoundActive(_, backing) => backing,
                            _ => panic!("bound"),
                        };
                        let FlowReadySlot::Live(record) = &mut backing.slots[0] else {
                            panic!("live");
                        };
                        record.revisions[category as usize - 2] = u64::MAX;
                    }
                    "source" => state.sources[CONTINUATION_SOURCE].revision = u64::MAX,
                    "process" => state.processes[SESSION_CLASS].revision = u64::MAX,
                    _ => unreachable!("finite exhaustion cases"),
                }
            }
            let before = snapshot(&index);
            let cells = || {
                let state = index.shared.slots[0].lock().unwrap();
                state
                    .sources
                    .iter()
                    .chain(state.processes.iter())
                    .map(|cell| (cell.phase as u8, cell.revision))
                    .collect::<Vec<_>>()
            };
            let before_cells = cells();
            assert_eq!(
                installed
                    .binding
                    .request_retained_flow_work(0, (association, 1, false), category),
                Err(ReadyFault::RevisionExhausted),
                "{active}/{exhausted}/{category:?}"
            );
            assert_eq!(snapshot(&index), before);
            assert!(!snapshot(&index).retiring);
            assert_eq!(
                cells(),
                before_cells,
                "no partially published original cells"
            );
            assert_eq!(index.check_root(), Err(ReadyFault::RevisionExhausted));
            assert_ne!(
                index.shared.slots[0].publication.load(Ordering::Acquire) & PUBLICATION_PENDING,
                0
            );
            assert_eq!(
                installed
                    .binding
                    .request_retained_flow_work(0, (association, 1, false), category),
                Err(ReadyFault::RevisionExhausted)
            );
            assert_eq!(snapshot(&index), before);
        }
    }
}

#[test]
fn quarantined_advisory_hint_is_ignored_but_owning_request_is_not_dropped() {
    let (mut index, mut installed, association) = fixture(true);
    installed
        .binding
        .activate(&mut index, 0, &installed.session, 1 << CONTINUATION_SOURCE)
        .unwrap();
    let _claim = index.claim(0).unwrap();
    let turn = installed
        .binding
        .begin_publication()
        .unwrap_or_else(|_| panic!("same claim"));
    index.quarantine(0).unwrap();
    let before = snapshot(&index);
    let count = Arc::new(NotifyCount {
        count: AtomicUsize::new(0),
        shared: Arc::clone(&index.shared),
    });
    let waker = Waker::from(Arc::clone(&count));
    let mut cx = Context::from_waker(&waker);
    let mut notification = Box::pin(Arc::clone(&index.shared.notify).notified_owned());
    assert!(notification.as_mut().poll(&mut cx).is_pending());
    installed.tickets[0]
        .target_waker(OwnerReadyCategory::ReadReady)
        .unwrap()
        .wake_by_ref();
    assert_eq!(snapshot(&index), before);
    assert_eq!(index.check_root(), Ok(()));
    assert!(index.shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(count.count.load(Ordering::SeqCst), 0);
    assert_eq!(
        installed.binding.request_retained_flow_work(
            0,
            (association, 1, false),
            OwnerReadyCategory::Retirement
        ),
        Err(ReadyFault::Corrupt)
    );
    assert_eq!(snapshot(&index), before);
    assert_eq!(index.check_root(), Err(ReadyFault::Corrupt));
    assert_eq!(count.count.load(Ordering::SeqCst), 0);
    assert_eq!(turn.finish_quarantined(), Err(ReadyFault::Corrupt));
    assert_eq!(count.count.load(Ordering::SeqCst), 1);
    let state = index.shared.slots[0].lock().unwrap();
    assert!(matches!(
        state.processes[SESSION_CLASS].phase,
        ReadyPhase::Quarantined
    ));
    assert!(matches!(state.occupancy, ReadyOccupancy::BoundActive(..)));
    // This still owns its nonzero quarantined metadata, not clean retirement.
}

#[test]
fn inactive_source_less_completion_does_not_create_a_receipt_claim() {
    let (index, installed, association) = fixture(false);
    let mut expected = snapshot(&index);
    installed
        .binding
        .request_retained_flow_work(0, (association, 1, false), OwnerReadyCategory::Completion)
        .unwrap();
    expected.revisions[9] += 1;
    expected.due |= 1 << 9;
    expected.indexed[9] = true;
    assert_eq!(snapshot(&index), expected);
    installed
        .binding
        .request_retained_flow_work(0, (association, 1, false), OwnerReadyCategory::Retirement)
        .unwrap();
    let before = snapshot(&index);
    installed
        .binding
        .request_retained_flow_work(0, (association, 1, true), OwnerReadyCategory::Completion)
        .unwrap();
    let mut expected = before;
    expected.revisions[10] += 1;
    assert_eq!(
        snapshot(&index),
        expected,
        "already Retiring Completion stays Retirement"
    );
    assert_eq!(index.shared.epoch.load(Ordering::Acquire), 0);
    assert!(index.shared.trees.iter().all(ReadyTree::is_empty));
    assert_eq!(index.shared.slots[0].publication.load(Ordering::Acquire), 0);
    assert_eq!(index.check_root(), Ok(()));
    assert_eq!(
        installed
            .binding
            .validate_receipt_flow(2, 0, (association, 1, true)),
        Err(ReadyFault::Corrupt),
        "inactive owning work is not an active receipt"
    );
    assert_eq!(snapshot(&index), expected);
}

#[test]
fn active_owning_request_requires_the_original_running_publication_interval() {
    for defect in ["no-interval", "process-phase", "source-phase"] {
        let (mut index, mut installed, association) = fixture(true);
        installed
            .binding
            .activate(&mut index, 0, &installed.session, 1 << CONTINUATION_SOURCE)
            .unwrap();
        let _claim = index.claim(0).unwrap();
        let _turn = if defect == "no-interval" {
            None
        } else {
            Some(
                installed
                    .binding
                    .begin_publication()
                    .unwrap_or_else(|_| panic!("same claim")),
            )
        };
        if defect != "no-interval" {
            let mut state = index.shared.slots[0].lock().unwrap();
            if defect == "process-phase" {
                state.processes[SESSION_CLASS].phase = ReadyPhase::Idle;
            } else {
                state.sources[CONTINUATION_SOURCE].phase = ReadyPhase::Reserved;
            }
        }
        let before = snapshot(&index);
        assert_eq!(
            installed.binding.request_retained_flow_work(
                0,
                (association, 1, false),
                OwnerReadyCategory::Retirement
            ),
            Err(ReadyFault::Corrupt),
            "{defect}"
        );
        assert_eq!(snapshot(&index), before);
        assert_eq!(index.check_root(), Err(ReadyFault::Corrupt));
    }
}
