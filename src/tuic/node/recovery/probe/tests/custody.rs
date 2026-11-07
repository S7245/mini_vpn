use super::*;
use std::sync::atomic::AtomicUsize;
use std::sync::{Condvar, Mutex};
use std::task::{Wake, Waker};

struct BlockingWake {
    state: Mutex<(bool, bool)>,
    changed: Condvar,
}
impl Wake for BlockingWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        let mut state = self.state.lock().unwrap();
        state.0 = true;
        self.changed.notify_all();
        while !state.1 {
            let (next, timeout) = self
                .changed
                .wait_timeout(state, Duration::from_secs(3))
                .unwrap();
            state = next;
            assert!(!timeout.timed_out(), "test must unblock actual zero waker");
        }
    }
}

fn pending() -> (
    Arc<StartupResources>,
    Ticket,
    Arc<TcpPoolGenerationActivity>,
    Arc<AtomicU64>,
) {
    let owner = Arc::new(StartupResources::new(2));
    let admission = TcpPoolAdmission::new(2);
    let observations = [0, 1].map(|i| {
        TcpPoolPathObservation::known(
            TcpPoolTransportIdentity::new(i + 1, 1),
            24000,
            Duration::from_millis(20),
            0,
        )
    });
    let TcpPoolAdmissionDecision::ReserveCurrent(primary) = admission
        .try_decide_once(&observations, &[], &[], false)
        .unwrap()
    else {
        panic!("primary reservation");
    };
    let TcpPoolAdmissionDecision::ReserveCurrent(mut reservation) = admission
        .try_decide_once(&observations, &[], &[], false)
        .unwrap()
    else {
        panic!("aux reservation");
    };
    assert_eq!(reservation.index, 1);
    reservation.lease.retain_until_release();
    let activity = reservation
        .lease
        .generation_active
        .as_ref()
        .unwrap()
        .clone();
    let slot = reservation.lease.slot_active.clone();
    drop(primary);
    let ticket = Ticket {
        index: 1,
        identity: TcpPoolTransportIdentity::new(7, 1),
        sequence: 1,
        deadline: ProbeKind::Idle.deadline(),
        kind: Kind::Probe(ProbeKind::Idle),
    };
    owner.state().recovery.demands[1] = Some(Demand {
        ticket,
        outcome: Outcome::Pending,
        attempts: 0,
        preparation: None,
        settled: false,
        ready_identity: None,
        receipt_issued: false,
    });
    owner.state().recovery.probe_custody[1] = Some(ProbeSlot {
        ticket,
        state: CustodyState::Held(Custody {
            lease: reservation.lease,
            preparation: Some(reservation.preparation),
            interrupted: false,
        }),
    });
    owner.state().recovery.active_ticket = Some(ticket);
    (owner, ticket, activity, slot)
}

#[tokio::test]
async fn original_release_in_flight_cannot_be_certified_by_second_settle_or_stop() {
    let (owner, ticket, activity, slot) = pending();
    let wake = Arc::new(BlockingWake {
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    let waker = Waker::from(wake.clone());
    let notified = activity.zero.notified();
    tokio::pin!(notified);
    assert!(
        notified
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let saved = settle_uncommitted(&owner);
    tokio::pin!(saved);
    let stop_wake = Arc::new(CountWake(AtomicUsize::new(0)));
    let stop_waker = Waker::from(stop_wake.clone());
    let (second_complete, stop_complete) = std::thread::scope(|scope| {
        let worker = scope.spawn(|| settle(&owner, ticket, true));
        let state = wake.state.lock().unwrap();
        let (state, timeout) = wake
            .changed
            .wait_timeout_while(state, Duration::from_secs(2), |s| !s.0)
            .unwrap();
        assert!(!timeout.timed_out());
        drop(state);
        assert_eq!(activity.active(), 0);
        assert_eq!(slot.load(Ordering::Acquire), 1);
        let second = settle(&owner, ticket, false);
        owner.state().stopping = true;
        assert!(
            saved
                .as_mut()
                .poll(&mut Context::from_waker(&stop_waker))
                .is_pending()
        );
        let certified = owner.state().recovery.demands[1].as_ref().unwrap().settled;
        wake.state.lock().unwrap().1 = true;
        wake.changed.notify_all();
        let _ = worker.join().unwrap();
        (second, certified)
    });
    assert!(
        second_complete == Settlement::Busy,
        "temporary custody absence is not a completed original debit"
    );
    assert!(
        !stop_complete,
        "stop must not certify a borrowed original lease"
    );
    assert_eq!(slot.load(Ordering::Acquire), 0);
    assert!(
        stop_wake.0.load(Ordering::Acquire) > 0,
        "saved stop wait must be woken"
    );
    tokio::time::timeout(Duration::from_secs(1), saved)
        .await
        .unwrap();
    assert!(
        !owner.state().quarantined,
        "known in-flight custody is not corruption"
    );
}

struct PanicWake(AtomicUsize);
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
        panic!("actual generation-zero wake interrupted original debit");
    }
}
#[tokio::test]
async fn panic_in_original_zero_waker_retains_partial_report_and_never_redebits() {
    let (owner, ticket, activity, slot) = pending();
    let wake = Arc::new(PanicWake(AtomicUsize::new(0)));
    let waker = Waker::from(wake.clone());
    let notified = activity.zero.notified();
    tokio::pin!(notified);
    assert!(
        notified
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    assert_eq!(settle(&owner, ticket, true), Settlement::Quarantined);
    assert_eq!(activity.active(), 0);
    assert_eq!(slot.load(Ordering::Acquire), 1);
    let report = {
        let root = owner.state();
        assert!(root.quarantined);
        assert_eq!(root.attempts_started, 0);
        let demand = root.recovery.demands[1].as_ref().unwrap();
        assert!(!demand.settled && demand.ready_identity.is_none());
        let CustodyState::Held(custody) = &root.recovery.probe_custody[1].as_ref().unwrap().state
        else {
            panic!("original custody lost");
        };
        assert!(custody.interrupted);
        assert!(
            custody
                .preparation
                .as_ref()
                .unwrap()
                .preparing
                .load(Ordering::Acquire)
        );
        let report = custody.lease.release.unwrap();
        assert_eq!(
            report.generation,
            tcp_pool_admission::CounterRelease::Decremented {
                before: 1,
                after: 0
            }
        );
        assert_eq!(report.slot, tcp_pool_admission::CounterRelease::Pending);
        assert_eq!(report.total, tcp_pool_admission::CounterRelease::Pending);
        assert_eq!(
            root.recovery.release_failure.as_ref().unwrap().2,
            Some(report)
        );
        report
    };
    assert_eq!(settle(&owner, ticket, false), Settlement::Quarantined);
    owner.state().stopping = true;
    clear_uncommitted(&owner);
    assert_eq!(wake.0.load(Ordering::Acquire), 1);
    assert_eq!(slot.load(Ordering::Acquire), 1);
    let root = owner.state();
    assert!(root.quarantined);
    let demand = root.recovery.demands[1].as_ref().unwrap();
    assert!(!demand.settled);
    let CustodyState::Held(custody) = &root.recovery.probe_custody[1].as_ref().unwrap().state
    else {
        panic!("original custody lost");
    };
    assert_eq!(custody.lease.release, Some(report));
    assert!(custody.preparation.is_some());
}

struct CountWake(AtomicUsize);
impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::AcqRel);
    }
}

#[tokio::test]
async fn damaged_control_ticket_keeps_original_physical_custody_and_preparation() {
    let (owner, ticket, activity, slot) = pending();
    let wake = Arc::new(BlockingWake {
        state: Mutex::new((false, false)),
        changed: Condvar::new(),
    });
    let waker = Waker::from(wake.clone());
    let notified = activity.zero.notified();
    tokio::pin!(notified);
    assert!(
        notified
            .as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    std::thread::scope(|scope| {
        let worker = scope.spawn(|| settle(&owner, ticket, true));
        let state = wake.state.lock().unwrap();
        let (state, timeout) = wake
            .changed
            .wait_timeout_while(state, Duration::from_secs(2), |s| !s.0)
            .unwrap();
        assert!(!timeout.timed_out());
        drop(state);
        owner.state().recovery.demands[1]
            .as_mut()
            .unwrap()
            .ticket
            .sequence += 1;
        assert!(!slot_available(&owner.state().recovery, 1));
        assert_eq!(settle(&owner, ticket, false), Settlement::Busy);
        owner.state().stopping = true;
        assert!(!clear_uncommitted(&owner));
        wake.state.lock().unwrap().1 = true;
        wake.changed.notify_all();
        assert_eq!(worker.join().unwrap(), Settlement::Quarantined);
    });
    assert!(clear_uncommitted(&owner));
    assert_eq!(slot.load(Ordering::Acquire), 0);
    let root = owner.state();
    assert!(root.quarantined);
    let physical = root.recovery.probe_custody[1].as_ref().unwrap();
    assert_eq!(physical.ticket, ticket);
    let CustodyState::Held(custody) = &physical.state else {
        panic!("original custody missing");
    };
    assert!(custody.lease.release.unwrap().complete());
    assert!(
        custody
            .preparation
            .as_ref()
            .unwrap()
            .preparing
            .load(Ordering::Acquire)
    );
    assert!(!root.recovery.demands[1].as_ref().unwrap().settled);
    assert!(root.recovery.preparation.is_none());
}

#[tokio::test]
async fn fully_returned_probe_does_not_quarantine_later_closed_demand_on_stop() {
    let (owner, ticket, _, slot) = pending();
    assert_eq!(settle(&owner, ticket, false), Settlement::Complete);
    assert!(slot_available(&owner.state().recovery, 1));
    let mut next = ticket;
    next.sequence += 1;
    next.kind = Kind::Closed;
    {
        let mut root = owner.state();
        let demand = root.recovery.demands[1].as_mut().unwrap();
        demand.ticket = next;
        root.recovery.active_ticket = Some(next);
        root.stopping = true;
    }
    assert_eq!(settle(&owner, ticket, false), Settlement::Complete);
    settle_uncommitted(&owner).await;
    assert!(!owner.state().quarantined);
    assert!(owner.state().recovery.demands[1].as_ref().unwrap().settled);
    assert_eq!(slot.load(Ordering::Acquire), 0);
}

#[tokio::test]
async fn expiry_follows_original_physical_ticket_when_control_demand_is_missing() {
    let (owner, mut ticket, activity, slot) = pending();
    ticket.deadline = tokio::time::Instant::now();
    {
        let mut root = owner.state();
        root.recovery.probe_custody[1].as_mut().unwrap().ticket = ticket;
        root.recovery.demands[1] = None;
        root.recovery.active_ticket = None;
    }
    owner.expire_demands();
    owner.expire_demands();
    let root = owner.state();
    assert!(root.quarantined);
    assert_eq!(activity.active(), 0);
    assert_eq!(slot.load(Ordering::Acquire), 0);
    assert_eq!(root.attempts_started, 0);
    let physical = root.recovery.probe_custody[1].as_ref().unwrap();
    assert_eq!(physical.ticket, ticket);
    let CustodyState::Held(custody) = &physical.state else {
        panic!("original custody missing");
    };
    assert!(custody.lease.release.unwrap().complete());
    assert!(
        custody
            .preparation
            .as_ref()
            .unwrap()
            .preparing
            .load(Ordering::Acquire)
    );
}
