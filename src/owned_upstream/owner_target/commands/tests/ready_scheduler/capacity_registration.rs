//! Capacity registration owned by the live root, not a temporary observer.
//!
//! Owns exact wait/cancel/wake lifetime tests against the real mailbox.
//! Does not establish process-wide ready routing, park, or session settlement.

use super::*;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};

#[derive(Default)]
struct RegistrationWake(AtomicUsize);

impl Wake for RegistrationWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn blocked_control() -> (SessionCommands, SessionOwnerIngress, SessionOwnerInbox) {
    let (_, request, _) = owner_request();
    let (ingress, inbox) = SessionOwnerMailbox::bounded(1, 1).unwrap();
    ingress
        .try_send_event(SessionEvent::PeerOpenResolved {
            request,
            result: OpenResultCode::PolicyDenied,
        })
        .unwrap();
    let mut root = SessionCommands::from_open_completion(
        ingress.clone(),
        request,
        OpenResultCode::TargetRefused,
    )
    .unwrap();
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::Full);
    (root, ingress, inbox)
}

fn held_control(root: &SessionCommands) -> &SessionOwnerCommand {
    let SendCell::Held {
        packet:
            HeldCommand {
                storage: CommandStorage::Returned(command),
                ..
            },
        ..
    } = &root.cells[SendLane::Control.index()]
    else {
        panic!("the root must retain its exact Control Full return");
    };
    command.as_ref()
}

#[tokio::test]
async fn cancelled_capacity_borrow_keeps_the_live_root_registered_for_real_progress() {
    let (mut root, ingress, mut inbox) = blocked_control();
    let original = held_control(&root) as *const SessionOwnerCommand;
    let SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, .. }) =
        held_control(&root)
    else {
        panic!("the root owns the real completion");
    };
    let request = *request;
    let count = Arc::new(RegistrationWake::default());
    let waker = Waker::from(Arc::clone(&count));
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    // Only the outer observer borrow was cancelled. The root, command and
    // capacity observer remain alive while a different caller releases space.
    let received = {
        let mut receive = pin!(inbox.recv_next());
        receive
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
    };
    assert!(matches!(received,
        Poll::Ready(Some(SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
            request: actual, result: OpenResultCode::PolicyDenied,
        }))) if actual == request));
    assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
    assert!(matches!(
        ingress.observe_capacity(),
        Err(SessionMailboxObserverBusy)
    ));
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert_eq!(
        count.0.load(Ordering::SeqCst),
        1,
        "the live root's registration must survive its cancelled observer borrow"
    );
}

#[tokio::test]
async fn cancelled_capacity_reentry_replaces_only_the_registered_waker() {
    let (mut root, _, mut inbox) = blocked_control();
    let original = held_control(&root) as *const SessionOwnerCommand;
    let old = Arc::new(RegistrationWake::default());
    let current = Arc::new(RegistrationWake::default());
    for count in [&old, &current] {
        let waker = Waker::from(Arc::clone(count));
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(current.0.load(Ordering::SeqCst), 0);
    assert!(matches!(
        pin!(inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(SessionOwnerCommand::Event(
            SessionEvent::PeerOpenResolved {
                result: OpenResultCode::PolicyDenied,
                ..
            }
        )))
    ));
    assert_eq!(old.0.load(Ordering::SeqCst), 0);
    assert_eq!(current.0.load(Ordering::SeqCst), 1);
    assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
    assert_eq!(
        pin!(root.rearm_full_commands())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(OwnerCapacityRearm::Rearmed)
    );
    assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(matches!(
        pin!(inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(SessionOwnerCommand::Event(
            SessionEvent::PeerOpenResolved {
                result: OpenResultCode::TargetRefused,
                ..
            }
        )))
    ));
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
}

#[tokio::test]
async fn cancelled_capacity_borrow_is_woken_by_close_and_inbox_destruction() {
    for destroy_inbox in [false, true] {
        let (mut root, ingress, mut inbox) = blocked_control();
        let original = held_control(&root) as *const SessionOwnerCommand;
        let SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved { request, .. }) =
            held_control(&root)
        else {
            panic!("the root owns the real completion");
        };
        let request = *request;
        let count = Arc::new(RegistrationWake::default());
        let waker = Waker::from(Arc::clone(&count));
        {
            let mut wait = pin!(root.rearm_full_commands());
            assert!(
                wait.as_mut()
                    .poll(&mut Context::from_waker(&waker))
                    .is_pending()
            );
        }
        assert_eq!(count.0.load(Ordering::SeqCst), 0);
        if destroy_inbox {
            drop(inbox);
        } else {
            inbox.close();
        }
        assert_eq!(count.0.load(Ordering::SeqCst), 1);
        assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
        assert_eq!(
            pin!(root.rearm_full_commands())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop())),
            Poll::Ready(OwnerCapacityRearm::Closed)
        );
        let SendCell::ClosedReturned(HeldCommand {
            storage: CommandStorage::Returned(command),
            ..
        }) = &root.cells[SendLane::Control.index()]
        else {
            panic!("closure retains the real Full return, not a replacement");
        };
        assert_eq!(command.as_ref() as *const SessionOwnerCommand, original);
        assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
        let Err(SessionMailboxTrySendError::Closed(rejected)) =
            ingress.try_send_event(SessionEvent::PeerOpenResolved {
                request,
                result: OpenResultCode::TargetRefused,
            })
        else {
            panic!("real admission must be closed");
        };
        assert!(matches!(*rejected,
            SessionOwnerCommand::Event(SessionEvent::PeerOpenResolved {
                request: actual, result: OpenResultCode::TargetRefused,
            }) if actual == request));
    }
}

struct RegistrationDropProbe {
    ingress: SessionOwnerIngress,
    drops: Arc<AtomicUsize>,
    busy: Arc<AtomicUsize>,
}

impl Wake for RegistrationDropProbe {
    fn wake(self: Arc<Self>) {}
}

impl Drop for RegistrationDropProbe {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
        if matches!(
            self.ingress.observe_capacity(),
            Err(SessionMailboxObserverBusy)
        ) {
            self.busy.fetch_add(1, Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn owning_registration_is_destroyed_before_its_exclusive_lease_is_released() {
    let (mut root, ingress, _inbox) = blocked_control();
    let drops = Arc::new(AtomicUsize::new(0));
    let busy = Arc::new(AtomicUsize::new(0));
    let waker = Waker::from(Arc::new(RegistrationDropProbe {
        ingress: ingress.clone(),
        drops: Arc::clone(&drops),
        busy: Arc::clone(&busy),
    }));
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    drop(waker);
    // With no test-owned strong reference, only the persistent registration
    // can keep this probe alive. Busy alone would also pass after early Drop.
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert_eq!(busy.load(Ordering::SeqCst), 0);
    drop(root);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(busy.load(Ordering::SeqCst), 1);
    let mut replacement = ingress.observe_capacity().unwrap();
    assert!(
        pin!(replacement.changed())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    // This is metadata/registration destruction, not whole-root settlement.
}

struct PanicRegistrationWake;

impl Wake for PanicRegistrationWake {
    fn wake(self: Arc<Self>) {
        panic!("real capacity wake interrupted after mailbox custody commit");
    }
}

#[tokio::test]
async fn cancelled_capacity_wake_unwind_retains_root_box_and_real_mailbox_receipt() {
    let (mut root, _, mut inbox) = blocked_control();
    let original = held_control(&root) as *const SessionOwnerCommand;
    let waker = Waker::from(Arc::new(PanicRegistrationWake));
    {
        let mut wait = pin!(root.rearm_full_commands());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(&waker))
                .is_pending()
        );
    }
    assert!(
        catch_unwind(AssertUnwindSafe(|| {
            pin!(inbox.recv_next())
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
        }))
        .is_err()
    );
    assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
    assert!(matches!(
        pin!(inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(SessionOwnerCommand::Event(
            SessionEvent::PeerOpenResolved {
                result: OpenResultCode::PolicyDenied,
                ..
            }
        )))
    ));
    assert_eq!(
        pin!(root.rearm_full_commands())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(OwnerCapacityRearm::Rearmed)
    );
    assert_eq!(held_control(&root) as *const SessionOwnerCommand, original);
    // Delivery from committed prefetch is not a second capacity epoch. This
    // checks the public observation, without exposing the mailbox's fields.
    {
        let mut wait = pin!(root.capacity.changed());
        assert!(
            wait.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::MailboxOwned);
    assert!(matches!(
        pin!(inbox.recv_next())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Some(SessionOwnerCommand::Event(
            SessionEvent::PeerOpenResolved {
                result: OpenResultCode::TargetRefused,
                ..
            }
        )))
    ));
    assert_eq!(root.drive_one_unit(), OwnerReadyDrive::NoRunnable);
}
