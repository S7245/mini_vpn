//! Published readiness epochs and cancellable observation for one actor.
//!
//! Owns epoch and one controlled Output registration, not terminal policy or
//! business resources. Reserved epochs stay unpublished; only explicit output
//! edges consume that registration. Entries: publish/wait_after/register_output/
//! advance_output. Real regressions live in tests/mod.rs and owner_output_ready.

use super::{
    Arc, AtomicU64, Mutex, Ordering, QuinnLegActorError, Readiness, SharedReadiness, wait_cancel,
    watch,
};
use std::task::Waker;

impl Readiness {
    pub(super) fn new() -> Self {
        let (sender, _) = watch::channel(0);
        Self {
            shared: Arc::new(SharedReadiness {
                epoch: AtomicU64::new(0),
                output: Mutex::new(None),
            }),
            sender,
        }
    }

    pub(super) fn current(&self) -> u64 {
        *self.sender.borrow()
    }

    pub(super) fn reserved(&self) -> u64 {
        self.shared.epoch.load(Ordering::Acquire)
    }

    pub(super) fn same_source(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    pub(super) fn advance(&self) {
        let next = self.reserve_next();
        self.publish(next);
    }

    pub(super) fn reserve_next(&self) -> u64 {
        self.shared
            .epoch
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                Some(value.saturating_add(1))
            })
            .unwrap_or_else(|value| value)
            .saturating_add(1)
    }

    pub(super) fn publish(&self, next: u64) {
        self.sender.send_if_modified(|published| {
            if *published >= next {
                false
            } else {
                *published = next;
                true
            }
        });
    }

    pub(super) fn register_output(&self, waker: &Waker) -> Result<(), QuinnLegActorError> {
        // Exclusive Bound ownership excludes both legacy borrowed wait APIs.
        // Do not assert this for unbound actors or create another subscriber.
        if self.sender.receiver_count() != 0 {
            return Err(QuinnLegActorError::Protocol(
                "owner Output registration overlaps a legacy readiness observer",
            ));
        }
        let prepared = waker.clone();
        let replaced = {
            let mut output = self.shared.output.lock().map_err(|_| {
                QuinnLegActorError::Protocol("actor Output registration was poisoned")
            })?;
            output.replace(prepared)
        };
        drop(replaced);
        Ok(())
    }

    pub(super) fn advance_output(&self) -> Result<(), QuinnLegActorError> {
        self.advance();
        let registered = {
            let mut output = self.shared.output.lock().map_err(|_| {
                QuinnLegActorError::Protocol("actor Output registration was poisoned")
            })?;
            output.take()
        };
        if let Some(waker) = registered {
            waker.wake();
        }
        Ok(())
    }

    pub(super) async fn wait_after(&self, previous: u64) -> Result<u64, QuinnLegActorError> {
        let current = self.current();
        if current != previous {
            return Ok(current);
        }
        let mut receiver = self.sender.subscribe();
        let current = self.current();
        if current != previous {
            return Ok(current);
        }
        loop {
            receiver
                .changed()
                .await
                .map_err(|_| QuinnLegActorError::ReadinessClosed)?;
            let current = *receiver.borrow_and_update();
            if current != previous {
                return Ok(current);
            }
        }
    }

    pub(super) async fn wait_after_or_cancel(
        &self,
        previous: u64,
        mut cancel: watch::Receiver<bool>,
    ) -> Result<Option<u64>, QuinnLegActorError> {
        tokio::select! {
            biased;
            () = wait_cancel(&mut cancel) => Ok(None),
            progress = self.wait_after(previous) => progress.map(Some),
        }
    }
}
