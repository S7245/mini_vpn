//! Opaque control custody for the one optional Native pump role.
//! DATA stays in the transport implementation. This owner retains the original
//! task, error and terminal receipt through actual join; it never aborts a task.
//! Only crate transport implementations can prepare work. Public callers cannot
//! supply a future or spawn through this interface. Tests live with that owner.
use std::any::Any;
use std::future::Future;
use std::io;
use std::mem::ManuallyDrop;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::Arc;
use crate::shared::errors::FailureClock;
use std::task::{Context, Poll};
use tokio::sync::oneshot;
use tokio::task::{JoinError, JoinHandle};

pub struct PreparedNativePump {
    work: Box<dyn Work>,
}

pub(crate) trait Work: Send {
    fn commit(
        self: Box<Self>,
        clock: Option<FailureClock>,
        gate: Option<oneshot::Receiver<()>>,
    ) -> RunningNativePump;
}
pub(crate) trait Control: Send + Sync {
    fn stop(&self);
    fn quarantined(&self) -> bool;
    fn quarantine(&self);
    fn unwind(&self, payload: Box<dyn Any + Send>);
    fn take_failure(&self) -> Option<Failure>;
    fn error_order(&self) -> Option<usize>;
    #[cfg(test)]
    fn source_read_bytes(&self) -> usize;
}
pub(crate) enum FailureValue {
    Io(io::Error),
    Panic(Box<dyn Any + Send>),
}
pub(crate) struct Failure {
    value: ManuallyDrop<FailureValue>,
    pub(crate) order: Option<usize>,
}
impl Failure {
    pub(crate) fn new(value: FailureValue, order: Option<usize>) -> Self {
        Self {
            value: ManuallyDrop::new(value),
            order,
        }
    }
    pub(crate) fn value_ref(&self) -> &FailureValue { &self.value }
    pub(crate) fn into_value(self) -> FailureValue {
        ManuallyDrop::into_inner(self.value)
    }
}

pub(crate) trait Finished: Send {
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<Receipt>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Receipt {
    pub(crate) source_read_bytes: usize,
    pub(crate) sent_items: usize,
    pub(crate) sent_bytes: usize,
    pub(crate) received_items: usize,
    pub(crate) received_bytes: usize,
    pub(crate) forwarded_bytes: usize,
    pub(crate) queue_discarded_items: usize,
    pub(crate) queue_discarded_bytes: usize,
    pub(crate) reader_discarded_bytes: usize,
    pub(crate) producer_discarded_bytes: usize,
}
enum Task {
    Running(JoinHandle<ManuallyDrop<Box<dyn Finished>>>),
    Joined(ManuallyDrop<Box<dyn Finished>>),
    Failed(ManuallyDrop<JoinError>),
    Disposed,
}
pub(crate) struct RunningNativePump {
    task: Task,
    control: Arc<dyn Control>,
    failure: Option<Failure>,
    receipt: Option<Receipt>,
}
impl PreparedNativePump {
    pub(crate) fn new(work: Box<dyn Work>) -> Self {
        Self { work }
    }
    pub(crate) fn commit(self, clock: Option<FailureClock>) -> RunningNativePump {
        self.work.commit(clock, None)
    }
    pub(crate) fn commit_gated(
        self,
        clock: Option<FailureClock>,
        gate: oneshot::Receiver<()>,
    ) -> RunningNativePump {
        self.work.commit(clock, Some(gate))
    }
}
impl RunningNativePump {
    pub(crate) fn new(
        task: JoinHandle<ManuallyDrop<Box<dyn Finished>>>,
        control: Arc<dyn Control>,
    ) -> Self {
        Self {
            task: Task::Running(task),
            control,
            failure: None,
            receipt: None,
        }
    }
    pub(crate) fn stop(&self) {
        self.control.stop();
    }
    pub(crate) fn joined(&self) -> bool {
        matches!(self.task, Task::Joined(_))
    }
    pub(crate) fn quarantined(&self) -> bool {
        self.control.quarantined() || matches!(self.task, Task::Failed(_))
    }
    pub(crate) fn error_order(&self) -> Option<usize> {
        self.control.error_order()
    }
    pub(crate) fn take_failure(&mut self) -> Option<Failure> {
        self.failure.take().or_else(|| self.control.take_failure())
    }
    pub(crate) fn receipt(&self) -> Option<Receipt> {
        self.receipt
    }
    #[cfg(test)]
    pub(crate) fn source_read_bytes(&self) -> usize {
        self.control.source_read_bytes()
    }
    #[cfg(test)]
    pub(crate) fn unwind_for_test(
        &self,
        payload: Box<dyn Any + Send>,
    ) {
        self.control.unwind(payload);
    }
    pub(crate) fn poll_join(&mut self, cx: &mut Context<'_>) {
        if let Task::Running(task) = &mut self.task {
            match Pin::new(task).poll(cx) {
                Poll::Ready(Ok(value)) => self.task = Task::Joined(value),
                Poll::Ready(Err(error)) => {
                    self.control.quarantine();
                    self.task = Task::Failed(ManuallyDrop::new(error));
                }
                Poll::Pending => {}
            }
        }
    }
    pub(crate) fn poll_settle(&mut self, cx: &mut Context<'_>) -> Poll<Result<Receipt, ()>> {
        self.stop();
        self.poll_join(cx);
        if self.failure.is_none() {
            self.failure = self.control.take_failure();
        }
        if matches!(self.task, Task::Running(_)) {
            return Poll::Pending;
        }
        if self.quarantined() {
            return Poll::Ready(Err(()));
        }
        if let Some(receipt) = self.receipt {
            return Poll::Ready(Ok(receipt));
        }
        let Task::Joined(outcome) = &mut self.task else {
            return Poll::Pending;
        };
        match catch_unwind(AssertUnwindSafe(|| outcome.poll_dispose(cx))) {
            Ok(Poll::Ready(receipt)) if !self.control.quarantined() => {
                self.receipt = Some(receipt);
                Poll::Ready(Ok(receipt))
            }
            Ok(Poll::Pending) if self.control.quarantined() => Poll::Ready(Err(())),
            Ok(Poll::Pending) => Poll::Pending,
            Ok(_) => Poll::Ready(Err(())),
            Err(payload) => {
                self.control.unwind(payload);
                Poll::Ready(Err(()))
            }
        }
    }
    pub(crate) async fn settle(&mut self) -> Result<Receipt, ()> {
        std::future::poll_fn(|cx| self.poll_settle(cx)).await
    }
}
impl Drop for RunningNativePump {
    fn drop(&mut self) {
        // A dropped observer never reaches this owner. Forced owner destruction
        // requests stop but supplies no join/receipt; the detached outcome is not
        // evidence that session resources were settled.
        if self.receipt.is_none() {
            self.control.quarantine();
        }
        self.control.stop();
        if self.receipt.is_some() && !self.quarantined() {
            // Every original source/lease/DATA object has already been disposed.
            // Guard the now-empty outcome shell too; Q outcomes are never
            // implicitly destroyed when an observer/legacy outer disappears.
            if let Task::Joined(outcome) = std::mem::replace(&mut self.task, Task::Disposed) {
                if let Err(payload) =
                    catch_unwind(AssertUnwindSafe(|| drop(ManuallyDrop::into_inner(outcome))))
                {
                    self.control.unwind(payload);
                }
            }
        }
    }
}

/// The existing legacy supervisor owns this value alongside its original roles.
/// It is not a session join receipt; saved sessions retain RunningNativePump in F.
pub(crate) struct LegacyPump {
    pump: Option<RunningNativePump>,
    deadline: Option<tokio::time::Instant>,
}
impl LegacyPump {
    pub(crate) fn new(prepared: Option<PreparedNativePump>) -> Self {
        Self {
            pump: prepared.map(|pump| pump.commit(None)),
            deadline: None,
        }
    }
    pub(crate) fn stop(&mut self, timeout: std::time::Duration) {
        if self.deadline.is_none() {
            self.deadline = Some(tokio::time::Instant::now() + timeout);
        }
        if let Some(pump) = &self.pump {
            pump.stop();
        }
    }
    pub(crate) async fn finish(&mut self) {
        let (Some(pump), Some(deadline)) = (&mut self.pump, self.deadline) else {
            return;
        };
        match tokio::time::timeout_at(deadline, pump.settle()).await {
            Ok(Ok(_)) => {}
            _ => eprintln!(
                "Native pump terminal custody incomplete; forced legacy destruction is not session settlement"
            ),
        }
        // These are the original bounded failure values, not reader notices.
        while let Some(failure) = pump.take_failure() {
            let order = failure.order;
            match failure.into_value() {
                FailureValue::Io(error) => {
                    eprintln!(
                        "Native pump source error kind={:?} os={:?}",
                        error.kind(),
                        error.raw_os_error()
                    );
                    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
                        pump.control.unwind(payload);
                        break;
                    }
                }
                FailureValue::Panic(payload) => {
                    eprintln!("Native pump retained callback panic; custody remains quarantined");
                    // Legacy has no root failure ledger. Keep the original box
                    // in the pump's custody through forced legacy destruction.
                    pump.failure = Some(Failure::new(FailureValue::Panic(payload), order));
                    break;
                }
            }
        }
    }
}
