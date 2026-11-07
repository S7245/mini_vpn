//! Return only availability through the original queue's protected Producer.
//! Weak handles own neither FIFO storage nor refund/JOIN authority.
use super::{Root, OutputSpace, StockWaitError};
use bytes::MeteredCopyError;
use std::sync::{Arc, Weak};
use super::super::{AsyncByteBoundedFlowQueueInner, native_wait::{Role, RunError}};

#[derive(Clone)]
pub(in crate::tcp_downlink_pump) enum QueueReturn {
    Bytes(Weak<AsyncByteBoundedFlowQueueInner>),
    Leased(super::super::leased::StockReturn),
}
impl QueueReturn {
    pub(super) async fn wait_returned(&self, root: &Root) -> Result<(), StockWaitError> {
        root.returned()?; // A live writer returns typed pressure before role acquisition.
        match self {
            Self::Bytes(weak) => {
                let Some(queue) = weak.upgrade() else {
                    if root.returned()? { return Ok(()); }
                    root.reject();
                    return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
                };
                let Some(waiters) = queue.waiters.get() else {
                    root.reject();
                    return Err(StockWaitError::Admission(MeteredCopyError::Quarantined));
                };
                match waiters.run(Role::Producer, async {
                    loop {
                        let notified = queue.not_full.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        if root.returned()? { return Ok(()); }
                        notified.await;
                    }
                }).await {
                    Ok(result) => result,
                    Err(RunError::Closed) => Err(StockWaitError::Closed),
                    Err(RunError::Quarantined) => Err(StockWaitError::Admission(MeteredCopyError::Quarantined)),
                }
            }
            Self::Leased(queue) => queue.wait_returned(root).await,
        }
    }
    pub(super) fn notify(&self) -> bool {
        match self {
            Self::Bytes(weak) => match weak.upgrade() {
                Some(queue) => { queue.not_full.notify_waiters(); true }
                None => false,
            },
            Self::Leased(weak) => weak.notify(),
        }
    }
    pub(super) async fn wait_prepare(&self, root: Arc<Root>, max: usize) -> Result<OutputSpace, StockWaitError> {
        match self {
            Self::Bytes(weak) => {
                let Some(queue) = weak.upgrade() else { root.reject(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined)); };
                let Some(waiters) = queue.waiters.get() else { root.reject(); return Err(StockWaitError::Admission(MeteredCopyError::Quarantined)); };
                let result = waiters.run(Role::Producer, async {
                    loop {
                        let notified = queue.not_full.notified();
                        tokio::pin!(notified);
                        notified.as_mut().enable();
                        if waiters.is_closed() { return Err(StockWaitError::Closed); }
                        if let Some(space) = root.reserve(max, true).map_err(StockWaitError::Admission)? { return Ok(space); }
                        notified.await;
                    }
                }).await;
                match result {
                    Ok(result) => result,
                    Err(RunError::Closed) => Err(StockWaitError::Closed),
                    Err(RunError::Quarantined) => Err(StockWaitError::Admission(MeteredCopyError::Quarantined)),
                }
            }
            Self::Leased(weak) => weak.wait_prepare(root, max).await,
        }
    }
}
