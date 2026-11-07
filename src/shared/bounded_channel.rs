//! Bounded channel's single physical owner: fixed slots, logical credits,
//! callback custody and final inventory. Every instance has its own cold Core;
//! no global coordinator is introduced. Typed values validate/dispose their own
//! fields; Flow/generation identity, ACK and deadlines remain with callers.
//! Entry: channel/native_channel; private characterization is in tests/ and
//! native/tests/. Legacy delegates to its original Tokio channel.
//! Private tests plus original reader/continuous/Service characterize both paths.
use tokio::sync::mpsc;
mod credit;
mod native;
mod ring;
mod value;
pub(crate) use credit::ReserveError;
pub(crate) use native::{Fault, Observer};
pub(crate) use value::Value;

enum Tx<T: Value> {
    Legacy(mpsc::Sender<T>),
    Native(native::Sender<T>),
}
enum Rx<T: Value> {
    Legacy(mpsc::Receiver<T>),
    Native(native::Receiver<T>),
}
enum WeakTx<T: Value> {
    Legacy(mpsc::WeakSender<T>),
    Native(native::WeakSender<T>),
}
enum Grant<'a, T: Value> {
    Legacy(mpsc::Permit<'a, T>),
    Native(native::Permit<'a, T>),
}
pub(crate) struct Sender<T: Value>(Tx<T>);
pub(crate) struct Receiver<T: Value>(Rx<T>);
pub(crate) struct WeakSender<T: Value>(WeakTx<T>);
#[derive(Debug)]
pub(crate) struct SendError<T>(pub(crate) T, pub(crate) ReserveError);
pub(crate) struct Permit<'a, T: Value>(Grant<'a, T>);

pub(crate) fn channel<T: Value>(capacity: usize) -> (Sender<T>, Receiver<T>) {
    let (tx, rx) = mpsc::channel(capacity);
    (Sender(Tx::Legacy(tx)), Receiver(Rx::Legacy(rx)))
}
pub(crate) fn native_layout_bytes<T: Value>(
    capacity: usize,
) -> Result<usize, bytes::MeteredCopyError> {
    native::layout_bytes::<T>(capacity, 4096)
}
pub(crate) fn native_channel<T: Value<Disposition = ()>>(
    capacity: usize,
    meter: &bytes::BackingMeter,
    clock: crate::shared::errors::FailureClock,
) -> Result<(Sender<T>, Receiver<T>, Observer<T>), bytes::MeteredCopyError> {
    let (tx, rx, observer) = native::prepare(capacity, 4096, meter, clock)?;
    Ok((Sender(Tx::Native(tx)), Receiver(Rx::Native(rx)), observer))
}
/// Cold-only physical bounds selected by the original caller authority.
#[derive(Clone, Copy)]
pub(crate) struct NativeCapacity {
    pub(crate) messages: usize,
    pub(crate) waiters: usize,
}
pub(crate) fn native_layout_with<T: Value>(
    limits: NativeCapacity,
) -> Result<usize, bytes::MeteredCopyError> {
    native::layout_bytes::<T>(limits.messages, limits.waiters)
}
pub(crate) fn native_with<T: Value>(
    limits: NativeCapacity,
    meter: &bytes::BackingMeter,
    clock: crate::shared::errors::FailureClock,
    disposition: &mut Option<T::Disposition>,
) -> Result<(Sender<T>, Receiver<T>, Observer<T>), bytes::MeteredCopyError> {
    let (tx, rx, observer) =
        native::prepare_with(limits.messages, limits.waiters, meter, clock, disposition)?;
    Ok((Sender(Tx::Native(tx)), Receiver(Rx::Native(rx)), observer))
}
impl<T: Value> std::fmt::Debug for Sender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sender").field("capacity", &self.capacity()).field("closed", &self.is_closed()).finish()
    }
}
impl<T: Value> Clone for Sender<T> {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            Tx::Legacy(tx) => Tx::Legacy(tx.clone()),
            Tx::Native(tx) => Tx::Native(tx.clone()),
        })
    }
}
impl<T: Value> Sender<T> {
    pub(crate) async fn send(&self, value: T) -> Result<(), SendError<T>> {
        match self.reserve().await {
            Ok(permit) => {
                permit.send(value);
                Ok(())
            }
            Err(error) => Err(SendError(value, error)),
        }
    }
    /// Synchronous admission keeps pressure, ordinary close and Q distinct.
    pub(crate) fn try_reserve(&self) -> Result<Permit<'_, T>, ReserveError> {
        match &self.0 {
            Tx::Legacy(tx) => tx.try_reserve().map(|p| Permit(Grant::Legacy(p))).map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => ReserveError::Capacity,
                mpsc::error::TrySendError::Closed(_) => ReserveError::Closed,
            }),
            Tx::Native(tx) => tx.try_reserve().map(|p| Permit(Grant::Native(p))),
        }
    }
    /// Stop new Native grants; existing Permits retain their publication promise.
    pub(crate) fn close_native_admission(&self) {
        if let Tx::Native(tx) = &self.0 { tx.close_admission(); }
    }
    /// Original bound capabilities; this does not expose a bare Q marker.
    pub(crate) fn native_context(&self) -> Option<(&bytes::BackingMeter, &crate::shared::errors::FailureClock)> {
        match &self.0 { Tx::Legacy(_) => None, Tx::Native(tx) => Some(tx.context()) }
    }
    pub(crate) async fn reserve(&self) -> Result<Permit<'_, T>, ReserveError> {
        match &self.0 {
            Tx::Legacy(tx) => tx
                .reserve()
                .await
                .map(|p| Permit(Grant::Legacy(p)))
                .map_err(|_| ReserveError::Closed),
            Tx::Native(tx) => tx.reserve().await.map(|p| Permit(Grant::Native(p))),
        }
    }
    pub(crate) fn try_send(&self, value: T) -> Result<(), mpsc::error::TrySendError<T>> {
        match &self.0 {
            Tx::Legacy(tx) => tx.try_send(value),
            Tx::Native(tx) => match tx.try_reserve() {
                Ok(permit) => {
                    permit.send(value);
                    Ok(())
                }
                Err(ReserveError::Capacity) => Err(mpsc::error::TrySendError::Full(value)),
                Err(_) => Err(mpsc::error::TrySendError::Closed(value)),
            },
        }
    }
    pub(crate) fn capacity(&self) -> usize {
        match &self.0 {
            Tx::Legacy(tx) => tx.capacity(),
            Tx::Native(tx) => tx.capacity(),
        }
    }
    pub(crate) fn max_capacity(&self) -> usize {
        match &self.0 {
            Tx::Legacy(tx) => tx.max_capacity(),
            Tx::Native(tx) => tx.max_capacity(),
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        match &self.0 {
            Tx::Legacy(tx) => tx.is_closed(),
            Tx::Native(tx) => tx.is_closed(),
        }
    }
    pub(crate) fn downgrade(&self) -> WeakSender<T> {
        WeakSender(match &self.0 {
            Tx::Legacy(tx) => WeakTx::Legacy(tx.downgrade()),
            Tx::Native(tx) => WeakTx::Native(tx.downgrade()),
        })
    }
}
impl<T: Value> Clone for WeakSender<T> {
    fn clone(&self) -> Self {
        Self(match &self.0 {
            WeakTx::Legacy(tx) => WeakTx::Legacy(tx.clone()),
            WeakTx::Native(tx) => WeakTx::Native(tx.clone()),
        })
    }
}
impl<T: Value> WeakSender<T> {
    pub(crate) fn upgrade(&self) -> Option<Sender<T>> {
        match &self.0 {
            WeakTx::Legacy(tx) => tx.upgrade().map(|t| Sender(Tx::Legacy(t))),
            WeakTx::Native(tx) => tx.upgrade().map(|t| Sender(Tx::Native(t))),
        }
    }
}
impl<T: Value> Permit<'_, T> {
    pub(crate) fn send(self, value: T) {
        match self.0 {
            Grant::Legacy(p) => {
                p.send(value);
            }
            Grant::Native(p) => p.send(value),
        }
    }
}
impl<T: Value> Receiver<T> {
    pub(crate) fn max_capacity(&self) -> usize {
        match &self.0 { Rx::Legacy(rx) => rx.max_capacity(), Rx::Native(rx) => rx.max_capacity() }
    }
    /// Physical EOF candidate only; the original generation still owns JOIN
    /// and consumer witnesses. Old borrowed Permits keep a real sender alive.
    pub(crate) fn producers_gone(&self) -> bool {
        match &self.0 {
            Rx::Legacy(rx) => rx.is_closed(),
            Rx::Native(rx) => rx.producers_gone(),
        }
    }
    pub(crate) fn close(&mut self) {
        match &mut self.0 {
            Rx::Legacy(rx) => rx.close(),
            Rx::Native(rx) => rx.close(),
        }
    }
    /// The original generation owner must exclude all publishers and readers;
    /// this read-only check never creates a producer/consumer witness.
    pub(crate) unsafe fn empty_under_producer_seal(&mut self) -> bool {
        match &mut self.0 {
            Rx::Legacy(_) => false,
            Rx::Native(rx) => unsafe { rx.empty_under_producer_seal() },
        }
    }
    pub(crate) async fn recv(&mut self) -> Option<T> {
        match &mut self.0 {
            Rx::Legacy(rx) => rx.recv().await,
            Rx::Native(rx) => rx.recv().await,
        }
    }
    pub(crate) fn try_recv(&mut self) -> Result<T, mpsc::error::TryRecvError> {
        match &mut self.0 {
            Rx::Legacy(rx) => rx.try_recv(),
            Rx::Native(rx) => rx.try_recv(),
        }
    }
    pub(crate) fn poll_recv(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<T>> {
        match &mut self.0 {
            Rx::Legacy(rx) => rx.poll_recv(cx),
            Rx::Native(rx) => rx.poll_recv(cx),
        }
    }
}
#[cfg(test)]
mod tests;
