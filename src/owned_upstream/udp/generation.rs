//! One original UDP channel and fragment owner across primary generations.
//! Root alone freezes/publishes; producer and consumer seal actual old objects.
//! Seals never reset or regress. No forwarding task or per-packet root lock.
//! Entry: channel, freeze, prepare_publication; tests live in generation/tests.
use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};
use tokio::sync::Notify;

pub(crate) struct GenerationControl {
    publication: std::sync::Mutex<()>,
    phase: AtomicU64,
    producer: AtomicU64,
    consumer: AtomicU64,
    consumer_disposition: AtomicU8, // 0 present, 1 destructing, 2 physically absent
    invalid: AtomicBool,
    pub(crate) changed: Notify,
    discarded_messages: AtomicU64,
    discarded_bytes: AtomicU64,
}
impl Default for GenerationControl {
    fn default() -> Self {
        Self {
            publication: std::sync::Mutex::new(()),
            phase: AtomicU64::new(2),
            producer: AtomicU64::new(0),
            consumer: AtomicU64::new(0),
            consumer_disposition: AtomicU8::new(0),
            invalid: AtomicBool::new(false),
            changed: Notify::new(),
            discarded_messages: AtomicU64::new(0),
            discarded_bytes: AtomicU64::new(0),
        }
    }
}
pub(crate) struct Publication<'a> {
    _guard: std::sync::MutexGuard<'a, ()>,
    control: &'a GenerationControl,
    phase: u64,
}
impl Publication<'_> {
    pub(crate) fn publish(self) {
        self.control.phase.store(self.phase, Ordering::Release);
        let control = self.control;
        drop(self._guard);
        control.changed.notify_waiters();
    }
}
impl GenerationControl {
    pub(crate) fn epoch(&self) -> u64 {
        self.phase.load(Ordering::Acquire) >> 1
    }
    pub(crate) fn frozen(&self, epoch: u64) -> bool {
        self.phase.load(Ordering::Acquire) == (epoch << 1 | 1)
    }
    pub(crate) fn freeze(&self, epoch: u64) -> bool {
        if epoch == 0 || epoch > u64::MAX >> 1 {
            return false;
        }
        let active = epoch << 1;
        let ok = self
            .phase
            .compare_exchange(active, active | 1, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
            || self.frozen(epoch);
        // Root notifies after its complete ticket/phase transaction releases locks.
        ok
    }
    pub(crate) fn producer_sealed(&self, epoch: u64) {
        if self.frozen(epoch) {
            self.producer.fetch_max(epoch, Ordering::AcqRel);
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn seal_unstarted(&self, epoch: u64) {
        if self.frozen(epoch) {
            self.producer.fetch_max(epoch, Ordering::AcqRel);
            self.consumer.fetch_max(epoch, Ordering::AcqRel);
        }
    }
    fn consumer_sealed(&self, epoch: u64) {
        if self.frozen(epoch) {
            self.consumer.fetch_max(epoch, Ordering::AcqRel);
            self.changed.notify_waiters();
        }
    }
    pub(crate) fn sealed(&self, epoch: u64) -> bool {
        self.frozen(epoch)
            && self.producer.load(Ordering::Acquire) == epoch
            && match self.consumer_disposition.load(Ordering::Acquire) {
                0 => self.consumer.load(Ordering::Acquire) == epoch,
                2 => true,
                _ => false,
            }
    }
    pub(crate) fn valid(&self) -> bool {
        !self.invalid.load(Ordering::Acquire)
    }
    pub(crate) async fn wait_frozen(&self, epoch: u64) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.frozen(epoch) {
                return;
            }
            notified.await;
        }
    }
    pub(crate) async fn wait_sealed(&self, epoch: u64) {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.sealed(epoch) {
                return;
            }
            notified.await;
        }
    }
    pub(crate) fn prepare_publication(&self, old: u64, new: u64) -> Option<Publication<'_>> {
        let guard = match self.publication.lock() {
            Ok(guard) => guard,
            Err(_) => {
                self.invalid.store(true, Ordering::Release);
                return None;
            }
        };
        if !self.sealed(old)
            || !self.valid()
            || self.consumer_disposition.load(Ordering::Acquire) != 0
            || old.checked_add(1) != Some(new)
            || new > u64::MAX >> 1
        {
            return None;
        }
        Some(Publication {
            _guard: guard,
            control: self,
            phase: new << 1,
        })
    }
}
struct Envelope {
    epoch: u64,
    bytes: Vec<u8>,
}
pub(crate) struct GenerationSender {
    sender: Option<mpsc::Sender<Envelope>>,
    control: Arc<GenerationControl>,
}
impl Clone for GenerationSender {
    fn clone(&self) -> Self {
        Self {
            sender: self.sender.clone(),
            control: self.control.clone(),
        }
    }
}
impl GenerationSender {
    pub(crate) async fn send(&self, epoch: u64, bytes: Vec<u8>) -> Result<(), ()> {
        match &self.sender {
            Some(tx) => tx.send(Envelope { epoch, bytes }).await.map_err(|_| ()),
            None => Err(()),
        }
    }
    pub(crate) fn is_closed(&self) -> bool {
        self.sender.as_ref().is_none_or(|tx| tx.is_closed())
    }
    pub(crate) async fn closed(&self) {
        if let Some(tx) = &self.sender {
            tx.closed().await;
        }
    }
}
impl Drop for GenerationSender {
    fn drop(&mut self) {
        // Notify only after the actual sender is destroyed: frozen source may
        // be waiting for terminal EOF rather than a new publication.
        drop(self.sender.take());
        self.control.changed.notify_waiters();
    }
}
pub(super) struct Source {
    receiver: Option<mpsc::Receiver<Envelope>>,
    reassembler: FragReassembler,
    control: Arc<GenerationControl>,
}
pub(crate) fn channel(
    control: Arc<GenerationControl>,
    capacity: usize,
) -> (GenerationSender, UdpDownlinkSource) {
    let (sender, receiver) = mpsc::channel(capacity);
    (
        GenerationSender {
            sender: Some(sender),
            control: control.clone(),
        },
        UdpDownlinkSource {
            inner: UdpDownlinkSourceInner::Generation(Source {
                receiver: Some(receiver),
                reassembler: FragReassembler::new(),
                control,
            }),
        },
    )
}
impl Source {
    fn discard(&mut self) {
        if let Some(receiver) = &mut self.receiver {
            while let Ok(envelope) = receiver.try_recv() {
                self.control
                    .discarded_messages
                    .fetch_add(1, Ordering::Relaxed);
                self.control
                    .discarded_bytes
                    .fetch_add(envelope.bytes.len() as u64, Ordering::Relaxed);
            }
        }
        if !self.reassembler.retire_generation() {
            self.control.invalid.store(true, Ordering::Release);
        }
    }
    pub(super) async fn recv(&mut self, clock: &Instant) -> Option<UdpDownlinkEvent> {
        loop {
            let control = self.control.clone();
            let notified = control.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let epoch = control.epoch();
            if control.frozen(epoch) {
                if control.producer.load(Ordering::Acquire) == epoch
                    && control.consumer.load(Ordering::Acquire) != epoch
                {
                    self.discard();
                    control.consumer_sealed(epoch);
                    return Some(UdpDownlinkEvent::Ignored);
                }
                if self
                    .receiver
                    .as_ref()
                    .is_some_and(|receiver| receiver.is_closed() && receiver.is_empty())
                    && control.consumer.load(Ordering::Acquire) == epoch
                {
                    return None;
                }
                notified.await;
                continue;
            }
            let envelope = tokio::select! { biased; _ = &mut notified => continue, item = self.receiver.as_mut()?.recv() => item? };
            if envelope.epoch != epoch || control.frozen(epoch) {
                return Some(UdpDownlinkEvent::Ignored);
            }
            let Some(meta) = decode_packet_meta(&envelope.bytes) else {
                return Some(UdpDownlinkEvent::Ignored);
            };
            let association = LocalAssociationId::new(meta.assoc_id);
            let payload = self.reassembler.accept(&meta, clock.elapsed().as_secs());
            return Some(match (association, payload) {
                (Some(association), Some(payload)) => {
                    UdpDownlinkEvent::Deliver(UdpDownlink::new(association, Bytes::from(payload)))
                }
                _ => UdpDownlinkEvent::Ignored,
            });
        }
    }
    #[cfg(test)]
    pub(super) fn pending(&self) -> usize {
        self.receiver.as_ref().map_or(0, |rx| rx.len())
    }
    pub(super) fn sweep(&mut self, now: u64, ttl: u64) {
        self.reassembler.sweep(now, ttl);
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        let control = self.control.clone();
        let guard = match control.publication.lock() {
            Ok(guard) => guard,
            Err(poisoned) => {
                control.invalid.store(true, Ordering::Release);
                poisoned.into_inner()
            }
        };
        control.consumer_disposition.store(1, Ordering::Release);
        drop(guard); // Mark retirement before wake-capable physical destruction.
        if let Some(receiver) = &mut self.receiver {
            receiver.close();
        }
        self.discard();
        drop(self.receiver.take());
        self.control
            .consumer_disposition
            .store(2, Ordering::Release);
        self.control.changed.notify_waiters();
    }
}

#[cfg(test)]
pub(crate) mod tests;
