//! Cold session boundary of the original device. Receipts cover owned buffers,
//! not unread kernel/network packets. No DATA scheduler or retry authority.
//! Tests: device/tests/cut.rs and harness/loopback/tests.rs.
use super::*;

/// Non-cloneable permission. Dropping it leaves ingress paused.
pub struct DeviceCutReceipt {
    authority: Arc<()>,
    sequence: u64,
    pub discarded_packets: u64,
    pub discarded_backing: u64,
}

#[derive(Default)]
pub(crate) struct CutBoundary {
    authority: Arc<()>,
    sequence: u64,
    issued: bool,
    pending: bool,
}
impl CutBoundary {
    #[cfg(test)]
    pub(crate) fn sequence_for_test(&self) -> u64 {
        self.sequence
    }

    pub(crate) fn begin(&mut self) -> std::io::Result<()> {
        if self.issued || self.sequence == u64::MAX {
            return Err(std::io::Error::from(ErrorKind::AlreadyExists));
        }
        self.pending = true;
        Ok(())
    }
    pub(crate) fn active(&self) -> bool {
        self.pending || self.issued
    }
    pub(crate) fn issue(&mut self, discarded: (u64, u64)) -> DeviceCutReceipt {
        // check_new precedes any destructive operation; no await/fallible work
        // follows the final physical drain and this publication.
        self.sequence += 1;
        self.issued = true;
        DeviceCutReceipt {
            authority: self.authority.clone(),
            sequence: self.sequence,
            discarded_packets: discarded.0,
            discarded_backing: discarded.1,
        }
    }
    pub(crate) fn validate(&self, receipt: &DeviceCutReceipt) -> std::io::Result<()> {
        if !self.issued
            || self.sequence != receipt.sequence
            || !Arc::ptr_eq(&self.authority, &receipt.authority)
        {
            return Err(std::io::Error::from(ErrorKind::InvalidInput));
        }
        Ok(())
    }
    pub(crate) fn resumed(&mut self) {
        self.issued = false;
        self.pending = false;
    }
}

pub(crate) fn discard_packets(packets: impl Iterator<Item = BytesMut>, totals: &mut (u64, u64)) {
    for packet in packets {
        totals.0 = totals.0.saturating_add(1);
        totals.1 = totals.1.saturating_add(packet.capacity() as u64);
        drop(packet);
    }
}

#[derive(Default)]
pub(super) struct PauseControl {
    pub(super) requested: AtomicBool,
    pub(super) paused: AtomicBool,
    pub(super) changed: Arc<Notify>,
    packets: AtomicU64,
    backing: AtomicU64,
}
impl PauseControl {
    pub(super) async fn requested(&self) {
        loop {
            let wait = self.changed.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if self.requested.load(Ordering::Acquire) {
                return;
            }
            wait.await;
        }
    }
    pub(super) fn discard(&self, packet: PumpPacket) {
        let backing = packet.backing;
        let adopted = packet.adopted;
        drop(packet);
        if adopted {
            self.packets.fetch_add(1, Ordering::Relaxed);
            self.backing.fetch_add(backing, Ordering::Relaxed);
        }
    }
    pub(super) async fn park(&self, control: &PumpControl) -> bool {
        // Called only after the original read/reserve future AND packet dropped.
        if !self.paused.swap(true, Ordering::AcqRel) {
            control.notify(pump_control::PAUSE);
        }
        loop {
            let wait = self.changed.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if !self.requested.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                biased;
                _ = control.stopped() => return false,
                _ = wait => {},
            }
        }
        self.paused.store(false, Ordering::Release);
        true
    }
}

impl TunIngressPump {
    #[cfg(test)]
    pub(crate) fn original_actor_id(&self) -> Option<tokio::task::Id> {
        match &self.task {
            PumpTask::Running(task) => Some(task.id()),
            PumpTask::Observed(_) => None,
        }
    }

    pub(crate) fn check_cut(&self) -> std::io::Result<()> {
        if !self.cut_supported
            || self.control.native_quarantined()
            || self.control.stop.load(Ordering::Acquire)
            || self.control.failed.load(Ordering::Acquire)
        {
            return Err(std::io::Error::from(ErrorKind::Unsupported));
        }
        Ok(())
    }
    pub(crate) async fn cut(&mut self) -> std::io::Result<(u64, u64)> {
        self.check_cut()?;
        let control = self.control.clone();
        if !control.pause.requested.swap(true, Ordering::AcqRel) {
            control.notify(pump_control::PAUSE);
        }
        loop {
            let wait = control.pause.changed.notified();
            tokio::pin!(wait);
            wait.as_mut().enable();
            if control.pause.paused.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                biased;
                _ = &mut wait => {},
                _ = self.observe_join() => return Err(control.failure_error()),
            }
        }
        if control.native_quarantined()
            || control.failed.load(Ordering::Acquire)
            || control.stop.load(Ordering::Acquire)
        {
            return Err(device_closed());
        }
        if control.producer_backing.load(Ordering::Acquire) != 0 {
            return Err(std::io::Error::other("paused producer still charged"));
        }
        let mut discarded = (
            control.pause.packets.swap(0, Ordering::Relaxed),
            control.pause.backing.swap(0, Ordering::Relaxed),
        );
        while let Ok(packet) = self.receiver.try_recv() {
            discarded.0 = discarded.0.saturating_add(1);
            discarded.1 = discarded.1.saturating_add(packet.backing);
            drop(packet);
        }
        if control.queued_backing.load(Ordering::Acquire) != 0 {
            return Err(std::io::Error::other("paused receiver still charged"));
        }
        if control.native_quarantined()
            || control.failed.load(Ordering::Acquire)
            || control.stop.load(Ordering::Acquire)
        {
            return Err(device_closed());
        }
        Ok(discarded)
    }
    pub(crate) fn validate_resume(&self) -> std::io::Result<()> {
        let live = matches!(&self.task, PumpTask::Running(task) if !task.is_finished());
        if !live
            || self.control.native_quarantined()
            || self.control.stop.load(Ordering::Acquire)
            || self.control.failed.load(Ordering::Acquire)
            || !self.control.pause.requested.load(Ordering::Acquire)
            || !self.control.pause.paused.load(Ordering::Acquire)
            || self.control.producer_backing.load(Ordering::Acquire) != 0
            || self.control.queued_backing.load(Ordering::Acquire) != 0
            || !self.receiver.is_empty()
        {
            return Err(std::io::Error::other(
                "original paused pump witness unavailable",
            ));
        }
        Ok(())
    }
    pub(crate) fn resume(&self) {
        if self.control.pause.requested.swap(false, Ordering::AcqRel) {
            self.control.notify(pump_control::PAUSE);
        }
    }
}
