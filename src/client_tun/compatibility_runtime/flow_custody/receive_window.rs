//! Per-flow storage credit. The physical FlowMirror owns the only issuer; its
//! reader owns cumulative consumption through join and disposal. Neither side
//! owns DATA, physical ACK, provider retirement, or a second registry.
//! Only confirmed pending-prefix admission extends the absolute ceiling.
//! Tests live in receive_window/tests and relay_reader/tests/outcomes/window.

use tokio::sync::watch;

#[cfg(test)]
#[derive(Debug, Default)]
pub(super) struct PublicationProbe {
    published: std::sync::atomic::AtomicUsize,
    consumed: std::sync::atomic::AtomicUsize,
    admitted: std::sync::atomic::AtomicUsize,
    issuer_failed: std::sync::atomic::AtomicBool,
}
#[cfg(test)]
impl PublicationProbe {
    // Published minus admitted covers lane plus physical pending conservatively.
    // Read between saved-host polls; a failed issuer cannot provide evidence.
    pub(super) fn outstanding_lower_bound(&self) -> Option<usize> {
        use std::sync::atomic::Ordering::Acquire;
        if self.issuer_failed.load(Acquire) {
            return None;
        }
        self.published
            .load(Acquire)
            .checked_sub(self.admitted.load(Acquire))
    }

    // The completed-batch publication copy is conservative, never an upper bound.
    pub(super) fn queued_lower_bound(&self) -> Option<usize> {
        use std::sync::atomic::Ordering::Acquire;
        self.published
            .load(Acquire)
            .checked_sub(self.consumed.load(Acquire))
    }
}

pub(super) const WINDOW_BYTES: usize = 65_536;

#[derive(Debug)]
pub(super) struct ReceiveWindow {
    tx: watch::Sender<Option<usize>>,
    reader: Option<ReaderWindow>,
    admitted: usize,
    issued: usize,
    publishing: bool,
    failure: Option<&'static str>,
    reported: bool,
    #[cfg(test)]
    probe: std::sync::Arc<PublicationProbe>,
}

#[derive(Debug)]
pub(super) struct ReaderWindow {
    rx: Option<watch::Receiver<Option<usize>>>,
    read: usize,
    ceiling: usize,
    failed: bool,
    stopped: bool,
    #[cfg(test)]
    probe: std::sync::Arc<PublicationProbe>,
}

impl ReceiveWindow {
    pub(super) fn new() -> Self {
        let (tx, rx) = watch::channel(Some(WINDOW_BYTES));
        #[cfg(test)]
        let probe = std::sync::Arc::new(PublicationProbe::default());
        Self {
            tx,
            reader: Some(ReaderWindow {
                rx: Some(rx),
                read: 0,
                ceiling: WINDOW_BYTES,
                failed: false,
                stopped: false,
                #[cfg(test)]
                probe: probe.clone(),
            }),
            admitted: 0,
            issued: WINDOW_BYTES,
            publishing: false,
            failure: None,
            reported: false,
            #[cfg(test)]
            probe,
        }
    }

    #[cfg(test)]
    pub(super) fn observe_consumed(&self, bytes: usize) {
        self.probe
            .consumed
            .store(bytes, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn take_reader(&mut self) -> Option<ReaderWindow> {
        self.reader.take()
    }

    pub(super) fn fail(&mut self, reason: &'static str) {
        if self.failure.is_none() {
            self.failure = Some(reason);
            #[cfg(test)]
            self.probe
                .issuer_failed
                .store(true, std::sync::atomic::Ordering::Release);
            self.publishing = true;
            self.tx.send_replace(None);
            self.publishing = false;
        }
    }

    pub(super) fn failed(&self) -> bool {
        self.failure.is_some() || self.publishing
    }

    pub(super) fn take_error(&mut self) -> Option<std::io::Error> {
        if self.failed() && !self.reported {
            self.reported = true;
            Some(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                self.failure
                    .unwrap_or("interrupted receive-window publication"),
            ))
        } else {
            None
        }
    }

    pub(super) fn admit(&mut self, before: usize, after: usize, n: usize) {
        if self.failed() {
            return;
        }
        let next = self.admitted.checked_add(n);
        if before.checked_sub(after) != Some(n) || next.is_none_or(|n| n > self.issued) {
            self.fail("receive-window admission mismatch");
            return;
        }
        if let Some(next) = next {
            self.admitted = next;
            #[cfg(test)]
            self.probe
                .admitted
                .store(next, std::sync::atomic::Ordering::Release);
        }
    }

    pub(super) fn audit_storage(&mut self, transferred: usize, pending: usize, abandoned: usize) {
        if self
            .admitted
            .checked_add(pending)
            .and_then(|n| n.checked_add(abandoned))
            != Some(transferred)
        {
            self.fail("receive-window physical inventory mismatch");
        }
    }

    pub(super) fn audit_reader(&mut self, read: usize) {
        if read > self.issued
            || read
                .checked_sub(self.admitted)
                .is_none_or(|n| n > WINDOW_BYTES)
        {
            self.fail("receive-window reader inventory mismatch");
        }
    }

    pub(super) fn publish(&mut self) {
        if self.failed() {
            return;
        }
        if *self.tx.borrow() != Some(self.issued) {
            self.fail("receive-window issued ceiling mismatch");
            return;
        }
        let Some(next) = self.admitted.checked_add(WINDOW_BYTES) else {
            self.fail("receive-window ceiling overflow");
            return;
        };
        if next == self.issued {
            return;
        }
        self.publishing = true;
        // send_replace retains the physical value even after the joined reader
        // drops its receiver. A wake panic leaves this transaction sticky.
        self.tx.send_replace(Some(next));
        self.issued = next;
        self.publishing = false;
    }
}

impl ReaderWindow {
    #[cfg(test)]
    pub(super) fn probe(&self) -> std::sync::Arc<PublicationProbe> {
        self.probe.clone()
    }

    #[cfg(test)]
    pub(super) fn observe_published(&self, bytes: usize) {
        self.probe
            .published
            .store(bytes, std::sync::atomic::Ordering::Release);
    }

    pub(super) fn remaining(&mut self) -> Option<usize> {
        let Some(next) = *self.rx.as_mut()?.borrow_and_update() else {
            self.stopped = true;
            return Some(0);
        };
        if self.failed
            || next < self.ceiling
            || next.checked_sub(self.read).is_none_or(|n| n > WINDOW_BYTES)
        {
            self.failed = true;
            return None;
        }
        self.ceiling = next;
        Some(next - self.read)
    }

    pub(super) fn debit(&mut self, n: usize) -> bool {
        let next = self.read.checked_add(n);
        if self.failed || next.is_none_or(|next| next > self.ceiling) {
            self.failed = true;
            return false;
        }
        if let Some(next) = next {
            self.read = next;
        }
        true
    }

    pub(super) async fn changed(&mut self) -> bool {
        match &mut self.rx {
            Some(rx) => rx.changed().await.is_ok(),
            None => false,
        }
    }

    pub(super) async fn wait_stopped(&mut self) {
        let Some(rx) = &mut self.rx else {
            return;
        };
        loop {
            if rx.borrow_and_update().is_none() {
                self.stopped = true;
                return;
            }
            if rx.changed().await.is_err() {
                self.stopped = true;
                return;
            }
        }
    }

    pub(super) fn stopped(&self) -> bool {
        self.stopped
    }
    pub(super) fn read(&self) -> usize {
        self.read
    }
    pub(super) fn failed(&self) -> bool {
        self.failed
    }
    pub(super) fn take_receiver(&mut self) -> Option<watch::Receiver<Option<usize>>> {
        self.rx.take()
    }
}

#[cfg(test)]
mod tests;
