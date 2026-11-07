//! Original Session cold composition and final observation of its event lane.
//! The non-clone observation is taken only at quiescence and synchronously
//! destroyed outside the Root lock before settlement can complete. No Flow ACK
//! authority is added. Entry: prepare_relay_lane; tests: relay_lane/tests/mod.rs.
use super::*;
use crate::client_tun::{RelayEvent, SocketHandle, relay_channel};
use crate::shared::errors::{FailureOrder, QuietFailureSink};
type Event = (SocketHandle, RelayEvent);
pub(super) type Observation = relay_channel::Observer<Event>;
impl SessionControl {
    pub(in crate::client_tun) fn prepare_relay_lane(
        self: &Arc<Self>,
        capacity: usize,
        hard: bool,
    ) -> std::io::Result<(relay_channel::Sender<Event>, relay_channel::Receiver<Event>)> {
        let Some(meter) = self.app_meter.as_ref() else {
            return if hard {
                Err(std::io::ErrorKind::Unsupported.into())
            } else {
                Ok(relay_channel::channel(capacity))
            };
        };
        if self.relay_lane_prepared.swap(true, Ordering::AcqRel) {
            meter.quarantine();
            return Err(std::io::ErrorKind::AlreadyExists.into());
        }
        let clock = Arc::new(FailureOrder::default());
        let sink: Arc<dyn QuietFailureSink> = self.clone();
        clock
            .bind(
                Arc::downgrade(&sink),
                Self::origin(FailurePhase::TcpOpen),
                Some(meter.clone()),
            )
            .map_err(quinn::owned_events::AdmissionError)?;
        let (tx, rx, observer) = relay_channel::native_channel(capacity, meter, clock)
            .map_err(quinn::owned_events::AdmissionError)?;
        *self.relay_lane.lock().unwrap_or_else(|e| e.into_inner()) = Some(observer);
        Ok((tx, rx))
    }
    pub(super) async fn settle_relay_lane(&self) {
        loop {
            let io_retired = egress::deadline::retire(self);
            let observer = {
                let mut slot = self.relay_lane.lock().unwrap_or_else(|e| e.into_inner());
                match slot.as_ref() {
                    None if io_retired => return,
                    None => None,
                    Some(observer) if observer.settled() => slot.take(),
                    _ => None,
                }
            };
            if let Some(observer) = observer {
                // try_unwrap excludes even a racing failed Weak upgrade. No
                // await between taking the original observer and its disposition.
                match observer.finish() {
                    Ok(()) if !self.app_meter.as_ref().is_some_and(|m| m.is_quarantined()) => {
                        if io_retired { return; }
                    }
                    Ok(()) => self.quarantine_quiet(),
                    Err(observer) => {
                        *self.relay_lane.lock().unwrap_or_else(|e| e.into_inner()) = Some(observer)
                    }
                }
            }
            if !io_retired && self.deadline_quiet().is_some_and(|d| tokio::time::Instant::now() >= d) {
                self.quarantine_quiet();
            }
            if self.app_meter.as_ref().is_some_and(|m| m.is_quarantined()) {
                self.quarantine_quiet();
                std::future::pending::<()>().await;
            }
            // Original saved retirement and its original parent deadline own
            // this wait; no new deadline or completion notification is created.
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
    }
}
#[cfg(test)]
mod tests;
