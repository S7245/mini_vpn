//! Owns the prepared/displaced Path during the existing Connection migration.
//! No path controller is destroyed by an assignment inside the commit. The
//! original meter retains unknown interruption; cleanup preserves the first panic.
//! Entry: PathDisposal; actual migration tests: endpoint_pacing/tests/migration.
use super::{ConnectionId, PathData};
use bytes::BackingMeter;

pub(super) struct PathDisposal {
    path: Option<PathData>,
    meter: Option<BackingMeter>,
}
impl PathDisposal {
    pub(super) fn prepare(meter: Option<BackingMeter>, build: impl FnOnce() -> PathData) -> Self {
        let mut disposal = Self { path: None, meter };
        disposal.path = Some(build());
        disposal
    }
    pub(super) fn install(&mut self, current: &mut PathData) {
        if let Some(path) = self.path.as_mut() {
            std::mem::swap(path, current);
        }
    }
    pub(super) fn retain_validated_previous(
        &mut self,
        previous: &mut Option<(ConnectionId, PathData)>,
        cid: ConnectionId,
        challenge: impl FnOnce() -> u64,
    ) {
        if self
            .path
            .as_ref()
            .is_some_and(|path| path.challenge.is_none())
        {
            let challenge = challenge();
            if let Some(mut path) = self.path.take() {
                path.challenge = Some(challenge);
                path.challenge_pending = true;
                self.path = previous.replace((cid, path)).map(|(_, path)| path);
            }
        }
    }
}
impl Drop for PathDisposal {
    fn drop(&mut self) {
        if std::thread::panicking() {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
        }
        if let Err(payload) =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(self.path.take())))
        {
            if let Some(meter) = &self.meter {
                meter.quarantine();
            }
            if std::thread::panicking() {
                std::mem::forget(payload);
            } else {
                std::panic::resume_unwind(payload);
            }
        }
    }
}
