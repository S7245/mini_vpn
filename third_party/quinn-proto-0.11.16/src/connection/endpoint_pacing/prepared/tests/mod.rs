use super::*;
impl EndpointPacingService {
    pub(crate) fn poison_prepared_for_probe(&self) {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _state = self.0.state.lock().unwrap();
            panic!("injected same-authority pacing poison");
        }));
    }
}
