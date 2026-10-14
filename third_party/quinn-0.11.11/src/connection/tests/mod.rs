//! Private loopback tests for atomic 1-RTT admission and exact destruction ordering.

#[derive(Debug)]
pub(super) struct DestructionBarrier {
    entered: std::sync::Arc<std::sync::Barrier>,
    release: std::sync::Arc<std::sync::Barrier>,
}

impl Drop for DestructionBarrier {
    fn drop(&mut self) {
        self.entered.wait();
        self.release.wait();
    }
}

#[cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]
mod retirement;

#[cfg(any(feature = "rustls-aws-lc-rs", feature = "rustls-ring"))]
mod bi_retirement;
