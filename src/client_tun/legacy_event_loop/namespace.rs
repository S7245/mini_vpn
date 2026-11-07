//! Service-lived fake-IP namespace and its original monotonic UDP clock.
//! No socket, flow, device, task or retry authority lives here. The legacy entry
//! creates one namespace exactly as before; a controlled service may retain it
//! across fully settled sessions. Entry: new; real loop tracers: tuic_service/tests.
use crate::fake_ip::FakeIpPool;

pub(in crate::client_tun) struct Namespace {
    pub(in crate::client_tun) fake_pool: FakeIpPool,
    pub(in crate::client_tun) clock: std::time::Instant,
}
impl Namespace {
    pub(in crate::client_tun) fn new() -> Self {
        Self {
            fake_pool: FakeIpPool::new(),
            clock: std::time::Instant::now(),
        }
    }
}
pub(in crate::client_tun) struct LoopContext<'a> {
    pub(in crate::client_tun) namespace: &'a mut Namespace,
    pub(in crate::client_tun) control:
        Option<std::sync::Arc<crate::client_tun::compatibility_runtime::SessionControl>>,
}
