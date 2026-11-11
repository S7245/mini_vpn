mod generation_shutdown;
mod lifecycle;

impl super::super::TuicUpstream {
    pub(crate) async fn udp_native_heartbeat_for_test(&self) -> u8 {
        let conn = self.tcp_pool_slots[0]
            .state
            .lock()
            .await
            .current
            .transport
            .connection()
            .unwrap();
        match self.udp_runtime.heartbeat(&conn) {
            super::UdpTurn::Continue => 0,
            super::UdpTurn::Disconnected => 1,
            super::UdpTurn::Stop => 2,
        }
    }
    pub(crate) fn udp_native_children_for_test(&self) -> u64 {
        self.udp_runtime
            .child_count
            .load(super::super::Ordering::Relaxed)
    }
    pub(crate) fn udp_native_clock_for_test(&self) -> crate::shared::errors::FailureClock {
        self.udp_runtime.native.get().unwrap().clock().unwrap()
    }
}
