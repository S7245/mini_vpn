#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamPendingFreshness {
    NoTransportSample,
    NoConnectionRx,
    ConnectionRxNoStreamFrames,
    ConnectionFreshStreamFramesPending,
    ConnectionStaleStreamFramesPending,
}

impl StreamPendingFreshness {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NoTransportSample => "no_transport_sample",
            Self::NoConnectionRx => "no_connection_rx",
            Self::ConnectionRxNoStreamFrames => "connection_rx_no_stream_frames",
            Self::ConnectionFreshStreamFramesPending => "connection_fresh_stream_frames_pending",
            Self::ConnectionStaleStreamFramesPending => "connection_stale_stream_frames_pending",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum StreamServiceBlockedReason {
    #[default]
    None,
    ReadCreditPaused,
    GlobalRxPressure,
    LocalAdmissionNoWork,
    LocalAdmissionNoProgress,
    LocalAdmissionCycleBudget,
    LocalAdmissionHardPause,
}

impl StreamServiceBlockedReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::ReadCreditPaused => "read_credit_paused",
            Self::GlobalRxPressure => "global_rx_pressure",
            Self::LocalAdmissionNoWork => "local_admission_no_work",
            Self::LocalAdmissionNoProgress => "local_admission_no_progress",
            Self::LocalAdmissionCycleBudget => "local_admission_cycle_budget",
            Self::LocalAdmissionHardPause => "local_admission_hard_pause",
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LocalAdmissionProgress {
    pub accepted_bytes: usize,
    pub egress_drain_bytes: usize,
    pub tun_rx_packets: usize,
    pub flush_tx_calls: usize,
    pub flush_tx_failures: usize,
    pub dirty_passes: usize,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct StreamServiceWindow {
    pub remote_poll_ticks: u64,
    pub remote_poll_len_min: usize,
    pub remote_poll_len_max: usize,
    pub pending_no_transport_sample: u64,
    pub pending_no_connection_rx: u64,
    pub pending_rx_no_stream_frames: u64,
    pub pending_fresh_stream_frames: u64,
    pub pending_stale_stream_frames: u64,
    pub remote_read_chunks: u64,
    pub remote_read_bytes: u64,
    pub global_rx_wait_max_micros: u128,
    pub global_rx_pressure_events: u64,
    pub global_rx_queue_used_max: usize,
    pub global_rx_queue_capacity: usize,
    pub local_accepted_bytes: u64,
    pub local_egress_drain_bytes: u64,
    pub local_tun_rx_packets: u64,
    pub local_flush_tx_calls: u64,
    pub local_flush_tx_failures: u64,
    pub local_dirty_passes: u64,
    pub last_blocked_reason: StreamServiceBlockedReason,
}

impl StreamServiceWindow {
    pub fn note_remote_poll(&mut self, read_len: usize) {
        if read_len == 0 {
            return;
        }
        self.remote_poll_ticks = self.remote_poll_ticks.saturating_add(1);
        if self.remote_poll_len_min == 0 {
            self.remote_poll_len_min = read_len;
        } else {
            self.remote_poll_len_min = self.remote_poll_len_min.min(read_len);
        }
        self.remote_poll_len_max = self.remote_poll_len_max.max(read_len);
    }

    pub fn note_pending(&mut self, freshness: StreamPendingFreshness) {
        match freshness {
            StreamPendingFreshness::NoTransportSample => {
                self.pending_no_transport_sample =
                    self.pending_no_transport_sample.saturating_add(1);
            }
            StreamPendingFreshness::NoConnectionRx => {
                self.pending_no_connection_rx = self.pending_no_connection_rx.saturating_add(1);
            }
            StreamPendingFreshness::ConnectionRxNoStreamFrames => {
                self.pending_rx_no_stream_frames =
                    self.pending_rx_no_stream_frames.saturating_add(1);
            }
            StreamPendingFreshness::ConnectionFreshStreamFramesPending => {
                self.pending_fresh_stream_frames =
                    self.pending_fresh_stream_frames.saturating_add(1);
            }
            StreamPendingFreshness::ConnectionStaleStreamFramesPending => {
                self.pending_stale_stream_frames =
                    self.pending_stale_stream_frames.saturating_add(1);
            }
        }
    }

    pub fn note_remote_read(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.remote_read_chunks = self.remote_read_chunks.saturating_add(1);
        self.remote_read_bytes = self.remote_read_bytes.saturating_add(bytes as u64);
    }

    pub fn note_global_rx_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.global_rx_wait_max_micros = self.global_rx_wait_max_micros.max(elapsed.as_micros());
        if elapsed >= pressure_threshold {
            self.global_rx_pressure_events = self.global_rx_pressure_events.saturating_add(1);
            self.note_blocked(StreamServiceBlockedReason::GlobalRxPressure);
        }
    }

    pub fn note_global_rx_queue(&mut self, used: usize, max_capacity: usize) {
        self.global_rx_queue_used_max = self.global_rx_queue_used_max.max(used);
        self.global_rx_queue_capacity = self.global_rx_queue_capacity.max(max_capacity);
    }

    pub fn note_local_admission(
        &mut self,
        progress: LocalAdmissionProgress,
        blocked_reason: StreamServiceBlockedReason,
    ) {
        self.local_accepted_bytes = self
            .local_accepted_bytes
            .saturating_add(progress.accepted_bytes as u64);
        self.local_egress_drain_bytes = self
            .local_egress_drain_bytes
            .saturating_add(progress.egress_drain_bytes as u64);
        self.local_tun_rx_packets = self
            .local_tun_rx_packets
            .saturating_add(progress.tun_rx_packets as u64);
        self.local_flush_tx_calls = self
            .local_flush_tx_calls
            .saturating_add(progress.flush_tx_calls as u64);
        self.local_flush_tx_failures = self
            .local_flush_tx_failures
            .saturating_add(progress.flush_tx_failures as u64);
        self.local_dirty_passes = self
            .local_dirty_passes
            .saturating_add(progress.dirty_passes as u64);
        self.note_blocked(blocked_reason);
    }

    pub fn note_blocked(&mut self, reason: StreamServiceBlockedReason) {
        if reason != StreamServiceBlockedReason::None {
            self.last_blocked_reason = reason;
        }
    }

    pub fn has_useful_progress(&self) -> bool {
        self.remote_read_bytes > 0
            || self.local_accepted_bytes > 0
            || self.local_egress_drain_bytes > 0
    }
}

#[derive(Debug, Default, Clone)]
pub struct StreamServiceController {
    window: StreamServiceWindow,
}

impl StreamServiceController {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_remote_read_len(
        &mut self,
        read_credit_paused: bool,
        max_batch_bytes: usize,
        buffer_len: usize,
    ) -> usize {
        if read_credit_paused || max_batch_bytes == 0 {
            self.window
                .note_blocked(StreamServiceBlockedReason::ReadCreditPaused);
            return 0;
        }
        let read_len = max_batch_bytes.min(buffer_len);
        self.window.note_remote_poll(read_len);
        read_len
    }

    pub fn note_remote_read(&mut self, bytes: usize) {
        self.window.note_remote_read(bytes);
    }

    pub fn note_global_rx_wait(
        &mut self,
        elapsed: std::time::Duration,
        pressure_threshold: std::time::Duration,
    ) {
        self.window.note_global_rx_wait(elapsed, pressure_threshold);
    }

    pub fn note_global_rx_queue(&mut self, used: usize, max_capacity: usize) {
        self.window.note_global_rx_queue(used, max_capacity);
    }

    pub fn note_pending(&mut self, freshness: StreamPendingFreshness) {
        self.window.note_pending(freshness);
    }

    pub fn window(&self) -> StreamServiceWindow {
        self.window
    }
}

pub fn format_stream_service_window_fields(window: &StreamServiceWindow) -> String {
    format!(
        "remote_poll_ticks={} remote_poll_len_min={} remote_poll_len_max={} \
         pending_no_transport_sample={} pending_no_connection_rx={} pending_rx_no_stream_frames={} pending_fresh_stream_frames={} pending_stale_stream_frames={} \
         remote_read_chunks={} remote_read_bytes={} global_rx_wait_max_us={} global_rx_pressure_events={} global_rx_queue_used_max={} global_rx_queue_capacity={} \
         local_accepted_bytes={} local_egress_drain_bytes={} local_tun_rx_packets={} local_flush_tx_calls={} local_flush_tx_failures={} local_dirty_passes={} last_blocked_reason={} useful_progress={}",
        window.remote_poll_ticks,
        window.remote_poll_len_min,
        window.remote_poll_len_max,
        window.pending_no_transport_sample,
        window.pending_no_connection_rx,
        window.pending_rx_no_stream_frames,
        window.pending_fresh_stream_frames,
        window.pending_stale_stream_frames,
        window.remote_read_chunks,
        window.remote_read_bytes,
        window.global_rx_wait_max_micros,
        window.global_rx_pressure_events,
        window.global_rx_queue_used_max,
        window.global_rx_queue_capacity,
        window.local_accepted_bytes,
        window.local_egress_drain_bytes,
        window.local_tun_rx_packets,
        window.local_flush_tx_calls,
        window.local_flush_tx_failures,
        window.local_dirty_passes,
        window.last_blocked_reason.as_str(),
        window.has_useful_progress()
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_service_window_records_remote_pending_and_local_admission() {
        let mut window = StreamServiceWindow::default();

        window.note_pending(StreamPendingFreshness::ConnectionFreshStreamFramesPending);
        window.note_pending(StreamPendingFreshness::ConnectionStaleStreamFramesPending);
        window.note_remote_poll(65_536);
        window.note_remote_read(32_768);
        window.note_global_rx_wait(
            std::time::Duration::from_micros(7),
            std::time::Duration::from_micros(5),
        );
        window.note_global_rx_queue(9, 1024);
        window.note_local_admission(
            LocalAdmissionProgress {
                accepted_bytes: 16_384,
                egress_drain_bytes: 4096,
                tun_rx_packets: 3,
                flush_tx_calls: 2,
                dirty_passes: 1,
                ..LocalAdmissionProgress::default()
            },
            StreamServiceBlockedReason::LocalAdmissionCycleBudget,
        );

        assert_eq!(window.pending_fresh_stream_frames, 1);
        assert_eq!(window.pending_stale_stream_frames, 1);
        assert_eq!(window.remote_poll_ticks, 1);
        assert_eq!(window.remote_read_bytes, 32_768);
        assert_eq!(window.global_rx_pressure_events, 1);
        assert_eq!(window.global_rx_queue_used_max, 9);
        assert_eq!(window.local_accepted_bytes, 16_384);
        assert_eq!(window.local_egress_drain_bytes, 4096);
        assert_eq!(
            window.last_blocked_reason,
            StreamServiceBlockedReason::LocalAdmissionCycleBudget
        );
        assert!(window.has_useful_progress());
    }

    #[test]
    fn stream_service_controller_uses_read_credit_as_remote_poll_gate() {
        let mut controller = StreamServiceController::new();

        assert_eq!(controller.next_remote_read_len(true, 65_536, 65_536), 0);
        assert_eq!(
            controller.window().last_blocked_reason,
            StreamServiceBlockedReason::ReadCreditPaused
        );
        assert_eq!(
            controller.next_remote_read_len(false, 16_384, 65_536),
            16_384
        );

        let window = controller.window();
        assert_eq!(window.remote_poll_ticks, 1);
        assert_eq!(window.remote_poll_len_min, 16_384);
        assert_eq!(window.remote_poll_len_max, 16_384);
    }

    #[test]
    fn stream_service_window_format_is_parseable_for_vps_bundles() {
        let mut window = StreamServiceWindow::default();
        window.note_pending(StreamPendingFreshness::ConnectionFreshStreamFramesPending);
        window.note_remote_poll(65_536);
        window.note_remote_read(65_536);
        window.note_local_admission(
            LocalAdmissionProgress {
                egress_drain_bytes: 4096,
                flush_tx_calls: 1,
                dirty_passes: 1,
                ..LocalAdmissionProgress::default()
            },
            StreamServiceBlockedReason::None,
        );

        let line = format_stream_service_window_fields(&window);

        assert!(line.contains("pending_fresh_stream_frames=1"), "{line}");
        assert!(line.contains("remote_read_bytes=65536"), "{line}");
        assert!(line.contains("local_accepted_bytes=0"), "{line}");
        assert!(line.contains("local_egress_drain_bytes=4096"), "{line}");
        assert!(line.contains("last_blocked_reason=none"), "{line}");
        assert!(line.contains("useful_progress=true"), "{line}");
    }
}
