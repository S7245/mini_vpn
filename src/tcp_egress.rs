use std::collections::VecDeque;

pub const D16_RECOVERY_CLEAN_CYCLES: u8 = 4;
pub const D16_ACTOR_PAYLOAD_PACKET_BUDGET: usize = 24;
const IPV4_TCP_MIN_HEADER_BYTES: usize = 40;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EgressPhase {
    Running,
    DrainOnly,
    Recovery { clean_cycles: u8 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EgressPermissions {
    pub allow_read: bool,
    pub allow_admission: bool,
    pub allow_drain: bool,
    pub max_read_quantum_bytes: usize,
    pub max_admit_quantum_bytes: usize,
}

impl EgressPermissions {
    pub fn for_phase(phase: EgressPhase) -> Self {
        match phase {
            EgressPhase::Running => Self {
                allow_read: true,
                allow_admission: true,
                allow_drain: true,
                max_read_quantum_bytes: 512 * 1024,
                max_admit_quantum_bytes: 128 * 1024,
            },
            EgressPhase::DrainOnly => Self {
                allow_read: false,
                allow_admission: false,
                allow_drain: true,
                max_read_quantum_bytes: 0,
                max_admit_quantum_bytes: 0,
            },
            EgressPhase::Recovery { .. } => Self {
                allow_read: true,
                allow_admission: true,
                allow_drain: true,
                max_read_quantum_bytes: 128 * 1024,
                max_admit_quantum_bytes: 128 * 1024,
            },
        }
    }

    pub fn max_admit_bytes_for_tun_mtu(self, tun_mtu: usize) -> usize {
        let tcp_payload_bytes = tun_mtu.saturating_sub(IPV4_TCP_MIN_HEADER_BYTES).max(1);
        self.max_admit_quantum_bytes
            .min(tcp_payload_bytes.saturating_mul(D16_ACTOR_PAYLOAD_PACKET_BUDGET))
    }

    pub fn available_admit_bytes_for_tun_mtu(
        self,
        tun_mtu: usize,
        existing_send_queue_bytes: usize,
    ) -> usize {
        let tcp_payload_bytes = tun_mtu.saturating_sub(IPV4_TCP_MIN_HEADER_BYTES).max(1);
        let occupied_packet_bytes = existing_send_queue_bytes
            .div_ceil(tcp_payload_bytes)
            .saturating_mul(tcp_payload_bytes);
        self.max_admit_bytes_for_tun_mtu(tun_mtu)
            .saturating_sub(occupied_packet_bytes)
    }
}

pub fn transition_egress_phase(
    phase: EgressPhase,
    hard_pressure: bool,
    drop_debt: bool,
    below_low_watermark: bool,
    drain_progress: bool,
) -> EgressPhase {
    if hard_pressure || drop_debt {
        return EgressPhase::DrainOnly;
    }
    match phase {
        EgressPhase::Running => EgressPhase::Running,
        EgressPhase::DrainOnly if below_low_watermark && drain_progress => {
            EgressPhase::Recovery { clean_cycles: 0 }
        }
        EgressPhase::DrainOnly => EgressPhase::DrainOnly,
        EgressPhase::Recovery { clean_cycles } if below_low_watermark && drain_progress => {
            let next = clean_cycles.saturating_add(1);
            if next >= D16_RECOVERY_CLEAN_CYCLES {
                EgressPhase::Running
            } else {
                EgressPhase::Recovery { clean_cycles: next }
            }
        }
        EgressPhase::Recovery { clean_cycles } => EgressPhase::Recovery { clean_cycles },
    }
}

pub trait TcpEgressLease: Sized {
    fn remaining_bytes(&self) -> usize;
    fn release(&mut self, bytes: usize) -> usize;
    fn split_to(&mut self, bytes: usize) -> Option<Self>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressPendingSnapshot {
    pub pending_bytes: usize,
    pub leased_bytes: usize,
    pub permit_chunks: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressPendingAppend {
    pub appended_bytes: usize,
    pub released_excess_bytes: usize,
    pub retained_lease_bytes: usize,
    pub pending_bytes_after: usize,
    pub leased_bytes_after: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressPendingConsume {
    pub consumed_bytes: usize,
    pub released_lease_bytes: usize,
    pub pending_bytes_after: usize,
    pub leased_bytes_after: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressPendingTransfer {
    pub transferred_bytes: usize,
    pub transferred_lease_bytes: usize,
    pub pending_bytes_after: usize,
    pub pending_leased_bytes_after: usize,
    pub inflight_leased_bytes_after: usize,
}

pub struct TcpEgressPendingQueueView<'a, P> {
    pending: &'a mut Vec<u8>,
    permits: &'a mut VecDeque<P>,
}

impl<'a, P> TcpEgressPendingQueueView<'a, P>
where
    P: TcpEgressLease,
{
    pub fn new(pending: &'a mut Vec<u8>, permits: &'a mut VecDeque<P>) -> Self {
        Self { pending, permits }
    }

    pub fn as_slice(&self) -> &[u8] {
        self.pending
    }

    pub fn len(&self) -> usize {
        self.pending.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    pub fn snapshot(&self) -> TcpEgressPendingSnapshot {
        TcpEgressPendingSnapshot {
            pending_bytes: self.pending.len(),
            leased_bytes: self.leased_bytes(),
            permit_chunks: self.permits.len(),
        }
    }

    pub fn append(&mut self, payload: &[u8], permit: Option<P>) -> TcpEgressPendingAppend {
        let mut released_excess_bytes = 0usize;
        let mut retained_lease_bytes = 0usize;

        if payload.is_empty() {
            if let Some(mut permit) = permit {
                released_excess_bytes = permit.release(usize::MAX);
            }
            return TcpEgressPendingAppend {
                appended_bytes: 0,
                released_excess_bytes,
                retained_lease_bytes: 0,
                pending_bytes_after: self.pending.len(),
                leased_bytes_after: self.leased_bytes(),
            };
        }

        self.pending.extend_from_slice(payload);
        if let Some(mut permit) = permit {
            let extra = permit.remaining_bytes().saturating_sub(payload.len());
            if extra > 0 {
                released_excess_bytes = permit.release(extra);
            }
            retained_lease_bytes = permit.remaining_bytes();
            if retained_lease_bytes > 0 {
                self.permits.push_back(permit);
            }
        }

        TcpEgressPendingAppend {
            appended_bytes: payload.len(),
            released_excess_bytes,
            retained_lease_bytes,
            pending_bytes_after: self.pending.len(),
            leased_bytes_after: self.leased_bytes(),
        }
    }

    pub fn consume_prefix(&mut self, bytes: usize) -> TcpEgressPendingConsume {
        let consumed = bytes.min(self.pending.len());
        if consumed == 0 {
            return TcpEgressPendingConsume {
                consumed_bytes: 0,
                released_lease_bytes: 0,
                pending_bytes_after: self.pending.len(),
                leased_bytes_after: self.leased_bytes(),
            };
        }

        let released = self.release_leases(consumed);
        self.pending.drain(..consumed);
        TcpEgressPendingConsume {
            consumed_bytes: consumed,
            released_lease_bytes: released,
            pending_bytes_after: self.pending.len(),
            leased_bytes_after: self.leased_bytes(),
        }
    }

    pub fn transfer_prefix_to(
        &mut self,
        bytes: usize,
        inflight: &mut VecDeque<P>,
    ) -> TcpEgressPendingTransfer {
        let transferred = bytes.min(self.pending.len());
        if transferred == 0 {
            return TcpEgressPendingTransfer {
                transferred_bytes: 0,
                transferred_lease_bytes: 0,
                pending_bytes_after: self.pending.len(),
                pending_leased_bytes_after: self.leased_bytes(),
                inflight_leased_bytes_after: leased_bytes(inflight),
            };
        }

        let transferred_lease_bytes = self.transfer_leases_to(transferred, inflight);
        self.pending.drain(..transferred);
        TcpEgressPendingTransfer {
            transferred_bytes: transferred,
            transferred_lease_bytes,
            pending_bytes_after: self.pending.len(),
            pending_leased_bytes_after: self.leased_bytes(),
            inflight_leased_bytes_after: leased_bytes(inflight),
        }
    }

    pub fn clear(&mut self) -> TcpEgressPendingConsume {
        let consumed = self.pending.len();
        self.pending.clear();
        let mut released = 0usize;
        while let Some(mut permit) = self.permits.pop_front() {
            released = released.saturating_add(permit.release(usize::MAX));
        }
        TcpEgressPendingConsume {
            consumed_bytes: consumed,
            released_lease_bytes: released,
            pending_bytes_after: 0,
            leased_bytes_after: 0,
        }
    }

    fn release_leases(&mut self, mut bytes: usize) -> usize {
        let mut released = 0usize;
        while bytes > 0 {
            let Some(front) = self.permits.front_mut() else {
                break;
            };
            let n = front.release(bytes);
            if n == 0 {
                break;
            }
            released = released.saturating_add(n);
            bytes = bytes.saturating_sub(n);
            if front.remaining_bytes() == 0 {
                self.permits.pop_front();
            }
        }
        released
    }

    fn transfer_leases_to(&mut self, mut bytes: usize, inflight: &mut VecDeque<P>) -> usize {
        let mut transferred = 0usize;
        while bytes > 0 {
            let Some(front) = self.permits.front_mut() else {
                break;
            };
            let Some(piece) = front.split_to(bytes) else {
                break;
            };
            let moved = piece.remaining_bytes();
            if moved == 0 {
                break;
            }
            transferred = transferred.saturating_add(moved);
            bytes = bytes.saturating_sub(moved);
            inflight.push_back(piece);
            if self
                .permits
                .front()
                .is_some_and(|front| front.remaining_bytes() == 0)
            {
                self.permits.pop_front();
            }
        }
        transferred
    }

    fn leased_bytes(&self) -> usize {
        leased_bytes(self.permits)
    }
}

fn leased_bytes<P: TcpEgressLease>(permits: &VecDeque<P>) -> usize {
    permits.iter().map(TcpEgressLease::remaining_bytes).sum()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressConfig {
    pub max_cycles: usize,
    pub target_bytes_per_window: usize,
    pub max_admit_bytes_per_cycle: usize,
    pub drain_bytes_per_cycle: usize,
}

impl TcpEgressConfig {
    pub fn validate(self) -> Result<Self, TcpEgressConfigError> {
        if self.max_cycles == 0 {
            return Err(TcpEgressConfigError::ZeroCycles);
        }
        if self.target_bytes_per_window == 0 {
            return Err(TcpEgressConfigError::ZeroTargetBytes);
        }
        if self.max_admit_bytes_per_cycle == 0 {
            return Err(TcpEgressConfigError::ZeroAdmitBytes);
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpEgressConfigError {
    ZeroCycles,
    ZeroTargetBytes,
    ZeroAdmitBytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpEgressStopReason {
    NoWork,
    TargetReached,
    NoProgress,
    CycleBudget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpEgressWake {
    None,
    Immediate,
    ExternalReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TcpEgressWindowOutcome {
    pub cycles: usize,
    pub drained_bytes: usize,
    pub admitted_bytes: usize,
    pub released_lease_bytes: usize,
    pub stop_reason: TcpEgressStopReason,
    pub wake: TcpEgressWake,
    pub pending_bytes: usize,
    pub leased_bytes: usize,
    pub socket_queue_bytes: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TcpEgressFlowModel {
    pending_bytes: usize,
    leased_bytes: usize,
    socket_queue_bytes: usize,
    socket_capacity_bytes: usize,
}

impl TcpEgressFlowModel {
    pub fn new(socket_capacity_bytes: usize) -> Self {
        Self {
            pending_bytes: 0,
            leased_bytes: 0,
            socket_queue_bytes: 0,
            socket_capacity_bytes,
        }
    }

    pub fn append_downlink(&mut self, bytes: usize) {
        if bytes == 0 {
            return;
        }
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.leased_bytes = self.leased_bytes.saturating_add(bytes);
    }

    pub fn set_socket_queue_bytes(&mut self, bytes: usize) {
        self.socket_queue_bytes = bytes.min(self.socket_capacity_bytes);
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub fn leased_bytes(&self) -> usize {
        self.leased_bytes
    }

    pub fn socket_queue_bytes(&self) -> usize {
        self.socket_queue_bytes
    }

    pub fn drop_pending(&mut self) -> usize {
        let dropped = self.pending_bytes;
        self.pending_bytes = 0;
        self.leased_bytes = self.leased_bytes.saturating_sub(dropped);
        dropped
    }

    pub fn service_window(
        &mut self,
        config: TcpEgressConfig,
    ) -> Result<TcpEgressWindowOutcome, TcpEgressConfigError> {
        let config = config.validate()?;
        let mut outcome = TcpEgressWindowOutcome {
            cycles: 0,
            drained_bytes: 0,
            admitted_bytes: 0,
            released_lease_bytes: 0,
            stop_reason: TcpEgressStopReason::NoWork,
            wake: TcpEgressWake::None,
            pending_bytes: self.pending_bytes,
            leased_bytes: self.leased_bytes,
            socket_queue_bytes: self.socket_queue_bytes,
        };

        if self.pending_bytes == 0 && self.socket_queue_bytes == 0 {
            return Ok(outcome);
        }

        for _ in 0..config.max_cycles {
            outcome.cycles = outcome.cycles.saturating_add(1);
            let drained = self.socket_queue_bytes.min(config.drain_bytes_per_cycle);
            self.socket_queue_bytes = self.socket_queue_bytes.saturating_sub(drained);

            let target_remaining = config
                .target_bytes_per_window
                .saturating_sub(outcome.admitted_bytes);
            let available_socket_bytes = self
                .socket_capacity_bytes
                .saturating_sub(self.socket_queue_bytes);
            let admit_budget = config
                .max_admit_bytes_per_cycle
                .min(target_remaining)
                .min(available_socket_bytes);
            let admitted = self.pending_bytes.min(admit_budget);
            self.pending_bytes = self.pending_bytes.saturating_sub(admitted);
            self.socket_queue_bytes = self.socket_queue_bytes.saturating_add(admitted);
            self.leased_bytes = self.leased_bytes.saturating_sub(admitted);

            outcome.drained_bytes = outcome.drained_bytes.saturating_add(drained);
            outcome.admitted_bytes = outcome.admitted_bytes.saturating_add(admitted);
            outcome.released_lease_bytes = outcome.released_lease_bytes.saturating_add(admitted);

            if outcome.admitted_bytes >= config.target_bytes_per_window {
                outcome.stop_reason = TcpEgressStopReason::TargetReached;
                return Ok(self.finish_outcome(outcome));
            }
            if self.pending_bytes == 0 {
                outcome.stop_reason = TcpEgressStopReason::NoWork;
                return Ok(self.finish_outcome(outcome));
            }
            if drained == 0 && admitted == 0 {
                outcome.stop_reason = TcpEgressStopReason::NoProgress;
                return Ok(self.finish_outcome(outcome));
            }
        }

        outcome.stop_reason = TcpEgressStopReason::CycleBudget;
        Ok(self.finish_outcome(outcome))
    }

    fn finish_outcome(&self, mut outcome: TcpEgressWindowOutcome) -> TcpEgressWindowOutcome {
        outcome.pending_bytes = self.pending_bytes;
        outcome.leased_bytes = self.leased_bytes;
        outcome.socket_queue_bytes = self.socket_queue_bytes;
        outcome.wake = match outcome.stop_reason {
            TcpEgressStopReason::NoWork => {
                if self.socket_queue_bytes > 0 {
                    TcpEgressWake::ExternalReadiness
                } else {
                    TcpEgressWake::None
                }
            }
            TcpEgressStopReason::NoProgress => {
                if self.pending_bytes > 0 {
                    TcpEgressWake::ExternalReadiness
                } else {
                    TcpEgressWake::None
                }
            }
            TcpEgressStopReason::TargetReached | TcpEgressStopReason::CycleBudget => {
                if self.pending_bytes > 0 {
                    TcpEgressWake::Immediate
                } else if self.socket_queue_bytes > 0 {
                    TcpEgressWake::ExternalReadiness
                } else {
                    TcpEgressWake::None
                }
            }
        };
        outcome
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct TestLease {
        remaining: usize,
    }

    impl TestLease {
        fn new(remaining: usize) -> Self {
            Self { remaining }
        }
    }

    impl TcpEgressLease for TestLease {
        fn remaining_bytes(&self) -> usize {
            self.remaining
        }

        fn release(&mut self, bytes: usize) -> usize {
            let released = self.remaining.min(bytes);
            self.remaining = self.remaining.saturating_sub(released);
            released
        }

        fn split_to(&mut self, bytes: usize) -> Option<Self> {
            let moved = self.remaining.min(bytes);
            if moved == 0 {
                return None;
            }
            self.remaining = self.remaining.saturating_sub(moved);
            Some(Self::new(moved))
        }
    }

    fn config() -> TcpEgressConfig {
        TcpEgressConfig {
            max_cycles: 8,
            target_bytes_per_window: 256 * 1024,
            max_admit_bytes_per_cycle: 64 * 1024,
            drain_bytes_per_cycle: 0,
        }
    }

    #[test]
    fn d16_drain_only_stops_read_and_admit_but_keeps_drain() {
        let permissions = EgressPermissions::for_phase(EgressPhase::DrainOnly);
        assert!(!permissions.allow_read);
        assert!(!permissions.allow_admission);
        assert!(permissions.allow_drain);
    }

    #[test]
    fn d16_running_keeps_read_reservoir_but_bounds_actor_admission() {
        let permissions = EgressPermissions::for_phase(EgressPhase::Running);

        assert_eq!(permissions.max_read_quantum_bytes, 512 * 1024);
        assert_eq!(permissions.max_admit_quantum_bytes, 128 * 1024);
    }

    #[test]
    fn d16_actor_subquantum_preserves_window_while_leaving_ack_ring_headroom() {
        const MODELED_TUN_RX_RING_PACKETS: usize = 64;
        const ORDINARY_TUN_RX_DRAIN_PACKETS: usize = 16;
        const ACTOR_SERVICE_WINDOW_BYTES: usize = 128 * 1024;
        const ACTOR_MAX_CYCLES: usize = 8;

        for tun_mtu in [1200usize, 1500] {
            let tcp_payload_bytes = tun_mtu - 40;
            let running = EgressPermissions::for_phase(EgressPhase::Running);
            let recovery = EgressPermissions::for_phase(EgressPhase::Recovery { clean_cycles: 0 });
            let running_bytes = running.max_admit_bytes_for_tun_mtu(tun_mtu);
            let recovery_bytes = recovery.max_admit_bytes_for_tun_mtu(tun_mtu);
            let payload_packets = running_bytes.div_ceil(tcp_payload_bytes);

            assert_eq!(running_bytes, recovery_bytes);
            assert_eq!(payload_packets, D16_ACTOR_PAYLOAD_PACKET_BUDGET);
            assert!(payload_packets + ORDINARY_TUN_RX_DRAIN_PACKETS <= MODELED_TUN_RX_RING_PACKETS);
            assert!(running_bytes * ACTOR_MAX_CYCLES >= ACTOR_SERVICE_WINDOW_BYTES);
            assert!(running_bytes < running.max_admit_quantum_bytes);
        }

        assert_eq!(
            EgressPermissions::for_phase(EgressPhase::DrainOnly).max_admit_bytes_for_tun_mtu(1500),
            0
        );
    }

    #[test]
    fn d16_actor_packet_budget_deducts_existing_unacked_send_queue() {
        let tun_mtu = 1500;
        let tcp_payload_bytes = tun_mtu - 40;
        let permissions = EgressPermissions::for_phase(EgressPhase::Running);

        assert_eq!(
            permissions.available_admit_bytes_for_tun_mtu(tun_mtu, 16 * tcp_payload_bytes,),
            8 * tcp_payload_bytes,
        );
        assert_eq!(
            permissions.available_admit_bytes_for_tun_mtu(
                tun_mtu,
                D16_ACTOR_PAYLOAD_PACKET_BUDGET * tcp_payload_bytes,
            ),
            0,
        );
        assert_eq!(
            permissions.available_admit_bytes_for_tun_mtu(
                tun_mtu,
                2 * D16_ACTOR_PAYLOAD_PACKET_BUDGET * tcp_payload_bytes,
            ),
            0,
        );
    }

    #[test]
    fn d16_actor_packet_budget_rounds_partial_unacked_segment_to_one_slot() {
        let tun_mtu = 1500;
        let tcp_payload_bytes = tun_mtu - 40;
        let permissions = EgressPermissions::for_phase(EgressPhase::Running);

        assert_eq!(
            permissions.available_admit_bytes_for_tun_mtu(tun_mtu, 1),
            (D16_ACTOR_PAYLOAD_PACKET_BUDGET - 1) * tcp_payload_bytes,
        );
        assert_eq!(
            permissions.available_admit_bytes_for_tun_mtu(tun_mtu, tcp_payload_bytes + 1),
            (D16_ACTOR_PAYLOAD_PACKET_BUDGET - 2) * tcp_payload_bytes,
        );
    }

    #[test]
    fn d16_actor_packet_budget_reserves_ack_and_window_update_feedback() {
        const MODELED_TUN_RX_RING_PACKETS: usize = 64;
        const ORDINARY_TUN_RX_DRAIN_PACKETS: usize = 16;
        const FEEDBACK_PACKETS_PER_PAYLOAD_PACKET: usize = 2;
        const ACTOR_SERVICE_WINDOW_BYTES: usize = 128 * 1024;
        const ACTOR_MAX_CYCLES: usize = 8;

        for tun_mtu in [1200usize, 1500] {
            let tcp_payload_bytes = tun_mtu - 40;
            let permissions = EgressPermissions::for_phase(EgressPhase::Running);
            let admit_bytes = permissions.max_admit_bytes_for_tun_mtu(tun_mtu);
            let payload_packets = admit_bytes.div_ceil(tcp_payload_bytes);

            assert!(
                payload_packets * FEEDBACK_PACKETS_PER_PAYLOAD_PACKET
                    + ORDINARY_TUN_RX_DRAIN_PACKETS
                    <= MODELED_TUN_RX_RING_PACKETS
            );
            assert!(admit_bytes * ACTOR_MAX_CYCLES >= ACTOR_SERVICE_WINDOW_BYTES);
        }
    }

    #[test]
    fn d16_running_enters_drain_only_on_drop() {
        assert_eq!(
            transition_egress_phase(EgressPhase::Running, false, true, false, false),
            EgressPhase::DrainOnly
        );
    }

    #[test]
    fn d16_drain_only_enters_recovery_below_low_after_progress() {
        assert_eq!(
            transition_egress_phase(EgressPhase::DrainOnly, false, false, true, false),
            EgressPhase::DrainOnly,
            "low pressure without drain progress must not reopen admission"
        );
        assert_eq!(
            transition_egress_phase(EgressPhase::DrainOnly, false, false, true, true),
            EgressPhase::Recovery { clean_cycles: 0 }
        );
    }

    #[test]
    fn d16_recovery_requires_four_clean_cycles() {
        let mut phase = EgressPhase::Recovery { clean_cycles: 0 };
        for clean_cycles in 1..D16_RECOVERY_CLEAN_CYCLES {
            phase = transition_egress_phase(phase, false, false, true, true);
            assert_eq!(phase, EgressPhase::Recovery { clean_cycles });
        }
        assert_eq!(
            transition_egress_phase(phase, false, false, true, true),
            EgressPhase::Running
        );
    }

    #[test]
    fn d16_recovery_falls_back_on_new_pressure() {
        assert_eq!(
            transition_egress_phase(
                EgressPhase::Recovery { clean_cycles: 3 },
                true,
                false,
                false,
                false,
            ),
            EgressPhase::DrainOnly
        );
        assert_eq!(
            transition_egress_phase(
                EgressPhase::Recovery { clean_cycles: 3 },
                false,
                true,
                true,
                true,
            ),
            EgressPhase::DrainOnly
        );
    }

    #[test]
    fn pending_queue_view_retains_only_payload_sized_lease_on_append() {
        let mut pending = Vec::new();
        let mut permits = VecDeque::new();

        let append = TcpEgressPendingQueueView::new(&mut pending, &mut permits)
            .append(&[7; 64], Some(TestLease::new(96)));

        assert_eq!(append.appended_bytes, 64);
        assert_eq!(append.released_excess_bytes, 32);
        assert_eq!(append.retained_lease_bytes, 64);
        assert_eq!(append.pending_bytes_after, 64);
        assert_eq!(append.leased_bytes_after, 64);

        let snapshot = TcpEgressPendingQueueView::new(&mut pending, &mut permits).snapshot();
        assert_eq!(
            snapshot,
            TcpEgressPendingSnapshot {
                pending_bytes: 64,
                leased_bytes: 64,
                permit_chunks: 1
            }
        );
    }

    #[test]
    fn pending_queue_view_consume_releases_admitted_prefix_only() {
        let mut pending = Vec::new();
        let mut permits = VecDeque::new();
        {
            let mut view = TcpEgressPendingQueueView::new(&mut pending, &mut permits);
            view.append(&[1; 64], Some(TestLease::new(64)));
            view.append(&[2; 64], Some(TestLease::new(64)));
        }

        let consume = TcpEgressPendingQueueView::new(&mut pending, &mut permits).consume_prefix(96);

        assert_eq!(consume.consumed_bytes, 96);
        assert_eq!(consume.released_lease_bytes, 96);
        assert_eq!(consume.pending_bytes_after, 32);
        assert_eq!(consume.leased_bytes_after, 32);
        assert_eq!(&pending[..], &[2; 32]);
    }

    #[test]
    fn pending_queue_view_transfer_prefix_moves_lease_without_release() {
        let mut pending = Vec::new();
        let mut permits = VecDeque::new();
        let mut inflight = VecDeque::new();
        {
            let mut view = TcpEgressPendingQueueView::new(&mut pending, &mut permits);
            view.append(&[1; 64], Some(TestLease::new(64)));
            view.append(&[2; 64], Some(TestLease::new(64)));
        }

        let transfer = TcpEgressPendingQueueView::new(&mut pending, &mut permits)
            .transfer_prefix_to(96, &mut inflight);

        assert_eq!(transfer.transferred_bytes, 96);
        assert_eq!(transfer.transferred_lease_bytes, 96);
        assert_eq!(transfer.pending_bytes_after, 32);
        assert_eq!(transfer.pending_leased_bytes_after, 32);
        assert_eq!(transfer.inflight_leased_bytes_after, 96);
        assert_eq!(&pending[..], &[2; 32]);

        let mut released = 0usize;
        while let Some(mut permit) = inflight.pop_front() {
            released = released.saturating_add(permit.release(usize::MAX));
        }
        assert_eq!(released, 96);
    }

    #[test]
    fn pending_queue_view_zero_consume_preserves_backlog_and_lease() {
        let mut pending = Vec::new();
        let mut permits = VecDeque::new();
        {
            let mut view = TcpEgressPendingQueueView::new(&mut pending, &mut permits);
            view.append(&[3; 64], Some(TestLease::new(64)));
        }

        let consume = TcpEgressPendingQueueView::new(&mut pending, &mut permits).consume_prefix(0);

        assert_eq!(consume.consumed_bytes, 0);
        assert_eq!(consume.released_lease_bytes, 0);
        assert_eq!(consume.pending_bytes_after, 64);
        assert_eq!(consume.leased_bytes_after, 64);
        assert_eq!(&pending[..], &[3; 64]);
    }

    #[test]
    fn pending_queue_view_clear_releases_all_remaining_leases() {
        let mut pending = Vec::new();
        let mut permits = VecDeque::new();
        {
            let mut view = TcpEgressPendingQueueView::new(&mut pending, &mut permits);
            view.append(&[4; 32], Some(TestLease::new(32)));
            view.append(&[5; 48], Some(TestLease::new(48)));
        }

        let clear = TcpEgressPendingQueueView::new(&mut pending, &mut permits).clear();

        assert_eq!(clear.consumed_bytes, 80);
        assert_eq!(clear.released_lease_bytes, 80);
        assert_eq!(clear.pending_bytes_after, 0);
        assert_eq!(clear.leased_bytes_after, 0);
        assert!(pending.is_empty());
        assert!(permits.is_empty());
    }

    #[test]
    fn admits_backlog_until_window_target_without_timer_dependency() {
        let mut flow = TcpEgressFlowModel::new(512 * 1024);
        flow.append_downlink(512 * 1024);

        let outcome = flow.service_window(config()).unwrap();

        assert_eq!(outcome.stop_reason, TcpEgressStopReason::TargetReached);
        assert_eq!(outcome.cycles, 4);
        assert_eq!(outcome.admitted_bytes, 256 * 1024);
        assert_eq!(outcome.released_lease_bytes, 256 * 1024);
        assert_eq!(outcome.pending_bytes, 256 * 1024);
        assert_eq!(outcome.leased_bytes, 256 * 1024);
        assert_eq!(outcome.wake, TcpEgressWake::Immediate);
    }

    #[test]
    fn drains_socket_pressure_then_admits_more_in_same_service_window() {
        let mut flow = TcpEgressFlowModel::new(64 * 1024);
        flow.set_socket_queue_bytes(64 * 1024);
        flow.append_downlink(128 * 1024);

        let outcome = flow
            .service_window(TcpEgressConfig {
                max_cycles: 2,
                target_bytes_per_window: 128 * 1024,
                max_admit_bytes_per_cycle: 64 * 1024,
                drain_bytes_per_cycle: 32 * 1024,
            })
            .unwrap();

        assert_eq!(outcome.stop_reason, TcpEgressStopReason::CycleBudget);
        assert_eq!(outcome.drained_bytes, 64 * 1024);
        assert_eq!(outcome.admitted_bytes, 64 * 1024);
        assert_eq!(outcome.pending_bytes, 64 * 1024);
        assert_eq!(outcome.socket_queue_bytes, 64 * 1024);
        assert_eq!(outcome.wake, TcpEgressWake::Immediate);
    }

    #[test]
    fn no_progress_with_backlog_waits_for_external_readiness_without_releasing_bytes() {
        let mut flow = TcpEgressFlowModel::new(64 * 1024);
        flow.set_socket_queue_bytes(64 * 1024);
        flow.append_downlink(64 * 1024);

        let outcome = flow
            .service_window(TcpEgressConfig {
                max_cycles: 4,
                target_bytes_per_window: 64 * 1024,
                max_admit_bytes_per_cycle: 64 * 1024,
                drain_bytes_per_cycle: 0,
            })
            .unwrap();

        assert_eq!(outcome.stop_reason, TcpEgressStopReason::NoProgress);
        assert_eq!(outcome.admitted_bytes, 0);
        assert_eq!(outcome.released_lease_bytes, 0);
        assert_eq!(outcome.pending_bytes, 64 * 1024);
        assert_eq!(outcome.leased_bytes, 64 * 1024);
        assert_eq!(outcome.wake, TcpEgressWake::ExternalReadiness);
    }

    #[test]
    fn explicit_drop_releases_pending_lease() {
        let mut flow = TcpEgressFlowModel::new(64 * 1024);
        flow.append_downlink(96 * 1024);
        let outcome = flow
            .service_window(TcpEgressConfig {
                max_cycles: 1,
                target_bytes_per_window: 96 * 1024,
                max_admit_bytes_per_cycle: 32 * 1024,
                drain_bytes_per_cycle: 0,
            })
            .unwrap();
        assert_eq!(outcome.admitted_bytes, 32 * 1024);
        assert_eq!(flow.leased_bytes(), 64 * 1024);

        assert_eq!(flow.drop_pending(), 64 * 1024);
        assert_eq!(flow.pending_bytes(), 0);
        assert_eq!(flow.leased_bytes(), 0);
    }

    #[test]
    fn invalid_configs_are_rejected_before_service_starts() {
        assert_eq!(
            TcpEgressConfig {
                max_cycles: 0,
                target_bytes_per_window: 1,
                max_admit_bytes_per_cycle: 1,
                drain_bytes_per_cycle: 0,
            }
            .validate()
            .unwrap_err(),
            TcpEgressConfigError::ZeroCycles
        );
        assert_eq!(
            TcpEgressConfig {
                max_cycles: 1,
                target_bytes_per_window: 0,
                max_admit_bytes_per_cycle: 1,
                drain_bytes_per_cycle: 0,
            }
            .validate()
            .unwrap_err(),
            TcpEgressConfigError::ZeroTargetBytes
        );
        assert_eq!(
            TcpEgressConfig {
                max_cycles: 1,
                target_bytes_per_window: 1,
                max_admit_bytes_per_cycle: 0,
                drain_bytes_per_cycle: 0,
            }
            .validate()
            .unwrap_err(),
            TcpEgressConfigError::ZeroAdmitBytes
        );
    }
}
