use super::*;

impl Dedup {
    pub(in crate::connection) fn snapshot_for_packet_probe(&self) -> (u128, u64) {
        (self.window, self.next)
    }
}

impl PendingAcks {
    pub(in crate::connection) fn snapshot_for_packet_probe(&self) -> String {
        format!(
            "{:?}",
            (
                (
                    self.immediate_ack_required,
                    self.ack_eliciting_since_last_ack_sent,
                    self.non_ack_eliciting_since_last_ack_sent,
                    self.ack_eliciting_threshold,
                    self.reordering_threshold,
                    self.earliest_ack_eliciting_since_last_ack_sent
                ),
                (
                    self.ranges.iter().collect::<Vec<_>>(),
                    self.largest_packet,
                    self.largest_ack_eliciting_packet,
                    self.largest_acked
                ),
                (
                    self.gap_ack_reinforcement_sent_at,
                    self.gap_ack_reinforcement_active,
                    self.gap_ack_reinforcement_due
                ),
            )
        )
    }
}
