//! Pacing of packet transmissions.

use std::num::NonZeroU16;

use crate::{Duration, Instant};

use tracing::warn;

/// A simple token-bucket pacer
///
/// The pacer's capacity is derived on a fraction of the congestion window
/// which can be sent in regular intervals
/// Once the bucket is empty, further transmission is blocked.
/// The bucket refills at a rate slightly faster
/// than one congestion window per RTT, as recommended in
/// <https://tools.ietf.org/html/draft-ietf-quic-recovery-34#section-7.7>
pub(super) struct Pacer {
    uncapped_capacity: u64,
    capacity: u64,
    last_window: u64,
    last_mtu: u16,
    tokens: u64,
    prev: Instant,
    max_burst_datagram_equivalents: Option<NonZeroU16>,
    delay_events: u64,
}

impl Pacer {
    /// Obtains a new [`Pacer`].
    pub(super) fn new(smoothed_rtt: Duration, window: u64, mtu: u16, now: Instant) -> Self {
        Self::new_with_max_burst(smoothed_rtt, window, mtu, now, None)
    }

    pub(super) fn new_configured(
        smoothed_rtt: Duration,
        window: u64,
        mtu: u16,
        now: Instant,
        max_burst_datagram_equivalents: Option<NonZeroU16>,
    ) -> Self {
        match max_burst_datagram_equivalents {
            None => Self::new(smoothed_rtt, window, mtu, now),
            Some(max_burst) => {
                Self::new_with_max_burst(smoothed_rtt, window, mtu, now, Some(max_burst))
            }
        }
    }

    pub(super) fn new_with_max_burst(
        smoothed_rtt: Duration,
        window: u64,
        mtu: u16,
        now: Instant,
        max_burst_datagram_equivalents: Option<NonZeroU16>,
    ) -> Self {
        let uncapped_capacity = optimal_capacity(smoothed_rtt, window, mtu);
        let capacity =
            apply_max_burst_capacity(uncapped_capacity, mtu, max_burst_datagram_equivalents);
        Self {
            uncapped_capacity,
            capacity,
            last_window: window,
            last_mtu: mtu,
            tokens: capacity,
            prev: now,
            max_burst_datagram_equivalents,
            delay_events: 0,
        }
    }

    pub(super) fn uncapped_capacity(&self) -> u64 {
        self.uncapped_capacity
    }

    pub(super) fn capacity(&self) -> u64 {
        self.capacity
    }

    pub(super) fn tokens(&self) -> u64 {
        self.tokens
    }

    pub(super) fn mtu(&self) -> u16 {
        self.last_mtu
    }

    pub(super) fn configured_cap_active(&self) -> bool {
        self.capacity < self.uncapped_capacity
    }

    pub(super) fn delay_events(&self) -> u64 {
        self.delay_events
    }

    pub(super) fn max_burst_datagram_equivalents(&self) -> Option<NonZeroU16> {
        self.max_burst_datagram_equivalents
    }

    /// Record that a packet has been transmitted.
    pub(super) fn on_transmit(&mut self, packet_length: u16) {
        self.tokens = self.tokens.saturating_sub(packet_length.into())
    }

    /// Return how long we need to wait before sending `bytes_to_send`
    ///
    /// If we can send a packet right away, this returns `None`. Otherwise, returns `Some(d)`,
    /// where `d` is the time before this function should be called again.
    ///
    /// The 5/4 ratio used here comes from the suggestion that N = 1.25 in the draft IETF RFC for
    /// QUIC.
    pub(super) fn delay(
        &mut self,
        smoothed_rtt: Duration,
        bytes_to_send: u64,
        mtu: u16,
        window: u64,
        now: Instant,
    ) -> Option<Instant> {
        debug_assert_ne!(
            window, 0,
            "zero-sized congestion control window is nonsense"
        );

        if window != self.last_window || mtu != self.last_mtu {
            self.uncapped_capacity = optimal_capacity(smoothed_rtt, window, mtu);
            self.capacity = apply_max_burst_capacity(
                self.uncapped_capacity,
                mtu,
                self.max_burst_datagram_equivalents,
            );

            // Clamp the tokens
            self.tokens = self.capacity.min(self.tokens);
            self.last_window = window;
            self.last_mtu = mtu;
        }

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        // we disable pacing for extremely large windows
        if window > u64::from(u32::MAX) {
            return None;
        }

        let window = window as u32;

        let time_elapsed = now.checked_duration_since(self.prev).unwrap_or_else(|| {
            warn!("received a timestamp early than a previous recorded time, ignoring");
            Default::default()
        });

        if smoothed_rtt.as_nanos() == 0 {
            return None;
        }

        let elapsed_rtts = time_elapsed.as_secs_f64() / smoothed_rtt.as_secs_f64();
        let new_tokens = window as f64 * 1.25 * elapsed_rtts;
        self.tokens = self
            .tokens
            .saturating_add(new_tokens as _)
            .min(self.capacity);

        self.prev = now;

        // if we can already send a packet, there is no need for delay
        if self.tokens >= bytes_to_send {
            return None;
        }

        let unscaled_delay = smoothed_rtt
            .checked_mul((bytes_to_send.max(self.capacity) - self.tokens) as _)
            .unwrap_or(Duration::MAX)
            / window;

        // divisions come before multiplications to prevent overflow
        // this is the time at which the pacing window becomes empty
        self.delay_events = self.delay_events.saturating_add(1);
        Some(self.prev + (unscaled_delay / 5) * 4)
    }
}

/// Calculates a pacer capacity for a certain window and RTT
///
/// The goal is to emit a burst (of size `capacity`) in timer intervals
/// which compromise between
/// - ideally distributing datagrams over time
/// - constantly waking up the connection to produce additional datagrams
///
/// Too short burst intervals means we will never meet them since the timer
/// accuracy in user-space is not high enough. If we miss the interval by more
/// than 25%, we will lose that part of the congestion window since no additional
/// tokens for the extra-elapsed time can be stored.
///
/// Too long burst intervals make pacing less effective.
fn optimal_capacity(smoothed_rtt: Duration, window: u64, mtu: u16) -> u64 {
    let rtt = smoothed_rtt.as_nanos().max(1);
    let capacity = ((window as u128 * BURST_INTERVAL_NANOS) / rtt) as u64;

    // Small bursts are less efficient (no GSO), could increase latency and don't effectively
    // use the channel's buffer capacity. Large bursts might block the connection on sending.
    capacity.clamp(MIN_BURST_SIZE * mtu as u64, MAX_BURST_SIZE * mtu as u64)
}

fn apply_max_burst_capacity(
    uncapped_capacity: u64,
    mtu: u16,
    max_burst_datagram_equivalents: Option<NonZeroU16>,
) -> u64 {
    let max_burst_size = max_burst_datagram_equivalents
        .map_or(MAX_BURST_SIZE, |value| u64::from(value.get()))
        .min(MAX_BURST_SIZE);
    uncapped_capacity.min(max_burst_size * u64::from(mtu))
}

/// The burst interval
///
/// The capacity will we refilled in 4/5 of that time.
/// 2ms is chosen here since framework timers might have 1ms precision.
/// If kernel-level pacing is supported later a higher time here might be
/// more applicable.
const BURST_INTERVAL_NANOS: u128 = 2_000_000; // 2ms

/// Allows some usage of GSO, and doesn't slow down the handshake.
const MIN_BURST_SIZE: u64 = 10;

/// Creating 256 packets took 1ms in a benchmark, so larger bursts don't make sense.
const MAX_BURST_SIZE: u64 = 256;

#[cfg(test)]
mod tests {
    use std::num::NonZeroU16;

    use super::*;

    #[test]
    fn configured_capacity_derives_from_one_upstream_capacity() {
        let mtu = 1200;
        let uncapped = 200 * u64::from(mtu);

        assert_eq!(apply_max_burst_capacity(uncapped, mtu, None), uncapped);
        assert_eq!(
            apply_max_burst_capacity(uncapped, mtu, NonZeroU16::new(1)),
            u64::from(mtu)
        );
        assert_eq!(
            apply_max_burst_capacity(uncapped, mtu, NonZeroU16::new(64)),
            64 * u64::from(mtu)
        );
        assert_eq!(
            apply_max_burst_capacity(uncapped, mtu, NonZeroU16::new(300)),
            uncapped
        );
    }

    #[test]
    fn configured_maximum_bounds_initial_capacity() {
        let mtu = 1200;
        let now = Instant::now();

        let capped =
            Pacer::new_with_max_burst(Duration::ZERO, 2_000_000, mtu, now, NonZeroU16::new(64));
        assert_eq!(capped.capacity, 64 * u64::from(mtu));

        let below_upstream_minimum =
            Pacer::new_with_max_burst(Duration::ZERO, 2_000_000, mtu, now, NonZeroU16::new(1));
        assert_eq!(below_upstream_minimum.capacity, u64::from(mtu));

        let above_upstream_maximum =
            Pacer::new_with_max_burst(Duration::ZERO, 2_000_000, mtu, now, NonZeroU16::new(300));
        assert_eq!(
            above_upstream_maximum.capacity,
            MAX_BURST_SIZE * u64::from(mtu)
        );
    }

    #[test]
    fn configured_maximum_preserves_refill_rate_and_elapsed_debt() {
        let window = 2_000_000;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();
        let empty_pacer = || {
            let mut pacer = Pacer::new_with_max_burst(rtt, window, mtu, now, NonZeroU16::new(64));
            assert_eq!(pacer.capacity, 64_000);
            for _ in 0..64 {
                pacer.on_transmit(mtu);
            }
            assert_eq!(pacer.tokens, 0);
            pacer
        };
        let mut pacer = empty_pacer();

        let full_refill = Duration::from_micros(1_280);
        assert_eq!(
            pacer
                .delay(rtt, u64::from(mtu), mtu, window, now)
                .expect("empty capped bucket must delay")
                .duration_since(now),
            full_refill
        );

        assert_eq!(
            pacer.delay(rtt, u64::from(mtu), mtu, window, now + full_refill / 2,),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        let mut full_pacer = empty_pacer();
        assert_eq!(
            full_pacer.delay(rtt, u64::from(mtu), mtu, window, now + full_refill,),
            None
        );
        assert_eq!(full_pacer.tokens, full_pacer.capacity);
    }

    #[test]
    fn configured_cap_refills_multiple_buckets_inside_one_millisecond() {
        let mtu = 1280;
        let rtt = Duration::from_micros(200);
        let window = 40_000;
        let start = Instant::now();
        let mut pacer = Pacer::new_with_max_burst(rtt, window, mtu, start, NonZeroU16::new(64));
        let mut sent_datagrams = 0;

        for tick in 0..=20 {
            let now = start + Duration::from_micros(tick * 50);
            for _ in 0..20 {
                if pacer.delay(rtt, u64::from(mtu), mtu, window, now).is_some() {
                    break;
                }
                pacer.on_transmit(mtu);
                sent_datagrams += 1;
            }
        }

        assert_eq!(pacer.capacity(), 64 * u64::from(mtu));
        assert_eq!(sent_datagrams, 259);
        assert!(
            sent_datagrams > 64,
            "the replay must distinguish stored capacity from a time-window bound"
        );
    }

    #[test]
    fn two_connections_bound_stored_paced_capacity_independently() {
        let mtu = 1200;
        let now = Instant::now();
        let new_pacer =
            || Pacer::new_with_max_burst(Duration::ZERO, 2_000_000, mtu, now, NonZeroU16::new(64));
        let first = new_pacer();
        let second = new_pacer();

        assert_eq!(first.capacity(), 64 * u64::from(mtu));
        assert_eq!(second.capacity(), 64 * u64::from(mtu));
        assert_eq!(first.tokens() + second.tokens(), 128 * u64::from(mtu));
    }

    #[test]
    fn zero_sized_ack_accounting_does_not_spend_pacing_tokens() {
        let mtu = 1200;
        let mut pacer = Pacer::new_with_max_burst(
            Duration::ZERO,
            2_000_000,
            mtu,
            Instant::now(),
            NonZeroU16::new(64),
        );
        let before = pacer.tokens();

        pacer.on_transmit(0);

        assert_eq!(pacer.tokens(), before);
    }

    #[test]
    fn does_not_panic_on_bad_instant() {
        let old_instant = Instant::now();
        let new_instant = old_instant + Duration::from_micros(15);
        let rtt = Duration::from_micros(400);

        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(Duration::from_micros(0), 0, 1500, 1, old_instant)
            .is_none());
        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(Duration::from_micros(0), 1600, 1500, 1, old_instant)
            .is_none());
        assert!(Pacer::new(rtt, 30000, 1500, new_instant)
            .delay(Duration::from_micros(0), 1500, 1500, 3000, old_instant)
            .is_none());
    }

    #[test]
    fn derives_initial_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(Duration::from_millis(0), window, mtu, now);
        assert_eq!(pacer.capacity, MAX_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);

        let pacer = Pacer::new(rtt, 1, mtu, now);
        assert_eq!(pacer.capacity, MIN_BURST_SIZE * mtu as u64);
        assert_eq!(pacer.tokens, pacer.capacity);
    }

    #[test]
    fn adjusts_capacity() {
        let window = 2_000_000;
        let mtu = 1500;
        let rtt = Duration::from_millis(50);
        let now = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, pacer.capacity);
        let initial_tokens = pacer.tokens;

        pacer.delay(rtt, mtu as u64, mtu, window * 2, now);
        assert_eq!(
            pacer.capacity,
            (2 * window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens);

        pacer.delay(rtt, mtu as u64, mtu, window / 2, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 / 2 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );
        assert_eq!(pacer.tokens, initial_tokens / 2);

        pacer.delay(rtt, mtu as u64, mtu * 2, window, now);
        assert_eq!(
            pacer.capacity,
            (window as u128 * BURST_INTERVAL_NANOS / rtt.as_nanos()) as u64
        );

        pacer.delay(rtt, mtu as u64, 20_000, window, now);
        assert_eq!(pacer.capacity, 20_000_u64 * MIN_BURST_SIZE);
    }

    #[test]
    fn computes_pause_correctly() {
        let window = 2_000_000u64;
        let mtu = 1000;
        let rtt = Duration::from_millis(50);
        let old_instant = Instant::now();

        let mut pacer = Pacer::new(rtt, window, mtu, old_instant);
        let packet_capacity = pacer.capacity / mtu as u64;

        for _ in 0..packet_capacity {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        let pace_duration = Duration::from_nanos((BURST_INTERVAL_NANOS * 4 / 5) as u64);

        assert_eq!(
            pacer
                .delay(rtt, mtu as u64, mtu, window, old_instant)
                .expect("Send must be delayed")
                .duration_since(old_instant),
            pace_duration
        );

        // Refill half of the tokens
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity / 2);

        for _ in 0..packet_capacity / 2 {
            assert_eq!(
                pacer.delay(rtt, mtu as u64, mtu, window, old_instant),
                None,
                "When capacity is available packets should be sent immediately"
            );

            pacer.on_transmit(mtu);
        }

        // Refill all capacity by waiting more than the expected duration
        assert_eq!(
            pacer.delay(
                rtt,
                mtu as u64,
                mtu,
                window,
                old_instant + pace_duration * 3 / 2
            ),
            None
        );
        assert_eq!(pacer.tokens, pacer.capacity);
    }
}
