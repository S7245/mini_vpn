//! Cold runtime environment configuration. Owns parsing into the original value
//! config, no budget, device, or actor. Entry: TunRuntimeConfig::from_env_with_relay_policy;
//! defaults/profile characterization remains in client_tun/tests/mod.rs.
use super::*;

/// One explicit cold configuration; a budget without the physical Device slot
/// bound is not a Native configuration. No queue estimate supplies either value.
#[derive(Clone, Copy, Debug)]
pub(super) struct NativeLimits {
    pub(super) backing_bytes: std::num::NonZeroUsize,
    pub(super) device_slots: std::num::NonZeroUsize,
    /// O3 (D105): optional control-plane process ceiling for hard + I_cfg.
    pub(super) memory_ceiling: Option<std::num::NonZeroU64>,
}
impl NativeLimits {
    fn new(bytes: usize, slots: usize) -> Result<Self, ClientError> {
        let checked = |n| {
            std::num::NonZeroUsize::new(n)
                .filter(|n| n.get() <= isize::MAX as usize)
                .ok_or_else(|| {
                    ClientError::InvalidTarget(
                        "Native backing bytes and Device slots must each be in 1..=isize::MAX"
                            .into(),
                    )
                })
        };
        Ok(Self {
            backing_bytes: checked(bytes)?,
            device_slots: checked(slots)?,
            memory_ceiling: None,
        })
    }
    fn from_sources(
        bytes: Option<&std::ffi::OsStr>,
        slots: Option<&std::ffi::OsStr>,
    ) -> Result<Option<Self>, ClientError> {
        let (bytes, slots) = match (bytes, slots) {
            (None, None) => return Ok(None),
            (Some(bytes), Some(slots)) => (bytes, slots),
            _ => return Err(ClientError::InvalidTarget(
                "MINI_VPN_NATIVE_BACKING_BYTES and MINI_VPN_NATIVE_DEVICE_SLOTS must be set together".into(),
            )),
        };
        let parse = |value: &std::ffi::OsStr| {
            value
                .to_str()
                .and_then(|value| value.parse::<usize>().ok())
                .ok_or_else(|| {
                    ClientError::InvalidTarget(
                        "Native backing bytes and Device slots must be positive integers".into(),
                    )
                })
        };
        Self::new(parse(bytes)?, parse(slots)?).map(Some)
    }
}

impl TunRuntimeConfig {
    /// Read config from process environment
    /// （`MINI_VPN_TUN_POOL_SIZE` + `MINI_VPN_METRICS_SECS` + `MINI_VPN_PROFILE_LOOP`
    /// + downlink backpressure watermarks + TCP socket buffers）。
    pub(super) fn from_env_with_relay_policy(
        policy: Arc<RelayFamilyPolicy>,
    ) -> Result<Self, ClientError> {
        let mut cfg = Self::from_sources(std::env::var("MINI_VPN_TUN_POOL_SIZE").ok().as_deref())?;
        cfg.tun_mtu = parse_tun_mtu(std::env::var("MINI_VPN_TUN_MTU").ok().as_deref());
        cfg.tun_tx_queue_len =
            parse_tun_tx_queue_len(std::env::var("MINI_VPN_TUN_TX_QUEUE_LEN").ok().as_deref());
        cfg.metrics_secs =
            parse_metrics_secs(std::env::var("MINI_VPN_METRICS_SECS").ok().as_deref());
        cfg.profile_loop =
            parse_profile_loop(std::env::var("MINI_VPN_PROFILE_LOOP").ok().as_deref());
        let tcp_rx_buffer = std::env::var("MINI_VPN_TCP_RX_BUFFER_BYTES").ok();
        let tcp_tx_buffer = std::env::var("MINI_VPN_TCP_TX_BUFFER_BYTES").ok();
        cfg.tcp_socket_buffers =
            parse_tcp_socket_buffer_config(tcp_rx_buffer.as_deref(), tcp_tx_buffer.as_deref());
        let downlink_backpressure_high =
            std::env::var("MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES").ok();
        let downlink_backpressure_low =
            std::env::var("MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES").ok();
        cfg.downlink_backpressure = parse_downlink_backpressure_config_for_tx_buffer(
            downlink_backpressure_high.as_deref(),
            downlink_backpressure_low.as_deref(),
            cfg.tcp_socket_buffers.tx_bytes,
            cfg.tun_mtu,
            cfg.tun_tx_queue_len,
        );
        cfg.downlink_flush_max_bytes = parse_downlink_flush_max_bytes(
            std::env::var("MINI_VPN_DOWNLINK_FLUSH_MAX_BYTES")
                .ok()
                .as_deref(),
        );
        cfg.downlink_egress_immediate_bytes = parse_downlink_egress_immediate_bytes(
            std::env::var("MINI_VPN_DOWNLINK_EGRESS_IMMEDIATE_BYTES")
                .ok()
                .as_deref(),
        );
        cfg.tun_rx_drain_budget = parse_tun_rx_drain_budget(
            std::env::var("MINI_VPN_TUN_RX_DRAIN_BUDGET")
                .ok()
                .as_deref(),
        );
        cfg.tun_rx_active_flow_timer_ms = parse_tun_rx_active_flow_timer_ms(
            std::env::var("MINI_VPN_TUN_RX_ACTIVE_FLOW_TIMER_MS")
                .ok()
                .as_deref(),
        );
        cfg.bounded_global_rx_receive_window = parse_trace(
            std::env::var("MINI_VPN_BOUNDED_GLOBAL_RX_RECEIVE_WINDOW")
                .ok()
                .as_deref(),
        );
        let buffered_downlink_enabled =
            parse_trace(std::env::var("MINI_VPN_BUFFERED_DOWNLINK").ok().as_deref());
        cfg.buffered_downlink = BufferedDownlinkConfig::from_backpressure(
            buffered_downlink_enabled,
            cfg.downlink_backpressure,
            cfg.listener.pool_size,
        );
        cfg.d3_egress_actor =
            parse_d3_egress_actor(std::env::var("MINI_VPN_D3_EGRESS_ACTOR").ok().as_deref());
        cfg.d3_egress_actor_adaptive_credit = parse_d3_egress_actor_adaptive_credit(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_ADAPTIVE_CREDIT")
                .ok()
                .as_deref(),
        );
        cfg.d3_egress_actor_legacy_credit = parse_d3_egress_actor_legacy_credit(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_LEGACY_CREDIT")
                .ok()
                .as_deref(),
        );
        cfg.d3_egress_actor_self_wake = parse_d3_egress_actor_self_wake(
            std::env::var("MINI_VPN_D3_EGRESS_ACTOR_SELF_WAKE")
                .ok()
                .as_deref(),
        );
        cfg.d4_stalled_read_service = parse_d4_stalled_read_service(
            std::env::var("MINI_VPN_D4_STALLED_READ_SERVICE")
                .ok()
                .as_deref(),
        );
        cfg.d5_capacity_backpressure = parse_d5_capacity_backpressure(
            std::env::var("MINI_VPN_D5_CAPACITY_BACKPRESSURE")
                .ok()
                .as_deref(),
        );
        cfg.native_limits = NativeLimits::from_sources(
            std::env::var_os("MINI_VPN_NATIVE_BACKING_BYTES").as_deref(),
            std::env::var_os("MINI_VPN_NATIVE_DEVICE_SLOTS").as_deref(),
        )?;
        if let Some(raw) = std::env::var_os("MINI_VPN_NATIVE_MEMORY_CEILING_BYTES") {
            let ceiling = raw.to_str().and_then(|value| value.parse::<u64>().ok());
            cfg = cfg.with_native_memory_ceiling(ceiling.unwrap_or(0))?;
        }
        cfg = cfg.with_relay_policy(policy);
        Ok(cfg)
    }
}

impl TunRuntimeConfig {
    /// Explicit shared backing limit and Device slots, checked and installed as
    /// one value. The public CLI opts in through the same pair of env values (D105).
    pub fn with_native_limits(
        mut self,
        bytes: usize,
        device_slots: usize,
    ) -> Result<Self, ClientError> {
        self.native_limits = Some(NativeLimits::new(bytes, device_slots)?);
        Ok(self)
    }

    /// O3 (D105): process ceiling for the Native total-memory preflight
    /// (hard + configured instances). Requires Native limits; zero is invalid.
    pub fn with_native_memory_ceiling(mut self, bytes: u64) -> Result<Self, ClientError> {
        let limits = self.native_limits.as_mut().ok_or_else(|| {
            ClientError::InvalidTarget(
                "MINI_VPN_NATIVE_MEMORY_CEILING_BYTES requires Native backing limits".into(),
            )
        })?;
        limits.memory_ceiling = Some(std::num::NonZeroU64::new(bytes).ok_or_else(|| {
            ClientError::InvalidTarget("Native memory ceiling must be a positive integer".into())
        })?);
        Ok(self)
    }
}

#[cfg(test)]
mod tests;
