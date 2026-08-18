//! Upstream ownership facade for Knife16.
//!
//! This module is the branch-by-abstraction boundary between the proven
//! legacy TUIC/REALITY relay implementations and the resumable session owned
//! data plane.  Transport legs and replay mechanics stay behind this facade.

pub(crate) mod leg;
pub(crate) mod leg_io;
mod legacy;
pub(crate) mod owner_target;
pub(crate) mod session;
pub(crate) mod standby;
pub(crate) mod supervisor;
pub(crate) mod target;
mod tcp;
pub(crate) mod two_leg;
pub(crate) mod two_leg_harness;
mod udp;

use crate::shared::{ClientError, TargetAddr};

pub use legacy::LegacyOwnedUpstream;
pub use tcp::{
    DriverInput, FlowPortConfig, FlowPortConfigError, FlowPortError, FlowPortProbe,
    FlowPortSnapshot, HalfCloseDelivery, ResumableTcpDownlink, ResumableTcpDriver,
    ResumableTcpFlow, ResumableTcpPortFactory, ResumableTcpUplink, SinkDelivery, TerminalAction,
    TerminalDelivery, TunAction, UplinkData, UplinkOwnership, UplinkReplayOwnership,
};
pub use udp::{LocalAssociationId, UdpDownlink, UdpDownlinkEvent, UdpDownlinkSource, UdpUplink};

use crate::upstream::OpenedTcpRelay;

/// One TCP flow opened through the ownership facade.
///
/// The legacy variant deliberately retains [`OpenedTcpRelay`] intact so the
/// existing Generic/Native/NativeByteOwned relay selection is unchanged.
/// The resumable variant exposes only a protocol-owned flow capability; its
/// transport leg and replay implementation remain private to the adapter.
pub enum OpenedOwnedTcp {
    Legacy(OpenedTcpRelay),
    Resumable(ResumableTcpFlow),
}

/// Local byte-ownership contract required before opening a TCP flow.
///
/// This contains no transport detail. It lets the TUN owner preserve the
/// proven legacy extraction order while ensuring a resumable adapter reserves
/// replay capacity before consuming bytes from smoltcp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpOwnershipMode {
    LegacyRelay,
    Resumable,
}

/// Transport-independent upstream facade selected by the TUN data plane.
#[async_trait::async_trait]
pub trait OwnedUpstream: Send + Sync {
    async fn open_tcp_flow(&self, target: &TargetAddr) -> Result<OpenedOwnedTcp, ClientError>;

    async fn send_udp(&self, uplink: UdpUplink<'_>);

    fn tcp_ownership_mode(&self) -> TcpOwnershipMode {
        TcpOwnershipMode::LegacyRelay
    }

    /// Whether opening a TCP flow is safe to await in the single-owner TUN
    /// event loop. Production network transports must return `false`.
    fn open_is_cheap(&self) -> bool {
        true
    }

    fn failover_leg_u8(&self) -> u8 {
        crate::metrics::NO_FAILOVER
    }

    fn udp_drops_up(&self) -> u64 {
        0
    }

    fn udp_stream_fallbacks(&self) -> u64 {
        0
    }
}
