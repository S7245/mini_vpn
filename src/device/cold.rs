//! Device cold composition, before any OS/source factory or actor exists. One
//! non-cloneable preparation owns Rx/Egress and an optional original pump lane.
//! Cancelling it releases unpublished storage; no new budget/refund/JOIN owner.
//! The Device keeps its Source until bind's last start commit. Tests: tests/cold_factory.rs.
use super::*;
use bytes::BackingMeter;

pub(crate) struct Parts {
    pub(crate) rx: rx::RxStock,
    pub(crate) egress: egress::Egress,
    pub(crate) pump: Option<pump_native::Prepared>,
    pub(crate) runtime: tokio::runtime::Handle,
}
pub(crate) struct PreparedDevice(Parts);

pub(crate) fn validate_request(
    slots: usize,
    mtu: usize,
    prefix: usize,
    meter: &BackingMeter,
) -> std::io::Result<()> {
    if slots == 0 || !(576..=9000).contains(&mtu) || !matches!(prefix, 0 | 4) {
        return Err(ErrorKind::InvalidInput.into());
    }
    if meter.budget_limit().is_none() {
        return Err(ErrorKind::Unsupported.into());
    }
    meter
        .admit_precharged()
        .map_err(quinn::owned_events::AdmissionError)?;
    Ok(())
}

impl PreparedDevice {
    /// Minimum free budget for `new(slots, mtu, prefix, pumped)` to succeed:
    /// every persistent cold owner it reserves, plus (pumped only) the one
    /// legal packet copy it checks and releases; direct reads lend the already
    /// charged protected RX allocation. Composed only from each owner's own
    /// reservation layout; the exactness tracer lives in tests/cold_factory.rs.
    /// Service-preflight form: the production TUN prefix and the pumped mode,
    /// each an upper bound over the device kinds a factory may build (harness
    /// loopback uses prefix 0; direct mode needs no pump/copy headroom).
    pub(crate) fn service_required_bytes(slots: usize, mtu: usize) -> Option<usize> {
        Self::required_bytes(slots, mtu, tun_packet_header_len(), true)
    }
    pub(crate) fn required_bytes(slots: usize, mtu: usize, prefix: usize, pumped: bool) -> Option<usize> {
        let (pump, copy) = if pumped {
            let copy = BytesMut::metered_copy_layout_for_len(mtu.checked_add(prefix)?).ok()?;
            (pump_native::Prepared::cold_bytes(slots)?, copy)
        } else {
            (0, 0)
        };
        callback::pair_layout_bytes()
            .checked_add(rx::RxStock::layout_bytes(slots)?)?
            .checked_add(rx_packet::cold_bytes())?
            .checked_add(egress::Egress::cold_bytes(slots, mtu, prefix)?)?
            .checked_add(pump)?
            .checked_add(copy)
    }
    pub(crate) fn new(
        slots: usize,
        mtu: usize,
        prefix: usize,
        pumped: bool,
        meter: &BackingMeter,
    ) -> std::io::Result<Self> {
        validate_request(slots, mtu, prefix, meter)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| std::io::Error::from(ErrorKind::Unsupported))?;
        let (tx_callbacks, rx_callbacks) = callback::Custody::pair(meter)?;
        let mut rx = rx::RxStock::native(slots, meter)?;
        rx.protect(rx_callbacks)?;
        let egress = egress::Egress::native_with_callbacks(slots, mtu, prefix, meter, tx_callbacks)?;
        let pump = if pumped {
            Some(pump_native::Prepared::with_ingress(slots, meter, rx.access())?)
        } else {
            None
        };
        let mut ready = Self(Parts {
            rx,
            egress,
            pump,
            runtime,
        });
        // All retained descriptor owners are already charged. Direct reads keep
        // the actual first buffer; pumped mode checks one legal copy's headroom
        // with a real allocation and releases it. That is not persistent credit.
        if pumped {
            drop(
                BytesMut::try_zeroed_metered(mtu + prefix, meter)
                    .map_err(quinn::owned_events::AdmissionError)?,
            );
        } else {
            ready.0.rx.prepare(mtu + prefix)?;
        }
        Ok(ready)
    }
    /// Whole-value ownership transfer, only after the source factory succeeds.
    pub(crate) fn into_parts(self) -> Parts {
        let this = std::mem::ManuallyDrop::new(self);
        // No field is pinned yet. Move the complete preparation exactly once;
        // suppress only its known-unpublished cancellation destructor.
        unsafe { std::ptr::read(&this.0) }
    }
}
impl Drop for PreparedDevice {
    fn drop(&mut self) {
        self.0.rx.discard(&mut (0, 0));
        self.0.egress.discard(&mut (0, 0));
    }
}

impl VirtualTunDevice {
    /// Cold admission precedes the OS factory. Bind is the sole pump start.
    /// The public CLI hard factory calls this with the real OS TUN source (D105).
    pub fn native_with_factory(
        slots: usize,
        mtu: usize,
        pumped: bool,
        meter: &BackingMeter,
        source: impl FnOnce() -> std::io::Result<tun::AsyncDevice>,
    ) -> std::io::Result<Self> {
        #[cfg(not(unix))]
        return Err(ErrorKind::Unsupported.into());
        let prepared = PreparedDevice::new(slots, mtu, tun_packet_header_len(), pumped, meter)?;
        let mut device = source()?;
        let actual_mtu = TunDevice::mtu(device.get_ref()).map_err(std::io::Error::other)?;
        if usize::try_from(actual_mtu).ok() != Some(mtu) {
            return Err(ErrorKind::InvalidInput.into());
        }
        #[cfg(unix)]
        if usize::from(device.get_mut().has_packet_information()) * 4 != tun_packet_header_len() {
            return Err(ErrorKind::InvalidInput.into());
        }
        let interface_name = TunDevice::name(device.get_ref()).ok();
        let parts = prepared.into_parts();
        let backend = if let Some(lane) = parts.pump {
            let (reader, writer) = tokio::io::split(device);
            VirtualTunBackend::Pumped {
                writer,
                ingress: pump_native::Startup::new(
                    lane,
                    AsyncReadPacketSource { reader, mtu },
                    parts.runtime,
                ),
            }
        } else {
            VirtualTunBackend::PreparedDirect {
                device,
                runtime: parts.runtime,
            }
        };
        Ok(Self {
            backend,
            cut: cut::CutBoundary::default(),
            stopping: false,
            shutdown_failure: None,
            shutdown_failure_transferred: false,
            shutdown_callbacks: None,
            interface_name,
            mtu,
            rx: parts.rx,
            egress: parts.egress,
        })
    }

    pub(super) fn native_pending(&self) -> bool {
        match &self.backend {
            VirtualTunBackend::PreparedDirect { .. } => true,
            VirtualTunBackend::Pumped { ingress, .. } => ingress.pending(),
            _ => false,
        }
    }

    pub(super) fn bind_native(&mut self, binding: DeviceBinding<'_>) -> std::io::Result<()> {
        let DeviceBinding::Native { meter, slots, mtu } = binding else {
            let native_pump = matches!(&self.backend, VirtualTunBackend::Pumped { ingress, .. } if ingress.is_native());
            return if self.rx.is_native()
                || self.egress.native_meter().is_some()
                || self.native_pending()
                || native_pump
            {
                Err(ErrorKind::Unsupported.into())
            } else {
                Ok(())
            };
        };
        validate_request(slots, mtu, tun_packet_header_len(), meter)?;
        if self.stopping || self.cut.active() {
            return Err(self.unavailable());
        }
        if self.mtu != mtu {
            return Err(ErrorKind::InvalidInput.into());
        }
        self.rx.bind(meter, slots)?;
        self.egress
            .bind(meter, slots, mtu, tun_packet_header_len())?;
        match &mut self.backend {
            VirtualTunBackend::Stopped => Err(device_closed()),
            VirtualTunBackend::Direct(_) => Ok(()),
            VirtualTunBackend::Pumped { ingress, .. } => {
                ingress.validate(meter, slots)?;
                ingress.commit()
            }
            VirtualTunBackend::PreparedDirect { runtime, .. } => {
                let current = tokio::runtime::Handle::try_current()
                    .map_err(|_| std::io::Error::from(ErrorKind::Unsupported))?;
                if current.id() != runtime.id() {
                    return Err(ErrorKind::InvalidInput.into());
                }
                // All fallible work is done. Moving the concrete source opens
                // direct reads, without polling or spawning during publication.
                let previous = std::mem::replace(&mut self.backend, VirtualTunBackend::Stopped);
                if let VirtualTunBackend::PreparedDirect { device, .. } = previous {
                    self.backend = VirtualTunBackend::Direct(device);
                }
                Ok(())
            }
        }
    }
}
