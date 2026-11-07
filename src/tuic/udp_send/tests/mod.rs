mod encoding;
mod uplink;

impl crate::tuic::TuicUpstream {
    pub(crate) async fn native_probe_primary_for_test(&self) -> bool {
        let conn = self.current_conn().unwrap();
        matches!(
            self.probe_tcp_pool_connection(&conn, std::time::Duration::from_secs(1))
                .await,
            crate::tuic::TcpPoolProbeOutcome::Alive { .. }
        )
    }
}

impl crate::tuic::TuicUpstream {
    pub(crate) fn retire_unused_uplink_for_test(&self) -> usize {
        let sender = self.udp_stream_sender.lock().unwrap().empty_take();
        assert!(sender.slots.iter().all(Option::is_none));
        let bytes = std::mem::size_of_val(&*sender);
        drop(sender);
        bytes
    }
}

impl std::ops::Deref for super::UdpStreamSender {
    type Target = super::Table;
    fn deref(&self) -> &Self::Target {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => &stock.table,
            _ => panic!("test expected a physical table"),
        }
    }
}
impl std::ops::DerefMut for super::UdpStreamSender {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => &mut stock.table,
            _ => panic!("test expected a physical table"),
        }
    }
}

mod custody;

impl super::UdpStreamSender {
    pub(in crate::tuic) fn cold_inventory_for_test(&self) -> Option<(usize, usize, bool, usize)> {
        match self {
            Self::Ready(stock) | Self::Lost(stock) => Some((
                stock.key(),
                std::mem::size_of::<super::Table>(),
                stock.valid(),
                stock.table.slots.iter().flatten().count(),
            )),
            _ => None,
        }
    }
}

thread_local! {
    static COLD_PRESSURE: std::cell::RefCell<Option<ColdPressure>> = const { std::cell::RefCell::new(None) };
}
struct ColdPressure {
    budget: bytes::BackingBudget,
    meter: bytes::BackingMeter,
    actual: Option<bytes::BytesMut>,
    observed: Option<(usize, usize, usize)>,
}
pub(crate) struct ColdPressureGuard;
impl ColdPressureGuard {
    pub(crate) fn observed(&self) -> (usize, usize, usize) {
        COLD_PRESSURE.with(|slot| slot.borrow().as_ref().unwrap().observed.unwrap())
    }
}
impl Drop for ColdPressureGuard {
    fn drop(&mut self) {
        let actual = COLD_PRESSURE.with(|slot| slot.borrow_mut().take());
        drop(actual);
    }
}
impl crate::tuic::TuicUpstream {
    pub(crate) fn hold_uplink_cold_headroom_for_test(
        budget: &bytes::BackingBudget,
    ) -> ColdPressureGuard {
        COLD_PRESSURE.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(ColdPressure {
                budget: budget.clone(),
                meter: bytes::BackingMeter::with_budget(budget),
                actual: None,
                observed: None,
            });
        });
        ColdPressureGuard
    }
}
pub(super) fn before_cold_prepare(meter: &bytes::BackingMeter, layout: usize) {
    COLD_PRESSURE.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(probe) = slot.as_mut().filter(|probe| probe.observed.is_none()) else {
            return;
        };
        assert!(probe.meter.same_budget(meter));
        let before = probe.budget.used();
        let header = bytes::BytesMut::metered_copy_layout_for_len(1).unwrap() - 1;
        let length = probe.budget.limit() - before - (layout - 1) - header;
        probe.actual = Some(bytes::BytesMut::try_zeroed_metered(length, &probe.meter).unwrap());
        probe.observed = Some((before, probe.budget.used(), layout));
        assert_eq!(probe.budget.limit() - probe.budget.used(), layout - 1);
    });
}

thread_local! {
    static POLL_PANIC: std::cell::RefCell<Option<std::sync::Arc<()>>> = const { std::cell::RefCell::new(None) };
}
impl crate::tuic::TuicUpstream {
    pub(crate) fn panic_next_native_uplink_poll_for_test(&self, original: std::sync::Arc<()>) {
        POLL_PANIC.with(|slot| {
            assert!(slot.borrow().is_none());
            *slot.borrow_mut() = Some(original);
        });
    }
    pub(crate) fn uplink_payload_bytes_for_test(&self) -> usize {
        self.udp_runtime
            .uplink_meter()
            .unwrap()
            .snapshot()
            .payload_bytes
    }
    pub(crate) fn uplink_panic_and_stream_for_test(&self, original: &std::sync::Arc<()>) -> bool {
        let state = self.udp_stream_sender.lock().unwrap();
        let super::UdpStreamSender::Lost(stock) = &*state else {
            return false;
        };
        stock.table.stream.is_some()
            && stock
                .table
                .panic
                .as_ref()
                .and_then(|payload| payload.downcast_ref::<std::sync::Arc<()>>())
                .is_some_and(|retained| std::sync::Arc::ptr_eq(retained, original))
    }
}
pub(super) fn before_native_poll() {
    let original = POLL_PANIC.with(|slot| slot.borrow_mut().take());
    if let Some(original) = original {
        std::panic::panic_any(original);
    }
}

impl crate::tuic::TuicUpstream {
    pub(crate) fn uplink_occupied_for_test(&self) -> usize {
        self.udp_stream_sender
            .lock()
            .unwrap()
            .slots
            .iter()
            .flatten()
            .count()
    }
}
