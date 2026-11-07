//! Passive test observations of the single transaction owner.
//! Owns no lifecycle or control state; methods only inspect retained root values.
//! Used by transaction and saved-loop compatibility tests.
use super::super::*;

#[derive(Debug, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime) struct PartialConnectObservation {
    pub(in crate::client_tun::compatibility_runtime) id: FlowIdentity,
    pub(in crate::client_tun::compatibility_runtime) provider: usize,
    pub(in crate::client_tun::compatibility_runtime) deadline: tokio::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) open_deadline: std::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) view: crate::upstream::TcpOpenView,
    pub(in crate::client_tun::compatibility_runtime) escrow: Vec<u8>,
    pub(in crate::client_tun::compatibility_runtime) reserved: usize,
    pub(in crate::client_tun::compatibility_runtime) physical: bool,
    pub(in crate::client_tun::compatibility_runtime) w: bool,
    pub(in crate::client_tun::compatibility_runtime) f: bool,
    pub(in crate::client_tun::compatibility_runtime) roles: bool,
    pub(in crate::client_tun::compatibility_runtime) carrier: bool,
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_stock_inventory_for_test(&self) -> Vec<String> {
        self.opens().slots.iter().flatten().filter_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else { return None; };
            flow.roles.as_ref()?.native_stock_key()?.inventory_for_test()
        }).collect()
    }

    pub(in crate::client_tun::compatibility_runtime) fn unwind_original_native_pump_for_test(
        &self,
        payload: Box<dyn std::any::Any + Send>,
    ) -> Result<(), Box<dyn std::any::Any + Send>> {
        let opens = self.opens();
        let Some(roles) = opens.slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref().filter(|roles| roles.is_native())
        }) else {
            return Err(payload);
        };
        roles.unwind_native_pump_for_test(payload)
    }
    pub(in crate::client_tun::compatibility_runtime) fn flow_native_chunk_queue(
        &self,
    ) -> Option<crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.native_chunk_queue().cloned()
        })
    }

    /// D72 tracer: first Flow (Closing, role close reason, producer_stopped).
    pub(in crate::client_tun::compatibility_runtime) fn flow_close_for_test(
        &self,
    ) -> Option<(bool, Option<(&'static str, &'static str)>, bool)> {
        self.opens().slots.iter().flatten().find_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) => Some((flow.stage == FlowStage::Closing,
                flow.roles.as_ref().and_then(FlowRoles::closed), flow.producer_stopped)),
            _ => None,
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_chunk_queue_for_test(
        &self,
        id: FlowIdentity,
    ) -> Option<crate::tcp_downlink_pump::AsyncByteBoundedFlowQueue> {
        self.opens().slots.iter().flatten().find_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) if flow.id == id => flow.roles.as_ref()?.native_chunk_queue().cloned(),
            _ => None,
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_permit_queue_for_test(
        &self, id: FlowIdentity,
    ) -> Option<crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        self.opens().slots.iter().flatten().find_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) if flow.id == id => {
                let roles = flow.roles.as_ref()?;
                roles.is_native().then(|| roles.permit_queue().cloned()).flatten()
            }
            _ => None,
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_permit_snapshot(
        &self,
    ) -> Option<crate::tcp_downlink_pump::LeasedByteQueueSnapshot> {
        let opens = self.opens();
        opens.slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            let roles = flow.roles.as_ref()?;
            if !roles.is_native() {
                return None;
            }
            Some(roles.permit_queue()?.snapshot_now())
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_permit_queue(
        &self,
    ) -> Option<crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            let roles = flow.roles.as_ref()?;
            roles
                .is_native()
                .then(|| roles.permit_queue().cloned())
                .flatten()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_pump_source_read_bytes(
        &self,
    ) -> Option<usize> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.native_pump_source_read_bytes()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_direct_source_received_bytes(
        &self,
    ) -> Option<usize> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.native_direct_source_received_bytes()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_pending_reader_error_order(
        &self,
    ) -> Option<usize> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.native_pending_reader_error_order()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_provider_view(
        &self,
    ) -> Option<crate::upstream::TcpOpenView> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            if !flow.roles.as_ref()?.is_native() {
                return None;
            }
            flow.attempt.as_ref().map(|attempt| attempt.view())
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_backing_meter(
        &self,
    ) -> Option<bytes::BackingMeter> {
        let opens = self.opens();
        opens.slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.native_backing_meter().cloned()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_backing_meters(
        &self,
    ) -> Vec<bytes::BackingMeter> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) => flow.roles.as_ref()?.native_backing_meter().cloned(),
                _ => None,
            })
            .collect()
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_native_join_counts(
        &self,
    ) -> Vec<usize> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) => {
                    let roles = flow.roles.as_ref()?;
                    roles.is_native().then(|| roles.join_count())
                }
                _ => None,
            })
            .collect()
    }

    pub(in crate::client_tun::compatibility_runtime) fn test_native_meter_bindings(
        &self,
        expected: &bytes::BackingMeter,
    ) -> (usize, usize) {
        let opens = self.opens();
        let mut flows = 0;
        let mut matching = 0;
        for slot in opens.slots.iter().flatten() {
            let OpenPhase::Flow(flow) = &slot.phase else {
                continue;
            };
            if !flow.published {
                continue;
            }
            flows += 1;
            if flow
                .roles
                .as_ref()
                .and_then(|roles| roles.native_backing_meter())
                .is_some_and(|meter| meter.same_authority(expected))
            {
                matching += 1;
            }
        }
        (flows, matching)
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_d16_disposition(
        &self,
    ) -> Option<(usize, usize, usize)> {
        self.opens().flow.as_ref()?.last_d16_disposition
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_d16_backing_meter(
        &self,
    ) -> Option<bytes::BackingMeter> {
        let opens = self.opens();
        opens.slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            flow.roles.as_ref()?.d16_backing_meter().cloned()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_d16_physical_snapshots(
        &self,
    ) -> Option<(
        crate::tcp_downlink_pump::LeasedByteQueueSnapshot,
        crate::tcp_downlink_pump::D16GlobalByteBudgetSnapshot,
        bool,
    )> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            let roles = flow.roles.as_ref()?;
            let queue = roles.d16_queue()?;
            let budget = roles.d16_budget()?;
            Some((
                queue.snapshot_now(),
                budget.snapshot(),
                queue.d16_uses_budget(budget),
            ))
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_d16_queue_for_test(
        &self,
    ) -> Option<crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            if !flow.published {
                return None;
            }
            flow.roles.as_ref()?.d16_queue().cloned()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_d16_close_reason_for_test(
        &self,
    ) -> Option<(&'static str, &'static str)> {
        self.opens().slots.iter().flatten().find_map(|slot| {
            let OpenPhase::Flow(flow) = &slot.phase else {
                return None;
            };
            let roles = flow.roles.as_ref()?;
            roles.is_d16().then(|| roles.closed()).flatten()
        })
    }

    pub(in crate::client_tun::compatibility_runtime) fn test_permit_queue(
        &self,
        id: FlowIdentity,
    ) -> Option<crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue> {
        let opens = self.opens();
        let slot = opens.slots.get(id.index)?.as_ref()?;
        let OpenPhase::Flow(flow) = &slot.phase else {
            return None;
        };
        if flow.id != id || !flow.physical || !flow.published {
            return None;
        }
        flow.roles.as_ref()?.permit_queue().cloned()
    }
    pub(in crate::client_tun::compatibility_runtime) fn retained_provider_io_kind(
        &self,
    ) -> Option<std::io::ErrorKind> {
        let opens = self.opens();
        match opens
            .flow
            .as_ref()?
            .first_provider_failure
            .as_ref()?
            .failure()?
        {
            crate::shared::ClientError::Io(source) => Some(source.kind()),
            _ => None,
        }
    }

    pub(in crate::client_tun::compatibility_runtime) fn partial_connect_observations(
        &self,
    ) -> Vec<PartialConnectObservation> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| {
                let OpenPhase::Flow(flow) = &slot.phase else {
                    return None;
                };
                let attempt = flow.attempt.as_ref()?;
                let view = attempt.view();
                (view.phase == TcpOpenPhase::Connecting).then(|| PartialConnectObservation {
                    id: flow.id,
                    provider: attempt.as_ref() as *const _ as *const () as usize,
                    deadline: slot.deadline,
                    open_deadline: flow.open_deadline,
                    view,
                    escrow: flow.escrow.to_vec(),
                    reserved: flow.reserved,
                    physical: flow.physical,
                    w: flow.w_held,
                    f: flow.f_held,
                    roles: flow.roles.is_some(),
                    carrier: flow.carrier.is_some(),
                })
            })
            .collect()
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(in crate::client_tun::compatibility_runtime) struct FlowObservation {
    pub(in crate::client_tun::compatibility_runtime) factory_started: u64,
    pub(in crate::client_tun::compatibility_runtime) last_fake_release: Option<(
        FlowIdentity,
        crate::fake_ip::FakeIpLeaseReceipt,
        (usize, usize),
    )>,
    pub(in crate::client_tun::compatibility_runtime) live: usize,
    pub(in crate::client_tun::compatibility_runtime) retained_identity_faults: usize,
    pub(in crate::client_tun::compatibility_runtime) w: usize,
    pub(in crate::client_tun::compatibility_runtime) f: usize,
    pub(in crate::client_tun::compatibility_runtime) reserved: usize,
    pub(in crate::client_tun::compatibility_runtime) retained_capacity: usize,
    pub(in crate::client_tun::compatibility_runtime) escrow_bytes: usize,
    pub(in crate::client_tun::compatibility_runtime) running: usize,
    pub(in crate::client_tun::compatibility_runtime) attempts: usize,
    pub(in crate::client_tun::compatibility_runtime) opening_providers: usize,
    pub(in crate::client_tun::compatibility_runtime) waiting: usize,
    pub(in crate::client_tun::compatibility_runtime) retry: usize,
    pub(in crate::client_tun::compatibility_runtime) stopped_producers: usize,
    pub(in crate::client_tun::compatibility_runtime) settled_providers: usize,
    pub(in crate::client_tun::compatibility_runtime) published: u64,
    pub(in crate::client_tun::compatibility_runtime) released: u64,
    pub(in crate::client_tun::compatibility_runtime) joined: u64,
    pub(in crate::client_tun::compatibility_runtime) audit: bool,
    pub(in crate::client_tun::compatibility_runtime) last_data_disposition:
        Option<(usize, usize, usize, usize, usize)>,
    #[cfg(feature = "harness")]
    pub(in crate::client_tun::compatibility_runtime) last_permit_reset:
        Option<crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::PermitResetObservation>,
}
#[cfg(feature = "harness")]
impl FlowObservation {
    /// Verify the original Root's complete retained cap/range receipt. This
    /// observes actual facts and never needs an extra queue owner at Node audit.
    pub(in crate::client_tun::compatibility_runtime) fn assert_permit_reset_receipt(
        &self,
    ) -> crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::PermitResetObservation{
        Self::replay_permit_reset_receipt(
            self.last_permit_reset
                .expect("original Permit reset receipt"),
        )
    }

    fn replay_permit_reset_receipt(
        reset: crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::PermitResetObservation,
    ) -> crate::client_tun::compatibility_runtime::flow_custody::rearm::physical_flow::PermitResetObservation{
        assert!(!reset.pre.event_overflow && !reset.pre.quarantined);
        assert!(reset.pre.cap_count > 0 && reset.pre.admitted > reset.pre.refunded);
        assert_eq!(
            reset.pre.admitted.checked_sub(reset.pre.refunded),
            Some(reset.discarded_inflight as u64)
        );
        assert_eq!(
            (reset.post.epoch, reset.post.origin, reset.post.admitted),
            (reset.pre.epoch, reset.pre.origin, reset.pre.admitted)
        );
        assert_eq!(
            (
                reset.post.cap_count,
                reset.post.refunded,
                reset.post.quarantined
            ),
            (0, reset.pre.admitted, false)
        );
        assert_eq!(
            (reset.post.event_count, reset.post.events),
            (reset.pre.event_count, reset.pre.events)
        );
        assert_eq!(
            reset.released_after.checked_sub(reset.released_before),
            reset.discarded_inflight.checked_add(reset.pending)
        );
        let mut admitted_end = 0u64;
        let mut refunded_end = 0u64;
        let mut cap_count = 0usize;
        let mut cap_ranges = [None; 32];
        let mut cap_remaining = [None; 32];
        for event in reset.pre.events[..reset.pre.event_count].iter().flatten() {
            assert_eq!(event.epoch, reset.pre.epoch);
            match event.kind {
                crate::client_tun::downlink_custody::EgressLedgerEventKind::Admit => {
                    assert_eq!(event.source, None);
                    assert_eq!(event.start, admitted_end);
                    let delta =
                        usize::try_from(event.end.checked_sub(event.start).unwrap()).unwrap();
                    assert!(delta > 0);
                    let ordinal = usize::try_from(event.cap_ordinal).unwrap();
                    if ordinal == cap_count {
                        assert_eq!(event.cap_before, delta);
                        assert_eq!(event.cap_after, delta);
                        cap_ranges[cap_count] = Some((event.start, event.end));
                        cap_remaining[cap_count] = Some(delta);
                        cap_count += 1;
                    } else {
                        assert!(ordinal < cap_count);
                        let remaining = cap_remaining[ordinal].unwrap();
                        assert!(remaining > 0, "retired cap cannot be extended");
                        assert_eq!(
                            cap_remaining[..cap_count]
                                .iter()
                                .filter(|n| n.is_some_and(|n| n > 0))
                                .count(),
                            1
                        );
                        let (start, end) = cap_ranges[ordinal].unwrap();
                        assert_eq!(end, event.start);
                        assert_eq!(event.cap_before, remaining);
                        assert_eq!(Some(event.cap_after), remaining.checked_add(delta));
                        cap_ranges[ordinal] = Some((start, event.end));
                        cap_remaining[ordinal] = Some(event.cap_after);
                    }
                    admitted_end = event.end;
                }
                crate::client_tun::downlink_custody::EgressLedgerEventKind::Refund => {
                    assert!(event.source.is_some());
                    assert_eq!(event.start, refunded_end);
                    let ordinal = event.cap_ordinal as usize;
                    let (start, end) = cap_ranges[ordinal].unwrap();
                    assert!(event.start >= start && event.end <= end);
                    assert_eq!(cap_remaining[ordinal], Some(event.cap_before));
                    assert_eq!(
                        event.cap_before - event.cap_after,
                        (event.end - event.start) as usize
                    );
                    cap_remaining[ordinal] = Some(event.cap_after);
                    refunded_end = event.end;
                }
            }
        }
        assert_eq!(
            (admitted_end, refunded_end),
            (reset.pre.admitted, reset.pre.refunded)
        );
        assert_eq!(
            cap_remaining[..cap_count]
                .iter()
                .filter(|remaining| remaining.is_some_and(|n| n > 0))
                .count(),
            reset.pre.cap_count
        );
        assert_eq!(
            cap_remaining[..cap_count]
                .iter()
                .map(|remaining| remaining.unwrap())
                .sum::<usize>(),
            reset.discarded_inflight
        );
        reset
    }

    pub(in crate::client_tun::compatibility_runtime) fn assert_permit_reset_balance(
        &self,
        queue: &crate::tcp_downlink_pump::AsyncLeasedByteFlowQueue,
    ) {
        let reset = self.assert_permit_reset_receipt();
        assert_eq!(
            queue.custodied_released_bytes_for_test(),
            Some(reset.released_after)
        );
        println!(
            "Permit reset cap balance: epoch={} admitted={} refunded_before={} discarded_inflight={} pending={} released_before={} released_after={} caps={} events={}",
            reset.pre.epoch,
            reset.pre.admitted,
            reset.pre.refunded,
            reset.discarded_inflight,
            reset.pending,
            reset.released_before,
            reset.released_after,
            reset.pre.cap_count,
            reset.pre.event_count
        );
    }
}
#[cfg(test)]
impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn retained_provider_failure(
        &self,
    ) -> Option<(usize, crate::upstream::TcpOpenView, String)> {
        let opens = self.opens();
        let attempt = opens.flow.as_ref()?.first_provider_failure.as_ref()?;
        let source = attempt.failure()?;
        Some((
            source as *const crate::shared::ClientError as usize,
            attempt.view(),
            source.to_string(),
        ))
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_debug(&self) -> Vec<String> {
        self.opens().slots.iter().flatten().filter_map(|slot| match &slot.phase {
            OpenPhase::Flow(flow) => Some(format!(
                "stage={:?} stop={} reason={:?} joins={:?} q={:?} settled={:?} d16={:?} provider={:?} steps={} published={:?} hints={:?}",
                flow.stage, flow.producer_stopped, flow.close_reason,
                flow.roles.as_ref().map(FlowRoles::join_count),
                flow.roles.as_ref().map(FlowRoles::quarantined),
                flow.roles.as_ref().map(FlowRoles::settled),
                flow.roles.as_ref().and_then(FlowRoles::d16_queue).map(|queue| queue.snapshot_now()),
                flow.attempt.as_ref().map(|a| a.view()), flow.open_steps,
                flow.roles.as_ref().and_then(FlowRoles::published),
                flow.roles.as_ref().and_then(FlowRoles::published_hints),
            )),
            _ => None,
        }).collect()
    }

    // Progress polling reads the original counter; boundary audits stay complete.
    pub(in crate::client_tun::compatibility_runtime) fn published_flow_count(&self) -> Option<u64> {
        Some(self.opens().flow.as_ref()?.published)
    }

    pub(in crate::client_tun::compatibility_runtime) fn flow_observation(
        &self,
    ) -> Option<FlowObservation> {
        let opens = self.opens();
        let accounting = opens.flow.as_ref()?;
        let (mut running, mut retained_capacity, mut joined) = (0, 0, accounting.joined);
        for slot in opens.slots.iter().flatten() {
            if let OpenPhase::Flow(flow) = &slot.phase {
                retained_capacity += flow.escrow.capacity();
                running += flow.roles.as_ref().map_or(0, FlowRoles::running_count);
                joined += flow.roles.as_ref().map_or(0, FlowRoles::join_count) as u64;
            }
        }
        Some(FlowObservation {
            factory_started: accounting.factory_started,
            last_fake_release: accounting.last_fake_release,
            live: opens.live,
            retained_identity_faults: usize::from(accounting.identity_fault.is_some()),
            w: accounting.w,
            f: accounting.f,
            reserved: accounting.bytes,
            retained_capacity,
            escrow_bytes: accounting.escrow_bytes,
            attempts: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.is_some())).count(),
            opening_providers: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.as_ref()
                    .is_some_and(|a| a.view().phase == TcpOpenPhase::Opening))).count(),
            waiting: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.stage == FlowStage::Waiting
                    && flow.attempt.is_none() && !flow.f_held && flow.w_held)).count(),
            retry: accounting.retry,
            running,
            stopped_producers: opens
                .slots
                .iter()
                .flatten()
                .filter(
                    |slot| matches!(&slot.phase, OpenPhase::Flow(flow) if flow.producer_stopped),
                )
                .count(),
            settled_providers: opens.slots.iter().flatten().filter(|slot|
                matches!(&slot.phase, OpenPhase::Flow(flow) if flow.attempt.as_ref()
                    .is_some_and(|a| {
                        let view = a.view();
                        view.phase == TcpOpenPhase::Settled && !view.lease_held && !view.retirement_pending
                    }))).count(),
            published: accounting.published,
            released: accounting.released,
            joined,
            audit: opens.audit_flows(),
            last_data_disposition: accounting.last_data_disposition,
            #[cfg(feature = "harness")]
            last_permit_reset: accounting.last_permit_reset,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::client_tun::compatibility_runtime) struct FlowContract {
    pub(in crate::client_tun::compatibility_runtime) id: FlowIdentity,
    pub(in crate::client_tun::compatibility_runtime) slot_deadline: tokio::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) open_deadline: std::time::Instant,
    pub(in crate::client_tun::compatibility_runtime) active: bool,
    pub(in crate::client_tun::compatibility_runtime) outstanding: Option<usize>,
    pub(in crate::client_tun::compatibility_runtime) roles_joined: Option<usize>,
    pub(in crate::client_tun::compatibility_runtime) waiting: bool,
    pub(in crate::client_tun::compatibility_runtime) published: bool,
    pub(in crate::client_tun::compatibility_runtime) attempt: bool,
    pub(in crate::client_tun::compatibility_runtime) w: bool,
    pub(in crate::client_tun::compatibility_runtime) f: bool,
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_contracts(&self) -> Vec<FlowContract> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .filter_map(|slot| {
                let OpenPhase::Flow(flow) = &slot.phase else {
                    return None;
                };
                Some(FlowContract {
                    id: flow.id,
                    slot_deadline: slot.deadline,
                    open_deadline: flow.open_deadline,
                    active: flow.stage == FlowStage::Published,
                    outstanding: flow
                        .roles
                        .as_ref()
                        .and_then(FlowRoles::test_outstanding_data),
                    roles_joined: flow.roles.as_ref().map(FlowRoles::join_count),
                    waiting: flow.stage == FlowStage::Waiting,
                    published: flow.published,
                    attempt: flow.attempt.is_some(),
                    w: flow.w_held,
                    f: flow.f_held,
                })
            })
            .collect()
    }
}

impl SessionControl {
    /// Original Flow -> original Roles -> original fixed Shared terminal. This
    /// Copy observation does not replace Root closure or dispatcher delivery.
    pub(in crate::client_tun::compatibility_runtime) fn flow_native_retained_terminal_for_test(
        &self,
        id: FlowIdentity,
    ) -> Option<(&'static str, &'static str)> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .find_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.id == id => {
                    flow.roles.as_ref()?.retained_native_terminal_for_test()
                }
                _ => None,
            })
    }
}

impl SessionControl {
    /// Copy the same Flow's original stop clock, phase and actual close reason.
    pub(in crate::client_tun::compatibility_runtime) fn flow_original_stop_for_test(
        &self,
        id: FlowIdentity,
    ) -> Option<(
        Option<tokio::time::Instant>,
        bool,
        crate::client_tun::RelayCloseReason,
    )> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .find_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.id == id => Some((
                    flow.stop_deadline.map(tokio::time::Instant::from_std),
                    flow.stage == FlowStage::Quarantined,
                    flow.close_reason,
                )),
                _ => None,
            })
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_stock_key_for_test(&self, id: FlowIdentity) -> Option<crate::tcp_downlink_pump::StockKey> {
        self.opens().flow_mut(id)?.roles.as_ref()?.native_stock_key().cloned()
    }
    pub(in crate::client_tun::compatibility_runtime) fn flow_roles_data_disposed_for_test(&self, id: FlowIdentity) -> Option<bool> {
        self.opens().flow_mut(id).and_then(|flow| flow.roles.as_ref().map(FlowRoles::data_disposed))
    }
    /// Observe actual original role receipts without retaining the queue's cold
    /// allocation. Only this exact Flow may authorize dropping a test observer.
    pub(in crate::client_tun::compatibility_runtime) fn flow_roles_settled_for_test(
        &self,
        id: FlowIdentity,
    ) -> Option<bool> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .find_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.id == id => Some(flow.roles.as_ref()?.settled()),
                _ => None,
            })
    }
}

#[cfg(feature = "harness")]
mod permit_receipt;

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_retained_permits_for_test(
        &self,
        id: FlowIdentity,
        identity: &crate::tcp_downlink_pump::PermitQueueIdentity,
    ) -> Option<(usize, usize, usize)> {
        self.opens()
            .slots
            .iter()
            .flatten()
            .find_map(|slot| match &slot.phase {
                OpenPhase::Flow(flow) if flow.id == id => {
                    let (handle, epoch, held, segment, published) = flow
                        .roles
                        .as_ref()?
                        .retained_native_permits_for_test(identity)?;
                    assert_eq!((handle, epoch), (id.handle, id.epoch));
                    Some((held, segment, published))
                }
                _ => None,
            })
    }
}

impl SessionControl {
    pub(in crate::client_tun::compatibility_runtime) fn flow_native_retirement_for_test(&self, id: FlowIdentity) -> Option<String> {
        self.opens().flow_mut(id)?.roles.as_ref()?.native_retirement_for_test()
    }
}
