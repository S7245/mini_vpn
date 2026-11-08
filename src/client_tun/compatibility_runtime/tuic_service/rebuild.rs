//! TUIC/Failover controlled rebuild at the saved service authority. Owns one device,
//! namespace, sink, current session and at most one retired diagnostic shell.
//! Explicit request only; no automatic fault retry or node selection. DATA stays
//! in the original loop. Tests: tests/rebuild.rs, lifecycle.rs and faults.rs.
use super::*;
use crate::client_tun::legacy_event_loop::namespace::Namespace;

pub(super) struct Retired {
    pub(super) node: NodeHost,
    pub(super) control: Arc<SessionControl>,
    receipt: Option<crate::device::DeviceCutReceipt>,
    _health: Option<HealthTask>,
}

pub(super) async fn drive<D: TunIo, M: MetricsSink, C: Composition, F, Fut>(
    factory: F,
    preflight: Option<ClientError>,
    config: Configuration<C>,
    mut runtime: TunRuntimeConfig,
    metrics: Arc<Metrics>,
    mut sink: M,
    saved: Arc<Control>,
) -> Completion
where
    F: FnOnce(Option<bytes::BackingMeter>) -> Fut,
    Fut: Future<Output = Result<D, ClientError>>,
{
    // The first owner must be composed from the same selected profile as its
    // business loop. Retain known validation failure until actual factory/D
    // custody exists so their original error and disposal order is preserved.
    let validation = select_runtime(&mut runtime).and(preflight.map_or(Ok(()), Err)).and_then(|()| {
        use crate::upstream::TuicNativeOverride;
        if runtime.native_limits.is_some()
            && (runtime.relay_policy().d16_byte_owned()
                || !matches!(config.tuic.relay_policy().tuic_native_override(),
                    TuicNativeOverride::NativeChunkPump | TuicNativeOverride::NativeOrderedPump))
        {
            return Err(ClientError::InvalidTarget(
                "hard Native Service requires a qualified Native TCP carrier".into(),
            ));
        }
        Ok(())
    });
    let initial_owner = business::control(&runtime, saved.app_meter.clone());
    let _ = initial_owner.parent_deadline.set(saved.deadline.clone());
    #[cfg(test)]
    {
        *saved
            .session
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            Some(Arc::downgrade(&initial_owner));
    }
    let mut io_rejected = if validation.is_ok() {
        record_io_admission(egress::deadline::prepare(&initial_owner), &initial_owner, &saved).await
    } else { false };
    let mut device = match factory(saved.app_meter.clone()).await {
        Ok(device) => device,
        Err(error) => {
            record_client(error, &initial_owner, &saved).await;
            saved.stop_session(&initial_owner);
            egress::deadline::retire(&initial_owner);
            // Factory contract: no device or actor was created. The original
            // cold owner records the failure; no fabricated shutdown receipt.
            initial_owner
                .business_complete
                .store(true, Ordering::Release);
            initial_owner.driver_complete.store(true, Ordering::Release);
            return Completion {
                _node: None,
                _retired: None,
                _health: None,
                control: initial_owner,
                report: Report {
                    session: SessionDeviceState::DeviceFailedSettled,
                    node: None,
                    rebuilds: 0,
                    discarded_packets: 0,
                    discarded_backing: 0,
                },
            };
        }
    };
    let mut admission_error = validation.err();
    if admission_error.is_none() && !io_rejected {
        match catch_unwind(AssertUnwindSafe(|| {
            let binding = match (runtime.native_limits, saved.app_meter.as_ref()) {
                (None, None) => crate::device::DeviceBinding::Legacy,
                (Some(limits), Some(meter)) => crate::device::DeviceBinding::Native {
                    meter,
                    slots: limits.device_slots.get(),
                    mtu: runtime.tun_mtu,
                },
                _ => return Err(std::io::Error::from(std::io::ErrorKind::InvalidInput)),
            };
            device.bind_backing_meter(binding)
        })) {
            Ok(result) => admission_error = result.err().map(ClientError::Io),
            Err(payload) => {
                // Keep actual D in this original future. In particular, do not
                // unwind through D to reach the outer saved-future guard.
                initial_owner.ledger().quarantined = true;
                initial_owner.record(RetainedFailure::opaque_panic(SessionControl::origin(
                    FailurePhase::DeviceRead,
                )));
                saved.terminal.retain(saved::Site::Bind, payload);
                saved::quarantine(&saved);
                std::future::pending::<()>().await;
            }
        }
    }

    let mut initial_owner = Some(initial_owner);
    let mut namespace = Namespace::new();
    let mut retired: Option<Retired> = None;
    let mut rebuilds = 0;
    let mut discarded = (0u64, 0u64);
    loop {
        let successor = initial_owner.is_none();
        let owner = initial_owner
            .take()
            .unwrap_or_else(|| business::control(&runtime, saved.app_meter.clone()));
        let _ = owner.parent_deadline.set(saved.deadline.clone());
        if successor {
            io_rejected = record_io_admission(egress::deadline::prepare(&owner), &owner, &saved).await;
        }
        if accept_ingress(
            business::ingress::take(&mut device, &owner).await,
            &owner,
            &saved,
        )
        .await
        {
            saved.stop_session(&owner);
        }
        let mut cut = None;
        let mut node = if admission_error.is_none() && !io_rejected && !saved.stop.stopping.load(Ordering::Acquire)
        {
            let host = NodeHost::prepare_with_budget(
                config.tuic.clone(),
                metrics.clone(),
                saved.backing_budget.as_ref(),
            );
            let sink: Arc<dyn crate::shared::errors::QuietFailureSink> = owner.clone();
            if let Err(error) = host.bind_udp_parent(Arc::downgrade(&sink)) {
                record_client(error, &owner, &saved).await;
            }
            drop(sink);
            Some(host)
        } else {
            None
        };
        #[cfg(test)]
        if rebuilds == 0 {
            *saved
                .old_retirement_probe
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                node.as_ref().map(NodeHost::retirement_probe_for_test);
            let observation = node.as_ref().map(NodeHost::native_weak_observer_for_test);
            let retired = {
                let mut slot = saved.native_node_sample.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                std::mem::replace(&mut *slot, observation)
            };
            drop(retired);
        }
        let mut ingress = None;
        let mut health = None;
        if let Some(host) = &mut node {
            let started = tokio::select! {
                biased;
                _ = saved.stop.stopped() => NodeStartup::Stopping,
                observation = business::ingress::wait(&mut device, &owner) => {
                    accept_ingress(observation, &owner, &saved).await;
                    saved.stop_session(&owner);
                    NodeStartup::Stopping
                },
                state = host.start() => state,
            };
            if owner.ledger().quarantined {
                saved.stop_session(&owner);
                saved.publish(Phase::Quarantined);
                std::future::pending::<()>().await;
            }
            if accept_ingress(
                business::ingress::take(&mut device, &owner).await,
                &owner,
                &saved,
            )
            .await
            {
                saved.stop_session(&owner);
            }
            if started == NodeStartup::Ready && !saved.stop.stopping.load(Ordering::Acquire) {
                #[cfg(test)]
                if saved.abort_stats.load(Ordering::Acquire) {
                    host.cancel_original_stats_for_service_test();
                }
                match host
                    .data()
                    .and_then(|data| data.start_udp_source().map(|source| (data, source)))
                {
                    Ok((data, source)) => {
                        #[cfg(test)]
                        {
                            *saved
                                .udp_generation
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                                source.generation_for_test();
                        }
                        let (upstream, task) = config.composition.compose(data);
                        health = task;
                        ingress = Some(SessionIngress {
                            upstream: Arc::new(LegacyOwnedUpstream::new(upstream)),
                            downlink: source,
                            health_join: health.as_ref().map(HealthTask::join_observation),
                        });
                    }
                    Err(error) => admission_error = Some(error),
                }
            }
        }
        let has_ingress = ingress.is_some();
        if has_ingress && !saved.stop.stopping.load(Ordering::Acquire) {
            if let Some(previous) = &mut retired {
                if let Some(receipt) = &mut previous.receipt {
                    match catch_unwind(AssertUnwindSafe(|| device.resume_ingress(receipt))) {
                        Ok(Ok(())) => {
                            discarded.0 = discarded.0.saturating_add(receipt.discarded_packets);
                            discarded.1 = discarded.1.saturating_add(receipt.discarded_backing);
                            previous.receipt = None;
                        }
                        Ok(Err(error)) => admission_error = Some(ClientError::Io(error)),
                        Err(payload) => {
                            let custody = previous
                                .control
                                .quarantine(FailurePhase::DeviceShutdown, payload);
                            saved.publish(Phase::Quarantined);
                            std::future::pending::<()>().await;
                            drop(custody);
                        }
                    }
                }
            }
        }
        #[cfg(test)]
        {
            if let Some(slot) = saved
                .namespace_clocks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get_mut(usize::from(rebuilds))
            {
                *slot = Some(namespace.clock);
            }
        }

        #[cfg(test)]
        {
            *saved
                .session
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::downgrade(&owner));
            *saved
                .health_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                health.as_ref().map(HealthTask::state_for_test);
            *saved
                .health_observation
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) =
                health.as_ref().map(HealthTask::observation_for_test);
            if saved.abort_health.load(Ordering::Acquire) {
                if let Some(task) = &health {
                    task.cancel_original_for_service_test();
                }
            }
        }
        if accept_ingress(
            business::ingress::take(&mut device, &owner).await,
            &owner,
            &saved,
        )
        .await
        {
            saved.stop_session(&owner);
        }
        if let Some(error) = admission_error.take() {
            record_client(error, &owner, &saved).await;
            saved.stop_session(&owner);
        }
        if !has_ingress {
            saved.stop_session(&owner);
        }
        let mut retiring = false;
        let mut stopping = false;
        let mut node_failed = !has_ingress;
        let mut health_exit = None;
        if !saved.stop.stopping.load(Ordering::Acquire) {
            saved.publish(Phase::Running);
        }
        let mut running = Box::pin(business::run_loop(
            &mut device,
            ingress,
            runtime.clone(),
            metrics.clone(),
            &mut sink,
            &mut namespace,
            owner.clone(),
        ));
        let retirement = loop {
            if !stopping && saved.stop.stopping.load(Ordering::Acquire) {
                stopping = true;
                saved.stop_session(&owner);
                if let Some(task) = &health {
                    task.request_stop();
                }
                saved.publish(Phase::Stopping);
            }
            tokio::select! {
                biased;
                _ = saved.stop.stopped(), if !stopping => {},
                _ = saved.rebuild.stopped(), if rebuilds == 0 && !retiring && !stopping => {
                    retiring = true; owner.request_stop();
                    if let Some(task) = &health { task.request_stop(); }
                    saved.publish(Phase::Rebuilding);
                },
                _ = async { if let Some(host) = &mut node {host.run().await} else {std::future::pending().await} }, if !node_failed => {
                    node_failed = true; saved.stop_session(&owner);
                },
                exit = async { if let Some(task) = &mut health {task.join().await} else {std::future::pending().await} }, if health_exit.is_none() && health.is_some() => {
                    health_exit = Some(exit); observe_health(exit, &owner, &saved, retiring);
                },
                tail = std::future::poll_fn(|cx| {
                    let result = running.as_mut().poll(cx);
                    let state = owner.state();
                    if state == SessionDeviceState::Incomplete { saved.publish(Phase::Quarantined); }
                    if state != SessionDeviceState::Running && !retiring { saved.stop_session(&owner); }
                    result
                }) => break tail,
            }
        };
        // Only the DATA borrow returned. The original source, receipts and
        // custody now live in this one pinned tail; no business proof yet.
        drop(running);
        if !retiring {
            saved.stop_session(&owner);
        }
        if let Some(task) = &health {
            task.request_stop();
        }
        let mut tail = Box::pin(retirement.settle());
        let mut business_done = false;
        let mut device_join = None;
        let mut physical = Box::pin(async {
            saved.stop.stopped().await;
            business::shutdown_device(&mut device, &owner).await
        });
        loop {
            if !stopping && saved.stop.stopping.load(Ordering::Acquire) {
                stopping = true;
                saved.stop_session(&owner);
                saved.publish(Phase::Stopping);
            }
            if business_done
                && (health.is_none() || health_exit.is_some())
                && (!saved.stop.stopping.load(Ordering::Acquire) || device_join.is_some())
            {
                break;
            }
            tokio::select! {
                biased;
                _ = saved.stop.stopped(), if !stopping => {},
                _ = async { if let Some(host) = &mut node {host.run().await} else {std::future::pending().await} }, if !node_failed => {
                    node_failed = true; saved.stop_session(&owner);
                },
                exit = async { if let Some(task) = &mut health {task.join().await} else {std::future::pending().await} }, if health_exit.is_none() && health.is_some() => {
                    health_exit = Some(exit); observe_health(exit, &owner, &saved, retiring);
                },
                custody = &mut physical, if device_join.is_none() && (health.is_some() || business_done) => { device_join = Some(custody); },
                () = &mut tail, if !business_done => {
                    business_done = true;
                    #[cfg(test)]
                    println!("B818 ServiceStage session={} stage=business_settled now={:?}", Arc::as_ptr(&owner) as usize, tokio::time::Instant::now());
                    owner.business_complete.store(true, Ordering::Release);
                    owner.state_changed.notify_waiters();
                    if !retiring { saved.stop_session(&owner); }
                },
            }
            if owner.ledger().quarantined {
                saved.stop_session(&owner);
                saved.publish(Phase::Quarantined);
            }
        }
        drop(tail);
        drop(physical);
        let business_state = owner.state();
        let deadline = owner.ledger().shutdown_deadline;
        let timely = deadline.is_some_and(|limit| tokio::time::Instant::now() < limit);
        if retiring && !timely {
            if let Some(limit) = deadline {
                saved.expire_at(limit);
            }
            owner.record(RetainedFailure::from_io(
                SessionControl::origin(FailurePhase::SessionLoop),
                std::io::Error::from(std::io::ErrorKind::TimedOut),
            ));
        }
        let can_retire = retiring
            && !node_failed
            && rebuilds == 0
            && !saved.stop.stopping.load(Ordering::Acquire)
            && business_state == SessionDeviceState::BusinessSettled
            && !owner.ledger().quarantined
            && namespace.fake_pool.usage().1 == 0
            && timely;
        if can_retire {
            #[cfg(test)]
            println!(
                "B818 ServiceStage session={} stage=cut_begin now={:?}",
                Arc::as_ptr(&owner) as usize,
                tokio::time::Instant::now()
            );
            cut = cut_device(&mut device, &owner, &saved, deadline).await;
            #[cfg(test)]
            println!(
                "B818 ServiceStage session={} stage=cut_end receipt={} now={:?}",
                Arc::as_ptr(&owner) as usize,
                cut.is_some(),
                tokio::time::Instant::now()
            );
            if cut.is_some() && !saved.stop.stopping.load(Ordering::Acquire) {
                if let Some(host) = &mut node {
                    #[cfg(test)]
                    let c4 = saved.rebuild_node_fault.load(Ordering::Acquire);
                    #[cfg(test)]
                    if c4 == NodeHost::C4_HOLD_DATA {
                        let data = host.c4_hold_data_for_service_test();
                        *saved.held_node_data.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(data);
                    }
                    match host.begin_controlled_rebuild() {
                        Ok(()) => {
                            #[cfg(test)]
                            if saved.abort_stats_at_rebuild.load(Ordering::Acquire) {
                                host.cancel_original_stats_for_service_test();
                            }
                            #[cfg(test)]
                            if c4 == NodeHost::C4_KNOWN_FAILURE {
                                host.record_known_failure_for_probe_test();
                            }
                            #[cfg(test)]
                            println!(
                                "B818 ServiceStage session={} stage=node_drain_begin now={:?}",
                                Arc::as_ptr(&owner) as usize,
                                tokio::time::Instant::now()
                            );
                            let limit = host.controlled_rebuild_deadline();
                            let (state, expiry_recorded) =
                                bounded_drain(host.drive_shutdown(), limit, &owner, &saved).await;
                            #[cfg(test)]
                            println!(
                                "B818 ServiceStage session={} stage=node_drain_end state={state:?} now={:?}",
                                Arc::as_ptr(&owner) as usize,
                                tokio::time::Instant::now()
                            );
                            #[cfg(test)]
                            host.c4_after_drain_for_service_test(c4);
                            if matches!(state, NodeShutdown::Incomplete | NodeShutdown::Quarantined)
                            {
                                #[cfg(test)]
                                if saved.abort_stats_at_rebuild.load(Ordering::Acquire) {
                                    host.assert_service_fault_settled_for_test();
                                    saved.node_fault_settled.store(true, Ordering::Release);
                                }
                                saved.publish(Phase::Quarantined);
                                std::future::pending::<()>().await;
                            }
                            let disposition = host.controlled_rebuild_disposition();
                            #[cfg(test)]
                            if c4 == NodeHost::C4_UNJOINED_TASK && disposition.is_quarantined() {
                                host.assert_c4_custody_contradiction_for_test();
                                saved.node_fault_settled.store(true, Ordering::Release);
                            }
                            if disposition.is_quarantined() {
                                saved.publish(Phase::Quarantined);
                                std::future::pending::<()>().await;
                            }
                            // One expiry is one TimedOut record (D107 F3).
                            if disposition.is_expired() && !expiry_recorded {
                                if let Some(limit) = host.controlled_rebuild_deadline() {
                                    saved.expire_at(limit);
                                }
                                owner.record(RetainedFailure::from_io(
                                    SessionControl::origin(FailurePhase::SessionLoop),
                                    std::io::Error::from(std::io::ErrorKind::TimedOut),
                                ));
                            }
                            if disposition.is_allowed()
                                && !saved.stop.stopping.load(Ordering::Acquire)
                            {
                                if let Some(old) = node.take() {
                                    #[cfg(test)]
                                    let retired_session_id = Arc::as_ptr(&owner) as usize;
                                    retired = Some(Retired {
                                        node: old,
                                        control: owner,
                                        receipt: cut.take(),
                                        _health: health,
                                    });
                                    rebuilds = 1;
                                    #[cfg(test)]
                                    println!(
                                        "B818 ServiceStage session={retired_session_id} stage=successor_start now={:?}",
                                        tokio::time::Instant::now()
                                    );
                                    saved.publish(Phase::Starting);
                                    continue;
                                }
                            }
                        }
                        Err(error) => record_client(error, &owner, &saved).await,
                    }
                }
            }
        }
        saved.stop_session(&owner);
        saved.publish(Phase::Stopping);
        // Final service stop really stops/joins/drops the device before Node.
        // A cut receipt held through this path is not consumed as a resume.
        let mut physical = Box::pin(async {
            if device_join.is_none() {
                device_join = Some(business::shutdown_device(&mut device, &owner).await);
            }
            business::drop_device(device, &owner).await;
        });
        std::future::poll_fn(|cx| {
            let result = physical.as_mut().poll(cx);
            if owner.ledger().quarantined {
                saved.stop_session(&owner);
                saved.publish(Phase::Quarantined);
            }
            result
        })
        .await;
        drop(physical);
        drop(device_join);
        // Actual original device poll_shutdown AND guarded Drop are now complete.
        owner.driver_complete.store(true, Ordering::Release);
        owner.state_changed.notify_waiters();
        if health_exit == Some(HealthExit::Quarantined) {
            saved.publish(Phase::Quarantined);
            std::future::pending::<()>().await;
        }
        let node_state = if let Some(host) = &mut node {
            let state = host.drive_shutdown().await;
            if matches!(state, NodeShutdown::Incomplete | NodeShutdown::Quarantined) {
                #[cfg(test)]
                if saved.abort_stats.load(Ordering::Acquire) {
                    host.assert_service_fault_settled_for_test();
                    saved.node_fault_settled.store(true, Ordering::Release);
                }
                saved.publish(Phase::Quarantined);
                std::future::pending::<()>().await;
            }
            Some(state)
        } else {
            None
        };
        let failed = owner.ledger().first_failure.is_some();
        for receipt in cut
            .as_ref()
            .into_iter()
            .chain(retired.as_ref().and_then(|old| old.receipt.as_ref()))
        {
            discarded.0 = discarded.0.saturating_add(receipt.discarded_packets);
            discarded.1 = discarded.1.saturating_add(receipt.discarded_backing);
        }
        drop(cut);
        return Completion {
            _node: node,
            _health: health,
            _retired: retired,
            control: owner,
            report: Report {
                session: if failed {
                    SessionDeviceState::DeviceFailedSettled
                } else {
                    SessionDeviceState::DeviceSettled
                },
                node: node_state,
                rebuilds,
                discarded_packets: discarded.0,
                discarded_backing: discarded.1,
            },
        };
    }
}

/// O-C4b (D105): the Service verdict on a stalled controlled old-Node drain is
/// bounded by the Node's own rebuild deadline. Expiry stops the service (no
/// successor), records TimedOut and publishes Incomplete; the same saved drain
/// stays owned and driven to its real settlement, never dropped or aborted.
/// Returns whether this verdict already recorded the expiry (D107 F3).
async fn bounded_drain<F: Future<Output = NodeShutdown>>(
    drain: F,
    limit: Option<tokio::time::Instant>,
    owner: &SessionControl,
    saved: &Control,
) -> (NodeShutdown, bool) {
    let mut drain = std::pin::pin!(drain);
    if let Some(limit) = limit {
        tokio::select! {
            biased;
            state = &mut drain => return (state, false),
            () = tokio::time::sleep_until(limit) => {}
        }
        saved.expire_at(limit);
        owner.record(RetainedFailure::from_io(
            SessionControl::origin(FailurePhase::SessionLoop),
            std::io::Error::from(std::io::ErrorKind::TimedOut),
        ));
        saved.publish(Phase::Incomplete);
        return (drain.await, true);
    }
    (drain.await, false)
}

fn observe_health(exit: HealthExit, owner: &SessionControl, saved: &Control, retiring: bool) {
    if exit == HealthExit::Quarantined {
        owner.record(RetainedFailure::from_io(
            SessionControl::origin(FailurePhase::SessionLoop),
            std::io::Error::other("original health join failure retained"),
        ));
        saved.publish(Phase::Quarantined);
        saved.stop_session(owner);
    } else if !retiring {
        saved.stop_session(owner);
    }
}

async fn cut_device<D: TunIo>(
    device: &mut D,
    owner: &Arc<SessionControl>,
    saved: &Control,
    deadline: Option<tokio::time::Instant>,
) -> Option<crate::device::DeviceCutReceipt> {
    let mut work = Box::pin(device.cut_ingress());
    let result = tokio::select! {
        biased;
        _ = saved.stop.stopped() => None,
        _ = async { if let Some(deadline) = deadline {tokio::time::sleep_until(deadline).await} else {std::future::pending().await} } => {
            owner.record(RetainedFailure::from_io(SessionControl::origin(FailurePhase::DeviceShutdown),std::io::Error::from(std::io::ErrorKind::TimedOut)));
            if let Some(limit) = deadline { saved.expire_at(limit); } else { saved.request_stop(); } None
        },
        result = std::future::poll_fn(|cx| match catch_unwind(AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(result)) => Poll::Ready(Ok(result)),
            Err(payload) => Poll::Ready(Err(payload)),
        }) => Some(result),
    };
    // Preserve the returned original cause before notifications or any unknown
    // future/error destructor. The original error itself stays in result custody.
    if let Some(Ok(Err(error))) = &result {
        owner.record(RetainedFailure::from_io_ref(
            SessionControl::origin(FailurePhase::DeviceShutdown),
            error,
        ));
    }
    // Every non-success adopts the original business limit before releasing the
    // original cut future/error. A successful successor never inherits it.
    if !matches!(&result, Some(Ok(Ok(_)))) {
        if let Some(limit) = deadline {
            saved.expire_at(limit);
        }
    }
    if let Some(Err(payload)) = result {
        let custody = owner.quarantine(FailurePhase::DeviceShutdown, payload);
        saved.publish(Phase::Quarantined);
        std::future::pending::<()>().await;
        drop(custody);
        return None;
    }
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(work))) {
        let custody = owner.quarantine(FailurePhase::DeviceShutdown, payload);
        saved.publish(Phase::Quarantined);
        std::future::pending::<()>().await;
        drop(custody);
        return None;
    }
    match result {
        Some(Ok(Ok(receipt))) => Some(receipt),
        Some(Ok(Err(error))) => {
            if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
                let custody = owner.quarantine(FailurePhase::DeviceShutdown, payload);
                saved.publish(Phase::Quarantined);
                std::future::pending::<()>().await;
                drop(custody);
            }
            None
        }
        _ => None,
    }
}

async fn accept_ingress(
    observation: business::ingress::Observation,
    owner: &SessionControl,
    saved: &Control,
) -> bool {
    if observation.unknown() {
        saved.stop_session(owner);
        saved.publish(Phase::Quarantined);
        // Keep the actual borrowed Device and original payload custody in this
        // saved driver, after the sole root committed its original deadline.
        std::future::pending::<()>().await;
    }
    observation.reported
}

async fn record_io_admission(
    result: Result<(), bytes::MeteredCopyError>, owner: &SessionControl, saved: &Control,
) -> bool {
    let Err(error) = result else { return false; };
    // Record before a factory which may already own D; actual D and independent
    // factory errors still take their original cleanup/recording path.
    record_client(ClientError::Io(egress::deadline::admission_io(error)), owner, saved).await;
    saved.stop_session(owner);
    true
}

async fn record_client(error: ClientError, owner: &SessionControl, saved: &Control) {
    // One startup classification boundary for cold preparation, actual Device
    // factory/bind and Node. Only this exact typed rejection is not source IO.
    let refused_q = matches!(&error, ClientError::Io(io) if io.get_ref()
        .and_then(|source| source.downcast_ref::<quinn::owned_events::AdmissionError>())
        .is_some_and(|cause| cause.0 == bytes::MeteredCopyError::Quarantined));
    if refused_q {
        owner.quarantine_quiet();
        saved.stop_session(owner);
        saved.publish(Phase::Quarantined);
        return;
    }
    owner.record(RetainedFailure::from_client_ref(
        SessionControl::origin(FailurePhase::SessionLoop),
        &error,
    ));
    if let Err(payload) = catch_unwind(AssertUnwindSafe(|| drop(error))) {
        let custody = owner.quarantine(FailurePhase::SessionLoopDrop, payload);
        saved.publish(Phase::Quarantined);
        std::future::pending::<()>().await;
        drop(custody);
    }
}

#[cfg(all(test, feature = "harness"))]
#[path = "tests/cut_deadline.rs"]
mod tests;
