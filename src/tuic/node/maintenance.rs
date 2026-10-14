//! TUIC node pool maintenance implementation.
//! Owns the existing selection/replacement call chain; slot state stays in tuic.
//! Behavior is preserved during movement; lifecycle consolidation follows here.
//! Characterization tests live in tuic/tests and tcp_open/tests.
use crate::tuic::*;

impl TuicUpstream {
    fn check_node_admission(&self) -> Result<(), ClientError> {
        if self.udp_runtime.node_stopping() {
            Err(io_err("tuic node", "stopping"))
        } else {
            Ok(())
        }
    }
    fn reject_unowned_maintenance(&self) -> Result<(), ClientError> {
        if self.udp_runtime.node_owned() {
            Err(io_err(
                "tuic node",
                "saved recovery executor not yet enabled",
            ))
        } else {
            Ok(())
        }
    }
    /// 取当前主连接的克隆；若已关闭则就地重连+重认证（13a 逻辑，UDP/health 共用）。
    /// 中文要点：主连接(index 0)是 UDP/health 的单一事实源；TCP pool 通过 `live_tcp_conn` 分散到其它连接。
    /// 刀9（spec §2.3）：重连握手封 **5s 超时**——黑洞/不可达 server 的 QUIC 握手默认受 idle(15s, quic.rs) 约束、
    /// 可阻塞数十秒；5s 封顶让「连接死 + 重连失败」这个 failover 快路强信号**快速暴露**（is_dead 仍为
    /// true，调用方 record_tuic_failure(dead) 即切 REALITY），同时也避免纯 TUIC open 任务久挂。
    pub(in crate::tuic) async fn live_conn(&self) -> Result<Connection, ClientError> {
        self.live_conn_at(0).await
    }

    /// 取一条 TCP pool 连接的克隆；连接自身已关闭则只重连该槽位。
    async fn live_conn_at(&self, index: usize) -> Result<Connection, ClientError> {
        self.live_conn_at_with_reason(index, None).await
    }

    async fn live_conn_at_with_reason(
        &self,
        index: usize,
        reconnect_reason: Option<&'static str>,
    ) -> Result<Connection, ClientError> {
        loop {
            self.check_node_admission()?;
            let slot = self.tcp_pool_slots.get(index).ok_or_else(|| {
                ClientError::InvalidTarget("tuic tcp pool index out of range".into())
            })?;
            let mut state = slot.state.lock().await;
            if self.udp_runtime.node_owned() && state.current.transport.close_reason().is_some() {
                let ticket = self.request_aux_recovery(index, state)?;
                self.await_aux_recovery(ticket).await?;
                continue;
            }
            let current = &mut state.current;
            let closed = current.transport.close_reason().is_some();
            if closed || reconnect_reason.is_some() {
                let reason = reconnect_reason.unwrap_or("transport_closed");
                self.reconnect_locked(index, &mut current.transport, reason)
                    .await?;
                current.note_reconnect_success();
            }
            return Ok(current.transport.clone());
        }
    }

    pub(in crate::tuic) async fn reconnect_locked(
        &self,
        index: usize,
        conn: &mut Connection,
        reason: &'static str,
    ) -> Result<(), ClientError> {
        self.reject_unowned_maintenance()?;
        println!(
            "🔁 tuic-tcp-pool-reconnect conn={index} id={} reason={reason}",
            conn.stable_id()
        );
        conn.close(VarInt::from_u32(0), reason.as_bytes());
        let hs = Self::handshake(
            &self.endpoint,
            self.server,
            &self.sni,
            &self.uuid,
            &self.password,
            self.zero_rtt,
        );
        *conn = tokio::time::timeout(TUIC_RECONNECT_TIMEOUT, hs)
            .await
            .map_err(|_| {
                io_err(
                    "tuic reconnect",
                    "5s 超时（黑洞/不可达；failover 据 is_dead 切备腿）",
                )
            })??;
        if let Some(secs) = self.quic_stats_secs
            && let Some(stop) = &self.quic_stats_stop
        {
            spawn_quic_stats_logger(conn.clone(), index, secs, stop.subscribe());
        }
        Ok(())
    }

    pub(in crate::tuic) fn sample_tcp_pool_observations(&self) -> Vec<TcpPoolPathObservation> {
        self.tcp_pool_slots
            .iter()
            .map(|slot| {
                let Ok(state) = slot.state.try_lock() else {
                    return TcpPoolPathObservation::Unknown;
                };
                let current = &state.current;
                let conn = &current.transport;
                if conn.close_reason().is_some() {
                    return TcpPoolPathObservation::Unknown;
                }
                let path = conn.stats().path;
                TcpPoolPathObservation::Known {
                    identity: current.identity(),
                    path_service: TcpPoolPathService::known(path.cwnd, path.rtt),
                    black_holes_detected: path.black_holes_detected,
                    path_generation: Some(conn.current_path_generation()),
                    successor_service_certificate: current.successor_service_certificate,
                }
            })
            .collect()
    }

    pub(in crate::tuic) fn tcp_pool_replacement_allowed(&self) -> Vec<bool> {
        if self.tcp_pool_draining_predecessor.load(Ordering::Acquire) {
            return vec![false; self.tcp_pool_slots.len()];
        }
        self.tcp_pool_slots
            .iter()
            .map(|slot| slot.replacement_available())
            .collect()
    }

    async fn acquire_tcp_pool_reservation(
        &self,
    ) -> Result<(TcpPoolSlotReservation, bool), ClientError> {
        // A failed maintenance replacement must not fail the business open when another
        // qualified current generation remains. Keep the exclusion local to this open so a
        // later independent open retains fresh replacement authority.
        let mut failed_replacement_slots = vec![false; self.tcp_pool_slots.len()];
        let mut failed_replacement: Option<(usize, String)> = None;
        loop {
            let available = self.tcp_pool_admission.available.notified();
            self.check_node_admission()?;
            let observations = self.sample_tcp_pool_observations();
            let replacement_allowed = self.tcp_pool_replacement_allowed();
            match self.tcp_pool_admission.try_decide_excluding(
                &observations,
                &replacement_allowed,
                &failed_replacement_slots,
                failed_replacement.is_some(),
            ) {
                Ok(TcpPoolAdmissionDecision::ReserveCurrent(reservation)) => {
                    if let Some((failed_slot, _)) = &failed_replacement {
                        println!(
                            "✅ tuic-tcp-pool-generation-replacement-fallback failed_slot={failed_slot} selected_slot={} same_open_retry=false",
                            reservation.index,
                        );
                    }
                    if self.tcp_pool_draining_predecessor.load(Ordering::Acquire)
                        && reservation.candidates.iter().any(|candidate| {
                            candidate.index != 0
                                && (candidate.qualification
                                    == TcpPoolForwardQualification::Degraded
                                    || candidate.successor_service_readiness.is_stale())
                                && !candidate.admitted
                        })
                    {
                        println!(
                            "⚠️ tuic-tcp-pool-generation-replacement-blocked slot={} reason=predecessor_draining",
                            reservation.index
                        );
                    }
                    return Ok((reservation, false));
                }
                Ok(TcpPoolAdmissionDecision::ReplaceAuxiliary(replacement)) => {
                    self.reject_unowned_maintenance()?;
                    let replacement_index = replacement.index;
                    let Some(permit) = TcpPoolDrainingPermit::try_acquire(
                        self.tcp_pool_draining_predecessor.clone(),
                    ) else {
                        drop(replacement);
                        continue;
                    };
                    match self.replace_auxiliary_generation(replacement, permit).await {
                        Ok(reservation) => return Ok((reservation, true)),
                        Err(error) => {
                            let error = format!("{error:?}");
                            let Some(excluded) =
                                failed_replacement_slots.get_mut(replacement_index)
                            else {
                                return Err(io_err(
                                    "tuic tcp pool generation replacement fallback",
                                    format!(
                                        "replacement slot index out of range: slot={replacement_index} error={error}"
                                    ),
                                ));
                            };
                            *excluded = true;
                            println!(
                                "⚠️ tuic-tcp-pool-generation-replacement-fallback failed_slot={replacement_index} same_open_retry=false error={error}"
                            );
                            failed_replacement = Some((replacement_index, error));
                        }
                    }
                }
                Err(TcpPoolReservationError::Busy) => available.await,
                Err(error) => {
                    if let Some((failed_slot, replacement_error)) = &failed_replacement {
                        return Err(io_err(
                            "tuic tcp pool generation replacement fallback",
                            format!(
                                "no current-generation fallback after slot={failed_slot} failure={replacement_error}: admission={error:?}"
                            ),
                        ));
                    }
                    return Err(ClientError::InvalidTarget(format!(
                        "tuic tcp pool forward admission failed: {error:?}"
                    )));
                }
            }
        }
    }

    async fn replace_auxiliary_generation(
        &self,
        replacement: TcpPoolAuxiliaryReplacementPreparation,
        permit: TcpPoolDrainingPermit,
    ) -> Result<TcpPoolSlotReservation, ClientError> {
        self.reject_unowned_maintenance()?;
        let slot = self
            .tcp_pool_slots
            .get(replacement.index)
            .ok_or_else(|| ClientError::InvalidTarget("tuic tcp pool index out of range".into()))?
            .clone();
        let expected_activity = self
            .tcp_pool_admission
            .current_generation_activity(replacement.index)
            .ok_or_else(|| {
                io_err(
                    "tuic tcp pool generation replacement",
                    "current generation activity unavailable",
                )
            })?;
        let replacement_candidate = replacement
            .candidates
            .iter()
            .find(|candidate| candidate.index == replacement.index);
        let successor_service_readiness = replacement_candidate
            .map(|candidate| candidate.successor_service_readiness)
            .unwrap_or_default();
        let replacement_reason = if successor_service_readiness.is_stale() {
            "successor_service_stale"
        } else {
            "forward_qualification_degraded"
        };
        let current_service_handoff = slot
            .prepare_replacement_current_service_handoff(replacement.identity)
            .await
            .map_err(|error| {
                io_err(
                    "tuic tcp pool generation replacement",
                    format!("predecessor current-service handoff failed: {error:?}"),
                )
            })?;
        let inherited_forward_service_floor = Some(current_service_handoff.required_cwnd_floor);
        let optional_counter = |value: Option<u64>| {
            value
                .map(|value| value.to_string())
                .unwrap_or_else(|| "unknown".into())
        };
        println!(
            "🔄 tuic-tcp-pool-generation-replacement-start slot={} predecessor_id={} generation={} active={} reason={} successor_service_certificate={} service_current_path_generation={} service_proved_path_generation={} service_proved_cwnd_floor={} observed_current_cwnd={} inherited_forward_service_floor={} black_hole_anchor={} black_holes_current={}",
            replacement.index,
            replacement.identity.stable_id,
            replacement.identity.generation,
            replacement.active_before,
            replacement_reason,
            successor_service_readiness.as_str(),
            optional_counter(
                replacement_candidate
                    .and_then(|candidate| candidate.successor_service_current_path_generation)
            ),
            optional_counter(
                replacement_candidate
                    .and_then(|candidate| candidate.successor_service_proved_path_generation)
            ),
            optional_counter(
                replacement_candidate
                    .and_then(|candidate| candidate.successor_service_proved_cwnd_floor)
            ),
            current_service_handoff.observed_cwnd,
            optional_counter(inherited_forward_service_floor),
            replacement.qualification_anchor,
            replacement.black_holes_current,
        );
        let started_at = Instant::now();
        let replacement_deadline = tokio::time::Instant::now() + TUIC_RECONNECT_TIMEOUT;
        let handshake = Self::handshake_aux_with_retries(
            &self.endpoint,
            self.server,
            &self.sni,
            &self.uuid,
            &self.password,
            self.zero_rtt,
            replacement.index,
        );
        let (successor_conn, auth_attempts) =
            tokio::time::timeout_at(replacement_deadline, handshake)
                .await
                .map_err(|_| {
                    io_err(
                        "tuic tcp pool generation replacement",
                        "successor handshake exceeded 5s",
                    )
                })??;
        let handshake_ms = started_at.elapsed().as_millis();
        let successor_stable_id = successor_conn.stable_id();
        let service_turn_started_at = Instant::now();
        let service_proof = prove_tcp_pool_successor_forward_service(
            &successor_conn,
            inherited_forward_service_floor,
            replacement_deadline,
        )
        .await?;
        let successor_path = successor_conn.stats().path;
        let successor_current_path_generation = successor_conn.current_path_generation();
        if successor_current_path_generation != service_proof.path_generation {
            return Err(io_err(
                "tuic tcp pool generation replacement",
                format!(
                    "successor path changed before install: proof={} current={successor_current_path_generation}",
                    service_proof.path_generation
                ),
            ));
        }
        if successor_path.cwnd < service_proof.final_cwnd {
            return Err(io_err(
                "tuic tcp pool generation replacement",
                format!(
                    "successor service regressed before install: proved={} current={}",
                    service_proof.final_cwnd, successor_path.cwnd
                ),
            ));
        }
        let service_turn_ms = service_turn_started_at.elapsed().as_millis();
        println!(
            "✅ tuic-tcp-pool-successor-forward-service-proof slot={} successor_id={} inherited_forward_service_floor={} initial_cwnd={} readiness_cwnd_floor={} final_cwnd={} rounds={} target_bytes={} sent_bytes={} acked_bytes={} lost_bytes={} path_generation={} service_turn_ms={}",
            replacement.index,
            successor_stable_id,
            optional_counter(service_proof.required_cwnd_floor),
            service_proof.initial_cwnd,
            service_proof.readiness_cwnd_floor,
            service_proof.final_cwnd,
            service_proof.rounds,
            service_proof.target_bytes,
            service_proof.sent_bytes,
            service_proof.acked_bytes,
            service_proof.lost_bytes,
            service_proof.path_generation,
            service_turn_ms,
        );
        let successor_generation =
            replacement
                .identity
                .generation
                .checked_add(1)
                .ok_or_else(|| {
                    io_err(
                        "tuic tcp pool generation replacement",
                        "generation counter saturated",
                    )
                })?;
        let successor = TcpPoolGeneration::new_successor(
            successor_conn.clone(),
            successor_generation,
            auth_attempts as u64,
            self.clock,
            service_proof.path_generation,
            service_proof.readiness_cwnd_floor,
        )
        .ok_or_else(|| {
            io_err(
                "tuic tcp pool generation replacement",
                "successful successor service turn produced a zero cwnd certificate",
            )
        })?;
        let successor_activity = successor.activity.clone();
        let Some(successor_install_service_proof) =
            TcpPoolSuccessorInstallServiceProof::from_forward_service(&successor, &service_proof)
        else {
            return Err(io_err(
                "tuic tcp pool generation replacement",
                "successful successor handoff proof does not match its readiness certificate",
            ));
        };
        let successor_observation = TcpPoolPathObservation::Known {
            identity: successor.identity(),
            path_service: TcpPoolPathService::known(successor_path.cwnd, successor_path.rtt),
            black_holes_detected: successor_path.black_holes_detected,
            path_generation: Some(service_proof.path_generation),
            successor_service_certificate: successor.successor_service_certificate,
        };
        let install = slot
            .install_successor_with(
                replacement.identity,
                &expected_activity,
                successor,
                successor_install_service_proof,
                started_at,
                |predecessor, successor| {
                    self.tcp_pool_admission.replace_current_generation_activity(
                        replacement.index,
                        predecessor,
                        successor.clone(),
                    )
                },
            )
            .await
            .map_err(|error| {
                io_err(
                    "tuic tcp pool generation replacement",
                    format!("successor install failed: {error:?}"),
                )
            })?;
        if let Some(secs) = self.quic_stats_secs
            && let Some(stop) = &self.quic_stats_stop
        {
            spawn_quic_stats_logger(successor_conn, replacement.index, secs, stop.subscribe());
        }
        println!(
            "✅ tuic-tcp-pool-generation-replacement-installed slot={} predecessor_id={} successor_id={} generation={} handshake_ms={} service_turn_ms={} replacement_ms={} active={}",
            replacement.index,
            install.predecessor_identity.stable_id,
            install.successor_identity.stable_id,
            install.successor_identity.generation,
            handshake_ms,
            service_turn_ms,
            started_at.elapsed().as_millis(),
            install.predecessor_activity.active(),
        );
        let predecessor_identity = install.predecessor_identity;
        let predecessor_activity = install.predecessor_activity.clone();
        let draining_owned = permit.transfer();
        tokio::spawn(async move {
            predecessor_activity.wait_for_zero().await;
            if let Some(draining) = slot.take_drained(predecessor_identity).await {
                draining
                    .generation
                    .transport
                    .close(VarInt::from_u32(0), b"tcp_pool_predecessor_drained");
                println!(
                    "✅ tuic-tcp-pool-generation-drained slot={} predecessor_id={} generation={} drain_ms={}",
                    slot.index,
                    predecessor_identity.stable_id,
                    predecessor_identity.generation,
                    draining.started_at.elapsed().as_millis(),
                );
                draining_owned.store(false, Ordering::Release);
            } else {
                println!(
                    "⚠️ tuic-tcp-pool-generation-replacement-blocked slot={} reason=predecessor_reap_identity_mismatch predecessor_id={} generation={}",
                    slot.index, predecessor_identity.stable_id, predecessor_identity.generation,
                );
            }
        });
        self.tcp_pool_admission
            .reserve_replacement_successor(replacement, successor_activity, successor_observation)
            .map_err(|error| {
                ClientError::InvalidTarget(format!(
                    "tuic tcp pool successor reservation failed: {error:?}"
                ))
            })
    }

    /// TCP 专用连接选择。Admission samples current Quinn path and PLPMTUD evidence without waiting,
    /// then atomically reserves an eligible slot. A busy slot that added a black-hole detection in
    /// its current ownership epoch is isolated while a non-degraded alternative exists. A
    /// certified auxiliary whose exact identity/path changed or whose current cwnd fell below its
    /// own post-turn floor uses the same bounded replacement seam while a non-stale lane exists.
    /// When all admitted candidates are busy with known service, ownership is ordered by exact
    /// `active * RTT / cwnd`; idle, unknown, and all-degraded fallback retain least-active ordering
    /// and equal nonzero `cwnd / RTT` tie-breaking. An idle pool keeps stable-index ordering so a
    /// following opener observes the first reservation.
    /// Idle age only requests a bounded liveness probe; it is not itself permission to destroy an
    /// auxiliary connection. The per-slot mutex still serializes probe/reconnect and connection
    /// cloning while the already-visible reservation steers unrelated opens toward other slots.
    pub(in crate::tuic) async fn live_tcp_conn(
        &self,
    ) -> Result<TcpPoolConnectionSelection, ClientError> {
        loop {
            let (reservation, replacement_installed) = self.acquire_tcp_pool_reservation().await?;
            let TcpPoolSlotReservation {
                index,
                mut lease,
                active_before,
                path_service,
                path_service_tiebreak,
                service_normalized_ordering,
                qualification,
                qualification_anchor,
                black_holes_current,
                qualification_override,
                all_degraded_fallback,
                candidates,
                preparation,
            } = reservation;
            let idle_exclusive = active_before == 0;
            let now_secs = self.clock.elapsed().as_secs();
            let slot = self.tcp_pool_slots.get(index).ok_or_else(|| {
                ClientError::InvalidTarget("tuic tcp pool index out of range".into())
            })?;
            let mut state = slot.state.lock().await;
            if self.udp_runtime.node_owned() && state.current.transport.close_reason().is_some() {
                let identity = state.current.identity();
                let activity = state.current.activity.clone();
                // Register the first deadline while holding the actual slot.
                // Original preparation prevents root physical work until release.
                let ticket = self.request_aux_recovery(index, state)?;
                self.release_recovery_lease(index, identity, &activity, &mut lease)?;
                drop(lease);
                drop(preparation);
                self.await_aux_recovery(ticket).await?;
                continue;
            }
            let current = &mut state.current;
            let open_state = current.open_state.clone();
            let last_success_age_secs = open_state.last_success_age_secs(now_secs);
            let last_success_secs = open_state.last_success_secs().unwrap_or(0);
            let mut probe_result = "not_due";
            let mut reconnect_reason = current
                .transport
                .close_reason()
                .is_some()
                .then_some("transport_closed");

            if reconnect_reason.is_none() && open_state.needs_reconnect(index) && idle_exclusive {
                reconnect_reason = Some("previous_open_failure");
            }
            if reconnect_reason.is_none()
                && should_probe_tcp_pool_generation(
                    index,
                    now_secs,
                    last_success_secs,
                    idle_exclusive,
                    replacement_installed,
                )
            {
                self.reject_unowned_maintenance()?;
                let outcome = probe_tcp_pool_connection(
                    &current.transport,
                    TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT,
                )
                .await;
                probe_result = match outcome {
                    TcpPoolProbeOutcome::Alive { .. } => "alive",
                    TcpPoolProbeOutcome::Closed => "closed",
                    TcpPoolProbeOutcome::SendFailed => "send_failed",
                    TcpPoolProbeOutcome::TimedOut { .. } => "timeout",
                };
                reconnect_reason = tcp_pool_probe_reconnect_reason(outcome);
                println!(
                    "🔎 tuic-tcp-pool-probe conn={index} id={} generation={} idle_age_secs={} result={probe_result}",
                    current.transport.stable_id(),
                    open_state.generation(),
                    now_secs.saturating_sub(last_success_secs),
                );
            }

            if let Some(reason) = reconnect_reason {
                self.reconnect_locked(index, &mut current.transport, reason)
                    .await?;
                current.note_reconnect_success();
                if index != 0 {
                    let ready = probe_tcp_pool_connection(
                        &current.transport,
                        TUIC_TCP_POOL_LIVENESS_PROBE_TIMEOUT,
                    )
                    .await;
                    probe_result = match ready {
                        TcpPoolProbeOutcome::Alive { .. } => "reconnected_alive",
                        TcpPoolProbeOutcome::Closed => "reconnected_closed",
                        TcpPoolProbeOutcome::SendFailed => "reconnected_send_failed",
                        TcpPoolProbeOutcome::TimedOut { .. } => "reconnected_timeout",
                    };
                    if !matches!(ready, TcpPoolProbeOutcome::Alive { .. }) {
                        open_state.note_open_failure(index);
                        current
                            .transport
                            .close(VarInt::from_u32(0), b"tcp_pool_ready_probe_failed");
                        return Err(io_err(
                            "tuic tcp pool ready probe",
                            format!("conn={index} result={probe_result}"),
                        ));
                    }
                }
            }

            let conn = current.transport.clone();
            let generation = open_state.generation();
            let write_pressure = current.write_pressure.clone();
            let read_pressure = current.read_pressure.clone();
            let startup_auth_attempts = current.auth_attempts;
            drop(state);
            drop(preparation);
            return Ok(TcpPoolConnectionSelection {
                conn_index: index,
                conn,
                lease,
                active_before,
                path_service,
                path_service_tiebreak,
                service_normalized_ordering,
                qualification,
                qualification_anchor,
                black_holes_current,
                qualification_override,
                all_degraded_fallback,
                candidates,
                generation,
                last_success_age_secs,
                probe_result,
                reconnect_reason,
                open_state,
                write_pressure,
                read_pressure,
                startup_auth_attempts,
                replacement_installed,
            });
        }
    }
}
