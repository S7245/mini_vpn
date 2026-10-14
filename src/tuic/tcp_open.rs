//! Owns TCP CONNECT preparation and the existing Generic/Native open entry points.
//! Pool admission/recovery, codecs and carrier DATA remain owned by the parent/reader Modules.
//! CONNECT keeps startup priority until the first business write; async compatibility entry
//! points retain their existing timeout semantics. ConnectWrite retains one writer, cursor and
//! source error across borrowed polling; it does not settle QUIC retirement. Tests live in tests/.

use super::*;

pub(super) trait TuicTcpStartupPriorityWrite: AsyncWrite + Unpin {
    fn current_priority(&mut self) -> io::Result<i32>;
    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()>;
    fn poll_write_then_set_priority(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        priority_after: i32,
    ) -> Poll<io::Result<usize>>;
}

impl TuicTcpStartupPriorityWrite for quinn::SendStream {
    fn current_priority(&mut self) -> io::Result<i32> {
        self.priority()
            .map_err(|error| io::Error::other(format!("tuic TCP priority read: {error}")))
    }

    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()> {
        self.set_priority(priority)
            .map_err(|error| io::Error::other(format!("tuic TCP priority set: {error}")))
    }

    fn poll_write_then_set_priority(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        priority_after: i32,
    ) -> Poll<io::Result<usize>> {
        match quinn::SendStream::poll_write_then_set_priority(self, cx, buf, priority_after) {
            Poll::Ready(Ok(written)) => Poll::Ready(Ok(written)),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error.into())),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Keeps a new TUIC TCP stream in one higher Quinn service class through Connect and the first
/// accepted nonempty business write, then atomically rejoins its original class. No payload is
/// copied or queued here; Quinn remains the only owner of transport backpressure.
pub(super) struct TuicTcpStartupWriter<W> {
    inner: W,
    original_priority: i32,
    startup_priority: i32,
    first_payload_pending: bool,
    diag_meta: Option<TuicTcpStreamDiagMeta>,
}

impl<W: TuicTcpStartupPriorityWrite> TuicTcpStartupWriter<W> {
    fn with_priority(inner: W, priority: StartupPriority) -> Self {
        Self {
            inner,
            original_priority: priority.original,
            startup_priority: priority.startup,
            first_payload_pending: true,
            diag_meta: None,
        }
    }

    fn set_diag_meta(&mut self, diag_meta: Option<TuicTcpStreamDiagMeta>) {
        self.diag_meta = diag_meta;
    }
}

impl TuicTcpStartupWriter<quinn::SendStream> {
    async fn prepare(
        send: quinn::SendStream,
        connect: Vec<u8>,
    ) -> io::Result<(Self, quinn::SendStreamProgress)> {
        let progress = send.progress_handle();
        let mut attempt = ConnectWrite::new(send, connect);
        std::future::poll_fn(|cx| attempt.poll(cx)).await;
        // This compatibility API still returns only an error on failure. The future staged
        // provider will retain the failure owner instead of using this legacy conversion.
        let writer = attempt.into_result().map_err(
            |ConnectWriteFailure {
                 inner: _inner,
                 written: _written,
                 error,
             }| error,
        )?;
        Ok((writer, progress))
    }
}

impl<W: TuicTcpStartupPriorityWrite> AsyncWrite for TuicTcpStartupWriter<W> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() || !self.first_payload_pending {
            return Pin::new(&mut self.inner).poll_write(cx, buf);
        }

        let original_priority = self.original_priority;
        match Pin::new(&mut self.inner).poll_write_then_set_priority(cx, buf, original_priority) {
            Poll::Ready(Ok(written)) if written > 0 => {
                self.first_payload_pending = false;
                if let Some(meta) = &self.diag_meta {
                    println!(
                        "{}",
                        format_tuic_tcp_startup_service_line(
                            meta,
                            written,
                            self.startup_priority,
                            self.original_priority,
                        )
                    );
                }
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Clone, Copy)]
struct StartupPriority {
    original: i32,
    startup: i32,
}

impl StartupPriority {
    fn arm<W: TuicTcpStartupPriorityWrite>(inner: &mut W) -> io::Result<Self> {
        let original = inner.current_priority()?;
        let startup = original
            .checked_add(TUIC_TCP_STARTUP_PRIORITY_DELTA)
            .ok_or_else(|| io::Error::other("tuic TCP startup priority overflow"))?;
        inner.set_stream_priority(startup)?;
        Ok(Self { original, startup })
    }
}

enum ConnectPhase {
    Unarmed,
    Writing(StartupPriority),
    Ready(StartupPriority),
    Failed(io::Error),
}

/// One CONNECT writer and cursor, retained independently of any borrowed observer future.
/// Production passes the existing encode_connect Vec (at most 261 bytes), without a copy.
/// This owner has no timer or task: caller cancellation/retirement belongs to the provider.
struct ConnectWrite<W> {
    inner: W,
    connect: Vec<u8>,
    written: usize,
    phase: ConnectPhase,
}

struct ConnectWriteFailure<W> {
    inner: W,
    written: usize,
    error: io::Error,
}

impl<W: TuicTcpStartupPriorityWrite> ConnectWrite<W> {
    fn new(inner: W, connect: Vec<u8>) -> Self {
        Self {
            inner,
            connect,
            written: 0,
            phase: ConnectPhase::Unarmed,
        }
    }

    /// Observe a retained terminal outcome, or admit at most one write.
    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        let priority = match &self.phase {
            ConnectPhase::Ready(_) | ConnectPhase::Failed(_) => return Poll::Ready(()),
            ConnectPhase::Writing(priority) => *priority,
            ConnectPhase::Unarmed => match StartupPriority::arm(&mut self.inner) {
                Ok(priority) => {
                    self.phase = ConnectPhase::Writing(priority);
                    priority
                }
                Err(error) => {
                    self.phase = ConnectPhase::Failed(error);
                    return Poll::Ready(());
                }
            },
        };
        if self.written == self.connect.len() {
            self.phase = ConnectPhase::Ready(priority);
            return Poll::Ready(());
        }
        let remaining = &self.connect[self.written..];
        // Bypass the startup wrapper: CONNECT must not consume first-business-write priority.
        match Pin::new(&mut self.inner).poll_write(cx, remaining) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.phase = ConnectPhase::Failed(error);
                Poll::Ready(())
            }
            Poll::Ready(Ok(0)) => {
                self.phase = ConnectPhase::Failed(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "tuic CONNECT writer made no progress",
                ));
                Poll::Ready(())
            }
            Poll::Ready(Ok(written)) if written > remaining.len() => {
                self.phase = ConnectPhase::Failed(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "tuic CONNECT writer exceeded supplied bytes",
                ));
                Poll::Ready(())
            }
            Poll::Ready(Ok(written)) => {
                self.written += written;
                if self.written == self.connect.len() {
                    self.phase = ConnectPhase::Ready(priority);
                    Poll::Ready(())
                } else {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
            }
        }
    }

    /// No fallible work follows Ready. Every other disposition returns the original writer.
    fn into_result(self) -> Result<TuicTcpStartupWriter<W>, ConnectWriteFailure<W>> {
        let error = match self.phase {
            ConnectPhase::Ready(priority) => {
                return Ok(TuicTcpStartupWriter::with_priority(self.inner, priority));
            }
            ConnectPhase::Failed(error) => error,
            ConnectPhase::Unarmed | ConnectPhase::Writing(_) => {
                io::Error::new(io::ErrorKind::WouldBlock, "tuic CONNECT has not completed")
            }
        };
        Err(ConnectWriteFailure {
            inner: self.inner,
            written: self.written,
            error,
        })
    }
}

struct GenericTracking {
    pressure_writer: TcpWritePressureWriter,
    tcp_stream_diag: Option<TuicTcpStreamDiag>,
    diag_meta: Option<TuicTcpStreamDiagMeta>,
}

impl GenericTracking {
    fn legacy(
        send: &mut TuicTcpStartupWriter<quinn::SendStream>,
        pressure: &Arc<TcpWritePressure>,
        stream_id: u64,
        diag_meta: Option<TuicTcpStreamDiagMeta>,
    ) -> Self {
        let tcp_stream_diag = diag_meta
            .clone()
            .map(|meta| TuicTcpStreamDiag::new(meta, Instant::now()));
        send.set_diag_meta(diag_meta.clone());
        Self {
            pressure_writer: pressure.writer(stream_id),
            tcp_stream_diag,
            diag_meta,
        }
    }
}

/// Exactly one registry owner; the carrier gets only a non-removing metrics handle.
struct RetainedPressure {
    pressure: Arc<TcpWritePressure>,
    state: Option<Arc<TcpWritePressureWriterState>>,
    removed: bool,
    failure: Option<PressureRetirementFailure>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PressureRetirementFailure {
    Poisoned,
    Identity,
}

impl RetainedPressure {
    fn split(mut writer: TcpWritePressureWriter) -> (Self, TcpWritePressureWriter) {
        writer.remove_on_drop = false;
        let retained = Self {
            pressure: writer.pressure.clone(),
            state: Some(writer.state.clone()),
            removed: false,
            failure: None,
        };
        (retained, writer)
    }

    fn try_retire(&mut self) -> Result<bool, PressureRetirementFailure> {
        if let Some(error) = self.failure {
            return Err(error);
        }
        if self.removed {
            return Ok(true);
        }
        let mut writers = match self.pressure.writers.try_lock() {
            Ok(writers) => writers,
            Err(std::sync::TryLockError::WouldBlock) => return Ok(false),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.failure = Some(PressureRetirementFailure::Poisoned);
                return Err(PressureRetirementFailure::Poisoned);
            }
        };
        let Some(state) = self.state.as_ref() else {
            self.failure = Some(PressureRetirementFailure::Identity);
            return Err(PressureRetirementFailure::Identity);
        };
        if !writers
            .get(&state.writer)
            .is_some_and(|stored| Weak::ptr_eq(stored, &Arc::downgrade(state)))
        {
            self.failure = Some(PressureRetirementFailure::Identity);
            return Err(PressureRetirementFailure::Identity);
        }
        // A live carrier or sampler still owns observation authority. Keep its
        // exact registration until it returns, even if a close was requested.
        if Arc::strong_count(state) != 1 {
            return Ok(false);
        }
        writers.remove(&state.writer);
        self.removed = true;
        drop(writers);
        // Drop the original strong progress reference before physical retirement.
        // The removal fact survives; lease/ACK still require the real bi witness.
        drop(self.state.take());
        Ok(true)
    }
}

impl TcpWritePressure {
    fn try_writer(self: &Arc<Self>, stream: u64) -> io::Result<Option<TcpWritePressureWriter>> {
        match self.writers.try_lock() {
            Ok(mut writers) => {
                let writer = self
                    .next_writer
                    .load(Ordering::Relaxed)
                    .checked_add(1)
                    .ok_or_else(|| io::Error::other("TUIC pressure writer IDs exhausted"))?;
                if writers.contains_key(&writer) {
                    return Err(io::Error::other("TUIC pressure writer identity collision"));
                }
                self.next_writer.store(writer, Ordering::Relaxed);
                Ok(Some(self.register_writer(stream, writer, &mut writers)))
            }
            Err(std::sync::TryLockError::WouldBlock) => Ok(None),
            Err(std::sync::TryLockError::Poisoned(_)) => {
                Err(io::Error::other("TUIC pressure registry poisoned"))
            }
        }
    }
}

struct GenericCarrierParts<'a> {
    recv: quinn::RecvStream,
    send: TuicTcpStartupWriter<quinn::SendStream>,
    send_progress: quinn::SendStreamProgress,
    tracking: GenericTracking,
    conn: &'a Connection,
}

impl GenericCarrierParts<'_> {
    fn build(self, relay_mode: TuicTcpRelayMode, lease: Option<TcpPoolSlotLease>) -> RelayStream {
        let Self {
            recv,
            send,
            send_progress,
            tracking,
            conn,
        } = self;
        let GenericTracking {
            pressure_writer,
            tcp_stream_diag,
            diag_meta,
        } = tracking;
        let relay: RelayStream = match relay_mode {
            TuicTcpRelayMode::OrderedJoin => Box::new(TrackedRelayStream::new_with_transport(
                TcpWritePressureAdapter::new_with_progress(
                    tokio::io::join(recv, send),
                    pressure_writer,
                    send_progress,
                ),
                lease,
                tcp_stream_diag,
                conn.clone(),
            )),
            TuicTcpRelayMode::OrderedChunk => Box::new(TrackedRelayStream::new_with_transport(
                TcpWritePressureAdapter::new_with_progress(
                    TuicOrderedRelayStream::from_quinn(recv, send),
                    pressure_writer,
                    send_progress,
                ),
                lease,
                tcp_stream_diag,
                conn.clone(),
            )),
            TuicTcpRelayMode::UnorderedReassembly => {
                Box::new(TrackedRelayStream::new_with_transport(
                    TcpWritePressureAdapter::new_with_progress(
                        TuicChunkRelayStream::new(recv, send, diag_meta),
                        pressure_writer,
                        send_progress,
                    ),
                    lease,
                    tcp_stream_diag,
                    conn.clone(),
                ))
            }
        };
        relay
    }
}

#[async_trait::async_trait]
impl ProxyUpstream for TuicUpstream {
    fn staged_tcp_open(
        self: Arc<Self>,
        request: crate::upstream::TcpOpenRequest,
    ) -> crate::upstream::StagedTcpOpen<OpenedTcpRelay> {
        Ok(Box::new(staged::create(self, request)?))
    }

    async fn open_tcp(&self, target: &TargetAddr) -> Result<RelayStream, ClientError> {
        // 取 TCP pool 活连接，断了只重连该槽位；pool=1 时等价旧行为。
        let selection = self.live_tcp_conn().await?;
        let TcpPoolConnectionSelection {
            conn_index,
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
            read_pressure: _,
            startup_auth_attempts,
            replacement_installed,
        } = selection;
        // 刀9（真出口 acceptance 修）：open_bi + write Connect 在**黑洞连接**上会 hang——连接尚未被
        // 判死（close_reason 仍 None，因 keepalive/非对称封锁架空 idle 检测），但 QUIC send 窗口满、
        // 收不到 ACK → write_all 无限阻塞，failover 快/慢路都收不到信号。封 5s 超时让黑洞 open **快速失败**
        // → FailoverUpstream 据「连接活但 open 超时」走 **慢路计数**（并发 open 下 ~5s 累计 3 次即切 REALITY，
        // 不再死等 close_reason）。正常 open（open_bi + 写小 Connect 头）是本地操作、远小于 5s，不误伤。
        let open = async {
            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| io_err("tuic open_bi", e))?;
            let stable_id = conn.stable_id();
            let stream_id = recv.id().index();
            let (mut send, send_progress) =
                TuicTcpStartupWriter::prepare(send, encode_connect(target))
                    .await
                    .map_err(|e| io_err("tuic connect write", e))?;
            open_state.note_open_success(self.clock.elapsed().as_secs());
            let relay_mode = self.relay_policy.tuic_generic_mode();
            let diag_meta = if tcp_diag_enabled() {
                println!(
                    "{}",
                    format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
                        conn_index,
                        stable_id,
                        active_before,
                        path_service,
                        path_service_tiebreak,
                        service_normalized_ordering,
                        qualification,
                        qualification_anchor,
                        black_holes_current,
                        qualification_override,
                        all_degraded_fallback,
                        candidates: &candidates,
                        generation,
                        last_success_age_secs,
                        probe_result,
                        reconnect_reason,
                        replacement_installed,
                    })
                );
                println!(
                    "{}",
                    format_tuic_tcp_open_line(
                        target,
                        conn_index,
                        stable_id,
                        stream_id,
                        startup_auth_attempts,
                        relay_mode.as_str(),
                    )
                );
                Some(TuicTcpStreamDiagMeta::new(
                    target, conn_index, stable_id, stream_id,
                ))
            } else {
                None
            };
            let tracking =
                GenericTracking::legacy(&mut send, &write_pressure, stream_id, diag_meta);
            let relay = GenericCarrierParts {
                recv,
                send,
                send_progress,
                tracking,
                conn: &conn,
            }
            .build(relay_mode, Some(lease));
            Ok::<RelayStream, ClientError>(relay)
        };
        let result = tokio::time::timeout(TUIC_OPEN_TIMEOUT, open)
            .await
            .map_err(|_| {
                io_err(
                    "tuic open_tcp",
                    "5s 超时（黑洞/send 窗口满无 ACK；failover 慢路据此累计切备腿）",
                )
            })
            .and_then(|result| result);
        if result.is_err() {
            open_state.note_open_failure(conn_index);
        }
        result
    }

    async fn open_tcp_relay(&self, target: &TargetAddr) -> Result<OpenedTcpRelay, ClientError> {
        let relay_mode = self.relay_policy.tuic_native_override();
        if relay_mode == TuicNativeOverride::None {
            return self.open_tcp(target).await.map(OpenedTcpRelay::Generic);
        };
        let d16_byte_owned = relay_mode == TuicNativeOverride::D16DirectOrdered;

        let selection = self.live_tcp_conn().await?;
        let TcpPoolConnectionSelection {
            conn_index,
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
        } = selection;
        let open = async {
            let (send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| io_err("tuic open_bi", e))?;
            let stable_id = conn.stable_id();
            let stream_id = recv.id().index();
            let (mut send, send_progress) =
                TuicTcpStartupWriter::prepare(send, encode_connect(target))
                    .await
                    .map_err(|e| io_err("tuic connect write", e))?;
            open_state.note_open_success(self.clock.elapsed().as_secs());
            let diag_meta = if tcp_diag_enabled() {
                println!(
                    "{}",
                    format_tuic_tcp_pool_selection_line(TuicTcpPoolSelectionDiag {
                        conn_index,
                        stable_id,
                        active_before,
                        path_service,
                        path_service_tiebreak,
                        service_normalized_ordering,
                        qualification,
                        qualification_anchor,
                        black_holes_current,
                        qualification_override,
                        all_degraded_fallback,
                        candidates: &candidates,
                        generation,
                        last_success_age_secs,
                        probe_result,
                        reconnect_reason,
                        replacement_installed,
                    })
                );
                println!(
                    "{}",
                    format_tuic_tcp_open_line(
                        target,
                        conn_index,
                        stable_id,
                        stream_id,
                        startup_auth_attempts,
                        relay_mode.as_str(),
                    )
                );
                Some(TuicTcpStreamDiagMeta::new(
                    target, conn_index, stable_id, stream_id,
                ))
            } else {
                None
            };
            let tcp_stream_diag = diag_meta
                .clone()
                .map(|meta| TuicTcpStreamDiag::new(meta, Instant::now()));
            send.set_diag_meta(diag_meta);
            let reader_lease = lease.try_clone().map_err(|error| {
                io_err(
                    "tuic TCP generation lease split",
                    format!("failed to reserve reader ownership: {error:?}"),
                )
            })?;
            let reader: NativeTcpReadHalf = if d16_byte_owned {
                let read_pressure = tcp_diag_enabled()
                    .then(|| read_pressure.reader(stream_id, recv.progress_handle()));
                Box::new(TuicNativeOrderedReader::new(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                    read_pressure,
                ))
            } else if relay_mode == TuicNativeOverride::NativeOrderedPump {
                Box::new(TuicNativeOrderedPumpReader::spawn(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                ))
            } else {
                Box::new(TuicNativeTcpReader::new(
                    recv,
                    reader_lease,
                    tcp_stream_diag,
                    conn.clone(),
                ))
            };
            let writer = Box::new(TuicNativeTcpWriter::new(
                send,
                lease,
                write_pressure.writer(stream_id),
                send_progress,
            ));
            let relay = NativeTcpRelayStream { reader, writer };
            Ok::<OpenedTcpRelay, ClientError>(if d16_byte_owned {
                OpenedTcpRelay::NativeByteOwned(relay)
            } else {
                OpenedTcpRelay::Native(relay)
            })
        };
        let result = tokio::time::timeout(TUIC_OPEN_TIMEOUT, open)
            .await
            .map_err(|_| {
                io_err(
                    "tuic open_tcp",
                    "5s 超时（黑洞/send 窗口满无 ACK；failover 慢路据此累计切备腿）",
                )
            })
            .and_then(|result| result);
        if result.is_err() {
            open_state.note_open_failure(conn_index);
        }
        result
    }

    /// 刀14d：TUIC `open_tcp` 通常很快，但它仍可能 await QUIC reconnect、`open_bi` 或 Connect write
    /// flow-control up to `TUIC_OPEN_TIMEOUT`. 在单任务主循环里 inline await 会让一条慢 open 暂停
    /// 下行 flush、timer、DNS 和 reap；所以走已有 async-open 状态机。
    fn open_is_cheap(&self) -> bool {
        false
    }
}

mod staged;

#[cfg(test)]
mod tests;
