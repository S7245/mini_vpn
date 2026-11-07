//! Fault injection only at the actual original Source disposition handoff.
use super::*;
impl Work {
    pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles) fn pending_stock_for_test(&self) -> Option<(usize, usize)> {
        match &*self.action { Action::Stock(stock, _) if !self.progressed && !self.interrupted => stock.pending_allocation_for_test(), _ => None }
    }
    pub(in crate::client_tun::compatibility_runtime::flow_custody::supervision::roles) fn is_source_for_test(&self) -> bool {
        matches!(&*self.action, Action::Source { .. })
    }
}

#[cfg(feature = "harness")]
mod full_signal {
    use super::*;
    use crate::client_tun::compatibility_runtime::{SessionControl, flow_custody::{
        rearm::physical_flow::{FlowAdmission, FatalPhysicalTurn, tests::admission::{receiving_pair, NeverOpen}},
        transaction::{OpenCustody, OpenDispatch}, receive_window::ReceiveWindow,
    }};
    use crate::upstream::{NativeTcpReader, NativeTcpChunk, NativeTcpRelayStream};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Reader {
        stream: tokio::io::DuplexStream,
        meter: bytes::BackingMeter,
        polls: Arc<AtomicUsize>,
    }
    impl NativeTcpReader for Reader {
        fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> { Some(self.meter.clone()) }
        fn source_read_bytes(&self) -> Option<usize> { Some(0) }
        fn poll_read_chunk(&mut self, cx: &mut Context<'_>, _: usize) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
            use tokio::io::AsyncRead;
            self.polls.fetch_add(1, Ordering::SeqCst);
            let mut byte = [0];
            let mut buffer = tokio::io::ReadBuf::new(&mut byte);
            match Pin::new(&mut self.stream).poll_read(cx, &mut buffer) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => {
                    assert!(buffer.filled().is_empty(), "fixture peer sends no DATA");
                    Poll::Ready(Ok(None))
                }
            }
        }
    }

    #[tokio::test]
    async fn native_poisoned_flow_full_signal_joins_through_original_fatal_turn() {
        full_signal(1).await;
    }
    #[tokio::test]
    async fn native_producer_watch_drop_notifies_outside_original_flow_lock() {
        full_signal(0).await;
    }
    #[tokio::test]
    async fn native_producer_lost_turn_retains_original_endpoints_without_stop_ack() { full_signal(2).await; }
    #[tokio::test]
    async fn native_producer_wrong_return_context_retains_original_endpoints() { full_signal(3).await; }
    #[tokio::test]
    async fn native_producer_foreign_context_is_rejected_before_taking_endpoints() { full_signal(4).await; }
    #[tokio::test]
    async fn native_producer_callback_identity_loss_cannot_commit_stop_ack() { full_signal(5).await; }
    #[tokio::test]
    async fn native_producer_callback_panic_keeps_original_box_and_blocks_empty_retry() { full_signal(6).await; }
    async fn full_signal(mode: u8) {
        let poisoned = mode == 1;
        use crate::client_tun::{SocketCtx, RelayEventLane};
        let (_client, mut server) = receiving_pair().await;
        let budget = crate::client_tun::compatibility_runtime::flow_custody::supervision::roles::native::stock_tests::budget_with_old_headroom(65536);
        let meter = bytes::BackingMeter::with_budget(&budget);
        let owner = Arc::new({
            let mut owner = SessionControl::default();
            owner.app_meter = Some(meter.clone());
            owner
        });
        *owner.opens() = OpenCustody::with_flows();
        let (dispatch, _notices) = OpenDispatch::channel(4, Some(owner.clone()));
        let upstream = Arc::new(NeverOpen::default());
        let mut contexts = std::collections::HashMap::from([(server.handle, SocketCtx::new(10000))]);
        let mut pool = crate::fake_ip::FakeIpPool::new();
        dispatch.begin_turn().await;
        dispatch.admit_flow(upstream.clone(), FlowAdmission {
            handle: server.handle,
            socket: server.sockets.get_mut::<smoltcp::socket::tcp::Socket>(server.handle),
            ctx: contexts.get_mut(&server.handle).unwrap(), fake_pool: &mut pool, fake_ip: None,
            target: crate::shared::TargetAddr::IpPort("127.0.0.1:10000".parse().unwrap()), now: 0,
        });
        assert!(contexts[&server.handle].flow_mirror.is_some());
        let id = owner.opens().slots.iter().flatten().find_map(|slot| {
            match &slot.phase {
                crate::client_tun::compatibility_runtime::flow_custody::transaction::OpenPhase::Flow(flow)
                    if flow.id.handle == server.handle => Some(flow.id),
                _ => None,
            }
        }).unwrap();
        let clock = FlowRoles::prepare_failure_owner(&owner, id).unwrap();
        let (tx, rx) = crate::client_tun::uplink::channel(RELAY_CHANNEL_CAPACITY);
        let (back_tx, back) = crate::client_tun::relay_channel::channel(RELAY_CHANNEL_CAPACITY);
        let (credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
        let mut observing_credit = credit.subscribe();
        let (writer, peer) = tokio::io::duplex(RELAY_CHANNEL_CAPACITY);
        let (reader, _reader_peer) = tokio::io::duplex(1);
        let polls = Arc::new(AtomicUsize::new(0));
        let mut roles = FlowRoles::new_native_with_clock(NativeLaunchRequest {
            uplink_observer: None,
            handle: id.handle, epoch: id.epoch,
            native: NativeTcpRelayStream::new(Box::new(Reader { stream: reader, meter, polls: polls.clone() }), Box::new(writer)),
            engine: NativeTcpRelayEngine::ChunkPump, read_floor: false, rx, back_tx, read_credit_rx,
            downlink_backpressure: DownlinkBackpressureConfig::default(),
        }, clock.clone()).ok().unwrap();
        let mut window = ReceiveWindow::new();
        roles.prepare(window.take_reader().unwrap());
        assert!(roles.commit_native());
        assert!(roles.native.as_ref().unwrap().source.is_some(), "actual hard SourceCallbacks stock");
        let start = {
            let mut opens = owner.opens();
            let flow = opens.flow_mut(id).unwrap();
            assert!(flow.physical && flow.roles.is_none());
            flow.roles = Some(roles);
            flow.roles.as_mut().unwrap().take_native_start(&owner, id, None).unwrap()
        };
        assert!(start.dispose(&owner, &mut Context::from_waker(std::task::Waker::noop())));
        let monitor_stop = owner.opens().flow_mut(id).unwrap().roles.as_mut().unwrap()
            .native.as_mut().unwrap().stops[2].take().unwrap();
        monitor_stop.send(()).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            std::future::poll_fn(|cx| {
                let mut opens = owner.opens();
                let native = opens.flow_mut(id).unwrap().roles.as_mut().unwrap().native.as_mut().unwrap();
                native.poll_handles(cx);
                if matches!(native.monitor, Role::Joined(_)) && polls.load(Ordering::SeqCst) > 0 { Poll::Ready(()) }
                else { cx.waker().wake_by_ref(); Poll::Pending }
            }).await;
            let signal_len = || {
                let mut opens = owner.opens();
                let roles = opens.flow_mut(id).unwrap().roles.as_ref().unwrap();
                let Role::Joined(monitor) = &roles.native.as_ref().unwrap().monitor else { panic!("actual monitor JOIN"); };
                monitor.monitor.signals.writer_signal_rx.len()
            };
            for n in 1..=RELAY_CHANNEL_CAPACITY / 3 {
                tx.send(RelayCommand::Data(vec![1])).await.unwrap();
                while signal_len() != n * 3 { tokio::task::yield_now().await; }
            }
            drop(peer); // Real AsyncWrite BrokenPipe after actual successful writes.
            tx.send(RelayCommand::Data(vec![1])).await.unwrap();
            while signal_len() != RELAY_CHANNEL_CAPACITY || clock.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        struct WakeProbe { owner: Arc<SessionControl>, calls: AtomicUsize, locked: AtomicUsize, invalidate: Option<usize>, panic: Option<Arc<usize>> }
        impl std::task::Wake for WakeProbe {
            fn wake(self: Arc<Self>) { self.wake_by_ref(); }
            fn wake_by_ref(self: &Arc<Self>) {
                let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
                if self.owner.opens.try_lock().is_err() { self.locked.fetch_add(1, Ordering::SeqCst); }
                else if first {
                    if let Some(index) = self.invalidate { self.owner.opens().slots.get_mut(index).unwrap().epoch += 1; }
                }
                if let Some(payload) = &self.panic { std::panic::panic_any(payload.clone()); }
            }
        }
        let producer_box = Arc::new(91usize);
        let wake_probe = Arc::new(WakeProbe { owner: owner.clone(), calls: AtomicUsize::new(0), locked: AtomicUsize::new(0), invalidate: (mode == 5).then_some(id.index), panic: (mode == 6).then(|| producer_box.clone()) });
        let original_waker = std::task::Waker::from(wake_probe.clone());
        let mut changed = Box::pin(observing_credit.changed());
        assert!(changed.as_mut().poll(&mut Context::from_waker(&original_waker)).is_pending());
        let ctx = contexts.get_mut(&id.handle).unwrap();
        ctx.uplink_tx = Some(tx.clone());
        ctx.relay_read_credit_tx = Some(credit);
        let first = owner.ledger().first_failure.as_ref().unwrap().facts().clone();
        let first_ptr = owner.ledger().first_failure.as_ref().unwrap() as *const _ as usize;
        assert_eq!(first.io_kind, Some(std::io::ErrorKind::BrokenPipe));
        let deadline = owner.request_stop();
        let payload = Box::new(Arc::new(73usize));
        let original = (*payload).clone();
        if poisoned { owner.opens().flow_mut(id).unwrap().panic = Some(payload); }
        let source_polls = polls.load(Ordering::SeqCst);
        if poisoned { owner.note_flow_unwind(id); } else { clock.panic(); }
        assert!(budget.quarantined() && owner.stop.stopping.load(Ordering::Acquire));
        if mode >= 2 {
            let ctx = contexts.get_mut(&id.handle).unwrap();
            if mode == 4 { ctx.conn_epoch += 1; }
            let turn = owner.opens().flow_mut(id).unwrap().roles.as_mut().unwrap()
                .take_native_producers(&owner, id, ctx, Some(deadline.into_std()));
            if mode == 4 {
                assert!(turn.is_none());
                assert!(ctx.uplink_tx.as_ref().unwrap().same_channel(&tx) && ctx.relay_read_credit_tx.is_some());
                let mut opens = owner.opens();
                let roles = opens.flow_mut(id).unwrap().roles.as_ref().unwrap();
                assert!(!roles.failure_disposal && roles.native.as_ref().unwrap().producers.is_none());
            } else {
                let turn = turn.unwrap();
                assert!(ctx.uplink_tx.is_none() && ctx.relay_read_credit_tx.is_none());
                if mode == 2 { drop(turn); }
                else {
                    if mode == 3 { ctx.conn_epoch += 1; }
                    assert!(!turn.dispose_producers(&owner, &mut Context::from_waker(std::task::Waker::noop()), ctx));
                }
                let mut opens = owner.opens();
                let flow = opens.flow_mut(id).unwrap();
                assert!(!flow.producer_stopped && flow.roles.as_ref().unwrap().failure_disposal);
                assert!(!flow.roles.as_ref().unwrap().settled());
                if mode == 6 {
                    let roles = flow.roles.as_mut().unwrap();
                    let native = roles.native.as_ref().unwrap();
                    assert!(native.retained_producers.is_some());
                    assert!(native.source.as_ref().unwrap().producer_payload_matches(&producer_box));
                    assert!(roles.take_native_producers(&owner, id, ctx, Some(deadline.into_std())).is_none(), "empty endpoints cannot retry partial Drop into success");
                }
            }
            if mode < 5 {
                assert_eq!(wake_probe.calls.load(Ordering::SeqCst), 0);
                assert!(changed.as_mut().poll(&mut Context::from_waker(&original_waker)).is_pending(), "the original watch Sender is physically retained");
            } else {
                assert_eq!(wake_probe.calls.load(Ordering::SeqCst), 1);
                assert_eq!(wake_probe.locked.load(Ordering::SeqCst), 0);
                assert!(matches!(changed.as_mut().poll(&mut Context::from_waker(&original_waker)), Poll::Ready(Err(_))));
            }
            assert_eq!(owner.ledger().first_failure.as_ref().unwrap() as *const _ as usize, first_ptr);
            assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
            if mode == 6 { assert_eq!(owner.ledger().additional_failures, 2, "one original Q fault and one actual callback panic, no JOIN recount"); }
            assert!(budget.quarantined() && budget.used() > 0);
            return;
        }
        let mut lane = RelayEventLane::new(back);
        let mut selected = None;
        let mut first_join_closed = None;
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            loop {
                dispatch.begin_turn().await;
                let mut physical = FatalPhysicalTurn::new(&server.sockets, &mut contexts, &pool, &mut lane, &mut selected);
                let _ = dispatch.poll_fatal(&mut Context::from_waker(std::task::Waker::noop()), &mut physical);
                let complete = {
                    let mut opens = owner.opens();
                    let roles = opens.flow_mut(id).unwrap().roles.as_ref().unwrap();
                    let closed = matches!(&roles.native.as_ref().unwrap().monitor,
                        Role::Joined(monitor) if monitor.closed && monitor.monitor.signals.writer_signal_rx.is_closed());
                    if roles.join_count() == 4 {
                        if first_join_closed.is_none() { first_join_closed = Some(closed); }
                        closed
                    } else { false }
                };
                if complete { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        eprintln!("[N1-writer-Q-physical] mode={mode} first_four_JOIN_monitor_closed={first_join_closed:?} final_monitor_closed=true original_observation_limit=1s");
        let mut opens = owner.opens();
        let flow = opens.flow_mut(id).unwrap();
        if poisoned {
            assert!(Arc::ptr_eq(flow.panic.as_ref().unwrap().downcast_ref::<Arc<usize>>().unwrap(), &original));
            assert!(!flow.producer_stopped, "poisoned provider was never re-entered");
        } else {
            assert!(flow.producer_stopped);
            assert!(wake_probe.calls.load(Ordering::SeqCst) > 0, "actual original watch sender Drop notified its registered receiver");
            assert_eq!(wake_probe.locked.load(Ordering::SeqCst), 0, "producer endpoint Drop must leave opens before callback");
        }
        let roles = flow.roles.as_ref().unwrap();
        assert!(!roles.settled());
        let Role::Joined(monitor) = &roles.native.as_ref().unwrap().monitor else { panic!("actual JOIN"); };
        assert!(monitor.closed && monitor.monitor.signals.writer_signal_rx.is_closed());
        assert_eq!(monitor.discarded[0] + monitor.monitor.signals.writer_signal_rx.len(), RELAY_CHANNEL_CAPACITY);
        drop(opens);
        assert_eq!(upstream.0.load(Ordering::SeqCst), 0);
        assert_eq!(polls.load(Ordering::SeqCst), source_polls, "Q cancel never re-enters Source DATA");
        assert_eq!(owner.ledger().first_failure.as_ref().unwrap() as *const _ as usize, first_ptr);
        assert_eq!(owner.ledger().first_failure.as_ref().unwrap().facts(), &first);
        assert_eq!(owner.ledger().shutdown_deadline, Some(deadline));
        assert!(budget.quarantined() && budget.used() > 0);
    }
}

impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn stock_turn_held_for_test(&self) -> bool {
        self.failure_disposal && self.native.as_ref().is_some_and(|roles| roles.stock_retiring && roles.stock.is_none())
    }
}
