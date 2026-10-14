//! Physical reset lifecycle with real sockets; fault devices alter only egress.
use super::super::super::super::transaction::OpenCustody;
use super::super::super::physical_flow::tests::admission::receiving_pair;
use super::super::*;
use crate::client_tun::compatibility_runtime::SessionControl;
use crate::device::{DeviceShutdown, TunFlushedTcpPacket};
use crate::harness::LoopbackTunDevice;
use smoltcp::phy::{Device, DeviceCapabilities};
use std::task::{Context, Poll};

fn dispatch() -> (Arc<SessionControl>, OpenDispatch) {
    let owner = Arc::new(SessionControl::default());
    *owner.opens() = OpenCustody::with_flows();
    let (dispatch, _rx) = OpenDispatch::channel(4, Some(owner.clone()));
    (owner, dispatch)
}

/// A temporarily unavailable interface cannot issue a TX token.
struct NoTx<'a>(&'a mut LoopbackTunDevice);
impl Device for NoTx<'_> {
    type RxToken<'a>
        = <LoopbackTunDevice as Device>::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = <LoopbackTunDevice as Device>::TxToken<'a>
    where
        Self: 'a;
    fn receive(
        &mut self,
        _: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        None
    }
    fn transmit(&mut self, _: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        None
    }
    fn capabilities(&self) -> DeviceCapabilities {
        self.0.capabilities()
    }
}

#[tokio::test]
async fn reset_keeps_identity_and_shutdown_pending_until_actual_dispatch_and_flush() {
    let (mut client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let mut pool = FakeIpPool::new();
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    let deadline = contexts[&handle].local_reset.as_ref().unwrap().deadline;
    owner.request_stop();
    for _ in 0..3 {
        server.iface.poll(
            smoltcp::time::Instant::now(),
            &mut NoTx(&mut server.device),
            &mut server.sockets,
        );
        legacy_bare_rearm_from_loop(
            &dispatch,
            handle,
            server.sockets.get_mut::<TcpSocket>(handle),
            contexts.get_mut(&handle).unwrap(),
            &mut pool,
            0,
        );
        let reset = contexts[&handle].local_reset.as_ref().unwrap();
        assert_eq!(reset.deadline, deadline);
        assert!(!reset.committing && !reset.quarantined());
        assert_eq!(contexts[&handle].conn_epoch, 0);
        assert!(
            server
                .sockets
                .get::<TcpSocket>(handle)
                .remote_endpoint()
                .is_some()
        );
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert!(!owner.ledger().quarantined);
    }
    // The real interface now accepts RST. No peer close or session teardown forces it.
    server.iface.poll(
        smoltcp::time::Instant::now(),
        &mut server.device,
        &mut server.sockets,
    );
    assert!(
        server
            .sockets
            .get::<TcpSocket>(handle)
            .remote_endpoint()
            .is_none()
    );
    assert!(server.device.queued_tx_bytes() > 0);
    flush_tx_and_release_downlink_permits(
        &mut server.device,
        &server.sockets,
        &mut contexts,
        "reset-test",
    )
    .await
    .0
    .unwrap();
    client.turn().await;
    assert_eq!(
        client.sockets.get::<TcpSocket>(client.handle).state(),
        TcpState::Closed
    );
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    assert!(contexts[&handle].local_reset.is_none());
    assert_eq!(contexts[&handle].conn_epoch, 1);
    assert_eq!(
        server.sockets.get::<TcpSocket>(handle).state(),
        TcpState::Listen
    );
    assert!(dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
}

/// A backend may report failure after consuming its queue (partial/uncertain IO).
/// This is a failure injection, never a successful egress/peer delivery oracle.
struct DrainThenFail<'a>(&'a mut LoopbackTunDevice);
impl Device for DrainThenFail<'_> {
    type RxToken<'a>
        = <LoopbackTunDevice as Device>::RxToken<'a>
    where
        Self: 'a;
    type TxToken<'a>
        = <LoopbackTunDevice as Device>::TxToken<'a>
    where
        Self: 'a;
    fn receive(
        &mut self,
        now: smoltcp::time::Instant,
    ) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        self.0.receive(now)
    }
    fn transmit(&mut self, now: smoltcp::time::Instant) -> Option<Self::TxToken<'_>> {
        self.0.transmit(now)
    }
    fn capabilities(&self) -> DeviceCapabilities {
        self.0.capabilities()
    }
}
impl TunIo for DrainThenFail<'_> {
    fn request_stop(&mut self) {
        self.0.request_stop();
    }
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<DeviceShutdown> {
        self.0.poll_shutdown(cx)
    }
    async fn wait_for_rx(&mut self) -> std::io::Result<()> {
        self.0.wait_for_rx().await
    }
    fn try_recv_rx(&mut self) -> std::io::Result<bool> {
        self.0.try_recv_rx()
    }
    fn rx_peek(&self) -> Option<&[u8]> {
        self.0.rx_peek()
    }
    fn rx_take(&mut self) -> Option<bytes::BytesMut> {
        self.0.rx_take()
    }
    fn stage_rx_for_iface(&mut self) -> bool {
        self.0.stage_rx_for_iface()
    }
    async fn flush_tx(&mut self) -> std::io::Result<()> {
        self.0.flush_tx().await?;
        Err(std::io::Error::from_raw_os_error(32))
    }
    fn queued_tx_bytes(&self) -> usize {
        self.0.queued_tx_bytes()
    }
    fn take_flushed_tcp_packets(&mut self) -> Vec<TunFlushedTcpPacket> {
        self.0.take_flushed_tcp_packets()
    }
    fn inject_ip_packet(&mut self, pkt: &[u8]) {
        self.0.inject_ip_packet(pkt);
    }
}

#[tokio::test]
async fn empty_device_queue_after_flush_error_cannot_erase_reset_or_original_error() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let handle = server.handle;
    let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
    let mut pool = FakeIpPool::new();
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    server.iface.poll(
        smoltcp::time::Instant::now(),
        &mut server.device,
        &mut server.sockets,
    );
    assert!(
        server
            .sockets
            .get::<TcpSocket>(handle)
            .remote_endpoint()
            .is_none()
    );
    let result = flush_tx_and_release_downlink_permits(
        &mut DrainThenFail(&mut server.device),
        &server.sockets,
        &mut contexts,
        "failed-reset",
    )
    .await
    .0;
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
    assert_eq!(server.device.queued_tx_bytes(), 0);
    flush_tx_and_release_downlink_permits(
        &mut server.device,
        &server.sockets,
        &mut contexts,
        "later-success",
    )
    .await
    .0
    .unwrap();
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    let reset = contexts[&handle].local_reset.as_ref().unwrap();
    assert!(reset.quarantined() && !reset.committing);
    assert_eq!(reset.first_io.as_ref().unwrap().raw_os_error(), Some(32));
    assert_eq!(reset.flush_failures, 1);
    assert_eq!(contexts[&handle].conn_epoch, 0);
    owner.request_stop();
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
    assert!(owner.ledger().quarantined);
}

#[tokio::test(start_paused = true)]
async fn reset_identity_deadline_and_interrupted_commit_never_restart() {
    for cause in [
        ResetInvalid::Identity,
        ResetInvalid::Deadline,
        ResetInvalid::Interrupted,
    ] {
        let (_client, mut server) = receiving_pair().await;
        let (owner, dispatch) = dispatch();
        let mut pool = FakeIpPool::new();
        let handle = server.handle;
        let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
        legacy_bare_rearm_from_loop(
            &dispatch,
            handle,
            server.sockets.get_mut::<TcpSocket>(handle),
            contexts.get_mut(&handle).unwrap(),
            &mut pool,
            0,
        );
        let deadline = contexts[&handle].local_reset.as_ref().unwrap().deadline;
        match cause {
            ResetInvalid::Identity => contexts.get_mut(&handle).unwrap().conn_epoch += 1,
            ResetInvalid::Deadline => {
                tokio::time::advance(std::time::Duration::from_secs(15)).await
            }
            ResetInvalid::Interrupted => {
                server.iface.poll(
                    smoltcp::time::Instant::now(),
                    &mut server.device,
                    &mut server.sockets,
                );
                flush_tx_and_release_downlink_permits(
                    &mut server.device,
                    &server.sockets,
                    &mut contexts,
                    "prepare-commit",
                )
                .await
                .0
                .unwrap();
                assert!(LocalReset::prepare(
                    contexts.get_mut(&handle).unwrap(),
                    server.sockets.get_mut::<TcpSocket>(handle),
                    None
                ));
                // Inject an interruption after prepare, before the physical sink.
            }
            ResetInvalid::EpochExhausted => unreachable!("covered by the exact epoch tests"),
        }
        for _ in 0..2 {
            legacy_bare_rearm_from_loop(
                &dispatch,
                handle,
                server.sockets.get_mut::<TcpSocket>(handle),
                contexts.get_mut(&handle).unwrap(),
                &mut pool,
                0,
            );
            let reset = contexts[&handle].local_reset.as_ref().unwrap();
            assert_eq!(reset.invalid, Some(cause));
            assert_eq!(reset.deadline, deadline);
        }
        owner.request_stop();
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert!(owner.ledger().quarantined);
    }
}

#[tokio::test]
async fn reset_epoch_exhaustion_preserves_both_tuple_shapes_without_physical_mutation() {
    for connected in [false, true] {
        let (_client, mut server) = receiving_pair().await;
        let (owner, dispatch) = dispatch();
        let handle = server.handle;
        let socket = server.sockets.get_mut::<TcpSocket>(handle);
        if !connected {
            socket.abort();
            socket.listen(10000).unwrap();
        }
        let before = (
            socket.state(),
            socket.local_endpoint(),
            socket.remote_endpoint(),
            socket.recv_queue(),
        );
        let mut ctx = SocketCtx::new(10000);
        ctx.conn_epoch = u64::MAX;
        let mut contexts = HashMap::from([(handle, ctx)]);
        let mut pool = FakeIpPool::new();
        for _ in 0..2 {
            legacy_bare_rearm_from_loop(
                &dispatch,
                handle,
                server.sockets.get_mut::<TcpSocket>(handle),
                contexts.get_mut(&handle).unwrap(),
                &mut pool,
                0,
            );
            let ctx = &contexts[&handle];
            assert_eq!(
                ctx.conn_epoch,
                u64::MAX,
                "permanent exhaustion cannot wrap or rearm"
            );
            assert!(
                ctx.local_reset
                    .as_ref()
                    .is_some_and(LocalReset::quarantined),
                "retain a typed nonzero failure even without a tuple"
            );
            assert_eq!(
                ctx.local_reset.as_ref().unwrap().invalid,
                Some(ResetInvalid::EpochExhausted)
            );
            let socket = server.sockets.get::<TcpSocket>(handle);
            assert_eq!(
                (
                    socket.state(),
                    socket.local_endpoint(),
                    socket.remote_endpoint(),
                    socket.recv_queue()
                ),
                before
            );
        }
        owner.request_stop();
        assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
        assert!(owner.ledger().quarantined);
    }
}

#[tokio::test]
async fn reset_can_reach_max_epoch_once_then_retains_next_reset_failure() {
    let (_client, mut server) = receiving_pair().await;
    let (owner, dispatch) = dispatch();
    let handle = server.handle;
    let mut ctx = SocketCtx::new(10000);
    ctx.conn_epoch = u64::MAX - 1;
    let mut contexts = HashMap::from([(handle, ctx)]);
    let mut pool = FakeIpPool::new();
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    server.iface.poll(
        smoltcp::time::Instant::now(),
        &mut server.device,
        &mut server.sockets,
    );
    flush_tx_and_release_downlink_permits(
        &mut server.device,
        &server.sockets,
        &mut contexts,
        "epoch-boundary",
    )
    .await
    .0
    .unwrap();
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    assert_eq!(contexts[&handle].conn_epoch, u64::MAX);
    assert!(contexts[&handle].local_reset.is_none());
    assert_eq!(
        server.sockets.get::<TcpSocket>(handle).state(),
        TcpState::Listen
    );
    legacy_bare_rearm_from_loop(
        &dispatch,
        handle,
        server.sockets.get_mut::<TcpSocket>(handle),
        contexts.get_mut(&handle).unwrap(),
        &mut pool,
        0,
    );
    assert_eq!(contexts[&handle].conn_epoch, u64::MAX);
    assert!(
        contexts[&handle]
            .local_reset
            .as_ref()
            .is_some_and(LocalReset::quarantined)
    );
    owner.request_stop();
    assert!(!dispatch.flow_shutdown_complete(&server.sockets, &mut contexts, &pool));
}

/// A real admission and refused factory driven through the actual root control
/// events. Only the physical loop turn is withheld to expose reset ownership.
struct RejectedFlow {
    client: super::super::super::physical_flow::tests::admission::Endpoint,
    server: super::super::super::physical_flow::tests::admission::Endpoint,
    owner: Arc<SessionControl>,
    dispatch: OpenDispatch,
    contexts: HashMap<SocketHandle, SocketCtx>,
    pool: FakeIpPool,
}

impl RejectedFlow {
    async fn new() -> Self {
        use super::super::super::physical_flow::tests::admission::NeverOpen;
        let (client, mut server) = receiving_pair().await;
        let owner = Arc::new(SessionControl::default());
        *owner.opens() = OpenCustody::with_flows();
        let (dispatch, mut receiver) = OpenDispatch::channel(4, Some(owner.clone()));
        let upstream = Arc::new(NeverOpen::default());
        let handle = server.handle;
        let mut contexts = HashMap::from([(handle, SocketCtx::new(10000))]);
        let mut pool = FakeIpPool::new();
        dispatch.begin_turn().await;
        dispatch.admit_flow(
            upstream.clone(),
            FlowAdmission {
                handle,
                socket: server.sockets.get_mut::<TcpSocket>(handle),
                ctx: contexts.get_mut(&handle).unwrap(),
                fake_pool: &mut pool,
                fake_ip: None,
                target: TargetAddr::IpPort("10.0.0.2:10000".parse().unwrap()),
                now: 0,
            },
        );
        let (global_tx, _global_rx) = mpsc::channel(4);
        let (owned_action_tx, _owned_rx) = mpsc::channel(4);
        for _ in 0..512 {
            dispatch.begin_turn().await;
            let event = std::future::poll_fn(|cx| {
                let mut recv = std::pin::pin!(dispatch.recv(&mut receiver));
                Poll::Ready(std::future::Future::poll(recv.as_mut(), cx))
            })
            .await;
            if let Poll::Ready(Some(event)) = event {
                crate::client_tun::HandshakeCompletion::from_loop(
                    &dispatch,
                    event,
                    crate::client_tun::HandshakeCompletion {
                        sockets: &mut server.sockets,
                        socket_ctxs: &mut contexts,
                        global_tx: &global_tx,
                        owned_action_tx: &owned_action_tx,
                        fake_pool: &mut pool,
                        now_secs: 0,
                        metrics_handle: &crate::metrics::Metrics::default(),
                        downlink_backpressure: DownlinkBackpressureConfig::default(),
                        relay_read_hard_pause: false,
                    },
                );
            }
            if contexts[&handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .reset
                .is_some()
            {
                break;
            }
            tokio::time::advance(std::time::Duration::from_millis(1)).await;
        }
        let reset = contexts[&handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .reset
            .as_ref()
            .unwrap();
        let id = contexts[&handle].flow_mirror.as_ref().unwrap().id;
        assert_eq!(
            Some(reset.deadline.into_std()),
            owner.opens().flow_mut(id).unwrap().stop_deadline
        );
        assert!(!reset.committing && !reset.quarantined());
        assert_eq!(upstream.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        Self {
            client,
            server,
            owner,
            dispatch,
            contexts,
            pool,
        }
    }

    fn service(&mut self) {
        let id = self.contexts[&self.server.handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .id;
        let (global_tx, _global_rx) = mpsc::channel(4);
        let (owned_action_tx, _owned_rx) = mpsc::channel(4);
        self.dispatch.service_flow(
            id,
            &mut crate::client_tun::HandshakeCompletion {
                sockets: &mut self.server.sockets,
                socket_ctxs: &mut self.contexts,
                global_tx: &global_tx,
                owned_action_tx: &owned_action_tx,
                fake_pool: &mut self.pool,
                now_secs: 0,
                metrics_handle: &crate::metrics::Metrics::default(),
                downlink_backpressure: DownlinkBackpressureConfig::default(),
                relay_read_hard_pause: false,
            },
        );
    }

    fn assert_retained(&mut self) {
        let ctx = &self.contexts[&self.server.handle];
        assert!(ctx.local_reset.is_none());
        let mirror = ctx.flow_mirror.as_ref().unwrap();
        assert!(mirror.reset.is_some());
        let mut opens = self.owner.opens();
        let flow = opens.flow_mut(mirror.id).unwrap();
        assert_eq!(
            flow.stage,
            super::super::super::super::transaction::FlowStage::Quarantined
        );
        assert_eq!(&flow.escrow, b"untouched-rx");
        drop(opens);
        let seen = self.owner.flow_observation().unwrap();
        assert_eq!((seen.live, seen.w, seen.f, seen.released), (1, 1, 1, 0));
        assert_eq!(seen.escrow_bytes, b"untouched-rx".len());
        assert!(seen.audit && self.owner.ledger().quarantined);
        self.owner.request_stop();
        assert!(!self.dispatch.flow_shutdown_complete(
            &self.server.sockets,
            &mut self.contexts,
            &self.pool
        ));
    }
}

#[tokio::test(start_paused = true)]
async fn f_reset_flush_error_retains_original_source_even_after_queue_drained_and_later_ok() {
    for wrapped in [false, true] {
        let mut flow = RejectedFlow::new().await;
        flow.server.iface.poll(
            smoltcp::time::Instant::now(),
            &mut flow.server.device,
            &mut flow.server.sockets,
        );
        let result = if wrapped {
            let mut device = crate::client_tun::compatibility_runtime::egress::SessionEgress::new(
                DrainThenFail(&mut flow.server.device),
                flow.owner.clone(),
            );
            flush_tx_and_release_downlink_permits(
                &mut device,
                &flow.server.sockets,
                &mut flow.contexts,
                "F-reset-wrapped-fault",
            )
            .await
            .0
        } else {
            flush_tx_and_release_downlink_permits(
                &mut DrainThenFail(&mut flow.server.device),
                &flow.server.sockets,
                &mut flow.contexts,
                "F-reset-fault",
            )
            .await
            .0
        };
        assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::BrokenPipe);
        assert_eq!(flow.server.device.queued_tx_bytes(), 0);
        flow.client.turn().await;
        let handle = flow.server.handle;
        let reset = flow.contexts[&handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .reset
            .as_ref()
            .unwrap();
        let source = Arc::as_ptr(reset.first_io.as_ref().unwrap());
        if wrapped {
            let ledger = flow.owner.ledger();
            let raw = ledger.first_egress_io.as_ref().unwrap();
            assert!(Arc::ptr_eq(raw, reset.first_io.as_ref().unwrap()));
            assert_eq!(raw.raw_os_error(), Some(32));
            assert_eq!(ledger.egress_failures, 1);
            let facts = ledger.first_failure.as_ref().unwrap().facts();
            assert_eq!(
                facts.origin.phase,
                crate::shared::errors::FailurePhase::DeviceWrite
            );
            assert_eq!(facts.raw_os_code, Some(32));
        }
        for _ in 0..2 {
            flush_tx_and_release_downlink_permits(
                &mut flow.server.device,
                &flow.server.sockets,
                &mut flow.contexts,
                "F-reset-later-ok",
            )
            .await
            .0
            .unwrap();
            flow.service();
            flow.assert_retained();
            let reset = flow.contexts[&handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .reset
                .as_ref()
                .unwrap();
            assert_eq!(Arc::as_ptr(reset.first_io.as_ref().unwrap()), source);
            assert_eq!(reset.first_io.as_ref().unwrap().raw_os_error(), Some(32));
            assert_eq!(reset.flush_failures, 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn f_reset_identity_deadline_and_interrupted_commit_never_release_the_actual_row() {
    for cause in [
        ResetInvalid::Identity,
        ResetInvalid::Deadline,
        ResetInvalid::Interrupted,
    ] {
        let mut flow = RejectedFlow::new().await;
        let handle = flow.server.handle;
        let deadline = flow.contexts[&handle]
            .flow_mirror
            .as_ref()
            .unwrap()
            .reset
            .as_ref()
            .unwrap()
            .deadline;
        match cause {
            ResetInvalid::Identity => flow.contexts.get_mut(&handle).unwrap().conn_epoch += 1,
            ResetInvalid::Deadline => {
                tokio::time::advance(std::time::Duration::from_secs(15)).await
            }
            ResetInvalid::Interrupted => {
                flow.server.iface.poll(
                    smoltcp::time::Instant::now(),
                    &mut flow.server.device,
                    &mut flow.server.sockets,
                );
                flush_tx_and_release_downlink_permits(
                    &mut flow.server.device,
                    &flow.server.sockets,
                    &mut flow.contexts,
                    "F-reset-prepare",
                )
                .await
                .0
                .unwrap();
                let ctx = flow.contexts.get_mut(&handle).unwrap();
                assert!(ctx.flow_mirror.as_mut().unwrap().prepare_reset(
                    ctx.conn_epoch,
                    ctx.local_port,
                    &mut ctx.state,
                    flow.server.sockets.get_mut::<TcpSocket>(handle),
                    deadline
                ));
                // Failure injection after actual wire dispatch/flush and prepare,
                // before the synchronous physical receipt commit.
            }
            ResetInvalid::EpochExhausted => unreachable!(),
        }
        for _ in 0..2 {
            flow.service();
            flow.assert_retained();
            let reset = flow.contexts[&handle]
                .flow_mirror
                .as_ref()
                .unwrap()
                .reset
                .as_ref()
                .unwrap();
            assert_eq!(reset.deadline, deadline);
            if cause != ResetInvalid::Identity {
                assert_eq!(reset.invalid, Some(cause));
            }
        }
    }
}
