//! Actual smoltcp admission plus the same prepared four-role Permit pipeline.
//! No fake provider/F slot/physical receipt. Final public integration owns that
//! proof; these tests challenge the original queue/ctx/window custody seam.
use super::admission::{Endpoint, receiving_pair};
use super::*;
use crate::client_tun::compatibility_runtime::flow_custody::supervision::GenericLaunchRequest;
use crate::tcp_downlink_pump::{AsyncLeasedByteFlowQueue, DownstreamPermitReleaseMode};
use std::task::Poll;
use tokio::io::AsyncWriteExt;

struct Fixture {
    client: Endpoint,
    server: Endpoint,
    ctx: SocketCtx,
    roles: FlowRoles,
    remote: tokio::io::DuplexStream,
    back: crate::client_tun::relay_channel::Receiver<(SocketHandle, RelayEvent)>,
    commands: Option<crate::client_tun::uplink::Sender>,
    _credit: watch::Sender<RelayReadCredit>,
}
impl Fixture {
    async fn new() -> Self {
        let (client, server) = receiving_pair().await;
        let (local, remote) = tokio::io::duplex(65_536);
        let (commands, rx) = crate::client_tun::uplink::channel(2);
        let (back_tx, back) = crate::client_tun::relay_channel::channel(4);
        let (credit, read_credit_rx) = watch::channel(RelayReadCredit {
            paused: false,
            max_batch_bytes: 65_536,
        });
        let mut mirror = flow_mirror();
        mirror.id.handle = server.handle;
        mirror.window = Some(ReceiveWindow::new());
        let mut roles = FlowRoles::new(GenericLaunchRequest {
            engine: TcpRelayEngine::PermitPump,
            handle: server.handle,
            epoch: 7,
            stream: Box::new(local),
            rx,
            back_tx,
            read_credit_rx,
            downlink_backpressure: DownlinkBackpressureConfig::default(),
        });
        roles.prepare(mirror.window.as_mut().unwrap().take_reader().unwrap());
        assert!(mirror.bind_permit_queue(&roles, TcpRelayEngine::PermitPump));
        let mut ctx = SocketCtx::new(10000);
        ctx.conn_epoch = 7;
        ctx.state = SocketState::Relaying;
        ctx.flow_mirror = Some(mirror);
        roles.start();
        Self {
            client,
            server,
            ctx,
            roles,
            remote,
            back,
            commands: Some(commands),
            _credit: credit,
        }
    }
    async fn data(&mut self, bytes: &[u8]) -> (Vec<u8>, Option<DownstreamBytePermit>) {
        self.remote.write_all(bytes).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let (_, event) = self.back.recv().await.unwrap();
                if let RelayEvent::Data {
                    epoch,
                    bytes,
                    downstream_permit,
                    ..
                } = event
                {
                    assert_eq!(epoch, 7);
                    return (bytes.to_vec(), downstream_permit);
                }
            }
        })
        .await
        .unwrap()
    }
    async fn settle(&mut self) {
        self.roles.stop();
        drop(self.commands.take());
        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            std::future::poll_fn(|cx| {
                self.roles.poll(cx, true);
                if self.roles.settled() {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }),
        )
        .await
        .unwrap();
        assert_eq!(self.roles.join_count(), 4);
    }
}

#[tokio::test]
async fn actual_ctx_admission_releases_prefix_and_joined_closing_releases_remaining() {
    let mut f = Fixture::new().await;
    let queue = f.roles.permit_queue().unwrap().clone();
    let expected = vec![77; 8192];
    let (bytes, mut permit) = f.data(&expected).await;
    assert_eq!(bytes, expected);
    let mirror = f.ctx.flow_mirror.as_mut().unwrap();
    assert!(mirror.validate_data_permit(7, bytes.len(), &permit));
    assert!(mirror.begin_event(7, Some(bytes.len())));
    FlowMirror::accept_data(&mut f.ctx, &bytes, &mut permit);
    f.ctx.flow_mirror.as_mut().unwrap().finish_event();
    assert_eq!(queue.snapshot_now().leased_bytes, 8192);
    let admitted = admit_downlink_for_handle(
        f.server.handle,
        f.server.sockets.get_mut::<TcpSocket>(f.server.handle),
        &mut f.ctx,
        1024,
        DownlinkBackpressureConfig::default(),
        1500,
        &mut DownlinkEgressDropDebt::default(),
        DownlinkAdmissionOrigin::ControlOnly,
    )
    .accepted_bytes;
    assert!(admitted > 0 && admitted <= 1024);
    assert_eq!(f.ctx.downlink_pending.len(), 8192 - admitted);
    assert_eq!(queue.snapshot_now().leased_bytes, 8192 - admitted);
    // These are actual packets and actual receiving socket bytes after original
    // admission, not a synthetic window/lease refund.
    let mut received = Vec::with_capacity(1024);
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while received.len() < admitted {
            f.server.turn().await;
            f.client.turn().await;
            let socket = f.client.sockets.get_mut::<TcpSocket>(f.client.handle);
            while socket.can_recv() {
                let mut scratch = [0; 1024];
                let n = socket.recv_slice(&mut scratch).unwrap();
                received.extend_from_slice(&scratch[..n]);
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(received, expected[..admitted]);
    f.settle().await;
    assert!(f.roles.settled()); // Local joins never wait for external Main lease.
    assert!(!queue.physical_zero());
    let mirror = f.ctx.flow_mirror.as_mut().unwrap();
    assert!(!mirror.permit_physical_zero(Some(&f.roles)));
    assert!(mirror.dispose_pending(
        &mut f.ctx.downlink_pending,
        &mut f.ctx.downlink_pending_permits
    ));
    assert_eq!(mirror.abandoned_pending, 8192 - admitted);
    assert!(mirror.permit_physical_zero(Some(&f.roles)));
    assert!(queue.physical_zero());
    assert!(f.ctx.downlink_inflight_permits.is_empty());
    assert!(!mirror.quarantined && !mirror.window.as_ref().unwrap().failed());
}

#[tokio::test]
async fn same_original_queue_epoch_len_mode_and_foreign_validation_never_refund() {
    let mut f = Fixture::new().await;
    let queue = f.roles.permit_queue().unwrap().clone();
    let (bytes, mut permit) = f.data(b"abcd").await;
    let mirror = f.ctx.flow_mirror.as_ref().unwrap();
    assert!(mirror.validate_data_permit(7, bytes.len(), &permit));
    assert!(!mirror.validate_data_permit(8, bytes.len(), &permit));
    assert!(!mirror.validate_data_permit(7, 3, &permit));
    assert!(!mirror.validate_data_permit(7, 4, &None));
    let foreign = AsyncLeasedByteFlowQueue::new(4).unwrap();
    let egress = AsyncLeasedByteFlowQueue::new_with_release_mode(
        4,
        DownstreamPermitReleaseMode::OnEgressDrain,
    )
    .unwrap();
    for other in [&foreign, &egress] {
        other
            .push(bytes::Bytes::from_static(b"1234"))
            .await
            .unwrap();
        let (_, wrong) = other.recv_up_to(4).await.unwrap().into_parts();
        let mut wrong = Some(wrong);
        assert!(!mirror.validate_data_permit(7, 4, &wrong));
        assert_eq!(other.snapshot_now().leased_bytes, 4);
        assert_eq!(wrong.as_mut().unwrap().release(4), 4); // Test owns this rejected cap.
    }
    assert_eq!(queue.snapshot_now().leased_bytes, 4);
    assert_eq!(permit.as_ref().unwrap().remaining_bytes(), 4);
    f.settle().await;
    assert!(!queue.physical_zero());
    assert_eq!(permit.as_mut().unwrap().release(4), 4);
    assert!(queue.physical_zero());
}
