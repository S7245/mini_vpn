//! Original capture characterization and passive foreign-custody injection.
use super::*;

#[tokio::test]
async fn duplicate_flush_endpoint_pair_quarantines_every_candidate() {
    let mut sockets = SocketSet::new(vec![]);
    let handles = [
        sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 8]),
            TcpSocketBuffer::new(vec![0; 8]),
        )),
        sockets.add(TcpSocket::new(
            TcpSocketBuffer::new(vec![0; 8]),
            TcpSocketBuffer::new(vec![0; 8]),
        )),
    ];
    let binding = Binding {
        epoch: 0,
        local: IpEndpoint::new(IpAddress::v4(203, 0, 113, 9), 443),
        remote: IpEndpoint::new(IpAddress::v4(10, 0, 0, 2), 53124),
    };
    let mut contexts = HashMap::new();
    let mut queues = Vec::new();
    for handle in handles {
        let queue = AsyncLeasedByteFlowQueue::new_with_release_mode(
            4,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap();
        queue
            .push(bytes::Bytes::from_static(b"abcd"))
            .await
            .unwrap();
        let (_, cap) = queue.recv_up_to(4).await.unwrap().into_parts();
        let mut ctx = SocketCtx::new(443);
        assert!(ctx.downlink_inflight_permits.permits.insert(&mut Some(cap)));
        contexts.insert(handle, ctx);
        queues.push(queue);
    }
    let captured = handles
        .into_iter()
        .map(|handle| FlushCapture {
            handle,
            binding,
            disposition: CaptureDisposition::Active,
        })
        .collect();
    let release = finish_flush_batch_legacy(
        vec![TunFlushedTcpPacket {
            src_addr: std::net::Ipv4Addr::new(203, 0, 113, 9),
            dst_addr: std::net::Ipv4Addr::new(10, 0, 0, 2),
            src_port: 443,
            dst_port: 53124,
            payload_bytes: 4,
            payload_start_seq: 1,
        }],
        &sockets,
        &mut contexts,
        captured,
        true,
    );
    assert_eq!(release.released_inflight_permit_bytes, 0);
    for handle in handles {
        assert_eq!(
            contexts[&handle].downlink_inflight_permits.fault(),
            Some("ambiguous flush endpoint pair")
        );
    }
    assert!(
        queues
            .iter()
            .all(|queue| queue.snapshot_now().leased_bytes == 4)
    );
}

impl SocketCtx {
    #[cfg(feature = "harness")]
    pub(in crate::client_tun) fn test_inject_foreign_dead_inflight(
        &mut self,
        handle: SocketHandle,
        socket: &TcpSocket<'_>,
        foreign: DownstreamBytePermit,
    ) -> Result<(usize, usize), DownstreamBytePermit> {
        let original = self.downlink_inflight_permit_bytes();
        if original == 0
            || foreign.remaining_bytes() == 0
            || foreign.release_mode() != DownstreamPermitReleaseMode::OnEgressDrain
            || !retains_dead_ordered_inflight(handle, self, socket)
        {
            return Err(foreign);
        }
        let foreign_bytes = foreign.remaining_bytes();
        self.downlink_inflight_permits
            .test_install_foreign_inflight(foreign);
        Ok((original, foreign_bytes))
    }
}

impl PollBatch {
    pub(in crate::client_tun) fn test_forget_native_index(&mut self) {
        if let Self::Native(batch) = self {
            batch.test_forget_index();
        } else {
            panic!("Native required");
        }
    }
}
impl SocketCtx {
    pub(in crate::client_tun) fn test_native_capture(&self) -> (usize, bool) {
        self.downlink_inflight_permits.capture.test_state()
    }
    pub(in crate::client_tun) fn test_swap_inflight_stocks(&mut self, other: &mut Self) {
        std::mem::swap(
            &mut self.downlink_inflight_permits.permits,
            &mut other.downlink_inflight_permits.permits,
        );
    }
}
