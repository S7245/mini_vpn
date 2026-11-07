//! Native Root role gate interruption through the actual four task handles.
use super::*;
use crate::upstream::{
    NativeTcpChunk, NativeTcpReader, NativeTcpRelayEngine, NativeTcpRelayStream,
};
mod failure_order;
mod forced_root;
mod output_capacity;
mod protocol_tier;
mod source_first;
mod writer_q;

struct PendingNativeReader(Arc<Probe>);
impl NativeTcpReader for PendingNativeReader {
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.0.reads.fetch_add(1, Ordering::SeqCst);
        Poll::Pending
    }
}

struct MismatchedSourceReader {
    emitted: bool,
    meter: bytes::BackingMeter,
}

struct OutOfOrderSourceReader {
    chunks: std::collections::VecDeque<NativeTcpChunk>,
    meter: bytes::BackingMeter,
    source_bytes: usize,
}

struct GapPendingSourceReader {
    chunk: Option<NativeTcpChunk>,
    meter: bytes::BackingMeter,
    polls: Arc<AtomicUsize>,
    source_bytes: usize,
}
impl NativeTcpReader for GapPendingSourceReader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        Some(self.meter.clone())
    }

    fn source_read_bytes(&self) -> Option<usize> {
        Some(self.source_bytes)
    }

    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        if let Some(chunk) = self.chunk.take() {
            self.source_bytes += chunk.bytes.len();
            Poll::Ready(Ok(Some(chunk)))
        } else {
            Poll::Pending
        }
    }
}

#[tokio::test]
async fn direct_gap_cancellation_joins_and_releases_original_backing() {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let meter = bytes::BackingMeter::new();
    let chunk = metered_chunk(&meter, 4, b"efgh");
    let charged_payload = meter.snapshot().payload_bytes;
    assert!(charged_payload >= 4);
    let polls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(GapPendingSourceReader {
                chunk: Some(chunk),
                meter: meter.clone(),
                polls: polls.clone(),
                source_bytes: 0,
            }),
            Box::new(Stream { probe, fail: false }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().expect("original reader window"));
    assert!(roles.commit_native());
    roles.start();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while polls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        meter.snapshot().payload_bytes,
        charged_payload,
        "same backing remains owned before stop"
    );
    assert!(back.try_recv().is_err(), "gap cannot publish DATA");
    drop(tx);
    roles.stop();
    until(&mut roles, true, FlowRoles::settled).await;
    assert_eq!(roles.join_count(), 4);
    assert_eq!(roles.native_reader_received_bytes(), Some(4));
    assert_eq!(roles.native_direct_source_received_bytes(), Some(4));
    assert!(!roles.quarantined());
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().payload_bytes, 0);
}
impl NativeTcpReader for OutOfOrderSourceReader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        Some(self.meter.clone())
    }

    fn source_read_bytes(&self) -> Option<usize> {
        Some(self.source_bytes)
    }

    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        let chunk = self.chunks.pop_front();
        if let Some(chunk) = &chunk {
            assert!(chunk.bytes.len() <= max_len);
            self.source_bytes += chunk.bytes.len();
        }
        Poll::Ready(Ok(chunk))
    }
}

fn metered_chunk(meter: &bytes::BackingMeter, offset: u64, payload: &[u8]) -> NativeTcpChunk {
    let mut backing = bytes::BytesMut::with_capacity(32);
    backing.extend_from_slice(payload);
    let chunk = backing.split_to(payload.len());
    assert!(chunk.charge_shared_backing(meter));
    drop(backing);
    NativeTcpChunk {
        offset,
        bytes: chunk.freeze(),
    }
}

#[tokio::test]
async fn direct_out_of_order_chunks_keep_original_backing_until_root_join() {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let meter = bytes::BackingMeter::new();
    let chunks = [
        metered_chunk(&meter, 4, b"efgh"),
        metered_chunk(&meter, 0, b"abcd"),
    ]
    .into_iter()
    .collect();
    assert_eq!(meter.snapshot().allocations, 2);
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, mut back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(OutOfOrderSourceReader {
                chunks,
                meter: meter.clone(),
                source_bytes: 0,
            }),
            Box::new(Stream { probe, fail: false }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().expect("original reader window"));
    assert!(roles.commit_native());
    roles.start();
    until(&mut roles, false, |roles| {
        roles.native_reader_received_bytes() == Some(8)
    })
    .await;
    assert_eq!(roles.native_reader_received_bytes(), Some(8));
    loop {
        let (_, event) = tokio::time::timeout(std::time::Duration::from_secs(1), back.recv())
            .await
            .unwrap()
            .unwrap();
        if let RelayEvent::Data {
            epoch,
            bytes,
            downstream_permit,
            ..
        } = event
        {
            assert_eq!(epoch, 7);
            assert_eq!(bytes, b"abcdefgh");
            assert!(
                downstream_permit.is_none(),
                "ChunkPump has no egress permit"
            );
            window.admit(bytes.len(), 0, bytes.len());
            window.publish();
            break;
        }
    }
    drop(tx);
    roles.stop();
    until(&mut roles, true, FlowRoles::settled).await;
    assert_eq!(roles.native_direct_source_received_bytes(), Some(8));
    assert_eq!(roles.join_count(), 4);
    assert!(!roles.quarantined());
    assert_eq!(meter.snapshot().allocations, 0);
    assert_eq!(meter.snapshot().payload_bytes, 0);
    assert!(!meter.snapshot().quarantined);
}

#[tokio::test]
async fn direct_gap_pressure_stops_at_original_cap_and_keeps_backing_charged_q() {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let meter = bytes::BackingMeter::new();
    let chunk_size = 64 * 1024;
    let cap = crate::client_tun::NATIVE_TCP_REASSEMBLY_MAX_BYTES;
    assert_eq!(cap % chunk_size, 0);
    let payload = vec![0x5a; chunk_size];
    let chunks = (0..cap / chunk_size)
        .map(|index| metered_chunk(&meter, (cap + index * chunk_size) as u64, &payload))
        .collect();
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(OutOfOrderSourceReader {
                chunks,
                meter: meter.clone(),
                source_bytes: 0,
            }),
            Box::new(Stream { probe, fail: false }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().expect("original reader window"));
    assert!(roles.commit_native());
    roles.start();
    until(&mut roles, false, |roles| {
        roles.native_reader_received_bytes() == Some(cap)
    })
    .await;
    assert_eq!(
        roles.closed(),
        Some(("remote_to_local", "native_reassembly_cap"))
    );
    drop(tx);
    roles.stop();
    until(&mut roles, true, |roles| roles.join_count() == 4).await;
    assert!(roles.quarantined());
    assert!(!roles.settled());
    assert_eq!(roles.native_reader_received_bytes(), Some(cap));
    assert_eq!(meter.snapshot().payload_bytes, cap);
    assert_eq!(meter.snapshot().allocations, cap / chunk_size);
}
impl NativeTcpReader for MismatchedSourceReader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> { Some(self.meter.clone()) }
    fn source_read_bytes(&self) -> Option<usize> {
        Some(0)
    }

    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        if self.emitted {
            Poll::Ready(Ok(None))
        } else {
            self.emitted = true;
            Poll::Ready(Ok(Some(metered_chunk(&self.meter, 0, b"data"))))
        }
    }
}

#[tokio::test]
async fn direct_source_receipt_mismatch_quarantines_after_actual_reader_join() {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(MismatchedSourceReader { emitted: false, meter: bytes::BackingMeter::new() }),
            Box::new(Stream { probe, fail: false }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().expect("original reader window"));
    assert!(roles.commit_native());
    roles.start();
    until(&mut roles, false, |roles| {
        roles.native_reader_received_bytes() == Some(4)
    })
    .await;
    drop(tx);
    roles.stop();
    until(&mut roles, true, |roles| roles.join_count() == 4 && roles.native_reader_receipt_present() == Some(true)).await;
    assert_eq!(roles.native_reader_received_bytes(), Some(4));
    assert_eq!(roles.native_direct_source_received_bytes(), Some(0));
    assert!(
        roles.quarantined(),
        "source/Root byte disagreement must keep Q"
    );
    assert!(!roles.settled());
}

#[tokio::test]
async fn missing_native_start_gate_is_sticky_before_any_join_poll() {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let probe = Arc::new(Probe::default());
    let (tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native: NativeTcpRelayStream::new(
            Box::new(PendingNativeReader(probe.clone())),
            Box::new(Stream {
                probe: probe.clone(),
                fail: false,
            }),
        ),
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new(
        );
    roles.prepare(window.take_reader().expect("original reader window"));
    assert!(roles.commit_native());
    roles.drop_native_start_gate_for_test(1);
    roles.start();
    assert!(
        roles.quarantined(),
        "failed publication gate must be sticky Q"
    );
    drop(tx);
    roles.stop();
    until(&mut roles, false, |roles| roles.join_count() == 4).await;
    assert_eq!(probe.reads.load(Ordering::SeqCst), 0);
    assert!(!roles.settled());
}

impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_native_terminal_for_test(
        &self,
    ) -> Option<(&'static str, &'static str)> {
        self.native.as_ref()?.retained_terminal_for_test()
    }
}

impl FlowRoles {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_native_permits_for_test(
        &self,
        identity: &crate::tcp_downlink_pump::PermitQueueIdentity,
    ) -> Option<(SocketHandle, u64, usize, usize, usize)> {
        self.native.as_ref()?.retained_permits_for_test(identity)
    }
}
