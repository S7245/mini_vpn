//! Both actual legacy Native reader entries obey the original logical cap.
use super::*;
use crate::upstream::NativeTcpReader;
use std::{
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};

struct Gap {
    bytes: Bytes,
    emitted: Arc<AtomicUsize>,
}
impl NativeTcpReader for Gap {
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        max_len: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        let before = self.emitted.load(Ordering::SeqCst);
        if before > NATIVE_TCP_REASSEMBLY_MAX_BYTES {
            return Poll::Ready(Ok(None));
        }
        let n = max_len
            .min(self.bytes.len())
            .min(NATIVE_TCP_REASSEMBLY_MAX_BYTES + 1 - before);
        self.emitted.fetch_add(n, Ordering::SeqCst);
        Poll::Ready(Ok(Some(NativeTcpChunk {
            offset: 1 + before as u64,
            bytes: self.bytes.slice(..n),
        })))
    }
}

#[tokio::test]
async fn both_real_legacy_readers_close_at_cap_without_one_extra_byte() {
    for permit in [false, true] {
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        ));
        let emitted = Arc::new(AtomicUsize::new(0));
        let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
        let (signal_tx, mut signals) = mpsc::channel(128);
        let (_stop, stop_rx) = tokio::sync::oneshot::channel();
        let request = Request {
            handle,
            epoch: 7,
            remote_reader: Box::new(Gap {
                bytes: Bytes::from(vec![0; 65536]),
                emitted: emitted.clone(),
            }),
            read_credit_rx,
            signal_tx,
            local_finish_seen: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            stop_rx,
            native_read_service_floor: false,
        };
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            if permit {
                run_permit(
                    request,
                    AsyncLeasedByteFlowQueue::new(NATIVE_TCP_REASSEMBLY_MAX_BYTES).unwrap(),
                )
                .await;
            } else {
                run_chunk(
                    request,
                    AsyncByteBoundedFlowQueue::new(NATIVE_TCP_REASSEMBLY_MAX_BYTES).unwrap(),
                )
                .await;
            }
        })
        .await
        .unwrap();
        let mut reason = None;
        while let Ok(signal) = signals.try_recv() {
            if let RelayReaderSignal::Closed { reason: r, .. } = signal {
                reason = Some(r);
            }
        }
        assert_eq!(
            emitted.load(Ordering::SeqCst),
            NATIVE_TCP_REASSEMBLY_MAX_BYTES,
            "permit={permit}"
        );
        assert_eq!(reason, Some("native_reassembly_cap"));
    }
}

struct HardReader {
    meter: bytes::BackingMeter,
    bytes: Bytes,
    polled: Arc<AtomicUsize>,
}
impl NativeTcpReader for HardReader {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        Some(self.meter.clone())
    }
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.polled.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(Ok(Some(NativeTcpChunk {
            offset: 0,
            bytes: self.bytes.clone(),
        })))
    }
}

#[tokio::test]
async fn all_three_actual_legacy_launchers_refuse_hard_capability_before_read() {
    for entry in 0..3 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let polled = Arc::new(AtomicUsize::new(0));
        let native = crate::upstream::NativeTcpRelayStream::new(
            Box::new(HardReader {
                bytes: Bytes::copy_from_slice_metered(b"cold", &meter).unwrap(),
                meter: meter.clone(),
                polled: polled.clone(),
            }),
            Box::new(tokio::io::sink()),
        );
        let mut sockets = SocketSet::new(vec![]);
        let handle = sockets.add(TcpSocket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        ));
        let (_commands, rx) = crate::client_tun::uplink::channel(1);
        let (back_tx, mut back) = crate::client_tun::relay_channel::channel(1);
        let (_credit, credit) = watch::channel(RelayReadCredit::default());
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            match entry {
                0 => run_relay_native_chunk(handle, 7, native, rx, back_tx, credit, false).await,
                1 => {
                    run_relay_native_permit(handle, 7, native, rx, back_tx, credit, 65536, false)
                        .await
                }
                _ => {
                    crate::client_tun::d16_reader::run_relay_d16(
                        handle,
                        7,
                        native,
                        rx,
                        back_tx,
                        credit,
                        AsyncLeasedByteFlowQueue::new(65536).unwrap(),
                    )
                    .await
                }
            }
        })
        .await
        .unwrap();
        let (_, RelayEvent::Closed(close)) = back.recv().await.unwrap() else {
            panic!("explicit refusal")
        };
        assert_eq!(close.reason, "native_hard_budget_requires_saved_root");
        assert_eq!(close.epoch, 7);
        assert_eq!(polled.load(Ordering::SeqCst), 0);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
