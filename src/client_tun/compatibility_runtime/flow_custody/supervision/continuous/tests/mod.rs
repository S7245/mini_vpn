//! Joined dispatcher validates physical source identity before a clean receipt.
use super::*;
use crate::tcp_downlink_pump::OwnedPayload;

#[tokio::test]
async fn cold_segment_rejects_equal_bytes_with_wrong_identity_or_slice_origin() {
    for fault in 0..4 {
        let budget = bytes::BackingBudget::new(65536).unwrap();
        let meter = bytes::BackingMeter::with_budget(&budget);
        let shared = Shared::configured_metered(
            TcpRelayEngine::ContinuousPump,
            DownlinkBackpressureConfig::default(),
            Some(meter.clone()),
        )
        .unwrap();
        let original = bytes::Bytes::copy_from_slice_metered(b"xxxx", &meter).unwrap();
        let mut pending = Some(OwnedPayload::Native((original.clone()).into()));
        shared.queue.push_custodied(&mut pending).await.unwrap();
        let mut held = Held::new(&shared.queue);
        shared.queue.recv_custodied(4, &mut held).await.unwrap();
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 8]),
        ));
        let mut window = super::super::super::receive_window::ReceiveWindow::new();
        let (start, gate) = oneshot::channel();
        drop(start);
        let mut state = run(Request {
            handle,
            epoch: 7,
            shared,
            back: None,
            signals: None,
            window: window.take_reader().unwrap(),
            stop: None,
            gate,
        })
        .await;
        state.held = held;
        state.consumed = PublishedReceipt {
            messages: 1,
            bytes: 4,
        };
        state.segment = Some(match fault {
            0 => OwnedPayload::Legacy(b"xx".to_vec()),
            1 => {
                OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"xx", &meter).unwrap()).into())
            }
            2 => OwnedPayload::Native((original.slice(1..3)).into()),
            _ => OwnedPayload::Native((original.slice(..2)).into()),
        });
        state.dispose(&mut Context::from_waker(std::task::Waker::noop()));
        assert_eq!(state.receipt.is_some(), fault == 3);
        assert_eq!(state.quarantined(), fault != 3);
        drop((state, original));
        assert_eq!(budget.used(), usize::from(fault != 3));
    }
}

// Copy the original fixed terminal only; never consume it or mint delivery.
impl Shared {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_terminal_for_test(
        &self,
    ) -> Option<(&'static str, &'static str)> {
        let slot = self.terminal.lock().ok()?;
        if !slot.emitted || slot.taken || slot.interrupted {
            return None;
        }
        match slot.signal.as_ref()? {
            RelayReaderSignal::Closed {
                direction, reason, ..
            } => Some((*direction, *reason)),
            _ => None,
        }
    }
}

impl DispatchOutcome {
    pub(in crate::client_tun::compatibility_runtime::flow_custody) fn retained_permits_for_test(
        &self,
        identity: &crate::tcp_downlink_pump::PermitQueueIdentity,
    ) -> Option<(SocketHandle, u64, usize, usize, usize)> {
        let Self::Continuous(value) = self else {
            return None;
        };
        let queue::Backend::Leased(queue) = &value.request.shared.queue else {
            return None;
        };
        assert!(identity.matches_queue(queue));
        let held = match &value.held {
            queue::Held::Leased(Some(batch)) => {
                assert!(identity.matches(
                    &batch.permit,
                    crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain
                ));
                batch.permit.remaining_bytes()
            }
            queue::Held::Leased(None) => 0,
            queue::Held::Bytes(_) => return None,
        };
        let segment = value.segment_permit.as_ref().map_or(0, |permit| {
            assert!(identity.matches(
                permit,
                crate::tcp_downlink_pump::DownstreamPermitReleaseMode::OnEgressDrain
            ));
            assert_eq!(
                value.segment.as_ref().unwrap().len(),
                permit.remaining_bytes()
            );
            permit.remaining_bytes()
        });
        let unpublished = value.held.as_ref().map_or(0, |batch| {
            batch.bytes.len().checked_sub(value.cursor).unwrap()
        });
        assert_eq!(held.checked_add(segment), Some(unpublished));
        Some((
            value.request.handle,
            value.request.epoch,
            held,
            segment,
            value.published.bytes,
        ))
    }
}
