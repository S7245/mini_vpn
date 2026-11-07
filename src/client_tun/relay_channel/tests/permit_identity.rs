//! A typed payload cannot lend Native callback safety to a foreign raw permit.
use super::*;
use crate::{
    client_tun::{RelayEvent, SocketHandle, StreamServiceWindow},
    tcp_downlink_pump::*,
};
use std::future::Future;
#[test]
fn actual_legacy_permit_cannot_smuggle_raw_notify_into_native_drain() {
    const CHILD: &str = "N1_NATIVE_FOREIGN_PERMIT_CHILD";
    if std::env::var_os(CHILD).is_some() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let budget = bytes::BackingBudget::new(1 << 22).unwrap();
                let meter = BackingMeter::with_budget(&budget);
                let (clock, facts) = clock(&meter);
                clock.io(&std::io::Error::from_raw_os_error(5));
                let raw = AsyncLeasedByteFlowQueue::new_with_release_mode(
                    3,
                    DownstreamPermitReleaseMode::OnEgressDrain,
                )
                .unwrap();
                raw.push(bytes::Bytes::from_static(b"abc")).await.unwrap();
                let (bytes, permit) = raw.recv_up_to(3).await.unwrap().into_parts();
                drop(bytes);
                let mut wait = Box::pin(raw.wait_read_len(1));
                let waker = Waker::from(Arc::new(Twice));
                assert!(
                    wait.as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                );
                drop(waker);
                // The deliberately hostile original registration must remain alive
                // through the invalid event's quarantine, not be cancelled in cleanup.
                std::mem::forget(wait);
                let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
                let handle: SocketHandle = sockets.add(smoltcp::socket::tcp::Socket::new(
                    smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
                    smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
                ));
                let (tx, rx, observer) =
                    native_channel::<(SocketHandle, RelayEvent)>(1, &meter, clock).unwrap();
                tx.reserve().await.unwrap().send((
                    handle,
                    RelayEvent::Data {
                        epoch: 1,
                        bytes: OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"abc", &meter).unwrap()).into()),
                        stream_service_window: StreamServiceWindow::default(),
                        downstream_permit: Some(permit),
                    },
                ));
                drop(rx); // Before the fix: actual raw Notify double unwind SIGABRT.
                assert!(budget.quarantined());
                assert_eq!(
                    raw.snapshot_now().leased_bytes,
                    3,
                    "invalid original permit stays in actual lane custody"
                );
                let ledger = facts.0.lock().unwrap();
                assert_eq!(ledger.len(), 2);
                assert_eq!(ledger[0].facts().raw_os_code, Some(5));
                assert_eq!(
                    ledger[1].facts().io_kind,
                    Some(std::io::ErrorKind::InvalidData)
                );
                drop(ledger);
                assert_eq!(unsafe { observer.inventory_for_test() }, Some((1, 3)));
                drop((tx, observer, raw));
                assert!(budget.used() > 1);
            });
        return;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "client_tun::relay_channel::tests::permit_identity::actual_legacy_permit_cannot_smuggle_raw_notify_into_native_drain", "--nocapture"])
        .env(CHILD, "1").env("RUST_BACKTRACE", "0").output().unwrap();
    eprintln!(
        "foreign permit child: {:?}\n{}\n{}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        output.status.success(),
        "must preserve original invalid permit without invoking its raw callback"
    );
}

#[tokio::test]
async fn actual_native_permit_budget_and_retired_root_identity_are_checked() {
    for foreign in [false, true] {
        let budget = bytes::BackingBudget::new(1 << 22).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let other = bytes::BackingBudget::new(65536).unwrap();
        let queue_meter = if foreign {
            BackingMeter::with_budget(&other)
        } else {
            meter.clone()
        };
        let (queue_clock, _queue_facts) = clock(&queue_meter);
        let queue = AsyncLeasedByteFlowQueue::new_custodied_with_mode(
            3,
            DownstreamPermitReleaseMode::OnEgressDrain,
        )
        .unwrap();
        queue.bind_native_meter(Some(queue_meter.clone())).unwrap();
        assert!(queue.bind_native_failures(queue_clock));
        queue
            .push_custodied(&mut Some(OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"abc", &queue_meter).unwrap()).into())))
            .await
            .unwrap();
        let mut held = None;
        queue.recv_custodied(3, &mut held).await.unwrap();
        let batch = held.unwrap();
        drop(batch.data);
        assert!(
            queue.dispose_custodied().unwrap().is_some(),
            "Control/roles can retire before old Main permit"
        );
        let (clock, _facts) = clock(&meter);
        let mut sockets = smoltcp::iface::SocketSet::new(vec![]);
        let handle = sockets.add(smoltcp::socket::tcp::Socket::new(
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
            smoltcp::socket::tcp::SocketBuffer::new(vec![0; 1]),
        ));
        let (tx, rx, observer) =
            native_channel::<(SocketHandle, RelayEvent)>(1, &meter, clock).unwrap();
        tx.reserve().await.unwrap().send((
            handle,
            RelayEvent::Data {
                epoch: 1,
                bytes: OwnedPayload::Native((bytes::Bytes::copy_from_slice_metered(b"abc", &meter).unwrap()).into()),
                stream_service_window: StreamServiceWindow::default(),
                downstream_permit: Some(batch.permit),
            },
        ));
        drop((rx, tx));
        if foreign {
            assert!(budget.quarantined());
            assert_eq!(queue.snapshot_now().leased_bytes, 3);
            assert!(!observer.settled());
            drop((observer, queue));
            assert!(budget.used() > 1 && other.used() > 0);
        } else {
            assert!(queue.physical_zero());
            assert!(observer.finish().is_ok());
            drop(queue);
            assert_eq!(budget.used(), 0);
            assert!(!budget.quarantined());
        }
    }
}
