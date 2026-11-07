//! Original ordered pump fault inventory across actual Native role joins.
//! Owns only test Control faults; production Root/Service acceptance lives in
//! tuic_service/tests. The same failure clock orders pump and reader faults.

use super::*;
use crate::upstream::{
    NativePumpControl, NativePumpFailure, NativePumpFailureValue, NativePumpFinished,
    NativePumpReceipt, NativePumpWork, PreparedNativePump, RunningNativePump,
};
use std::any::Any;
use std::collections::VecDeque;
use std::mem::ManuallyDrop;
use std::sync::Mutex;

struct Source {
    next: usize,
    meter: bytes::BackingMeter,
    polls: Arc<AtomicUsize>,
}
impl NativeTcpReader for Source {
    fn receive_backing_meter(&self) -> Option<bytes::BackingMeter> {
        Some(self.meter.clone())
    }
    fn source_read_bytes(&self) -> Option<usize> {
        Some(4)
    }
    fn poll_read_chunk(
        &mut self,
        _: &mut Context<'_>,
        _: usize,
    ) -> Poll<std::io::Result<Option<NativeTcpChunk>>> {
        self.next += 1;
        self.polls.fetch_add(1, Ordering::SeqCst);
        match self.next {
            1 => Poll::Ready(Ok(Some(NativeTcpChunk {
                offset: 4,
                bytes: bytes::Bytes::copy_from_slice_metered(b"gap!", &self.meter).unwrap(),
            }))),
            _ => Poll::Ready(Err(std::io::Error::from_raw_os_error(54))),
        }
    }
}

struct PanicDrop(Arc<AtomicUsize>);
impl Drop for PanicDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Control {
    faults: Mutex<VecDeque<NativePumpFailure>>,
    quarantine: std::sync::atomic::AtomicBool,
}
impl NativePumpControl for Control {
    fn stop(&self) {}
    fn quarantined(&self) -> bool {
        self.quarantine.load(Ordering::SeqCst)
    }
    fn quarantine(&self) {
        self.quarantine.store(true, Ordering::SeqCst);
    }
    fn unwind(&self, _: Box<dyn Any + Send>) {
        self.quarantine();
    }
    fn take_failure(&self) -> Option<NativePumpFailure> {
        self.faults.lock().unwrap().pop_front()
    }
    fn error_order(&self) -> Option<usize> {
        self.faults.lock().unwrap().front().and_then(|fault| fault.order)
    }
    fn source_read_bytes(&self) -> usize {
        0
    }
}

struct PendingDispose;
impl NativePumpFinished for PendingDispose {
    fn poll_dispose(&mut self, _: &mut Context<'_>) -> Poll<NativePumpReceipt> {
        Poll::Pending
    }
}

struct Work {
    faults: Vec<NativePumpFailureValue>,
}
impl NativePumpWork for Work {
    fn commit(
        self: Box<Self>,
        clock: Option<crate::shared::errors::FailureClock>,
        gate: Option<tokio::sync::oneshot::Receiver<()>>,
    ) -> RunningNativePump {
        let clock = clock.expect("original Root clock");
        let faults = self
            .faults
            .into_iter()
            .map(|value| {
                let order = clock.stamp();
                NativePumpFailure::new(value, Some(order))
            })
            .collect();
        let control = Arc::new(Control {
            faults: Mutex::new(faults),
            quarantine: std::sync::atomic::AtomicBool::new(false),
        });
        let task = tokio::spawn(async move {
            let _ = gate.expect("original start gate").await;
            ManuallyDrop::new(Box::new(PendingDispose) as Box<dyn NativePumpFinished>)
        });
        RunningNativePump::new(task, control)
    }
}

pub(super) async fn joined_roles(faults: Vec<NativePumpFailureValue>) -> FlowRoles {
    let mut sockets = SocketSet::new(Vec::new());
    let handle = sockets.add(Socket::new(
        SocketBuffer::new(vec![0; 8]),
        SocketBuffer::new(vec![0; 8]),
    ));
    let probe = Arc::new(Probe::default());
    let (_tx, rx) = crate::client_tun::uplink::channel(2);
    let (back_tx, _back) = crate::client_tun::relay_channel::channel(2);
    let (_credit, read_credit_rx) = watch::channel(RelayReadCredit::default());
    let polls = Arc::new(AtomicUsize::new(0));
    let source_meter = bytes::BackingMeter::new();
    let native = NativeTcpRelayStream::new(
        Box::new(Source {
            next: 0,
            meter: source_meter.clone(),
            polls: polls.clone(),
        }),
        Box::new(Stream { probe, fail: false }),
    )
    .with_prepared_pump(PreparedNativePump::new(Box::new(Work { faults })));
    let mut roles = FlowRoles::new_native(NativeLaunchRequest {
            uplink_observer: None,
        handle,
        epoch: 7,
        native,
        engine: NativeTcpRelayEngine::ChunkPump,
        read_floor: false,
        rx,
        back_tx,
        read_credit_rx,
        downlink_backpressure: DownlinkBackpressureConfig::default(),
    })
    .ok()
    .expect("valid Native ordered launch");
    let mut window =
        crate::client_tun::compatibility_runtime::flow_custody::receive_window::ReceiveWindow::new();
    roles.prepare(window.take_reader().unwrap());
    assert!(roles.commit_native());
    roles.start();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while polls.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original reader reached the later IO");
    // Explicit terminal uncertainty after both IO ordinals, not an invalid
    // unmetered DATA record that admission must now reject before the second IO.
    source_meter.quarantine();
    roles.stop();
    until(&mut roles, true, FlowRoles::all_original_roles_joined).await;
    assert_eq!(roles.join_count(), 5);
    assert_eq!(roles.native_reader_receipt_present(), Some(false));
    assert!(roles.quarantined());
    assert!(!roles.settled());
    roles
}

#[tokio::test]
async fn joined_pump_io_precedes_reader_io_without_reader_physical_receipt() {
    let mut roles = joined_roles(vec![NativePumpFailureValue::Io(
        std::io::Error::from_raw_os_error(5),
    )])
    .await;
    let first = roles.take_original_failure().expect("original pump IO");
    let super::super::super::roles::OriginalFailure::Io(error) = first else {
        panic!("pump IO must be first");
    };
    assert_eq!(error.raw_os_error(), Some(5));
    let second = roles.take_original_failure().expect("later reader IO");
    let super::super::super::roles::OriginalFailure::Io(error) = second else {
        panic!("reader IO must follow");
    };
    assert_eq!(error.raw_os_error(), Some(54));
    assert!(roles.take_original_failure().is_none());
}

#[tokio::test]
async fn joined_pump_two_panics_keep_both_original_boxes_before_reader_io() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut roles = joined_roles(vec![
        NativePumpFailureValue::Panic(Box::new(PanicDrop(drops.clone()))),
        NativePumpFailureValue::Panic(Box::new(PanicDrop(drops.clone()))),
    ])
    .await;
    for _ in 0..2 {
        assert!(matches!(
            roles.take_original_failure(),
            Some(super::super::super::roles::OriginalFailure::OpaquePanic)
        ));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
    }
    let third = roles.take_original_failure().expect("later reader IO");
    let super::super::super::roles::OriginalFailure::Io(error) = third else {
        panic!("reader IO must follow both panics");
    };
    assert_eq!(error.raw_os_error(), Some(54));
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    assert!(roles.take_original_failure().is_none());
}
