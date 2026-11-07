use super::*;
use crate::shared::errors::*;
struct Facts;
impl QuietFailureSink for Facts {
    fn record_quiet(&self, _: RetainedFailure) {}
}
struct Fixture {
    control: Arc<GenerationControl>,
    meter: BackingMeter,
    callbacks: Callbacks,
    clock: FailureClock,
    _facts: Arc<dyn QuietFailureSink>,
    budget: bytes::BackingBudget,
}
impl Fixture {
    fn new() -> Self {
        let budget = bytes::BackingBudget::new(16 << 20).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let clock = Arc::new(FailureOrder::default());
        let facts: Arc<dyn QuietFailureSink> = Arc::new(Facts);
        clock
            .bind(
                Arc::downgrade(&facts),
                FailureOrigin {
                    phase: FailurePhase::UdpJoin,
                    resource: FailureResource::UdpDriver,
                    resource_id: 0,
                    attempt_id: None,
                },
                Some(meter.clone()),
            )
            .unwrap();
        let callbacks = Callbacks::prepare(&meter, clock.clone()).unwrap();
        Self {
            control: Arc::new(GenerationControl::default()),
            meter,
            callbacks,
            clock,
            _facts: facts,
            budget,
        }
    }
    fn lane(&self) -> (GenerationSender, UdpDownlinkSource, Observer) {
        prepare(
            self.control.clone(),
            &self.meter,
            self.clock.clone(),
            &self.callbacks,
        )
        .unwrap()
    }
    fn finish(self) {
        let budget = self.budget.clone();
        drop(self);
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
#[tokio::test]
async fn actual_native_source_eof_retires_before_source_drop() {
    let f = Fixture::new();
    let (sender, mut source, observer) = f.lane();
    drop(sender);
    assert_eq!(source.recv(&Instant::now()).await, None);
    assert!(
        observer.settled(),
        "observing EOF must physically retire the receiver"
    );
    assert!(observer.finish().is_ok());
    assert_eq!(f.control.consumer_disposition.load(Ordering::Acquire), 2);
    assert!(
        f.callbacks.quiescent(),
        "EOF must also release the original Source role"
    );
    drop(source);
    f.finish();
}
#[tokio::test]
async fn original_granted_permit_tail_stays_present_until_real_lane_retirement() {
    let f = Fixture::new();
    let (sender, source, observer) = f.lane();
    let tx = &sender.native.as_ref().unwrap().0;
    let permit = tx.reserve().await.unwrap();
    drop(source);
    assert_eq!(f.control.consumer_disposition.load(Ordering::Acquire), 1);
    assert!(!observer.settled());
    let bytes = Bytes::copy_from_slice_metered(b"original tail", &f.meter).unwrap();
    let size = bytes.len();
    permit.send(Frame { epoch: 1, bytes });
    assert_eq!(f.control.discarded_messages.load(Ordering::Acquire), 1);
    assert_eq!(
        f.control.discarded_bytes.load(Ordering::Acquire),
        size as u64
    );
    assert_eq!(f.control.consumer_disposition.load(Ordering::Acquire), 1);
    drop(sender);
    assert!(observer.finish().is_ok());
    assert_eq!(f.control.consumer_disposition.load(Ordering::Acquire), 2);
    f.finish();
}

fn frame(meter: &BackingMeter, id: u8, bytes: &[u8]) -> Bytes {
    let mut wire = vec![5, 2, 0, 7, 0, 3, 2, id];
    wire.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    wire.push(0xff);
    wire.extend_from_slice(bytes);
    Bytes::copy_from_slice_metered(&wire, meter).unwrap()
}
#[tokio::test]
async fn native_source_capacity_drops_only_one_frame_and_preserves_partial() {
    let f = Fixture::new();
    let (sender, mut source, observer) = f.lane();
    sender.send(1, frame(&f.meter, 0, b"old")).await.unwrap();
    assert_eq!(
        source.recv(&Instant::now()).await,
        Some(UdpDownlinkEvent::Ignored)
    );
    let tail = frame(&f.meter, 1, b"tail");
    let filler = f
        .meter
        .reserve_cold_layout(f.budget.limit() - f.budget.used())
        .unwrap();
    sender.send(1, tail).await.unwrap();
    assert_eq!(
        source.recv(&Instant::now()).await,
        Some(UdpDownlinkEvent::Ignored)
    );
    assert_eq!(f.control.capacity_drops.load(Ordering::Acquire), 1);
    assert!(!f.budget.quarantined());
    drop(filler);
    sender.send(1, frame(&f.meter, 1, b"tail")).await.unwrap();
    let Some(UdpDownlinkEvent::Deliver(payload)) = source.recv(&Instant::now()).await else {
        panic!("original partial must survive pressure");
    };
    assert_eq!(payload.payload().as_ref(), b"oldtail");
    assert!(
        payload
            .payload()
            .backing_meter()
            .unwrap()
            .same_authority(&f.meter)
    );
    drop(payload);
    assert!(f.control.freeze(1));
    f.control.producer_sealed(1);
    assert_eq!(
        source.recv(&Instant::now()).await,
        Some(UdpDownlinkEvent::Ignored)
    );
    f.control.prepare_publication(1, 2).unwrap().publish(); // same original channel
    drop((source, sender));
    assert!(observer.finish().is_ok());
    f.finish();
}
#[tokio::test]
async fn native_source_quarantine_is_terminal_and_keeps_original_stock() {
    let f = Fixture::new();
    let (sender, mut source, observer) = f.lane();
    f.meter.quarantine();
    assert_eq!(source.recv(&Instant::now()).await, None);
    assert_eq!(source.recv(&Instant::now()).await, None);
    drop((source, sender, observer));
    assert!(f.budget.used() > 0);
    assert!(f.budget.quarantined());
}

#[tokio::test]
async fn original_deliver_completed_before_q_stays_in_actual_source_box() {
    let f = Fixture::new();
    let (sender, mut source, observer) = f.lane();
    let mut wire = vec![5, 2, 0, 7, 0, 3, 1, 0, 0, 7, 0xff];
    wire.extend_from_slice(b"payload");
    let bytes = Bytes::copy_from_slice_metered(&wire, &f.meter).unwrap();
    let identity = bytes.allocation_identity();
    sender.send(1, bytes).await.unwrap();
    let UdpDownlinkSourceInner::Native(original) = &mut source.inner else {
        unreachable!()
    };
    original.root.quarantine_ready = true; // inject at actual Work Ready/Q fence
    assert_eq!(source.recv(&Instant::now()).await, None);
    let UdpDownlinkSourceInner::Native(original) = &source.inner else {
        unreachable!()
    };
    let Some(UdpDownlinkEvent::Deliver(deliver)) = &*original.root.held else {
        panic!("original completed output must remain physically held");
    };
    assert_eq!(deliver.payload().as_ref(), b"payload");
    assert_eq!(deliver.payload().allocation_identity(), identity);
    assert!(f.control.freeze(1));
    f.control.producer_sealed(1); // Q never fabricates an actual producer witness
    assert_eq!(f.control.producer.load(Ordering::Acquire), 0);
    assert!(!f.control.sealed(1));
    drop((source, sender, observer));
    assert!(f.budget.quarantined());
    assert!(f.budget.used() > 0);
}

#[tokio::test]
async fn frozen_and_sealed_native_source_observes_actual_sender_eof() {
    let f = Fixture::new();
    let (sender, mut source, observer) = f.lane();
    assert!(f.control.freeze(1));
    f.control.producer_sealed(1);
    assert_eq!(
        source.recv(&Instant::now()).await,
        Some(UdpDownlinkEvent::Ignored)
    );
    drop(sender);
    let eof = tokio::time::timeout(
        std::time::Duration::from_millis(50),
        source.recv(&Instant::now()),
    )
    .await;
    // Always dispose the exact Source even when the EOF assertion is RED.
    drop(source);
    assert!(observer.finish().is_ok());
    f.finish();
    assert_eq!(
        eof,
        Ok(None),
        "a frozen sealed receiver must not wait forever after actual sender EOF"
    );
}
