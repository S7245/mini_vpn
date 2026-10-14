use super::*;
use crate::tuic::encode_packet;

#[tokio::test]
async fn exact_seals_dispose_original_queue_and_reject_late_epoch_ack() {
    let control = Arc::new(GenerationControl::default());
    let (sender, mut source) = channel(control.clone(), 2);
    let clock = Instant::now();
    let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
    sender
        .send(1, encode_packet(7, &target, b"old"))
        .await
        .unwrap();
    assert!(control.freeze(1));
    assert!(!control.sealed(1));
    assert!(control.prepare_publication(1, 2).is_none());
    control.producer_sealed(1);
    assert_eq!(source.recv(&clock).await, Some(UdpDownlinkEvent::Ignored));
    assert_eq!(control.discarded_messages.load(Ordering::Relaxed), 1);
    assert_eq!(
        control.discarded_bytes.load(Ordering::Relaxed),
        encode_packet(7, &target, b"old").len() as u64
    );
    control.prepare_publication(1, 2).unwrap().publish();
    assert!(control.freeze(2));
    control.producer_sealed(2);
    source.recv(&clock).await;
    control.producer_sealed(1);
    control.consumer_sealed(1);
    assert!(control.sealed(2));
    assert!(!control.freeze(1));
    control.prepare_publication(2, 3).unwrap().publish();
    sender
        .send(2, encode_packet(7, &target, b"stale"))
        .await
        .unwrap();
    assert_eq!(source.recv(&clock).await, Some(UdpDownlinkEvent::Ignored));
    sender
        .send(3, encode_packet(7, &target, b"fresh"))
        .await
        .unwrap();
    assert!(matches!(
        source.recv(&clock).await,
        Some(UdpDownlinkEvent::Deliver(_))
    ));
}

#[test]
fn actual_source_destruction_and_publication_have_one_control_order() {
    let control = Arc::new(GenerationControl::default());
    let (sender, source) = channel(control.clone(), 2);
    assert!(control.freeze(1));
    control.seal_unstarted(1); // Control-only concurrency test, no producer task.
    let publication = control.prepare_publication(1, 2).unwrap();
    let (started, receiver) = std::sync::mpsc::channel();
    let thread = std::thread::spawn(move || {
        started.send(()).unwrap();
        drop(source);
    });
    receiver.recv().unwrap();
    assert!(
        !sender.is_closed(),
        "Drop cannot close receiver through held publication guard"
    );
    publication.publish();
    thread.join().unwrap();
    assert!(sender.is_closed());
    assert_eq!(control.epoch(), 2);
    assert!(control.freeze(2));
    control.producer_sealed(2);
    assert!(control.sealed(2));
    assert!(
        control.prepare_publication(2, 3).is_none(),
        "absent source never recreates UDP"
    );
}

#[tokio::test]
async fn sealed_closed_source_returns_eof_without_forced_drop() {
    let control = Arc::new(GenerationControl::default());
    let (sender, mut source) = channel(control.clone(), 2);
    control.freeze(1);
    control.producer_sealed(1);
    assert_eq!(
        source.recv(&Instant::now()).await,
        Some(UdpDownlinkEvent::Ignored)
    );
    let clock = Instant::now();
    let receive = source.recv(&clock);
    tokio::pin!(receive);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(10), &mut receive)
            .await
            .is_err()
    );
    drop(sender);
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(1), &mut receive)
            .await
            .unwrap(),
        None
    );
}

#[test]
fn poisoned_publication_guard_is_sticky_invalid() {
    let control = Arc::new(GenerationControl::default());
    let poison = control.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.publication.lock().unwrap();
            panic!("control poison");
        })
        .join()
        .is_err()
    );
    control.freeze(1);
    control.seal_unstarted(1);
    assert!(control.prepare_publication(1, 2).is_none());
    assert!(!control.valid());
}

pub(crate) fn poison_publication(control: &Arc<GenerationControl>) {
    let poison = control.clone();
    assert!(
        std::thread::spawn(move || {
            let _guard = poison.publication.lock().unwrap();
            panic!("control poison");
        })
        .join()
        .is_err()
    );
}

#[test]
fn destructing_source_cannot_reuse_previous_consumer_seal() {
    let control = GenerationControl::default();
    control.freeze(1);
    control.seal_unstarted(1);
    assert!(control.sealed(1));
    control.consumer_disposition.store(1, Ordering::Release);
    assert!(!control.sealed(1));
    control.consumer_disposition.store(2, Ordering::Release);
    assert!(control.sealed(1));
    assert!(control.prepare_publication(1, 2).is_none());
}
