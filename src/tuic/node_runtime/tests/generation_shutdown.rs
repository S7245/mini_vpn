use super::super::*;
use crate::owned_upstream::generation::channel;

#[tokio::test(flavor = "current_thread")]
async fn unknown_join_freezes_queued_delivery_until_actual_sender_eof() {
    let runtime = Arc::new(UdpRuntime::default());
    let (sender, mut source) = channel(runtime.generation.clone(), 2);
    let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
    sender
        .send(1, encode_packet(7, &target, b"queued old payload"))
        .await
        .unwrap();
    let retained = sender.clone();
    let task = tokio::spawn(async move {
        let _sender = retained;
        std::future::pending::<UdpOutcome>().await
    });
    task.abort();
    {
        let mut state = runtime.state();
        state.started = true;
        state.typed = true;
        state.task = Some(task);
    }
    let mut shutdown = UdpShutdown(runtime.clone());
    assert_eq!(
        shutdown.drive_shutdown().await,
        UdpShutdownState::Quarantined
    );
    assert_eq!(runtime.settle_source().await, Ok(false));
    assert!(runtime.generation.frozen(1));
    assert!(!runtime.generation.sealed(1));
    let clock = Instant::now();
    {
        let receive = source.recv(&clock);
        tokio::pin!(receive);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut receive)
                .await
                .is_err()
        );
        drop(sender);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut receive)
                .await
                .unwrap(),
            None
        );
    }
    assert_eq!(source.recv(&clock).await, None);
    assert!(!runtime.generation.sealed(1));
    assert!(runtime.generation.prepare_publication(1, 2).is_none());
    assert_eq!(
        shutdown.drive_shutdown().await,
        UdpShutdownState::Quarantined
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unknown_outer_join_never_certifies_generation_producer() {
    // Real Tokio outer errors through the original saved join path. This is a
    // control-evidence tracer, not a substitute for physical Quinn acceptance.
    for panic in [false, true] {
        let runtime = Arc::new(UdpRuntime::default());
        let (sender, source) = channel(runtime.generation.clone(), 2);
        let task: JoinHandle<UdpOutcome> = tokio::spawn(async move {
            let _sender = sender;
            if panic {
                panic!("unknown outer UDP task");
            }
            std::future::pending().await
        });
        if !panic {
            task.abort(); // Fault injection only; never a cleanup strategy.
        }
        {
            let mut state = runtime.state();
            state.started = true;
            state.typed = true;
            state.task = Some(task);
        }
        let mut shutdown = UdpShutdown(runtime.clone());
        assert_eq!(
            shutdown.drive_shutdown().await,
            UdpShutdownState::Quarantined
        );
        drop(source); // Physical consumer disposal cannot certify its producer.
        assert_eq!(runtime.settle_source().await, Ok(false));
        assert!(runtime.generation.frozen(1));
        assert!(!runtime.generation.sealed(1));
        assert!(runtime.generation.prepare_publication(1, 2).is_none());
        assert_eq!(
            shutdown.drive_shutdown().await,
            UdpShutdownState::Quarantined
        );
        let (first, additional) = shutdown.take_node_failure();
        assert!(first.is_some());
        assert_eq!(additional, 0);
    }
}
