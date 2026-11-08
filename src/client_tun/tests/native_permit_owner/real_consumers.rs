//! Real public TUIC carriers into the three original legacy consumers.
//! NativeOrdered Chunk/Permit consume their prepared pump. ByteOwned keeps its
//! original direct reader and no pump; no test converts the carrier variant.
use super::*;
use crate::shared::TargetAddr;
use crate::tuic::tcp_fixture::{UdpPair, active_tcp_leases};
use crate::tuic::{TuicClientConfig, TuicUpstream, encode_authenticate, encode_connect};
use std::future::{Future, poll_fn};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::task::Poll;
use std::time::Duration;

#[derive(Clone, Copy, Debug)]
enum Consumer {
    Chunk,
    Permit,
    ByteOwned,
}
async fn capture<F: Future>(future: F) -> Result<F::Output, Box<dyn std::any::Any + Send>> {
    let mut future = std::pin::pin!(future);
    poll_fn(
        |cx| match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        },
    )
    .await
}
async fn actual_consumer(mode: Consumer) {
    let limit = tokio::time::Instant::now() + Duration::from_secs(5);
    let pair = tokio::time::timeout_at(limit, UdpPair::with_limits(8, 65536, 100))
        .await
        .unwrap();
    pair.server
        .set_server_config(Some(UdpPair::server_config(8, 65536, 100, true)));
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        tuic_native_ordered: (!matches!(mode, Consumer::ByteOwned)).then_some("1"),
        native_egress_permit: matches!(mode, Consumer::Permit).then_some("1"),
        d16_byte_owned: matches!(mode, Consumer::ByteOwned).then_some("1"),
        ..Default::default()
    }));
    let mut cfg = TuicClientConfig::from_sources(
        Some(&pair.server.local_addr().unwrap().to_string()),
        Some("00000000-0000-0000-0000-000000000000"),
        Some("local-test-only"),
        Some("localhost"),
        Some(crate::tuic::tcp_fixture::test_ca_path().as_str()),
        Some("h3"),
    )
    .unwrap()
    .with_relay_policy(policy);
    cfg.tcp_pool = 1;
    let mut upstream = None;
    let mut peer = None;
    let mut wire = None;
    let mut carrier = None;
    let mut actor: Option<tokio::task::JoinHandle<()>> = None;
    let mut commands = None;
    let mut events = None;
    let mut queue = None;
    let mut consumed_join = false;
    let result = capture(tokio::time::timeout_at(limit, async {
        let (u,p) = tokio::join!(
            TuicUpstream::connect(&cfg, Arc::new(crate::metrics::Metrics::default())),
            async { pair.server.accept().await.unwrap().await.unwrap() }
        );
        upstream = Some(u.unwrap()); peer = Some(p);
        let u = upstream.as_ref().unwrap(); let p = peer.as_ref().unwrap();
        let mut auth = p.accept_uni().await.unwrap();
        let auth = auth.read_to_end(64).await.unwrap(); let mut token = [0; 32];
        p.export_keying_material(&mut token, &cfg.uuid, cfg.password.as_bytes()).unwrap();
        assert_eq!(auth, encode_authenticate(&cfg.uuid, &token));
        let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
        carrier = Some(u.open_tcp_relay(&target).await.unwrap());
        wire = Some(p.accept_bi().await.unwrap());
        let (send,recv) = wire.as_mut().unwrap();
        let expected = encode_connect(&target); let mut actual = vec![0;expected.len()];
        recv.read_exact(&mut actual).await.unwrap(); assert_eq!(actual, expected); assert_eq!(actual.len(),9);
        assert_eq!(active_tcp_leases(u), 2);
        let mut sockets = SocketSet::new(vec![]); let handle = mk_test_handle(&mut sockets);
        let epoch = 73;
        let (tx,rx) = crate::client_tun::uplink::channel(8); commands=Some(tx);
        let (back_tx,back_rx) = crate::client_tun::relay_channel::channel(8); events=Some(back_rx);
        let (_credit,credit_rx) = watch::channel(RelayReadCredit { paused:false, max_batch_bytes:4096 });
        let original = carrier.take().unwrap();
        actor=Some(match (mode,original) {
            (Consumer::Chunk, OpenedTcpRelay::Native(native)) => tokio::spawn(run_relay_native_chunk(handle,epoch,native,rx,back_tx,credit_rx,false)),
            (Consumer::Permit, OpenedTcpRelay::Native(native)) => tokio::spawn(run_relay_native_permit(handle,epoch,native,rx,back_tx,credit_rx,native_tcp_permit_queue_capacity_bytes(DownlinkBackpressureConfig::default()),false)),
            (Consumer::ByteOwned, OpenedTcpRelay::NativeByteOwned(native)) => {
                let budget=d16_process_global_budget(); assert_eq!(budget.snapshot().total_bytes(),0);
                let q=AsyncLeasedByteFlowQueue::new_with_global_budget(D16_PER_FLOW_BYTE_CAPACITY,DownstreamPermitReleaseMode::OnEgressDrain,budget).unwrap();
                queue=Some(q.clone()); tokio::spawn(run_relay_d16(handle,epoch,native,rx,back_tx,credit_rx,q))
            }
            _ => panic!("public factory returned the wrong original carrier for {mode:?}"),
        });
        let tx=commands.as_ref().unwrap();
        tx.send(RelayCommand::Data(b"real uplink".to_vec())).await.unwrap();
        tx.send(RelayCommand::Finish).await.unwrap();
        assert_eq!(recv.read_to_end(32).await.unwrap(), b"real uplink");
        send.write_all(&[0xa7;512]).await.unwrap(); send.finish().unwrap();
        let mut payload=Vec::new();
        let mut observed_eof=false;
        while let Some((h,event))=events.as_mut().unwrap().recv().await {
            assert_eq!(h,handle);
            match event {
                RelayEvent::Data { epoch:e,bytes,downstream_permit,.. } => {
                    assert_eq!(e,epoch); let count=bytes.len(); payload.extend_from_slice(&bytes);
                    if let Some(mut permit)=downstream_permit { assert_eq!(permit.release(count),count); }
                }
                RelayEvent::DataReady { epoch:e } => {
                    assert_eq!(e,epoch); let q=queue.as_ref().expect("original ByteOwned queue");
                    loop { match q.try_recv_up_to(D16_ACTOR_QUANTUM_BYTES) {
                        LeasedQueuePoll::Data(lease) => { let (bytes,mut permit)=lease.into_parts(); let count=bytes.len(); payload.extend_from_slice(&bytes); assert_eq!(permit.release(count),count); }
                        LeasedQueuePoll::Empty | LeasedQueuePoll::Closed => break,
                    }}
                }
                RelayEvent::Closed(close) => { assert!(!matches!(mode,Consumer::ByteOwned)); assert_eq!(close.epoch,epoch); assert_eq!(close.direction,"remote_to_local"); assert_eq!(close.reason,"remote_eof"); observed_eof=true; break; }
                _ => {}
            }
        }
        assert_eq!(payload, [0xa7;512]);
        if !matches!(mode,Consumer::ByteOwned) { assert!(observed_eof,"actual original remote EOF close required"); }
        let joined=actor.as_mut().unwrap().await; consumed_join=true; joined.expect("actual original consumer JoinHandle");
        assert_eq!(active_tcp_leases(u),0,"pump/direct reader and writer actually disposed");
        if let Some(q)=queue.as_ref() {
            let s=q.snapshot_now(); assert_eq!(s.closure,crate::tcp_downlink_pump::LeasedByteQueueClosure::RemoteEof); assert_eq!((s.queued_bytes,s.leased_bytes,s.reserved_bytes),(0,0,0));
            assert_eq!(d16_process_global_budget().snapshot().total_bytes(),0);
        }
        println!("B74 actual {mode:?} AUTH/CONNECT9: uplink11/FIN downlink512/EOF, original outer actualJoin, leases0; not session-root join");
    })).await;
    let cleanup = capture(async {
        drop(commands.take());
        drop(carrier.take());
        if let Some(p) = peer.as_ref() {
            p.close(0u32.into(), b"B74 fixture cleanup");
        }
        drop(events.take());
        if !consumed_join {
            if let Some(task) = actor.as_mut() {
                let done = tokio::time::timeout_at(limit, task).await;
                println!(
                    "B74 original cleanup consumed consumer join={}",
                    matches!(done, Ok(Ok(())))
                );
            }
        }
        drop(wire.take());
        drop(upstream.take());
        drop(peer.take());
        pair.close().await;
    })
    .await;
    match result {
        Err(payload) => resume_unwind(payload),
        Ok(Err(error)) => panic!("original single5 expired: {error}"),
        Ok(Ok(())) => {}
    }
    if let Err(payload) = cleanup {
        resume_unwind(payload);
    }
}
#[tokio::test]
async fn actual_auth_prepared_chunk_consumer_payload_eof_and_join() {
    actual_consumer(Consumer::Chunk).await;
}
#[tokio::test]
async fn actual_auth_prepared_permit_consumer_payload_eof_and_join() {
    actual_consumer(Consumer::Permit).await;
}
#[tokio::test]
async fn actual_auth_direct_byteowned_consumer_payload_eof_and_join() {
    actual_consumer(Consumer::ByteOwned).await;
}
