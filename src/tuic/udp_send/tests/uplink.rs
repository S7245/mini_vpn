use crate::tuic::tcp_fixture::UdpPair;
use crate::tuic::*;
use std::future::Future;
use std::task::Waker;

#[test]
fn udp_send_plan_native_is_size_based() {
    use UdpRelayMode::Native;
    use UdpSend::*;
    // Native：装得下 → datagram 快路径（含边界 ==max）——保刀3 现行语义，零回归。
    assert_eq!(udp_send_plan(Native, Some(1242), 1000), Datagram);
    assert_eq!(udp_send_plan(Native, Some(1242), 1242), Datagram);
    // 超上限 → stream 兜底。
    assert_eq!(udp_send_plan(Native, Some(1242), 1243), Stream);
    // datagram 不可用（对端不支持/未协商）→ stream。
    assert_eq!(udp_send_plan(Native, None, 100), Stream);
}

#[test]
fn udp_send_plan_quic_is_always_stream() {
    use UdpRelayMode::Quic;
    use UdpSend::*;
    // Quic：无论装不装得下、datagram 是否可用，首包起恒走 uni-stream
    // （触发 TUIC server 镜像下行也走 stream，摆脱 datagram 天花板）。
    assert_eq!(udp_send_plan(Quic, Some(1242), 1000), Stream);
    assert_eq!(udp_send_plan(Quic, Some(1242), 1242), Stream);
    assert_eq!(udp_send_plan(Quic, Some(1242), 1243), Stream);
    assert_eq!(udp_send_plan(Quic, None, 100), Stream);
}

fn packet() -> Vec<u8> {
    encode_packet(
        7,
        &TargetAddr::IpPort("127.0.0.1:9000".parse().unwrap()),
        &[0x5a; 128],
    )
}

async fn poll_udp_once(upstream: &TuicUpstream, packet: Vec<u8>) -> Poll<()> {
    let mut send = std::pin::pin!(upstream.send_udp(packet));
    std::future::poll_fn(|cx| Poll::Ready(send.as_mut().poll(cx))).await
}

#[tokio::test]
async fn public_udp_stream_preserves_complete_packet_and_fin() {
    let pair = UdpPair::new(4, 65535).await;
    let upstream = pair.upstream(UdpRelayMode::Quic).await;
    let packet = packet();
    assert!(poll_udp_once(&upstream, packet.clone()).await.is_ready());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        pair.peer
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(MAX_TUIC_PACKET_BYTES)
            .await
            .unwrap()
    })
    .await
    .expect("complete Packet and FIN must reach the peer");
    assert_eq!(received, packet);
    assert_eq!(upstream.udp_drop_count(), 0);
    assert_eq!(upstream.udp_stream_fallback_count(), 0);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn public_udp_stream_pressure_settles_without_suspension_or_partial_fin() {
    for (uni_streams, stream_window) in [(0, 65535), (1, 16), (1, 0)] {
        let pair = UdpPair::new(uni_streams, stream_window).await;
        let upstream = pair.upstream(UdpRelayMode::Quic).await;
        assert!(
            poll_udp_once(&upstream, packet()).await.is_ready(),
            "UDP must settle in its initial poll: uni={uni_streams}, window={stream_window}"
        );
        assert_eq!(upstream.udp_drop_count(), 1);
        assert_eq!(upstream.udp_stream_fallback_count(), 0);
        if uni_streams != 0 {
            let received = tokio::time::timeout(Duration::from_secs(2), async {
                pair.peer
                    .accept_uni()
                    .await
                    .unwrap()
                    .read_to_end(MAX_TUIC_PACKET_BYTES)
                    .await
            })
            .await
            .expect("the rejected stream must reach a terminal reset");
            assert!(
                matches!(
                    received,
                    Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
                ),
                "pressure must reset; Drop must not FIN a truncated Packet: {received:?}"
            );
        }
        if stream_window != 0 {
            pair.assert_bi_progress().await;
        } else {
            pair.connection
                .send_datagram(bytes::Bytes::from_static(b"marker"))
                .unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), pair.peer.read_datagram())
                    .await
                    .expect("datagrams must progress while all streams have zero credit")
                    .unwrap()
                    .as_ref(),
                b"marker"
            );
        }
        drop(upstream);
        pair.close().await;
    }
}

#[tokio::test]
async fn public_udp_rejection_does_not_leave_delayed_stream_work() {
    let pair = UdpPair::new(4, 65535).await;
    let upstream = pair.upstream(UdpRelayMode::Quic).await;
    assert!(
        poll_udp_once(&upstream, vec![0; MAX_TUIC_PACKET_BYTES + 1])
            .await
            .is_ready()
    );
    assert_eq!(upstream.udp_drop_count(), 1);

    let guard = upstream.tcp_pool_slots[0].state.lock().await;
    assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    assert_eq!(upstream.udp_drop_count(), 2);
    drop(guard);

    let expected = packet();
    assert!(poll_udp_once(&upstream, expected.clone()).await.is_ready());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        let mut stream = pair.peer.accept_uni().await.unwrap();
        assert_eq!(
            stream.id().index(),
            0,
            "rejection must not allocate streams"
        );
        stream.read_to_end(MAX_TUIC_PACKET_BYTES).await.unwrap()
    })
    .await
    .expect("a later healthy Packet must have no delayed predecessor");
    assert_eq!(received, expected);
    assert_eq!(upstream.udp_drop_count(), 2);

    pair.connection.close(0u32.into(), b"test closed");
    assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    assert_eq!(upstream.udp_drop_count(), 3);
    drop(upstream);
    pair.close().await;
}

#[tokio::test]
async fn public_udp_native_preserves_datagram_and_counts_stream_fallback() {
    let pair = UdpPair::new(4, 65535).await;
    let upstream = pair.upstream(UdpRelayMode::Native).await;
    let small = packet();
    assert!(poll_udp_once(&upstream, small.clone()).await.is_ready());
    let received = tokio::time::timeout(Duration::from_secs(2), pair.peer.read_datagram())
        .await
        .expect("the native path must remain a datagram")
        .unwrap();
    assert_eq!(&received[..], small);
    assert_eq!(upstream.udp_stream_fallback_count(), 0);

    let large = encode_packet(
        8,
        &TargetAddr::IpPort("127.0.0.1:9001".parse().unwrap()),
        &vec![0x6b; pair.connection.max_datagram_size().unwrap() + 1],
    );
    assert!(poll_udp_once(&upstream, large.clone()).await.is_ready());
    let received = tokio::time::timeout(Duration::from_secs(2), async {
        pair.peer
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(MAX_TUIC_PACKET_BYTES)
            .await
            .unwrap()
    })
    .await
    .expect("an oversized native Packet must preserve stream fallback");
    assert_eq!(received, large);
    assert_eq!(upstream.udp_stream_fallback_count(), 1);
    assert_eq!(upstream.udp_drop_count(), 0);
    drop(upstream);
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_udp_reset_pressure_bounds_physical_stream_admission() {
    for window in [0, 16] {
        let pair = UdpPair::new(1024, window).await;
        let upstream = pair.upstream(UdpRelayMode::Quic).await;
        // Every call must finish in its first poll; this loop never yields to ACK processing.
        for _ in 0..257 {
            assert!(poll_udp_once(&upstream, packet()).await.is_ready());
        }
        let next = {
            let mut open = std::pin::pin!(pair.connection.open_uni());
            std::future::poll_fn(|cx| Poll::Ready(open.as_mut().poll(cx))).await
        };
        let Poll::Ready(Ok(mut probe)) = next else {
            panic!("peer still grants enough credit for the independent stream-ID probe");
        };
        let admitted = probe.id().index();
        let dropped = upstream.udp_drop_count();
        let (occupied, cursor) = {
            let sender = upstream.udp_stream_sender.lock().unwrap();
            (sender.slots.iter().flatten().count(), sender.cursor)
        };
        probe.reset(0u32.into()).unwrap();
        drop(probe);
        drop(upstream);
        pair.close().await;
        assert_eq!(
            admitted, 256,
            "RESET tails must consume the local admission bound, window={window}"
        );
        assert_eq!(dropped, 257);
        assert_eq!(
            occupied, 256,
            "physical stream count and ticket occupancy agree"
        );
        assert_eq!(
            cursor, 4,
            "the first pressured call inspects exactly four slots"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn public_udp_reset_ack_recovers_custody_and_native_bypasses_its_lock() {
    let pair = UdpPair::new(1024, 0).await;
    let mut upstream = pair.upstream(UdpRelayMode::Quic).await;
    for _ in 0..256 {
        assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    }
    {
        let sender = upstream.udp_stream_sender.lock().unwrap();
        assert_eq!(sender.slots.iter().flatten().count(), 256);
        assert!(
            sender
                .slots
                .iter()
                .flatten()
                .all(|ticket| !ticket.is_retired())
        );
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if upstream
                .udp_stream_sender
                .lock()
                .unwrap()
                .slots
                .iter()
                .flatten()
                .all(quinn::SendStreamRetirement::is_retired)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("real RESET ACKs must remove the send-map entries");
    for _ in 0..256 {
        assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    }
    let mut probe = pair.connection.open_uni().await.unwrap();
    assert_eq!(
        probe.id().index(),
        512,
        "the existing sender must reuse retired slots"
    );
    probe.reset(0u32.into()).unwrap();
    drop(probe);
    upstream.udp_relay_mode = UdpRelayMode::Native;
    {
        let _held = upstream.udp_stream_sender.lock().unwrap();
        let mut send = std::pin::pin!(upstream.send_udp(packet()));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }
    let native = tokio::time::timeout(Duration::from_secs(2), pair.peer.read_datagram())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(native.as_ref(), packet());
    assert_eq!(upstream.udp_drop_count(), 512);
    drop(upstream);
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_udp_custody_spans_generations_until_other_tcp_handles_retire() {
    let old = UdpPair::new(1024, 0).await;
    let replacement = UdpPair::new(1024, 0).await;
    let upstream = old.upstream(UdpRelayMode::Quic).await;
    let (other_tcp_send, other_tcp_recv) = old.connection.open_bi().await.unwrap();
    for _ in 0..256 {
        assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    }
    old.connection.close(0u32.into(), b"replace generation");
    {
        let mut slot = upstream.tcp_pool_slots[0].state.lock().await;
        slot.current = TcpPoolGeneration::new(replacement.connection.clone(), 2, 1, upstream.clock);
    }
    old.close().await;
    {
        let sender = upstream.udp_stream_sender.lock().unwrap();
        assert_eq!(sender.slots.iter().flatten().count(), 256);
        assert!(
            sender
                .slots
                .iter()
                .flatten()
                .all(|ticket| !ticket.is_retired())
        );
    }
    for _ in 0..64 {
        assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    }
    let mut probe = replacement.connection.open_uni().await.unwrap();
    assert_eq!(
        probe.id().index(),
        0,
        "closed old generations still consume custody"
    );
    probe.reset(0u32.into()).unwrap();
    drop(probe);
    drop((other_tcp_send, other_tcp_recv));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if upstream
                .udp_stream_sender
                .lock()
                .unwrap()
                .slots
                .iter()
                .flatten()
                .all(quinn::SendStreamRetirement::is_retired)
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("weak-only custody must allow the old connection's actual destruction");
    for _ in 0..256 {
        assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    }
    let mut probe = replacement.connection.open_uni().await.unwrap();
    assert_eq!(
        probe.id().index(),
        257,
        "the same adapter must recover across generations"
    );
    probe.reset(0u32.into()).unwrap();
    drop(probe);
    assert_eq!(upstream.udp_drop_count(), 576);
    drop(upstream);
    replacement.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn public_udp_interruption_resets_and_keeps_poisoned_custody_occupied() {
    use crate::tuic::udp_send::ResetOnDrop;
    let pair = UdpPair::new(1024, 16).await;
    let mut upstream = pair.upstream(UdpRelayMode::Quic).await;
    drop(upstream.send_udp(packet())); // Cancellation before first poll admits nothing.
    {
        let _held = upstream.udp_stream_sender.lock().unwrap();
        let mut send = std::pin::pin!(upstream.send_udp(packet()));
        assert!(
            send.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_ready()
        );
    }
    let interrupted = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut sender = upstream.udp_stream_sender.lock().unwrap();
        let (stream, ticket) = pair.connection.try_open_uni_1rtt_with_retirement().unwrap();
        assert_eq!(
            stream.id().index(),
            0,
            "cancellation and lock pressure allocate nothing"
        );
        sender.slots[0] = Some(ticket);
        let mut guard = ResetOnDrop {
            stream,
            finished: false,
        };
        assert!(matches!(
            Pin::new(&mut guard.stream)
                .poll_write(&mut Context::from_waker(Waker::noop()), &packet()),
            Poll::Ready(Ok(16))
        ));
        panic!("injected interruption after custody and a physical prefix write");
    }));
    assert!(interrupted.is_err());
    {
        let poisoned = match upstream.udp_stream_sender.lock() {
            Err(error) => error.into_inner(),
            Ok(_) => panic!("interrupted custody must be sticky poisoned"),
        };
        assert_eq!(poisoned.slots.iter().flatten().count(), 1);
        assert!(!poisoned.slots[0].as_ref().unwrap().is_retired());
    }
    assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    let mut probe = pair.connection.open_uni().await.unwrap();
    assert_eq!(
        probe.id().index(),
        1,
        "poison must reject without normalization or new admission"
    );
    probe.reset(0u32.into()).unwrap();
    drop(probe);
    let outcome = tokio::time::timeout(Duration::from_secs(2), async {
        pair.peer
            .accept_uni()
            .await
            .unwrap()
            .read_to_end(MAX_TUIC_PACKET_BYTES)
            .await
    })
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        Err(quinn::ReadToEndError::Read(quinn::ReadError::Reset(_)))
    ));
    assert!(upstream.udp_stream_sender.is_poisoned());
    upstream.udp_relay_mode = UdpRelayMode::Native;
    assert!(poll_udp_once(&upstream, packet()).await.is_ready());
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), pair.peer.read_datagram())
            .await
            .unwrap()
            .unwrap()
            .as_ref(),
        packet()
    );
    assert_eq!(upstream.udp_drop_count(), 2);
    drop(upstream);
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn udp_retirement_requires_fin_ack_after_data_ack() {
    let pair = UdpPair::new(8, 65535).await;
    let (mut stream, ticket) = pair.connection.try_open_uni_1rtt_with_retirement().unwrap();
    assert!(
        !ticket.is_retired(),
        "an uninitialized send-map entry is still allocated"
    );
    stream.write_all(b"prefix").await.unwrap();
    let progress = stream.progress_handle();
    let mut recv = pair.peer.accept_uni().await.unwrap();
    let mut prefix = [0; 6];
    recv.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, b"prefix");
    tokio::time::timeout(Duration::from_secs(2), async {
        while progress.sample().unwrap().acknowledged_bytes != 6 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        !ticket.is_retired(),
        "ACKing a prefix does not retire an unfinished stream"
    );
    stream.finish().unwrap();
    assert!(
        !ticket.is_retired(),
        "FIN publication without its ACK retains custody"
    );
    assert!(recv.read_to_end(1).await.unwrap().is_empty());
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ticket.is_retired() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the real FIN ACK must remove this exact send-map entry");
    assert!(
        progress.sample().is_err(),
        "independent progress query also observes physical removal"
    );
    drop((stream, progress, recv));
    pair.close().await;
}

#[tokio::test(flavor = "current_thread")]
async fn udp_retirement_does_not_treat_stop_sending_as_reset_retirement() {
    let pair = UdpPair::new(8, 65535).await;
    let (mut stream, ticket) = pair.connection.try_open_uni_1rtt_with_retirement().unwrap();
    stream.write_all(b"prefix").await.unwrap();
    let mut recv = pair.peer.accept_uni().await.unwrap();
    let mut prefix = [0; 6];
    recv.read_exact(&mut prefix).await.unwrap();
    recv.stop(41u32.into()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.stopped())
            .await
            .unwrap()
            .unwrap(),
        Some(41u32.into())
    );
    assert!(
        !ticket.is_retired(),
        "a real STOP_SENDING result is not physical absence"
    );
    stream.reset(0u32.into()).unwrap();
    assert_eq!(stream.stopped().await.unwrap(), Some(41u32.into()));
    assert!(
        !ticket.is_retired(),
        "ResetSent still exposes STOP before the RESET ACK"
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        while !ticket.is_retired() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("only the subsequent real RESET ACK retires the stream");
    drop((stream, recv));
    pair.close().await;
}

#[tokio::test]
async fn saved_policy_keeps_generic_and_native_open_paths_distinct() {
    use crate::upstream::RelayFamilyPolicySources;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let pair = UdpPair::new(8, 65536).await;
    let target = TargetAddr::IpPort("192.0.2.1:443".parse().unwrap());
    for mode in 0..3 {
        let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
            tuic_ordered_chunk: (mode == 1).then_some("1"),
            tuic_unordered_reassembly: (mode == 2).then_some("yes"),
            d16_byte_owned: Some("true"),
            ..Default::default()
        }));
        let mut upstream = pair.upstream(UdpRelayMode::Native).await;
        upstream.relay_policy = Arc::clone(&policy);
        assert!(Arc::ptr_eq(&upstream.relay_policy, &policy));
        let exchange = tokio::time::timeout(Duration::from_secs(3), async {
            let generic = async {
                let mut stream = upstream
                    .open_tcp(&target)
                    .await
                    .expect("actual Generic open");
                stream.write_all(b"ping").await.unwrap();
                let mut reply = [0; 4];
                stream.read_exact(&mut reply).await.unwrap();
                assert_eq!(&reply, b"pong");
                stream.shutdown().await.unwrap();
            };
            let peer = async {
                let (mut send, mut recv) = pair.peer.accept_bi().await.unwrap();
                let mut expected = encode_connect(&target);
                expected.extend_from_slice(b"ping");
                let mut actual = vec![0; expected.len()];
                recv.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, expected);
                send.write_all(b"pong").await.unwrap();
                send.finish().unwrap();
                recv.read_to_end(1).await.unwrap();
            };
            tokio::join!(generic, peer);
            let native = async {
                let opened = upstream
                    .open_tcp_relay(&target)
                    .await
                    .expect("actual native open");
                let OpenedTcpRelay::NativeByteOwned(mut stream) = opened else {
                    panic!("saved D16 override must select native only for open_tcp_relay");
                };
                stream.writer.write_all(b"ping").await.unwrap();
                let mut reply = Vec::with_capacity(4);
                while reply.len() < 4 {
                    let chunk = std::future::poll_fn(|cx| {
                        stream.reader.poll_read_chunk(cx, 4 - reply.len())
                    })
                    .await
                    .unwrap()
                    .expect("native reply");
                    assert_eq!(chunk.offset, reply.len() as u64);
                    reply.extend_from_slice(&chunk.bytes);
                }
                assert_eq!(reply, b"pong");
                stream.writer.shutdown().await.unwrap();
            };
            let peer = async {
                let (mut send, mut recv) = pair.peer.accept_bi().await.unwrap();
                let mut expected = encode_connect(&target);
                expected.extend_from_slice(b"ping");
                let mut actual = vec![0; expected.len()];
                recv.read_exact(&mut actual).await.unwrap();
                assert_eq!(actual, expected);
                send.write_all(b"pong").await.unwrap();
                send.finish().unwrap();
                recv.read_to_end(1).await.unwrap();
            };
            tokio::join!(native, peer);
        })
        .await;
        drop(upstream);
        exchange.expect("both actual open APIs must use the stored policy and make progress");
    }
    pair.close().await;
}

#[test]
fn first_tcp_open_has_no_prior_success_timestamp() {
    let state = TcpPoolOpenState::new();
    assert_eq!(state.last_success_secs(), None);
    assert_eq!(state.last_success_age_secs(17), None);
    state.note_open_success(0);
    assert_eq!(state.last_success_secs(), Some(0));
    assert_eq!(state.last_success_age_secs(17), Some(17));
}
