//! Stage 12 layer-2 integration: deterministic, no TUN.
//! client QUIC ↔ server QUIC over loopback; server relays to a local UDP echo;
//! asserts round-trip + flow-id demux across two flows.

use std::net::SocketAddr;
use std::time::Duration;

use mini_vpn::quic::{client_quic_config, server_endpoint, server_quic_config};
use mini_vpn::shared::TargetAddr;
use mini_vpn::udp_relay::{decode_downlink, encode_uplink, serve_quic_connection};

/// Start a UDP echo server; returns its address. Echoes every datagram back to sender.
async fn start_udp_echo() -> SocketAddr {
    let sock = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let addr = sock.local_addr().unwrap();
    tokio::spawn(async move {
        let mut buf = [0u8; 2048];
        loop {
            let Ok((n, peer)) = sock.recv_from(&mut buf).await else {
                break;
            };
            let _ = sock.send_to(&buf[..n], peer).await;
        }
    });
    addr
}

/// Start the QUIC relay server; returns its address.
async fn start_quic_server() -> SocketAddr {
    let scfg =
        server_quic_config("certs/dev/server-cert.pem", "certs/dev/server-key.pem").unwrap();
    let endpoint = server_endpoint(scfg, "127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = endpoint.local_addr().unwrap();
    tokio::spawn(async move {
        while let Some(connecting) = endpoint.accept().await {
            if let Ok(conn) = connecting.await {
                tokio::spawn(serve_quic_connection(conn, 60));
            }
        }
    });
    addr
}

#[tokio::test]
async fn udp_relay_roundtrip_and_flow_demux() {
    let echo_addr = start_udp_echo().await;
    let server_addr = start_quic_server().await;

    // client QUIC connection
    let ccfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(ccfg);
    let conn = client
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .expect("quic connect");

    // two flows to the same echo target — replies must carry their own flow-id.
    let target = TargetAddr::IpPort(echo_addr);
    conn.send_datagram(encode_uplink(7, &target, b"ping").into())
        .unwrap();
    conn.send_datagram(encode_uplink(9, &target, b"pong").into())
        .unwrap();

    let mut seen = std::collections::HashMap::new();
    for _ in 0..2 {
        let dg = tokio::time::timeout(Duration::from_secs(5), conn.read_datagram())
            .await
            .expect("downlink within 5s")
            .expect("datagram ok");
        let (fid, payload) = decode_downlink(&dg).expect("decodes");
        seen.insert(fid, payload.to_vec());
    }

    assert_eq!(seen.get(&7).map(|v| v.as_slice()), Some(&b"ping"[..]));
    assert_eq!(seen.get(&9).map(|v| v.as_slice()), Some(&b"pong"[..]));
}

/// Many concurrent flows over ONE QUIC connection must (almost) all round-trip — isolates
/// the relay/server from the client TUN main loop. If this passes but the field collapses
/// under concurrency, the bottleneck is the TUN main loop (e.g. per-packet stdout logging).
#[tokio::test]
async fn udp_relay_handles_many_concurrent_flows() {
    let echo_addr = start_udp_echo().await;
    let server_addr = start_quic_server().await;

    let ccfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(ccfg);
    let conn = client
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .expect("quic connect");

    let target = TargetAddr::IpPort(echo_addr);
    const N: u32 = 100;
    // Fire N distinct flows back-to-back.
    for fid in 0..N {
        let dg = encode_uplink(fid, &target, format!("flow-{fid}").as_bytes());
        // datagram send buffer can briefly fill under burst; that's a drop, tolerate.
        let _ = conn.send_datagram(dg.into());
    }
    // Collect replies for up to 5s.
    let mut seen = std::collections::HashSet::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while seen.len() < N as usize {
        match tokio::time::timeout_at(deadline, conn.read_datagram()).await {
            Ok(Ok(dg)) => {
                if let Some((fid, _)) = decode_downlink(&dg) {
                    seen.insert(fid);
                }
            }
            _ => break,
        }
    }
    // Loopback should deliver the vast majority; a handful of datagram drops is acceptable.
    assert!(
        seen.len() >= 90,
        "relay should handle ~all concurrent flows; only {} / {N} returned",
        seen.len()
    );
}

/// A full-size (1200-byte) UDP payload must round-trip intact — proves the relay does
/// not truncate large datagrams (the 1024 seen via `nc` is BSD nc chunking stdin, not us).
#[tokio::test]
async fn udp_relay_carries_full_1200_byte_payload() {
    let echo_addr = start_udp_echo().await;
    let server_addr = start_quic_server().await;

    let ccfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(ccfg);
    let conn = client
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .expect("quic connect");

    // quinn starts at the safe initial MTU (1200 → max_datagram_size ~1162), too small for a
    // 1200-byte inner payload; PLPMTUD raises it once the path is probed. Warm up first.
    // 中文要点：真·QUIC 的 1200B initial 同理——数据面 warm 起来后(PLPMTUD)才装得下，
    // 我们的持久数据面在用户发请求时早已 warm。这条测试钉住「warm 后大包不截断」。
    let target = TargetAddr::IpPort(echo_addr);
    for _ in 0..5 {
        let _ = conn.send_datagram(encode_uplink(99, &target, b"warmup").into());
        let _ = tokio::time::timeout(Duration::from_millis(300), conn.read_datagram()).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
    }
    assert!(
        conn.max_datagram_size().unwrap_or(0) >= 1224,
        "PLPMTUD should raise datagram size enough for a 1200B inner payload, got {:?}",
        conn.max_datagram_size()
    );

    let payload = vec![0xABu8; 1200];
    conn.send_datagram(encode_uplink(11, &target, &payload).into())
        .expect("send 1200B");
    let dg = tokio::time::timeout(Duration::from_secs(5), conn.read_datagram())
        .await
        .expect("reply within 5s")
        .expect("datagram ok");
    let (fid, got) = decode_downlink(&dg).expect("decodes");
    assert_eq!(fid, 11);
    assert_eq!(got.len(), 1200, "full payload must round-trip, not truncate");
    assert_eq!(got, &payload[..]);
}

/// Diagnostic: the QUIC data plane must survive an idle period longer than quinn's
/// default 10s idle timeout (keep-alive must keep it up). Slow (12s); run explicitly.
#[tokio::test]
async fn quic_connection_survives_idle_beyond_default_timeout() {
    let echo_addr = start_udp_echo().await;
    let server_addr = start_quic_server().await;

    let ccfg = client_quic_config("certs/dev/ca-cert.pem").unwrap();
    let mut client = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
    client.set_default_client_config(ccfg);
    let conn = client
        .connect(server_addr, "localhost")
        .unwrap()
        .await
        .expect("quic connect");

    // Stay completely idle past the default 10s idle timeout.
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert!(
        conn.close_reason().is_none(),
        "connection died during idle: {:?}",
        conn.close_reason()
    );

    // And it must still relay after the idle period.
    let target = TargetAddr::IpPort(echo_addr);
    conn.send_datagram(encode_uplink(5, &target, b"alive").into())
        .expect("send after idle");
    let dg = tokio::time::timeout(Duration::from_secs(5), conn.read_datagram())
        .await
        .expect("reply within 5s after idle")
        .expect("datagram ok");
    assert_eq!(decode_downlink(&dg).unwrap(), (5, &b"alive"[..]));
}
