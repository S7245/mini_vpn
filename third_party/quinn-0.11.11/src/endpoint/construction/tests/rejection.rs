//! Original UDP receive distinguishes backing pressure from owned-lane rejection.
//! One held original ticket blocks the reservation fence; this is not a filled
//! physical mailbox. The test does not prove
//! application DATA/ACK-dependent recovery through mini_vpn's two Flow Roots.
use super::*;

#[tokio::test(flavor = "current_thread")]
async fn actual_udp_mailbox_rejection_has_separate_cause_and_recovers() {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let server_config = crate::ServerConfig::with_single_cert(
        vec![cert.cert.der().clone()],
        rustls::pki_types::PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()).into(),
    )
    .unwrap();
    let mut server = Endpoint::server(server_config, "127.0.0.1:0".parse().unwrap()).unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert.cert.der().clone()).unwrap();
    let budget = bytes::BackingBudget::new(8 << 20).unwrap();
    let runtime = crate::default_runtime().unwrap();
    let socket = runtime
        .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
        .unwrap();
    let mut client = Endpoint::new_with_metered_abstract_socket(
        EndpointConfig::default(), None, socket, runtime, BackingMeter::with_budget(&budget),
    ).unwrap();
    client.set_default_client_config(ClientConfig::with_root_certificates(Arc::new(roots)).unwrap());
    let (connection, remote) = timeout(Duration::from_secs(5), async {
        tokio::join!(
            client.connect(server.local_addr().unwrap(), "localhost").unwrap(),
            async { server.accept().await.unwrap().await },
        )
    }).await.unwrap();
    let connection = connection.unwrap();
    let remote = remote.unwrap();
    let mut tickets = Vec::new();
    {
        let root = client.inner.lock_state();
        assert_eq!(root.recv_state.connections.catalog.len(), 1);
        let sender = root.recv_state.connections.catalog.values().next().unwrap();
        loop {
            match sender.reserve_proto() {
                Ok(ticket) => tickets.push(ticket),
                Err(proto::owned_events::ReserveError::Full) => break,
                Err(proto::owned_events::ReserveError::Closed) => panic!("original lane closed"),
            }
        }
    }
    assert_eq!(tickets.len(), 1, "one outstanding reservation fences further admission");
    remote.send_datagram(Bytes::from_static(b"mailbox-full")).unwrap();
    let rejected = timeout(Duration::from_secs(2), async {
        loop {
            let stats = client.stats();
            if stats.owned_event_rejected_datagrams > 0 { break stats; }
            tokio::task::yield_now().await;
        }
    }).await;
    drop(tickets);
    remote.send_datagram(Bytes::from_static(b"after-mailbox-release")).unwrap();
    // The fenced receive can be a handshake/control datagram. QUIC datagrams
    // are unordered: the earlier DATA may therefore arrive after release too.
    // Observe both original values, bounded by the two actual sends and the
    // same deadline; do not mistake a control rejection for a DATA drop proof.
    let delivered = timeout(Duration::from_secs(2), async {
        let first = connection.read_datagram().await?;
        if &first[..] == b"after-mailbox-release" { return Ok::<_, crate::ConnectionError>(first); }
        assert_eq!(&first[..], b"mailbox-full", "no unrelated or replacement DATA");
        connection.read_datagram().await
    }).await;
    let correct = matches!(&delivered, Ok(Ok(bytes)) if &bytes[..] == b"after-mailbox-release");
    drop(delivered);
    let final_stats = client.stats();
    remote.close(0u32.into(), b"done");
    connection.close(0u32.into(), b"done");
    drop((remote, connection));
    let client_drain = client.take_driver_drain_handle().unwrap();
    let server_drain = server.take_driver_drain_handle().unwrap();
    client.close(0u32.into(), b"done");
    server.close(0u32.into(), b"done");
    drop((client, server));
    assert!(timeout(Duration::from_secs(5), client_drain.drained()).await.unwrap());
    assert!(timeout(Duration::from_secs(5), server_drain.drained()).await.unwrap());
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
    let rejected = rejected.expect("original endpoint never counted mailbox rejection");
    for stats in [rejected, final_stats] {
        assert_eq!(stats.receive_backing_rejected_datagrams, 0);
        assert!(stats.owned_event_rejected_datagrams > 0);
        assert_eq!(stats.budget_rejected_datagrams, stats.owned_event_rejected_datagrams);
    }
    assert!(correct, "same original connection must deliver the post-release datagram");
}
