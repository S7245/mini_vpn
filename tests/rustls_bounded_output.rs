//! Real TLS provider handshake only; not a QUIC/Root or hard-budget acceptance.
use rustls::{
    RootCertStore,
    pki_types::*,
    quic::{Connection, KeyChange, Version},
};
use std::sync::Arc;

fn peers() -> (Connection, Connection) {
    let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let cert = identity.cert.der().clone();
    let key = PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der());
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut server = rustls::ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert.clone()], key.into())
        .unwrap();
    server.alpn_protocols = vec![b"h3".to_vec()];
    let mut roots = RootCertStore::empty();
    roots.add(cert).unwrap();
    let mut client = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    client.alpn_protocols = vec![b"h3".to_vec()];
    (
        rustls::quic::ClientConnection::new(
            Arc::new(client),
            Version::V1,
            ServerName::try_from("localhost").unwrap(),
            vec![0x01],
        )
        .unwrap()
        .into(),
        rustls::quic::ServerConnection::new(Arc::new(server), Version::V1, vec![0x02])
            .unwrap()
            .into(),
    )
}

fn flight(from: &mut Connection, to: &mut Connection, keys: &mut [usize; 3], legacy: bool) -> bool {
    let required = match from.write_hs_bounded(&mut []) {
        Err(rustls::quic::HandshakeOutputError::Insufficient { required }) => required,
        Err(other) => panic!("unexpected refusal: {other:?}"),
        Ok((0, change)) => {
            keys[2] += usize::from(change.is_some());
            count(change, keys);
            return false;
        }
        Ok(_) => unreachable!("empty target cannot receive bytes"),
    };
    assert!(required > 0);
    let mut short = vec![0xa5; required - 1];
    assert!(
        matches!(from.write_hs_bounded(&mut short), Err(rustls::quic::HandshakeOutputError::Insufficient { required: n }) if n == required)
    );
    assert!(short.iter().all(|b| *b == 0xa5));
    assert!(
        matches!(from.write_hs_bounded(&mut []), Err(rustls::quic::HandshakeOutputError::Insufficient { required: n }) if n == required)
    );
    let mut exact = vec![0xa5; required + 7];
    let (written, change) = if legacy {
        let mut original = Vec::new();
        let change = from.write_hs(&mut original);
        assert_eq!(original.len(), required);
        exact[..required].copy_from_slice(&original);
        (required, change)
    } else {
        from.write_hs_bounded(&mut exact).unwrap()
    };
    assert_eq!(written, required);
    assert!(exact[written..].iter().all(|b| *b == 0xa5));
    count(change, keys);
    to.read_hs(&exact[..written]).unwrap();
    true
}

fn count(change: Option<KeyChange>, keys: &mut [usize; 3]) {
    match change {
        Some(KeyChange::Handshake { .. }) => keys[0] += 1,
        Some(KeyChange::OneRtt { .. }) => keys[1] += 1,
        None => {}
    }
}

#[test]
fn bounded_provider_refusal_preserves_flights_and_key_barriers() {
    for modes in [[false, false], [true, false], [false, true], [true, true]] {
        let (mut client, mut server) = peers();
        let (mut ck, mut sk) = ([0; 3], [0; 3]);
        for _ in 0..16 {
            let a = flight(&mut client, &mut server, &mut ck, modes[0]);
            let b = flight(&mut server, &mut client, &mut sk, modes[1]);
            if !a && !b {
                break;
            }
        }
        assert!(!client.is_handshaking() && !server.is_handshaking());
        assert_eq!(ck[..2], [1, 1]);
        assert!(
            ck[2] > 0,
            "real client must hand off keys on an empty flight"
        );
        assert_eq!(sk[..2], [1, 1]);
        assert_eq!(client.quic_transport_parameters(), Some(&[0x02][..]));
        assert_eq!(server.quic_transport_parameters(), Some(&[0x01][..]));
        let mut empty = [0xa5; 17];
        assert!(matches!(client.write_hs_bounded(&mut empty), Ok((0, None))));
        assert!(empty.iter().all(|b| *b == 0xa5));
    }
}

#[test]
fn refused_provider_can_be_dropped_without_publishing_output() {
    let (mut client, server) = peers();
    let required = match client.write_hs_bounded(&mut []) {
        Err(rustls::quic::HandshakeOutputError::Insufficient { required }) => required,
        _ => panic!("initial ClientHello must require output"),
    };
    let mut short = vec![0xa5; required - 1];
    assert!(client.write_hs_bounded(&mut short).is_err());
    assert!(short.iter().all(|b| *b == 0xa5));
    drop(client);
    assert!(server.is_handshaking());
}

#[test]
fn provider_writes_directly_into_precharged_writable_backing() {
    use bytes::{BackingBudget, BackingMeter, BytesMut, MeteredCopyError};
    // A fixture capacity, not a universal TLS output bound. Production must use
    // typed Insufficient and its own control credit before each barrier call.
    const CAPACITY: usize = 16 * 1024;
    let physical = BytesMut::metered_copy_layout_for_len(CAPACITY).unwrap();
    let budget = BackingBudget::new(physical).unwrap();
    let meter = BackingMeter::with_budget(&budget);
    let (mut client, mut server) = peers();
    let (mut client_keys, mut server_keys) = ([0; 3], [0; 3]);
    for _ in 0..16 {
        let mut made_progress = false;
        for client_turn in [true, false] {
            let (from, to, keys) = if client_turn {
                (&mut client, &mut server, &mut client_keys)
            } else {
                (&mut server, &mut client, &mut server_keys)
            };
            // Even an empty key-only barrier is called only after backing
            // admission. This provider test has no Connection metadata owner.
            let mut output = BytesMut::try_zeroed_metered(CAPACITY, &meter).unwrap();
            assert_eq!(budget.used(), physical);
            assert!(matches!(
                BytesMut::try_zeroed_metered(1, &meter),
                Err(MeteredCopyError::BudgetFull)
            ));
            let (written, change) = from.write_hs_bounded(&mut output).unwrap();
            made_progress |= written > 0 || change.is_some();
            keys[2] += usize::from(written == 0 && change.is_some());
            count(change, keys);
            assert!(output[written..].iter().all(|byte| *byte == 0));
            output.truncate(written);
            let wire = output.freeze();
            let held = wire.clone();
            assert_eq!(meter.snapshot().allocations, 1);
            if !wire.is_empty() {
                to.read_hs(&wire).unwrap();
            }
            drop(wire);
            assert_eq!(budget.used(), physical);
            drop(held);
            assert_eq!((budget.used(), meter.snapshot().physical_bytes), (0, 0));
            assert!(!budget.quarantined());
        }
        if !made_progress {
            break;
        }
    }
    assert!(!client.is_handshaking() && !server.is_handshaking());
    assert_eq!(client_keys[..2], [1, 1]);
    assert_eq!(server_keys[..2], [1, 1]);
    assert!(client_keys[2] > 0);
    assert_eq!(client.quic_transport_parameters(), Some(&[0x02][..]));
    assert_eq!(server.quic_transport_parameters(), Some(&[0x01][..]));
}
