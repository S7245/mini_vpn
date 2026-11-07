//! Real Quinn Session crypto adapters; not Connection-budget/Root acceptance.
use super::*;
use crate::crypto::{HandshakeOutputError, Session};

fn transfer(
    from: &mut dyn Session,
    to: &mut dyn Session,
    bounded: bool,
    keys: &mut [usize; 2],
) -> bool {
    let (output, changed) = if bounded {
        match from.write_handshake_bounded(&mut []) {
            Ok((0, changed)) => (Vec::new(), changed),
            Err(HandshakeOutputError::Insufficient { required }) => {
                let mut short = vec![0xa5; required - 1];
                assert!(matches!(from.write_handshake_bounded(&mut short),
                    Err(HandshakeOutputError::Insufficient { required: n }) if n == required));
                assert!(short.iter().all(|b| *b == 0xa5));
                let mut output = vec![0xa5; required + 8];
                let (written, changed) = from.write_handshake_bounded(&mut output).unwrap();
                assert_eq!(written, required);
                assert!(output[written..].iter().all(|b| *b == 0xa5));
                output.truncate(written);
                (output, changed)
            }
            _ => panic!("unexpected bounded session result"),
        }
    } else {
        let mut output = Vec::new();
        let changed = from.write_handshake(&mut output);
        (output, changed)
    };
    keys[0] += usize::from(changed.is_some());
    keys[1] += usize::from(output.is_empty() && changed.is_some());
    let progress = !output.is_empty() || changed.is_some();
    if !output.is_empty() {
        to.read_handshake(&output).unwrap();
    }
    progress
}

fn exchange_key(sender: &dyn crate::crypto::PacketKey, receiver: &dyn crate::crypto::PacketKey) {
    let header = b"session-key-header";
    let plain = b"actual next traffic secret";
    let mut wire = [header.as_slice(), plain.as_slice()].concat();
    wire.resize(wire.len() + sender.tag_len(), 0);
    sender.encrypt(7, &mut wire, header.len());
    let mut payload = BytesMut::from(&wire[header.len()..]);
    receiver.decrypt(7, header, &mut payload).unwrap();
    assert_eq!(payload.as_ref(), plain);
}

#[test]
fn bounded_quinn_sessions_keep_handshake_and_next_key_ownership() {
    for modes in [[false, false], [false, true], [true, false], [true, true]] {
        let params = TransportParameters::default();
        let mut client = client_config()
            .crypto
            .start_session(1, "localhost", &params)
            .unwrap();
        let mut server = server_config().crypto.start_session(1, &params);
        let (mut ck, mut sk) = ([0; 2], [0; 2]);
        for _ in 0..16 {
            let a = transfer(client.as_mut(), server.as_mut(), modes[0], &mut ck);
            let b = transfer(server.as_mut(), client.as_mut(), modes[1], &mut sk);
            if !a && !b {
                break;
            }
        }
        assert!(!client.is_handshaking() && !server.is_handshaking());
        assert_eq!((ck[0], sk[0]), (2, 2));
        assert!(ck[1] > 0, "client must transfer keys on an empty flight");
        assert!(client.transport_parameters().unwrap().is_some());
        assert!(server.transport_parameters().unwrap().is_some());
        for _ in 0..2 {
            let client_keys = client.next_1rtt_keys().unwrap();
            let server_keys = server.next_1rtt_keys().unwrap();
            exchange_key(client_keys.local.as_ref(), server_keys.remote.as_ref());
            exchange_key(server_keys.local.as_ref(), client_keys.remote.as_ref());
        }
    }
}
