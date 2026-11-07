//! Only the original socket owns sequence and payload storage. These tests use
//! its real enqueue, dispatch and process/ACK paths; the getter never mutates it.
use super::*;

fn read_only(s: &TestSocket) -> Option<(u32, usize)> {
    let before = (
        s.state,
        s.local_seq_no,
        s.remote_last_seq,
        s.tx_buffer.len(),
        s.tx_buffer.capacity(),
    );
    let first = s.send_payload_range();
    assert_eq!(s.send_payload_range(), first);
    assert_eq!(
        (
            s.state,
            s.local_seq_no,
            s.remote_last_seq,
            s.tx_buffer.len(),
            s.tx_buffer.capacity()
        ),
        before
    );
    first
}

#[test]
fn enqueue_dispatch_and_actual_ack_preserve_read_only_payload_range() {
    let mut s = socket_established();
    let start = (LOCAL_SEQ.0 as u32).wrapping_add(1);
    assert_eq!(read_only(&s), Some((start, 0)));
    assert_eq!(s.send_slice(b"abcdef"), Ok(6));
    assert_eq!(read_only(&s), Some((start, 6)));
    recv(&mut s, Instant::from_millis(0), |out| {
        let repr = out.unwrap();
        assert_eq!(repr.seq_number, LOCAL_SEQ + 1);
        assert_eq!(repr.payload, b"abcdef");
    });
    assert_eq!(read_only(&s), Some((start, 6))); // Dispatch is not ACK.
    send!(s, time 1, TcpRepr {
        seq_number: REMOTE_SEQ + 1,
        ack_number: Some(LOCAL_SEQ + 3),
        ..SEND_TEMPL
    });
    assert_eq!(read_only(&s), Some((start + 2, 4)));
    assert_eq!(s.tx_buffer.get_allocated(0, 4), b"cdef");
}

#[test]
fn both_syn_states_skip_one_control_octet_without_authorizing_admission() {
    for mut s in [socket_syn_sent(), socket_syn_received()] {
        assert!(!s.may_send());
        assert_eq!(s.send_slice(b"not-admitted"), Err(SendError::InvalidState));
        assert_eq!(read_only(&s), Some(((LOCAL_SEQ.0 as u32) + 1, 0)));
        recv(&mut s, Instant::from_millis(0), |out| {
            let repr = out.unwrap();
            assert_eq!(repr.control, TcpControl::Syn);
            assert_eq!(repr.seq_number, LOCAL_SEQ);
            assert!(repr.payload.is_empty());
        });
        assert_eq!(read_only(&s), Some(((LOCAL_SEQ.0 as u32) + 1, 0)));
    }
}

#[test]
fn actual_fin_ack_never_becomes_a_payload_refund_origin() {
    for mut s in [socket_established(), socket_close_wait()] {
        let start = s.local_seq_no;
        let remote = s.remote_seq_no;
        assert_eq!(s.send_slice(b"data"), Ok(4));
        s.close();
        assert!(!s.may_send());
        assert_eq!(read_only(&s), Some((start.0 as u32, 4)));
        recv(&mut s, Instant::from_millis(0), |out| {
            let repr = out.unwrap();
            assert_eq!(repr.control, TcpControl::Fin);
            assert_eq!(repr.payload, b"data");
            assert_eq!(repr.seq_number, start);
        });
        send!(s, time 1, TcpRepr {
            seq_number: remote, ack_number: Some(start + 4), ..SEND_TEMPL
        });
        assert_eq!(read_only(&s), Some(((start + 4).0 as u32, 0)));
        send!(s, time 2, TcpRepr {
            seq_number: remote, ack_number: Some(start + 5), ..SEND_TEMPL
        });
        assert!(matches!(s.state, State::FinWait2 | State::Closed));
        assert_eq!(s.send_queue(), 0);
        assert_eq!(read_only(&s), None);
    }
}

#[test]
fn actual_ack_across_u32_wrap_returns_unsigned_payload_origin() {
    let mut s = socket_established();
    // The original test fixture chooses an initial TCP sequence near wrap;
    // subsequent buffer changes and ACK are the original socket operations.
    s.local_seq_no = TcpSeqNumber(-2);
    s.remote_last_seq = s.local_seq_no;
    assert_eq!(s.send_slice(b"wrap"), Ok(4));
    assert_eq!(read_only(&s), Some((u32::MAX - 1, 4)));
    recv(&mut s, Instant::from_millis(0), |out| {
        let repr = out.unwrap();
        assert_eq!(repr.seq_number, TcpSeqNumber(-2));
        assert_eq!(repr.payload, b"wrap");
    });
    send!(s, time 1, TcpRepr {
        seq_number: REMOTE_SEQ + 1, ack_number: Some(TcpSeqNumber(1)), ..SEND_TEMPL
    });
    assert_eq!(read_only(&s), Some((1, 1)));
    assert_eq!(s.tx_buffer.get_allocated(0, 1), b"p");
}

#[test]
fn absent_or_fin_acked_socket_never_fabricates_an_empty_origin() {
    for s in [
        socket(),
        socket_listen(),
        socket_fin_wait_2(),
        socket_time_wait(false),
    ] {
        assert_eq!(read_only(&s), None);
    }
    let s = socket_closing();
    assert_eq!(read_only(&s), Some(((LOCAL_SEQ + 1).0 as u32, 0)));
    assert!(!s.may_send());
}
