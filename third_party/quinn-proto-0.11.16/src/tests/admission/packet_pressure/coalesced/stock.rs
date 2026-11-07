//! Original encrypted early packets cross the actual datagram Stock boundary.
use super::*;

#[test]
fn ninth_early_packet_full_is_rejected_before_authentication_while_eight_commit() {
    let Coalesced {
        budget,
        mut pair,
        client,
        server,
        wire,
        ..
    } = fixture_with_early(false, client_config(), Some(9));
    let packets = pair
        .client_conn_mut(client)
        .packet_spaces_wire_for_probe(&wire);
    assert_eq!(packets.len(), 10);
    assert!(packets[..9]
        .iter()
        .all(|(s, _)| *s == Some(packet::SpaceId::Handshake)));
    assert_eq!(packets[9].0, Some(packet::SpaceId::Data));
    let before = (
        pair.client_conn_mut(client)
            .packet_admission_snapshot_for_probe(),
        pair.client_conn_mut(client)
            .packet_spaces_snapshot_for_probe(),
    );
    let baseline = budget.used();
    let authed = pair.client_conn_mut(client).authenticated_packets();
    let refused = pair
        .client_conn_mut(client)
        .stats()
        .udp_rx
        .admission_refused;
    for count in [9, 8] {
        let wire: Vec<_> = packets[..count]
            .iter()
            .flat_map(|(_, p)| p.iter().copied())
            .collect();
        pair.client
            .inbound
            .push_back((pair.time, None, BytesMut::from(wire.as_slice())));
        pair.client.drive_incoming(pair.time, pair.server.addr);
        let pressure = budget.try_reserve(budget.limit() - budget.used()).unwrap();
        assert!(pair.client.process_one_connection_event(client));
        assert!(pair.client_conn_mut(client).retained_error().is_none());
        if count == 9 {
            assert_eq!(
                (
                    pair.client_conn_mut(client)
                        .packet_admission_snapshot_for_probe(),
                    pair.client_conn_mut(client)
                        .packet_spaces_snapshot_for_probe()
                ),
                before
            );
            assert_eq!(pair.client_conn_mut(client).authenticated_packets(), authed);
            assert_eq!(
                pair.client_conn_mut(client)
                    .stats()
                    .udp_rx
                    .admission_refused,
                refused + 1
            );
        } else {
            assert_eq!(
                pair.client_conn_mut(client).authenticated_packets(),
                authed + 8
            );
            assert_eq!(
                pair.client_conn_mut(client)
                    .stats()
                    .udp_rx
                    .admission_refused,
                refused + 1
            );
        }
        assert!(!budget.quarantined());
        drop(pressure);
        if count == 9 {
            assert_eq!(
                budget.used(),
                baseline,
                "refused input and temporary stock actually retire"
            );
        }
    }
    assert!(!pair.drive_bounded());
    assert!(
        pair.server_conn_mut(server).retained_error().is_none(),
        "client={:?}, server={:?}",
        pair.client_conn_mut(client).retained_error(),
        pair.server_conn_mut(server).retained_error()
    );
    control_records::stop(&mut pair, &[client]);
    drop(pair);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}

#[test]
fn many_real_early_acks_and_cids_cancel_then_peer_recovers_in_both_key_phases() {
    let slab = Connection::coalesced_stock_bytes_for_probe();
    assert!(slab > 0);
    for key in [false, true] {
        // Below the slab: outer admission refuses. Exact slab: outer admission
        // succeeds, then STREAM inventory refuses and must cancel the slab.
        for headroom in [0, slab - 1, slab] {
            recovery_with_fixture(
                fixture_with_early(key, client_config(), Some(8)),
                false,
                headroom,
            );
        }
    }
}

#[test]
fn repeated_many_early_full_exits_at_original_deadline_in_both_key_phases() {
    for key in [false, true] {
        deadline(fixture_with_early(key, client_config(), Some(8)));
    }
}

#[test]
fn duplicate_early_packet_and_whole_datagram_replay_commit_once() {
    let mut fixture = fixture_with_early(false, client_config(), Some(8));
    let parts = fixture
        .pair
        .client_conn_mut(fixture.client)
        .packet_spaces_wire_for_probe(&fixture.wire);
    // Repeat original ciphertext in the same datagram; unique PNs must commit
    // once, and later replay of the whole datagram must not repeat any effects.
    let mut wire = parts[0].1.clone();
    wire.extend_from_slice(&fixture.wire);
    assert!(wire.len() <= 1472);
    fixture.wire = wire;
    recovery_with_fixture(fixture, true, 0);
}
