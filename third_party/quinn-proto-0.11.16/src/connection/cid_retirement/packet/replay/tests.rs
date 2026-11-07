//! Differential oracle: projection is cancelled, then the same original
//! Connection executes on_ack_received. Controlled clocks/early-space record
//! placement exercise the algorithm; cid_loss separately proves encrypted wire.
use super::*;
use crate::connection::sent_history::PacketClass;

fn one(pn: u64) -> frame::Ack {
    frame::Ack {
        largest: pn,
        delay: 0,
        additional: Bytes::from_static(&[0]),
        ecn: None,
    }
}
fn clock(rtt: RttEstimator) -> [Duration; 4] {
    [rtt.get(), rtt.conservative(), rtt.min(), rtt.pto_base()]
}
impl Connection {
    pub(crate) fn assert_cid_replay_for_probe(
        &mut self,
        now: Instant,
        actual: frame::Ack,
        case: u8,
    ) {
        let mut steps = Vec::new();
        let mut data_pns: Vec<_> = self.spaces[SpaceId::Data]
            .sent_packets
            .iter()
            .map(|(&pn, _)| pn)
            .collect();
        assert!(data_pns.len() >= 4);
        if case != 0 {
            self.path
                .rtt
                .update(Duration::ZERO, Duration::from_millis(40));
            // Nonmonotonic times disallow a prefix approximation of loss.
            for (i, (_, info)) in self.spaces[SpaceId::Data]
                .sent_packets
                .iter_mut()
                .enumerate()
            {
                info.time_sent = now - Duration::from_millis([80, 5, 60, 1][i % 4]);
            }
        }
        if case == 3 {
            // Move an actual PING record into early space without cloning its
            // inventory or changing path in-flight accounting.
            let pn = data_pns.pop().unwrap();
            let record = self.spaces[SpaceId::Data].take(pn).unwrap();
            assert_eq!(
                record.retransmits.get().map_or(0, |r| r.retire_cids.len()),
                0
            );
            let early = &mut self.spaces[SpaceId::Handshake];
            let number = early.next_packet_number;
            early.next_packet_number += 1;
            let mut permit = early.sent_packets.reserve(PacketClass::Control).unwrap();
            permit.begin_encoding().unwrap();
            assert!(early.sent(number, record, permit).is_ok());
            steps.push((SpaceId::Handshake, one(number)));
        }
        if case == 5 {
            let probe = *data_pns.last().unwrap();
            self.path.mtud = crate::connection::mtud::MtuDiscovery::new(
                1200,
                1200,
                Some(1472),
                crate::MtuDiscoveryConfig::default(),
            );
            assert!(self.path.mtud.poll_transmit(now, probe).is_some());
            assert_eq!(self.path.mtud.in_flight_mtu_probe(), Some(probe));
            // Distinct spaces may legally reuse the same PN. Transfer two
            // actual control records into Handshake, retaining path accounting.
            for index in 0..2 {
                let source = data_pns.remove(data_pns.len() - 2);
                let mut record = self.spaces[SpaceId::Data].take(source).unwrap();
                assert_eq!(
                    record.retransmits.get().map_or(0, |r| r.retire_cids.len()),
                    0
                );
                record.time_sent = now - Duration::from_millis(if index == 0 { 80 } else { 1 });
                let early = &mut self.spaces[SpaceId::Handshake];
                let mut permit = early.sent_packets.reserve(PacketClass::Control).unwrap();
                permit.begin_encoding().unwrap();
                assert!(early.sent(probe + index, record, permit).is_ok());
            }
            self.spaces[SpaceId::Handshake].next_packet_number = probe + 2;
            steps.push((SpaceId::Handshake, one(probe + 1)));
        }
        let largest = *data_pns.last().unwrap();
        if case == 2 {
            self.ack_frequency
                .ack_frequency_sent(largest, Duration::from_millis(3));
        }
        if case == 4 {
            // An ACK-only packet can already have been forgotten by History.
            let absent = self.spaces[SpaceId::Data].next_packet_number;
            self.spaces[SpaceId::Data].next_packet_number += 1;
            steps.push((SpaceId::Data, one(absent)));
        }
        let mut ack = if case == 0 { actual } else { one(largest) };
        ack.delay = 10_000;
        steps.push((SpaceId::Data, ack.clone()));
        steps.push((SpaceId::Data, ack)); // exact duplicate selects nothing
        steps.push((SpaceId::Data, one(data_pns[0]))); // overlap/late ACK of loss
        let identity = self.spaces.each_ref().map(|s| {
            s.sent_packets
                .iter()
                .map(|(&pn, info)| (pn, info as *const _ as usize))
                .collect::<Vec<_>>()
        });
        let pns = identity
            .each_ref()
            .map(|s| s.iter().map(|x| x.0).collect::<Vec<_>>());
        let original_clock = clock(self.path.rtt);
        let original_pending = self.spaces.each_ref().map(|s| s.pending.retire_cids.len());
        let projected = {
            let mut replay = Replay::new(self, now).unwrap();
            let mut projected = Vec::new();
            for (space, ack) in &steps {
                let added = if *space == SpaceId::Handshake {
                    let mut ranges = crate::range_set::ArrayRangeSet::new();
                    ranges.insert(ack.largest..ack.largest + 1);
                    let mut bytes = Vec::new();
                    frame::Ack::encode(ack.delay, &ranges, None, &mut bytes);
                    let bytes = Bytes::from(bytes);
                    // Incoming PN was not seen before. This is the same early
                    // entry used by the coalesced prepare call chain.
                    replay.early(*space, u64::MAX / 2, &bytes).unwrap();
                    0
                } else {
                    replay.ack(*space, ack.clone()).unwrap()
                };
                let alive: [Vec<u64>; 3] = std::array::from_fn(|i| {
                    pns[i]
                        .iter()
                        .copied()
                        .filter(|&pn| replay.spaces[i].history.get(pn).is_some())
                        .collect()
                });
                projected.push((
                    added,
                    alive,
                    clock(replay.rtt),
                    replay.frequency.peer_max_ack_delay,
                    replay
                        .spaces
                        .each_ref()
                        .map(|s| (s.largest, s.largest_sent)),
                ));
            }
            projected
        };
        assert_eq!(clock(self.path.rtt), original_clock);
        assert_eq!(
            self.spaces.each_ref().map(|s| s.pending.retire_cids.len()),
            original_pending
        );
        assert_eq!(
            self.spaces.each_ref().map(|s| s
                .sent_packets
                .iter()
                .map(|(&pn, info)| {
                    assert_eq!(info.recovery_selection, 0);
                    (pn, info as *const _ as usize)
                })
                .collect::<Vec<_>>()),
            identity
        );
        for ((space, ack), (added, alive, rtt, delay, largest)) in steps.into_iter().zip(projected)
        {
            let before = self.spaces[space].pending.retire_cids.len();
            self.on_ack_received(now, space, ack).unwrap();
            assert_eq!(
                self.spaces[space].pending.retire_cids.len() - before,
                added,
                "case {case}: retirement count"
            );
            assert_eq!(
                self.spaces.each_ref().map(|s| s
                    .sent_packets
                    .iter()
                    .map(|(&pn, _)| pn)
                    .collect::<Vec<_>>()),
                alive,
                "case {case}: actual record selection"
            );
            assert_eq!(clock(self.path.rtt), rtt, "case {case}: shared RTT");
            assert_eq!(self.ack_frequency.peer_max_ack_delay, delay);
            assert_eq!(
                self.spaces
                    .each_ref()
                    .map(|s| (s.largest_acked_packet, s.largest_acked_packet_sent)),
                largest
            );
        }
    }
}

impl Connection {
    pub(crate) fn assert_cids_before_ack_for_probe(
        &mut self,
        now: Instant,
        frames: Vec<frame::Frame>,
    ) {
        let mut wire = Vec::new();
        for f in &frames {
            if let frame::Frame::NewConnectionId(cid) = f {
                cid.encode(&mut wire);
            }
        }
        for f in frames {
            if let frame::Frame::Ack(ack) = f {
                let mut ranges = crate::range_set::ArrayRangeSet::new();
                for r in ack.iter() {
                    ranges.insert(*r.start()..r.end() + 1);
                }
                frame::Ack::encode(ack.delay, &ranges, ack.ecn.as_ref(), &mut wire);
            }
        }
        let payload = Bytes::from(wire);
        self.check_packet_cids(now, &payload, std::iter::empty())
            .unwrap();
        let meter = self.endpoint_config.receive_backing_meter.clone().unwrap();
        let mut prepared = self
            .prepare_packet_cids(now, &payload, &meter, std::iter::empty())
            .unwrap_or_else(|_| panic!("valid ordered CID preparation"));
        for f in frame::Iter::new(payload).unwrap() {
            match f.unwrap() {
                frame::Frame::NewConnectionId(_) => prepared.commit_next(self, now).unwrap(),
                frame::Frame::Ack(ack) => self.on_ack_received(now, SpaceId::Data, ack).unwrap(),
                _ => unreachable!(),
            }
        }
        assert_eq!(
            self.spaces[SpaceId::Data].pending.retire_cids.len(),
            51,
            "original limit applies at CID insertion, before later ACK loss"
        );
    }
}
