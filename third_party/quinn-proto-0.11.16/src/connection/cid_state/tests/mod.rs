use super::*;
use crate::{ConnectionId, ResetToken};

fn issue(state: &mut CidState, count: u64, now: Instant) {
    let ids: Vec<_> = (state.issued..state.issued + count)
        .map(|sequence| IssuedCid {
            sequence,
            id: ConnectionId::new(&[sequence as u8; 8]),
            reset_token: ResetToken::from([0; 16]),
        })
        .collect();
    state.new_cids(ids.iter().copied(), now).unwrap();
    // This helper models the immediate original writer after settlement.
    for cid in ids {
        state.mark_encoded(cid.sequence);
    }
}

#[test]
fn immediate_settlement_bounds_active_cids_across_timeout_and_retirement_bursts() {
    for initial in [1, 2] {
        for limit in 2..=crate::LOC_CID_COUNT {
            let mut now = Instant::now();
            let mut state = CidState::new(8, Some(Duration::from_secs(2)), now, initial);
            issue(&mut state, limit - initial, now);
            let mut maximum = state.active_seq.len();
            for round in 0..128 {
                now += Duration::from_secs(1);
                if state.next_timeout().is_some_and(|deadline| deadline <= now) {
                    if state.on_cid_timeout() {
                        issue(&mut state, 1, now);
                    }
                }
                maximum = maximum.max(state.active_seq.len());
                assert!(state.active_seq.len() <= limit as usize + 1);
                // The packet's events settle after its entire retirement burst,
                // before a later packet or timeout can mutate this state.
                let mut seqs: Vec<_> = state.active_seq.iter().copied().collect();
                seqs.sort_unstable();
                if round % 4 != 0 {
                    seqs.truncate(usize::from(round % 2 == 0));
                }
                let mut replenish = 0;
                for seq in seqs {
                    if state.on_cid_retirement(seq, limit).unwrap() == Some(true) {
                        replenish += 1;
                    }
                    assert_eq!(state.on_cid_retirement(seq, limit).unwrap(), None);
                }
                for _ in 0..replenish {
                    issue(&mut state, 1, now);
                }
                assert!(state.active_seq.len() <= limit as usize + 1);
            }
            assert_eq!(
                maximum,
                limit as usize + 1,
                "timeout must challenge the peer-limit-only bound"
            );
        }
    }
}

#[test]
fn generated_future_ids_are_rejected_but_holes_below_encoded_max_are_retirable() {
    let now = Instant::now();
    let mut state = CidState::new(8, None, now, 1);
    let ids = std::array::from_fn::<_, 2, _>(|index| IssuedCid {
        sequence: index as u64 + 1,
        id: ConnectionId::new(&[index as u8; 8]),
        reset_token: ResetToken::from([0; 16]),
    });
    state.new_cids(ids.iter().copied(), now).unwrap();
    let before = state.active_seq.clone();
    assert!(state.on_cid_retirement(1, 8).is_err());
    assert!(state.on_cid_retirement(2, 8).is_err());
    assert!(state.on_cid_retirement(state.issued, 8).is_err());
    assert_eq!(state.active_seq, before);
    state.mark_encoded(2);
    // Sequence 1 is conservatively possible below the original writer's max;
    // reject only future sequences, preserving delayed/duplicate retirement.
    assert_eq!(state.on_cid_retirement(1, 8).unwrap(), Some(true));
    assert_eq!(state.on_cid_retirement(1, 8).unwrap(), None);
    state.mark_encoded(1);
    assert_eq!(state.on_cid_retirement(2, 8).unwrap(), Some(true));
    assert_eq!(state.on_cid_retirement(0, 8).unwrap(), Some(true));
    assert!(state.on_cid_retirement(3, 8).is_err());
}

mod native;

impl CidState {
    pub(crate) fn snapshot_for_packet_probe(&self) -> String {
        let mut active: Vec<_> = match &self.active_seq {
            Active::Legacy(values) => values.iter().copied().collect(),
            Active::Native(values) => values.iter().flatten().copied().collect(),
        };
        active.sort_unstable();
        format!("{:?}", (self.issued, self.largest_encoded_seq, active,
            self.prev_retire_seq, self.retire_seq, self.cid_len, self.cid_lifetime,
            self.retire_timestamp.iter().map(|v| (v.sequence, v.timestamp)).collect::<Vec<_>>()))
    }
}
