//! STREAM frame custody in the existing Connection pool. The ordinary class
//! cannot take control nodes. PreparedStreams owns the first pre-PN credit;
//! each later credit precedes pending/offset/FIN mutation. Original ACK/loss
//! callbacks retain their node until settlement, including interrupted Q.
//! Legacy keeps its single-frame inline representation and grows only at two.
//! Tests: records/tests and tests/admission/{metadata,stream_records}.
use super::*;

#[derive(Clone, Copy)]
pub(in crate::connection) struct StreamRecord {
    id: StreamId,
    start: u64,
    end: u64,
    fin: bool,
}
impl StreamRecord {
    fn placeholder() -> Self {
        Self {
            id: StreamId::from(VarInt::from_u32(0)),
            start: 0,
            end: 0,
            fin: false,
        }
    }
    fn frame(self) -> frame::StreamMeta {
        frame::StreamMeta {
            id: self.id,
            offsets: self.start..self.end,
            fin: self.fin,
        }
    }
}
impl From<frame::StreamMeta> for StreamRecord {
    fn from(frame: frame::StreamMeta) -> Self {
        Self {
            id: frame.id,
            start: frame.offsets.start,
            end: frame.offsets.end,
            fin: frame.fin,
        }
    }
}
impl Item for StreamRecord {
    const CLASS: usize = 1;
    fn pack(self) -> Value {
        Value::Stream(self)
    }
    fn unpack(value: Value) -> Option<Self> {
        if let Value::Stream(v) = value {
            Some(v)
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub(in crate::connection) enum StreamFrames {
    Inline(Option<frame::StreamMeta>),
    Many(Vec<frame::StreamMeta>),
    Cold(Queue<StreamRecord>),
}
impl Default for StreamFrames {
    fn default() -> Self {
        Self::Inline(None)
    }
}
impl StreamFrames {
    pub(in crate::connection) fn len(&self) -> usize {
        match self {
            Self::Inline(frame) => usize::from(frame.is_some()),
            Self::Many(frames) => frames.len(),
            Self::Cold(frames) => frames.len(),
        }
    }
    pub(in crate::connection) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(in crate::connection) fn iter(&self) -> StreamIter<'_> {
        match self {
            Self::Inline(frame) => StreamIter::Inline(frame.as_ref()),
            Self::Many(frames) => StreamIter::Many(frames.iter()),
            Self::Cold(frames) => StreamIter::Cold(frames.iter()),
        }
    }
    fn prepare(&self) -> Result<Entry<StreamRecord>, ()> {
        match self {
            Self::Cold(frames) => frames.prepare(StreamRecord::placeholder()),
            _ => Ok(Entry {
                value: StreamRecord::placeholder(),
                slot: None,
            }),
        }
    }
    pub(in crate::connection) fn authority_ready(&self) -> bool {
        match self {
            Self::Cold(frames) => frames.authority_ready(),
            _ => true,
        }
    }
    fn push(&mut self, record: Entry<StreamRecord>) {
        if let Self::Cold(frames) = self {
            frames.push(record);
            return;
        }
        // A cold ticket must never escape to an unmetered legacy collection.
        if let Some(slot) = &record.slot {
            slot.pool.quarantine();
            return;
        }
        let frame = record.value.frame();
        match self {
            Self::Inline(None) => *self = Self::Inline(Some(frame)),
            Self::Inline(previous) => {
                let mut frames = Vec::with_capacity(2);
                if let Some(previous) = previous.take() {
                    frames.push(previous);
                }
                frames.push(frame);
                *self = Self::Many(frames);
            }
            Self::Many(frames) => frames.push(frame),
            Self::Cold(frames) => frames.push(record),
        }
    }
    pub(in crate::connection) fn dispose_with(self, mut settle: impl FnMut(frame::StreamMeta) -> Disposition) {
        match self {
            Self::Inline(frame) => {
                if let Some(frame) = frame {
                    settle(frame);
                }
            }
            Self::Many(frames) => {
                for frame in frames {
                    settle(frame);
                }
            }
            Self::Cold(mut frames) => {
                while let Some(mut record) = frames.pop_front() {
                    record.begin_mutation();
                    settle(record.value.frame());
                    // Original ACK/loss success or explicit Connection close
                    // disposal consumes this metadata; unwind retains shared Q.
                    if let Some(slot) = &mut record.slot {
                        slot.phase = Phase::Prepared;
                    }
                }
            }
        }
    }
}
pub(in crate::connection) enum StreamIter<'a> {
    Inline(Option<&'a frame::StreamMeta>),
    Many(std::slice::Iter<'a, frame::StreamMeta>),
    Cold(Iter<'a, StreamRecord>),
}
impl Iterator for StreamIter<'_> {
    type Item = frame::StreamMeta;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(frame) => frame.take().cloned(),
            Self::Many(frames) => frames.next().cloned(),
            Self::Cold(frames) => frames.next().map(StreamRecord::frame),
        }
    }
}

pub(in crate::connection) struct PreparedStreams {
    frames: StreamFrames,
    first: Option<Entry<StreamRecord>>,
}
impl Default for PreparedStreams {
    fn default() -> Self {
        Self {
            frames: StreamFrames::default(),
            first: None,
        }
    }
}
impl PreparedStreams {
    pub(in crate::connection) fn ready(owner: &Queue<(StreamId, VarInt)>) -> bool {
        match &owner.mode {
            Mode::Legacy(_) => true,
            Mode::Cold(chain, _) => chain.pool.available(StreamRecord::CLASS),
        }
    }
    pub(in crate::connection) fn prepare(
        owner: &Queue<(StreamId, VarInt)>,
        wants_stream: bool,
    ) -> Result<Self, ()> {
        let frames = match &owner.mode {
            Mode::Legacy(_) => StreamFrames::default(),
            Mode::Cold(chain, _) => StreamFrames::Cold(Queue::new(Some(&chain.pool))),
        };
        let first = if wants_stream {
            match frames.prepare() {
                Ok(credit) => Some(credit),
                Err(()) if frames.authority_ready() => None, // Ordinary Full.
                Err(()) => return Err(()),
            }
        } else {
            None
        };
        Ok(Self { frames, first })
    }
    pub(in crate::connection) fn next_credit(&mut self) -> Result<Option<Entry<StreamRecord>>, ()> {
        if !self.frames.authority_ready() {
            return Err(());
        }
        if let Some(credit) = self.first.take() {
            return Ok(Some(credit));
        }
        match self.frames.prepare() {
            Ok(credit) => Ok(Some(credit)),
            Err(()) if self.frames.authority_ready() => Ok(None),
            Err(()) => Err(()),
        }
    }
    pub(in crate::connection) fn encoded(
        &mut self,
        mut credit: Entry<StreamRecord>,
        frame: frame::StreamMeta,
    ) {
        credit.value = frame.into();
        self.frames.push(credit);
    }
    pub(in crate::connection) fn finish(self) -> StreamFrames {
        self.frames
    }
}

pub(in crate::connection) enum Disposition { Applied, Closed }
