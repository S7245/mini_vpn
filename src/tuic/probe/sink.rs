//! Original sink statistics loop; owns no Node, open attempt or lifecycle policy.
//! Entry `read` borrows the actual Generic carrier. Tests live in tests/sink.rs.
use crate::shared::ClientError;
use crate::upstream::RelayStream;
use std::time::Duration;
use tokio::io::AsyncReadExt;

#[derive(Debug, Clone, PartialEq)]
pub struct SinkReport {
    pub target: String,
    pub requested_duration_secs: u64,
    pub elapsed_ms: u128,
    pub bytes: u64,
    pub reads: u64,
    pub first_rx_ms: Option<u128>,
    pub max_read_gap_ms: u128,
    pub eof: bool,
}

pub(super) async fn read(
    stream: &mut RelayStream,
    target: &str,
    duration_secs: u64,
) -> Result<SinkReport, ClientError> {
    let duration = Duration::from_secs(duration_secs);
    let start = tokio::time::Instant::now();
    let deadline = start + duration;
    let mut buf = vec![0u8; 256 * 1024];
    let mut bytes = 0u64;
    let mut reads = 0u64;
    let mut first_rx_ms = None;
    let mut last_read_at = None;
    let mut max_read_gap_ms = 0u128;
    let mut eof = false;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.duration_since(now);
        let read_result = tokio::time::timeout(remaining, stream.read(&mut buf)).await;
        let n = match read_result {
            Ok(result) => result?,
            Err(_) => break,
        };
        if n == 0 {
            eof = true;
            break;
        }
        let read_at = tokio::time::Instant::now();
        bytes = bytes.saturating_add(n as u64);
        reads = reads.saturating_add(1);
        first_rx_ms.get_or_insert_with(|| read_at.duration_since(start).as_millis());
        if let Some(prev) = last_read_at {
            max_read_gap_ms = max_read_gap_ms.max(read_at.duration_since(prev).as_millis());
        }
        last_read_at = Some(read_at);
    }

    Ok(SinkReport {
        target: target.to_string(),
        requested_duration_secs: duration_secs,
        elapsed_ms: start.elapsed().as_millis(),
        bytes,
        reads,
        first_rx_ms,
        max_read_gap_ms,
        eof,
    })
}
