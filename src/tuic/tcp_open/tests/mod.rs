use super::*;
use tokio::io::AsyncWriteExt;

// Preserve the original characterization API using the production priority transition.
impl<W: TuicTcpStartupPriorityWrite> TuicTcpStartupWriter<W> {
    fn arm(mut inner: W) -> io::Result<Self> {
        let priority = StartupPriority::arm(&mut inner)?;
        Ok(Self::with_priority(inner, priority))
    }

    async fn write_connect(&mut self, connect: &[u8]) -> io::Result<()> {
        tokio::io::AsyncWriteExt::write_all(&mut self.inner, connect).await
    }
}

#[derive(Debug, Default)]
struct StartupPriorityProbeState {
    priority: i32,
    atomic_pending_once: bool,
    atomic_calls: usize,
    queued_priorities: Vec<i32>,
    writes: Vec<Vec<u8>>,
}

struct StartupPriorityProbeWriter {
    state: Arc<StdMutex<StartupPriorityProbeState>>,
}

impl AsyncWrite for StartupPriorityProbeWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.state.lock().unwrap().writes.push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

impl TuicTcpStartupPriorityWrite for StartupPriorityProbeWriter {
    fn current_priority(&mut self) -> io::Result<i32> {
        Ok(self.state.lock().unwrap().priority)
    }

    fn set_stream_priority(&mut self, priority: i32) -> io::Result<()> {
        self.state.lock().unwrap().priority = priority;
        Ok(())
    }

    fn poll_write_then_set_priority(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
        priority_after: i32,
    ) -> Poll<io::Result<usize>> {
        let mut state = self.state.lock().unwrap();
        state.atomic_calls += 1;
        if state.atomic_pending_once {
            state.atomic_pending_once = false;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let priority = state.priority;
        state.queued_priorities.push(priority);
        state.priority = priority_after;
        state.writes.push(buf.to_vec());
        Poll::Ready(Ok(buf.len()))
    }
}

#[tokio::test]
async fn tcp_startup_service_is_one_atomic_business_write_after_connect() {
    let state = Arc::new(StdMutex::new(StartupPriorityProbeState {
        priority: 7,
        atomic_pending_once: true,
        ..StartupPriorityProbeState::default()
    }));
    let probe = StartupPriorityProbeWriter {
        state: Arc::clone(&state),
    };
    let mut writer = TuicTcpStartupWriter::arm(probe).unwrap();

    writer.write_connect(b"connect").await.unwrap();
    std::future::poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, &[]))
        .await
        .unwrap();
    writer.write_all(b"first-payload").await.unwrap();
    writer.write_all(b"later-payload").await.unwrap();

    let state = state.lock().unwrap();
    assert_eq!(state.priority, 7);
    assert_eq!(state.atomic_calls, 2, "Pending and successful admission");
    assert_eq!(state.queued_priorities, vec![8]);
    assert_eq!(
        state.writes,
        vec![
            b"connect".to_vec(),
            Vec::new(),
            b"first-payload".to_vec(),
            b"later-payload".to_vec()
        ]
    );
}

mod connect;

mod pressure;
mod staged;
