//! Saved probe composition owns original Node, job and staged-open custody.
//! Public callers receive only an owner, one stop observer and value reports.
//! No DATA escapes; no UDP actor starts for TCP-only jobs. Unknown poll/drop
//! retains the original saved work in quarantine. Entries: `prepare_sink/run`;
//! real endpoint and lifecycle tracers live in tests/.
use super::{NodeHost, NodeShutdown, NodeStartup, TuicClientConfig, TuicUpstream};
use crate::metrics::Metrics;
use crate::shared::{ClientError, TargetAddr};
use std::future::Future;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;
mod child;
pub(crate) mod direct;
mod open;
mod sink;
pub use sink::SinkReport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Starting,
    Running,
    Stopping,
    Incomplete,
    Complete,
    Stopped,
    Failed,
    Quarantined,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Observation {
    pub phase: Phase,
    pub sink: Option<SinkReport>,
    pub node: Option<NodeShutdown>,
}

struct Control {
    #[cfg(test)]
    facts: Arc<tests::faults::Facts>,
    stopped: AtomicBool,
    phase: Mutex<Phase>,
    deadline: Mutex<Option<tokio::time::Instant>>,
    changed: Notify,
}
impl Control {
    fn local() -> Arc<Self> {
        Arc::new(Self {
            #[cfg(test)]
            facts: Arc::new(tests::faults::Facts::default()),
            stopped: AtomicBool::new(false),
            phase: Mutex::new(Phase::Starting),
            deadline: Mutex::new(None),
            changed: Notify::new(),
        })
    }
    fn phase(&self) -> Phase {
        *self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn publish(&self, phase: Phase) {
        if matches!(phase, Phase::Stopping | Phase::Quarantined) {
            self.begin_retirement();
        }
        let mut saved = self
            .phase
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *saved != Phase::Quarantined {
            *saved = phase;
        }
        self.changed.notify_waiters();
    }
    fn begin_retirement(&self) -> tokio::time::Instant {
        let deadline = *self
            .deadline
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get_or_insert_with(|| tokio::time::Instant::now() + Duration::from_secs(15));
        self.changed.notify_waiters();
        deadline
    }
    fn request_stop(&self) -> tokio::time::Instant {
        let deadline = self.begin_retirement();
        self.stopped.store(true, Ordering::Release);
        self.changed.notify_waiters();
        deadline
    }
    // Only the aggregate root uses this after retaining its original owners.
    // A local relay Q must still let useful siblings finish their own work.
    fn quarantine_aggregate(&self) {
        self.request_stop();
        self.publish(Phase::Quarantined);
    }
    async fn stop(&self) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.stopped.load(Ordering::Acquire) {
                return;
            }
            changed.await;
        }
    }
}

/// A single observer. Cancelling its borrow never cancels original owned work.
pub struct Handle(Arc<Control>);
impl Handle {
    pub fn request_stop(&self) {
        self.0.request_stop();
    }
    pub async fn observe_shutdown(&self) -> Phase {
        let deadline = self.0.request_stop();
        tokio::select! {
            biased;
            phase=async {
                loop {
                    let changed=self.0.changed.notified();tokio::pin!(changed);changed.as_mut().enable();
                    let phase=self.0.phase();
                    if matches!(phase,Phase::Complete|Phase::Stopped|Phase::Failed|Phase::Quarantined) {return phase;}
                    changed.await;
                }
            }=>phase,
            _=tokio::time::sleep_until(deadline)=>Phase::Incomplete,
        }
    }
}
impl Drop for Handle {
    fn drop(&mut self) {
        self.request_stop();
    }
}

// These helpers own only a borrow or a disposal argument. Physical authorities
// remain in their caller, outside the unwind boundary.
async fn guard_poll<F: Future + ?Sized>(mut work: Pin<&mut F>) -> std::thread::Result<F::Output> {
    std::future::poll_fn(|cx| {
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| work.as_mut().poll(cx))) {
            Ok(Poll::Pending) => Poll::Pending,
            Ok(Poll::Ready(value)) => Poll::Ready(Ok(value)),
            Err(payload) => Poll::Ready(Err(payload)),
        }
    })
    .await
}
fn guard_drop<T>(value: T) -> std::thread::Result<()> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)))
}

struct Finished {
    direct: Option<direct::Finished>,
    sink: Option<SinkReport>,
    error: Option<ClientError>,
    open: Option<open::Open>,
    stopped: bool,
}
impl Finished {
    fn failure(&self) -> Option<&ClientError> {
        self.error
            .as_ref()
            .or_else(|| self.open.as_ref().and_then(open::Open::failure))
            .or_else(|| self.direct.as_ref().and_then(direct::Finished::failure))
    }
    fn settled(&self) -> bool {
        self.open.as_ref().is_none_or(open::Open::settled)
            && self.direct.as_ref().is_none_or(direct::Finished::settled)
    }
}
enum Job {
    Direct(direct::Spec),
    Sink {
        target: TargetAddr,
        label: String,
        duration_secs: u64,
    },
}
impl Job {
    async fn run(self, data: Arc<TuicUpstream>, control: Arc<Control>) -> Finished {
        #[cfg(test)]
        tests::faults::gate(&control).await;
        match self {
            Self::Direct(spec) => {
                let result = direct::run(spec, data, control.clone()).await;
                Finished {
                    direct: Some(result),
                    sink: None,
                    error: None,
                    open: None,
                    stopped: control.stopped.load(Ordering::Acquire),
                }
            }
            Self::Sink {
                target,
                label,
                duration_secs,
            } => {
                let mut open = match open::Open::new(data, target, control.clone()) {
                    Ok(open) => open,
                    Err(error) => {
                        return Finished {
                            direct: None,
                            sink: None,
                            error: Some(error),
                            open: None,
                            stopped: false,
                        };
                    }
                };
                let mut report = None;
                let mut error = None;
                if open.acquire(&control).await {
                    if let Some(stream) = open.stream() {
                        // Original Open/carrier lives outside both borrowed read
                        // catches. Observer cancellation retains this entire job.
                        let mut read = Box::pin(sink::read(stream, &label, duration_secs));
                        let observed = tokio::select! {biased;
                            _=control.stop()=>None,
                            result=guard_poll(read.as_mut())=>Some(result),
                        };
                        match observed {
                            Some(Err(payload)) => {
                                // Preserve poisoned borrowed future storage. Its
                                // original carrier remains in Open, never repolled.
                                std::mem::forget(read);
                                open.quarantine(payload);
                            }
                            other => {
                                control.begin_retirement();
                                match other {
                                    Some(Ok(Ok(value))) => report = Some(value),
                                    Some(Ok(Err(value))) => error = Some(value),
                                    _ => {}
                                }
                                if let Err(payload) = guard_drop(read) {
                                    open.quarantine(payload);
                                }
                            }
                        }
                    }
                }
                open.finish().await;
                Finished {
                    direct: None,
                    sink: report,
                    error,
                    open: Some(open),
                    stopped: control.stopped.load(Ordering::Acquire),
                }
            }
        }
    }
}

struct Completion {
    _node: NodeHost,
    job: Option<Finished>,
    error: Option<ClientError>,
    observation: Observation,
}
type Saved = Pin<Box<dyn Future<Output = Completion> + Send>>;

/// One saved cold driver, not a DATA wrapper or spawned task. The cold future
/// uses the same boxed representation as NodeHost; stream dispatch is unchanged.
pub struct Probe {
    driver: Option<Saved>,
    completion: Option<Completion>,
    control: Arc<Control>,
    handle_issued: bool,
    // Never recursively invoke an unknown panic payload's destructor.
    panic: Option<ManuallyDrop<Box<dyn std::any::Any + Send>>>,
}
impl Probe {
    pub fn take_handle(&mut self) -> Option<Handle> {
        if self.handle_issued {
            return None;
        }
        self.handle_issued = true;
        Some(Handle(self.control.clone()))
    }
    pub(crate) fn direct_report(&self) -> Option<&direct::Report> {
        self.completion
            .as_ref()?
            .job
            .as_ref()?
            .direct
            .as_ref()
            .map(|d| &d.report)
    }
    pub fn failure(&self) -> Option<&ClientError> {
        self.completion.as_ref().and_then(|c| {
            c.error
                .as_ref()
                .or_else(|| c.job.as_ref().and_then(Finished::failure))
        })
    }
    pub async fn run(&mut self) -> Observation {
        std::future::poll_fn(|cx| {
            if let Some(done) = &self.completion {
                return Poll::Ready(done.observation.clone());
            }
            if self.control.phase() == Phase::Quarantined {
                return Poll::Ready(Observation {
                    phase: Phase::Quarantined,
                    sink: None,
                    node: None,
                });
            }
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                self.driver.as_mut().map(|work| work.as_mut().poll(cx))
            }));
            match result {
                Ok(Some(Poll::Ready(done))) => {
                    // Install actual original diagnostic owners BEFORE future disposal.
                    self.completion = Some(done);
                    let work = self.driver.take();
                    if let Err(payload) =
                        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(work)))
                    {
                        self.panic = Some(ManuallyDrop::new(payload));
                        self.control.quarantine_aggregate();
                        if let Some(done) = &mut self.completion {
                            done.observation.phase = Phase::Quarantined;
                        }
                    }
                    if let Some(done) = &self.completion {
                        self.control.publish(done.observation.phase);
                        return Poll::Ready(done.observation.clone());
                    }
                }
                Err(payload) => {
                    self.panic = Some(ManuallyDrop::new(payload));
                    self.control.quarantine_aggregate();
                }
                Ok(None) => self.control.quarantine_aggregate(),
                Ok(Some(Poll::Pending)) => {}
            }
            if self.control.phase() == Phase::Quarantined {
                Poll::Ready(Observation {
                    phase: Phase::Quarantined,
                    sink: None,
                    node: None,
                })
            } else {
                Poll::Pending
            }
        })
        .await
    }
}
impl Drop for Probe {
    fn drop(&mut self) {
        self.control.request_stop();
        if let Some(work) = self.driver.take() {
            // Forced owner destruction cannot join. Retain charged original work,
            // including poisoned future storage, rather than destroy it as clean.
            self.control.publish(Phase::Quarantined);
            std::mem::forget(work);
        }
    }
}

pub fn prepare_sink(
    config: TuicClientConfig,
    target: &str,
    duration_secs: u64,
    metrics: Arc<Metrics>,
) -> Result<Probe, ClientError> {
    if !(1..=600).contains(&duration_secs) {
        return Err(ClientError::InvalidTarget(
            "sink duration must be1..600s".into(),
        ));
    }
    let parsed = TargetAddr::parse(target)?;
    if matches!(&parsed,TargetAddr::DomainPort{host,..} if host.is_empty()||host.len()>255) {
        return Err(ClientError::InvalidTarget(
            "sink domain must be1..255bytes".into(),
        ));
    }
    Ok(prepare(
        config,
        Job::Sink {
            target: parsed,
            label: target.to_string(),
            duration_secs,
        },
        metrics,
    ))
}
pub(crate) fn prepare_direct(
    config: TuicClientConfig,
    target: TargetAddr,
    duration_secs: u64,
    max_connections: usize,
    command: tokio::process::Command,
    metrics: Arc<Metrics>,
    floor_mbps: Option<f64>,
) -> Result<Probe, ClientError> {
    if !(1..=30).contains(&duration_secs) || !(1..=32).contains(&max_connections) {
        return Err(ClientError::InvalidTarget(
            "direct duration1..30/max_connections1..32".into(),
        ));
    }
    Ok(prepare(
        config,
        Job::Direct(direct::Spec {
            target,
            duration_secs,
            max_connections,
            command,
            floor_mbps,
        }),
        metrics,
    ))
}
fn prepare(config: TuicClientConfig, job: Job, metrics: Arc<Metrics>) -> Probe {
    let policy = Arc::new(config.relay_policy().generic_tuic_transport());
    let config = config.with_relay_policy(policy);
    let control = Control::local();
    let saved = control.clone();
    let driver = Box::pin(async move {
        let mut node = NodeHost::prepare(config, metrics);
        let started = tokio::select! {biased; _=saved.stop()=>NodeStartup::Stopping,result=node.start()=>result};
        let mut error = None;
        let mut finished = None;
        let mut node_failed = false;
        if started == NodeStartup::Ready {
            match node.data() {
                Ok(data) => {
                    #[cfg(test)]
                    {
                        *saved.facts.data.lock().unwrap() = Arc::downgrade(&data);
                        *saved.facts.stats_abort.lock().unwrap() =
                            node.probe_stats_abort_for_test();
                    }
                    saved.publish(Phase::Running);
                    let work = job.run(data.clone(), saved.clone());
                    #[cfg(test)]
                    let work = tests::job_unwind::wrap(work, saved.clone());
                    let mut work = Box::pin(work);
                    let done = loop {
                        tokio::select! {
                            biased;
                            result=guard_poll(work.as_mut())=>break result,
                            result=node.run(),if !node_failed=>{
                                error=result.err();node_failed=true;saved.request_stop();
                            }
                        }
                    };
                    let done = match done {
                        Ok(done) => done,
                        Err(payload) => {
                            #[cfg(test)]
                            tests::job_unwind::retained(&saved, None, &payload);
                            let _charged = (ManuallyDrop::new(payload), work, data, node, error);
                            saved.quarantine_aggregate();
                            return std::future::pending::<Completion>().await;
                        }
                    };
                    // Finished and original Open are now outside work disposal.
                    if let Err(payload) = guard_drop(work) {
                        #[cfg(test)]
                        tests::job_unwind::retained(&saved, Some(&done), &payload);
                        let _charged = (ManuallyDrop::new(payload), done, data, node, error);
                        saved.quarantine_aggregate();
                        return std::future::pending::<Completion>().await;
                    }
                    // Read original provider failure while its storage is still
                    // held; a defensive accessor fault also blocks Node cleanup.
                    let _ = done.failure();
                    if !done.settled() {
                        let _charged = (done, data, node, error);
                        saved.quarantine_aggregate();
                        return std::future::pending::<Completion>().await;
                    }
                    finished = Some(done);
                    drop(data);
                }
                Err(value) => error = Some(value),
            }
        }
        let stopped =
            started == NodeStartup::Stopping || finished.as_ref().is_some_and(|f| f.stopped);
        saved.request_stop();
        saved.publish(Phase::Stopping);
        #[cfg(test)]
        if saved.facts.known_cleanup_failure.load(Ordering::Acquire) {
            node.record_known_failure_for_probe_test();
        }
        let shutdown = node.drive_shutdown().await;
        let phase = if shutdown == NodeShutdown::Quarantined {
            Phase::Quarantined
        } else if shutdown == NodeShutdown::Incomplete {
            Phase::Incomplete
        } else if shutdown == NodeShutdown::FailedSettled
            || node_failed
            || started == NodeStartup::Failed
            || error.is_some()
        {
            Phase::Failed
        } else if stopped {
            Phase::Stopped
        } else if finished.as_ref().is_none_or(|f| {
            (f.sink.is_none() && f.direct.is_none())
                || f.failure().is_some()
                || f.direct.as_ref().is_some_and(|d| !d.success())
        }) {
            Phase::Failed
        } else {
            Phase::Complete
        };
        let observation = Observation {
            phase,
            sink: finished.as_ref().and_then(|f| f.sink.clone()),
            node: Some(shutdown),
        };
        Completion {
            _node: node,
            job: finished,
            error,
            observation,
        }
    });
    Probe {
        driver: Some(driver),
        completion: None,
        control,
        handle_issued: false,
        panic: None,
    }
}
#[cfg(test)]
mod tests;
