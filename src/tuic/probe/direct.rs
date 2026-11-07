//! Direct listener/relay custody; shares the saved Probe/Node root.
//! Value classification is shared with the original seven characterizations.
//! No provider algorithm or DATA wrapper belongs here. Entry: prepare_direct;
//! lifecycle tests live in ../tests/direct.rs.
use crate::shared::TargetAddr;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct BridgeResult {
    pub(crate) accepted_connections: usize,
    pub(crate) completed_connections: usize,
    pub(crate) terminal_reset_connections: usize,
    pub(crate) failed_connections: usize,
    pub(crate) local_to_remote_bytes: u64,
    pub(crate) remote_to_local_bytes: u64,
    pub(crate) first_terminal_reset: Option<String>,
    pub(crate) first_failure: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BridgeFailureKind {
    TerminalReset,
    Unexpected,
}

#[derive(Debug)]
pub(crate) struct BridgeFailure {
    pub(crate) kind: BridgeFailureKind,
    pub(crate) message: String,
    _source: Option<ClientError>,
}

impl BridgeFailure {
    pub(crate) fn unexpected(message: String) -> Self {
        Self {
            kind: BridgeFailureKind::Unexpected,
            message,
            _source: None,
        }
    }

    pub(crate) fn relay_io(target: &TargetAddr, err: std::io::Error) -> Self {
        let kind = match err.kind() {
            std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe => {
                BridgeFailureKind::TerminalReset
            }
            _ => BridgeFailureKind::Unexpected,
        };
        Self {
            kind,
            message: format!("direct TUIC probe relay {target:?}: {err}"),
            _source: Some(ClientError::Io(err)),
        }
    }
}
use super::*;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Report {
    pub(crate) bridge: BridgeResult,
    pub(crate) child: child::Report,
}
pub(super) struct Spec {
    pub(super) target: TargetAddr,
    pub(super) duration_secs: u64,
    pub(super) max_connections: usize,
    pub(super) command: tokio::process::Command,
    pub(super) floor_mbps: Option<f64>,
}
struct FinishedRelay {
    open: Option<open::Open>,
    error: Option<ClientError>,
    result: Result<(u64, u64), BridgeFailure>,
    control: Arc<Control>,
    local: Option<TcpStream>,
}
impl FinishedRelay {
    fn settled(&self) -> bool {
        self.control.phase() != Phase::Quarantined
            && self.local.is_none()
            && self.open.as_ref().is_none_or(open::Open::settled)
    }
    fn failure(&self) -> Option<&ClientError> {
        self.error
            .as_ref()
            .or_else(|| self.open.as_ref().and_then(open::Open::failure))
            .or_else(|| {
                self.result
                    .as_ref()
                    .err()
                    .filter(|f| f.kind == BridgeFailureKind::Unexpected)
                    .and_then(|f| f._source.as_ref())
            })
    }
}
pub(super) struct Finished {
    pub(super) report: Report,
    relays: Vec<FinishedRelay>,
    child: Option<child::Finished>,
    error: Option<ClientError>,
    quarantined: bool,
    // Actual unknown task error, never recursively disposed.
    unknown: Vec<ManuallyDrop<tokio::task::JoinError>>,
}
impl Finished {
    pub(super) fn settled(&self) -> bool {
        !self.quarantined
            && self.unknown.is_empty()
            && self.relays.iter().all(FinishedRelay::settled)
            && self.child.as_ref().is_none_or(child::Finished::settled)
    }
    pub(super) fn failure(&self) -> Option<&ClientError> {
        self.error
            .as_ref()
            .or_else(|| self.relays.iter().find_map(FinishedRelay::failure))
            .or_else(|| self.child.as_ref().and_then(|c| c.error.as_ref()))
    }
    pub(super) fn success(&self) -> bool {
        self.report.bridge.failed_connections == 0
            && self.report.child.success
            && self.report.child.error.is_none()
    }
}
async fn relay(
    mut local: TcpStream,
    data: Arc<TuicUpstream>,
    target: TargetAddr,
    control: Arc<Control>,
) -> FinishedRelay {
    let mut open = match open::Open::new(data, target.clone(), control.clone()) {
        Ok(value) => value,
        Err(error) => {
            let result = Err(BridgeFailure::unexpected(format!(
                "direct TUIC probe open target {target:?}: {error:?}"
            )));
            if let Err(payload) = guard_drop(local) {
                control.publish(Phase::Quarantined);
                std::mem::forget(payload);
            }
            return FinishedRelay {
                open: None,
                error: Some(error),
                result,
                control,
                local: None,
            };
        }
    };
    let mut result = Err(BridgeFailure::unexpected(format!(
        "direct TUIC probe open target {target:?}"
    )));
    if open.acquire(&control).await {
        if let Some(remote) = open.stream() {
            let mut copy = Box::pin(tokio::io::copy_bidirectional(&mut local, remote));
            let observed = tokio::select! {biased;
                _=control.stop()=>None,
                value=guard_poll(copy.as_mut())=>Some(value),
            };
            match observed {
                Some(Err(payload)) => {
                    std::mem::forget(copy);
                    open.quarantine(payload);
                }
                other => {
                    control.begin_retirement();
                    if let Some(Ok(value)) = other {
                        result = value.map_err(|error| BridgeFailure::relay_io(&target, error));
                    }
                    if let Err(payload) = guard_drop(copy) {
                        open.quarantine(payload);
                    }
                }
            }
        }
    }
    if let Some(error) = open.failure() {
        result = Err(BridgeFailure::unexpected(format!(
            "direct TUIC probe open target {target:?}: {error:?}"
        )));
    }
    open.finish().await;
    let local = if open.settled() {
        if let Err(payload) = guard_drop(local) {
            open.quarantine(payload);
        }
        None
    } else {
        Some(local)
    };
    FinishedRelay {
        open: Some(open),
        error: None,
        result,
        control,
        local,
    }
}

pub(super) async fn run(mut spec: Spec, data: Arc<TuicUpstream>, outer: Arc<Control>) -> Finished {
    let mut finished = Finished {
        report: Report {
            bridge: BridgeResult::default(),
            child: child::Report::default(),
        },
        relays: Vec::with_capacity(32),
        child: None,
        error: None,
        quarantined: false,
        unknown: Vec::with_capacity(33),
    };
    let listener = match TcpListener::bind("127.0.0.1:0").await {
        Ok(value) => value,
        Err(error) => {
            finished.error = Some(ClientError::Io(error));
            return finished;
        }
    };
    let address = match listener.local_addr() {
        Ok(value) => value,
        Err(error) => {
            finished.error = Some(ClientError::Io(error));
            return finished;
        }
    };
    if let Some(floor) = spec.floor_mbps {
        println!(
            "direct_tuic_probe_start server={} target={:?} listen={} profile=cubic_safe1200 pool=1 relay=ordered_join duration_secs={} floor_mbps={:.3} max_connections={}",
            data.server, spec.target, address, spec.duration_secs, floor, spec.max_connections
        );
    }
    spec.command
        .arg("-c")
        .arg(address.ip().to_string())
        .arg("-p")
        .arg(address.port().to_string())
        .arg("-t")
        .arg(spec.duration_secs.to_string())
        .arg("-P")
        .arg("1")
        .arg("-R")
        .arg("-i")
        .arg("1")
        .arg("--json");
    let mut child = child::spawn(spec.command, spec.duration_secs, outer.clone());
    let mut relays: JoinSet<(usize, FinishedRelay)> = JoinSet::new();
    let mut slots: [Option<Arc<Control>>; 32] = std::array::from_fn(|_| None);
    let mut accepting = true;
    let mut bridge_deadline = None;
    let mut stopping = false;
    loop {
        if !accepting && relays.is_empty() {
            if finished.child.is_some() {
                break;
            }
            child.stop.request_stop();
        }
        #[cfg(test)]
        {
            *outer.facts.direct.bridge_deadline.lock().unwrap() = bridge_deadline;
        }
        tokio::select! {
            biased;
            _=outer.stop(),if !stopping=>{
                stopping=true;accepting=false;child.stop.request_stop();
                bridge_deadline.get_or_insert_with(||tokio::time::Instant::now()+Duration::from_secs(5));
                for local in slots.iter().flatten(){local.request_stop();}
            },
            _=async {if let Some(deadline)=bridge_deadline {tokio::time::sleep_until(deadline).await}},if bridge_deadline.is_some() && !stopping=>{
                #[cfg(test)] outer.facts.direct.bridge_expired.fetch_add(1,Ordering::SeqCst);
                stopping=true;accepting=false;child.stop.request_stop();
                finished.error.get_or_insert_with(||ClientError::InvalidTarget("direct TUIC probe bridge drain timed out after 5000ms".into()));
                for local in slots.iter().flatten(){local.request_stop();}
            },
            result=&mut child.join,if finished.child.is_none()=>{
                accepting=false;outer.begin_retirement();
                bridge_deadline.get_or_insert_with(||tokio::time::Instant::now()+Duration::from_secs(5));
                match result {
                    Ok(mut value)=>{
                        #[cfg(test)] outer.facts.direct.joins.fetch_add(1,Ordering::SeqCst);
                        value.report.joined=true;finished.report.child=value.report.clone();finished.child=Some(value);},
                    Err(error)=>{
                        finished.quarantined=true;finished.unknown.push(ManuallyDrop::new(error));
                        // JoinError consumed, but physical process/pipe outcome unknown.
                        for local in slots.iter().flatten(){local.request_stop();}
                        while let Some(result)=relays.join_next().await {
                            match result {Ok((_,value))=>finished.relays.push(value),Err(error)=>finished.unknown.push(ManuallyDrop::new(error))}
                        }
                        break;
                    }
                }
            },
            result=relays.join_next(),if !relays.is_empty()=>{
                match result {
                    Some(Ok((slot,value)))=>{
                        let provider_failure=value.error.is_some() || value.open.as_ref().and_then(open::Open::failure).is_some();
                        let unexpected=matches!(&value.result,Err(error) if error.kind==BridgeFailureKind::Unexpected);
                        let settled=value.settled();
                        let values=value.result.as_ref().ok().copied();
                        match &value.result {
                            Ok(_)=>{},
                            Err(error)=>match error.kind {
                                BridgeFailureKind::TerminalReset=>{
                                    finished.report.bridge.terminal_reset_connections=finished.report.bridge.terminal_reset_connections.saturating_add(1);
                                    finished.report.bridge.first_terminal_reset.get_or_insert_with(||error.message.clone());
                                },
                                BridgeFailureKind::Unexpected=>{
                                    finished.report.bridge.failed_connections=finished.report.bridge.failed_connections.saturating_add(1);
                                    finished.report.bridge.first_failure.get_or_insert_with(||error.message.clone());
                                    accepting=false;
                                }
                            }
                        }
                        if provider_failure || !settled {accepting=false;}
                        if !settled {finished.quarantined=true;}
                        if settled && !provider_failure && !unexpected {
                            let local=value.control.clone();
                            if let Err(payload)=guard_drop(value) {
                                local.publish(Phase::Quarantined);std::mem::forget(payload);
                            }
                            if local.phase()==Phase::Quarantined {finished.quarantined=true;accepting=false;}
                            else {
                                slots[slot]=None;
                                if let Some((up,down))=values {
                                    finished.report.bridge.completed_connections=finished.report.bridge.completed_connections.saturating_add(1);
                                    finished.report.bridge.local_to_remote_bytes=finished.report.bridge.local_to_remote_bytes.saturating_add(up);
                                    finished.report.bridge.remote_to_local_bytes=finished.report.bridge.remote_to_local_bytes.saturating_add(down);
                                }
                            }
                        } else {finished.relays.push(value);}
                        if !accepting {
                            #[cfg(test)] outer.facts.direct.failures.fetch_add(1,Ordering::SeqCst);
                            outer.begin_retirement();
                            bridge_deadline.get_or_insert_with(||tokio::time::Instant::now()+Duration::from_secs(5));
                        }
                    },
                    Some(Err(error))=>{
                        finished.quarantined=true;accepting=false;outer.begin_retirement();
                        finished.unknown.push(ManuallyDrop::new(error));
                        bridge_deadline.get_or_insert_with(||tokio::time::Instant::now()+Duration::from_secs(5));
                    },
                    None=>{accepting=false;finished.quarantined=true;}
                }
            },
            accepted=listener.accept(),if accepting && slots.iter().filter(|slot|slot.is_some()).count()<spec.max_connections=>{
                match accepted {
                    Ok((local,_))=>{
                        if let Some(slot)=slots.iter().position(Option::is_none) {
                            let control=Control::local();
                            #[cfg(test)] let control={
                                let mut control=control;
                                Arc::get_mut(&mut control).unwrap().facts=outer.facts.clone();
                                control
                            };
                            slots[slot]=Some(control.clone());
                            finished.report.bridge.accepted_connections=finished.report.bridge.accepted_connections.saturating_add(1);
                            let data=data.clone();let target=spec.target.clone();
                            relays.spawn(async move {(slot,relay(local,data,target,control).await)});
                        }
                    },
                    Err(error)=>{finished.error=Some(ClientError::Io(error));accepting=false;outer.begin_retirement();bridge_deadline.get_or_insert_with(||tokio::time::Instant::now()+Duration::from_secs(5));}
                }
            }
        }
    }
    #[cfg(test)]
    {
        *outer.facts.direct.report.lock().unwrap() = Some(finished.report.clone());
        outer
            .facts
            .direct
            .retained
            .store(finished.relays.len(), Ordering::Release);
    }
    finished
}
