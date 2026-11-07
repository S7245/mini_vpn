//! Sole process actor: owns Child, both pipes and wait; no DATA or reader tasks.
//! Outer stop survives a forgotten Probe driver. Overflow kills then boundedly
//! discard-drains; wait alone never proves pipe EOF. Entry spawn; tests in
//! ../tests/direct*.rs. Unconsumed actor join remains charged at the saved root.
use super::*;
use std::process::{ExitStatus, Stdio};
use tokio::io::AsyncReadExt;
use tokio::process::Command;
use tokio::task::JoinHandle;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Report {
    pub(crate) stdout: Arc<Vec<u8>>,
    pub(crate) stderr: Arc<Vec<u8>>,
    pub(crate) waited: bool,
    pub(crate) stdout_eof: bool,
    pub(crate) stderr_eof: bool,
    pub(crate) joined: bool,
    pub(crate) success: bool,
    pub(crate) status: Option<String>,
    pub(crate) killed: bool,
    pub(crate) error: Option<String>,
}
pub(super) struct Finished {
    pub(super) report: Report,
    // Original failures/status, never reconstructed from report strings.
    pub(super) error: Option<ClientError>,
    _status: Option<ExitStatus>,
    spawned: bool,
    _unsettled: Option<(
        tokio::process::Child,
        Option<tokio::process::ChildStdout>,
        Option<tokio::process::ChildStderr>,
    )>,
}
impl Finished {
    pub(super) fn settled(&self) -> bool {
        if !self.report.joined || self._unsettled.is_some() {
            return false;
        }
        match (self.spawned, self._status.as_ref()) {
            (true, Some(status)) => {
                self.report.waited
                    && self.report.stdout_eof
                    && self.report.stderr_eof
                    && self.report.success == status.success()
                    && self.report.status.as_deref() == Some(status.to_string().as_str())
            }
            (false, None) => {
                self.error.is_some()
                    && !self.report.waited
                    && !self.report.stdout_eof
                    && !self.report.stderr_eof
                    && !self.report.success
                    && self.report.status.is_none()
            }
            _ => false,
        }
    }
}
pub(super) struct Actor {
    pub(super) join: JoinHandle<Finished>,
    pub(super) stop: Arc<Control>,
}
pub(super) fn spawn(mut command: Command, duration: u64, outer: Arc<Control>) -> Actor {
    let stop = Control::local();
    let saved = stop.clone();
    command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(test)]
    let observer = outer.clone();
    let join = tokio::spawn(async move {
        let mut report = Report::default();
        let mut stdout_bytes = Vec::with_capacity(1048576);
        let mut stderr_bytes = Vec::with_capacity(65536);
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                outer.begin_retirement();
                report.error = Some(format!("direct TUIC probe run iperf3: {error}"));
                return Finished {
                    report,
                    error: Some(ClientError::Io(error)),
                    _status: None,
                    spawned: false,
                    _unsettled: None,
                };
            }
        };
        #[cfg(test)]
        outer
            .facts
            .direct
            .pid
            .store(child.id().unwrap_or(0), Ordering::Release);
        let mut stdout = child.stdout.take();
        let mut stderr = child.stderr.take();
        let mut out = [0u8; 8192];
        let mut err = [0u8; 8192];
        let deadline = tokio::time::Instant::now() + Duration::from_secs(duration + 30);
        let mut status = None;
        let mut wait_failed = false;
        let mut first_error = None;
        let mut out_failed = stdout.is_none();
        let mut err_failed = stderr.is_none();
        if out_failed || err_failed {
            report.error = Some("direct child missing piped output".into());
        }
        loop {
            if (report.waited || wait_failed)
                && (report.stdout_eof || out_failed)
                && (report.stderr_eof || err_failed)
            {
                break;
            }
            let mut kill = false;
            tokio::select! {
                _=outer.stop(),if !report.killed=>{kill=true;},
                _=saved.stop(),if !report.killed=>{kill=true;},
                _=tokio::time::sleep_until(deadline),if !report.killed=>{
                    report.error.get_or_insert_with(||"direct TUIC probe iperf3 timed out".into());kill=true;
                },
                result=child.wait(),if !report.waited && !wait_failed=>{
                    outer.begin_retirement();
                    match result {
                        Ok(value)=>{#[cfg(test)] outer.facts.direct.child_waited.store(true,Ordering::Release);
                            report.waited=true;report.success=value.success();report.status=Some(value.to_string());status=Some(value);},
                        Err(error)=>{
                            report.error.get_or_insert_with(||format!("direct child wait: {error}"));
                            first_error.get_or_insert(ClientError::Io(error));
                            // Retain the original Child as unknown; never repoll a
                            // failed wait as a new proof. Still kill and drain both pipes.
                            wait_failed=true;kill=true;
                        }
                    }
                },
                result=async {read_pipe(&mut stdout,&mut out).await},if !report.stdout_eof && !out_failed=>{
                    match result {
                        Ok(0)=>report.stdout_eof=true,
                        Ok(n)=>{if capture(&mut stdout_bytes,&out[..n],1048576){report.error.get_or_insert_with(||"direct child stdout overflow".into());kill=true;}},
                        Err(error)=>{out_failed=true;report.error.get_or_insert_with(||format!("direct child stdout: {error}"));first_error.get_or_insert(ClientError::Io(error));kill=true;}
                    }
                },
                result=async {read_pipe(&mut stderr,&mut err).await},if !report.stderr_eof && !err_failed=>{
                    match result {
                        Ok(0)=>report.stderr_eof=true,
                        Ok(n)=>{
                            #[cfg(test)] outer.facts.direct.stderr_bytes.fetch_add(n,Ordering::SeqCst);
                            if capture(&mut stderr_bytes,&err[..n],65536){report.error.get_or_insert_with(||"direct child stderr overflow".into());kill=true;}},
                        Err(error)=>{err_failed=true;report.error.get_or_insert_with(||format!("direct child stderr: {error}"));first_error.get_or_insert(ClientError::Io(error));kill=true;}
                    }
                }
            }
            if kill && !report.killed {
                outer.begin_retirement();
                report.killed = true;
                if let Err(error) = child.start_kill() {
                    report
                        .error
                        .get_or_insert_with(|| format!("direct child kill: {error}"));
                    first_error.get_or_insert(ClientError::Io(error));
                }
            }
        }
        report.stdout = Arc::new(stdout_bytes);
        report.stderr = Arc::new(stderr_bytes);
        let unsettled = if report.waited && report.stdout_eof && report.stderr_eof {
            None
        } else {
            Some((child, stdout, stderr))
        };
        #[cfg(test)]
        {
            *outer.facts.direct.child.lock().unwrap() = Some(report.clone());
        }
        Finished {
            report,
            error: first_error,
            _status: status,
            spawned: true,
            _unsettled: unsettled,
        }
    });
    #[cfg(test)]
    {
        *observer.facts.direct.actor.lock().unwrap() = Some(join.abort_handle());
    }
    Actor { join, stop }
}
fn capture(saved: &mut Vec<u8>, bytes: &[u8], limit: usize) -> bool {
    let available = limit.saturating_sub(saved.len());
    saved.extend_from_slice(&bytes[..bytes.len().min(available)]);
    bytes.len() > available
}

async fn read_pipe<T: tokio::io::AsyncRead + Unpin>(
    pipe: &mut Option<T>,
    scratch: &mut [u8],
) -> std::io::Result<usize> {
    match pipe.as_mut() {
        Some(pipe) => pipe.read(scratch).await,
        None => Err(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "missing child pipe",
        )),
    }
}
