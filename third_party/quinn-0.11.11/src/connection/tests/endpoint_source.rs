//! Probe the real published Connection's original reverse sender, never a fake port.
use super::super::*;
impl Connecting {
    pub(crate) fn send_endpoint_event_for_probe(
        &self,
        handle: ConnectionHandle,
        event: crate::ConnectionToEndpointEvent,
    ) -> Result<Option<proto::ConnectionEvent>, ()> {
        let state = self
            .conn
            .as_ref()
            .unwrap()
            .state
            .lock("original endpoint sender probe");
        state.endpoint_events.send((handle, event))
    }
}

impl Connecting {
    pub(crate) fn register_forward_waker_for_probe(&self, waker: &std::task::Waker) -> bool {
        let mut state = self
            .conn
            .as_ref()
            .unwrap()
            .state
            .lock("actual forward receiver probe");
        state
            .conn_events
            .poll_recv(&mut Context::from_waker(waker))
            .is_pending()
    }
}

impl Connecting {
    pub(crate) fn forward_queue_empty_for_probe(&self) -> bool {
        let state = self
            .conn
            .as_ref()
            .unwrap()
            .state
            .lock("physical forward inventory probe");
        match &state.conn_events {
            crate::endpoint::budgeted_events::ForwardReceiver::Legacy(rx) => rx.is_empty(),
            crate::endpoint::budgeted_events::ForwardReceiver::Budgeted(rx) => rx.is_empty(),
        }
    }
}
