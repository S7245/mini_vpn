//! Cold Endpoint construction owns receive/source preparation before driver publication.
//! It composes Proto registration and Quinn authority, not Connection or DATA processing.
//! Entry: Endpoint::new_with_receive_meter; tests: root quinn_metered_udp/construction.
use super::*;

impl Endpoint {
    pub(super) fn new_with_receive_meter(
        mut config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: Arc<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
        meter: Option<BackingMeter>,
    ) -> io::Result<Self> {
        match (config.receive_backing_meter_authority(), meter.as_ref()) {
            (Some(configured), Some(requested)) if !configured.same_authority(requested) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "endpoint receive authority conflicts with embedded configuration",
                ));
            }
            (Some(configured), None) if configured.budget_limit().is_some() => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "embedded hard receive authority requires the metered constructor",
                ));
            }
            _ => {}
        }
        let addr = socket.local_addr()?;
        let allow_mtud = !socket.may_fragment();
        let recv_buffer =
            RecvState::prepare_buffer(&config, &*socket, meter.as_ref(), server_config.is_none())?;
        if let Some(meter) = &meter {
            config.receive_backing_meter(meter.clone());
        }
        config
            .receive_datagram_limit(recv_buffer.buf.len() / BATCH_SIZE)
            .map_err(|_| io::Error::other("invalid actual receive datagram bound"))?;
        let one_datagram =
            BytesMut::metered_copy_layout_for_len(recv_buffer.buf.len() / BATCH_SIZE)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "receive copy layout"))?;
        let inner = proto::Endpoint::try_new(
            Arc::new(config),
            server_config.map(Arc::new),
            allow_mtud,
            None,
        )?;
        // All physical cold owners now exist. Prove that the same shared budget
        // can also admit one legal receive copy before publishing/spawning. This
        // canceled preparation is a capacity preflight, not a perpetual credit.
        let receive_headroom = meter
            .as_ref()
            .map(|meter| meter.reserve_cold_layout(one_datagram))
            .transpose()
            .map_err(|error| io::Error::other(format!("receive headroom admission {error:?}")))?;
        drop(receive_headroom);
        let rc = EndpointRef::new(
            socket,
            inner,
            addr.is_ipv6(),
            runtime.clone(),
            meter.clone(),
            recv_buffer,
        );
        let driver = EndpointDriver(rc.clone());
        let (driver_released_send, driver_released_recv) = oneshot::channel();
        runtime.spawn(Box::pin(
            async {
                // The await temporary, including its EndpointRef and socket
                // ownership, is dropped at this statement boundary. Only then
                // may normal completion mint the release acknowledgement.
                let result = driver.await;
                match result {
                    Ok(()) => {
                        let _ = driver_released_send.send(());
                    }
                    Err(e) => {
                        tracing::error!("I/O error: {}", e);
                    }
                }
            }
            .instrument(Span::current()),
        ));
        Ok(Self {
            inner: rc,
            receive_backing_meter: meter,
            default_client_config: None,
            runtime,
            driver_drain: Arc::new(Mutex::new(Some(driver_released_recv))),
        })
    }
}

#[cfg(test)]
mod tests;
