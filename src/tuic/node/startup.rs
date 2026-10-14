//! TUIC node startup and authentication implementation.
//! Existing configuration, handshake and AUTH behavior is preserved.
//! NodeHost custody is the next behavior step; current tracers live in tcp_open/tests.
use super::StartupResources;
use crate::tuic::*;

impl TuicUpstream {
    /// 建连 + 发 Authenticate（token 经 keying-material 导出，字节级对齐 sing-box）。
    pub async fn connect(
        cfg: &TuicClientConfig,
        metrics: Arc<Metrics>,
    ) -> Result<Self, ClientError> {
        Self::connect_in(cfg, metrics, None).await
    }

    pub(super) async fn connect_in(
        cfg: &TuicClientConfig,
        metrics: Arc<Metrics>,
        owner: Option<Arc<StartupResources>>,
    ) -> Result<Self, ClientError> {
        // 刀3.5：从 config 解析 CC（未知值回落 Cubic + 告警，失败自愈不致命）。
        let (cc, fell_back) = quic::parse_cc(&cfg.congestion_control);
        if fell_back {
            println!(
                "⚠️ TUIC 未知 congestion_control={:?}，回落 Cubic（quinn 默认）",
                cfg.congestion_control
            );
        }
        // 刀3.5：解析 UDP relay mode（未知值回落 Native + 告警，与 CC 对称）。
        let udp_relay_mode = UdpRelayMode::parse(&cfg.udp_relay_mode).unwrap_or_else(|| {
            println!(
                "⚠️ TUIC 未知 udp_relay_mode={:?}，回落 native（datagram 主 + 超限 stream 兜底）",
                cfg.udp_relay_mode
            );
            UdpRelayMode::Native
        });
        let (mtu_policy, mtu_fell_back) = quic::parse_mtu_policy(Some(&cfg.mtu_policy));
        if mtu_fell_back {
            println!(
                "⚠️ TUIC 未知 mtu_policy={:?}，回落 default（1280 + PLPMTUD）",
                cfg.mtu_policy
            );
        }
        let (gso_policy, gso_fell_back) = quic::parse_gso_policy(Some(&cfg.gso_policy));
        if gso_fell_back {
            println!(
                "⚠️ TUIC 未知 gso_policy={:?}，回落 enabled（Quinn 生产默认）",
                cfg.gso_policy
            );
        }
        let (udp_send_service, udp_send_service_fell_back) =
            quic::parse_udp_send_service_policy(Some(&cfg.udp_send_service));
        if udp_send_service_fell_back {
            println!(
                "⚠️ TUIC 未知 udp_send_service={:?}，回落 quinn（生产默认）",
                cfg.udp_send_service
            );
        }
        let (pacing_policy, pacing_policy_fell_back) =
            quic::parse_pacing_policy(Some(&cfg.pacing_policy));
        if pacing_policy_fell_back {
            println!(
                "⚠️ TUIC 未知 pacing_policy={:?}，回落 quinn（生产默认）",
                cfg.pacing_policy
            );
        }
        quic::validate_quic_send_policies(pacing_policy, udp_send_service)
            .map_err(ClientError::InvalidTarget)?;
        let qcfg = quic::client_quic_config_alpn(
            &cfg.ca_path,
            vec![cfg.alpn.as_bytes().to_vec()],
            cc,
            mtu_policy,
            gso_policy,
            pacing_policy,
        )
        .map_err(ClientError::InvalidTarget)?;
        let (mut endpoint, udp_send_service_stats) =
            quic::client_endpoint_with_udp_send_service(qcfg, udp_send_service, pacing_policy)
                .map_err(ClientError::InvalidTarget)?;
        if let Some(owner) = &owner {
            owner.register_endpoint(&mut endpoint)?;
        }
        let conn = if let Some(owner) = &owner {
            owner.handshake(&endpoint, cfg).await?
        } else {
            Self::handshake(
                &endpoint,
                cfg.server,
                &cfg.sni,
                &cfg.uuid,
                &cfg.password,
                cfg.zero_rtt,
            )
            .await?
        };
        // 刀3：记录初始 datagram 发送上限（acceptance 据此读真实 datagram 天花板；PLPMTUD 探完会更大）。
        // 超此上限的上行包走 uni-stream 兜底；下行大包由 server 分片、本端重组。
        println!(
            "📏 TUIC datagram 初始上限 = {:?} 字节（超此走 uni-stream 兜底；PLPMTUD 将继续上探）",
            conn.max_datagram_size()
        );
        // 刀3.5：打实际生效的 CC + relay mode，供 acceptance 确认 BBR/quic 真装上（A/B 归因）。
        println!(
            "🧭 TUIC 拥塞控制器={cc:?} | UDP relay mode={udp_relay_mode:?} | QUIC MTU policy={} | QUIC GSO policy={} | QUIC UDP send service={} | QUIC pacing policy={}",
            mtu_policy.label(),
            gso_policy.label(),
            udp_send_service.label(),
            pacing_policy.label(),
        );
        println!(
            "🪟 QUIC flow windows: bidi={} uni={} stream_rx={}B conn_rx={}B send={}B",
            quic::QUIC_MAX_CONCURRENT_BIDI_STREAMS,
            quic::QUIC_MAX_CONCURRENT_UNI_STREAMS,
            quic::quic_stream_receive_window_bytes(mtu_policy),
            quic::quic_receive_window_bytes(mtu_policy),
            quic::QUIC_SEND_WINDOW_BYTES
        );
        if let Some(stats) = endpoint.endpoint_pacing_snapshot() {
            println!("{}", format_endpoint_pacing_stats_line(stats));
        }
        let tcp_pool = cfg.tcp_pool.clamp(MIN_TUIC_TCP_POOL, MAX_TUIC_TCP_POOL);
        let mut conns = Vec::with_capacity(tcp_pool);
        let quic_stats_stop = cfg.quic_stats_secs.map(|_| watch::channel(false).0);
        if let Some(owner) = &owner {
            owner.state().stats_stop = quic_stats_stop.clone();
        }
        if let Some(secs) = cfg.quic_stats_secs {
            println!("🔬 TUIC QUIC stats 已启用：每 {secs}s 打印连接级 flow/congestion 指标");
            if let Some(stop) = &quic_stats_stop {
                track_stats(&owner, Some(conn.stable_id()), || {
                    spawn_quic_stats_logger(conn.clone(), 0, secs, stop.subscribe())
                })?;
                if endpoint.endpoint_pacing_snapshot().is_some() {
                    track_stats(&owner, None, || {
                        spawn_endpoint_pacing_stats_logger(endpoint.clone(), secs, stop.subscribe())
                    })?;
                }
                if let Some(stats) = udp_send_service_stats.clone() {
                    track_stats(&owner, None, || {
                        spawn_udp_send_service_stats_logger(stats, secs, stop.subscribe())
                    })?;
                }
            }
        }
        conns.push(conn);
        let mut tcp_startup_auth_attempts = Vec::with_capacity(tcp_pool);
        tcp_startup_auth_attempts.push(1_u64);
        for index in 1..tcp_pool {
            let result = if let Some(owner) = &owner {
                owned_aux(owner, &endpoint, cfg).await
            } else {
                Self::handshake_aux_with_retries(
                    &endpoint,
                    cfg.server,
                    &cfg.sni,
                    &cfg.uuid,
                    &cfg.password,
                    cfg.zero_rtt,
                    index,
                )
                .await
            };
            let (extra, auth_attempts) = match result {
                Ok(conn) => conn,
                Err(e) if owner.is_some() => return Err(e),
                Err(e) => match tcp_pool_aux_failure_action(conns.len()) {
                    TcpPoolAuxFailureAction::FailStartup => return Err(e),
                    TcpPoolAuxFailureAction::ContinueWithEstablished => {
                        println!(
                            "⚠️ TUIC TCP connection pool auxiliary slot {index} failed to authenticate/connect; continuing with {} established connection(s): {e:?}",
                            conns.len()
                        );
                        break;
                    }
                },
            };
            tcp_startup_auth_attempts.push(auth_attempts as u64);
            if let Some(secs) = cfg.quic_stats_secs
                && let Some(stop) = &quic_stats_stop
            {
                track_stats(&owner, Some(extra.stable_id()), || {
                    spawn_quic_stats_logger(extra.clone(), index, secs, stop.subscribe())
                })?;
            }
            conns.push(extra);
        }
        let tcp_pool = conns.len();
        debug_assert_eq!(tcp_startup_auth_attempts.len(), tcp_pool);
        if tcp_pool > 1 {
            println!(
                "🧵 TUIC TCP connection pool={tcp_pool}（UDP/health 仍走 primary connection）"
            );
        }
        let clock = std::time::Instant::now();
        let tcp_pool_slots = conns
            .into_iter()
            .zip(tcp_startup_auth_attempts)
            .enumerate()
            .map(|(index, (conn, auth_attempts))| {
                Arc::new(TcpPoolGenerationSlot::new(
                    index,
                    conn,
                    1,
                    auth_attempts,
                    clock,
                ))
            })
            .collect::<Vec<_>>();
        let mut current_generation_activity = Vec::with_capacity(tcp_pool_slots.len());
        for slot in &tcp_pool_slots {
            current_generation_activity.push(slot.current_activity().await);
        }
        let tcp_pool_admission =
            TcpPoolAdmission::with_current_generation_activity(current_generation_activity);
        Ok(Self {
            relay_policy: Arc::clone(&cfg.relay_policy),
            endpoint,
            udp_send_service_policy: udp_send_service,
            udp_send_service_stats,
            udp_runtime: Arc::new(node_runtime::UdpRuntime::default()),
            server: cfg.server,
            sni: cfg.sni.clone(),
            uuid: cfg.uuid,
            password: cfg.password.clone(),
            tcp_pool_slots,
            tcp_pool_draining_predecessor: Arc::new(AtomicBool::new(false)),
            tcp_pool_admission,
            udp_drops: AtomicU64::new(0),
            udp_stream_sender: Default::default(),
            udp_stream_fallbacks: AtomicU64::new(0),
            last_udp_activity: AtomicU64::new(0),
            clock,
            zero_rtt: cfg.zero_rtt,
            quic_stats_secs: cfg.quic_stats_secs,
            quic_stats_stop,
            udp_relay_mode,
            metrics,
        })
    }

    /// 当前 UDP relay mode（可观测/测试）。
    pub fn udp_relay_mode(&self) -> UdpRelayMode {
        self.udp_relay_mode
    }

    /// 建连 + 认证。`zero_rtt` 时先试 0-RTT（early data）；任何原因失败（无 ticket / 服务端不支持 /
    /// early-exporter 认证不齐）都**自愈回落 1-RTT**，绝不卡死重连循环。
    /// 中文要点：首连必无 ticket → `into_0rtt` 必失败 → 走 1-RTT（与 13a 行为一致，零回归）。
    pub(super) async fn handshake(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
        zero_rtt: bool,
    ) -> Result<Connection, ClientError> {
        if zero_rtt && let Some(conn) = Self::try_0rtt(endpoint, server, sni, uuid, password).await
        {
            println!("⚡ TUIC 0-RTT 重连成功（early data）");
            return Ok(conn);
        }
        // 关了 0-RTT，或 0-RTT 不可用（无 ticket/不支持/认证不齐）→ 回落 1-RTT。
        Self::handshake_1rtt(endpoint, server, sni, uuid, password).await
    }

    /// 尝试 0-RTT 握手 + early-data 认证；不可用返回 None（交由调用方回落 1-RTT）。
    async fn try_0rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Option<Connection> {
        let connecting = endpoint.connect(server, sni).ok()?;
        // into_0rtt 的 Err 返回原 Connecting（非 Error 类型），无 ticket/不支持时走这里。
        let (conn, _accepted) = match connecting.into_0rtt() {
            Ok(pair) => pair,
            Err(_connecting) => return None,
        };
        // early-exporter 认证：若与 sing-box 不齐则失败 → 记一行(便于 e2e 诊断)并 None 回落（不卡死）。
        if let Err(e) = Self::authenticate(&conn, uuid, password).await {
            println!(
                "⚠️ TUIC 0-RTT 认证失败(可能 early-exporter 与 sing-box 不齐)，回落 1-RTT: {e:?}"
            );
            return None;
        }
        Some(conn)
    }

    /// 普通 1-RTT 握手 + 认证（13a 行为）。
    async fn handshake_1rtt(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<Connection, ClientError> {
        let conn = endpoint
            .connect(server, sni)
            .map_err(|e| io_err("tuic connect", e))?
            .await
            .map_err(|e| io_err("tuic handshake", e))?;
        Self::authenticate(&conn, uuid, password).await?;
        Ok(conn)
    }

    pub(super) async fn handshake_aux_with_retries(
        endpoint: &Endpoint,
        server: SocketAddr,
        sni: &str,
        uuid: &[u8; 16],
        password: &str,
        zero_rtt: bool,
        index: usize,
    ) -> Result<(Connection, usize), ClientError> {
        for attempt in 0..TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS {
            match Self::handshake(endpoint, server, sni, uuid, password, zero_rtt).await {
                Ok(conn) => {
                    if attempt > 0 {
                        println!(
                            "✅ TUIC TCP connection pool auxiliary slot {index} recovered after {} attempt(s)",
                            attempt + 1
                        );
                    }
                    return Ok((conn, attempt + 1));
                }
                Err(e) => {
                    let Some(delay) = tcp_pool_aux_retry_delay(attempt) else {
                        return Err(e);
                    };
                    println!(
                        "⚠️ TUIC TCP connection pool auxiliary slot {index} failed to authenticate/connect on attempt {}/{}; retrying in {}ms: {e:?}",
                        attempt + 1,
                        TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS,
                        delay.as_millis()
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
        unreachable!("auxiliary TCP pool retry loop must return on success or final error")
    }

    /// 在已建立(0-RTT 或 1-RTT)的连接上发 TUIC Authenticate（单向流）。
    /// token = export_keying_material(out=32, label=UUID(16), context=password) —— 字节级对齐 sing-box。
    pub(super) async fn authenticate(
        conn: &Connection,
        uuid: &[u8; 16],
        password: &str,
    ) -> Result<(), ClientError> {
        let mut token = [0u8; 32];
        conn.export_keying_material(&mut token, uuid, password.as_bytes())
            .map_err(|_| ClientError::InvalidTarget("tuic keying-material export failed".into()))?;
        let mut uni = conn
            .open_uni()
            .await
            .map_err(|e| io_err("tuic open_uni", e))?;
        uni.write_all(&encode_authenticate(uuid, &token))
            .await
            .map_err(|e| io_err("tuic auth write", e))?;
        uni.finish().map_err(|e| io_err("tuic auth finish", e))?;
        Ok(())
    }
}

// Temporary branch-by-abstraction adapter; the running-node migration removes None.
fn track_stats(
    owner: &Option<Arc<StartupResources>>,
    stable_id: Option<usize>,
    spawn: impl FnOnce() -> tokio::task::JoinHandle<()>,
) -> Result<(), ClientError> {
    if let Some(owner) = owner {
        owner.spawn_stats_identified(stable_id, spawn)
    } else {
        drop(spawn());
        Ok(())
    }
}

async fn owned_aux(
    owner: &StartupResources,
    endpoint: &Endpoint,
    cfg: &TuicClientConfig,
) -> Result<(Connection, usize), ClientError> {
    for attempt in 0..TUIC_TCP_POOL_AUX_CONNECT_ATTEMPTS {
        match owner.handshake(endpoint, cfg).await {
            Ok(conn) => return Ok((conn, attempt + 1)),
            Err(error) => {
                owner.record_startup_result(error);
                let Some(delay) = tcp_pool_aux_retry_delay(attempt).filter(|_| {
                    let state = owner.state();
                    !state.quarantined && !state.config_rejected
                }) else {
                    return Err(super::recorded_failure());
                };
                tokio::time::sleep(delay).await;
            }
        }
    }
    Err(io_err("tuic auxiliary startup", "attempt budget exhausted"))
}
