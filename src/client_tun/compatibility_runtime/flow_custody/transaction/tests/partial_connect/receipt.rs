//! Actual physical commit followed by deferred, missing or mismatched root ACK.
//! Reuses the real partial CONNECT fixture; no receipt, provider or join is minted
//! here. The optional pool mapping tests release custody, not DNS resolution.
use super::super::super::super::rearm::physical_flow::FlowPhysicalReceipt;
use super::super::super::super::rearm::physical_flow::tests::ReceiptFault;
use super::super::super::super::rearm::LocalReset;
use super::*;

#[derive(Clone, Copy, Debug)]
enum Case {
    Deferred,
    PriorFlowQ,
    TurnFlowQ,
    WrongBytes,
    WrongIdentity,
    Missing,
    Physical(PhysicalFault),
    Field(ReceiptFault),
}

#[derive(Clone, Copy, Debug)]
enum PhysicalFault {
    MissingRoot,
    CensusRoot,
    CensusSocket,
    ScanRoot,
    SlotEpoch,
    SlotHandle,
    MissingContext,
    MissingMirror,
    MirrorIdentity,
    ContextEpoch,
    PhysicalFlag,
    RootAudit,
    EgressHeld,
    LocalReset,
}

fn reject_physical_fault(physical: &mut Physical, id: FlowIdentity, fault: PhysicalFault) {
    match fault {
        PhysicalFault::CensusSocket => {
            let original = physical.server.sockets.remove(id.handle);
            assert!(!physical.dispatch.flow_shutdown_complete(
                &physical.server.sockets,
                &mut physical.contexts,
                &physical.pool,
            ));
            let smoltcp::socket::Socket::Tcp(original) = original else {
                panic!("original TCP socket")
            };
            assert_eq!(physical.server.sockets.add(original), id.handle);
        }
        PhysicalFault::MissingRoot | PhysicalFault::CensusRoot | PhysicalFault::ScanRoot => {
            let original = physical.owner.opens().slots.take(id.index).unwrap();
            match fault {
                PhysicalFault::CensusRoot => assert!(!physical.dispatch.flow_shutdown_complete(
                    &physical.server.sockets,
                    &mut physical.contexts,
                    &physical.pool
                )),
                PhysicalFault::ScanRoot => physical.dispatch.drain_shutdown_physical(
                    &mut physical.server.sockets,
                    &mut physical.contexts,
                    &mut physical.pool,
                    &mut 0,
                    0,
                ),
                _ => assert!(!service(physical, id)),
            }
            assert!(matches!(physical.owner.opens().slots.put(id.index, original), Ok(None)));
        }
        PhysicalFault::SlotEpoch | PhysicalFault::RootAudit => {
            physical.owner.opens().slots.get_mut(id.index)
                .unwrap()
                .epoch += 1;
            let accepted = if matches!(fault, PhysicalFault::RootAudit) {
                physical.dispatch.release_flow(original(physical).unwrap())
            } else {
                service(physical, id)
            };
            physical.owner.opens().slots.get_mut(id.index)
                .unwrap()
                .epoch = id.epoch;
            assert!(!accepted);
        }
        PhysicalFault::SlotHandle | PhysicalFault::MirrorIdentity => {
            // A distinct actual socket is never an index obtained from bad receipt data.
            let other = physical
                .server
                .sockets
                .add(crate::client_tun::build_listener_socket(
                    &crate::client_tun::ListenerSpec { local_port: 10001 },
                ));
            if matches!(fault, PhysicalFault::SlotHandle) {
                physical.owner.opens().slots.get_mut(id.index)
                    .unwrap()
                    .handle = other;
            } else {
                physical
                    .contexts
                    .get_mut(&id.handle)
                    .unwrap()
                    .flow_mirror
                    .as_mut()
                    .unwrap()
                    .replace_identity(FlowIdentity {
                        handle: other,
                        ..id
                    });
            }
            let accepted = service(physical, id);
            physical.owner.opens().slots.get_mut(id.index)
                .unwrap()
                .handle = id.handle;
            physical
                .contexts
                .get_mut(&id.handle)
                .unwrap()
                .flow_mirror
                .as_mut()
                .unwrap()
                .replace_identity(id);
            assert_eq!(
                physical.server.sockets.get::<TcpSocket>(other).state(),
                smoltcp::socket::tcp::State::Listen
            );
            physical.server.sockets.remove(other);
            assert!(!accepted);
        }
        PhysicalFault::EgressHeld => {
            // D216 (F2): a faulted ctx with an egress-held permit marker is
            // shared egress custody ⇒ Session, never a row-contained Flow Q.
            physical.contexts.get_mut(&id.handle).unwrap().downlink_delay_permit_release_until_egress = true;
            let accepted = service(physical, id);
            physical.contexts.get_mut(&id.handle).unwrap().downlink_delay_permit_release_until_egress = false;
            assert!(!accepted);
            assert!(physical.owner.ledger().session_quarantined()
                && physical.owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire),
                "egress-held permit ⇒ Session scope");
        }
        PhysicalFault::LocalReset => {
            // D216: a local_reset marker is a second physical owner on this
            // socket ⇒ Session. The marker comes from the original private
            // prepare on a scratch ctx (epoch exhausted ⇒ no abort), never a
            // hand-built value.
            let port = physical.contexts[&id.handle].local_port;
            let mut scratch = crate::client_tun::SocketCtx::new(port);
            scratch.conn_epoch = u64::MAX;
            let socket = physical.server.sockets.get_mut::<TcpSocket>(id.handle);
            assert!(!LocalReset::prepare_wire_test(&mut scratch, socket));
            let marker = scratch.local_reset.take();
            assert!(marker.is_some(), "original prepare retained an invalid marker");
            physical.contexts.get_mut(&id.handle).unwrap().local_reset = marker;
            let accepted = service(physical, id);
            physical.contexts.get_mut(&id.handle).unwrap().local_reset = None;
            assert!(!accepted);
        }
        PhysicalFault::MissingContext => {
            let ctx = physical.contexts.remove(&id.handle).unwrap();
            assert!(!service(physical, id));
            physical.contexts.insert(id.handle, ctx);
        }
        PhysicalFault::MissingMirror => {
            let mirror = physical
                .contexts
                .get_mut(&id.handle)
                .unwrap()
                .flow_mirror
                .take();
            assert!(!service(physical, id));
            physical.contexts.get_mut(&id.handle).unwrap().flow_mirror = mirror;
        }
        PhysicalFault::ContextEpoch => {
            physical.contexts.get_mut(&id.handle).unwrap().conn_epoch += 1;
            let accepted = service(physical, id);
            physical.contexts.get_mut(&id.handle).unwrap().conn_epoch = id.epoch + 1;
            assert!(!accepted);
        }
        PhysicalFault::PhysicalFlag => {
            physical.owner.opens().flow_mut(id).unwrap().physical = false;
            let accepted = service(physical, id);
            physical.owner.opens().flow_mut(id).unwrap().physical = true;
            assert!(!accepted);
        }
    }
}

fn original(physical: &Physical) -> Option<FlowPhysicalReceipt> {
    physical.contexts[&physical.server.handle]
        .flow_mirror
        .as_ref()
        .and_then(|mirror| mirror.retained_receipt())
}

pub(super) fn service(physical: &mut Physical, id: FlowIdentity) -> bool {
    let (global_tx, _global_rx) = crate::client_tun::relay_channel::channel(4);
    let (owned_action_tx, _owned_rx) = mpsc::channel(4);
    physical.dispatch.service_flow(
        id,
        &mut HandshakeCompletion {
            sockets: &mut physical.server.sockets,
            socket_ctxs: &mut physical.contexts,
            global_tx: &global_tx,
            owned_action_tx: &owned_action_tx,
            fake_pool: &mut physical.pool,
            now_secs: 0,
            metrics_handle: &Metrics::default(),
            downlink_backpressure: DownlinkBackpressureConfig::default(),
            relay_read_hard_pause: false,
        },
    )
}

fn retained(physical: &Physical, id: FlowIdentity, source: &(usize, String), retry: usize) {
    let seen = physical.owner.flow_observation().unwrap();
    assert_eq!((seen.live, seen.w, seen.f), (1, 1, 1));
    assert_eq!(
        (seen.reserved, seen.escrow_bytes),
        (MAX_UPLINK_BUFFER, PAYLOAD.len())
    );
    assert_eq!(
        (
            seen.factory_started,
            seen.published,
            seen.released,
            seen.joined
        ),
        (1, 0, 0, 0)
    );
    assert_eq!(
        (
            seen.running,
            seen.retry,
            seen.stopped_producers,
            seen.settled_providers
        ),
        (0, retry, 1, 1)
    );
    assert!(seen.audit);
    assert_eq!(&active_error(&physical.owner), source);
    let opens = physical.owner.opens();
    let flow = opens.slots[id.index].as_ref().unwrap();
    assert!(matches!(&flow.phase, OpenPhase::Flow(flow)
        if flow.id == id && flow.escrow.as_slice() == PAYLOAD && flow.physical));
    assert_eq!(physical.contexts[&id.handle].conn_epoch, id.epoch + 1);
    assert!(physical.contexts[&id.handle].flow_mirror.is_some());
    assert_eq!(physical.pool.usage(), (1, 0));
    assert_eq!(
        physical.server.sockets.get::<TcpSocket>(id.handle).state(),
        smoltcp::socket::tcp::State::Listen
    );
    assert_eq!(
        physical
            .client
            .sockets
            .get::<TcpSocket>(physical.client.handle)
            .state(),
        smoltcp::socket::tcp::State::Closed
    );
}

async fn actual_receipt(case: Case) {
    let ready = ReadyPeer::new().await;
    let setup = capture(Physical::new_with_fake_ip(ready.upstream.clone(), true)).await;
    let mut physical = match setup {
        Ok(physical) => physical,
        Err(panic) => {
            ready.close().await;
            std::panic::resume_unwind(panic)
        }
    };
    let mut halves = None;
    let scenario = capture(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if view(&physical.owner)
                    .is_some_and(|v| v.phase == TcpOpenPhase::Connecting && v.connect_written == 4)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let id = held(&physical.owner);
        let acquired = {
            let mut opens = physical.owner.opens();
            opens.flow.as_mut().unwrap().defer_receipt = Some(id);
            let OpenPhase::Flow(flow) = &opens.slots[id.index].as_ref().unwrap().phase else {
                panic!("actual admitted flow")
            };
            flow.fake_ip.unwrap()
        };
        assert_eq!((acquired.before, acquired.after), (0, 1));
        assert_eq!(physical.pool.usage(), (1, 1));
        halves = Some(ready.prefix().await);
        physical.owner.request_stop();
        tokio::time::timeout(Duration::from_secs(2), async {
            while view(&physical.owner).unwrap().phase != TcpOpenPhase::Retiring {
                physical.control().await;
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let source = active_error(&physical.owner);
        tokio::time::timeout(Duration::from_secs(2), peer_retirement(halves.take().unwrap()))
            .await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                physical.control().await;
                if original(&physical).is_some() {
                    break;
                }
                physical.flush().await;
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        let receipt = original(&physical).unwrap();
        assert_eq!(receipt.identity(), id);
        let released = receipt.actual_fake_release().unwrap();
        assert_eq!((released.ip, released.incarnation, released.acquisition),
            (acquired.ip, acquired.incarnation, acquired.acquisition));
        assert_eq!((released.before, released.after), (1, 0));
        assert!(physical.owner.opens().flow.as_ref().unwrap().defer_receipt.is_none());
        let retry = physical.owner.flow_observation().unwrap().retry;
        retained(&physical, id, &source, retry);
        assert!(!physical.owner.ledger().quarantined());
        assert!(!physical.dispatch.flow_shutdown_complete(&physical.server.sockets, &mut physical.contexts, &physical.pool));
        match case {
            Case::Deferred => {
                assert!(service(&mut physical, id), "retained exact ACK must consume once");
                assert_eq!(settled(&physical.owner), source);
                for _ in 0..3 {
                    physical.control().await;
                    physical.flush().await;
                    assert!(physical.dispatch.flow_shutdown_complete(&physical.server.sockets, &mut physical.contexts, &physical.pool));
                    assert_eq!(physical.pool.usage(), (1, 0));
                    assert_eq!(physical.contexts[&id.handle].conn_epoch, id.epoch + 1);
                    assert!(physical.contexts[&id.handle].flow_mirror.is_none());
                    assert_eq!(settled(&physical.owner), source);
                }
            }
            Case::PriorFlowQ => {
                physical.owner.quarantine_flow(FlowQRow::unheld_for_test());
                assert!(service(&mut physical, id), "D210: a Flow Q raised before this Turn does not refuse it");
                assert_eq!(physical.owner.opens().live, 0);
                assert!(!physical.owner.ledger().session_quarantined());
                assert!(!physical.dispatch.flow_shutdown_complete(&physical.server.sockets, &mut physical.contexts, &physical.pool),
                    "a Flow Q still ends the session Incomplete");
                assert!(physical.owner.ledger().session_quarantined(), "bounded Incomplete end at stop");
            }
            Case::TurnFlowQ => {
                physical.owner.arm_before_release_query_flow_q();
                assert!(!service(&mut physical, id), "B560: a Flow Q raised inside the Turn refuses it");
                {
                    let opens = physical.owner.opens();
                    assert_eq!(opens.live, 1);
                    assert!(matches!(&opens.slots[id.index].as_ref().unwrap().phase,
                        OpenPhase::Flow(flow) if flow.stage == FlowStage::Quarantined));
                }
                assert!(physical.owner.ledger().quarantined() && !physical.owner.ledger().session_quarantined());
            }
            Case::WrongBytes | Case::WrongIdentity | Case::Missing | Case::Physical(_) | Case::Field(_) => {
                match case {
                    Case::Physical(fault) => {
                        if matches!(fault, PhysicalFault::ScanRoot) {
                            physical.dispatch.begin_turn().await;
                            assert_eq!(physical.owner.control_remaining.load(
                                std::sync::atomic::Ordering::Relaxed), 32);
                        }
                        reject_physical_fault(&mut physical, id, fault);
                        // F8: every physical fault here is root/identity/shared
                        // custody ⇒ Session scope, never a row-contained Flow Q.
                        assert!(physical.owner.ledger().session_quarantined()
                            && physical.owner.stop.stopping.load(std::sync::atomic::Ordering::Acquire),
                            "{fault:?} must be Session scope");
                    }
                    Case::Field(fault) => {
                        physical.contexts.get_mut(&id.handle).unwrap().flow_mirror.as_mut().unwrap()
                            .replace_receipt(Some(receipt.corrupt_actual(fault)));
                        assert!(!service(&mut physical, id), "actual field {fault:?}");
                        physical.contexts.get_mut(&id.handle).unwrap().flow_mirror.as_mut().unwrap()
                            .replace_receipt(Some(receipt));
                    }
                    Case::WrongIdentity => {
                        physical.contexts.get_mut(&id.handle).unwrap()
                            .flow_mirror.as_mut().unwrap().replace_receipt(Some(receipt.wrong_generation()));
                        assert!(!service(&mut physical, id));
                        physical.contexts.get_mut(&id.handle).unwrap()
                            .flow_mirror.as_mut().unwrap().replace_receipt(Some(receipt));
                    }
                    Case::WrongBytes => {
                        assert!(!physical.dispatch.release_flow(receipt.wrong_data_bytes()));
                    }
                    Case::Missing => {
                        physical.contexts.get_mut(&id.handle).unwrap()
                            .flow_mirror.as_mut().unwrap().replace_receipt(None);
                        assert!(!service(&mut physical, id));
                        physical.contexts.get_mut(&id.handle).unwrap()
                            .flow_mirror.as_mut().unwrap().replace_receipt(Some(receipt));
                    }
                    Case::Deferred | Case::PriorFlowQ | Case::TurnFlowQ => unreachable!(),
                }
                assert!(physical.owner.ledger().quarantined());
                retained(&physical, id, &source, retry);
                let accepted = service(&mut physical, id);
                let after = physical.owner.flow_observation().unwrap();
                assert!(!accepted, "receipt disagreement must be sticky, not accept restored ACK: {case:?} {after:?}");
                assert!(!physical.dispatch.release_flow(receipt), "direct root retry cannot bypass quarantine");
                for _ in 0..3 {
                    physical.control().await;
                    physical.flush().await;
                    assert!(!service(&mut physical, id));
                    assert!(!physical.dispatch.flow_shutdown_complete(&physical.server.sockets, &mut physical.contexts, &physical.pool));
                    retained(&physical, id, &source, retry);
                    assert!(physical.owner.ledger().quarantined());
                }
                let opens = physical.owner.opens();
                assert!(matches!(&opens.slots[id.index].as_ref().unwrap().phase,
                    OpenPhase::Flow(flow) if flow.stage == FlowStage::Quarantined));
            }
        }
        ready.assert_current(1);
    }).await;
    drop(halves.take());
    // Quarantine deliberately retains logical custody. All actual provider
    // resources are already retired; never clear the row to fake cleanup.
    // On an earlier fixture failure, close the peer and preserve the first panic.
    drop(physical);
    let cleanup = capture(ready.close()).await;
    if let Err(panic) = scenario {
        std::panic::resume_unwind(panic);
    }
    if let Err(panic) = cleanup {
        std::panic::resume_unwind(panic);
    }
}

#[tokio::test]
async fn retained_actual_receipt_consumes_once_after_physical_commit() {
    actual_receipt(Case::Deferred).await;
}

#[tokio::test]
async fn d210_flow_q_before_the_turn_does_not_refuse_release() {
    actual_receipt(Case::PriorFlowQ).await;
}

#[tokio::test]
async fn d210_b560_flow_q_raised_inside_the_turn_refuses_release() {
    actual_receipt(Case::TurnFlowQ).await;
}

#[tokio::test]
async fn wrong_actual_receipt_cannot_be_repaired_into_clean_zero() {
    actual_receipt(Case::WrongBytes).await;
}

#[tokio::test]
async fn missing_actual_receipt_remains_nonzero_after_original_is_restored() {
    actual_receipt(Case::Missing).await;
}

#[tokio::test]
async fn wrong_actual_receipt_identity_keeps_its_physical_donor_quarantined() {
    actual_receipt(Case::WrongIdentity).await;
}

#[tokio::test]
async fn missing_actual_root_cannot_accept_restored_physical_ack() {
    actual_receipt(Case::Physical(PhysicalFault::MissingRoot)).await;
}

#[tokio::test]
async fn actual_slot_envelope_rejection_survives_restoration() {
    actual_receipt(Case::Physical(PhysicalFault::SlotEpoch)).await;
}

#[tokio::test]
async fn actual_root_audit_rejection_survives_restoration() {
    actual_receipt(Case::Physical(PhysicalFault::RootAudit)).await;
}

#[tokio::test]
async fn actual_physical_identity_faults_keep_original_custody() {
    for fault in [
        PhysicalFault::SlotHandle,
        PhysicalFault::MissingContext,
        PhysicalFault::MissingMirror,
        PhysicalFault::MirrorIdentity,
        PhysicalFault::ContextEpoch,
        PhysicalFault::PhysicalFlag,
    ] {
        actual_receipt(Case::Physical(fault)).await;
    }
}

#[tokio::test]
async fn d216_egress_held_permit_fault_is_session_scope() {
    actual_receipt(Case::Physical(PhysicalFault::EgressHeld)).await;
}

#[tokio::test]
async fn d216_local_reset_second_owner_is_session_scope() {
    actual_receipt(Case::Physical(PhysicalFault::LocalReset)).await;
}

#[tokio::test]
async fn actual_receipt_revalidated_fields_cannot_be_restored_into_clean_zero() {
    use ReceiptFault::*;
    for fault in [
        Index,
        Epoch,
        NextEpoch,
        Messages,
        TransferredMessages,
        DiscardedMessages,
        TransferredBytes,
        DiscardedBytes,
        Escrow,
        Uplink,
        Downlink,
        FakeMissing,
        FakeIp,
        FakeIncarnation,
        FakeAcquisition,
        FakeBefore,
        FakeAfter,
        SumOverflow,
    ] {
        actual_receipt(Case::Field(fault)).await;
    }
}

#[tokio::test]
async fn census_only_missing_root_keeps_its_actual_donor_nonzero() {
    actual_receipt(Case::Physical(PhysicalFault::CensusRoot)).await;
}

#[tokio::test]
async fn shutdown_scan_missing_root_keeps_its_actual_donor_nonzero() {
    actual_receipt(Case::Physical(PhysicalFault::ScanRoot)).await;
}

#[tokio::test]
async fn census_only_missing_socket_keeps_its_actual_donor_nonzero() {
    actual_receipt(Case::Physical(PhysicalFault::CensusSocket)).await;
}
