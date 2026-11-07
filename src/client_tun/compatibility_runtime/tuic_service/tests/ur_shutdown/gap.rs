//! Real QUIC wire gaps through the original saved Service. These tests own only
//! real peer/App work and one server-network fault; the original root retains
//! stream DATA, task handles, provider retirement and D/Node. Gap inventories
//! are actual samples, never constructed receiver state.
use super::udp_fault;
use super::*;
mod native;

#[tokio::test(flavor = "current_thread")]
async fn actual_thin_wire_gap_retains_nonzero_ur_until_four_joins_and_provider_before_device_node()
{
    exercise(Ending::Stop).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_thin_wire_gap_read_fatal_retains_first_and_charged_q_after_original_limits() {
    exercise(Ending::ReadFatal).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_thin_wire_cap4m_recovers_after_original_prefix_is_acknowledged() {
    exercise(Ending::Cap4M).await;
}

#[tokio::test(flavor = "current_thread")]
async fn actual_thin_wire_cap4m_delivers_exact_payload_eof_and_provider_before_device_node() {
    exercise(Ending::Cap4MClean).await;
}

fn retained_io(owner: &SessionControl) -> (usize, String, Vec<u8>) {
    let value = {
        let ledger = owner.ledger();
        ledger.first_failure.as_ref().map(|first| {
            (
                first.facts().raw_os_code,
                first.facts().origin.phase == FailurePhase::DeviceRead,
                first as *const _ as usize,
                format!("{:?}", first.facts()),
                first.text_bytes().to_vec(),
            )
        })
    };
    let (code, device_read, address, facts, text) = value.expect("original IO first");
    assert_eq!(code, Some(5));
    assert!(device_read);
    (address, facts, text)
}

#[derive(Clone, Copy, Debug)]
enum Ending {
    Stop,
    ReadFatal,
    Cap4M,
    Cap4MClean,
}

async fn exercise(ending: Ending) {
    let mut server_config = UdpPair::server_config(8, 65536, 100, true);
    if matches!(ending, Ending::Cap4M | Ending::Cap4MClean) {
        // A real peer transport configuration for this capacity falsifier only.
        // All other original transport fields, including PTO, remain unchanged.
        let mut cubic = quinn::congestion::CubicConfig::default();
        cubic.initial_window(8 * 1024 * 1024);
        Arc::get_mut(&mut server_config.transport)
            .unwrap()
            .congestion_controller_factory(Arc::new(cubic));
    }
    let (mut server, fault) = udp_fault::server(server_config);
    let policy = Arc::new(RelayFamilyPolicy::from_sources(RelayFamilyPolicySources {
        thin: Some("1"),
        tuic_unordered_reassembly: Some("1"),
        ..Default::default()
    }));
    assert_eq!(policy.generic_engine(), TcpRelayEngine::ThinStaging);
    assert_eq!(
        policy.tuic_generic_mode(),
        TuicGenericStreamMode::UnorderedReassembly
    );
    let cfg = config(server.local_addr().unwrap()).with_relay_policy(policy.clone());
    assert_eq!(cfg.tcp_pool, 1);
    let runtime = TunRuntimeConfig::from_sources(Some("1"))
        .unwrap()
        .with_relay_policy(policy.clone());
    assert!(
        Arc::ptr_eq(&policy, cfg.relay_policy()) && Arc::ptr_eq(&policy, runtime.relay_policy())
    );
    let (device, application) = LoopbackTunDevice::bounded_pair(64, 4);
    let gate = Arc::new(Gate::default());
    let mut service = prepare(
        GatedDevice {
            inner: device,
            gate: gate.clone(),
        },
        cfg.clone(),
        runtime,
        Arc::new(Metrics::default()),
        NoopSink,
    );
    let control = service.take_handle().unwrap();
    let saved = service.control.clone();
    let (watch, registration) = TuicUpstream::watch_ur_carrier_for_test(1);
    let mut app = make_app(application);
    let mut peer = None;
    let mut streams = [None, None];
    let mut retirement_started = [false; 2];
    let mut completed = false;
    let mut forced = false;
    let wire_until = tokio::time::Instant::now() + Duration::from_secs(5);
    let phase = std::cell::Cell::new("auth");
    let scenario=capture(async {
        wire::drive_until(&mut service,wire_until,async {
            let p=server.accept().await.unwrap().await.unwrap();
            wire::auth(&p,&cfg).await;peer=Some(p);
        }).await;
        phase.set("healthy TCP/UDP");
        let p=peer.as_ref().unwrap();
        let (healthy,assoc)=wire::drive_until(&mut service,wire_until,wire::exchange(&mut app,p)).await;
        assert_eq!(healthy.0.id().index(),0);streams[0]=Some(healthy);
        assert_eq!(watch.snapshot().created,0);
        println!("B68 healthy B TCP+sameUDP={assoc} actual before wire fault; no healthy-during-loss claim");
        phase.set("A CONNECT+7B");
        let owner=original_owner(&saved);
        let mut socket=tcp::Socket::new(tcp::SocketBuffer::new(vec![0;4096]),tcp::SocketBuffer::new(vec![0;4096]));
        socket.connect(app.iface.context(),(Ipv4Address::from(app.target),10002),40003).unwrap();
        let a=app.sockets.add(socket);
        wire::drive_until(&mut service,wire_until,async {
            let remote=wire::accept(p,10002,b"blocked");
            let application=async {
                loop {
                    app.turn().await;
                    let socket=app.sockets.get_mut::<tcp::Socket>(a);
                    if socket.can_send() {
                        assert_eq!(socket.send_slice(b"blocked").unwrap(),7);
                        app.turn().await; // Actual original iface.poll/flush_tx after enqueue.
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            };
            let (original,())=tokio::join!(remote,application);
            assert_eq!(original.0.id().index(),1);streams[1]=Some(original);
        }).await;
        println!("B68 A original CONNECT+7B actually read by peer");
        phase.set("prefix write");
        let contracts=owner.flow_contracts();
        assert_eq!(contracts.len(),2);
        let target=contracts.iter().find(|f|f.id.index==1).unwrap();
        assert!(target.active && target.published && target.f && target.attempt);
        let key=(target.id,target.slot_deadline,target.open_deadline);
        let before_stream_frames=p.stats().frame_tx.stream;
        fault.prefix().unwrap();
        wire::drive_until(&mut service,wire_until,async {
            streams[1].as_mut().unwrap().0.write_all(&[0x41;1024]).await.unwrap();
            phase.set("prefix original STREAM+held barrier");
            println!("B68 prefix1024 original write returned; delivery not inferred");
            loop {
                if p.stats().frame_tx.stream>before_stream_frames && fault.snapshot().held_bytes>0 {break;}
                tokio::task::yield_now().await;
            }
        }).await;
        let prefix=fault.snapshot();
        println!("B68 prefix original STREAM+held barrier passed {prefix:?}");
        phase.set("suffix write");
        assert_eq!(prefix.counts.captured,1);
        assert!(prefix.held_bytes>0 && prefix.held_bytes<=65535);
        if matches!(ending,Ending::Cap4M|Ending::Cap4MClean) {
            const CAP:usize=4*1024*1024;
            forced=true;
            phase.set("actual 4MiB cap through original wire");
            fault.flood().unwrap();
            wire::drive_until(&mut service,wire_until,async {
                let tx=&mut streams[1].as_mut().unwrap().0;
                tx.write_all(&vec![0x5a;CAP+65536]).await.unwrap();
                tx.finish().unwrap();
                println!("B69 cap actual tail{} originalwrite+finish; serverCubic initial8MiB only; no default-config claim",CAP+65536);
                loop {
                    let actual=watch.snapshot();
                    if let Some(i)=actual.latest {
                        assert_eq!(i.stored,i.independently_summed);
                        assert!(i.stored<=CAP);
                        if i.stored==CAP && i.next==0 && i.front.is_some_and(|(offset,_,_)|offset>i.next) {break;}
                    }
                    tokio::task::yield_now().await;
                }
            }).await;
            let full=watch.snapshot().latest.unwrap();
            assert_eq!((full.stored,full.independently_summed),(CAP,CAP));
            assert!(watch.snapshot().before_drop.is_none());
            let held=fault.snapshot();
            assert_eq!(held.mode,udp_fault::Mode::Flood);
            assert_eq!((held.held_pointer,held.held_bytes),(prefix.held_pointer,prefix.held_bytes));
            assert_eq!(held.counts.restored,0);
            println!("B69 actual cap4194304 originalUR={full:?} network={held:?} wire5={wire_until:?}");
            phase.set("cap realheld restore and all stream DATA ACK");
            wire::drive_until(&mut service,wire_until,fault.restore()).await.unwrap();
            let restored=fault.snapshot();
            assert_eq!(restored.mode,udp_fault::Mode::Pass);
            assert_eq!((restored.held_bytes,restored.counts.restored,restored.counts.restored_bytes),(0,1,prefix.held_bytes as u64));
            let acked=wire::drive_until(&mut service,wire_until,streams[1].as_mut().unwrap().0.stopped()).await.unwrap();
            assert_eq!(acked,None,"original finish DATA fully acknowledged, including missing prefix");
            println!("B69 actual ALL DATA ACK including prefix after realrestore: stopped=None currentUR={:?} network={restored:?} originalwire5={wire_until:?}",watch.snapshot().latest);
            phase.set("cap recovered prefix must advance original UR");
            wire::drive_until(&mut service,wire_until,async {
                loop {
                    let actual=watch.snapshot().latest.unwrap();
                    assert_eq!(actual.stored,actual.independently_summed);
                    assert!(actual.stored<=CAP);
                    if actual.next>full.next && actual.stored<CAP {break;}
                    app.turn().await;
                    tokio::task::yield_now().await;
                }
            }).await;
            if matches!(ending,Ending::Cap4MClean) {
                phase.set("full cap original App exact payload and EOF");
                let total=1024+CAP+65536;
                let mut delivered=0usize;
                let mut scratch=[0;8192];
                wire::drive_until(&mut service,wire_until,async {
                    loop {
                        app.turn().await;
                        let socket=app.sockets.get_mut::<tcp::Socket>(a);
                        while socket.can_recv() {
                            let n=socket.recv_slice(&mut scratch).unwrap();
                            assert!(n>0 && delivered+n<=total,"no duplicate or extra cap payload");
                            for byte in &scratch[..n] {
                                let expected=if delivered<1024 {0x41} else {0x5a};
                                assert_eq!(*byte,expected,"exact original byte at {delivered}");
                                delivered+=1;
                            }
                        }
                        assert_ne!(socket.state(),tcp::State::Closed,"RST is not the original EOF");
                        if socket.state()==tcp::State::CloseWait {
                            assert_eq!(delivered,total);
                            assert!(!socket.may_recv() && !socket.can_recv());
                            assert_eq!(socket.recv_queue(),0);
                            socket.close();
                            app.turn().await; // Actual final FIN/ACK flush, not a receipt marker.
                            break;
                        }
                        let current=watch.snapshot().latest.unwrap();
                        assert_eq!(current.stored,current.independently_summed);
                        assert!(current.stored<=CAP && current.next<=total as u64);
                        tokio::task::yield_now().await;
                    }
                    while app.sockets.get::<tcp::Socket>(a).state()!=tcp::State::Closed {
                        app.turn().await;
                        tokio::task::yield_now().await;
                    }
                    app.turn().await;
                }).await;
                println!("B71 actual cap full App bytes{delivered}/4260864 exact prefix1024+tail4259840/EOF/AppClosed samewire5={wire_until:?}");
                phase.set("full cap original stop and actual provider retirement");
                control.request_stop();
                let parent_limit=parent_deadline(&saved).unwrap();
                let mut observer=Box::pin(control.observe_shutdown());
                tokio::time::timeout_at(parent_limit,async {
                    let work=async {
                        let [healthy,finished]=&mut streams;
                        let [b_started,a_started]=&mut retirement_started;
                        let finished_retire=async {
                            assert!(!*a_started);
                            *a_started=true; // Install before the first real retirement await.
                            let (tx,rx)=finished.as_mut().unwrap();
                            assert_eq!(tx.stopped().await.unwrap(),None,"original A FIN already fully ACKed");
                            assert!(rx.read_to_end(0).await.unwrap().is_empty(),"actual original A uplink EOF");
                            *finished=None; // Only after both original peer halves actually settled.
                        };
                        tokio::join!(finished_retire,retire_once(healthy,b_started));
                        while gate.polls.load(Ordering::Acquire)==0 {
                            app.turn().await;
                            tokio::task::yield_now().await;
                        }
                    };
                    tokio::select! {()=work=>(), report=service.run()=>panic!("Dgate bypassed {report:?}")}
                }).await.unwrap();
                let actual=owner.flow_observation().unwrap();
                assert_eq!((actual.live,actual.w,actual.f,actual.reserved,actual.escrow_bytes,actual.running,actual.attempts),(0,0,0,0,0,0,0));
                assert_eq!((actual.factory_started,actual.published,actual.released,actual.joined),(2,2,2,8));
                assert!(actual.audit && owner.business_complete.load(Ordering::Acquire));
                assert!(!owner.driver_complete.load(Ordering::Acquire));
                let facts=owner_facts(&owner);assert!(facts.0 && !facts.1);
                let owner_limit=facts.2.unwrap();
                assert_eq!(parent_deadline(&saved),Some(parent_limit));
                assert_eq!((gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0));
                let disposed=watch.snapshot();let final_ur=disposed.before_drop.unwrap();
                assert!(disposed.drop_returned);
                assert_eq!(final_ur.stream,1);
                assert_eq!((final_ur.next,final_ur.stored,final_ur.independently_summed,final_ur.chunks),(total as u64,0,0,0));
                assert_eq!(final_ur.front,None);
                assert!(p.close_reason().is_none() && service.completion.is_none());
                assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err());
                println!("B71 actual cap full originalURDrop={final_ur:?}/provider released2/join8/businesszero BEFORE DNode {actual:?}, owner15={owner_limit:?} parent15={parent_limit:?}");
                phase.set("full cap DNode actual join");
                gate.open();
                let (report,final_phase)=tokio::time::timeout_at(parent_limit,async {tokio::join!(service.run(),observer.as_mut())}).await.unwrap();
                assert_eq!(final_phase,Phase::Complete);
                assert_eq!(report.session,SessionDeviceState::DeviceSettled);
                assert_eq!(report.node,Some(NodeShutdown::RunningResourcesSettled));
                assert_eq!((report.rebuilds,report.discarded_packets,report.discarded_backing),(0,0,0));
                assert_eq!((gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(1,1));
                println!("B71 actual cap full final {report:?}");
                completed=true;
                forced=false;
                return;
            }
            println!("B69 cap actual recovered originalUR={:?}; forced final disposal is not settlement",watch.snapshot().latest);
            return;
        }
        fault.suffix().unwrap();
        wire::drive_until(&mut service,wire_until,async {
            streams[1].as_mut().unwrap().0.write_all(&[0x5a;4096]).await.unwrap();
            phase.set("actual UR gap");
            println!("B68 suffix4096 original write returned; actual UR gap still required");
            loop {
                let actual=watch.snapshot();
                if actual.latest.is_some_and(|i|i.stream==1 && i.stored>0 && i.stored==i.independently_summed && i.front.is_some_and(|(offset,_,_)|offset>i.next)) {break;}
                tokio::task::yield_now().await;
            }
        }).await;
        fault.suppress().unwrap();
        let observed=watch.snapshot();
        let gap=observed.latest.unwrap();
        assert_eq!(observed.created,1);
        assert!(observed.before_drop.is_none() && !observed.drop_returned);
        assert!(gap.stored>0 && gap.stored==gap.independently_summed && gap.front.unwrap().0>gap.next);
        let current=owner.flow_contracts().into_iter().find(|f|f.id==key.0).unwrap();
        assert_eq!((current.id,current.slot_deadline,current.open_deadline),key);
        assert!(current.active && current.published && current.f && current.attempt);
        let facts=owner_facts(&owner);assert!(facts.0 && !facts.1);
        println!("B68 actual original Thin gap key={key:?} current={gap:?} network={:?} wire5={wire_until:?}",fault.snapshot());
        if matches!(ending,Ending::ReadFatal) {
            forced=true;
            phase.set("real TunIo read IO5");
            gate.read_error.store(true,Ordering::Release);
            // Actual app ingress wakes the original pending TunIo receive.
            assert_eq!(app.sockets.get_mut::<tcp::Socket>(app.tcp).send_slice(b"wake").unwrap(),4);
            wire::drive_until(&mut service,wire_until,async {
                while owner.ledger().first_failure.is_none() {app.turn().await;tokio::task::yield_now().await;}
            }).await;
            assert!(!gate.read_error.load(Ordering::Acquire));
            let first=retained_io(&owner);
            let owner_limit=owner_facts(&owner).2.unwrap();
            control.request_stop();
            let parent_limit=parent_deadline(&saved).unwrap();
            let mut observer=Box::pin(control.observe_shutdown());
            let mut observer_result=None;
            for _ in 0..4 {
                if observer_result.is_none() {
                    if let Ok(done)=tokio::time::timeout(Duration::from_millis(1),observer.as_mut()).await {
                        assert!(matches!(done,Phase::Quarantined|Phase::Incomplete));
                        observer_result=Some(done);
                    }
                }
                assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
                assert_eq!(parent_deadline(&saved),Some(parent_limit));
                assert_eq!(owner_facts(&owner).2,Some(owner_limit));
                assert_eq!(retained_io(&owner),first);
            }
            phase.set("fatal original carrier Drop and A four joins");
            wire::drive_until(&mut service,parent_limit,async {
                loop {
                    let actual=watch.snapshot();
                    let row=owner.flow_contracts().into_iter().find(|row|row.id==key.0).unwrap();
                    assert_eq!((row.id,row.slot_deadline,row.open_deadline),key);
                    assert!(row.f && row.attempt && row.published);
                    let held=fault.snapshot();
                    assert_eq!(held.mode,udp_fault::Mode::Suppress);
                    assert_eq!((held.held_pointer,held.held_bytes),(prefix.held_pointer,prefix.held_bytes));
                    assert_eq!(held.counts.restored,0);
                    if actual.drop_returned && row.roles_joined==Some(4) {break;}
                    tokio::task::yield_now().await;
                }
            }).await;
            let disposed=watch.snapshot();let before=disposed.before_drop.unwrap();
            assert!(disposed.drop_returned);
            assert_eq!(before.stream,1);assert_eq!(before.next,gap.next);
            assert!(before.stored>=gap.stored && before.stored==before.independently_summed && before.front.unwrap().0>before.next);
            let joins=owner.flow_observation().unwrap();
            assert_eq!((joins.f,joins.attempts,joins.published,joins.released,joins.joined),(2,2,2,0,8));
            assert!(joins.audit);
            println!("B69 actual UR fatal Dropbefore={before:?}/A4/total8 stillprovidercharged {joins:?} first={first:?} owner15={owner_limit:?} parent15={parent_limit:?}");
            phase.set("original fatal first15 expiry");
            // Each original deadline is immutable. This observer cap is the later
            // of those same existing instants, never a renewed shutdown budget.
            let both_original_limits=owner_limit.max(parent_limit);
            assert!(tokio::time::timeout_at(both_original_limits,service.run()).await.is_err());
            assert!(tokio::time::Instant::now()>=owner_limit && tokio::time::Instant::now()>=parent_limit);
            if observer_result.is_none() {observer_result=Some(observer.as_mut().await);}
            let done=observer_result.unwrap();
            assert!(matches!(done,Phase::Quarantined|Phase::Incomplete));
            let bridge=owner.relay_event_test_bridge().unwrap();
            let expiry_observed_at=tokio::time::Instant::now();
            assert_eq!(retained_io(&owner),first);
            assert_eq!(parent_deadline(&saved),Some(parent_limit));
            assert_eq!(owner_facts(&owner).2,Some(owner_limit));
            let at_limit=owner.flow_observation().unwrap();
            assert_eq!((at_limit.f,at_limit.attempts,at_limit.released,at_limit.joined),(2,2,0,8));
            assert_eq!((gate.polls.load(Ordering::Acquire),gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0,0));
            assert!(!control.request_rebuild());
            println!("B69 actual original15 observer={done:?} observedAt={expiry_observed_at:?} owner15={owner_limit:?} parent15={parent_limit:?} firstQ={:?} census={:?} debt={at_limit:?}",owner_facts(&owner),bridge.fatal_census());
            phase.set("actual original cold fatal census/Q publication");
            // The deadline observer has ended. This waits for the original
            // bounded cold transaction's real notification; no new time budget.
            tokio::select! {
                ()=async {
                    loop {
                        let changed=saved.changed.notified();
                        tokio::pin!(changed);changed.as_mut().enable();
                        if control.phase()==Phase::Quarantined {break;}
                        assert_eq!(retained_io(&owner),first);
                        assert_eq!(parent_deadline(&saved),Some(parent_limit));
                        assert_eq!(owner_facts(&owner).2,Some(owner_limit));
                        let debt=owner.flow_observation().unwrap();
                        assert_eq!((debt.f,debt.attempts,debt.released,debt.joined),(2,2,0,8));
                        assert_eq!((gate.polls.load(Ordering::Acquire),gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0,0));
                        assert!(!control.request_rebuild());
                        changed.await;
                    }
                }=>(),
                report=service.run()=>panic!("original fatal debt cannot complete {report:?}"),
            }
            let q_committed_at=tokio::time::Instant::now();
            assert!(owner_facts(&owner).1);
            let (census,inflight)=bridge.fatal_census().expect("actual original cold census");
            assert_eq!(census[0],census[1]);assert_eq!(census[0].1,9);
            assert!((1..=9).contains(&census[0].0));
            assert_eq!(census[2],(0,0));assert_eq!(inflight,0);
            println!("B69 actual cold census/Q commitAt={q_committed_at:?} originalObserver={done:?} expiryObservedAt={expiry_observed_at:?} census={:?}",bridge.fatal_census());
            for _ in 0..4 {
                assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
                assert_eq!(retained_io(&owner),first);
                let facts=owner_facts(&owner);assert!(facts.1 && facts.2==Some(owner_limit));
                assert_eq!(parent_deadline(&saved),Some(parent_limit));
                let row=owner.flow_contracts().into_iter().find(|row|row.id==key.0).unwrap();
                assert_eq!((row.id,row.slot_deadline,row.open_deadline),key);
                assert_eq!(row.roles_joined,Some(4));assert!(row.f && row.attempt && row.published);
                let debt=owner.flow_observation().unwrap();
                assert_eq!((debt.f,debt.attempts,debt.released,debt.joined),(2,2,0,8));assert!(debt.audit);
                assert!(!owner.business_complete.load(Ordering::Acquire) && !owner.driver_complete.load(Ordering::Acquire));
                assert!(service.completion.is_none() && p.close_reason().is_none());
                assert_eq!((gate.polls.load(Ordering::Acquire),gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0,0));
                let held=fault.snapshot();
                assert_eq!(held.mode,udp_fault::Mode::Suppress);
                assert_eq!((held.held_pointer,held.held_bytes),(prefix.held_pointer,prefix.held_bytes));
                assert_eq!(held.counts.restored,0);
                assert_eq!(watch.snapshot().before_drop,Some(before));
                assert!(!control.request_rebuild());
            }
            assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err());
            println!("B69 actual original15 expired observer={done:?} firstVALUE/facts/text kept, samegap4KiB discarded by actualDrop, originalproviderF2/attempt2 charged Q/DNode0/no successor; forced teardown is not clean");
            return;
        }
        phase.set("original stop/four joins");
        control.request_stop();
        let parent_limit=parent_deadline(&saved).unwrap();
        let mut observer=Box::pin(control.observe_shutdown());
        for _ in 0..4 {
            assert!(tokio::time::timeout(Duration::from_millis(1),observer.as_mut()).await.is_err());
            assert!(tokio::time::timeout(Duration::from_millis(1),service.run()).await.is_err());
            let limit=parent_deadline(&saved);assert_eq!(limit,Some(parent_limit));
        }
        let owner_limit=owner_facts(&owner).2.unwrap();
        tokio::time::timeout_at(parent_limit,async {
            let original_drop=async {
                loop {
                    let actual=watch.snapshot();
                    let row=owner.flow_contracts().into_iter().find(|f|f.id==key.0)
                        .expect("same original A retained until actual joins and wire restore");
                    assert_eq!((row.id,row.slot_deadline,row.open_deadline),key);
                    assert!(row.f && row.attempt);
                    let network=fault.snapshot();
                    assert_eq!(network.mode,udp_fault::Mode::Suppress);
                    assert_eq!((network.held_pointer,network.held_bytes),(prefix.held_pointer,prefix.held_bytes));
                    assert_eq!((network.counts.captured,network.counts.restored),(1,0));
                    let facts=owner_facts(&owner);assert!(facts.0 && !facts.1 && facts.2==Some(owner_limit));
                    assert_eq!(parent_deadline(&saved),Some(parent_limit));
                    if let Some(before)=actual.before_drop {
                        assert_eq!(before.next,gap.next);
                        assert!(before.stored>=gap.stored && before.stored==before.independently_summed
                            && before.front.unwrap().0>before.next);
                    }
                    // Task-local Drop is earlier than the root consuming JoinHandles.
                    if actual.drop_returned && row.roles_joined==Some(4) {break;}
                    tokio::task::yield_now().await;
                }
            };
            tokio::select! {()=original_drop=>(), report=service.run()=>panic!("held network may not settle original service early {report:?}")}
        }).await.unwrap();
        let disposed=watch.snapshot();
        assert!(disposed.drop_returned);
        let original=disposed.before_drop.unwrap();
        assert_eq!(original.stream,1);assert_eq!(original.next,gap.next);
        assert!(original.stored>=gap.stored && original.stored==original.independently_summed && original.front.unwrap().0>original.next);
        let rows=owner.flow_contracts();
        let a=rows.iter().find(|f|f.id==key.0).expect("original provider still charged before real wire restore");
        assert_eq!((a.id,a.slot_deadline,a.open_deadline),key);
        assert_eq!(a.roles_joined,Some(4));assert!(a.f && a.attempt);
        let charged=owner.flow_observation().unwrap();
        assert!(charged.f>0 && charged.released<2);
        assert_eq!((gate.polls.load(Ordering::Acquire),gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0,0));
        let facts=owner_facts(&owner);assert!(facts.0 && !facts.1);
        println!("B68 actual A4 consumed joins/current nonzero original Dropreturned BEFORE provider/wire release; inventory={original:?} charged={charged:?} network={:?}",fault.snapshot());
        phase.set("real wire restore");
        tokio::time::timeout_at(parent_limit,wire::drive_until(&mut service,parent_limit,fault.restore())).await.unwrap().unwrap();
        phase.set("original provider retirement");
        let restored=fault.snapshot();
        assert_eq!(restored.mode,udp_fault::Mode::Pass);assert_eq!(restored.held_bytes,0);
        assert_eq!(restored.counts.restored,1);assert_eq!(restored.counts.restored_bytes,prefix.held_bytes as u64);
        tokio::time::timeout_at(parent_limit,async {
            let work=async {
                retire_two(&mut streams,&mut retirement_started).await;
                while gate.polls.load(Ordering::Acquire)==0 {app.turn().await;tokio::task::yield_now().await;}
            };
            tokio::select! {()=work=>(), report=service.run()=>panic!("Dgate bypassed {report:?}")}
        }).await.unwrap();
        let actual=owner.flow_observation().unwrap();
        assert_eq!((actual.live,actual.w,actual.f,actual.reserved,actual.escrow_bytes,actual.running,actual.attempts),(0,0,0,0,0,0,0));
        assert_eq!((actual.factory_started,actual.published,actual.released,actual.joined),(2,2,2,8));
        assert!(actual.audit && owner.business_complete.load(Ordering::Acquire));
        assert!(!owner.driver_complete.load(Ordering::Acquire));
        let facts=owner_facts(&owner);assert!(facts.0 && !facts.1 && facts.2==Some(owner_limit));
        let limit=parent_deadline(&saved);assert_eq!(limit,Some(parent_limit));
        assert_eq!((gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(0,0));
        assert!(p.close_reason().is_none() && service.completion.is_none());
        assert!(tokio::time::timeout(Duration::from_millis(20),server.accept()).await.is_err());
        println!("B68 actual8joins/provider retired BEFORE DNode {actual:?} restored={restored:?} owner15={owner_limit:?} parent15={parent_limit:?}; backing capacity Unknown");
        phase.set("D/Node actual join");
        gate.open();
        let (report,phase)=tokio::time::timeout_at(parent_limit,async {tokio::join!(service.run(),observer.as_mut())}).await.unwrap();
        assert_eq!(phase,Phase::Complete);
        assert_eq!(report.session,SessionDeviceState::DeviceSettled);
        assert_eq!(report.node,Some(NodeShutdown::RunningResourcesSettled));
        assert_eq!((report.rebuilds,report.discarded_packets,report.discarded_backing),(0,0,0));
        assert_eq!((gate.joins.load(Ordering::Acquire),gate.drops.load(Ordering::Acquire)),(1,1));
        println!("B68 actual gap final {report:?}; no successor");
        completed=true;
    }).await;
    if scenario.is_err() {
        eprintln!(
            "B68 FIRST phase={} network={:?} UR={:?} peerStats={:?}",
            phase.get(),
            fault.snapshot(),
            watch.snapshot(),
            peer.as_ref().map(quinn::Connection::stats)
        );
        let original_weak = {
            saved
                .session
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        };
        if let Some(original) = original_weak.and_then(|weak| weak.upgrade()) {
            eprintln!(
                "B68 FIRST original contracts={:?} firstQdeadline={:?}",
                original.flow_contracts(),
                owner_facts(&original)
            );
        }
    }
    let cleanup=capture(async {
        // Freeze (never renew) the original cleanup limit before first awaited
        // cleanup: actual wire restore must precede any provider retirement.
        control.request_stop();
        let limit=parent_deadline(&saved).unwrap();
        tokio::time::timeout_at(limit,fault.restore()).await.unwrap().unwrap();
        if !forced && !gate.release.load(Ordering::Acquire) {gate.open();}
        if !completed && !forced {
            for (stream,started) in streams.iter_mut().zip(retirement_started.iter()) {
                if *started && stream.is_some() {
                    eprintln!("B68 partial original peer retirement Unknown; forced halves release not clean");
                    drop(stream.take());
                }
            }
            tokio::time::timeout_at(limit,async {tokio::join!(service.run(),retire_two(&mut streams,&mut retirement_started));}).await.unwrap();
        }
    }).await;
    let disposal = capture(async {
        drop(streams);
        drop(service);
        drop(registration);
    })
    .await;
    if let Some(p) = peer {
        p.close(0u32.into(), b"done");
    }
    server.close(0u32.into(), b"done");
    let drain = server.take_driver_drain_handle().unwrap();
    drop(server);
    let drained = tokio::time::timeout(Duration::from_secs(10), drain.drained()).await;
    let final_network = fault.snapshot();
    drop(fault);
    if let Err(error) = scenario {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = cleanup {
        std::panic::resume_unwind(error);
    }
    if let Err(error) = disposal {
        std::panic::resume_unwind(error);
    }
    assert!(drained.unwrap());
    assert_eq!(final_network.held_bytes, 0);
}
