use super::*;

#[test]
fn exact_cold_descriptor_full_is_atomic_and_payload_is_not_a_free_slot() {
    let layout = Layout::array::<RxPacket>(3).unwrap().size();
    for limit in [layout - 1, layout] {
        let budget = bytes::BackingBudget::new(limit).unwrap();
        let meter = BackingMeter::with_budget(&budget);
        let result = RxStock::native(3, &meter);
        if limit < layout {
            assert!(result.is_err());
        } else {
            let mut stock = result.unwrap();
            assert_eq!(stock.staged.capacity(), 3);
            assert_eq!(budget.used(), layout);
            assert_eq!(
                stock.prepare(128).unwrap_err().kind(),
                ErrorKind::WouldBlock
            );
            assert!(stock.prepared.is_none() && !stock.has_current());
        }
        assert_eq!(budget.used(), 0);
        assert!(!budget.quarantined());
    }
}
#[test]
fn full_staging_preserves_current_and_escape_keeps_charge_until_last_alias() {
    let (budget, meter, mut stock) = native(1);
    let cold = budget.used();
    stock.prepare(128).unwrap()[..4].copy_from_slice(b"head");
    stock.finish_read(4, 0).unwrap();
    assert!(stock.stage());
    stock.prepare(128).unwrap()[..4].copy_from_slice(b"tail");
    stock.finish_read(4, 0).unwrap();
    let charged = budget.used();
    let pointer = stock.current.as_ref().unwrap().as_ptr();
    assert!(!stock.stage());
    assert_eq!(stock.current.as_ref().unwrap().as_ptr(), pointer);
    assert_eq!(stock.peek(), Some(&b"tail"[..]));
    assert_eq!(budget.used(), charged);
    let first = stock.take_staged().unwrap();
    assert!(first.precharged_backing_matches(&meter));
    let first = first.into_raw().unwrap().freeze();
    let alias = first.slice(1..3);
    assert!(stock.stage());
    stock.discard(&mut (0, 0));
    assert_eq!(
        budget.used(),
        cold + BytesMut::metered_copy_layout_for_len(128).unwrap()
    );
    drop(stock);
    drop(first);
    assert_eq!(
        budget.used(),
        BytesMut::metered_copy_layout_for_len(128).unwrap()
    );
    assert_eq!(&alias[..], b"ea");
    drop(alias);
    assert_eq!(budget.used(), 0);
    assert!(!budget.quarantined());
}
#[tokio::test]
async fn cancelled_borrowed_direct_read_keeps_one_original_preparation_then_strips_pi() {
    let (budget, meter, mut stock) = native(2);
    let (mut writer, mut reader) = tokio::io::duplex(256);
    let cold = budget.used();
    {
        let mut read = Box::pin(reader.read(stock.prepare(128).unwrap()));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(std::task::Waker::noop()))
                .is_pending()
        );
    }
    let pointer = stock.prepared.as_ref().unwrap().as_ptr();
    assert_eq!(
        budget.used(),
        cold + BytesMut::metered_copy_layout_for_len(128).unwrap()
    );
    writer.write_all(&[0, 0, 0, 2, b'i', b'p']).await.unwrap();
    let n = reader.read(stock.prepare(128).unwrap()).await.unwrap();
    assert_eq!(stock.prepared.as_ref().unwrap().as_ptr(), pointer);
    stock.finish_read(n, 4).unwrap();
    let escaped = stock.take().unwrap();
    assert_eq!(&escaped[..], b"ip");
    assert!(escaped.precharged_backing_matches(&meter));
    assert_eq!(escaped.as_ptr(), unsafe { pointer.add(4) });
    drop(stock);
    assert!(budget.used() > 0);
    drop(escaped);
    assert_eq!(budget.used(), 0);
}
#[test]
fn raw_foreign_and_current_full_rejections_keep_borrowed_input() {
    for wrong_source in [false, true] {
        let (budget, _, mut stock) = native(1);
        let foreign = BackingMeter::with_budget(&budget);
        let mut packet = Some(RxPacket::from(if wrong_source {
            BytesMut::try_zeroed_metered(64, &foreign).unwrap()
        } else {
            BytesMut::zeroed(64)
        }));
        let pointer = packet.as_ref().unwrap().as_ptr();
        assert!(stock.install(&mut packet, 0).is_err());
        assert_eq!(packet.as_ref().unwrap().as_ptr(), pointer);
        assert!(!stock.has_current());
        assert!(budget.quarantined());
        drop((stock, packet));
        assert_eq!(budget.used(), 1);
    }
    let (budget, meter, mut stock) = native(1);
    let mut a = Some(BytesMut::try_zeroed_metered(16, &meter).unwrap().into());
    let mut b = Some(BytesMut::try_zeroed_metered(16, &meter).unwrap().into());
    stock.install(&mut a, 0).unwrap();
    assert!(a.is_none());
    assert_eq!(
        stock.install(&mut b, 0).unwrap_err().kind(),
        ErrorKind::WouldBlock
    );
    assert!(b.is_some());
    stock.discard(&mut (0, 0));
    drop((stock, b));
    assert_eq!(budget.used(), 0);
}
#[test]
fn forced_prepared_or_staged_loss_and_physical_capacity_corruption_stay_nonzero_q() {
    for staged in [false, true] {
        let (budget, _, mut stock) = native(1);
        stock.prepare(64).unwrap();
        if staged {
            stock.finish_read(64, 0).unwrap();
            assert!(stock.stage());
        }
        drop(stock);
        assert!(budget.quarantined());
        assert_eq!(budget.used(), 1);
    }
    let (budget, _, mut stock) = native(1);
    stock.staged.reserve(8); // Explicit physical-corruption probe, never a production admission.
    assert!(stock.admit().is_err());
    drop(stock);
    assert_eq!(budget.used(), 1);
}
#[test]
fn real_device_receive_full_tx_preserves_native_staged_packet() {
    let (budget, meter, mut rx) = native(1);
    rx.prepare(64).unwrap()[..4].copy_from_slice(b"ipv4");
    rx.finish_read(4, 0).unwrap();
    assert!(rx.stage());
    let mut egress = egress::Egress::native(1, 1200, tun_packet_header_len(), &meter).unwrap();
    egress
        .token(1200, tun_packet_header_len())
        .unwrap()
        .consume(4, |out| out.copy_from_slice(b"busy"));
    let mut device = VirtualTunDevice {
        backend: VirtualTunBackend::Stopped,
        cut: cut::CutBoundary::default(),
        stopping: false,
        shutdown_failure: None,
        shutdown_failure_transferred: false,
        shutdown_callbacks: None,
        interface_name: None,
        mtu: 1200,
        rx,
        egress,
    };
    assert!(
        device
            .receive(smoltcp::time::Instant::from_millis(0))
            .is_none()
    );
    assert_eq!(&device.rx.staged.front().unwrap()[..], b"ipv4");
    assert_eq!(device.rx.staged.len(), 1);
    assert!(!budget.quarantined());
    device.rx.discard(&mut (0, 0));
    device.egress.discard(&mut (0, 0));
    drop(device);
    assert_eq!(budget.used(), 0);
}

#[tokio::test]
async fn physical_backing_gate_precedes_read_and_every_selected_rx_transfer() {
    for site in 0..4 {
        for foreign in [false, true] {
            let (budget, _, mut stock) = native(1);
            let other = BackingMeter::with_budget(&budget);
            stock.prepare(64).unwrap();
            let replacement = if foreign {
                BytesMut::try_zeroed_metered(64, &other).unwrap()
            } else {
                BytesMut::zeroed(64)
            };
            let mut read_called = false;
            let mut escaped = None;
            let peeked;
            // Deliberate private physical corruption. No source/packet gate is replaced.
            if site == 0 {
                stock.prepared = Some(replacement.into());
                if let Ok(buffer) = stock.prepare(64) {
                    read_called = true;
                    tokio::io::repeat(7).read_exact(buffer).await.unwrap();
                }
                peeked = false;
            } else {
                stock.finish_read(64, 0).unwrap();
                if site == 3 {
                    assert!(stock.stage());
                    stock.staged[0] = replacement.into();
                    escaped = stock.take_staged();
                    peeked = false;
                } else {
                    stock.current = Some(replacement.into());
                    if site == 1 {
                        peeked = stock.peek().is_some();
                    } else {
                        escaped = stock.take().map(Into::into);
                        peeked = false;
                    }
                }
            }
            let exposed = read_called || peeked || escaped.is_some();
            let immediate_q = budget.quarantined();
            let retained =
                stock.prepared.is_some() || stock.current.is_some() || !stock.staged.is_empty();
            drop(escaped);
            stock.discard(&mut (0, 0));
            drop(stock);
            assert!(
                !exposed,
                "site={site} foreign={foreign}: unadmitted backing reached the real read/selected consumer"
            );
            assert!(retained && immediate_q);
            assert_eq!(budget.used(), 1);
        }
    }
}

#[test]
fn cold_disposal_validates_every_owned_backing_before_destroying_it() {
    for foreign in [false, true] {
        let (budget, meter, mut stock) = native(2);
        for _ in 0..2 {
            stock.prepare(64).unwrap();
            stock.finish_read(64, 0).unwrap();
            assert!(stock.stage());
        }
        let other = BackingMeter::with_budget(&budget);
        stock.staged[1] = RxPacket::from(if foreign {
            BytesMut::try_zeroed_metered(64, &other).unwrap()
        } else {
            BytesMut::zeroed(64)
        });
        assert!(
            stock
                .staged
                .front()
                .unwrap()
                .precharged_backing_matches(&meter)
        );
        stock.discard(&mut (0, 0));
        let immediate = budget.quarantined();
        drop(stock);
        assert!(
            immediate,
            "unselected physical corruption must not disappear through cold disposal"
        );
        assert_eq!(budget.used(), 1);
    }
    let (budget, meter, mut stock) = native(1);
    stock.prepare(64).unwrap();
    stock.current = Some(BytesMut::try_zeroed_metered(64, &meter).unwrap().into());
    let mut input = Some(BytesMut::try_zeroed_metered(64, &meter).unwrap().into());
    assert_eq!(
        stock.install(&mut input, 0).unwrap_err().kind(),
        ErrorKind::BrokenPipe
    );
    assert!(input.is_some() && stock.current.is_some() && stock.prepared.is_some());
    stock.discard(&mut (0, 0));
    drop((stock, input));
    assert_eq!(budget.used(), 1);
}
