//! knife1：大并发压测 harness 整合测试（feature = "harness"）。
//!
//! 跑法：`cargo test --features harness --test concurrency_harness -- --nocapture`
//! 大并发 sweep 标了 `#[ignore]`，显式跑：`... -- --ignored --nocapture`。
//!
//! 全部 smoltcp/device/mock 复杂度封在 `mini_vpn::harness` 内，本文件只编排 + 断言 + 打表。
#![cfg(feature = "harness")]

use mini_vpn::harness::{
    ScenarioParams, run_tcp_hol_scenario, run_tcp_scenario, run_tcp_slow_open_scenario,
    run_udp_echo_scenario, run_udp_throughput_scenario,
};
use std::time::Duration;

/// 烟雾测试：单连接经回环 device + mock echo 完成一次 TCP 往返。
/// 验证 SUT 主循环（SYN inspector → 建池 → accept → relay → echo 回程）全链路在内存里跑通。
#[tokio::test]
async fn single_tcp_connection_round_trips() {
    let report = run_tcp_scenario(ScenarioParams {
        connections: 1,
        distinct_ports: 1,
        payload_len: 1024,
        pool_size: 2,
        timeout: Duration::from_secs(10),
        cpu_burn_per_flush: Duration::ZERO,
    })
    .await;
    report.print_row();
    assert_eq!(report.completed, 1, "单连接应完成一次 echo 往返");
    assert!(report.tcp_opens >= 1, "mock 上游应至少开一条 TCP");
    assert!(report.bytes_echoed >= 1024, "应收满 echo 回的字节");
}

/// 中等并发正确性（常驻 feature gate）：64 路并发跨 64 端口全部 echo 往返完成。
/// 兼做 Stage 12 那种 loopback 并发回归（localize：通了即证明 relay/调度本身正确）。
#[tokio::test]
async fn concurrent_64_all_complete() {
    let report = run_tcp_scenario(ScenarioParams {
        connections: 64,
        distinct_ports: 64,
        payload_len: 1024,
        pool_size: 8,
        timeout: Duration::from_secs(30),
        cpu_burn_per_flush: Duration::ZERO,
    })
    .await;
    report.print_row();
    assert_eq!(report.completed, 64, "64 路应全部完成（N/N）");
}

/// 刀11：可观测性计数随真负载增长——N 路 TCP flow 各开一次远端 → `relays_spawned` ≥ 完成数。
/// （cumulative counter 走事件点 fetch_add，不依赖 30s gauge tick；gauge 单独单测覆盖。）
#[tokio::test]
async fn metrics_relays_spawned_tracks_load() {
    let report = run_tcp_scenario(ScenarioParams {
        connections: 32,
        distinct_ports: 32,
        payload_len: 1024,
        pool_size: 8,
        timeout: Duration::from_secs(30),
        cpu_burn_per_flush: Duration::ZERO,
    })
    .await;
    report.print_row();
    assert_eq!(report.completed, 32, "32 路应全部完成");
    assert!(
        report.metrics.relays_spawned >= report.completed as u64,
        "relays_spawned({}) 应 ≥ 完成数({})——每条 flow 开远端成功计一次",
        report.metrics.relays_spawned,
        report.completed,
    );
}

/// 刀13：一条上游停读的慢 TCP flow 不应 head-of-line 阻塞另一条正常 flow。
///
/// 旧实现会在主循环里 `tx.send(payload).await` 等慢流 relay channel 腾位，导致 B 流的 SYN/上行/下行
/// 都不能推进；修复后 A 满了只保留 smoltcp 背压，B 应在 A 仍堵住时完成。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stalled_tcp_uplink_does_not_block_other_flows() {
    let report = run_tcp_hol_scenario().await;
    eprintln!("HoL report: {report:?}");
    assert!(
        report.stall_observed,
        "测试必须先观察到 mock 上游停读，否则没有触发 HoL 场景"
    );
    assert!(
        report.normal_completed_while_stalled,
        "A 流上游停读时，B 流仍应完成；否则主循环仍被慢流 HoL 阻塞"
    );
    assert!(
        !report.stalled_completed_while_stalled,
        "释放 stall 前 A 不应完成，否则测试没有保持慢流拥塞窗口"
    );
    assert!(
        report.stalled_completed_after_release,
        "释放 stall 后 A 应最终完成"
    );
    assert!(report.normal_bytes_match, "B 流 echo 字节必须逐字节完整");
    assert!(
        report.stalled_bytes_match,
        "A 流释放后 echo 字节必须逐字节完整"
    );
    assert_eq!(
        report.tcp_opens_while_stalled, 2,
        "stall 期间应只有 A+B 两次 open；spurious rearm/reopen 会破坏 relay 稳定性"
    );
    assert_eq!(
        report.tcp_opens_after_release, 2,
        "释放后也不应为 A 重开远端；Full 路径应保留原 relay 并靠背压恢复"
    );
}

/// 刀14d：一条慢 `open_tcp` 不能 head-of-line 阻塞另一条正常 flow。
///
/// 旧实现会在主循环里 inline await A 的 `open_tcp`，导致 B 的 SYN/上行/下行都不能推进；
/// 修复后 A 的远端开流在后台完成，B 应在 A 仍卡住时完成。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_tcp_open_does_not_block_other_flows() {
    let report = run_tcp_slow_open_scenario().await;
    eprintln!("slow-open report: {report:?}");
    assert!(
        report.slow_open_observed,
        "测试必须先观察到 mock open_tcp 卡住，否则没有触发 slow-open 场景"
    );
    assert!(
        report.normal_completed_while_slow_open,
        "A 流 open_tcp 卡住时，B 流仍应完成；否则主循环仍被慢 open HoL 阻塞"
    );
    assert!(
        report.slow_completed_after_release,
        "释放 slow open 后 A 应最终完成"
    );
    assert!(report.normal_bytes_match, "B 流 echo 字节必须逐字节完整");
    assert!(report.slow_bytes_match, "A 流 echo 字节必须逐字节完整");
    assert_eq!(
        report.tcp_opens_while_slow_open, 2,
        "slow open 期间应只打开 A+B 两条远端流"
    );
    assert_eq!(
        report.tcp_opens_after_release, 2,
        "释放后不应为 A 重开远端；迟到 open 结果应安装原 flow"
    );
}

/// 轻量 UDP liveness：datagram 上行经 mock echo 不被 TCP 饿死（主体吞吐压测留刀3）。
#[tokio::test]
async fn udp_datagrams_reach_upstream() {
    let report = run_udp_echo_scenario(32, 512).await;
    println!("UDP sent={} uplinks={}", report.sent, report.uplinks);
    assert_eq!(report.sent, 32);
    assert!(
        report.uplinks >= 32,
        "全部 UDP datagram 应抵达 mock 上游上行（uplinks={}）",
        report.uplinks
    );
}

/// 刀3 下行**分片重组正确性**（常驻）：大 payload 经 mock 拆成多帧（FRAG_TOTAL>1）回灌，
/// 主循环 `FragReassembler` 必须逐字节还原 → 全部 echo intact。验证重组在真主循环里跑通（无需真网络）。
#[tokio::test]
async fn fragmented_downlink_reassembles_intact() {
    // 4000B payload，chunk=1200 → 4 帧/包；16 个独立 flow。
    let report = run_udp_throughput_scenario(16, 4000, Some(1200), Duration::from_secs(10)).await;
    report.print_row();
    assert_eq!(
        report.echoed_intact, report.sent,
        "分片下行应全部逐字节重组还原（intact={}/{}）",
        report.echoed_intact, report.sent
    );
    assert!(report.fragmented, "本场景应触发分片");
}

/// 刀3 下行 datagram **直通**正确性（常驻，零回归）：单帧（FRAG_TOTAL=1）经新 decode_packet_meta
/// 路径仍正确 echo 回 app。守住「重组改造没破坏快路径」。
#[tokio::test]
async fn passthrough_downlink_still_intact() {
    let report = run_udp_throughput_scenario(16, 1000, None, Duration::from_secs(10)).await;
    report.print_row();
    assert_eq!(
        report.echoed_intact, report.sent,
        "单帧直通应全部完整 echo（intact={}/{}）",
        report.echoed_intact, report.sent
    );
}

/// 刀3.5 quic 模式高量级下行回归（常驻）：quic-relay-mode 下行是**整包不分片**（FRAG_TOTAL=1，
/// uni-stream 承载完整 Packet），等价 passthrough 形态。本测以高量级（300 包）代表高码率直播下行，
/// 守住主循环在持续高速下逐字节零损坏 + 无丢。注：真 datagram↔stream 传输选择在 TuicUpstream、
/// harness 测不到（同 #3 边界，归 acceptance T-B/T-D/T-G）。
#[tokio::test]
async fn quic_mode_highrate_downlink_intact() {
    // 1200B × 300 包 ≈ 代表高码率流的整包下行（mock 直通 = quic 模式下行形态）。
    let report = run_udp_throughput_scenario(300, 1200, None, Duration::from_secs(15)).await;
    report.print_row();
    assert!(
        !report.fragmented,
        "quic 模式下行应为整包直通（FRAG_TOTAL=1，不分片）"
    );
    assert_eq!(
        report.echoed_intact, report.sent,
        "高量级整包下行应全部逐字节完整（intact={}/{}）",
        report.echoed_intact, report.sent
    );
}

/// 刀3 UDP 吞吐 sweep（重，显式 `--ignored` 跑）：对比 passthrough vs 分片下行的吞吐/丢包，
/// 量化主循环 UDP 路径 + 重组成本。真 datagram/stream 兜底对比归真出口 acceptance。
#[tokio::test]
#[ignore = "UDP throughput sweep; run with --ignored --nocapture"]
async fn udp_throughput_sweep_report() {
    println!("\n==== 刀3 UDP 吞吐表（mock 回环，隔离主循环 UDP 路径 + 重组）====");
    for &(payload, chunk) in &[
        (1000usize, None),
        (1400, None),
        (4000, Some(1200usize)),
        (8000, Some(1200)),
    ] {
        let report =
            run_udp_throughput_scenario(500, payload, chunk, Duration::from_secs(60)).await;
        report.print_row();
        assert_eq!(
            report.echoed_intact, report.sent,
            "应零丢包零损坏（mock 无网络丢失）"
        );
    }
    println!(
        "==== 真 datagram TooLarge / stream 兜底 / 真 sing-box 大流量见 acceptance（#3 复测）====\n"
    );
}

/// 大并发 sweep（重，显式 `--ignored` 跑）：N∈{64,256,1024} 多端口，打印三段耗时定位表。
/// 产出供 docs/tech 的瓶颈定位结论引用。
#[tokio::test]
#[ignore = "heavy concurrency sweep; run with --ignored --nocapture"]
async fn concurrency_sweep_report() {
    println!("\n==== knife1 并发压测定位表（mock 回环，隔离客户端处理）====");
    for &n in &[64usize, 256, 1024] {
        let report = run_tcp_scenario(ScenarioParams {
            connections: n,
            distinct_ports: 64,
            payload_len: 1024,
            pool_size: 16,
            timeout: Duration::from_secs(120),
            cpu_burn_per_flush: Duration::ZERO,
        })
        .await;
        report.print_row();
        assert_eq!(report.completed, n, "N={n} 应全部完成");
    }
    println!("==== 端到端 #3（单条 QUIC 连接）见 spec：deferred，需真 sing-box probe ====\n");
}

/// 隔离怀疑瓶颈 #1（all_handles O(n) 全量遍历）：固定 N=256，只变 pool_size →
/// 总 listener 槽 = distinct_ports × pool_size。若 relay 段耗时随「槽数」涨而非随连接数，
/// 即坐实 sweep 成本来自 O(总槽数) 而非 O(活跃连接)。
#[tokio::test]
#[ignore = "experiment for #1; run with --ignored --nocapture"]
async fn pool_size_isolates_sweep_cost() {
    println!("\n==== #1 隔离：固定 N=256，扫 pool_size（总槽=64×pool）====");
    for &pool in &[8usize, 16, 32] {
        let report = run_tcp_scenario(ScenarioParams {
            connections: 256,
            distinct_ports: 64,
            payload_len: 1024,
            pool_size: pool,
            timeout: Duration::from_secs(60),
            cpu_burn_per_flush: Duration::ZERO,
        })
        .await;
        print!("pool={pool:>2} 总槽={:>4} | ", 64 * pool);
        report.print_row();
        assert_eq!(report.completed, 256);
    }
}

/// 验证 #2 修复（弹性扩容打掉每端口 pool 硬上限）：256 路全打到**单个**目标端口，
/// pool_size=2（生产默认）。优化前：超出 2 槽的连接靠慢速 SYN 重传 + rearm，窗口内 done=2/256 stall。
/// 优化后（刀2）：SYN 命中端口即弹性补足空闲 listening 槽，单热门端口（如 :443）突发也能排空 → 接近 N/N。
#[tokio::test]
#[ignore = "verifies #2 elastic pool drains hot port; run with --ignored --nocapture"]
async fn elastic_pool_drains_hot_port() {
    println!("\n==== #2 验证：256 路全压单端口，pool_size=2 + 弹性扩容 ====");
    let report = run_tcp_scenario(ScenarioParams {
        connections: 256,
        distinct_ports: 1, // 全压一个端口
        payload_len: 1024,
        pool_size: 2,
        timeout: Duration::from_secs(30),
        cpu_burn_per_flush: Duration::ZERO,
    })
    .await;
    print!("单端口 pool=2 弹性 | ");
    report.print_row();
    // 优化前这里 done=2/256 stall；弹性扩容后应接近 N/N。
    assert!(
        report.completed >= 250,
        "弹性扩容后单端口应接近 N/N（completed={}）——#2 硬上限已打掉",
        report.completed
    );
    println!(
        "→ 256 路完成 {}/256：弹性扩容把每端口 pool 硬上限打掉（#2 fixed）",
        report.completed
    );
}

/// 刀12 T4：multi_thread runtime + 注入 on-loop CPU burn → 验证 profiler 的 loop-active/poll 信号
/// 随主循环 CPU 负载上升（仪器正确性 + harness 能造单核饱和信号，为刀13 真分片 gate 铺路）。
///
/// **不**断言 wall/throughput（harness wall 被 generator `sleep(200µs)` 节拍污染，findings 刀1 局限）
/// ——只断段 fraction 的相对趋势。这正是 #4 判决在真出口的核心读法：on-loop CPU↑ → loop-active↑。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loop_profiler_detects_on_loop_cpu_saturation() {
    let base_params = ScenarioParams {
        connections: 32,
        distinct_ports: 8,
        payload_len: 2048,
        pool_size: 8,
        timeout: Duration::from_secs(10),
        cpu_burn_per_flush: Duration::ZERO,
    };
    // 基线：无 burn，主循环处理快 → 多在 select! 空等 → loop-active 低。
    let baseline = run_tcp_scenario(base_params.clone()).await;
    // 加载：每 flush 300µs 合成 on-loop CPU（落在 poll 段）→ loop-active / poll fraction 显著上升。
    let loaded = run_tcp_scenario(ScenarioParams {
        cpu_burn_per_flush: Duration::from_micros(300),
        ..base_params
    })
    .await;

    let base = baseline.loop_profile();
    let load = loaded.loop_profile();
    eprintln!(
        "T4 baseline: loop-active={:.3} poll={:.3} iters={} | loaded: loop-active={:.3} poll={:.3} iters={}",
        base.loop_active_fraction(),
        base.poll_fraction(),
        base.iters,
        load.loop_active_fraction(),
        load.poll_fraction(),
        load.iters,
    );

    // fraction 在合法区间。
    assert!(
        (0.0..=1.0).contains(&base.loop_active_fraction()),
        "base loop-active 越界: {base:?}"
    );
    assert!(
        (0.0..=1.0).contains(&load.loop_active_fraction()),
        "load loop-active 越界: {load:?}"
    );
    // 主循环确有迭代（仪器在真 run_event_loop 里被调用过）。
    assert!(base.iters > 0 && load.iters > 0, "两轮都应有 select! 迭代");

    // 仪器正确性 + harness 能造饱和：注入 on-loop CPU → loop-active 明显上升。
    assert!(
        load.loop_active_fraction() > base.loop_active_fraction() + 0.05,
        "on-loop CPU burn 应抬升 loop-active：base={:.3} load={:.3}",
        base.loop_active_fraction(),
        load.loop_active_fraction()
    );
    // burn 落在 poll 段（flush_tx）→ poll fraction 也随之上升。
    assert!(
        load.poll_fraction() > base.poll_fraction(),
        "burn 在 poll 段应抬升 poll fraction：base={:.3} load={:.3}",
        base.poll_fraction(),
        load.poll_fraction()
    );
}
