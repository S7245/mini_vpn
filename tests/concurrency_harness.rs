//! knife1：大并发压测 harness 整合测试（feature = "harness"）。
//!
//! 跑法：`cargo test --features harness --test concurrency_harness -- --nocapture`
//! 大并发 sweep 标了 `#[ignore]`，显式跑：`... -- --ignored --nocapture`。
//!
//! 全部 smoltcp/device/mock 复杂度封在 `mini_vpn::harness` 内，本文件只编排 + 断言 + 打表。
#![cfg(feature = "harness")]

use mini_vpn::harness::{ScenarioParams, run_tcp_scenario, run_udp_echo_scenario};
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
    })
    .await;
    report.print_row();
    assert_eq!(report.completed, 64, "64 路应全部完成（N/N）");
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
        })
        .await;
        report.print_row();
        assert_eq!(report.completed, n, "N={n} 应全部完成");
    }
    println!("==== 端到端 #3（单条 QUIC 连接）见 spec：deferred，需真 sing-box probe ====\n");
}
