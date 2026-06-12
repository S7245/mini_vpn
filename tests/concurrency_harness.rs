//! knife1：大并发压测 harness 整合测试（feature = "harness"）。
//!
//! 跑法：`cargo test --features harness --test concurrency_harness -- --nocapture`
//! 大并发 sweep 标了 `#[ignore]`，显式跑：`... -- --ignored --nocapture`。
//!
//! 全部 smoltcp/device/mock 复杂度封在 `mini_vpn::harness` 内，本文件只编排 + 断言 + 打表。
#![cfg(feature = "harness")]

use mini_vpn::harness::{ScenarioParams, run_tcp_scenario};
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
