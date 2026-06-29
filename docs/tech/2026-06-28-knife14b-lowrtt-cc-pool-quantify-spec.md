# 刀14b spec — low-RTT fat-path #3 quantify gate

> 日期：2026-06-28 ｜ 分支：`codex/knife14-post13-lowrtt-quantify`
> 配套 plan：`docs/tech/2026-06-28-knife14b-lowrtt-cc-pool-quantify-plan.md`。
> 背景：ADR-0013 已证明当前深圳↔US 路径不是 client poll/CPU 墙，刀13 已修热路径日志和跨流 HoL。
> 本刀只建立 **#3 connection-pool 的量化 gate**，不实现连接池。

## TL;DR

Connection pool 只有在一个干净的低 RTT 胖链路上才值得写。刀14b 的交付是：

- 明确 **什么链路才有资格评估 #3**；
- 给出单 QUIC connection baseline 的 probe；
- 产出可复跑脚本 `scripts/knife14b-lowrtt-probe.sh`；
- 记录判据：什么时候继续做 connection-pool spike，什么时候停止。

## Grill 设计树

```text
要不要写 QUIC connection pool?
├─ 没有低 RTT、端到端 >100M 的可测路径
│  └─ 不写 pool。继续停在量化 gate，避免把 WAN cap 当成 client cap。
├─ 有干净路径，但单 QUIC connection 已能随并行 flow 线性过 100M
│  └─ 不写 pool。#3 不成立，复杂度无收益。
├─ 有干净路径，single-conn 聚合卡在 <路径上限>，loop/poll 不 CPU-bound
│  └─ 进入下一刀：connection-pool spike，做 A/B。
└─ loop-active 高且 poll 或 on-loop CPU 成本占主导
   └─ 回到 #4 家族，不写 pool；先定位新的 loop CPU 点。
```

## 合格链路前置条件

必须同时满足：

1. **低 RTT**：client 到 Upstream / iperf target 的稳态 RTT 建议 `<30ms`；`<50ms` 可记录但判读保守。
2. **胖链路**：裸路径或隧道外受控路径可稳定提供 `>=150M`，避免在 100M 附近和链路 cap 缠绕。
3. **低丢包**：iperf/curl 无明显重传风暴；否则优先判为 path wall。
4. **确认真进隧道**：`curl ipinfo.io` 必须显示 exit IP；`dig example.com +short` 应返回 `198.18.x.x` fake-IP。
5. **LoopProfiler 开启**：`MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5`，取稳态 `🔬`，忽略启动第一条。

不满足这些条件时，结果只能记录为 **inconclusive**，不能驱动连接池实现。

## Probe

### Probe A — single-connection scaling baseline

先在出口 / sing-box 服务器上跑 direct baseline，证明 exit→iperf target 裸路径本身足够胖：

```bash
scripts/knife14b-direct-baseline.sh <TARGET> <PORT>
```

如果 direct baseline 自身也低于 `150M` 或重传高，本刀结论是 path wall，不能驱动 connection pool。

在现有单 TUIC/QUIC connection 上跑 TCP 或 UDP 并行 flow：

```bash
for P in 1 2 4 8; do
  iperf3 -c <TARGET> -p <PORT> -t 30 -P "$P"
  iperf3 -c <TARGET> -p <PORT> -t 30 -P "$P" -R
done
```

读取：

- iperf aggregate throughput、retransmits/loss；
- `📊 数据面` 的 `TCP relay 累计`、UDP drops/backpressure；
- `🔬 主循环` 的 `loop-active/poll/relay/park`；
- OS sample（macOS `sample $(pgrep -n mini_vpn) 10 -mayDie`）确认 CPU 热点。

### Probe B — UDP native stress

若目标是直播/UDP：

```bash
for P in 1 2 4; do
  iperf3 -c <TARGET> -u -b 90M -l 1200 -t 30 -P "$P"
  iperf3 -c <TARGET> -u -b 90M -l 1200 -t 30 -P "$P" -R
done
```

读取 `cwnd/RTT/lost/sent` 和 datagram pressure。若 native/cubic 已稳定过 100M，pool 无必要。

## 判据

| 观测 | 决策 |
|---|---|
| P=1→2→4→8 聚合近似线性，超过 100M，`poll` 低 | 不写 connection pool；single conn 足够 |
| 聚合早早卡住，明显低于链路上限；`poll` 低，OS sample 低 CPU/parked | #3 可疑，下一刀做 connection-pool spike |
| 聚合卡住但裸路径/反向路径也卡，或丢包/重传高 | path wall；不写 pool |
| `poll` 或某个 loop arm CPU 占大头 | #4 家族；不写 pool，先定位 loop CPU |

## 产物

- `scripts/knife14b-lowrtt-probe.sh`：收集 tunnel gold checks、`📊/🔬` 日志和 iperf P sweep。
- `scripts/knife14b-direct-baseline.sh`：在出口 / sing-box 服务器上收集 direct path baseline，验证裸路径是否 `>=150M`。
- 本 spec + plan。
- 真跑时把结果另存为 `docs/tech/YYYY-MM-DD-knife14b-lowrtt-results.md`（不要伪造；无环境则不创建结果文档）。
