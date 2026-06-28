# 刀14b plan — low-RTT fat-path #3 quantify gate

> 配套 spec：`docs/tech/2026-06-28-knife14b-lowrtt-cc-pool-quantify-spec.md`。
> 范围：量化 gate + probe 脚本。**不实现 connection pool**。

## 任务拆分

```text
T0 刀14a 文档纠偏
 ├─ HANDOFF/TODO/ADR0013：刀13 从候选改完成
 └─ 下一步边界：#3 只在 low-RTT fat path 量化

T1 刀14b spec/plan
 ├─ 资格条件：低 RTT、>100M、低丢包、确认真进隧道
 ├─ Probe A：single-conn TCP scaling
 └─ Probe B：UDP native stress

T2 probe 脚本
 ├─ `scripts/knife14b-lowrtt-probe.sh <target> [port]`
 ├─ 记录 `curl ipinfo.io`、fake-IP DNS、`📊/🔬` 尾部日志
 ├─ 跑 `iperf3 -P 1 2 4 8` 正/反向
 └─ 可选 UDP probe（`RUN_UDP=1`）

T3 验证
 ├─ `bash -n scripts/knife14b-lowrtt-probe.sh`
 ├─ 文档 grep 自检：刀13 不再作为候选
 └─ 代码质量门按需；本刀无 Rust 代码变更，不跑 cargo 全套
```

## T2 脚本接口

```bash
scripts/knife14b-lowrtt-probe.sh <iperf-target> [port]
```

环境变量：

| Env | 默认 | 说明 |
|---|---:|---|
| `PARALLEL_SET` | `1 2 4 8` | iperf parallel flow sweep |
| `DURATION` | `30` | 每组秒数 |
| `LOG` | `/tmp/mvpn_accept.log` | mini_vpn soak 日志 |
| `OUT` | `/tmp/mvpn_knife14b_lowrtt_<timestamp>.md` | 输出报告 |
| `RUN_UDP` | `0` | `1` 时额外跑 UDP |
| `UDP_BW` | `90M` | UDP offered bandwidth |
| `UDP_LEN` | `1200` | UDP payload length |

脚本不启动/停止隧道；必须先手动启动：

```bash
cargo build --release
sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 \
  bash scripts/knife35-acceptance.sh soak
```

启动后先确认：

```bash
curl ipinfo.io
dig example.com +short
grep -E '📊 数据面|🔬 主循环' /tmp/mvpn_accept.log | tail
```

## 红线

- 没有符合 spec 的低 RTT 胖链路，不写 connection pool。
- `curl ipinfo.io` 显示本地出口，所有结果作废。
- `📊 TCP relay 累计` 不增长，所有吞吐结果作废。
- 结果必须标注 path、RTT、target、P sweep、`🔬` 稳态行；没有结果就只提交 gate，不提交结论。
