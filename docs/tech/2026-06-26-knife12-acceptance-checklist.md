# 刀12 真出口 acceptance 清单 — 多核瓶颈归因（#4 vs #3）

> 配套 spec/plan：同目录 `2026-06-26-knife12-multicore-quantify-{spec,plan}.md`。
> 目标：在 **≥100M 真出口链路**上跑两个 probe，得出「100M 的墙是单核 smoltcp poll（#4）还是
> 单条 QUIC 连接 CC（#3）」的可信裁决 → 落 ADR-0013 → 刀13 据此选连接池/分片。
> **尽力而为、如实记录**：链路 cap 与 #3 缠绕导致模棱时，记「未达可信裁决」+ 补测条件，不强行下结论。

## 0. 仪器就位（已验，无需重测）

- `LoopProfiler`：主循环每报告周期打 `🔬 主循环: loop-active=..% | poll=..% relay=..% | park=..% |
  iters=../wall=..ms`。默认关（`NoopSink` 零开销）；`MINI_VPN_PROFILE_LOOP=1` 开。
- 判决逻辑（spec §3.2）：
  - **loop-active → ~100% 且 poll 占大头** ⇒ 主循环 CPU/卡在 smoltcp ⇒ **#4**（刀13 上分片）。
  - **loop-active 低（多在 park 空等上游）** ⇒ **#3**（刀13 上连接池）。
  - **loop-active 高但 poll 不占大头** ⇒ 某非-poll arm 吃 CPU（仍 #4 家族）→ 细化归因、如实记。

## 1. 准备

```bash
cd <repo>; git checkout claude/knife12-multicore-100m && git pull
cargo build --release
# 凭据（Not in git，向用户要；勿入库）：
export MINI_VPN_TUIC_SERVER=<VPS_IP>:8443 MINI_VPN_TUIC_UUID=<uuid> MINI_VPN_TUIC_PASSWORD=<pass>
export MINI_VPN_TUIC_SNI=example.com MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem MINI_VPN_TUIC_ALPN=h3
# 起客户端：开 profiler + 5s 节拍（秒级看），复用 soak（全局隧道 native+cubic）。
sudo -E MINI_VPN_PROFILE_LOOP=1 MINI_VPN_METRICS_SECS=5 \
  bash scripts/knife35-acceptance.sh soak
```

- 连上应见启动行 `🔬 主循环 profiler 已启用（… 每 5s；MINI_VPN_PROFILE_LOOP）`（**确认透传**——
  教训同刀11：`sudo -E` + 显式 env 才透传；若启动行显示「每 30s」则 env 没透进去，重设）。
- **链路铁律**：出口链路 **≥100M**（否则 100M 处链路 cap 与 #3 缠绕、污染判读）。靠 `-b` 控码率压到 80–100M。
- 受控目标 IP 直连绕 fake-IP：`sudo route -n add -host <target_ip> -interface utunX`。
- 收尾：`sudo -E bash scripts/knife35-acceptance.sh soak-stop`（还原 DNS/路由）。

## 2. Probe ① — poll-fraction 归因（#4 vs #3，主判据）

**做法**：在 ≥100M 链路上推大流量到 80–100M offered，看 `🔬` 行的 loop-active / poll fraction，
同时 OS 层交叉验证 run_event_loop 线程 CPU%。

```bash
# A. 高带宽 TCP 下载（推 client TCP relay 热路径）：
#    受控大文件 / iperf3 TCP 经隧道，offered 推到 ~80-100M。
iperf3 -c <egress-reachable> -p <port> -t 60          # TCP，看隧道吞吐
#    或大文件 bulk download：curl -o /dev/null https://<target>/<bigfile>
# B. 高带宽 UDP（推 native datagram 上/下行）：
iperf3 -c <egress-reachable> -u -b 90M -l 1200 -t 60        # 上行
iperf3 -c <egress-reachable> -u -b 90M -l 1200 -R -t 60     # 下行
# C. 同时另开终端，OS 层抓 run_event_loop 所在线程 CPU%（交叉验证 in-process loop-active）：
top -H -pid $(pgrep -n mini_vpn)      # 看线程级 CPU%
#    或更细：sample $(pgrep -n mini_vpn) 5 -mayDie    # 5s 采样栈，看 CPU 热点在不在 iface.poll
```

**读什么**（每 5s 一条 `🔬`，取负载稳态的几条）：

| 观测 | 裁决 |
|---|---|
| 吞吐封顶（卡某值 < 链路上限），`🔬` **loop-active ≳ 80% 且 poll 占大头**；`top -H` 某线程 ~100% | **#4 坐实**：单核 smoltcp poll 是墙 → 刀13 分片 |
| 吞吐封顶，`🔬` **loop-active 偏低**（如 <40–50%），主循环大量 park；`top -H` 无单线程饱和 | **#3 坐实**：墙在单 QUIC 连接 CC / 路径 → 刀13 连接池 |
| loop-active 高但 poll 不占大头（某非-poll arm 吃 CPU） | #4 家族但点不同 → 记 `iters`/各段占比，细化 |

> ⚠️ R1（spec §5）：多核 runtime 下 run_event_loop task 可在 worker 间迁移，`top -H` 单线程 CPU% 不严格
> 对应该 task——**以 in-process `🔬` loop-active 为主**，`top -H`/`sample` 仅交叉验证 + 看热点是否在 iface.poll。

## 3. Probe ② — 单连接 CC scaling（证实/证伪 #3）

**做法**：固定单条 QUIC 连接，1 vs 2 vs 4 并行 flow，看聚合吞吐是否随 flow 数线性涨。

```bash
# 单连接上跑 N 路并行（TCP 或 native UDP）：
for P in 1 2 4; do
  iperf3 -c <egress-reachable> -u -b 90M -l 1200 -R -P $P -t 30
done
# 同时读 start_udp 的 30s 📊 行（RTT/cwnd/lost/sent）。
```

| 观测 | 裁决 |
|---|---|
| 聚合吞吐随 flow 数**不涨**（卡 ~34–40M，与刀3.5 T-E 一致），cwnd 见单 CC 上限 | **#3 坐实**：单 cubic CC 是墙 → 连接池有杠杆 |
| 聚合**线性涨**（接近 N × 单流） | 墙**不**在单连接 CC → 在 poll(#4) 或 endpoint/链路；连接池无用 |

## 4. 结果记录模板（跑完填这里）

```
日期/机器/链路上限：
启动行（确认 🔬 透传 + 节拍）：
Probe ① 稳态 🔬 行（取 3 条）：
  - offered=__M 实收=__M | loop-active=__% poll=__% park=__% iters=__
  - top -H 线程 CPU%：__
Probe ② 聚合：1 flow=__M / 2 flow=__M / 4 flow=__M | cwnd=__ RTT=__ms
裁决：#4 / #3 / 模棱（补测条件：__）
```

## 5. 裁决 → 刀13 + ADR

- 跑完把数据填入 → 写 `docs/adr/0013-*.md`（裁决 + 刀13 干预建议 + 路线 (a) 三残留单核点记录）+
  findings 末节「刀12」+ HANDOFF「下一刀=刀13」。
- **#4** → 刀13 事件循环分片（先解 spec §5 R5 三残留点：单 fd 读/写、共享 fake_pool）。
- **#3** → 刀13 QUIC 连接池（内聚 TuicUpstream、不碰无锁主循环；先验 endpoint driver 多核性 + sing-box 多连接支持）。
- **模棱** → 记补测条件（更高链路 / 更长 soak / 更细 sample 栈），不强行下结论（诚实优先）。
