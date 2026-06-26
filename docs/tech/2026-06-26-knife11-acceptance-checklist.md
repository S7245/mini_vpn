# 刀11 数据面可观测性 — 真出口 acceptance 测试单

> 分支 `claude/knife11-observability`。配套：spec/plan `2026-06-26-knife11-observability-{spec,plan}.md`、ADR-0012、
> findings 末节「刀11」。在**真机（深圳测试机）+ 真 sing-box VPS** 上跑，需 `MINI_VPN_TUIC_*`（+ failover 需 `MINI_VPN_REALITY_*`）凭据。
> 目标：证 `📊 数据面` 周期快照各指标**随对应负载变化**（量化底座可信）。尽力而为如实记录。

## ⚠️ 第一轮复盘（2026-06-26，两条方法教训，非程序 bug）

1. **`📊` 是周期快照（默认 30s）**：首轮 18 次 DNS forge / 35 次 relay **全发生在最后一条快照之后**、soak 就停了 → 全 0 是
   **采样时机**，非计数错（源码已证：`metrics_handle` 在 run_event_loop 内单一 Arc，写=读，累计计数器写 N 次必读 N；harness 端到端测试已证）。
   **→ 已加 env `MINI_VPN_METRICS_SECS`**（默认 30，acceptance 设 `=5` 秒级看；0/非法回落 30 防 `tokio::interval` panic）。
2. **app 缓存上轮 fake-IP**：client 重启后池空 → app 狂连旧 fake-IP 被 `🚫 无映射` 拒（不 forge、不 relay、不进池）→ soak 前**必须清 DNS 缓存**。

**两条铁律**：① 真流量后**至少多等 1 个周期（5s+）**再看 `📊`；② 看 `tail` 的**最后一条** `📊`，不是第一条。

---

## 0. 前置准备（每次必做）

```bash
# ① 必须重新 build——env 旋钮是新 commit，且保证 binary == 分支 HEAD（消除旧 binary 疑虑）
cd <repo>/mini_vpn && git checkout claude/knife11-observability && git pull
cargo build --release          # 产出 target/release/mini_vpn（脚本就用它）

# ② 设快照周期=5s（关键！默认 30s 对短 soak 太粗）
export MINI_VPN_METRICS_SECS=5

# ③ 你原有凭据照常 export（脚本要）：MINI_VPN_TUIC_* / MINI_VPN_REALITY_*
# ④ 清 DNS 缓存（macOS）
sudo dscacheutil -flushcache; sudo killall -HUP mDNSResponder
```

启动后应见（确认旋钮生效）：`📊 数据面可观测性：快照周期 = 5s（MINI_VPN_METRICS_SECS 可调）`

`📊` 行格式：
```
📊 数据面: DNS forge=<n>/drop=<n> | TCP relay 活跃=<g>/累计=<n> | fake-IP 活跃=<g>/在册=<g> | UDP↓丢=<n> 背压=<n> | UDP↑丢=<n> stream兜底=<n> | leg=<TUIC|REALITY|->
```
日志路径：knife35 → `/tmp/mvpn_accept.log`；knife9 → `/tmp/mvpn_failover_accept.log`。

---

## A. 纯 TUIC — DNS forge / TCP relay / fake-IP gauge

```bash
sudo -E bash scripts/knife35-acceptance.sh soak     # 起 TUIC client-tun + 路由进 TUN
# 浏览器开几个【没访问过的新站】/ 或：
dig @8.8.8.8 example.org;  dig @8.8.8.8 wikipedia.org
for d in example.org wikipedia.org archlinux.org; do curl -sI https://$d -o /dev/null & done; wait
sleep 8                                              # 等 ≥1 个 5s 周期
grep '📊 数据面' /tmp/mvpn_accept.log | tail -3       # 看最后几条
sudo -E bash scripts/knife35-acceptance.sh soak-stop
```
| 指标 | 预期 | 通过判据 |
|---|---|---|
| `DNS forge` | 随新域名查询↑ | **> 0** 且 ≥ 唯一新域名数 |
| `TCP relay 累计` | 随连接↑ | **> 0** |
| `TCP relay 活跃` | 并发时刻 >0 | 负载瞬间快照 **>0**（空闲回 0 正常） |
| `fake-IP 在册` | 随域名↑ | **> 0** |
| `leg` | 纯 TUIC 无 failover | **`-`**（NO_FAILOVER） |

---

## B. failover — leg 翻转（TUIC ↔ REALITY）

```bash
sudo -E bash scripts/knife9-failover-acceptance.sh soak          # 两腿，TUIC 当班
grep '📊 数据面' /tmp/mvpn_failover_accept.log | tail -1          # 应 leg=TUIC

sudo -E bash scripts/knife9-failover-acceptance.sh cut-tuic      # 封 TUIC
curl -sI https://example.org -o /dev/null; sleep 12              # 切后访问新站让 REALITY 腿被真连接走过
grep -E '🔀|🔐|📊 数据面' /tmp/mvpn_failover_accept.log | tail -6  # 切换+握手+leg=REALITY

sudo -E bash scripts/knife9-failover-acceptance.sh restore-tuic  # 恢复
sleep 70                                                          # 等冷却(60s)+周期
grep '📊 数据面' /tmp/mvpn_failover_accept.log | tail -1          # 应 leg=TUIC
sudo -E bash scripts/knife9-failover-acceptance.sh soak-stop
```
| 阶段 | 预期 `leg=` | 旁证 |
|---|---|---|
| 起始 | `TUIC` | — |
| cut-tuic 后 | **`REALITY`** | `🔀 切到 REALITY` + `🔐 REALITY 握手成功` |
| restore 冷却后 | **`TUIC`** | `🔀 切回 TUIC` |

> 首轮栽点：切换在最后快照之后、且切后全是旧 fake-IP 拒连。这次切后务必**访问新站 + 等 ≥12s**。

---

## C. UDP 下行 drop / 背压（尽力而为，可能恒 0）

```bash
sudo -E bash scripts/knife35-acceptance.sh soak
# 大码率 UDP：YouTube 4K 播放，或 iperf3 -u -b 50M 经隧道
sleep 10
grep '📊 数据面' /tmp/mvpn_accept.log | tail -3      # 看 UDP↓丢 / 背压
sudo -E bash scripts/knife35-acceptance.sh soak-stop
```
| 指标 | 预期 | 说明 |
|---|---|---|
| `UDP↓丢` / `背压` | 高保真链路常 **0** | 刀3.5 已证 native+cubic datagram 够用；触发则**如实记录**（不算失败） |
| `UDP↑丢` / `stream兜底` | 视链路 | 上行既有计数，顺带核对 |

---

## D. 回归（既有功能不破，必过）

| 检查 | 命令 | 通过判据 |
|---|---|---|
| HTTPS 端到端 | soak 期间 `curl -sI https://example.org` | `HTTP/2 200`，出口 IP=VPS |
| DNS 劫持仍正常 | `dig @8.8.8.8 <新域名>` | 返回 `198.18.x.x` fake-IP |
| 无 panic/崩溃 | `grep -iE 'panic|RUST_BACKTRACE' /tmp/mvpn_*.log` | **无命中** |

---

## 结果记录表

| 项 | 关键指标 | 实测值 | 判定 |
|---|---|---|---|
| A DNS forge | `DNS forge=` | ___ | ☐通过 ☐失败 |
| A TCP relay | `relay 累计=` | ___ | ☐通过 ☐失败 |
| A fake-IP | `fake-IP 在册=` | ___ | ☐通过 ☐失败 |
| B leg→REALITY | cut 后 `leg=` | ___ | ☐通过 ☐失败 |
| B leg→TUIC | restore 后 `leg=` | ___ | ☐通过 ☐失败 |
| C UDP↓/背压 | `UDP↓丢=/背压=` | ___ | ☐如实记录 |
| D 回归 | curl 200 / 无 panic | ___ | ☐通过 ☐失败 |

**总判据**：A 三项 + B 两项全 >0 / 翻转正确，D 全过 → **刀11 acceptance ✅**，量化底座可信，可合 main。C 尽力而为。
