# 刀10 — F5 TLS 1.3 KeyUpdate 密钥轮换 spec/plan

> 日期：2026-06-25 ｜ 分支：`claude/knife10-keyupdate`（从 main `7507434` 起）
> 范围：把 `RealityStream` 收到 post-handshake TLS 1.3 KeyUpdate 从 **loud-fail** 改为**正确轮换**。
> 来源：`docs/tech/2026-06-24-knife9-research-brief.md §6`（V1 字节级对抗核验，四点全真）。**本 spec 不重新研究，照 §6 落地**。
> 原则：系统稳定 > 代码漂亮（trade-off 冲突一律选稳）。

---

## 1. 背景与边界

- 刀6→9 完成 REALITY mini-project；KeyUpdate 是刀9 拆出的最后一个 deferred 项（ADR-0010 登记），**与 failover 主链零耦合**，只碰 `src/reality*` 模块。
- 现状：`reality_upstream.rs:117-126` 对内层 `0x16` 且 `body[0]==0x18`（KeyUpdate）**loud-fail**（M1：避免后续 record 静默 bad-decrypt 误判随机断连）。
- 本刀把 loud-fail 换成 RFC 8446 §4.6.3/§7.2/§5.3 的轮换。
- **不在范围**：服务端主动发 KeyUpdate（我们是客户端，只需正确响应）；UDP-over-VLESS；连接复用。

---

## 2. 精确算法（brief §6.4，V1 VERIFIED，逐字照实现）

触发点：`decode_one` 解出一条 record，内层 `content_type==0x16(handshake)` 且 `content[0]==0x18(key_update)` → 调 `on_key_update(&content)`。

```text
fn on_key_update(msg):                         # msg = 解密后 handshake message = [0x18, len_u24, request_update]
    # 帧校验（先于任何 mutation）：
    if msg.len() < 5 or msg[1..4] != [0x00,0x00,0x01]: Err(decode)        # KeyUpdate body 长度必为 1
    request_update = msg[4]
    if request_update > 1: Err(illegal_parameter)                         # 非 0/1 → 终止（无 mutation）

    # 步骤 A：总是先轮换“接收”方向（对端已轮它的发送密钥）  §7.2 / §5.3
    server_ap_secret = HKDF-Expand-Label(server_ap_secret, "traffic upd", "", 32)
    recv_keys = RecordKeys(key=ExpandLabel(server_ap_secret,"key","",16),
                           iv =ExpandLabel(server_ap_secret,"iv","",12))   # seq 自动归 0

    # 步骤 B：仅 update_requested(1) 才回发 + 轮“发送”方向  §4.6.3
    if request_update == 1:
        # B1: 必须先用「旧」send key 封装回发 KeyUpdate(update_not_requested=0)
        reply = send_keys.seal(content_type=0x16, body=[0x18,0x00,0x00,0x01,0x00])  # 旧 key, 旧 seq
        write_pending += reply
        # B2: 封装完毕「才」轮发送方向（铁律 B1 必先于 B2）
        client_ap_secret = HKDF-Expand-Label(client_ap_secret, "traffic upd", "", 32)
        send_keys = RecordKeys(key=ExpandLabel(client_ap_secret,"key","",16),
                               iv =ExpandLabel(client_ap_secret,"iv","",12))        # seq 归 0
    # request_update == 0: 只轮接收，不回发、不动发送
```

**四条铁律（V1 核验，不可改）：**
1. label = `"traffic upd"`（11 字节 ASCII，`upd` 非 `update`，中间一个空格 → 本仓 `expand_label` 包成 `"tls13 traffic upd"`）；context = 空；len = 32。
2. 换密钥后 record seq 归 0 → `RecordKeys::new` 天然满足，**record.rs 结构不改**。
3. 收 `update_requested(1)`：**B1（旧 send key 封 reply）必先于 B2（轮 send 密钥）**。先换再封 = 对端用旧 key 解新 key 的 record → 解密失败掉线。
4. 收 `update_not_requested(0)`：只轮接收，不回发、不动发送。防环：回发的 request_update 恒为 0。

---

## 3. 对 brief §6.4 的两处实现裁决（保留全部 V1 行为，仅更稳/更可测）

| 裁决 | brief 原文 | 本实现 | 理由 |
|---|---|---|---|
| **非法 request_update 校验时机** | 伪代码先轮 recv（步骤 A）再 `_ => fatal_alert` | **前置校验** `request_update ∈ {0,1}`，非法 → Err 且**零 mutation** | 非法值本就 fatal 终止连接，前置校验语义等价但状态干净、便于「非法→不改状态」断言。四条铁律未触及。 |
| **回发 reply 的写出** | 「回发要能写底层（RealityStream 有 write_half）」 | `on_key_update` 只把 reply 入 `write_pending`；`poll_read` 顶部**机会性 best-effort flush**（`write_pending` 非空时 `let _ = poll_flush_pending(cx)?`），并给 `AsyncRead` impl 补 `W: AsyncWrite + Unpin` bound | reply 是 ~26B 控制帧，几乎总能即时 flush；纯下载（消费者只读不写）时不会卡在 `write_pending`。FIFO + seq 单调 ⇒ reply 必在后续新 key app data 之前出，铁律 3 在 wire 层也成立。Pending 不阻塞读（best-effort），TCP 写满时 reply 留 `write_pending` 待下次 poll 重试。 |

> `AsyncRead` impl 加 `W: AsyncWrite` bound 安全：prod=`OwnedWriteHalf`、test=`WriteHalf<DuplexStream>` 都 impl `AsyncWrite`，无非可写 W 的用例。

---

## 4. 改点清单（文件:符号）

1. **`src/reality/key_schedule.rs`** — 加纯函数 `next_application_traffic_secret(&[u8;32]) -> [u8;32]`（= `expand_label(s,"traffic upd",b"",32)`）。带 KAT。
2. **`src/reality/handshake.rs`** — `HandshakeOutput` 加 `s_ap_secret: [u8;32]`、`c_ap_secret: [u8;32]`；`drive` 末尾从 `app.{s,c}_ap_secret` 透出。
3. **`src/reality_upstream.rs`**：
   - `RealityStream` 加 `server_ap_secret`、`client_ap_secret` 两字段；`new` 接收（追加在 `leftover` 之后）。
   - 加私有 `fn record_keys_from_secret(&[u8;32]) -> RecordKeys`（ExpandLabel key16/iv12 + 新建）+ `fn on_key_update(&mut self, &[u8]) -> io::Result<()>`。
   - `decode_one` 0x16 分支：`content[0]==0x18` → `self.on_key_update(&content)?`（替代 loud-fail），仍返回 `Decoded::Drop`（KeyUpdate 不上抛 app data）。
   - `poll_read`：impl bound 加 `W: AsyncWrite + Unpin`，循环顶部机会性 flush `write_pending`。
   - `open_tcp`（:428）+ 各测试构造点补两 secret 实参（非 KeyUpdate 测试传 `[0u8;32]` dummy）。
   - **record.rs 不改**（铁律 2）。

---

## 5. TDD 任务（brief §8 T16-T20）

- **T16 [离线 KAT]** `key_schedule`：
  - `hkdf_label(32,"traffic upd",b"")` == 手算字节 `0020 11 "tls13 traffic upd" 00`（钉死 label 字符串，#1 静默互通杀手）。
  - `next_application_traffic_secret` 链式派生（N→N+1）+ frozen 回归值；新 key/iv round-trip（seal/open）证 seq 归 0。
- **T17 [离线 KAT]** 时序铁律：`on_key_update(req=1)` 后 → `write_pending` 的 reply 用**旧 send key（旧 seq）**可解出 `[0x18,0,0,1,0]`（crypto evidence 证 B1 先于 B2：若先轮 send，旧 key 解必败）；`send_keys` 已轮到 N+1 seq0。
- **T18 [离线 KAT]** `on_key_update(req=0)` 只轮 recv、`write_pending` 空、send 不变；非法 `request_update`(如 2) → Err 且 `server_ap_secret`/recv 不变。
- **T19 [loopback]** 扩 server-sim：握手后发 `KeyUpdate(update_requested=1)` + 新 key app data；验 `RealityStream` 轮 recv 解新 key 数据 + 回发 reply（server 用旧 c_ap 解）+ 轮 send 后 `up` 数据 server 用新 c_ap 解。**替换** `realitystream_keyupdate_loud_fails`。
- **T20 [真出口 acceptance]** KeyUpdate 难诱发（server 极少主动发）；以 T16-T19 为主，acceptance 尽力而为并如实记录于 HANDOFF/commit。

## 6. 质量门

lib+harness 测全绿 ｜ `clippy --all-targets --features harness` 0 warning ｜ `cargo build --release` 绿 ｜ 每任务 红→绿→commit→push ｜ 完成 `/code-review` + 对抗式核验。
