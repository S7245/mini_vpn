#!/usr/bin/env bash
#    ↑ 用 env 查 bash 路径，比硬编码 /bin/bash 更可移植（macOS 默认 bash 较老，
#    用户可能通过 Homebrew 安装新版 bash 并在 PATH 中前置）。
#
# 刀3.5 真出口 acceptance helper (macOS). NOT a product file — dev/test only.
# 读 env 的 MINI_VPN_TUIC_*（含 UUID/PASSWORD）——脚本本身不含任何凭据。
#
# 用法（凭据先 export，见下）：
#   构建：      cargo build --release            # (在 repo 根，无需 sudo)
#   iperf3 配置：sudo -E bash scripts/knife35-acceptance.sh start <cc> <mode>   # cc=bbr|cubic mode=native|quic
#   全局 soak： sudo -E bash scripts/knife35-acceptance.sh soak [cc]            # 默认 cubic；DNS=198.18.0.1+/15 路由
#   刀5 soak： sudo -E bash scripts/knife35-acceptance.sh soak-knife5 [cc]      # DNS=8.8.8.8(非我方 resolver)+路由进 TUN
#   停 iperf3： sudo -E bash scripts/knife35-acceptance.sh stop
#   停 soak：   sudo -E bash scripts/knife35-acceptance.sh soak-stop            # 自动还原 DNS（soak / soak-knife5 通用）
#
# 凭据 export（向项目负责人要，勿入库）：
#   export MINI_VPN_TUIC_SERVER=47.251.188.205:8443
#   export MINI_VPN_TUIC_UUID=<uuid>
#   export MINI_VPN_TUIC_PASSWORD=<pass>
#   （SNI/ALPN/CA 有默认：example.com / h3 / certs/dev/ca-cert.pem）
#
# 可覆盖 env：IPERF_TARGET（默认 43.110.37.170）、NETSVC（默认自动探测默认路由所在网络服务）。

# ========== 严格模式 ==========
# set -u：引用未定义变量时立即报错退出，防止拼写错误导致的静默 bug。
#         例如 $TYPO_VAR 会直接报 "unbound variable" 而不是偷偷用空字符串。
set -u

# ========== 路径推导：可靠定位脚本和项目根目录 ==========

# 为什么用 BASH_SOURCE[0] 而不是 $0？
#   当脚本被 source 加载时，$0 是调用方 shell 的名字（如 -bash），
#   BASH_SOURCE[0] 始终是当前脚本自身的路径。
#
# cd + pwd 组合：将相对路径/符号链接转为绝对路径。
#   dirname 取目录 → cd 进入 → pwd 输出绝对路径 → 用 $(...) 捕获。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 脚本在 scripts/ 子目录下，取父目录即 repo 根。
REPO="$(cd "$SCRIPT_DIR/.." && pwd)"
BIN="$REPO/target/release/mini_vpn"       # 要启动的 VPN 客户端二进制

# ========== 配置区 ==========
# 所有可覆盖配置都用 ${VAR:-默认值} 语法：
#   - 环境变量已设置 → 用环境变量的值
#   - 环境变量未设置或为空 → 用冒号后面的默认值
# 这样用户零配置也能直接跑，同时可以灵活覆盖。

LOG=/tmp/mvpn_accept.log                  # VPN 客户端日志（用于连接状态检测）

# iperf3 测试目标 IP：可设 IPERF_TARGET 环境变量覆盖
TARGET="${IPERF_TARGET:-43.110.37.170}"

# soak 模式用的假 DNS 和假网段：
#   198.18.0.0/15 是 RFC 3330 规定的基准测试保留地址段（Benchmarking），
#   不会出现在公网，VPN TUN 接口用它生成 fake IP 做 DNS 劫持。
FAKE_DNS=198.18.0.1                        # 假 DNS 服务器地址
FAKE_NET=198.18.0.0/15                     # 将整个假网段路由进 TUN

# 刀5 acceptance 专用：
#   系统 DNS 不指向我方 resolver（198.18.0.1），而是指向 8.8.8.8，
#   验证「任意 :53 流量仍被本地劫持伪造」——这意味着 TUN 层做了 DNAT 劫持，
#   不依赖系统 DNS 配置。可用 K5_RES 环境变量覆盖为其他外部 DNS。
K5_RES="${K5_RES:-8.8.8.8}"

# DNS 备份文件：soak 模式修改系统 DNS 之前，先保存原始值到此文件。
#   固定路径而非临时文件——脚本重启后仍可追溯上次设了什么 DNS。
#   便于排查问题和手动还原。
DNS_SAVE=/tmp/mvpn_prior_dns.txt

# ${1:-}：取第一个命令行参数。如果没传 → 返回空字符串（而不是触发 set -u 报错）。
#   不加冒号的 ${1-} 只兜底「未设置」而不兜底「空字符串」，这里用 :- 更安全。
ACTION="${1:-}"

# ========== 辅助函数 ==========

# detect_netsvc — 探测当前活跃网络服务的"人类可读名称"
#   返回值如 "Wi-Fi"、"Ethernet"、"USB 10/100/1000 LAN" 等。
#   这个名称是 networksetup 命令使用的标识符（不是 en0 这种 BSD 设备名）。
#
#   优先级：NETSVC 环境变量 > 自动探测（默认路由设备 → 服务名映射）
detect_netsvc() {
  # 用户显式指定 → 直接返回，跳过探测
  if [ -n "${NETSVC:-}" ]; then echo "$NETSVC"; return; fi

  # 1) 拿到默认路由的出接口设备名（如 en0、en5）
  #    route -n get default：查默认路由表项
  #    awk '/interface:/{print $2}'：提取 "interface: en0" 中冒号后的设备名
  #    2>/dev/null：抑制权限不足等场景的错误输出
  local dev
  dev="$(route -n get default 2>/dev/null | awk '/interface:/{print $2}')"

  # 2) 拿不到设备名（极罕见，如无默认路由）→ 回退为 "Wi-Fi"
  [ -z "$dev" ] && { echo "Wi-Fi"; return; }

  # 3) 把设备名映射为 networksetup 使用的服务名
  #    networksetup -listnetworkserviceorder 输出格式：
  #      (1) Wi-Fi
  #      (Hardware Port: Wi-Fi, Device: en0)
  #
  #    awk 逻辑：
  #      模式1 — /^\([0-9*]+\)/ 匹配 "(1) Wi-Fi" 这样的行：
  #              用 sub() 去掉前缀 "(1) "，提取纯服务名存到 name 变量
  #      模式2 — $0 ~ ("Device: " d ")") 匹配 "Device: en0" 这样的行：
  #              说明上一行存的 name 就是我们要的服务名，print 并退出
  #    -v d="$dev"：把 shell 变量 $dev 安全地传给 awk（比字符串插值安全得多）
  networksetup -listnetworkserviceorder 2>/dev/null \
    | awk -v d="$dev" '
        /^\([0-9*]+\)/ { name=$0; sub(/^\([0-9*]+\) /,"",name) }
        $0 ~ ("Device: " d ")") { print name; exit }
      '
}

# stop — 停止 start 模式（iperf3 单项测试）
#   只杀 VPN 进程 + 删目标主机路由。不涉及 DNS（start 模式不改系统 DNS）。
stop() {
  # pkill -f：按完整命令行匹配（不只是进程名），避免误杀同名进程
  pkill -f "mini_vpn client-tun" 2>/dev/null

  # route -n delete：删除路由表项。-n 跳过 DNS 反向查询，更快
  route -n delete -host "$TARGET" >/dev/null 2>&1

  sleep 1
  echo "stopped + route to $TARGET removed"
}

# soak_stop — 停止 soak / soak-knife5 模式
#   职责比 stop 重得多：还原 DNS、清理多条路由。
#   这是 soak 模式的"撤销键"——soak 改了什么，soak-stop 就还原什么。
soak_stop() {
  local svc
  svc="$(detect_netsvc)"

  # 读取之前保存的 DNS 配置（soak 模式启动时写入 DNS_SAVE）
  local prior
  prior="$(cat "$DNS_SAVE" 2>/dev/null)"

  # DNS 还原的特殊情况处理：
  #   1) prior 为空 → 原来就没有 DNS 服务器 → 设为 "empty"（回退到 DHCP）
  #   2) prior 是 "There aren't any DNS Servers set on X." → 同上
  #      （macOS networksetup 在无 DNS 时的返回文本）
  #   用 printf '%s' 而不是 echo，防止 prior 中包含 -n 等特殊字符串。
  if [ -z "$prior" ] || printf '%s' "$prior" | grep -qi "aren't any"; then
    prior="empty"
  fi

  # 还原 DNS：注意 $prior 故意不加引号！
  #   networksetup -setdnsservers 接受空格分隔的多个 DNS 地址：
  #     setdnsservers Wi-Fi 8.8.8.8 1.1.1.1
  #   如果加引号 "$prior"，"8.8.8.8 1.1.1.1" 会被当作一个参数。
  #   这是 shell 中极少数「刻意不引号」的场景。
  networksetup -setdnsservers "$svc" $prior \
    && echo "DNS($svc) restored: $prior"

  # 停止 VPN 进程
  pkill -f "mini_vpn client-tun" 2>/dev/null

  # 删除所有可能被添加的路由（幂等操作：不存在也不报错，因为 stderr 被丢弃）
  route -n delete -net "$FAKE_NET" >/dev/null 2>&1    # soak 模式添加的 /15 网段
  route -n delete -host "$TARGET"  >/dev/null 2>&1    # start 模式可能的主机路由
  route -n delete -host "$K5_RES"  >/dev/null 2>&1    # 刀5 模式添加的 alt-resolver 路由

  sleep 1
  echo "soak stopped + /15 route removed + DNS restored"
}

# ========== 命令路由：case 分支分发 action ==========

# 提前处理「停止」类 action（匹配后直接 exit 0，不进入后续启动逻辑）。
# 用 | 合并多个 pattern：start、soak、soak-knife5 三个 action 共享后续流程，
# 空分支 ;; 即 fall-through（什么都不做，继续往下执行）。
#
# 退出码约定：
#   0 — 正常（stop/soak-stop）
#   1 — 运行时错误（二进制缺失、连接超时、找不到 utun）
#   2 — 用法错误（非法参数）
case "$ACTION" in
  stop)      stop; exit 0 ;;
  soak-stop) soak_stop; exit 0 ;;
  start|soak|soak-knife5) ;;                      # fall-through：继续执行后续启动流程
  *) echo "usage: $0 {start <cc> <mode> | soak [cc] | soak-knife5 [cc] | stop | soak-stop}"; exit 2 ;;
esac

# 根据 action 确定拥塞控制算法（CC）和 UDP 模式（MODE）的默认值
#   soak / soak-knife5：默认 CC=cubic，MODE 固定为 native（不需要第三个参数）
#   start（iperf3 单项）：默认 CC=bbr，MODE 可选 native|quic
#   变量名必须加引号 "$ACTION"——不加的话，$ACTION 为空时变成 [ = "soak" ]，语法错误
if [ "$ACTION" = "soak" ] || [ "$ACTION" = "soak-knife5" ]; then
  CC="${2:-cubic}"; MODE="native"
else
  CC="${2:-bbr}"; MODE="${3:-native}"
fi

# ========== 预检查 ==========
# -x：检查文件存在且可执行。比 [ -f "$BIN" ] 更精确。
[ -x "$BIN" ] || { echo "!! 未找到 release binary: $BIN（先 'cargo build --release'）"; exit 1; }

# ========== 阶段 1：清理旧状态 ==========
# 杀掉可能残留的旧 VPN 进程 + 删除旧路由（幂等操作，忽略不存在时的错误）
pkill -f "mini_vpn client-tun" 2>/dev/null
route -n delete -host "$TARGET" >/dev/null 2>&1
sleep 1

# ========== 阶段 2：记录起前 utun 接口集合 ==========
# 目的：启动 VPN 后通过差集找出新创建的 utun 接口。
#
#   管道拆解：
#     ifconfig -l              → 输出所有接口名，空格分隔："lo0 gif0 stf0 en0 utun0 utun1"
#     tr ' ' '\n'              → 空格替换为换行，每行一个接口名
#     grep '^utun'             → 只保留 utun 开头的接口（VPN tunnel 接口）
#     sort                     → 排序，comm 命令要求输入有序
BEFORE="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"

# ========== 阶段 3：启动 VPN 客户端 ==========

# : > "$LOG" — 清空（truncate）日志文件
#   : 是 bash 内建的空命令（no-op），> 做输出重定向。
#   效果：把 LOG 文件截断为 0 字节但不改变 inode。
#   比 rm + touch 好在——如果有其他进程正 tail -f 这个文件，不会断。
: > "$LOG"

# 切换到 repo 根目录（二进制可能依赖相对路径的配置文件）
# cd 后必须检查 || exit 1，否则后续操作都在错误的目录执行
cd "$REPO" || { echo "cd $REPO failed"; exit 1; }

# 用行末 \ 续行：在命令行上一次性设置多个环境变量，只为子进程生效。
#   注意：\ 必须是该行最后一个字符——\ 后面不能有空格！
#   VAR1=v1 VAR2=v2 command → VAR1 VAR2 只对 command 进程可见，当前 shell 不保留。
#
# nohup：让进程忽略 SIGHUP 信号，即使终端关闭也不会被杀。
# >>"$LOG" 2>&1：stdout 追加到日志，stderr 合并到 stdout（注意顺序：先 >> 再 2>&1）。
# &：放入后台运行，脚本继续往下走。
# $!：最后一个后台进程的 PID，在此输出给用户参考。
MINI_VPN_TUIC_SERVER="${MINI_VPN_TUIC_SERVER:-47.251.188.205:8443}" \
MINI_VPN_TUIC_SNI="${MINI_VPN_TUIC_SNI:-example.com}" \
MINI_VPN_TUIC_CA_PATH="${MINI_VPN_TUIC_CA_PATH:-$REPO/certs/dev/ca-cert.pem}" \
MINI_VPN_TUIC_ALPN="${MINI_VPN_TUIC_ALPN:-h3}" \
MINI_VPN_TUIC_CC="$CC" MINI_VPN_TUIC_UDP_MODE="$MODE" \
nohup "$BIN" client-tun >>"$LOG" 2>&1 &
echo "client-tun started: cc=$CC mode=$MODE pid=$!"

# ========== 阶段 4：轮询等待连接就绪 ==========
# 不假设连接是瞬间的——通过日志关键词轮询确认，最多等 30 秒。
#
#   seq 1 30：生成 1~30 的整数序列
#   _ 作为循环变量：约定俗成"我不需要这个值"
#   grep -q：静默模式，找到匹配返回 0，没找到返回 1（不输出文本）
#   && break：grep 成功 → 跳出循环（连接就绪）
#   sleep 1：每次检查间隔 1 秒
for _ in $(seq 1 30); do grep -q "TUIC datagram" "$LOG" && break; sleep 1; done

# 循环结束后再次检查：如果仍然没找到 → 超时，输出诊断信息并退出
# tail -15：日志最后 15 行，给用户诊断线索
if ! grep -q "TUIC datagram" "$LOG"; then
  echo "!! not connected in 30s; last 15 log lines:"; tail -15 "$LOG"; exit 1
fi

# ========== 阶段 5：通过差集定位新建的 utun 接口 ==========
#
# 原理：启动前后分别记录 utun 列表 → 用 comm 做集合差运算（AFTER - BEFORE）
#   得到的就是 VPN 客户端创建的隧道接口。
#
#   comm — 比较两个已排序的文件/流，输出三列：
#     第 1 列: 只在文件 A 中的行
#     第 2 列: 只在文件 B 中的行
#     第 3 列: 两文件共有的行
#   -13：抑制第 1、3 列 → 只输出"AFTER 中有但 BEFORE 中没有" = 新建的 utun
#
#   <(echo "$BEFORE") — 进程替换（Process Substitution）
#     把 echo 的输出变成一个虚拟文件路径（/dev/fd/N），供 comm 读取。
#     之所以不用管道：comm 需要两个独立的输入流，管道只能提供一个。
#
#   head -1：多项取第一个（正常情况下只有一个新建 utun，这是防御性做法）
AFTER="$(ifconfig -l | tr ' ' '\n' | grep '^utun' | sort)"
UTUN="$(comm -13 <(echo "$BEFORE") <(echo "$AFTER") | head -1)"

# 没找到新建 utun → 异常，输出 BEFORE/AFTER 供排查
[ -z "$UTUN" ] && { echo "!! no new utun detected (BEFORE=[$BEFORE] AFTER=[$AFTER])"; exit 1; }

# ========== 阶段 6：配置路由和 DNS ==========

# 两种场景走不同分支：
#   soak / soak-knife5：全局劫持（添加 /15 网段路由 + 改系统 DNS）
#   start（iperf3 单项）：只把目标 IP 的流量路由进 TUN，不碰 DNS

if [ "$ACTION" = "soak" ] || [ "$ACTION" = "soak-knife5" ]; then

  # ── 获取当前网络服务名（用于 networksetup 操作 DNS）──
  SVC="$(detect_netsvc)"

  # ── 添加网段路由：将 198.18.0.0/15 所有流量导向 VPN TUN ──
  #   -net（不是 -host）：操作网段而非单台主机
  #   -interface：强制走指定接口，不查路由表
  #   >/dev/null 2>&1：静默标准输出和标准错误（route add 会输出操作结果）
  route -n add -net "$FAKE_NET" -interface "$UTUN" >/dev/null 2>&1 \
    && echo "route ${FAKE_NET} -> ${UTUN} OK" \
    || echo "!! /15 route add failed"

  # ── 保存当前 DNS 配置（给 soak-stop 还原用）──
  #   networksetup -getdnsservers 输出可能是：
  #     正常： 8.8.8.8\n1.1.1.1
  #     无 DNS："There aren't any DNS Servers set on Wi-Fi."
  networksetup -getdnsservers "$SVC" 2>/dev/null > "$DNS_SAVE"

  # ── 刀5 模式：特殊 DNS 劫持验证 ──
  if [ "$ACTION" = "soak-knife5" ]; then

    # 把外部 DNS（如 8.8.8.8）的流量也路由进 TUN
    # 这样即使系统 DNS 不指向我们，:53 请求仍经过 TUN → 被本地劫持
    route -n add -host "$K5_RES" -interface "$UTUN" >/dev/null 2>&1 \
      && echo "route ${K5_RES} -> ${UTUN} OK (alt-resolver into TUN)" \
      || echo "!! alt-resolver route add failed"

    # 把系统 DNS 设成外部 resolver（验证不依赖系统 DNS 配置）
    networksetup -setdnsservers "$SVC" "$K5_RES" \
      && echo "DNS(${SVC}) -> ${K5_RES} (NOT 198.18.0.1; saved=${DNS_SAVE}; soak-stop auto-reverts)"

    # 输出验证命令（需要用户另开终端手动执行）：
    #   dig @8.8.8.8 example.com +short       → 期望返回 198.18.x.x（说明 DNS 被劫持）
    #   dig +tcp @8.8.8.8 example.com +short  → 期望超时/拒绝（TCP :53 被 RST）
    #   curl https://example.com               → 期望 200（说明经隧道正常访问）
    echo "---- 刀5 验证（另开终端跑；判据见 plan T-DNS）----"
    echo "  dig @${K5_RES} example.com +short       # 期望 198.18.x.x（fake-IP，非真实 IP）"
    echo "  dig +tcp @${K5_RES} example.com +short  # 期望 超时/拒绝（TCP :53 被 RST）"
    echo "  curl -sS -o /dev/null -w '%{http_code}\\n' https://example.com  # 期望 200/301（经隧道）"
    echo "  grep '🪪 DNS' ${LOG}                     # 期望 见 example.com → fake-IP"

  else
    # ── 普通 soak 模式：系统 DNS 指向假 DNS（198.18.0.1）──
    #   VPN TUN 在 198.18.0.1:53 上做 DNS 劫持，返回 fake IP。
    #   所有 DNS 查询 → 198.18.0.1 → TUN 内劫持 → fake IP → 经隧道代理。
    networksetup -setdnsservers "$SVC" "$FAKE_DNS" \
      && echo "DNS(${SVC}) -> ${FAKE_DNS} (saved=${DNS_SAVE}; soak-stop auto-reverts)"
  fi

else
  # ── start 模式：只添加单主机路由（iperf3 目标 IP）──
  #   不改系统 DNS——iperf3 测试只需目标 IP 走 VPN 隧道即可。
  route -n add -host "$TARGET" -interface "$UTUN" >/dev/null 2>&1 \
    && echo "route ${TARGET} -> ${UTUN} OK" \
    || echo "!! route add failed"
fi

# ========== 阶段 7：就绪 —— 输出启动摘要 ==========

# 从日志中提取最关键的启动信息
#   grep -E：扩展正则（等同于 egrep），用 | 匹配多个模式
#   tail -3：只取最后 3 行（避免日志太长淹没终端）
echo "---- startup lines ----"
grep -E "TUIC datagram|UDP relay mode" "$LOG" | tail -3

# 一行汇总：列出所有关键信息，方便用户确认状态
echo "READY  utun=${UTUN}  cc=${CC} mode=${MODE}  log=${LOG}"
