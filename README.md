# 分层限速采集调度系统（纯本地演示）

同一来源按**接口、账号、出口 IP** 三层分别限速，429 返回的等待时间互相矛盾。
系统由四个角色组成，全部只依赖 Python 标准库，不访问任何真实网站：

| 角色 | 进程 | 默认端口 | 职责 |
|---|---|---|---|
| 限速来源 | `src/source_server.py` | 9000 | 本地模拟目标网站：三层令牌桶、稀疏目录、请求日志（地面真值）、在线改限速 |
| 统一调度 | `src/scheduler.py` | 8000 | 任务分配/转移（epoch 围栏）、许可发放（分层建模）、策略决策、周期对账、Web UI |
| 入库端 | `src/ingestor.py` | 8100 | 幂等入库、校验和验证、完整性统计、epoch 围栏拒绝旧进度 |
| 采集进程 ×2 | `src/collector.py` | — | A 绑定出口 127.0.0.1，B 绑定 127.0.0.2（Linux 127/8 回环，来源看到的是真实不同 IP） |

## 运行

```bash
./run_all.sh        # 启动全部（含两个演示任务），Web UI: http://127.0.0.1:8000
./stop_all.sh       # 停止
python3 tests/scenario_test.py   # 端到端场景测试（独立端口，7 项断言）
```

网页上可以：调整调度策略参数与来源限速、创建/暂停/转移任务、查看每个任务的
**真实入库进度**（实收/应收来自入库端统计，不是静态进度条）、来源端实际请求
记录、许可决策理由、对账审计计数。

## 分层限速与"矛盾的等待时间"

来源对每个请求检查三个令牌桶：接口桶（list/detail/image 各一）、账号桶（每个
token 一个）、出口桶（每个来源 IP 一个）。任一层不足即 429，并返回：

- `Retry-After`：只反映**等待最短的阻塞层**；
- `X-RateLimit-Wait-Interface/Account/Egress`：三层各自的等待。

两者经常矛盾（按 Retry-After 醒来会撞在另一层上）。调度器因此**不信任何单一
等待时间**：为每个 （接口/账号/出口） 键维护 `earliest_ok` 估计，许可的
`not_before = max(三层估计)`；收到 429 时用三层响应头分别校正对应层。这
就是"不统一睡眠、区分各层限制"——detail 冷却时 image 许可照常发放。

## 可说明的调度策略（公平分配 vs 高优先级补缺）

每个候选 = （任务 × 接口 × 账号），评分：

```
有效权重 = 优先级 × (1 + gap_bonus × 缺口率)     # 缺口率 = 缺失/应收（来自入库端）
得分     = 有效权重 / (1 + 窗口内该任务已获许可数)  # 公平项：近期拿得越多分越低
```

- 赢家仅凭优先级+公平项胜出 → 记 **fair-share**；
- 存在基础优先级更高的候选、但赢家靠缺口加分反超 → 记 **gap-fill**。

每次决策把全部候选的评分明细与文字说明写入 `decisions` 表，网页实时可见；
`gap_bonus` 等参数可在线调整（PUT /ui/api/policy）。

## 任务转移的围栏（fencing）

任务是 `(task_id, epoch)` 租约。转移 = epoch+1，且**先在入库端登记新 epoch、
再在调度器生效**（同一把锁内完成，与许可发放严格串行）：

- 旧进程申请许可携带旧 epoch → 409，立即放弃任务；
- 旧进程提交数据/进度 → 入库端按当前 epoch 校验 → 409 围栏丢弃；
- 转移瞬间已发出未过期（TTL 默认 4s）的许可最多泄漏一次请求，对账循环会
  在来源请求日志里发现它们并记 `stale-permit-used` 告警——有界且可审计。

## 恢复核对

| 故障 | 行为 |
|---|---|
| 调度器短暂离线 | 采集进程拿不到许可 → 指数退避等待，绝不绕过许可直接请求；调度器状态全在 SQLite，重启后续跑并立即重新对账 |
| 请求成功后进程退出 | 成功响应先写本地 WAL 再上报；重启时重放 WAL，入库端按主键幂等去重；未回报的许可由"许可超时 + 来源请求日志对账"补记为已消耗 |
| 来源临时改限速 | 429 的三层等待头实时校正估计；每 5s 周期核对 `/admin/limits` 发现配置变化并记 `limits-changed`；恢复后估计随之回落 |
| 任务转移后旧进程残留 | epoch 围栏（见上节）；新进程从入库端缺失集合续抓，已入库的绝不重复抓取 |

对账循环（reconcile）持续比对三份账：调度器许可账、来源请求日志、入库端
完整性。`GET /ui/api/audit` 给出差异计数（无许可请求、过期许可被用、成功未
回报、围栏回报）。

## 目录发现与完整性

目录是稀疏的（约 3/4 的 id 存在）。任务指定 id 范围后，采集进程先走 `list`
接口发现存在的 id 并推送到入库端（`/expected`，同样受 epoch 围栏），再按
`detail`（必需）/`image`（可选）两部分抓取。入库端按 (task,item,part) 主键
幂等、重算校验和，完整性 = 实收/应收，缺失集合驱动续抓与补缺。

## 主要接口

```
来源   GET /api/{list|detail|image}   GET/PUT /admin/limits   GET /admin/requests
调度   POST /api/register|heartbeat|permit/request|permit/report
       GET  /ui/api/overview|events|decisions|permits|audit|policy
       POST /ui/api/tasks  /ui/api/tasks/{id}/transfer|pause|resume|priority
入库   POST /task|expected|batch      GET /completeness|missing|stats
```

数据文件在 `data/`（SQLite，WAL 模式），日志在 `logs/`。
