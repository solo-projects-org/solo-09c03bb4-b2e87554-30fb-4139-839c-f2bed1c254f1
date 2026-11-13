# 灭菌釜温度采集系统

罐头厂质控场景：按锅次时间段向验证程序交付**完整**的灭菌釜温度序列。

- **collector**：FastAPI + asyncua + SQLite 采集 API。指定起始 UTC 整秒后持续采集一个
  OPC UA 温度节点；**实时订阅**与 **Historical Access 翻页补采**并行；断线或服务重启后
  只补缺段，不重读已收齐区间。Swagger 文档在 `/docs`。
- **opcua-source**：本地 OPC UA 温度源。每整秒一条（SourceTimestamp 唯一、落在整秒、
  历史不可更正），历史存 SQLite 数据卷；自带故障注入控制口（慢翻页 / 断线 / 缺秒样例）。

## 快速启动

```bash
docker compose up -d --build
```

| 服务 | 地址 |
|---|---|
| 采集 API / Swagger | http://localhost:8000/docs |
| OPC UA 源 | opc.tcp://localhost:4840，节点 `ns=2;s=Kettle1.Temperature` |
| 源故障注入控制口 | http://localhost:8081 |

状态与记录分别落在数据卷 `collector-data`（采集库）和 `source-data`（源历史库）。

## API 摘要

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/jobs` | 创建采集任务：`{"node_id": "...", "start_utc": <起始UTC整秒>}` |
| GET | `/jobs` | 任务列表 |
| GET | `/jobs/{job_id}` | 任务状态（实时连接、入库数、确认缺失数、待补秒数） |
| POST | `/jobs/{job_id}/stop` | 停止任务 |
| GET | `/jobs/{job_id}/query?start=&end=` | 查询**已结束**区间 `[start, end)`：温度、缺秒、是否收齐 |

`query` 返回字段：

- `samples`：每秒一条 `{ts, value, source}`（`source` 为 `realtime`/`history`，可直观看到补采与实时入库）；
- `missing`：缺秒列表。**每秒均已持久保存才报 `complete=true`**；源历史里也没有的秒
  （`missing_confirmed`）仍然列在 `missing` 里；
- `pending`：缺秒中尚未补采确认的部分（补采进行中）。

## 演示流程

```bash
# 0. 取当前 UTC 整秒
NOW=$(date +%s)

# 1. 创建任务：从 10 分钟前开始采集（补采历史的同时实时值持续入库）
curl -s -X POST localhost:8000/jobs -H 'Content-Type: application/json' \
  -d "{\"node_id\":\"ns=2;s=Kettle1.Temperature\",\"start_utc\":$((NOW-600))}"
# 观察状态：samples 增长来自 history（补采）与 realtime（实时）两路
curl -s localhost:8000/jobs/<job_id>

# 2. 慢翻页样例：历史读取每页延迟 800ms（页大小 HISTORY_PAGE_SIZE=50）
curl -s -X POST 'localhost:8081/control/paging_delay?ms=800'
# 再建一个起始更早的任务，观察 pending_seconds 随各补采段提交阶梯式下降，
# 同时 rt_inserted 持续增长（补采中实时照常入库）；演示完恢复：
curl -s -X POST 'localhost:8081/control/paging_delay?ms=0'

# 3a. 断网样例（源仍在产出）：断开采集器网络，实时订阅掉线、补采暂停；
#     恢复网络后自动重连，断网期间的秒从源历史补齐 -> complete=true
docker network disconnect $(docker inspect collector --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}') collector
sleep 15
docker network connect $(docker inspect opcua-source --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}') collector

# 3b. 源停机样例：源进程退出，compose 自动拉起；停机期间源本身不产出，
#     这些秒源历史里也没有 -> 查询时如实列为缺口（complete=false）
curl -s -X POST localhost:8081/control/kill
# 或手动：docker compose restart opcua-source

# 4. 缺秒样例：源接下来 5 个整秒不产出（源历史同样没有 -> 永久缺口）
curl -s -X POST 'localhost:8081/control/drop?seconds=5'
# 几秒后查询覆盖该时段的区间：complete=false，这 5 秒出现在 missing / missing_confirmed
curl -s "localhost:8000/jobs/<job_id>/query?start=<T>&end=<T+60>"

# 5. 重启续采：重启采集服务，任务自动恢复，只补缺段，不重读已收齐区间
docker compose restart collector
curl -s localhost:8000/jobs/<job_id>   # samples 保留，pending_seconds 回落到 0
```

## 设计要点（与质控要求对应）

- **并行双路**：实时订阅（看门狗自动重连）负责低延迟入库；补采循环周期性用
  Historical Access 按页（`NumValuesPerNode` + continuation point）读取缺段。两路独立，
  补采中实时照常入库。
- **只补缺段**：补采范围完全由 SQLite 中已持久化状态（`samples ∪ checked_missing`）
  推导，重启后从缺口继续，已收齐区间不重读。
- **较晚的实时值不跳过缺段**：实时值只写它自己的那一秒；缺段计算不引用实时水位。
- **读写失败不跳过**：一个补采段读完所有页后，"样本 + 段内确认缺失"在同一事务提交；
  读失败或写失败都不更新状态，下一轮重试。
- **重复时刻只存一次**：`PRIMARY KEY(job_id, ts)` + `INSERT OR IGNORE`。
- **收齐口径**：`complete = (missing 为空)`；源历史确认缺失的秒记入 `checked_missing`
  （避免对永久缺口反复重读），查询时仍列入 `missing`。

## 配置（环境变量）

collector：`OPCUA_ENDPOINT`、`DATA_DIR`、`BACKFILL_INTERVAL`(2s)、`HISTORY_PAGE_SIZE`(50)、
`MAX_SEGMENT_SECONDS`(300)、`REALTIME_PERIOD_MS`(500)、`REQUEST_TIMEOUT`(10)。

opcua-source：`PORT`(4840)、`CONTROL_PORT`(8081)、`DATA_DIR`、`HISTORY_MAX_PAGE`(100)、
`DROP_PROB`(0，>0 时按比例随机缺秒)。
