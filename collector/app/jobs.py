"""采集任务运行时：每个任务两条并行的监督循环。

1. 实时订阅循环：订阅节点数据变化，值到达即落库；看门狗周期读服务器时间，
   断线自动重连重订阅。较晚到达的实时值只写它自己的那一秒，绝不影响缺段计算。
2. 历史翻页补采循环：周期性地根据 SQLite 中已持久化状态（samples ∪ checked_missing）
   计算 [start_utc, 最近已结束秒] 内的缺段，用 Historical Access 按页读取补齐；
   读或写失败时不更新任何状态，下一轮重试，未收齐数据不会被跳过。
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime, timezone

from asyncua import Client, ua

from .config import Config
from .db import Database, now_sec

logger = logging.getLogger("collector.jobs")

# Server_ServerStatus_CurrentTime，用于实时连接看门狗
WATCHDOG_NODE = "i=2258"


def ts_to_sec(dt: datetime | None) -> int | None:
    """OPC UA SourceTimestamp -> UTC 整秒； naive 按 UTC 处理。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def sec_to_dt(sec: int) -> datetime:
    return datetime.fromtimestamp(sec, timezone.utc)


class _RealtimeHandler:
    """订阅回调：把到达的值按 SourceTimestamp 整秒落库（重复时刻由主键去重）。"""

    def __init__(self, job: "JobRuntime") -> None:
        self._job = job

    async def datachange_notification(self, node, val, data) -> None:
        try:
            dv = data.monitored_item.Value
            st = dv.SourceTimestamp
            # 源契约每整秒一条：非整秒时间戳（如订阅初始值）直接忽略
            if st is None or st.microsecond != 0:
                return
            sec = ts_to_sec(st)
            if sec is None or sec < self._job.start_utc:
                return
            inserted = await self._job.manager.db.insert_sample(
                self._job.job_id, sec, float(dv.Value.Value), "realtime"
            )
            if inserted:
                self._job.rt_inserted += 1
        except Exception:  # 回调里不能抛出，失败仅记录；缺段由补采兜底
            logger.exception("job %s: 实时值落库失败", self._job.job_id)


class JobRuntime:
    def __init__(self, manager: "JobManager", job_id: str, node_id: str, start_utc: int) -> None:
        self.manager = manager
        self.job_id = job_id
        self.node_id = node_id
        self.start_utc = start_utc
        self.realtime_connected = False
        self.rt_inserted = 0
        self.segments_done = 0
        self.last_segment: str | None = None
        self.last_error: str | None = None
        self._stop = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    @property
    def cfg(self) -> Config:
        return self.manager.cfg

    def start(self) -> None:
        self._tasks = [
            asyncio.create_task(self._realtime_loop(), name=f"rt-{self.job_id}"),
            asyncio.create_task(self._backfill_loop(), name=f"bf-{self.job_id}"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    # ---------------- 实时订阅 ----------------

    async def _realtime_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with Client(self.cfg.endpoint, timeout=self.cfg.request_timeout) as client:
                    node = client.get_node(self.node_id)
                    sub = await client.create_subscription(
                        self.cfg.realtime_period_ms, _RealtimeHandler(self)
                    )
                    await sub.subscribe_data_change(node)
                    self.realtime_connected = True
                    backoff = 1.0
                    logger.info("job %s: 实时订阅已建立 (%s)", self.job_id, self.node_id)
                    while not self._stop.is_set():
                        await asyncio.sleep(self.cfg.watchdog_interval)
                        await client.get_node(WATCHDOG_NODE).read_value()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.last_error = f"realtime: {exc!r}"
                logger.warning("job %s: 实时连接断开: %r", self.job_id, exc)
            finally:
                self.realtime_connected = False
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 15.0)

    # ---------------- 历史翻页补采 ----------------

    async def _backfill_loop(self) -> None:
        client: Client | None = None
        backoff = 1.0
        try:
            while not self._stop.is_set():
                try:
                    hi = now_sec() - 1  # 最近一个已结束的整秒
                    if hi >= self.start_utc:
                        gaps = await self.manager.db.find_gaps(
                            self.job_id, self.start_utc, hi, self.cfg.max_segment_seconds
                        )
                        if gaps:
                            if client is None:
                                client = Client(self.cfg.endpoint, timeout=self.cfg.request_timeout)
                                await client.connect()
                                logger.info("job %s: 补采连接已建立", self.job_id)
                            node = client.get_node(self.node_id)
                            for a, b in gaps:
                                if self._stop.is_set():
                                    break
                                await self._fetch_segment(node, a, b)
                    backoff = 1.0
                    wait = self.cfg.backfill_interval
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    # 读失败：不更新任何持久状态，缺段保留，退避后重试
                    self.last_error = f"backfill: {exc!r}"
                    logger.warning("job %s: 补采失败(稍后重试): %r", self.job_id, exc)
                    if client is not None:
                        try:
                            await client.disconnect()
                        except Exception:
                            pass
                        client = None
                    wait = backoff
                    backoff = min(backoff * 2, 30.0)
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=wait)
                except asyncio.TimeoutError:
                    pass
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass

    async def _fetch_segment(self, node, a: int, b: int) -> None:
        """用 Historical Access 按页读取 [a, b]，整段读完后一次性事务落库。"""
        details = ua.ReadRawModifiedDetails()
        details.IsReadModified = False
        details.StartTime = sec_to_dt(a)
        details.EndTime = sec_to_dt(b + 1)  # 边界多要一秒，客户端再过滤，避免服务端包含性差异
        details.NumValuesPerNode = self.cfg.history_page_size
        details.ReturnBounds = False

        values: dict[int, float] = {}
        continuation: bytes | None = None
        pages = 0
        while True:
            if self._stop.is_set():
                return  # 任务停止：本段不落库，下次重新计算缺段
            result = await node.history_read(details, continuation)
            result.StatusCode.check()
            pages += 1
            history_data = result.HistoryData
            if history_data is not None and history_data.DataValues:
                for dv in history_data.DataValues:
                    sec = ts_to_sec(dv.SourceTimestamp)
                    if sec is None or not (a <= sec <= b) or sec < self.start_utc:
                        continue
                    val = dv.Value.Value
                    if val is not None:
                        values[sec] = float(val)
            prev = continuation
            continuation = result.ContinuationPoint
            if not continuation:
                break
            if continuation == prev:
                raise RuntimeError("服务端 continuation point 未推进，中止本段防止死循环")

        missing_cnt = await self.manager.db.save_segment(self.job_id, a, b, values)
        self.segments_done += 1
        self.last_segment = f"[{a}, {b}] 页数={pages} 入库={len(values)} 源缺失={missing_cnt}"
        logger.info(
            "job %s: 补采段 [%d, %d] 完成: %d 页, 入库 %d 条, 源历史缺失 %d 秒",
            self.job_id, a, b, pages, len(values), missing_cnt,
        )


class JobManager:
    def __init__(self, cfg: Config, db: Database) -> None:
        self.cfg = cfg
        self.db = db
        self.runtimes: dict[str, JobRuntime] = {}

    async def load_active_jobs(self) -> None:
        """服务重启后恢复所有未停止的任务（重启续采）。"""
        for job in await self.db.list_jobs():
            if not job["stopped"]:
                self._start_runtime(job["job_id"], job["node_id"], job["start_utc"])
                logger.info(
                    "重启续采: job %s node=%s start=%d",
                    job["job_id"], job["node_id"], job["start_utc"],
                )

    async def create_job(self, node_id: str, start_utc: int) -> str:
        job_id = uuid.uuid4().hex[:12]
        await self.db.create_job(job_id, node_id, start_utc)
        self._start_runtime(job_id, node_id, start_utc)
        logger.info("job %s: 新建任务 node=%s start=%d", job_id, node_id, start_utc)
        return job_id

    async def stop_job(self, job_id: str) -> bool:
        job = await self.db.get_job(job_id)
        if job is None:
            return False
        await self.db.stop_job(job_id)
        rt = self.runtimes.pop(job_id, None)
        if rt is not None:
            await rt.stop()
        return True

    def runtime(self, job_id: str) -> JobRuntime | None:
        return self.runtimes.get(job_id)

    async def shutdown(self) -> None:
        for job_id, rt in list(self.runtimes.items()):
            await rt.stop()
        self.runtimes.clear()

    def _start_runtime(self, job_id: str, node_id: str, start_utc: int) -> None:
        rt = JobRuntime(self, job_id, node_id, start_utc)
        self.runtimes[job_id] = rt
        rt.start()
