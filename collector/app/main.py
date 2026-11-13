"""灭菌釜温度采集 API。

- POST /jobs                 指定起始 UTC 整秒，开始持续采集一个温度节点
- GET  /jobs                 任务列表
- GET  /jobs/{job_id}        任务状态（实时连接、已入库、确认缺失、待补秒数）
- POST /jobs/{job_id}/stop   停止任务
- GET  /jobs/{job_id}/query  按起止秒查询已结束区间 [start, end)：温度、缺秒、是否收齐

Swagger 文档：/docs
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field, StrictInt

from .config import Config
from .db import Database, now_sec
from .jobs import JobManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logging.getLogger("asyncua").setLevel(logging.WARNING)

DEFAULT_NODE = "ns=2;s=Kettle1.Temperature"


class JobCreate(BaseModel):
    node_id: str = Field(
        default=DEFAULT_NODE,
        description="OPC UA 温度节点 NodeId",
        examples=[DEFAULT_NODE],
    )
    start_utc: StrictInt = Field(
        description="起始 UTC 整秒（epoch 秒，必须为整数）",
        examples=[1700000000],
    )


class JobOut(BaseModel):
    job_id: str
    node_id: str
    start_utc: int
    created_at: int
    stopped: bool


class JobStatus(JobOut):
    realtime_connected: bool = False
    rt_inserted: int = 0
    samples: int = 0
    checked_missing: int = 0
    pending_seconds: int = 0
    segments_done: int = 0
    last_segment: str | None = None
    last_error: str | None = None


class SampleOut(BaseModel):
    ts: int = Field(description="UTC 整秒（SourceTimestamp）")
    value: float = Field(description="温度")
    source: str = Field(description="入库途径：realtime / history")


class QueryResult(BaseModel):
    job_id: str
    node_id: str
    start: int = Field(description="区间起始秒（含）")
    end: int = Field(description="区间结束秒（不含）")
    expected: int = Field(description="区间应收秒数 = end - start")
    received: int = Field(description="实际已持久保存秒数")
    complete: bool = Field(description="是否收齐：每秒均已持久保存才为 true")
    missing: list[int] = Field(description="缺秒列表（含源历史缺失的秒）")
    missing_confirmed: list[int] = Field(description="其中已经历史读取确认源缺失的秒")
    pending: list[int] = Field(description="其中尚未补采确认的秒（补采进行中）")
    samples: list[SampleOut]


def build_app() -> FastAPI:
    cfg = Config()
    db = Database(cfg.db_path)
    manager = JobManager(cfg, db)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await db.init()
        await manager.load_active_jobs()  # 重启续采：恢复未停止的任务
        yield
        await manager.shutdown()
        await db.close()

    app = FastAPI(
        title="灭菌釜温度采集",
        version="1.0.0",
        description="实时订阅 + Historical Access 翻页补采并行，断线/重启只补缺段。",
        lifespan=lifespan,
    )

    def job_out(job: dict) -> JobOut:
        return JobOut(**job)

    @app.get("/", summary="服务信息")
    async def root():
        return {"service": "retort-temperature-collector", "docs": "/docs"}

    @app.get("/health", summary="健康检查")
    async def health():
        return {"ok": True, "now": now_sec()}

    @app.post("/jobs", response_model=JobOut, status_code=201, summary="创建采集任务")
    async def create_job(body: JobCreate):
        """指定起始 UTC 整秒，持续采集一个 OPC UA 温度节点（实时订阅与历史补采并行）。"""
        if body.start_utc < 0:
            raise HTTPException(400, "start_utc 必须为非负整数秒")
        job_id = await manager.create_job(body.node_id, body.start_utc)
        job = await db.get_job(job_id)
        return job_out(job)

    @app.get("/jobs", response_model=list[JobOut], summary="任务列表")
    async def list_jobs():
        return [job_out(j) for j in await db.list_jobs()]

    @app.get("/jobs/{job_id}", response_model=JobStatus, summary="任务状态")
    async def job_status(job_id: str):
        job = await db.get_job(job_id)
        if job is None:
            raise HTTPException(404, "任务不存在")
        counts = await db.job_counts(job_id, now_sec() - 1, job["start_utc"])
        rt = manager.runtime(job_id)
        return JobStatus(
            **job,
            realtime_connected=rt.realtime_connected if rt else False,
            rt_inserted=rt.rt_inserted if rt else 0,
            segments_done=rt.segments_done if rt else 0,
            last_segment=rt.last_segment if rt else None,
            last_error=rt.last_error if rt else None,
            **counts,
        )

    @app.post("/jobs/{job_id}/stop", summary="停止任务")
    async def stop_job(job_id: str):
        if not await manager.stop_job(job_id):
            raise HTTPException(404, "任务不存在")
        return {"stopped": True}

    @app.get(
        "/jobs/{job_id}/query",
        response_model=QueryResult,
        summary="按起止秒查询已结束区间（含起不含止）",
    )
    async def query(
        job_id: str,
        start: int = Query(description="起始 UTC 整秒（含）"),
        end: int = Query(description="结束 UTC 整秒（不含）"),
    ):
        job = await db.get_job(job_id)
        if job is None:
            raise HTTPException(404, "任务不存在")
        if start >= end:
            raise HTTPException(400, "要求 start < end")
        if start < job["start_utc"]:
            raise HTTPException(400, f"start 早于任务起始秒 {job['start_utc']}")
        if end > now_sec():
            raise HTTPException(400, "区间尚未结束：要求 end <= 当前 UTC 整秒")
        rows, missing, confirmed = await db.query_range(job_id, start, end)
        confirmed_in_range = sorted(confirmed)
        pending = sorted(set(missing) - confirmed)
        return QueryResult(
            job_id=job_id,
            node_id=job["node_id"],
            start=start,
            end=end,
            expected=end - start,
            received=len(rows),
            complete=len(missing) == 0,
            missing=missing,
            missing_confirmed=confirmed_in_range,
            pending=pending,
            samples=[SampleOut(ts=r[0], value=r[1], source=r[2]) for r in rows],
        )

    return app


app = build_app()
