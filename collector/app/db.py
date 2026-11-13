"""SQLite 持久层。

三张表：
- jobs            采集任务（起始 UTC 整秒、节点、是否停止）
- samples         已持久保存的温度值，每秒最多一行（PRIMARY KEY(job_id, ts) + INSERT OR IGNORE，重复时刻只存一次）
- checked_missing 已通过历史读取确认"源历史缺失"的秒，避免对永久缺口反复翻页重读；
                  这些秒在查询时仍然作为缺口列出（源历史缺失仍列缺口）。

补采状态完全由 samples ∪ checked_missing 推导，因此服务重启后只需补缺段，
不会重读已收齐区间。
"""
from __future__ import annotations

import asyncio
import os
import time

import aiosqlite

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id     TEXT PRIMARY KEY,
    node_id    TEXT NOT NULL,
    start_utc  INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    stopped    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS samples (
    job_id      TEXT NOT NULL,
    ts          INTEGER NOT NULL,
    value       REAL NOT NULL,
    ingested_at INTEGER NOT NULL,
    source      TEXT NOT NULL,          -- realtime / history
    PRIMARY KEY (job_id, ts)
);
CREATE TABLE IF NOT EXISTS checked_missing (
    job_id     TEXT NOT NULL,
    ts         INTEGER NOT NULL,
    checked_at INTEGER NOT NULL,
    PRIMARY KEY (job_id, ts)
);
CREATE INDEX IF NOT EXISTS idx_samples_job_ts ON samples(job_id, ts);
CREATE INDEX IF NOT EXISTS idx_missing_job_ts ON checked_missing(job_id, ts);
"""


def now_sec() -> int:
    """当前 UTC 整秒（epoch）。"""
    return int(time.time())


class Database:
    def __init__(self, path: str) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None
        # 所有写操作共用一把锁：保证 save_segment 的"样本 + 确认缺失"事务
        # 不会被实时写入插队，事务要么整体落库要么整体回滚。
        self._wlock = asyncio.Lock()

    async def init(self) -> None:
        os.makedirs(os.path.dirname(os.path.abspath(self._path)), exist_ok=True)
        # isolation_level=None：自动提交模式，事务边界由本层显式控制
        self._conn = await aiosqlite.connect(self._path, isolation_level=None)
        await self._conn.execute("PRAGMA journal_mode=WAL")
        await self._conn.execute("PRAGMA synchronous=NORMAL")
        await self._conn.executescript(SCHEMA)

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None

    @property
    def conn(self) -> aiosqlite.Connection:
        assert self._conn is not None, "database not initialized"
        return self._conn

    # ---------------- jobs ----------------

    async def create_job(self, job_id: str, node_id: str, start_utc: int) -> None:
        async with self._wlock:
            await self.conn.execute(
                "INSERT INTO jobs(job_id, node_id, start_utc, created_at) VALUES(?,?,?,?)",
                (job_id, node_id, start_utc, now_sec()),
            )

    async def get_job(self, job_id: str) -> dict | None:
        cur = await self.conn.execute(
            "SELECT job_id, node_id, start_utc, created_at, stopped FROM jobs WHERE job_id=?",
            (job_id,),
        )
        row = await cur.fetchone()
        await cur.close()
        if row is None:
            return None
        return {
            "job_id": row[0],
            "node_id": row[1],
            "start_utc": row[2],
            "created_at": row[3],
            "stopped": bool(row[4]),
        }

    async def list_jobs(self) -> list[dict]:
        cur = await self.conn.execute(
            "SELECT job_id, node_id, start_utc, created_at, stopped FROM jobs ORDER BY created_at"
        )
        rows = await cur.fetchall()
        await cur.close()
        return [
            {
                "job_id": r[0],
                "node_id": r[1],
                "start_utc": r[2],
                "created_at": r[3],
                "stopped": bool(r[4]),
            }
            for r in rows
        ]

    async def stop_job(self, job_id: str) -> None:
        async with self._wlock:
            await self.conn.execute("UPDATE jobs SET stopped=1 WHERE job_id=?", (job_id,))

    # ---------------- samples ----------------

    async def insert_sample(self, job_id: str, ts: int, value: float, source: str) -> bool:
        """写入一条温度值；同一时刻已存在则忽略（重复时刻只存一次）。返回是否真正写入。"""
        async with self._wlock:
            cur = await self.conn.execute(
                "INSERT OR IGNORE INTO samples(job_id, ts, value, ingested_at, source)"
                " VALUES(?,?,?,?,?)",
                (job_id, ts, value, now_sec(), source),
            )
            return cur.rowcount > 0

    async def save_segment(self, job_id: str, seg_lo: int, seg_hi: int, values: dict[int, float]) -> int:
        """一个补采段落库：读到的值写入 samples，段内没读到的秒记入 checked_missing。

        两者在同一事务中提交：写失败整体回滚，段不会被标记为已检查，
        下一轮补采会重试（读写失败不得跳过未收齐数据）。
        返回本段确认缺失的秒数。
        """
        now = now_sec()
        missing = [s for s in range(seg_lo, seg_hi + 1) if s not in values]
        async with self._wlock:
            try:
                await self.conn.execute("BEGIN IMMEDIATE")
                if values:
                    await self.conn.executemany(
                        "INSERT OR IGNORE INTO samples(job_id, ts, value, ingested_at, source)"
                        " VALUES(?,?,?,?,?)",
                        [
                            (job_id, s, v, now, "history")
                            for s, v in sorted(values.items())
                        ],
                    )
                if missing:
                    await self.conn.executemany(
                        "INSERT OR IGNORE INTO checked_missing(job_id, ts, checked_at) VALUES(?,?,?)",
                        [(job_id, s, now) for s in missing],
                    )
                await self.conn.execute("COMMIT")
            except Exception:
                await self.conn.execute("ROLLBACK")
                raise
        return len(missing)

    # ---------------- 缺口计算 ----------------

    async def find_gaps(self, job_id: str, lo: int, hi: int, max_segment: int) -> list[tuple[int, int]]:
        """计算 [lo, hi] 内尚未收齐（既无样本也未确认缺失）的连续秒段，按 max_segment 切块。"""
        if hi < lo:
            return []
        expected = hi - lo + 1
        cur = await self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM samples WHERE job_id=? AND ts BETWEEN ? AND ?)"
            " + (SELECT COUNT(*) FROM checked_missing WHERE job_id=? AND ts BETWEEN ? AND ?)",
            (job_id, lo, hi, job_id, lo, hi),
        )
        row = await cur.fetchone()
        await cur.close()
        if row and row[0] == expected:
            return []  # 已收齐，不重读

        cur = await self.conn.execute(
            "SELECT ts FROM samples WHERE job_id=? AND ts BETWEEN ? AND ?"
            " UNION SELECT ts FROM checked_missing WHERE job_id=? AND ts BETWEEN ? AND ?",
            (job_id, lo, hi, job_id, lo, hi),
        )
        present = {r[0] for r in await cur.fetchall()}
        await cur.close()

        gaps: list[tuple[int, int]] = []
        run_start: int | None = None
        for s in range(lo, hi + 1):
            if s in present:
                if run_start is not None:
                    gaps.append((run_start, s - 1))
                    run_start = None
            elif run_start is None:
                run_start = s
        if run_start is not None:
            gaps.append((run_start, hi))

        chunked: list[tuple[int, int]] = []
        for a, b in gaps:
            while b - a + 1 > max_segment:
                chunked.append((a, a + max_segment - 1))
                a += max_segment
            chunked.append((a, b))
        return chunked

    # ---------------- 查询与统计 ----------------

    async def query_range(self, job_id: str, start: int, end: int):
        """查询 [start, end) 区间：返回 (样本行, 缺秒列表, 其中源历史确认缺失的集合)。"""
        cur = await self.conn.execute(
            "SELECT ts, value, source FROM samples WHERE job_id=? AND ts>=? AND ts<? ORDER BY ts",
            (job_id, start, end),
        )
        rows = await cur.fetchall()
        await cur.close()
        cur = await self.conn.execute(
            "SELECT ts FROM checked_missing WHERE job_id=? AND ts>=? AND ts<?",
            (job_id, start, end),
        )
        confirmed = {r[0] for r in await cur.fetchall()}
        await cur.close()
        present = {r[0] for r in rows}
        missing = [s for s in range(start, end) if s not in present]
        return rows, missing, confirmed

    async def job_counts(self, job_id: str, hi: int, start_utc: int) -> dict:
        """统计：样本数、确认缺失数、已结束但未收齐的秒数。"""
        cur = await self.conn.execute(
            "SELECT (SELECT COUNT(*) FROM samples WHERE job_id=?),"
            "       (SELECT COUNT(*) FROM checked_missing WHERE job_id=?)",
            (job_id, job_id),
        )
        row = await cur.fetchone()
        await cur.close()
        samples_cnt, missing_cnt = (row[0], row[1]) if row else (0, 0)
        pending = 0
        if hi >= start_utc:
            cur = await self.conn.execute(
                "SELECT (SELECT COUNT(*) FROM samples WHERE job_id=? AND ts BETWEEN ? AND ?)"
                " + (SELECT COUNT(*) FROM checked_missing WHERE job_id=? AND ts BETWEEN ? AND ?)",
                (job_id, start_utc, hi, job_id, start_utc, hi),
            )
            row = await cur.fetchone()
            await cur.close()
            covered = row[0] if row else 0
            pending = (hi - start_utc + 1) - covered
        return {
            "samples": samples_cnt,
            "checked_missing": missing_cnt,
            "pending_seconds": pending,
        }
