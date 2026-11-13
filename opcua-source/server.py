"""本地 OPC UA 灭菌釜温度源。

- 节点 ns=2;s=Kettle1.Temperature，每整秒产生一条温度值，SourceTimestamp 唯一且落在整秒上；
- Historical Access：自定义 SQLite 历史后端，落在数据卷里，源重启后历史仍在；
  历史一旦写入不可更正（INSERT OR IGNORE，绝不更新）；
- 故障注入控制口（HTTP，默认 8081）：
    POST /control/drop?seconds=N     接下来 N 个整秒不产出（源历史同样没有 -> 永久缺秒样例）
    POST /control/paging_delay?ms=M  历史读取每页延迟 M 毫秒（慢翻页样例）
    POST /control/kill               进程退出（配合 compose restart 演示断线）
    GET  /control/state              当前故障注入状态与计数
    GET  /health
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from asyncua import Server, ua
from asyncua.server.history import HistoryStorageInterface

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("asyncua").setLevel(logging.WARNING)
logger = logging.getLogger("retort-source")

PORT = int(os.getenv("PORT", "4840"))
CONTROL_PORT = int(os.getenv("CONTROL_PORT", "8081"))
DATA_DIR = os.getenv("DATA_DIR", "./data")
DROP_PROB = float(os.getenv("DROP_PROB", "0.0"))
HISTORY_MAX_PAGE = int(os.getenv("HISTORY_MAX_PAGE", "100"))

NODE_ID = "Kettle1.Temperature"


def dt_to_sec(dt: datetime | None) -> int | None:
    if dt is None or dt == ua.get_win_epoch():
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


class Chaos:
    """故障注入状态（控制线程与发射循环共享）。"""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.drop_remaining = 0
        self.page_delay_ms = 0


class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.emitted = 0
        self.dropped = 0


class SqliteHistoryStorage(HistoryStorageInterface):
    """SQLite 历史后端：持久化在数据卷，源重启后历史可继续翻页读取。"""

    def __init__(self, db_path: str, chaos: Chaos, max_page: int) -> None:
        super().__init__(max_page)
        self._chaos = chaos
        self._lock = threading.Lock()
        self._db = sqlite3.connect(db_path, check_same_thread=False)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS history("
            " node TEXT NOT NULL, ts INTEGER NOT NULL, value REAL NOT NULL,"
            " PRIMARY KEY(node, ts))"
        )
        self._db.commit()

    async def init(self) -> None:
        pass

    async def new_historized_node(self, node_id, period, count: int = 0) -> None:
        pass

    async def save_node_value(self, node_id, datavalue) -> None:
        st = datavalue.SourceTimestamp
        # 源契约：每整秒一条。非整秒的时间戳（如变量初始值）不进历史
        if st is None or st.microsecond != 0:
            return
        sec = dt_to_sec(st)
        # INSERT OR IGNORE：历史不可更正，同一时刻只存一条
        with self._lock:
            self._db.execute(
                "INSERT OR IGNORE INTO history(node, ts, value) VALUES(?,?,?)",
                (node_id.to_string(), sec, float(datavalue.Value.Value)),
            )
            self._db.commit()

    async def read_node_history(self, node_id, start, end, nb_values: int):
        # 慢翻页样例：每次（每页）历史读取前睡眠
        with self._chaos.lock:
            delay_ms = self._chaos.page_delay_ms
        if delay_ms > 0:
            await asyncio.sleep(delay_ms / 1000.0)

        lo = dt_to_sec(start)
        hi = dt_to_sec(end)
        lo = lo if lo is not None else -(2**62)
        hi = hi if hi is not None else 2**62 - 1
        with self._lock:
            rows = self._db.execute(
                "SELECT ts, value FROM history WHERE node=? AND ts BETWEEN ? AND ? ORDER BY ts",
                (node_id.to_string(), lo, hi),
            ).fetchall()

        limit = self.max_history_data_response_size
        if nb_values:
            limit = min(limit, nb_values)
        continuation = None
        if len(rows) > limit:
            # 续读点 = 第一条未返回值的 SourceTimestamp（服务端翻页）
            continuation = datetime.fromtimestamp(rows[limit][0], timezone.utc)
            rows = rows[:limit]

        values = []
        for ts, val in rows:
            dv = ua.DataValue(ua.Variant(val, ua.VariantType.Double))
            dv.SourceTimestamp = datetime.fromtimestamp(ts, timezone.utc)
            values.append(dv)
        return values, continuation

    async def new_historized_event(self, source_id, evtypes, period, count: int = 0) -> None:
        pass

    async def save_event(self, event) -> None:
        pass

    async def read_event_history(self, source_id, start, end, nb_values, evfilter):
        return [], None

    async def stop(self) -> None:
        with self._lock:
            self._db.close()


# ---------------- 故障注入控制口 ----------------

def make_control_handler(chaos: Chaos, stats: Stats):
    class ControlHandler(BaseHTTPRequestHandler):
        def _json(self, code: int, obj) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = urlparse(self.path).path
            if path == "/health":
                self._json(200, {"ok": True})
            elif path == "/control/state":
                with chaos.lock:
                    state = {
                        "drop_remaining": chaos.drop_remaining,
                        "page_delay_ms": chaos.page_delay_ms,
                    }
                with stats.lock:
                    state.update({"emitted": stats.emitted, "dropped": stats.dropped})
                self._json(200, state)
            else:
                self._json(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            qs = parse_qs(parsed.query)
            if parsed.path == "/control/drop":
                n = int(qs.get("seconds", ["5"])[0])
                with chaos.lock:
                    chaos.drop_remaining += n
                    remaining = chaos.drop_remaining
                logger.warning("故障注入: 接下来 %d 个整秒不产出", n)
                self._json(200, {"drop_remaining": remaining})
            elif parsed.path == "/control/paging_delay":
                ms = int(qs.get("ms", ["0"])[0])
                with chaos.lock:
                    chaos.page_delay_ms = ms
                logger.warning("故障注入: 历史翻页延迟 %d ms/页", ms)
                self._json(200, {"page_delay_ms": ms})
            elif parsed.path == "/control/kill":
                self._json(200, {"killed": True})
                logger.warning("故障注入: 进程退出（等待 compose 重启）")
                threading.Timer(0.3, lambda: os._exit(1)).start()
            else:
                self._json(404, {"error": "not found"})

        def log_message(self, *args) -> None:
            pass

    return ControlHandler


def start_control_server(chaos: Chaos, stats: Stats) -> None:
    httpd = ThreadingHTTPServer(("", CONTROL_PORT), make_control_handler(chaos, stats))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    logger.info("控制口: http://0.0.0.0:%d", CONTROL_PORT)


# ---------------- 温度发射 ----------------

def temperature_at(ts: int) -> float:
    """模拟灭菌釜温度曲线：基准 85°C 附近缓慢波动。"""
    return round(85.0 + 20.0 * math.sin(ts / 180.0) + random.gauss(0.0, 0.2), 3)


async def emit_loop(var, node_id, storage: SqliteHistoryStorage, chaos: Chaos, stats: Stats) -> None:
    # 计划时间戳独立递增：即使某次睡眠睡过头，也按原计划补发对应整秒，
    # 保证每整秒一条、SourceTimestamp 唯一（进程卡顿后会快速追平）
    ts = math.floor(time.time()) + 1
    while True:
        await asyncio.sleep(max(0.0, ts - time.time()))

        with chaos.lock:
            drop = chaos.drop_remaining > 0
            if drop:
                chaos.drop_remaining -= 1
        if not drop and DROP_PROB > 0 and random.random() < DROP_PROB:
            drop = True
        if drop:
            with stats.lock:
                stats.dropped += 1
            logger.warning("缺秒样例: ts=%d 不产出（实时与历史都没有）", ts)
        else:
            value = temperature_at(ts)
            dv = ua.DataValue(ua.Variant(value, ua.VariantType.Double))
            dv.SourceTimestamp = datetime.fromtimestamp(ts, timezone.utc)
            await var.write_value(dv)
            # 历史与发射同步落库，保证补采读到的历史不滞后
            await storage.save_node_value(node_id, dv)
            with stats.lock:
                stats.emitted += 1
        ts += 1


async def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    chaos = Chaos()
    stats = Stats()

    server = Server()
    await server.init()
    server.set_endpoint(f"opc.tcp://0.0.0.0:{PORT}")
    server.set_server_name("cannery-retort-source")
    idx = await server.register_namespace("urn:cannery:retort")
    obj = await server.nodes.objects.add_object(
        ua.NodeId("Kettle1", idx), ua.QualifiedName("Kettle1", idx)
    )
    var = await obj.add_variable(
        ua.NodeId(NODE_ID, idx), ua.QualifiedName("Temperature", idx), 0.0
    )

    storage = SqliteHistoryStorage(os.path.join(DATA_DIR, "source.db"), chaos, HISTORY_MAX_PAGE)
    await storage.init()
    server.iserver.history_manager.set_storage(storage)
    await server.iserver.enable_history_data_change(var, period=None, count=0)

    start_control_server(chaos, stats)
    await server.start()
    logger.info(
        "OPC UA 源已启动: opc.tcp://0.0.0.0:%d 节点=%s (HISTORY_MAX_PAGE=%d)",
        PORT, var.nodeid.to_string(), HISTORY_MAX_PAGE,
    )
    try:
        await emit_loop(var, var.nodeid, storage, chaos, stats)
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
