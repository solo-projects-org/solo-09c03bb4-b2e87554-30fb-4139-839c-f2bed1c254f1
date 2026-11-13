"""运行配置：全部通过环境变量覆盖，方便在 docker-compose 中调整。"""
from __future__ import annotations

import os


class Config:
    def __init__(self) -> None:
        # OPC UA 源地址
        self.endpoint: str = os.getenv("OPCUA_ENDPOINT", "opc.tcp://127.0.0.1:4840")
        # 数据卷目录，SQLite 库文件放在这里
        self.data_dir: str = os.getenv("DATA_DIR", "./data")
        # 补采循环间隔（秒）
        self.backfill_interval: float = float(os.getenv("BACKFILL_INTERVAL", "2.0"))
        # Historical Access 每页条数（NumValuesPerNode），用于演示翻页
        self.history_page_size: int = int(os.getenv("HISTORY_PAGE_SIZE", "50"))
        # 单个补采段最大秒数，超出会切块，避免一次事务过大；
        # 慢翻页演示时也能看到 pending_seconds 随各段提交阶梯式下降
        self.max_segment_seconds: int = int(os.getenv("MAX_SEGMENT_SECONDS", "300"))
        # 实时订阅发布周期（毫秒）
        self.realtime_period_ms: float = float(os.getenv("REALTIME_PERIOD_MS", "500"))
        # OPC UA 请求超时（秒）
        self.request_timeout: float = float(os.getenv("REQUEST_TIMEOUT", "10"))
        # 实时连接看门狗间隔（秒）
        self.watchdog_interval: float = float(os.getenv("WATCHDOG_INTERVAL", "2.0"))

    @property
    def db_path(self) -> str:
        return os.path.join(self.data_dir, "collector.db")
