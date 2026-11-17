"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

健康统计与可调度节点结果相关的表（worker_* / dispatch_snapshot）也由仓库统一持有，
并通过一把可重入锁保证：采集节点并发上报时，同一批信号只生成一份可调度节点结果，
批量状态变更期间统计口径不会被并发写穿（负数、超过节点总数）。
"""
from __future__ import annotations

import threading
from typing import Any

from app.seed import SEED_ROWS

# 健康链路新增的表名集中在这里，避免各模块各自拼写导致口径漂移。
TABLE_WORKER_EVENTS = "worker_events"            # 节点事件审计流（可按记录回溯）
TABLE_HEALTH_STATS = "worker_health_stats"       # 健康统计快照（每次聚合一行版本化记录）
TABLE_SCHED_SNAPSHOTS = "worker_sched_snapshots"  # 可调度节点结果数据集（不可变版本）
TABLE_DISPATCH_SNAPSHOT = "dispatch_snapshot"     # 流量分配侧清单（只保存当前同步版本）

EXTRA_TABLES = [
    TABLE_WORKER_EVENTS,
    TABLE_HEALTH_STATS,
    TABLE_SCHED_SNAPSHOTS,
    TABLE_DISPATCH_SNAPSHOT,
]


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        for table in EXTRA_TABLES:
            self._tables.setdefault(table, [])
        # 可重入锁：服务层一次业务动作里可能多次取锁（如聚合 + 写快照 + 写事件）。
        self.lock = threading.RLock()

    def module_names(self) -> list[str]:
        return sorted(name for name in self._tables if name not in EXTRA_TABLES)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def next_id(self, module: str) -> int:
        return max((int(row.get("id", 0)) for row in self.rows(module)), default=0) + 1

    def insert(self, module: str, row: dict[str, Any]) -> dict[str, Any]:
        """落库一条记录并补主键；调用方需持有 store.lock。"""
        if "id" not in row:
            row["id"] = self.next_id(module)
        self.rows(module).append(row)
        return row

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()
