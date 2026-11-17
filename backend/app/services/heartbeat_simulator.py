"""采集节点心跳模拟器。

仅用于演示环境：带「模拟心跳」标记的活跃节点由后台线程按固定间隔刷新心跳时间，
模拟真实采集节点的持续上报，避免种子心跳随服务运行超过超时阈值后整体滑向离线。

- 只在 FastAPI 生命周期里启动，模块导入与单元测试进程不会拉起线程；
- 只刷新「上次心跳时间」，不改动资源占用与人工状态，因此不会改变健康评估结论；
- 不写节点事件审计流，避免定时任务刷屏，真实心跳仍走 worker 服务的上报接口并留痕。
"""
from __future__ import annotations

import threading
from datetime import datetime

from app.services.node_health import HEARTBEAT_TIMEOUT_SECONDS
from app.store import store

MODULE = "worker"
# 刷新间隔取超时阈值的三分之一，保证刷新两次内不会误判过期。
INTERVAL_SECONDS = max(10, HEARTBEAT_TIMEOUT_SECONDS // 3)


def _refresh_once() -> int:
    refreshed = 0
    now_text = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    with store.lock:
        for row in store.rows(MODULE):
            if row.get("模拟心跳"):
                row["上次心跳时间"] = now_text
                refreshed += 1
    return refreshed


def _run(stop_event: threading.Event) -> None:
    while not stop_event.wait(INTERVAL_SECONDS):
        try:
            _refresh_once()
        except Exception:  # pragma: no cover - 模拟器异常不能拖垮主流程
            pass


def start_heartbeat_simulator() -> threading.Event:
    """启动心跳模拟守护线程，返回停止事件（测试与关闭流程可显式停止）。"""
    stop_event = threading.Event()
    thread = threading.Thread(
        target=_run, args=(stop_event,), name="heartbeat-simulator", daemon=True
    )
    thread.start()
    return stop_event
