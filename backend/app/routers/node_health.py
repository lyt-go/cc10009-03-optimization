"""节点健康统计与可调度节点结果接口。

同一条链路上的读口与写口：
- 概览卡片、健康记录筛选、健康统计历史/单版本回溯、节点事件审计
- 可调度节点结果生成（幂等键 + 并发只出一份）、版本列表/单版本回溯
- 把指定版本快照同步到流量分配侧清单
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import EntryPayload, PageResult
from app.services.node_health import (
    EXHAUSTION_REASONS,
    HEALTH_STATES,
    node_health_service,
)

router = APIRouter(prefix="/api/node-health", tags=["节点健康"])


@router.get("/overview")
def health_overview() -> dict[str, Any]:
    """采集节点概览：在线、离线、资源耗尽数量卡片，附口径核对结果。"""
    return node_health_service.health_overview()


@router.get("/records", response_model=PageResult[dict])
def health_records(
    keyword: str | None = Query(default=None, description="按节点编号或主机地址检索"),
    health_status: str | None = Query(default=None, description="在线、离线、资源耗尽"),
    exhaustion_reason: str | None = Query(default=None, description="槽位占满、CPU过载、内存不足、心跳过期"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """健康记录列表：筛选字段、清除筛选后的总数与资源耗尽原因使用同一套评估口径。"""
    if health_status and health_status not in HEALTH_STATES:
        raise HTTPException(status_code=400, detail=f"健康状态只支持：{'、'.join(HEALTH_STATES)}")
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = node_health_service.list_health(
        keyword=keyword,
        health_status=health_status,
        exhaustion_reason=exhaustion_reason,
        page=page,
        size=size,
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/reasons")
def exhaustion_reasons() -> dict[str, Any]:
    """资源耗尽原因字典：前端筛选项、卡片与明细共用，避免原因文案漂移。"""
    return {"reasons": EXHAUSTION_REASONS}


@router.post("/aggregate")
def aggregate_health(payload: EntryPayload | None = None) -> dict[str, Any]:
    """手动聚合一版健康统计并落库；每次都从当前信号重算，旧统计只保留为历史不会被写回。"""
    operator = str((payload.values.get("operator") if payload else None) or "值班管理员")
    trigger = str((payload.values.get("trigger") if payload else None) or "手动聚合")
    return node_health_service.aggregate_health(operator=operator, trigger=trigger)


@router.get("/stats")
def stats_history(page: int = 1, size: int = 20) -> dict[str, Any]:
    """健康统计版本历史，支持按记录回溯（单版本明细走 /stats/{id}）。"""
    items, total = node_health_service.list_stats_history(page=page, size=size)
    return {"items": items, "total": total, "page": page, "size": size}


@router.get("/stats/{stats_id}")
def stats_detail(stats_id: int) -> dict[str, Any]:
    """回溯某一版健康统计的完整记录（含每个节点的评估依据）。"""
    record = node_health_service.get_stats(stats_id)
    if record is None:
        raise HTTPException(status_code=404, detail=f"健康统计记录 {stats_id} 不存在")
    return record


@router.post("/snapshots")
def generate_snapshot(payload: EntryPayload | None = None) -> dict[str, Any]:
    """生成可调度节点结果。

    - 传 idempotency_key 时，同键重复提交直接回放原版本（幂等）；
    - 采集节点并发上报时只放行一个生成动作，其余请求复用同一版结果。
    """
    values = payload.values if payload else {}
    idempotency_key = str(values.get("idempotency_key") or "").strip() or None
    operator = str(values.get("operator") or "值班管理员").strip()
    snapshot, replayed = node_health_service.generate_snapshot(
        idempotency_key=idempotency_key, operator=operator
    )
    return {"ok": True, "replayed": replayed, "message": "命中幂等键，回放上一版结果" if replayed else "可调度节点结果已生成", "snapshot": snapshot}


@router.get("/snapshots")
def snapshot_history(page: int = 1, size: int = 20) -> dict[str, Any]:
    """可调度节点结果版本列表（不含全量入集明细，明细走 /snapshots/{id}）。"""
    items, total = node_health_service.list_snapshots(page=page, size=size)
    return {"items": items, "total": total, "page": page, "size": size}


@router.get("/snapshots/latest")
def latest_snapshot() -> dict[str, Any]:
    """读取当前最新一版可调度节点结果；尚未生成时明确返回空态而不是报错。"""
    snapshot = node_health_service.latest_snapshot()
    return {"snapshot": snapshot}


@router.get("/snapshots/{snapshot_id}")
def snapshot_detail(snapshot_id: int) -> dict[str, Any]:
    """回溯某一版可调度节点结果的完整入集记录与核对信息。"""
    snapshot = node_health_service.get_snapshot(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"可调度节点结果 {snapshot_id} 不存在")
    return snapshot


@router.post("/snapshots/sync-dispatch")
def sync_dispatch(payload: EntryPayload | None = None) -> dict[str, Any]:
    """把最新一版可调度节点结果同步到流量分配侧清单，并更新各目标域名分流结果。"""
    operator = str((payload.values.get("operator") if payload else None) or "值班管理员")
    record, message = node_health_service.sync_to_dispatch(operator=operator)
    if record is None:
        raise HTTPException(status_code=409, detail=message)
    return {"ok": True, "message": "快照已同步到流量分配侧清单", "sync": record}


@router.get("/events")
def node_events(
    node_code: str | None = Query(default=None, description="按节点编号过滤事件"),
    page: int = 1,
    size: int = 20,
) -> dict[str, Any]:
    """节点事件审计流：心跳、人工变更、入集/出集、重新入集、快照同步均可按记录回溯。"""
    items, total = node_health_service.list_events(node_code=node_code, page=page, size=size)
    return {"items": items, "total": total, "page": page, "size": size}
