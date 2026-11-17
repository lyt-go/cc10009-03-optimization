"""采集节点接口：维护工作节点，并接入心跳上报、人工状态变更（含批量）。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.worker import MANUAL_OPTIONS, worker_service

router = APIRouter(prefix="/api/worker", tags=["采集节点"])

LIST_FIELDS = ["节点编号", "主机地址", "并发槽位数", "已用槽位数", "地域标签", "CPU使用率", "内存可用MB", "上次心跳时间", "节点状态标签", "人工状态"]
STATUSES = ["草稿", "注册中", "在线", "离线"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按节点编号或主机地址检索"),
    status: str | None = Query(default=None, description="草稿、注册中、在线、离线"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按节点编号与状态过滤采集节点列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = worker_service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/manual-options")
def manual_options() -> dict[str, Any]:
    """人工状态可选项，供前端下拉与批量操作共用同一份口径。"""
    return {"options": MANUAL_OPTIONS}


@router.post("/batch/manual-status")
def batch_manual_status(payload: EntryPayload) -> dict[str, Any]:
    """批量人工状态变更：同一节点重复提交只处理一次，返回成功与失败明细便于核对。"""
    raw_ids = payload.values.get("entry_ids") or []
    if not isinstance(raw_ids, list) or not raw_ids:
        raise HTTPException(status_code=400, detail="entry_ids 必须是非空数组")
    try:
        entry_ids = [int(i) for i in raw_ids]
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="entry_ids 只接受节点 id 数字")
    manual_status = str(payload.values.get("manual_status") or "").strip()
    operator = str(payload.values.get("operator") or "值班管理员").strip()
    updated, failures = worker_service.batch_set_manual_status(entry_ids, manual_status, operator=operator)
    return {
        "ok": not failures,
        "message": f"批量变更完成：成功 {len(updated)} 个，失败 {len(failures)} 个",
        "updated_count": len(updated),
        "failed": failures,
        "items": updated,
    }


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出采集节点清单：返回当前过滤条件下的全量数据。"""
    items, total = worker_service.list_entries(page=1, size=10000)
    return {"module": "worker", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条工作节点明细；不存在时给出可读的错误说明。"""
    entry = worker_service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"工作节点 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条工作节点，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = worker_service.create_entry(payload.values)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="工作节点已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条工作节点执行提交注册、标记在线、标记离线；不允许的动作会被拦下并说明原因。"""
    action = str(payload.values.get("action") or "").strip()
    entry, message = worker_service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/heartbeat", response_model=ActionResult)
def report_heartbeat(entry_id: int, payload: EntryPayload) -> ActionResult:
    """采集节点心跳上报：刷新心跳时间，可选更新已用槽位、CPU、内存。"""
    entry, message = worker_service.report_heartbeat(entry_id, payload.values)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)


@router.post("/{entry_id}/manual-status", response_model=ActionResult)
def set_manual_status(entry_id: int, payload: EntryPayload) -> ActionResult:
    """人工置位（人工停用/维护中/恢复正常）；冲突处置中人工状态优先级最高。"""
    manual_status = str(payload.values.get("manual_status") or "").strip()
    operator = str(payload.values.get("operator") or "值班管理员").strip()
    entry, message = worker_service.set_manual_status(entry_id, manual_status, operator=operator)
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
