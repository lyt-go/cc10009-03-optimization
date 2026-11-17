"""采集节点业务规则：状态流转、字段校验与筛选口径都收在这里。

节点的心跳上报与人工状态变更只在本服务落库，并统一写入节点事件审计流；
健康结论不在这里计算，一律由 node_health.NodeHealthService 按固定优先级评估，
保证节点清单、健康统计与请求分配使用同一个可用性口径。
"""
from __future__ import annotations

from typing import Any

from app.services.node_health import (
    EVENT_HEARTBEAT,
    EVENT_MANUAL_CHANGE,
    MANUAL_NORMAL,
    MANUAL_STATES,
    node_health_service,
)
from app.store import store

MODULE = "worker"
REQUIRED_FIELDS = ["节点编号", "主机地址", "并发槽位数"]
STATUS_ORDER = ["草稿", "注册中", "在线", "离线"]
ACTION_RULES = {"提交注册": "注册中", "标记在线": "在线", "标记离线": "离线"}
NEGATIVE_ACTIONS = []

# 允许人工设置的状态：正常（恢复参与调度）/ 人工停用 / 维护中（后两者最高优先级判离线）。
MANUAL_OPTIONS = [MANUAL_NORMAL, *sorted(MANUAL_STATES)]


class WorkerService:
    def list_entries(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("节点编号", "")) or keyword in str(row.get("主机地址", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        with store.lock:
            entry = {"id": store.next_id(MODULE)}
            entry.update({field: values.get(field) for field in REQUIRED_FIELDS})
            entry["status"] = STATUS_ORDER[0]
            entry["pending"] = True
            entry["abnormal"] = False
            entry.setdefault("人工状态", MANUAL_NORMAL)
            entry.setdefault("已用槽位数", "0")
            entry.setdefault("CPU使用率", "0")
            entry.setdefault("内存可用MB", "0")
            entry.setdefault("上次心跳时间", "")
            entry.setdefault("节点状态标签", "新登记")
            store.rows(MODULE).append(entry)
            node_health_service._record_event(
                str(entry["节点编号"]), "节点登记", f"工作节点 {entry['节点编号']} 已登记，状态={entry['status']}"
            )
            return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        with store.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"工作节点 {entry_id} 不存在或已归档"
            if action not in ACTION_RULES:
                return None, f"动作「{action}」不属于采集节点可执行范围"
            target = ACTION_RULES[action]
            if target not in STATUS_ORDER:
                return None, f"目标状态「{target}」不在允许的状态序列里"
            entry["status"] = target
            entry["pending"] = target != STATUS_ORDER[-1]
            entry["abnormal"] = action in NEGATIVE_ACTIONS
            return entry, f"工作节点已{action}"

    # ------------------------------------------------------------------ 信号上报

    def report_heartbeat(self, entry_id: int, values: dict[str, Any]) -> tuple[dict[str, Any] | None, str]:
        """采集节点心跳上报：刷新心跳时间与资源占用，可附带槽位使用情况。"""
        with store.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"工作节点 {entry_id} 不存在或已归档"
            from app.services.node_health import _now_text

            entry["上次心跳时间"] = _now_text()
            for field in ("已用槽位数", "CPU使用率", "内存可用MB"):
                if str(values.get(field) or "").strip() != "":
                    entry[field] = str(values.get(field)).strip()
            if entry["status"] == "离线" and entry.get("人工状态", MANUAL_NORMAL) == MANUAL_NORMAL:
                entry["status"] = "在线"
                entry["abnormal"] = False
            node_health_service._record_event(
                str(entry["节点编号"]), EVENT_HEARTBEAT,
                f"心跳上报：已用槽位 {entry.get('已用槽位数')}/{entry.get('并发槽位数')}，"
                f"CPU {entry.get('CPU使用率')}%，内存 {entry.get('内存可用MB')}MB",
            )
            # 心跳可能让节点从离线/耗尽恢复在线：即时检测迁移并写重新入集事件。
            node_health_service.bump_signal_version()
            node_health_service.record_transitions()
            return entry, "心跳已接收"

    def set_manual_status(
        self, entry_id: int, manual_status: str, *, operator: str = "值班管理员"
    ) -> tuple[dict[str, Any] | None, str]:
        """人工置位：人工停用 / 维护中 / 恢复正常。落库即对后续健康评估生效。"""
        if manual_status not in MANUAL_OPTIONS:
            return None, f"人工状态「{manual_status}」不在允许范围：{'、'.join(MANUAL_OPTIONS)}"
        with store.lock:
            entry = store.find(MODULE, entry_id)
            if entry is None:
                return None, f"工作节点 {entry_id} 不存在或已归档"
            old = entry.get("人工状态", MANUAL_NORMAL)
            if old == manual_status:
                return entry, f"人工状态已是「{manual_status}」，无需重复变更"
            entry["人工状态"] = manual_status
            entry["节点状态标签"] = manual_status if manual_status != MANUAL_NORMAL else "正常"
            if manual_status in MANUAL_STATES:
                entry["status"] = "离线"
                entry["abnormal"] = True
            else:
                entry["abnormal"] = False
            node_health_service._record_event(
                str(entry["节点编号"]), EVENT_MANUAL_CHANGE,
                f"人工状态 {old} -> {manual_status}（操作人：{operator}）", operator=operator,
            )
            # 人工状态是最高优先级信号，落库后立即重评，出集/重新入集事件当场可回溯。
            node_health_service.bump_signal_version()
            node_health_service.record_transitions(operator=operator)
            return entry, f"人工状态已变更为「{manual_status}」"

    def batch_set_manual_status(
        self, entry_ids: list[int], manual_status: str, *, operator: str = "值班管理员"
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """批量人工状态变更：逐节点处理并计数，整体在同一把锁内完成，统计不会出现负数或越界。"""
        if manual_status not in MANUAL_OPTIONS:
            return [], [f"人工状态「{manual_status}」不在允许范围：{'、'.join(MANUAL_OPTIONS)}"]
        # 去重：同一节点在一次批量请求里即使出现多次，也只处理一次，不重复计数。
        unique_ids = list(dict.fromkeys(int(i) for i in entry_ids))
        updated, failures = [], []
        with store.lock:
            for entry_id in unique_ids:
                entry, message = self.set_manual_status(entry_id, manual_status, operator=operator)
                if entry is None:
                    failures.append(message)
                else:
                    updated.append(entry)
        return updated, failures


worker_service = WorkerService()
