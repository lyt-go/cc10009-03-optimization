"""节点健康统计与可调度节点结果数据集。

一条链路，一个口径：

    心跳 / 资源占用 / 人工状态（三类原始信号）
        -> evaluate_node 统一健康评估（冲突处置保留固定优先级）
        -> aggregate_health 健康统计（版本化、计数有界、可回溯）
        -> generate_snapshot 可调度节点结果（不可变快照、幂等、并发只出一份）
        -> sync_to_dispatch 流量分配侧读取同一版快照并更新域名分流结果

冲突处置优先级（高 -> 低）：人工状态 > 心跳时效 > 资源耗尽。
例如心跳刚过期但人工停用尚未落库的窗口期，以心跳判离线；人工停用落库后，
后续评估一律以人工状态为准，统计侧与请求分配侧读同一份快照，不会各判各的。
"""
from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Any

from app.store import (
    TABLE_DISPATCH_SNAPSHOT,
    TABLE_HEALTH_STATS,
    TABLE_SCHED_SNAPSHOTS,
    TABLE_WORKER_EVENTS,
    store,
)

MODULE = "worker"

# 健康三态：互斥且穷尽，任一节点恰好落入其中一个桶。
HEALTH_ONLINE = "在线"
HEALTH_OFFLINE = "离线"
HEALTH_EXHAUSTED = "资源耗尽"
HEALTH_STATES = [HEALTH_ONLINE, HEALTH_OFFLINE, HEALTH_EXHAUSTED]

# 人工状态（最高优先级信号）。
MANUAL_STATES = {"人工停用", "维护中"}
MANUAL_NORMAL = "正常"

# 评估阈值：心跳 90 秒未上报视为过期；CPU 达到 90% 视为过载；可用内存低于 256MB 视为不足。
HEARTBEAT_TIMEOUT_SECONDS = 90
CPU_OVERLOAD_PERCENT = 90
MEMORY_LOW_MB = 256

# 资源耗尽原因按固定顺序判定，一个节点只落一个原因，保证卡片/筛选/明细三处口径一致。
REASON_SLOTS_FULL = "槽位占满"
REASON_CPU_OVERLOAD = "CPU过载"
REASON_MEMORY_LOW = "内存不足"
REASON_HEARTBEAT_STALE = "心跳过期"
EXHAUSTION_REASONS = [REASON_SLOTS_FULL, REASON_CPU_OVERLOAD, REASON_MEMORY_LOW]

# 节点事件类型。
EVENT_HEARTBEAT = "心跳上报"
EVENT_MANUAL_CHANGE = "人工状态变更"
EVENT_ENTER_POOL = "节点入集"
EVENT_LEAVE_POOL = "节点出集"
EVENT_REENTER_POOL = "节点重新入集"
EVENT_HEALTH_CHANGE = "健康状态变化"
EVENT_SNAPSHOT_SYNC = "快照同步分流"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S")


def _parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _parse_time(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).strip().replace("Z", "")
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


class NodeHealthService:
    """健康评估、统计聚合、可调度快照与分流同步的统一入口。"""

    def __init__(self) -> None:
        # 快照生成的并发闸门：配合 store.lock 使用，保证并发上报只出一份结果。
        # 两道防线：
        # 1) 生成轮次屏障——同时在方法内排队的请求等当前生成结束后复用同一版；
        # 2) 合并时间窗——线程池延迟创建等原因导致“错峰到达”的请求，在窗口内也复用最新版，
        #    避免屏障复位后被当成新一轮并发而重复生成。
        self._condition = threading.Condition()
        self._generation = 0          # 已完成的生成轮次
        self._generating = False      # 是否有生成正在进行
        self._last_finish_ts = 0.0    # 最近一次生成结束的 monotonic 时间
        self._coalesce_seconds = 0.15
        # 信号版本：心跳、人工状态等原始信号每次写入都 +1。
        # 合并窗口只在“自上次生成以来信号没有变化”时回放，信号一旦变化立即生成新版本，
        # 既合并并发请求，又不会把节点恢复后的新结果错误地合并成旧版本。
        self._signal_version = 0
        self._last_snapshot_signal_version = -1
        # 曾经进入过可调度节点结果的节点 id：用于把“重新入集”和“首次入集”区分开，
        # 不依赖统计版本里的上一状态，避免快照与聚合交错时重新入集记录丢失。
        self._ever_in_pool: set[Any] = set()
        # 每个节点最近一次观测到的健康状态：信号每次写入即时比对，
        # 不再依赖“两次聚合之间”的版本差，人工变更后即使不立即聚合也不丢迁移事件。
        self._last_health: dict[Any, str] = {}

    def record_transitions(self, *, operator: str = "系统") -> list[dict[str, Any]]:
        """按当前信号重评全部节点，与最近一次观测状态比对并写入迁移事件。

        人工状态变更、心跳上报等信号写入后立即调用，保证事件节奏跟信号一致；
        统计聚合只负责版本化计数，不再承担事件检测。
        """
        events: list[dict[str, Any]] = []
        with store.lock:
            items = self._evaluate_all()
            for item in items:
                node_id = item["id"]
                node_code = str(item["节点编号"])
                current = item["健康状态"]
                old = self._last_health.get(node_id)
                if old is None:
                    # 首次观测：在线节点记首次入集并建立基线。
                    if current == HEALTH_ONLINE:
                        events.append(self._record_event(
                            node_code, EVENT_ENTER_POOL,
                            f"首次评估即在线，进入可调度节点结果：{item['评估依据'][0]}", operator=operator,
                        ))
                        self._ever_in_pool.add(node_id)
                elif old != current:
                    events.append(self._record_event(
                        node_code, EVENT_HEALTH_CHANGE,
                        f"{old} -> {current}（原因：{item['不可调度原因'] or '恢复正常'}）", operator=operator,
                    ))
                    if current == HEALTH_ONLINE:
                        if node_id in self._ever_in_pool:
                            events.append(self._record_event(
                                node_code, EVENT_REENTER_POOL,
                                f"节点恢复在线后重新入集：{item['评估依据'][0]}", operator=operator,
                            ))
                        else:
                            events.append(self._record_event(
                                node_code, EVENT_ENTER_POOL, "首次进入可调度节点结果", operator=operator,
                            ))
                            self._ever_in_pool.add(node_id)
                    else:
                        events.append(self._record_event(
                            node_code, EVENT_LEAVE_POOL,
                            f"节点退出可调度节点结果，原因：{item['不可调度原因']}", operator=operator,
                        ))
                self._last_health[node_id] = current
        return events

    def bump_signal_version(self) -> None:
        """原始信号（心跳、人工状态、资源占用）写入后调用，标记信号已变化。"""
        with self._condition:
            self._signal_version += 1
            self._condition.notify_all()

    def _finish_generation(self) -> None:
        with self._condition:
            self._generating = False
            self._generation += 1
            self._last_finish_ts = time.monotonic()
            self._condition.notify_all()

    # ------------------------------------------------------------------ 信号评估

    def evaluate_node(self, row: dict[str, Any], *, now: datetime | None = None) -> dict[str, Any]:
        """对单个节点做一次健康评估，输出口径固定的健康记录。

        输出同时给出结论（健康状态/可调度/原因）和依据（心跳年龄、阈值、优先级说明），
        让每条统计都能回溯到原始信号。
        """
        now = now or datetime.now()
        manual = str(row.get("人工状态") or MANUAL_NORMAL).strip() or MANUAL_NORMAL

        total_slots = max(_parse_int(row.get("并发槽位数")), 0)
        used_slots = min(max(_parse_int(row.get("已用槽位数")), 0), total_slots) if total_slots else 0
        free_slots = max(total_slots - used_slots, 0)
        slot_usage = round(used_slots / total_slots, 4) if total_slots else 0.0
        cpu = max(0, min(_parse_int(row.get("CPU使用率")), 100))
        memory_mb = max(_parse_int(row.get("内存可用MB")), 0)

        hb_time = _parse_time(row.get("上次心跳时间"))
        age = int((now - hb_time).total_seconds()) if hb_time else None
        heartbeat_stale = age is None or age > HEARTBEAT_TIMEOUT_SECONDS

        # 资源耗尽原因（固定顺序，只取第一个命中的）。
        exhaustion_reason = ""
        if total_slots and used_slots >= total_slots:
            exhaustion_reason = REASON_SLOTS_FULL
        elif cpu >= CPU_OVERLOAD_PERCENT:
            exhaustion_reason = REASON_CPU_OVERLOAD
        elif memory_mb <= MEMORY_LOW_MB:
            exhaustion_reason = REASON_MEMORY_LOW

        evidence: list[str] = []
        # 冲突处置保留优先级：人工状态 > 心跳时效 > 资源耗尽。
        if manual in MANUAL_STATES:
            health, reason, schedulable = HEALTH_OFFLINE, manual, False
            evidence.append(f"人工状态={manual}，优先级最高，直接判离线")
        elif heartbeat_stale:
            health, reason, schedulable = HEALTH_OFFLINE, REASON_HEARTBEAT_STALE, False
            age_text = f"{age}秒" if age is not None else "无法解析心跳时间"
            evidence.append(f"心跳年龄{age_text}，超过{HEARTBEAT_TIMEOUT_SECONDS}秒阈值")
        elif exhaustion_reason:
            health, reason, schedulable = HEALTH_EXHAUSTED, exhaustion_reason, False
            evidence.append(f"资源信号命中耗尽规则：{exhaustion_reason}")
        else:
            health, reason, schedulable = HEALTH_ONLINE, "", True
            evidence.append("心跳在有效期内且未命中任何资源耗尽规则，判在线可调度")

        evidence.append(
            f"槽位 {used_slots}/{total_slots}（空闲{free_slots}）、CPU {cpu}%、内存 {memory_mb}MB"
        )
        return {
            "id": row.get("id"),
            "节点编号": row.get("节点编号"),
            "主机地址": row.get("主机地址"),
            "地域标签": row.get("地域标签") or "未标注",
            "生命周期状态": row.get("status"),
            "人工状态": manual,
            "上次心跳时间": row.get("上次心跳时间"),
            "心跳年龄秒": age,
            "心跳过期": heartbeat_stale,
            "并发槽位数": total_slots,
            "已用槽位数": used_slots,
            "空闲槽位数": free_slots,
            "槽位使用率": slot_usage,
            "CPU使用率": cpu,
            "内存可用MB": memory_mb,
            "健康状态": health,
            "不可调度原因": reason,
            "可调度": schedulable,
            "评估时间": _now_text(),
            "评估依据": evidence,
        }

    def _evaluate_all(self, *, now: datetime | None = None) -> list[dict[str, Any]]:
        # 按 id 去重后再评估：同一节点即使在清单里被重复登记，也不会被重复计数。
        unique: dict[int, dict[str, Any]] = {}
        for row in store.rows(MODULE):
            unique.setdefault(int(row.get("id", 0)), row)
        return [self.evaluate_node(row, now=now) for row in sorted(unique.values(), key=lambda r: int(r.get("id", 0)))]

    # ------------------------------------------------------------------ 筛选口径

    def list_health(
        self,
        *,
        keyword: str | None = None,
        health_status: str | None = None,
        exhaustion_reason: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        """健康记录筛选。筛选字段、清除筛选后的总数与资源耗尽原因共用同一套口径。"""
        items = self._evaluate_all()
        if keyword:
            items = [
                item for item in items
                if keyword in str(item["节点编号"]) or keyword in str(item["主机地址"])
            ]
        if health_status:
            items = [item for item in items if item["健康状态"] == health_status]
        if exhaustion_reason:
            items = [item for item in items if item["不可调度原因"] == exhaustion_reason]
        total = len(items)  # 先全量过滤算总数，再分页，清除筛选即回到节点总数。
        start = max(page - 1, 0) * size
        return items[start:start + size], total

    # ------------------------------------------------------------------ 统计聚合

    @staticmethod
    def _count_health(items: list[dict[str, Any]]) -> dict[str, int]:
        total = len(items)
        counts = {HEALTH_ONLINE: 0, HEALTH_OFFLINE: 0, HEALTH_EXHAUSTED: 0}
        for item in items:
            counts[item["健康状态"]] += 1
        # 批量状态变更期间也不允许出现负数或超过节点总数：显式钳制并核对三态之和。
        counts = {key: max(0, min(value, total)) for key, value in counts.items()}
        if sum(counts.values()) != total:
            raise RuntimeError("健康统计三态之和与节点总数不一致，聚合已中止以保护口径")
        reason_counts = {reason: 0 for reason in EXHAUSTION_REASONS}
        reason_counts[REASON_HEARTBEAT_STALE] = 0
        for item in items:
            reason = item["不可调度原因"]
            if reason in reason_counts:
                reason_counts[reason] += 1
        schedulable = counts[HEALTH_ONLINE]
        return {
            "节点总数": total,
            "在线数": counts[HEALTH_ONLINE],
            "离线数": counts[HEALTH_OFFLINE],
            "资源耗尽数": counts[HEALTH_EXHAUSTED],
            "可调度节点数": schedulable,
            "原因计数": reason_counts,
        }

    def health_overview(self) -> dict[str, Any]:
        """采集节点概览：在线、离线、资源耗尽卡片数据，数量可对照节点清单逐项核对。"""
        items = self._evaluate_all()
        counts = self._count_health(items)
        return {
            "cards": [
                {"label": "节点总数", "value": counts["节点总数"]},
                {"label": "在线节点", "value": counts["在线数"]},
                {"label": "离线节点", "value": counts["离线数"]},
                {"label": "资源耗尽", "value": counts["资源耗尽数"]},
                {"label": "可调度节点", "value": counts["可调度节点数"]},
            ],
            "reason_cards": [
                {"label": REASON_HEARTBEAT_STALE, "value": counts["原因计数"][REASON_HEARTBEAT_STALE]},
                *[
                    {"label": reason, "value": counts["原因计数"][reason]}
                    for reason in EXHAUSTION_REASONS
                ],
            ],
            "评估时间": _now_text(),
            "核对": {
                "三态之和等于总数": (
                    counts["在线数"] + counts["离线数"] + counts["资源耗尽数"] == counts["节点总数"]
                ),
                "计数均在节点总数内": all(
                    0 <= counts[key] <= counts["节点总数"]
                    for key in ("在线数", "离线数", "资源耗尽数", "可调度节点数")
                ),
            },
        }

    def _latest_stats(self) -> dict[str, Any] | None:
        rows = store.rows(TABLE_HEALTH_STATS)
        return rows[-1] if rows else None

    def _ensure_baseline(self, items: list[dict[str, Any]] | None = None) -> None:
        """服务首次使用时落一版基线统计，后续每次信号变更才能检测为状态迁移并写事件。

        基线只在健康统计表为空时创建一次；它本身不产生节点事件，避免污染审计流。
        """
        if store.rows(TABLE_HEALTH_STATS):
            return
        items = items if items is not None else self._evaluate_all()
        counts = self._count_health(items)
        record = {
            "统计版本": f"HS-{len(store.rows(TABLE_HEALTH_STATS)) + 1:04d}",
            "生成时间": _now_text(),
            "触发方式": "系统基线",
            "操作人": "系统",
            **counts,
            "节点状态": {item["id"]: item["健康状态"] for item in items},
            "items": items,
            "核对": {
                "三态之和等于总数": (
                    counts["在线数"] + counts["离线数"] + counts["资源耗尽数"] == counts["节点总数"]
                ),
                "去重节点数": len({item["id"] for item in items}),
                "计数均在节点总数内": all(
                    0 <= counts[key] <= counts["节点总数"]
                    for key in ("在线数", "离线数", "资源耗尽数", "可调度节点数")
                ),
            },
        }
        store.insert(TABLE_HEALTH_STATS, record)
        # 基线中各节点的健康状态作为迁移观测起点；在线节点视为已经入集，
        # 之后离线再恢复即可正确记为“重新入集”。
        for item in items:
            self._last_health[item["id"]] = item["健康状态"]
            if item["健康状态"] == HEALTH_ONLINE:
                self._ever_in_pool.add(item["id"])

    def _record_event(
        self,
        node_code: str,
        event_type: str,
        detail: str,
        *,
        operator: str = "系统",
        snapshot_version: str | None = None,
    ) -> dict[str, Any]:
        event = {
            "时间": _now_text(),
            "节点编号": node_code,
            "事件类型": event_type,
            "详情": detail,
            "操作人": operator,
            "关联快照版本": snapshot_version,
        }
        return store.insert(TABLE_WORKER_EVENTS, event)

    def list_events(self, *, node_code: str | None = None, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(TABLE_WORKER_EVENTS))
        if node_code:
            rows = [row for row in rows if row.get("节点编号") == node_code]
        rows.reverse()  # 最新事件在前
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def aggregate_health(self, *, operator: str = "系统", trigger: str = "手动聚合") -> dict[str, Any]:
        """聚合一次健康统计并落一条版本化记录。

        高频刷新只读取最新记录，不会把旧统计写回：每次聚合都从当前信号重新评估，
        历史记录只追加、不改写。迁移事件由信号写入点即时记录，聚合只负责版本化计数。
        """
        with store.lock:
            items = self._evaluate_all()
            counts = self._count_health(items)
            self._ensure_baseline(items)
            # 心跳随时间自然过期这类“无写入动作”的状态变化，也借聚合节奏补一次迁移检测；
            # 无变化时 record_transitions 不写任何事件，因此高频聚合不会产生重复事件。
            self.record_transitions(operator=operator)

            stats_version = f"HS-{len(store.rows(TABLE_HEALTH_STATS)) + 1:04d}"
            record = {
                "统计版本": stats_version,
                "生成时间": _now_text(),
                "触发方式": trigger,
                "操作人": operator,
                **counts,
                "节点状态": {item["id"]: item["健康状态"] for item in items},
                "items": items,
                "核对": {
                    "三态之和等于总数": (
                        counts["在线数"] + counts["离线数"] + counts["资源耗尽数"] == counts["节点总数"]
                    ),
                    "去重节点数": len({item["id"] for item in items}),
                    "计数均在节点总数内": all(
                        0 <= counts[key] <= counts["节点总数"]
                        for key in ("在线数", "离线数", "资源耗尽数", "可调度节点数")
                    ),
                },
            }
            store.insert(TABLE_HEALTH_STATS, record)
            return record

    def list_stats_history(self, *, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(TABLE_HEALTH_STATS))
        rows.reverse()
        total = len(rows)
        start = max(page - 1, 0) * size
        # 列表不回传全量 items，避免高频刷新拉取过大；明细走单版本回溯接口。
        slim = [{k: v for k, v in row.items() if k != "items"} for row in rows]
        return slim[start:start + size], total

    def get_stats(self, stats_id: int) -> dict[str, Any] | None:
        return store.find(TABLE_HEALTH_STATS, stats_id)

    # ------------------------------------------------------------------ 可调度快照

    @staticmethod
    def _snapshot_rows() -> list[dict[str, Any]]:
        return store.rows(TABLE_SCHED_SNAPSHOTS)

    def _find_by_idempotency_key(self, key: str) -> dict[str, Any] | None:
        for row in self._snapshot_rows():
            if row.get("幂等键") == key:
                return row
        return None

    def latest_snapshot(self) -> dict[str, Any] | None:
        rows = self._snapshot_rows()
        return rows[-1] if rows else None

    def generate_snapshot(self, *, idempotency_key: str | None = None, operator: str = "系统") -> tuple[dict[str, Any], bool]:
        """生成一版不可变的可调度节点结果。

        - 同一幂等键再次提交：直接回放上一版结果，不产生新版本（幂等）。
        - 采集节点并发上报：并发窗口内只放行一个生成动作，其余请求在屏障处等待，
          生成结束后统一复用这一版结果，不会出现版本相互覆盖的两组记录。
        """
        idempotency_key = (idempotency_key or "").strip() or None

        with self._condition:
            if idempotency_key:
                existing = self._find_by_idempotency_key(idempotency_key)
                if existing:
                    return existing, True
            # 合并时间窗：上一轮刚结束且“信号没有新变化”时错峰到达的并发请求，直接复用最新版本；
            # 若信号版本已推进（如节点刚恢复心跳），则必须生成反映新信号的新版本。
            if (
                not self._generating
                and self.latest_snapshot() is not None
                and self._signal_version == self._last_snapshot_signal_version
                and time.monotonic() - self._last_finish_ts < self._coalesce_seconds
            ):
                return self.latest_snapshot(), True
            if self._generating:
                # 屏障：记录入队时的生成轮次，等该轮结束后复用其产物（wait_for 防虚假唤醒）。
                enqueued_generation = self._generation
                self._condition.wait_for(
                    lambda: not self._generating and self._generation > enqueued_generation
                )
                if idempotency_key:
                    existing = self._find_by_idempotency_key(idempotency_key)
                    if existing:
                        return existing, True
                latest = self.latest_snapshot()
                if latest is not None:
                    return latest, True
                # 极端情况下上一轮生成失败未留结果，则本线程继续补一次生成。
            self._generating = True

        try:
            with store.lock:
                # 双重检查：拿到 store 锁后再查一次幂等键，覆盖排队期间的写入。
                if idempotency_key:
                    existing = self._find_by_idempotency_key(idempotency_key)
                    if existing:
                        self._finish_generation()
                        return existing, True
                stats = self.aggregate_health(operator=operator, trigger="可调度快照生成")
                items = stats["items"]
                schedulable = [item for item in items if item["可调度"]]
                # 入集结果按节点编号去重，同一节点在一版快照里只出现一次。
                deduped: dict[Any, dict[str, Any]] = {}
                for item in schedulable:
                    deduped.setdefault(item["id"], item)
                pool_items = [
                    {
                        "节点编号": item["节点编号"],
                        "主机地址": item["主机地址"],
                        "地域标签": item["地域标签"],
                        "健康状态": item["健康状态"],
                        "空闲槽位数": item["空闲槽位数"],
                        "并发槽位数": item["并发槽位数"],
                        "CPU使用率": item["CPU使用率"],
                        "内存可用MB": item["内存可用MB"],
                        "心跳年龄秒": item["心跳年龄秒"],
                        "入集依据": "；".join(item["评估依据"]),
                        "评估时间": item["评估时间"],
                    }
                    for item in deduped.values()
                ]

                version = f"SNAP-{len(self._snapshot_rows()) + 1:04d}"
                snapshot = {
                    "版本": version,
                    "生成时间": _now_text(),
                    "幂等键": idempotency_key,
                    "操作人": operator,
                    "来源统计版本": stats["统计版本"],
                    "节点总数": stats["节点总数"],
                    "在线数": stats["在线数"],
                    "离线数": stats["离线数"],
                    "资源耗尽数": stats["资源耗尽数"],
                    "可调度节点数": len(pool_items),
                    "items": pool_items,
                    "状态": "已生成",
                    "核对": {
                        **stats["核对"],
                        "入集节点去重数": len({item["节点编号"] for item in pool_items}),
                        "入集数等于在线数": len(pool_items) == stats["在线数"],
                    },
                }
                store.insert(TABLE_SCHED_SNAPSHOTS, snapshot)
                # 记录本版快照所基于的信号版本，供合并窗口判断信号是否已变化。
                self._last_snapshot_signal_version = self._signal_version
                # 入集节点登记到追踪集；逐节点入集/重新入集事件由信号写入点的
                # record_transitions 统一产生，这里只补一条快照级审计，避免重复计数。
                for item in pool_items:
                    node_id = next((n["id"] for n in stats["items"] if n["节点编号"] == item["节点编号"]), None)
                    self._ever_in_pool.add(node_id)
                return snapshot, False
        finally:
            self._finish_generation()

    def list_snapshots(self, *, page: int = 1, size: int = 20) -> tuple[list[dict[str, Any]], int]:
        rows = list(self._snapshot_rows())
        rows.reverse()
        total = len(rows)
        start = max(page - 1, 0) * size
        slim = [{k: v for k, v in row.items() if k != "items"} for row in rows]
        return slim[start:start + size], total

    def get_snapshot(self, snapshot_id: int) -> dict[str, Any] | None:
        return store.find(TABLE_SCHED_SNAPSHOTS, snapshot_id)

    # ------------------------------------------------------------------ 分流同步

    def sync_to_dispatch(self, *, operator: str = "系统") -> tuple[dict[str, Any] | None, str]:
        """把最新一版可调度节点结果同步到流量分配侧清单。

        所有目标域名读取同一版快照，按节点亲和性（地域标签）过滤后更新分流结果，
        并把快照版本写到每条分配策略上，避免两组版本记录相互覆盖。
        """
        with store.lock:
            snapshot = self.latest_snapshot()
            if snapshot is None:
                return None, "尚无可调度节点结果，请先生成快照"
            pool = snapshot["items"]
            domain_results: list[dict[str, Any]] = []
            for policy in store.rows("dispatch"):
                affinity = str(policy.get("节点亲和性") or "").strip()
                if affinity and affinity not in ("任意", "全部", "不限"):
                    eligible = [node for node in pool if affinity in str(node["地域标签"]) or str(node["地域标签"]) in affinity]
                else:
                    eligible = list(pool)
                node_codes = [str(node["节点编号"]) for node in eligible]
                policy["分流快照版本"] = snapshot["版本"]
                policy["分流节点数"] = len(node_codes)
                policy["分流节点清单"] = "、".join(node_codes) if node_codes else "（当前版本无可调度节点）"
                policy["分流更新时间"] = _now_text()
                policy["分流状态"] = "已同步"
                domain_results.append({
                    "策略编号": policy.get("策略编号"),
                    "目标域名": policy.get("目标域名"),
                    "节点亲和性": affinity or "任意",
                    "分流节点数": len(node_codes),
                    "分流节点清单": node_codes,
                })

            record = {
                "快照版本": snapshot["版本"],
                "来源快照id": snapshot.get("id"),
                "同步时间": _now_text(),
                "操作人": operator,
                "可调度节点数": snapshot["可调度节点数"],
                "域名分流结果数": len(domain_results),
                "域名结果": domain_results,
                "状态": "已同步",
            }
            # 流量分配侧清单只保留当前版本：整表替换，下游永远读到最新一次同步。
            table = store.rows(TABLE_DISPATCH_SNAPSHOT)
            table.clear()
            store.insert(TABLE_DISPATCH_SNAPSHOT, record)
            self._record_event(
                "-", EVENT_SNAPSHOT_SYNC,
                f"快照 {snapshot['版本']} 已同步到流量分配侧，覆盖 {len(domain_results)} 个域名分流结果",
                operator=operator, snapshot_version=snapshot["版本"],
            )
            return record, ""

    def get_dispatch_snapshot(self) -> dict[str, Any] | None:
        rows = store.rows(TABLE_DISPATCH_SNAPSHOT)
        return rows[-1] if rows else None


node_health_service = NodeHealthService()

# 模块加载即落基线统计并建立健康状态观测起点：
# 后续心跳、人工状态、资源信号的任何变化都能对照基线即时检测为迁移。
node_health_service._ensure_baseline()
