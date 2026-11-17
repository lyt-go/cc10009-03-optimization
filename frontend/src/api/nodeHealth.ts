/** 节点健康统计与可调度节点结果的接口封装与类型定义。
 *
 * 所有可用性口径都由后端按“人工状态 > 心跳时效 > 资源耗尽”的优先级评估，
 * 前端只做读取与展示，不在浏览器侧重复计算健康状态，避免两侧口径漂移。
 */
import { request } from './client'

export type HealthStatus = '在线' | '离线' | '资源耗尽'

export interface HealthCard {
  label: string
  value: number
}

export interface HealthOverview {
  cards: HealthCard[]
  reason_cards: HealthCard[]
  评估时间: string
  核对: Record<string, boolean>
}

export interface HealthRecord {
  id: number
  节点编号: string
  主机地址: string
  地域标签: string
  生命周期状态: string
  人工状态: string
  上次心跳时间: string | null
  心跳年龄秒: number | null
  心跳过期: boolean
  并发槽位数: number
  已用槽位数: number
  空闲槽位数: number
  槽位使用率: number
  CPU使用率: number
  内存可用MB: number
  健康状态: HealthStatus
  不可调度原因: string
  可调度: boolean
  评估时间: string
  评估依据: string[]
}

export interface PagePayload<T> {
  items: T[]
  total: number
  page: number
  size: number
}

export interface SnapshotPoolItem {
  节点编号: string
  主机地址: string
  地域标签: string
  健康状态: HealthStatus
  空闲槽位数: number
  并发槽位数: number
  CPU使用率: number
  内存可用MB: number
  心跳年龄秒: number | null
  入集依据: string
  评估时间: string
}

export interface SnapshotSummary {
  id: number
  版本: string
  生成时间: string
  幂等键: string | null
  操作人: string
  来源统计版本: string
  节点总数: number
  在线数: number
  离线数: number
  资源耗尽数: number
  可调度节点数: number
  状态: string
  核对: Record<string, boolean | number>
}

export interface SnapshotDetail extends SnapshotSummary {
  items: SnapshotPoolItem[]
}

export interface StatsSummary {
  id: number
  统计版本: string
  生成时间: string
  触发方式: string
  操作人: string
  节点总数: number
  在线数: number
  离线数: number
  资源耗尽数: number
  可调度节点数: number
  原因计数: Record<string, number>
  节点状态: Record<string, HealthStatus>
  核对: Record<string, boolean | number>
}

export interface StatsDetail extends StatsSummary {
  items: HealthRecord[]
}

export interface NodeEvent {
  id: number
  时间: string
  节点编号: string
  事件类型: string
  详情: string
  操作人: string
  关联快照版本: string | null
}

export interface DomainDispatch {
  策略编号: string
  目标域名: string
  节点亲和性: string
  分流节点数: number
  分流节点清单: string[]
}

export interface DispatchSnapshot {
  id: number
  快照版本: string
  来源快照id: number
  同步时间: string
  操作人: string
  可调度节点数: number
  域名分流结果数: number
  域名结果: DomainDispatch[]
  状态: string
}

async function readJson<T>(path: string): Promise<T> {
  const response = await request(path)
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ? String(detail.detail) : `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, { method: 'POST', body: JSON.stringify(body) })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({}))
    throw new Error(detail.detail ? String(detail.detail) : `接口返回 ${response.status}`)
  }
  return (await response.json()) as T
}

export const nodeHealthApi = {
  overview: () => readJson<HealthOverview>('/api/node-health/overview'),
  reasons: () => readJson<{ reasons: string[] }>('/api/node-health/reasons'),
  records: (query: URLSearchParams) => readJson<PagePayload<HealthRecord>>(`/api/node-health/records?${query.toString()}`),
  aggregate: () => postJson<unknown>('/api/node-health/aggregate', { values: { trigger: '页面手动聚合' } }),
  statsList: (query: URLSearchParams) => readJson<PagePayload<StatsSummary>>(`/api/node-health/stats?${query.toString()}`),
  statsDetail: (id: number) => readJson<StatsDetail>(`/api/node-health/stats/${id}`),
  generateSnapshot: (idempotencyKey: string) =>
    postJson<{ ok: boolean; replayed: boolean; message: string; snapshot: SnapshotDetail }>(
      '/api/node-health/snapshots',
      { values: { idempotency_key: idempotencyKey } },
    ),
  snapshotList: (query: URLSearchParams) => readJson<PagePayload<SnapshotSummary>>(`/api/node-health/snapshots?${query.toString()}`),
  snapshotDetail: (id: number) => readJson<SnapshotDetail>(`/api/node-health/snapshots/${id}`),
  latestSnapshot: () => readJson<{ snapshot: SnapshotDetail | null }>('/api/node-health/snapshots/latest'),
  syncDispatch: () =>
    postJson<{ ok: boolean; message: string; sync: DispatchSnapshot }>(
      '/api/node-health/snapshots/sync-dispatch',
      { values: {} },
    ),
  events: (query: URLSearchParams) => readJson<PagePayload<NodeEvent>>(`/api/node-health/events?${query.toString()}`),
  dispatchSnapshot: () => readJson<{ snapshot: DispatchSnapshot | null }>('/api/dispatch/snapshot/current'),
}

export const workerApi = {
  manualOptions: () => readJson<{ options: string[] }>('/api/worker/manual-options'),
  heartbeat: (id: number) =>
    postJson<{ ok: boolean; message: string }>(`/api/worker/${id}/heartbeat`, { values: {} }),
  setManualStatus: (id: number, manual_status: string) =>
    postJson<{ ok: boolean; message: string }>(`/api/worker/${id}/manual-status`, { values: { manual_status } }),
  batchManualStatus: (entry_ids: number[], manual_status: string) =>
    postJson<{ ok: boolean; message: string; updated_count: number; failed: string[] }>(
      '/api/worker/batch/manual-status',
      { values: { entry_ids, manual_status } },
    ),
}
