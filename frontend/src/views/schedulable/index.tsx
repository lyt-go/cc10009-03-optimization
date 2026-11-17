import { useCallback, useEffect, useState } from 'react'

import {
  nodeHealthApi,
  type DispatchSnapshot,
  type NodeEvent,
  type SnapshotDetail,
  type SnapshotSummary,
  type StatsSummary,
} from '@/api/nodeHealth'

type TabKey = 'snapshots' | 'stats' | 'events' | 'dispatch'

const TABS: { key: TabKey; label: string }[] = [
  { key: 'snapshots', label: '可调度节点结果' },
  { key: 'stats', label: '健康统计历史' },
  { key: 'events', label: '节点事件审计' },
  { key: 'dispatch', label: '流量分配侧清单' },
]

export default function SchedulableNodesView() {
  const [tab, setTab] = useState<TabKey>('snapshots')
  const [snapshots, setSnapshots] = useState<SnapshotSummary[]>([])
  const [snapshotTotal, setSnapshotTotal] = useState(0)
  const [latest, setLatest] = useState<SnapshotDetail | null>(null)
  const [stats, setStats] = useState<StatsSummary[]>([])
  const [statsTotal, setStatsTotal] = useState(0)
  const [events, setEvents] = useState<NodeEvent[]>([])
  const [eventTotal, setEventTotal] = useState(0)
  const [eventNodeFilter, setEventNodeFilter] = useState('')
  const [dispatchSnapshot, setDispatchSnapshot] = useState<DispatchSnapshot | null>(null)
  const [detail, setDetail] = useState<SnapshotDetail | null>(null)
  const [idemKey, setIdemKey] = useState('')
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [busy, setBusy] = useState(false)

  const loadSnapshots = useCallback(async () => {
    try {
      const [listPayload, latestPayload] = await Promise.all([
        nodeHealthApi.snapshotList(new URLSearchParams({ page: '1', size: '20' })),
        nodeHealthApi.latestSnapshot(),
      ])
      setSnapshots(listPayload.items)
      setSnapshotTotal(listPayload.total)
      setLatest(latestPayload.snapshot)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '可调度节点结果读取失败')
    }
  }, [])

  const loadStats = useCallback(async () => {
    try {
      const payload = await nodeHealthApi.statsList(new URLSearchParams({ page: '1', size: '20' }))
      setStats(payload.items)
      setStatsTotal(payload.total)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '健康统计历史读取失败')
    }
  }, [])

  const loadEvents = useCallback(async () => {
    try {
      const query = new URLSearchParams({ page: '1', size: '30' })
      if (eventNodeFilter) query.set('node_code', eventNodeFilter)
      const payload = await nodeHealthApi.events(query)
      setEvents(payload.items)
      setEventTotal(payload.total)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '节点事件读取失败')
    }
  }, [eventNodeFilter])

  const loadDispatch = useCallback(async () => {
    try {
      const payload = await nodeHealthApi.dispatchSnapshot()
      setDispatchSnapshot(payload.snapshot)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '流量分配侧清单读取失败')
    }
  }, [])

  useEffect(() => {
    void loadSnapshots()
  }, [loadSnapshots])

  useEffect(() => {
    if (tab === 'stats') void loadStats()
    if (tab === 'events') void loadEvents()
    if (tab === 'dispatch') void loadDispatch()
  }, [tab, loadStats, loadEvents, loadDispatch])

  async function generate() {
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const key = idemKey.trim() || `ui-${Date.now()}`
      const payload = await nodeHealthApi.generateSnapshot(key)
      setNotice(payload.replayed ? `命中幂等键，回放 ${payload.snapshot.版本}（未生成新版本）` : `已生成 ${payload.snapshot.版本}，入集 ${payload.snapshot.可调度节点数} 个节点`)
      setIdemKey('')
      await loadSnapshots()
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '快照生成失败')
    } finally {
      setBusy(false)
    }
  }

  async function openDetail(id: number) {
    setErrorMessage('')
    try {
      setDetail(await nodeHealthApi.snapshotDetail(id))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '快照明细读取失败')
    }
  }

  async function syncDispatch() {
    setBusy(true)
    setErrorMessage('')
    setNotice('')
    try {
      const payload = await nodeHealthApi.syncDispatch()
      setNotice(payload.message)
      await Promise.all([loadDispatch(), loadSnapshots()])
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '分流同步失败，请确认已生成可调度节点结果')
    } finally {
      setBusy(false)
    }
  }

  return (
    <section className="page" data-module="schedulable-nodes">
      <header className="page-head">
        <div>
          <h2>可调度节点结果</h2>
          <p className="page-desc">
            健康统计聚合后生成不可变的版本化快照，并发上报只出一份结果、同幂等键重复提交回放原版本；
            快照可同步到流量分配侧，供各目标域名读取同一版分流口径。
          </p>
        </div>
        <div className="page-actions">
          <input
            className="idem-input"
            value={idemKey}
            placeholder="幂等键（留空自动生成）"
            onChange={(event) => setIdemKey(event.target.value)}
          />
          <button className="btn primary" type="button" disabled={busy} onClick={() => void generate()}>
            生成可调度节点结果
          </button>
          <button className="btn" type="button" disabled={busy} onClick={() => void syncDispatch()}>
            同步到流量分配侧
          </button>
        </div>
      </header>

      {latest && (
        <div className="stat-row">
          <article className="stat-card">
            <span className="stat-label">最新版本</span>
            <strong className="stat-value">{latest.版本}</strong>
          </article>
          <article className="stat-card">
            <span className="stat-label">可调度节点数</span>
            <strong className="stat-value">{latest.可调度节点数}</strong>
          </article>
          <article className="stat-card">
            <span className="stat-label">在线 / 离线 / 资源耗尽</span>
            <strong className="stat-value">{latest.在线数} / {latest.离线数} / {latest.资源耗尽数}</strong>
          </article>
          <article className="stat-card">
            <span className="stat-label">生成时间</span>
            <strong className="stat-value stat-value-small">{latest.生成时间}</strong>
          </article>
        </div>
      )}

      <nav className="tab-bar">
        {TABS.map((item) => (
          <button
            key={item.key}
            type="button"
            className={`tab-item ${tab === item.key ? 'tab-active' : ''}`}
            onClick={() => setTab(item.key)}
          >
            {item.label}
          </button>
        ))}
      </nav>

      {tab === 'snapshots' && (
        <>
          <table className="data-table">
            <thead>
              <tr>
                <th>版本</th>
                <th>生成时间</th>
                <th>幂等键</th>
                <th>来源统计版本</th>
                <th>可调度/总数</th>
                <th>在线/离线/耗尽</th>
                <th>操作人</th>
                <th>回溯</th>
              </tr>
            </thead>
            <tbody>
              {snapshots.map((item) => (
                <tr key={item.id}>
                  <td>{item.版本}</td>
                  <td>{item.生成时间}</td>
                  <td>{item.幂等键 || '—'}</td>
                  <td>{item.来源统计版本}</td>
                  <td>{item.可调度节点数}/{item.节点总数}</td>
                  <td>{item.在线数}/{item.离线数}/{item.资源耗尽数}</td>
                  <td>{item.操作人}</td>
                  <td><button className="link" type="button" onClick={() => void openDetail(item.id)}>查看入集明细</button></td>
                </tr>
              ))}
              {snapshots.length === 0 && (
                <tr><td className="empty-state" colSpan={8}>尚未生成可调度节点结果，点击右上角按钮生成第一版</td></tr>
              )}
            </tbody>
          </table>
          <footer className="page-foot"><span>共 {snapshotTotal} 个版本（只追加、不可变）</span></footer>
        </>
      )}

      {tab === 'stats' && (
        <>
          <table className="data-table">
            <thead>
              <tr>
                <th>统计版本</th><th>生成时间</th><th>触发方式</th><th>节点总数</th>
                <th>在线</th><th>离线</th><th>资源耗尽</th><th>可调度</th><th>口径核对</th>
              </tr>
            </thead>
            <tbody>
              {stats.map((item) => (
                <tr key={item.id}>
                  <td>{item.统计版本}</td>
                  <td>{item.生成时间}</td>
                  <td>{item.触发方式}</td>
                  <td>{item.节点总数}</td>
                  <td>{item.在线数}</td>
                  <td>{item.离线数}</td>
                  <td>{item.资源耗尽数}</td>
                  <td>{item.可调度节点数}</td>
                  <td>{Object.entries(item.核对).map(([key, value]) => (
                    <span key={key} className={value ? 'check-ok' : 'check-bad'}>{key}：{value ? '通过' : '异常'} </span>
                  ))}</td>
                </tr>
              ))}
              {stats.length === 0 && <tr><td className="empty-state" colSpan={9}>暂无健康统计记录</td></tr>}
            </tbody>
          </table>
          <footer className="page-foot"><span>共 {statsTotal} 版统计，高频刷新只追加新版本，不会把旧统计写回</span></footer>
        </>
      )}

      {tab === 'events' && (
        <>
          <form
            className="filter-bar"
            onSubmit={(event) => {
              event.preventDefault()
              void loadEvents()
            }}
          >
            <label className="filter-item">
              <span>按节点编号过滤</span>
              <input value={eventNodeFilter} placeholder="如 WORK-0001" onChange={(event) => setEventNodeFilter(event.target.value)} />
            </label>
            <button className="btn" type="submit">查询</button>
            <button className="btn ghost" type="button" onClick={() => setEventNodeFilter('')}>清除筛选</button>
          </form>
          <table className="data-table">
            <thead>
              <tr><th>时间</th><th>节点编号</th><th>事件类型</th><th>详情</th><th>操作人</th><th>关联快照</th></tr>
            </thead>
            <tbody>
              {events.map((event) => (
                <tr key={event.id}>
                  <td>{event.时间}</td>
                  <td>{event.节点编号}</td>
                  <td><span className="event-type">{event.事件类型}</span></td>
                  <td>{event.详情}</td>
                  <td>{event.操作人}</td>
                  <td>{event.关联快照版本 || '—'}</td>
                </tr>
              ))}
              {events.length === 0 && <tr><td className="empty-state" colSpan={6}>暂无事件记录</td></tr>}
            </tbody>
          </table>
          <footer className="page-foot"><span>共 {eventTotal} 条事件，入集、出集、重新入集均可按记录回溯</span></footer>
        </>
      )}

      {tab === 'dispatch' && (
        dispatchSnapshot ? (
          <>
            <div className="stat-row">
              <article className="stat-card">
                <span className="stat-label">同步版本</span>
                <strong className="stat-value">{dispatchSnapshot.快照版本}</strong>
              </article>
              <article className="stat-card">
                <span className="stat-label">同步时间</span>
                <strong className="stat-value stat-value-small">{dispatchSnapshot.同步时间}</strong>
              </article>
              <article className="stat-card">
                <span className="stat-label">覆盖域名</span>
                <strong className="stat-value">{dispatchSnapshot.域名分流结果数}</strong>
              </article>
              <article className="stat-card">
                <span className="stat-label">状态</span>
                <strong className="stat-value">{dispatchSnapshot.状态}</strong>
              </article>
            </div>
            <table className="data-table">
              <thead>
                <tr><th>策略编号</th><th>目标域名</th><th>节点亲和性</th><th>分流节点数</th><th>分流节点清单</th></tr>
              </thead>
              <tbody>
                {dispatchSnapshot.域名结果.map((domain) => (
                  <tr key={domain.策略编号}>
                    <td>{domain.策略编号}</td>
                    <td>{domain.目标域名}</td>
                    <td>{domain.节点亲和性}</td>
                    <td>{domain.分流节点数}</td>
                    <td>{domain.分流节点清单.length ? domain.分流节点清单.join('、') : '（当前版本无可调度节点）'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        ) : (
          <p className="empty-state page-foot">流量分配侧清单为空，请先生成可调度节点结果并执行同步</p>
        )
      )}

      {detail && (
        <div className="modal-mask" role="dialog" aria-modal="true" onClick={() => setDetail(null)}>
          <div className="modal-box" onClick={(event) => event.stopPropagation()}>
            <header className="modal-head">
              <h3>{detail.版本} 入集明细（{detail.可调度节点数} 个节点）</h3>
              <button className="btn ghost" type="button" onClick={() => setDetail(null)}>关闭</button>
            </header>
            <table className="data-table">
              <thead>
                <tr><th>节点编号</th><th>主机地址</th><th>地域</th><th>空闲槽位</th><th>CPU</th><th>内存MB</th><th>心跳年龄(秒)</th><th>入集依据</th></tr>
              </thead>
              <tbody>
                {detail.items.map((item) => (
                  <tr key={item.节点编号}>
                    <td>{item.节点编号}</td>
                    <td>{item.主机地址}</td>
                    <td>{item.地域标签}</td>
                    <td>{item.空闲槽位数}/{item.并发槽位数}</td>
                    <td>{item.CPU使用率}%</td>
                    <td>{item.内存可用MB}</td>
                    <td>{item.心跳年龄秒 ?? '—'}</td>
                    <td className="reason-cell">{item.入集依据}</td>
                  </tr>
                ))}
                {detail.items.length === 0 && <tr><td className="empty-state" colSpan={8}>该版本没有可调度节点入集</td></tr>}
              </tbody>
            </table>
          </div>
        </div>
      )}

      <footer className="page-foot">
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
