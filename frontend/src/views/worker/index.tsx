import { useCallback, useEffect, useMemo, useRef, useState } from 'react'

import { nodeHealthApi, workerApi, type HealthOverview, type HealthRecord, type HealthStatus } from '@/api/nodeHealth'

const HEALTH_OPTIONS: HealthStatus[] = ['在线', '离线', '资源耗尽']
const REASON_OPTIONS = ['心跳过期', '槽位占满', 'CPU过载', '内存不足', '人工停用', '维护中']
const PAGE_SIZE = 20

type Row = HealthRecord
type Filters = { keyword: string; health_status: string; exhaustion_reason: string }
const EMPTY_FILTERS: Filters = { keyword: '', health_status: '', exhaustion_reason: '' }

const FALLBACK_OVERVIEW: HealthOverview = {
  cards: [
    { label: '节点总数', value: 0 },
    { label: '在线节点', value: 0 },
    { label: '离线节点', value: 0 },
    { label: '资源耗尽', value: 0 },
    { label: '可调度节点', value: 0 },
  ],
  reason_cards: [],
  评估时间: '',
  核对: {},
}

function statusBadgeClass(status: HealthStatus): string {
  if (status === '在线') return 'badge badge-online'
  if (status === '资源耗尽') return 'badge badge-exhausted'
  return 'badge badge-offline'
}

export default function WorkerView() {
  const [overview, setOverview] = useState<HealthOverview>(FALLBACK_OVERVIEW)
  const [rows, setRows] = useState<Row[]>([])
  const [total, setTotal] = useState(0)
  const [page, setPage] = useState(1)
  const [filters, setFilters] = useState<Filters>(EMPTY_FILTERS)
  const [manualOptions, setManualOptions] = useState<string[]>(['正常', '人工停用', '维护中'])
  const [selected, setSelected] = useState<Set<number>>(new Set())
  const [batchStatus, setBatchStatus] = useState('人工停用')
  const [autoRefresh, setAutoRefresh] = useState(false)
  const [errorMessage, setErrorMessage] = useState('')
  const [notice, setNotice] = useState('')
  const [loading, setLoading] = useState(false)
  const filterInput = useRef(filters)
  filterInput.current = filters

  const load = useCallback(async (targetPage: number) => {
    setLoading(true)
    setErrorMessage('')
    try {
      const query = new URLSearchParams({ page: String(targetPage), size: String(PAGE_SIZE) })
      const f = filterInput.current
      if (f.keyword) query.set('keyword', f.keyword)
      if (f.health_status) query.set('health_status', f.health_status)
      if (f.exhaustion_reason) query.set('exhaustion_reason', f.exhaustion_reason)
      const [overviewPayload, recordsPayload] = await Promise.all([
        nodeHealthApi.overview(),
        nodeHealthApi.records(query),
      ])
      setOverview(overviewPayload)
      setRows(recordsPayload.items)
      setTotal(recordsPayload.total)
      setPage(targetPage)
      // 翻页/刷新后清理已不在当前页的勾选项，避免批量操作作用到不可见节点。
      setSelected((prev) => new Set([...prev].filter((id) => recordsPayload.items.some((row) => row.id === id))))
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '健康数据读取失败')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    void load(1)
  }, [load, filters])

  useEffect(() => {
    if (!autoRefresh) return
    const timer = window.setInterval(() => void load(page), 10000)
    return () => window.clearInterval(timer)
  }, [autoRefresh, load, page])

  useEffect(() => {
    workerApi.manualOptions().then((payload) => setManualOptions(payload.options)).catch(() => undefined)
  }, [])

  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE))

  async function handleAction(action: () => Promise<{ ok: boolean; message: string }>) {
    setErrorMessage('')
    setNotice('')
    try {
      const result = await action()
      if (!result.ok) {
        setErrorMessage(result.message)
        return
      }
      setNotice(result.message)
      await load(page)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '操作失败')
    }
  }

  function toggleRow(id: number) {
    setSelected((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function togglePageSelection() {
    setSelected((prev) => {
      const pageIds = rows.map((row) => row.id)
      const allSelected = pageIds.every((id) => prev.has(id)) && pageIds.length > 0
      const next = new Set(prev)
      if (allSelected) pageIds.forEach((id) => next.delete(id))
      else pageIds.forEach((id) => next.add(id))
      return next
    })
  }

  async function runBatch() {
    if (selected.size === 0) {
      setErrorMessage('请先勾选需要批量变更的节点')
      return
    }
    setErrorMessage('')
    setNotice('')
    try {
      const result = await workerApi.batchManualStatus([...selected], batchStatus)
      setNotice(result.message + (result.failed.length ? `；失败：${result.failed.join('、')}` : ''))
      setSelected(new Set())
      await load(page)
    } catch (error) {
      setErrorMessage(error instanceof Error ? error.message : '批量变更失败')
    }
  }

  const reasonCards = useMemo(() => overview.reason_cards, [overview])

  return (
    <section className="page" data-module="worker">
      <header className="page-head">
        <div>
          <h2>采集节点管理</h2>
          <p className="page-desc">
            统一读取节点心跳、资源占用与人工状态，按“人工状态 &gt; 心跳时效 &gt; 资源耗尽”的优先级评估健康与可调度性。
          </p>
        </div>
        <div className="page-actions">
          <label className="switch-item">
            <input type="checkbox" checked={autoRefresh} onChange={(e) => setAutoRefresh(e.target.checked)} />
            10秒自动刷新
          </label>
          <button className="btn" type="button" onClick={() => void load(page)}>刷新健康统计</button>
        </div>
      </header>

      <div className="stat-row">
        {overview.cards.map((card) => (
          <article key={card.label} className="stat-card">
            <span className="stat-label">{card.label}</span>
            <strong className="stat-value">{card.value}</strong>
          </article>
        ))}
      </div>
      <div className="stat-row reason-row">
        {reasonCards.map((card) => (
          <article key={card.label} className="stat-card stat-card-compact">
            <span className="stat-label">{card.label}</span>
            <strong className="stat-value">{card.value}</strong>
          </article>
        ))}
      </div>
      <p className="check-line">
        评估时间 {overview.评估时间 || '—'}
        {Object.entries(overview.核对).map(([key, value]) => (
          <span key={key} className={value ? 'check-ok' : 'check-bad'}>{key}：{value ? '通过' : '异常'}</span>
        ))}
      </p>

      <form
        className="filter-bar"
        onSubmit={(event) => {
          event.preventDefault()
          void load(1)
        }}
      >
        <label className="filter-item">
          <span>节点编号/主机地址</span>
          <input
            value={filters.keyword}
            placeholder="按节点编号或主机地址检索"
            onChange={(event) => setFilters((prev) => ({ ...prev, keyword: event.target.value }))}
          />
        </label>
        <label className="filter-item">
          <span>健康状态</span>
          <select value={filters.health_status} onChange={(event) => setFilters((prev) => ({ ...prev, health_status: event.target.value }))}>
            <option value="">全部</option>
            {HEALTH_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
          </select>
        </label>
        <label className="filter-item">
          <span>不可调度原因</span>
          <select value={filters.exhaustion_reason} onChange={(event) => setFilters((prev) => ({ ...prev, exhaustion_reason: event.target.value }))}>
            <option value="">全部</option>
            {REASON_OPTIONS.map((option) => <option key={option} value={option}>{option}</option>)}
          </select>
        </label>
        <button className="btn" type="submit">查询</button>
        <button
          className="btn ghost"
          type="button"
          onClick={() => {
            setFilters(EMPTY_FILTERS)
          }}
        >
          清除筛选
        </button>
      </form>

      <div className="batch-bar">
        <span>已选 {selected.size} 个节点</span>
        <select value={batchStatus} onChange={(event) => setBatchStatus(event.target.value)}>
          {manualOptions.map((option) => <option key={option} value={option}>{option}</option>)}
        </select>
        <button className="btn primary" type="button" onClick={() => void runBatch()}>批量人工状态变更</button>
      </div>

      <table className="data-table">
        <thead>
          <tr>
            <th className="col-check">
              <input
                type="checkbox"
                checked={rows.length > 0 && rows.every((row) => selected.has(row.id))}
                onChange={togglePageSelection}
                aria-label="全选当前页"
              />
            </th>
            <th>节点编号</th>
            <th>主机地址</th>
            <th>地域</th>
            <th>健康状态</th>
            <th>不可调度原因</th>
            <th>槽位(已用/总)</th>
            <th>CPU</th>
            <th>内存可用MB</th>
            <th>心跳年龄(秒)</th>
            <th>人工状态</th>
            <th>可执行动作</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((row) => (
            <tr key={row.id}>
              <td className="col-check">
                <input type="checkbox" checked={selected.has(row.id)} onChange={() => toggleRow(row.id)} aria-label={`选择 ${row.节点编号}`} />
              </td>
              <td>{row.节点编号}</td>
              <td>{row.主机地址}</td>
              <td>{row.地域标签}</td>
              <td><span className={statusBadgeClass(row.健康状态)}>{row.健康状态}</span></td>
              <td>{row.不可调度原因 || '—'}</td>
              <td>{row.已用槽位数}/{row.并发槽位数}（空闲{row.空闲槽位数}）</td>
              <td>{row.CPU使用率}%</td>
              <td>{row.内存可用MB}</td>
              <td>{row.心跳年龄秒 ?? '—'}{row.心跳过期 ? '（已过期）' : ''}</td>
              <td>{row.人工状态}</td>
              <td className="row-actions">
                <button className="link" type="button" onClick={() => void handleAction(() => workerApi.heartbeat(row.id))}>
                  心跳上报
                </button>
                {manualOptions.map((option) => (
                  <button
                    key={option}
                    className="link"
                    type="button"
                    onClick={() => void handleAction(() => workerApi.setManualStatus(row.id, option))}
                  >
                    {option === '正常' ? '恢复正常' : option}
                  </button>
                ))}
              </td>
            </tr>
          ))}
          {rows.length === 0 && (
            <tr>
              <td className="empty-state" colSpan={12}>暂无符合筛选条件的节点，清除筛选后可查看全部 {overview.cards[0]?.value ?? 0} 个节点</td>
            </tr>
          )}
        </tbody>
      </table>

      <footer className="page-foot">
        <span>
          共 {total} 条健康记录（第 {page}/{pageCount} 页）{loading ? '，刷新中…' : ''}
        </span>
        <span className="foot-actions">
          <button className="btn" type="button" disabled={page <= 1} onClick={() => void load(page - 1)}>上一页</button>
          <button className="btn" type="button" disabled={page >= pageCount} onClick={() => void load(page + 1)}>下一页</button>
        </span>
        {notice && <span className="notice-text">{notice}</span>}
        {errorMessage && <span className="error-text">{errorMessage}</span>}
      </footer>
    </section>
  )
}
