# 分布式网页抓取调度平台

面向分布式网页抓取任务调度、节点协同、请求分配、反爬策略编排、数据校验入库与抓取质量大盘的采集工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 采集节点 | `worker` | 工作节点 | 节点编号、主机地址、并发槽位数、心跳、人工状态 |
| 节点健康 | `node_health` | 健康统计与可调度节点结果 | 健康状态、不可调度原因、快照版本、幂等键 |
| 请求分配 | `dispatch` | 分配策略 | 策略编号、目标域名、分配算法、分流快照版本 |
| 抓取作业 | `crawljob` | 作业定义 | 作业编号、目标站点、起始URL |
| URL过滤 | `urlfilter` | 过滤规则 | 规则编号、包含模式、排除模式 |
| DNS解析 | `dnsresolver` | 解析配置 | 配置编号、域名模式、DNS服务器 |
| 内容提取 | `contentextract` | 提取器 | 提取器编号、内容类型、正文选择器 |
| 存储策略 | `storagestrategy` | 存储方案 | 方案编号、存储类型、连接参数 |
| 渲染任务 | `rendertask` | 渲染作业 | 作业编号、目标URL、浏览器版本 |
| 采集质量 | `qualityreport` | 质量报告 | 报告编号、作业编号、总URL数 |
| 带宽控制 | `bandwidth` | 带宽策略 | 策略编号、节点标识、下载带宽上限 |
| 重试队列 | `retryqueue` | 重试条目 | 条目编号、失败URL、失败原因码 |
| 翻页管理 | `pagemarker` | 翻页规则 | 规则编号、目标站点、翻页类型 |
| 增量标记 | `incrementmark` | 增量水位 | 水位编号、目标源、增量字段名 |
| 配置同步 | `configsync` | 同步任务 | 任务编号、源节点、目标节点 |
| 采集API | `scrapeapi` | API定义 | API编号、API名称、请求路径 |
| 数据抽样 | `datasample` | 抽样方案 | 方案编号、数据源、抽样方式 |
| 采集告警 | `alertpolicy` | 告警策略 | 策略编号、监控指标、触发条件 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 节点健康统计与可调度节点结果

采集节点的心跳、资源占用（槽位/CPU/内存）与人工状态三类信号，沿一条链路聚合，
节点清单、健康统计与请求分配使用同一个可用性口径：

```text
心跳 / 资源占用 / 人工状态
  -> 健康评估（冲突优先级：人工状态 > 心跳时效 > 资源耗尽）
  -> 健康统计（在线/离线/资源耗尽三态互斥且穷尽，计数钳制在 [0, 节点总数]）
  -> 可调度节点结果（版本化、不可变、可按记录回溯）
  -> 同步到流量分配侧清单（各目标域名读取同一版快照更新分流结果）
```

关键约束及落地方式：

- **口径一致**：健康三态由 `NodeHealthService.evaluate_node` 单点计算，前端不重复评估；
  筛选字段、清除筛选后的总数、资源耗尽原因共用同一份记录。
- **不重复计数**：评估前按节点 id 去重；批量人工变更对重复 id 只处理一次。
- **计数有界**：三态计数显式钳制并校验“三态之和=节点总数”，批量变更期间不出现负数或越界。
- **并发只出一份**：快照生成用“生成轮次屏障 + 信号版本合并窗”，并发上报只产生一版结果，
  不会出现版本相互覆盖；信号变化后立即生成新版本，节点恢复不会被旧版本合并。
- **幂等提交**：同 `idempotency_key` 重复提交直接回放原版本，不产生新版本。
- **可回溯**：健康统计、可调度快照均版本化只追加；节点事件流记录心跳、人工变更、
  入集/出集/重新入集与快照同步，恢复在线有可解释的重新入集记录。
- **分流同步**：`sync-dispatch` 让所有目标域名读取同一版快照，按节点亲和性更新分流结果，
  并把版本号回写到每条分配策略，下游经 `/api/dispatch/snapshot/current` 读取。

主要接口（前缀 `/api`）：

- `GET /node-health/overview`：在线/离线/资源耗尽卡片与口径核对
- `GET /node-health/records`：健康记录筛选（`health_status`、`exhaustion_reason`、`keyword`）
- `POST /node-health/aggregate`：手动聚合一版健康统计
- `GET /node-health/stats`、`GET /node-health/stats/{id}`：统计历史与单版本回溯
- `POST /node-health/snapshots`：生成可调度节点结果（body 可带 `idempotency_key`）
- `GET /node-health/snapshots`、`/snapshots/latest`、`/snapshots/{id}`：版本列表与回溯
- `POST /node-health/snapshots/sync-dispatch`：同步到流量分配侧清单
- `GET /node-health/events`：节点事件审计流
- `POST /worker/{id}/heartbeat`、`POST /worker/{id}/manual-status`、`POST /worker/batch/manual-status`

演示环境带心跳模拟器（`heartbeat_simulator.py`，随 FastAPI 生命周期启停），
持续刷新活跃节点心跳；心跳过期（WORK-0004）与人工维护（WORK-0008）节点不模拟，保留真实离线样本。

领域：数据工程
子方向：数据采集、爬虫与RPA
