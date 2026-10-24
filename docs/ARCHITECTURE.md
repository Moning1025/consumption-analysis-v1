# 多源数据融合的本地消费分析系统 —— 架构设计与开发路线（v1.1）

> 状态：设计稿（2026-10-24，已合并软著说明书 V1.0 功能基线）。执行者：DSH 编码 agent；项目所有人：Moning1025（毕设）。
> 本文档是后续所有改动的**唯一设计基线**：每个 Step 完成后在文末「变更记录」追加一行，并同步更新对应章节。
> 阅读顺序：第 1 章现状体检 + 说明书功能基线（1.4）→ 第 2 章目标架构 → 第 3 章数据模型 → 第 4 章任务与进度 → 第 5 章数据接入 → 第 6 章 LLM → 第 7 章前端 → 第 8 章路线图。
> 现有仓库约定见 `AGENTS.md`（Issue/spec 走 GitHub Issues，`gh` CLI 操作）。

---

## 1. 现状体检（基于代码逐行核对，不是猜的）

### 1.1 技术栈（已确认）

| 层 | 技术 | 证据 |
|---|---|---|
| 桌面壳 | pywebview 6.2.1 + pythonnet 3.1.0（Windows WebView2） | `requirements.txt`、`main.py` |
| 前端 | 单文件 `web/index.html`（2086 行），原生 JS + ECharts 5.4.3（**CDN 引入**） | `web/index.html:6` |
| 后端 | 纯 Python 包，无 Web 框架；JS API 直接暴露 `BridgeApi` 方法 | `bridge_api.py` |
| 存储 | SQLite（WAL），`local_consumption_insight.db`（417,792 B），13 张表 | `schema.sql`、`store/db_connection.py` |
| 运行环境 | Windows x64，Python 3.10.9（venv 内 3.10.9，系统同为 3.10.9） | 实测 |

> **运行环境硬约束（S1 期间实测踩坑）**：虚拟环境必须放在项目目录之外，本机固定在 `D:\Python\venv_p3`。
> 仓库来自压缩包下载，项目目录带 Windows「来自 Internet」标记（MOTW），.NET 拒绝从该路径加载程序集，
> `webview.start()` 会抛 `RuntimeError: Failed to resolve Python.Runtime.Loader.Initialize`（窗口起不来）。
> 另外 venv 迁移后 `Scripts\pip.exe` / `python.exe` 里的启动器路径会失效，统一改用 `python -m pip` 调用。

### 1.2 真实 / 半成品 / 假：逐项判定

| 模块 | 判定 | 关键事实 |
|---|---|---|
| 登录鉴权 | ✅ 真实 | PBKDF2-HMAC-SHA256(120000 轮) + HMAC 签名 token；角色矩阵 `auth/role_policy_registry.py`；审计落 `audit_log` |
| 任务分发 | ⚠️ 真实但同步 | `pipeline/task_dispatch_service.py` 按 `_TASK_DISPATCH_TABLE` 直接同步调用 handler，**无队列、无取消**，pywebview 调用线程被占满 |
| 数据接入（登记） | ⚠️ 半真实 | `register_source` 真的写库；但 `ingest_local_files` **只登记文件路径，不解析内容**；`import_demo_batch` 用的是 `bridge_api.py` 顶部硬编码的 `DEMO_SAMPLES`（餐饮/旅游/零售各 4 条） |
| 清洗融合算法 | ✅ 真实 | `pipeline/cleaning_fusion_service.py`（614 行）5 阶段：规范化/去重/对齐/融合/落库，带字段别名表、商户相似度 0.92、金额容差 0.01、90 秒时间窗 |
| 导入进度 | ❌ 伪造（两套） | ① `bridge_api.py:173-207` 按 `created_at` 秒数推算：<2s→已接入10% / <5s→预处理中50% / 其余→已完成100%，并**写回 DB**；② `bridge_api.py:484-528` 线程 sleep 1s/2s/3s 硬推 50%→100% |
| 启动即“全完成” | ❌ 伪造 | `main.py:120-128` 启动时 `UPDATE ... SET status='已完成', progress=100 WHERE status IN ('已接入','排队中','预处理中')` |
| RFM | ⚠️ 半成品 + 必崩 Bug | 服务 `pipeline/rfm_segmentation_service.py` 从 payload 的 records 读数据（前端从不传）；落库在 `store/rfm_segmentation_store.py:186-224`：`id TEXT PRIMARY KEY` 却按 segment 插多行同一 id → UNIQUE 冲突 |
| 消费预测/异常 | ⚠️ 半成品 | `pipeline/forecast_anomaly_service.py`（323 行）移动平均/指数平滑/线性趋势 + z-score 异常，从 `consumption_record` 读真实序列；**但库表里没有数据、前端是静态页** |
| 情感/主题 | ❌ 占位 | `pipeline/sentiment_topic_service.py` 词袋正负词 + `text[:8]` 当主题名（占位）；前端页面数字全硬编码 |
| 空间热力 | ⚠️ 静态数据 | 前端 `web/index.html:1593` `fetch('heatmap_data.json')`；`pipeline/hotspot_data_fetcher.py:172/266` 两个函数**缩进写进了 `fetch_real_consumption_data()` 内部 return 之后 → 死代码，永远无法调用** |
| 推荐 | ⚠️ 规则占位 | `pipeline/recommendation_ranking_service.py` 按 频次+金额*0.001+偏好 排序；前端静态 |
| 大屏 | ❌ 纯静态 | `renderDashboard` 是写死的 SVG/KPI（`web/index.html:1956-2011`）；`extensions/echarts_dashboard_feed.py` 存在但**从未被 bridge 调用** |
| API 监测 | ⚠️ 未启动 | `extensions/realtime_api_probe.py` 实现了 60s 采样线程，但 `main.py` **没有启动它** |
| 结果回读 | ❌ 缺失 | `BridgeApi` 没有任何 `get_rfm_result / get_forecast_result / get_sentiment_result / get_spatial_payload / get_dashboard_payload` → 前端即使跑了任务也读不到结果 |

### 1.3 结论

**“假数据 + 静态页 + 假进度”的根因不是页面写得差，而是缺少三样基础设施**：
1. **原始数据层**：没有任何一张表保存“导入的原始行”，算法只能吃 demo 常量；
2. **任务运行器**：重任务只能同步跑在 JS API 线程里，只能靠 sleep 假装进度；
3. **结果查询 API**：分析结果落了库，但没有桥接方法把它们读回前端。

真实数据流 = 原始层 + 可观测任务 + 结果回读。这三件事做完，页面自然“活”了。

---

### 1.4 软著说明书（V1.0，49 页 / 11 章）功能基线

> 来源：`D:\1\软著\多源数据融合的本地消费分析系统\…_V1.0_用户说明书.docx`（与同名 PDF 同内容；PDF 为 49 页，正文约 9.4k 字 + 截图）。
> 本节把说明书里**可验收的功能点**逐条冻结，后续所有页面/接口以本表为准；截图里的数字（如 128,460 次调用）是示例值，不算需求。

#### 1.4.1 逐章功能点

| 章 | 页面 | 说明书明确要求 | 目标实现 | 当前状态 |
|---|---|---|---|---|
| 1 | 系统登录 | 卡片式登录页；账号+密码；登录失败提示 | 已有（真实） | ✅ 已满足 |
| 2.1 | 数据文件导入 | 按钮/拖拽导入；餐饮、旅游、零售三类混排；按后缀识别格式；“最近导入”按时间倒序 | 文件适配器 + 批次登记 | ⚠️ 文件未解析 |
| 2.2 | 导入批次清单 | 每批次显示文件名、来源类型、**存储路径**、状态、进度百分比；状态四态：**排队中 / 预处理中 / 格式校验 / 已完成**；刷新列表；新建记录 | `data_ingestion_console` 扩展四态 + `subtitle/priority` 列 | ⚠️ 三态、无格式校验态 |
| 2.2.1 | 批次详情抽屉 | 字段：标题、来源路径、状态、进度、导入时间、操作人、记录条数 | 已有详情接口，补字段 | ⚠️ 缺标题/条数语义 |
| 2.2.2 | 删除批次 | 确认弹窗；删除批次**连同关联解析结果** | 级联删除 `ingestion_row` | ❌ 未级联 |
| 2.2.3 | 新建批次 | 两列表单：标题、副标题、状态、进度、导入时间、操作人；含“真源载荷 / 元数据”区域；提交后进入等待预处理（排队中） | `register_source` 扩展表单字段 | ⚠️ 字段不全 |
| 2.3 | 导入预处理进度 | 每批次：文件名、百分比、当前阶段；**解析中说明正在处理哪些字段**；**格式校验提示异常条数**；**排队给出预计开始时间**；完成注明数据写到哪里 | `job_run.stage/message` + 校验报告 | ❌ 现在是假进度 |
| 3.1 | 清洗融合任务队列 | 四列：任务、状态、进度、操作；每条标注来源表与目标表；刷新/新建；详情含标题、来源与目标、状态、进度、优先级、提交组、操作人；删除确认 | `cleaning_fusion_pipeline_task` 补 `subtitle/priority/submitter/source_table/target_table` | ⚠️ 缺字段 |
| 3.2 | 流水线阶段进度 | **七个阶段**：数据接入、去重清洗、缺失值处理、字段对齐、多源融合、质量校验、结果输出；每阶段百分比或条数 | 清洗 job 由 5 阶段扩为 7 阶段 | ❌ 现为 5 阶段 |
| 3.3 | 融合执行日志 | 终端样式滚动；带时间戳与级别 INFO/WARN/ERROR | `job_event` 增量拉取 | ❌ 现为前端本地日志 |
| 4.1 | 挖掘参数配置 | 12 项参数：情感模型（BERT-wwm-ext / RoBERTa-wwm-ext / SnowNLP 规则基线 / LSTM 融合）、主题数量、主题模型（LDA / BTM / NMF / BERTopic）、数据来源、时间范围、最短评论长度、停用词表、自定义停用词、负面增强识别、置信度阈值、批处理条数、输出格式 | 参数落 `app_setting` + `sentiment_job` | ❌ 静态表单 |
| 4.2 | 情感与主题结论摘要 | 8 个指标：有效评论总数、正/中/负占比、识别主题数、主题平均一致性、负面评论环比、分析耗时；5 段结论：整体极性、高频主题、代表性负面评论、主题时间趋势、模型质量 | `sentiment_job` + LLM/规则双通道 | ❌ 硬编码数字 |
| 4.3 | 情感与主题可视化 | 三维柱状（情感占比 × 主题分布 × 时间趋势）；可旋转缩放；点击弹出情感类别/关键词/数量 | ECharts 3D bar（echarts-gl）或分组柱替代 | ❌ 静态 div |
| 5.1 | RFM 参数调节 | R/F/M 权重滑块；分箱阈值；**分层数量**；分析时间窗口；排除退款订单开关；归一化方式；点“执行”重算 | `rfm_job` 参数化 | ❌ 滑块不生效 |
| 5.2 | 分层画像结果摘要 | 参与分层用户数、有效消费记录数、分层数量、高价值用户占比、计算耗时；**按分层逐一列人数/占比/消费行为中位数 + 运营建议**（重要价值→会员权益、流失风险→自动化召回） | `rfm_batch/rfm_segment` | ❌ 静态 |
| 5.3 | 用户分层明细 | 用户编号、名称、所属分层、最近消费间隔、消费频次、消费金额、R/F/M 得分、综合得分、最近消费时间、注册时间、常驻城市、偏好品类 | `rfm_user_snapshot` | ❌ 无 |
| 6.1 | 预测与检测参数 | 窗口默认未来 14 天；模型：**ARIMA / Prophet / LSTM / XGBoost**；异常阈值；检测维度：金额、频次、客单价、品类偏好偏移 | 模型注册表 + 可用性降级 | ⚠️ 现为移动平均/指数平滑/线性趋势 |
| 6.2 | 趋势与异常可视化 | 历史实线 + 未来 14 天虚线；异常点单独标出；点击看数值与偏离程度 | `forecast_job` 结果 + ECharts markPoint | ❌ 静态 |
| 6.3 | 异常事件清单 | 字段：事件编号、发生时间、数据来源、区域、消费金额、偏离度、异常类型、处理状态；按偏离度降序；刷新；新建/编辑/详情/删除（确认弹窗） | 表 `anomaly_event` + CRUD bridge | ❌ 无表无接口 |
| 7.1 | 消费热点地图 | 区划轮廓着色；左上角区域名+热度指数；右上 2D/3D 切换 | `get_spatial_payload` + ECharts geo/map3D | ⚠️ 静态 json |
| 7.2 | 区域热点摘要 | 四指标卡：覆盖区域数、热点聚集区、消费突变区、热点迁移区（带状态标签）；前五商圈+消费密度；空间分布特征与异常结论段落 | `spatial_job` 聚合 | ⚠️ 部分（四川硬编码） |
| 7.3 | 空间异常提示 | 彩色级别条；状态：待处理/观察中/已确认/已关闭；待处理+高等级靠前；展开含密度变化幅度、阈值对比、核查建议；底部级别/触发时间/检测来源 | 表 `spatial_anomaly_signal` | ❌ 无 |
| 8.1 | 推荐策略配置台 | 策略版本、画像覆盖用户数；召回三通道：协同过滤 / 内容标签 / 空间热点（可勾选）；单路召回数量、最低置信度、行为回溯窗口；排序/过滤/画像参数；应用召回配置；执行任务（确认弹窗） | 表 `recommendation_strategy` + `recommendation_job` | ❌ 无 |
| 8.2 | 推荐结果摘要 | 覆盖率、命中率、人均推荐条数、品类多样性指数、冷启动用户占比（警告态）；两类样例：高价值活跃用户（匹配度/距离）、潜力成长用户（匹配度/人均价格） | `recommendation_workbench` 扩指标列 | ❌ 静态 |
| 9.1 | API 实时读数 | 近 5 分钟总调用量、当前 QPS、平均响应延迟、调用成功率、异常调用数；六个服务状态标签（分析服务“负载偏高”） | `realtime_api_monitor` 聚合 + `service_health` | ⚠️ 探测器未启动 |
| 9.2 | 服务指示灯墙 | 四组：API 端点 / 数据源 / 依赖服务 / 运行环境 | `service_health` 分组 | ❌ 无 |
| 9.3 | API 运行日志 | 时间倒序；请求路径、耗时、状态码、异常堆栈；INFO/WARN/ERROR | `realtime_api_monitor` + 日志表 | ⚠️ 有表无展示 |
| 10.1 | 大屏指标卡 | 六卡：消费总额、活跃用户数、异常事件量、融合数据源规模、平均客单价、情感正向占比；环比说明；统计周期固定 2026-08-15~09-13 | `get_dashboard_payload` + 周期配置 | ❌ 写死 |
| 10.2 | 趋势与分布 | 近 30 天柱+线；品类分布；RFM 分层对比；图例切换；浮层 | 同左 | ❌ 写死 SVG |
| 10.3 | 区域热力地图 | 区划底图色块；2D/3D 切换；左上口径+截止时间；左下金额图例 | 同 7.1 数据 | ❌ 写死 |
| 11.1 | 初始化向导 | 四步：数据源接入→分析参数→运行环境→校验生效；步骤条高亮/对勾；上一步/编辑记录/执行任务 | `app_setting` + 向导状态 | ❌ 静态 |
| 11.2 | 分析参数设置 | 必填星号；情感正负阈值、RFM 窗口与高频阈值、高价值金额阈值、预测窗口、异常灵敏度(低中高)、网格粒度、推荐条数、融合策略、存储路径、调度表达式、大屏刷新间隔、最大并发数；保存弹窗 | `app_setting` 13 项 | ❌ 不落库 |
| 11.3 | 配置帮助 | 折叠 FAQ（阈值范围/RFM 窗口依据/预测窗长/灵敏度影响/网格粒度/调度表达式） | 静态文案即可 | ⚠️ 已有雏形 |

#### 1.4.2 说明书与现有代码的关键差异（及本设计的处理）

| # | 说明书 | 现有代码 | 决策 |
|---|---|---|---|
| D1 | 清洗 **7 阶段**（含缺失值处理、质量校验、结果输出） | `cleaning_fusion_service.py` 只有 5 阶段（normalize/dedup/align/fuse/persist） | 扩为 7 阶段，权重：接入 15 / 去重清洗 25 / 缺失值处理 15 / 字段对齐 15 / 多源融合 15 / 质量校验 10 / 结果输出 5 |
| D2 | 批次状态 **四态**（排队中/预处理中/格式校验/已完成） | 三态（已接入/预处理中/已完成） | 状态机四态 + `validation` 结果；前端中文映射集中在一处常量 |
| D3 | RFM 摘要示例 **5 个分层** | `SEGMENT_LABELS` 8 类 | 三条口径同时成立：**计算层**仍按 8 类算（`label_8` 全量落库）；**展示层**默认 5 层（`segment_count=5`，`label_5` 归并后给 UI）；**切换**：`app_setting.rfm.display_layers` 可改 5/8。8 类→5 类归并表：重要价值←重要价值；重要挽留←重要挽留；重要发展←深耕+唤回；一般←潜力+新客户；流失←流失+一般 |
| D4 | 预测模型 **ARIMA/Prophet/LSTM/XGBoost** | 移动平均/指数平滑/线性趋势 | 模型注册表：ARIMA（statsmodels，必装）、移动平均/线性趋势（内置兜底）、Prophet/LSTM/XGBoost（可选依赖，未装则前端置灰并提示“需安装 xxx”） |
| D5 | 大屏地图示例 **上海**（浦东新区/黄浦区） | 数据与地图是 **四川 17 市州** | **演示区域以四川为准**（用户 2026-10-24 拍板）：引入 `region_profile`（adcode 前缀 + 城市清单），**默认 `sichuan`（510000，17 市州）**；`shanghai`（310000，浦东新区/黄浦区）保留为可切换 profile，仅用于与说明书截图逐张对照。geo 数据已本地化 `web/sichuan_geo.json`，`web/shanghai_geo.json` 按需补 |
| D6 | API 读数 128,460 次 / QPS 428 | 探测器从未启动 | 全部改为真实采样聚合；无采样时显示“暂无采样”，绝不显示假数字 |
| D7 | 大屏统计周期固定 2026-08-15~09-13 | 无 | `dashboard.period` 配置，默认最近 30 天；说明书演示模式下可锁定该区间 |
| D8 | 异常事件/批次/清洗任务支持 **新建/编辑/删除** | 只有清洗任务增删，其余无 | 补 CRUD bridge + 审计（`task_dispatch`/`permission_denied` 之外新增 `record_write` 事件类型） |
| D9 | 推荐召回三通道 | 只有一条规则排序 | 实现 协同过滤（用户-品类共现）/ 内容标签（品类偏好）/ 空间热点（距离+热度），加权融合；冷启动=无历史用户 |
| D10 | “融合数据源规模”指标 | 无 | `COUNT(DISTINCT data_source)` + 已接入类型数 |

---

## 2. 目标架构

### 2.1 分层与依赖方向（只允许上层依赖下层）

```
┌──────────────────────────── 前端 web/index.html ────────────────────────────┐
│ 登录/会话 · 接入台 · 清洗任务 · RFM · 预测异常 · 空间热力 · 情感主题 ·         │
│ 推荐 · 大屏 · 系统设置     ← 全部改为“空态/加载/数据/失败”四态 + 轮询          │
└───────────────────────────────┬──────────────────────────────────────────────┘
                                │ pywebview JS API（每个写操作带 token）
┌───────────────────────────────▼──────────────────────────────────────────────┐
│ bridge_api.py（门面）：鉴权 → 参数校验 → 派发 job → 立即返回 job_id            │
│            只读 API：get_job / list_jobs / get_*_result / get_*_payload        │
└───────────────┬───────────────────────────────┬──────────────────────────────┘
                │                               │
┌───────────────▼─────────────┐   ┌─────────────▼──────────────────────────────┐
│ job_runner（新）            │   │ 查询层 store/*（只读视图 + 聚合 SQL）        │
│ ThreadPoolExecutor(2~4)     │   │ rfm / forecast / spatial / sentiment /      │
│ JobContext.report() → 落库  │   │ recommendation / dashboard                  │
└───────────────┬─────────────┘   └────────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────────────────────┐
│ pipeline/* 业务编排（全部改为 job 函数，签名 fn(ctx, params) -> result）      │
│  ingestion_job（解析+落原始层）· cleaning_fusion_job · rfm_job ·             │
│  forecast_job · spatial_job · sentiment_job · recommendation_job             │
└───────────────┬──────────────────────────────────────────────────────────────┘
                │
┌───────────────▼─────────────────┐  ┌────────────────────────────────────────┐
│ 数据源适配层 datasource/*（新）  │  │ LLM 层 llm/*（新，可插拔）              │
│ file / nbs（国家统计局）/ amap   │  │ provider 协议 · Null 降级 · 缓存 · 审计 │
└───────────────┬─────────────────┘  └────────────────────────────────────────┘
                │
┌───────────────▼──────────────────────────────────────────────────────────────┐
│ SQLite：原始层 ingestion_row / 规范层 consumption_record / 维度 merchant_dim │
│         结果层 rfm_* · consumption_forecast_anomaly · spatial_hotspot_map ·  │
│         sentiment_topic_mining · recommendation_workbench                   │
│         运行层 job_run · job_event · llm_call_log · data_source              │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 一次“导入 → 分析 → 展示”的真实链路（验收主线）

1. 用户拖入 `xxx.csv` → `bridge_api.start_job("ingestion.parse", {"file_paths": [...]}, token)` 返回 `job_id`。
2. `job_runner` 起床：解析 CSV → 每行写 `ingestion_row`（含 `row_hash`、`raw_json`）→ `JobContext.report(progress, stage, processed, total)` 节流写 `job_run/job_event`。
3. 前端每 700ms 调 `get_job(job_id)`，进度条/阶段日志/ETA 全部来自 DB，不再有 sleep。
4. 解析完成自动串联 `cleaning_fusion_job`：读 `ingestion_row` → 去重/对齐/融合 → 写 `consumption_record`（规范层，带 source 溯源列）。
5. 用户在 RFM 页点“执行” → `start_job("rfm.compute", {...})` → 读 `consumption_record` → 写 `rfm_batch/rfm_segment` → 前端轮询完自动 `get_rfm_result(batch_code)` 渲染 ECharts。
6. 情感页：有 LLM Key 走 LLM（批量、缓存、脱敏），没有 Key 走规则基线并明确标注“基线模式”。
7. 空间页：`get_spatial_payload()` 返回 {data, echarts_option}，数据来自 `consumption_record` 聚合 + 高德 POI 密度，不再 fetch 静态 json。
8. 大屏：`get_dashboard_payload()` 聚合各结果表，替换写死的 SVG。

---

## 3. 数据模型（复用现有表 + 新增表）

### 3.1 现有表（保留，尽量不改列）

`user_account` / `password_reset_token` / `audit_log` / `data_ingestion_console`（**语义改为“导入批次”**）/ `cleaning_fusion_pipeline_task`（**语义改为“清洗 job 的展示投影”或直接废弃，见 3.3**）/ `consumption_record`（规范层，已有 `region_code, consumption_amount, occurred_date, occurred_at`）/ `consumption_forecast_anomaly` / `forecast_anomaly_event` / `spatial_hotspot_map` / `sentiment_topic_mining` / `recommendation_workbench` / `realtime_api_monitor`。

### 3.2 新增表（`schema_v2.sql`，幂等 `CREATE TABLE IF NOT EXISTS`）

```sql
-- 数据源登记（文件目录 / 国家统计局 / 高德 / 自有 API）
CREATE TABLE IF NOT EXISTS data_source (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_name TEXT NOT NULL UNIQUE,
    source_kind TEXT NOT NULL,           -- file | nbs | amap | api
    config_json TEXT,                    -- {path, api_base, city_codes, types, schedule}
    enabled INTEGER NOT NULL DEFAULT 1,
    license_note TEXT,                   -- 合规备注：数据来源/授权/使用范围
    last_run_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);

-- 原始层：一行一条原始记录，永不覆盖（可追溯、可重放）
CREATE TABLE IF NOT EXISTS ingestion_row (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_identifier TEXT NOT NULL,      -- 关联 data_ingestion_console.batch_identifier
    source_id INTEGER,
    row_index INTEGER,
    row_hash TEXT NOT NULL,              -- sha256(规范化字段拼接)，跨批次去重用
    raw_json TEXT NOT NULL,
    parse_status TEXT NOT NULL DEFAULT 'ok',  -- ok | error
    error_text TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now','localtime'))
);
CREATE INDEX IF NOT EXISTS idx_ingestion_row_batch ON ingestion_row(batch_identifier);
CREATE INDEX IF NOT EXISTS idx_ingestion_row_hash  ON ingestion_row(row_hash);

-- 任务运行记录（真实进度的唯一来源）
CREATE TABLE IF NOT EXISTS job_run (
    job_id TEXT PRIMARY KEY,             -- job_<epoch_ms>_<rand4>
    job_type TEXT NOT NULL,              -- ingestion.parse | cleaning.fusion | rfm.compute | ...
    params_json TEXT,
    status TEXT NOT NULL DEFAULT 'queued',  -- queued | running | succeeded | failed | canceled
    stage TEXT,                          -- parse | normalize | dedup | align | fuse | persist
    progress_percent INTEGER NOT NULL DEFAULT 0,
    processed_count INTEGER NOT NULL DEFAULT 0,
    total_count INTEGER,
    message TEXT,
    result_json TEXT,
    error_text TEXT,
    operator TEXT,
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_run_status ON job_run(status, created_at);

-- 任务事件流（阶段日志，前端轮询增量拉取）
CREATE TABLE IF NOT EXISTS job_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL,
    seq INTEGER NOT NULL,
    ts TEXT NOT NULL,
    level TEXT NOT NULL DEFAULT 'info',  -- info | stage | warn | error
    stage TEXT,
    message TEXT,
    payload_json TEXT
);
CREATE INDEX IF NOT EXISTS idx_job_event_job ON job_event(job_id, seq);

-- 商户维度（高德 POI / 文件解析统一落到这里）
CREATE TABLE IF NOT EXISTS merchant_dim (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    merchant_name TEXT NOT NULL,
    merchant_norm TEXT,
    category_l1 TEXT, category_l2 TEXT,
    city TEXT, adcode TEXT, lng REAL, lat REAL,
    source TEXT,                          -- file | amap
    updated_at TEXT,
    UNIQUE(merchant_name, city)
);

-- LLM 调用审计 + 响应缓存
CREATE TABLE IF NOT EXISTS llm_call_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    purpose TEXT NOT NULL,               -- sentiment | topics | insight | recommendation
    provider TEXT, model TEXT,
    request_chars INTEGER, response_chars INTEGER,
    latency_ms INTEGER,
    ok INTEGER NOT NULL DEFAULT 1,
    error_text TEXT,
    cache_hit INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key TEXT PRIMARY KEY,          -- sha256(purpose + provider + model + 输入指纹)
    response_text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT
);
```

`consumption_record` 建议补 3 列（用 `ALTER TABLE` 迁移，重复执行要捕获 “duplicate column”）：`source_kind TEXT`、`batch_identifier TEXT`、`raw_row_id INTEGER`，用于“哪条统计/文件产生的这个数”的溯源（答辩加分项）。

### 3.2.1 说明书功能对应的新增表（`schema_v2.sql`）

```sql
-- 全局参数与向导状态（说明书 11.1 / 11.2）
CREATE TABLE IF NOT EXISTS app_setting (
    setting_key   TEXT PRIMARY KEY,
    setting_value TEXT NOT NULL,
    value_kind    TEXT NOT NULL DEFAULT 'text',   -- text|number|bool|json
    updated_at    TEXT NOT NULL DEFAULT (datetime('now','localtime')),
    updated_by    TEXT
);

-- 批次预处理校验结果（说明书 2.3 “格式校验提示发现多少条异常”）
CREATE TABLE IF NOT EXISTS ingestion_validation (
    validation_code TEXT PRIMARY KEY,
    batch_identifier TEXT NOT NULL,
    valid_rows   INTEGER NOT NULL DEFAULT 0,
    error_rows   INTEGER NOT NULL DEFAULT 0,
    warning_rows INTEGER NOT NULL DEFAULT 0,
    report_json  TEXT,                 -- 每条异常：行号/字段/原因/建议
    created_at   TEXT NOT NULL
);

-- 原始行（真实数据流的地基）
CREATE TABLE IF NOT EXISTS ingestion_row (
    row_id       INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_identifier TEXT NOT NULL,
    source_kind  TEXT NOT NULL,        -- catering|tourism|retail|nbs|amap
    line_no      INTEGER NOT NULL,
    row_hash     TEXT NOT NULL,        -- 去重指纹
    raw_json     TEXT NOT NULL,        -- 原始结构，永不改写
    parse_status TEXT NOT NULL DEFAULT 'ok',  -- ok|error|skipped
    parse_error  TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ingestion_row_batch ON ingestion_row(batch_identifier, parse_status);

-- RFM（说明书 5.2 / 5.3）
CREATE TABLE IF NOT EXISTS rfm_batch (
    batch_code   TEXT PRIMARY KEY,
    analysis_window TEXT NOT NULL,
    segment_count INTEGER NOT NULL DEFAULT 5,
    total_customers INTEGER NOT NULL DEFAULT 0,
    total_records INTEGER NOT NULL DEFAULT 0,
    high_value_ratio REAL NOT NULL DEFAULT 0,
    excluded_refund_count INTEGER NOT NULL DEFAULT 0,
    params_json  TEXT,
    elapsed_ms   INTEGER,
    generated_at TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'completed'
);
CREATE TABLE IF NOT EXISTS rfm_segment (
    batch_code   TEXT NOT NULL,
    segment_label TEXT NOT NULL,
    customer_count INTEGER NOT NULL DEFAULT 0,
    customer_ratio REAL NOT NULL DEFAULT 0,
    median_recency_days REAL,
    median_frequency REAL,
    median_monetary REAL,
    suggestion   TEXT,
    PRIMARY KEY (batch_code, segment_label)
);
CREATE TABLE IF NOT EXISTS rfm_user_snapshot (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_code   TEXT NOT NULL,
    customer_identifier TEXT NOT NULL,
    customer_name TEXT,
    segment_label TEXT NOT NULL,
    recency_days INTEGER, frequency_count INTEGER, monetary_amount REAL, average_order_value REAL,
    r_score INTEGER, f_score INTEGER, m_score INTEGER, composite_score REAL,
    last_consumption_at TEXT, registered_at TEXT, city TEXT, preferred_category TEXT,
    UNIQUE (batch_code, customer_identifier)
);

-- 异常事件（说明书 6.3：编号/发生时间/来源/区域/金额/偏离度/类型/处理状态）
CREATE TABLE IF NOT EXISTS anomaly_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_code   TEXT NOT NULL UNIQUE,
    occurred_at  TEXT NOT NULL,
    source_kind  TEXT,
    region_code  TEXT, region_name TEXT,
    consumption_amount REAL,
    deviation_ratio REAL,
    anomaly_type TEXT,          -- 金额突增|金额骤降|频次异常|客单价异常|品类偏好偏移
    z_score REAL, level TEXT,
    status       TEXT NOT NULL DEFAULT '待处理',
    detected_at  TEXT, updated_at TEXT, operator TEXT
);

-- 空间异常信号（说明书 7.3）
CREATE TABLE IF NOT EXISTS spatial_anomaly_signal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_code  TEXT UNIQUE,
    region_name  TEXT,
    level        TEXT NOT NULL DEFAULT '中',        -- 高|中|低
    status       TEXT NOT NULL DEFAULT '待处理',    -- 待处理|观察中|已确认|已关闭
    density_change_ratio REAL,
    threshold_ratio REAL,
    description  TEXT,
    suggestion   TEXT,
    triggered_at TEXT,
    source_kind  TEXT
);

-- 推荐策略与指标（说明书 8.1 / 8.2）
CREATE TABLE IF NOT EXISTS recommendation_strategy (
    strategy_version TEXT PRIMARY KEY,
    recall_channels_json TEXT NOT NULL,  -- ["collaborative","content","spatial"]
    recall_size_per_channel INTEGER NOT NULL DEFAULT 20,
    min_confidence REAL NOT NULL DEFAULT 0.3,
    behavior_lookback_days INTEGER NOT NULL DEFAULT 90,
    ranking_json TEXT, filter_json TEXT, profile_json TEXT,
    updated_at TEXT NOT NULL
);

-- 服务健康（说明书 9.2 四组指示灯）
CREATE TABLE IF NOT EXISTS service_health (
    service_name TEXT PRIMARY KEY,
    service_group TEXT NOT NULL,        -- endpoint|datasource|dependency|env
    status       TEXT NOT NULL DEFAULT 'unknown',  -- running|warning|down|unknown
    detail       TEXT,
    latency_ms   REAL,
    last_probe_at TEXT
);
```

旧表补列（`ALTER TABLE … ADD COLUMN`，迁移脚本先 `PRAGMA table_info` 去重）：

| 表 | 新增列 |
|---|---|
| `data_ingestion_console` | `subtitle TEXT`、`priority INTEGER DEFAULT 0`、`error_rows INTEGER DEFAULT 0`（状态四态化） |
| `cleaning_fusion_pipeline_task` | `subtitle TEXT`、`priority INTEGER DEFAULT 0`、`submitter TEXT`、`source_table TEXT`、`target_table TEXT` |
| `forecast_anomaly_event` | `event_code TEXT`、`anomaly_type TEXT`、`status TEXT DEFAULT '待处理'`、`operator TEXT`、`updated_at TEXT` |
| `recommendation_workbench` | `strategy_version TEXT`、`coverage_ratio REAL`、`hit_ratio REAL`、`avg_items REAL`、`diversity_index REAL`、`cold_start_ratio REAL` |

### 3.3 迁移策略（不破坏现有可运行状态）

- 新增 `schema_v2.sql`，`main.py` 启动时先跑 v1 再跑 v2；`store/db_connection.py:initialize_schema()` 保持幂等。
- `cleaning_fusion_pipeline_task` **暂不删除**：清洗 job 启动时仍插一条（展示用），进度以 `job_run` 为准。
- RFM 表修复二选一（推荐 A）：
  - **A. 新表**：`rfm_batch(batch_code PK, analysis_window, total_customers, total_monetary, generated_at)` + `rfm_segment(batch_code, segment_label, customer_count, ratio, PRIMARY KEY(batch_code, segment_label))`；旧表保留只读。
  - B. 旧表 `id` 改为自增 + 加 `batch_code` 列（SQLite 改 PK 需重建表，成本高）。
- 迁移脚本 `scripts/migrate_v2.py`：可重复执行，先 `PRAGMA table_info` 判断，再 `ALTER/INSERT`，输出迁移报告。

---

## 4. 任务系统与“真实进度”

### 4.1 核心契约

```python
# pipeline/job_runner.py（新增）
class JobCanceled(Exception): ...

class JobContext:
    job_id: str
    def report(self, *, progress=None, stage=None, processed=None, total=None,
               message=None, level="info", payload=None) -> None:
        """节流：同阶段同进度最多 5 次/秒；每次写 job_run，关键阶段写 job_event。"""

    def log(self, message, level="info", payload=None) -> None: ...
    def check_cancel(self) -> None:  # 读 job_run.status == 'canceled' 则抛 JobCanceled
        ...

def submit_job(job_type: str, params: dict, operator: str) -> str:
    """写 job_run(queued) → ThreadPoolExecutor.submit(_execute) → 立即返回 job_id"""

def _execute(job_id, job_type, params, operator) -> None:
    """status=running → handler(ctx, params) → succeeded/failed 落库；异常写 error_text + error 事件"""
```

- 线程池：`max_workers=2`（SQLite 写串行 + 桌面端不抢 CPU）；后续要并行再调。
- handler 注册表与 `task_dispatch_service._TASK_DISPATCH_TABLE` 解耦：**新 job_type 用新表 `_JOB_HANDLERS`**，旧 `run_task` 保留兼容（内部转成同步执行，避免破坏已通过的验证脚本）。
- 取消：`cancel_job(job_id)` 置 `status='canceled'`，handler 在每个阶段边界 `ctx.check_cancel()`。

### 4.2 桥接 API 新增（都要 `token` 鉴权 + 角色矩阵补 endpoint）

| 方法 | 入参 | 返回 |
|---|---|---|
| `start_job(job_type, params, token)` | 见上 | `{success, job_id}` |
| `get_job(job_id, after_seq=0)` | | `{success, job:{status,progress,stage,processed,total,elapsed_ms,eta_ms}, events:[{seq,ts,level,message}]}` |
| `list_jobs(job_type=None, limit=20)` | | `{success, jobs:[...]}` |
| `cancel_job(job_id, token)` | | `{success}` |
| `get_dashboard_payload(token)` | | `extensions.echarts_dashboard_feed.build_dashboard_payload()` 的真实聚合 |
| `get_spatial_payload(token)` | | `{data:[{city,heat_score,retail_total_yi,...}], echarts_option}` |
| `get_rfm_result(batch_code="")` | 空=最新 | 批次 + 分层明细 + 8 类占比 |
| `get_forecast_result(region_code)` | | 预测曲线 + 异常事件列表 |
| `get_sentiment_result(batch_code="")` | | 情感分布 + 主题簇 + 样本评论 |
| `probe_api_source(source_id, token)` | | 立即探测一次并写 `realtime_api_monitor` |
| `create_ingestion_batch(fields, token)` / `update_ingestion_batch(id, fields, token)` | 说明书 2.2.3 字段（标题/副标题/状态/进度/时间/操作人/真源载荷/元数据） | `{success, batch_identifier}` |
| `list_anomaly_events(limit)` / `create_anomaly_event(fields, token)` / `update_anomaly_event(id, fields, token)` / `delete_anomaly_event(id, token)` | 说明书 6.3 八个字段 | 列表 / `{success}` |
| `get_recommendation_payload(token)` | | 策略版本 + 五项指标 + 两类样例 |
| `get_service_health(token)` | | 四组指示灯 + 近 5 分钟调用统计 + 日志 |
| `get_app_settings(token)` / `save_app_settings(fields, token)` | 说明书 11.2 十三项 | 已保存参数（带 `value_kind`） |

### 4.2.1 job_type 注册表（与说明书章节一一对应）

| job_type | 章节 | 输入参数 | 主要产出 |
|---|---|---|---|
| `ingestion.parse` ✅ | 2.1 / 2.3 | `file_paths[]`、`operator`、`region_code` | S1 已实现：解包/逐文件解析计数/登记批次（预处理中→已完成），按文件数推进真实进度；S2 再落 `ingestion_row`、`ingestion_validation` |
| `ingestion.register` ✅ | 2.2.3 | `source_name`、`records[]`（演示数据） | S1 已实现：字段校验 → 登记一条批次 → 100%；S2 起改走真实解析 |
| `nbs.fetch` | 5.3 | `cids[]`、`period_shards[]` | `ingestion_row`(source_kind=nbs) → 季度/月度宏观序列 |
| `amap.poi.fetch` | 5.4 | `city`、`adcode`、`grid`、`types` | `merchant_dim` + `ingestion_row`(amap) |
| `cleaning.fusion` | 3.2 | `batch_identifiers[]` | 7 阶段进度 + `consumption_record` |
| `sentiment.mine` | 4 | 12 项参数 | `sentiment_topic_mining` + 摘要指标 + 主题簇 |
| `rfm.compute` | 5 | 权重/分箱/分层数/窗口/排退款/归一化 | `rfm_batch` + `rfm_segment` + `rfm_user_snapshot` |
| `forecast.detect` | 6 | 窗口(默认14)/模型/阈值/维度 | `consumption_forecast_anomaly` + `anomaly_event` |
| `spatial.aggregate` | 7 | 网格粒度(coarse/medium/fine) | `spatial_hotspot_map` + `spatial_anomaly_signal` |
| `recommendation.generate` | 8 | 策略版本/召回三通道/数量/置信度/回溯窗 | `recommendation_workbench` + 指标 |
| `api.probe` | 9 | `endpoints[]` | `realtime_api_monitor` + `service_health` |

状态映射（只在**渲染层**做中文转换，后端一律英文）：`queued→排队中`、`running→运行中/预处理中`、`validating→格式校验`、`succeeded→已完成`、`failed→失败`、`canceled→已取消`。批次表另保留“已接入”作为 `queued` 的初始别名以兼容旧数据。

### 4.3 前端轮询协议（替换假进度）

```js
async function runJobAndTrack(jobType, params, {onProgress, onLog}) {
  const started = await pywebview.api.start_job(jobType, params, state.token);
  if (!started.success) return started;
  let seq = 0;
  return await new Promise((resolve) => {
    const timer = setInterval(async () => {
      const r = await pywebview.api.get_job(started.job_id, seq);
      if (!r.success) return;
      const job = r.job;
      (r.events || []).forEach(e => { seq = Math.max(seq, e.seq); onLog && onLog(e); });
      onProgress && onProgress(job);
      if (["succeeded","failed","canceled"].includes(job.status)) { clearInterval(timer); resolve(job); }
    }, 700);
  });
}
```

约束：单页最多 1 个轮询器；页面切换时 `renderPage()` 先 `clearInterval`；job 进入终态后自动刷新本页数据（`loadIngestionAll` / `get_rfm_result` 等）。

---

## 5. 数据接入设计（真实数据从哪来）

### 5.1 统一适配器协议

```python
# datasource/base.py
@dataclass
class FetchRequest:  # 参数全 JSON 可序列化，便于写进 job_run.params_json
    city_codes: list[str] = field(default_factory=list)
    start_period: str = ""
    end_period: str = ""
    page: int = 1
    extra: dict = field(default_factory=dict)

class DataSourceAdapter(Protocol):
    kind: str                       # file | nbs | amap
    def validate(self) -> None: ...  # 缺 key / 路径不存在时抛带中文提示的异常
    def fetch(self, req: FetchRequest) -> list[dict]: ...  # 返回“原始行 dict”，不负责落库
    def describe(self) -> dict: ...  # {name, license, rate_limit, fields}
```

落库统一由 `pipeline/ingestion_job.py` 做：`source_adapter.fetch()` → `ingestion_row` + `data_ingestion_console`（批次）→ 返回 `{rows, skipped, errors}`。这样“文件/统计局/高德”走同一条入库流水线。

### 5.2 文件适配器（第一步落地，零外部依赖）

- 支持 `.csv / .json / .xlsx / .xls / .zip`；xls 用 `xlrd`（需加依赖）或先用 `pandas.read_excel(engine="xlrd")`；zip 解压到 `store/unpacked/<stem>_<HHMMSS>/`。
- 列名别名表复用 `cleaning_fusion_service.FIELD_ALIAS_TABLE`；解析出 `row_index, raw_json, row_hash`，坏行写 `parse_status='error'` 而非整批失败。
- 产出“校验报告”：总行数/成功/失败/缺列/重复 hash 数，前端在批次详情里展示。

### 5.3 国家统计局（国家数据）适配器

- 入口：`https://data.stats.gov.cn/dg/website/publicrelease/web/external`（公开，无需登录）。
- 机制：`cid`（指标集合 UUID，从目录树接口发现）+ 时间分片（2016–2020 / 2021–2025 / 2026–），返回 JSON 后按 `code/name/value` 归一。
- 目标指标（建议）：社会消费品零售总额、居民人均消费支出、分行业零售额（餐饮/零售/旅游相关）。
- 落库形态：`consumption_record`（region_code=行政区划码，occurred_date=期末日期，amount=金额，source_kind='nbs'）；粒度是**城市/省/月**，不是单笔订单——这点在毕设里要说清楚“宏观指标 + 微观流水”的融合。
- 合规：只取公开汇总字段；请求间隔 ≥500ms；失败重试 2 次；把 `cid/period` 写进 `job_event` 便于复核。

### 5.4 高德 POI 适配器

- 用途：商户/门店维度（餐饮 050000、购物 060000、生活服务 070000 等细类），以及网格密度；**不做消费金额**（高德没有交易数据）。
- 配额：个人开发者 3000 次/日、单次最多 200 条（以控制台实际为准，代码里做 `AMAP_DAILY_QUOTA` 软限制 + 本地缓存 7 天）。
- 分块：按城市 → 区县 adcode → 矩形网格（lng/lat 步长约 0.02°）拉取，`types` 用细分码，结果写 `merchant_dim` + `ingestion_row`（source_kind='amap'）。
- 结算：`heat_score = 0.6*POI密度分 + 0.4*统计公报消费额分`（权重可配，写进 `data_source.config_json`）。

### 5.5 密钥与配置

`settings.py` 增加（全部 `os.environ` 优先，缺省值保证离线可跑）：

```
LLM_ENABLED=false
LLM_BASE_URL=https://api.deepseek.com/v1
LLM_MODEL=deepseek-chat
LLM_API_KEY=
NBS_API_BASE=https://data.stats.gov.cn
AMAP_KEY=
AMAP_DAILY_QUOTA=3000
```

`.env`（本地、**加入 .gitignore**）+ `.env.example`（入库、只有键名）。禁止把 key 写进 `settings.py` 默认值。

---

## 6. LLM 接口（预留，先可插拔后接真实模型）

### 6.1 目录与协议

```
llm/
├── __init__.py
├── base.py            # LlmMessage / LlmResult / LlmProvider(Protocol) / LlmError
├── config.py          # LlmConfig.from_settings()；enabled 由 LLM_ENABLED + key 是否为空决定
├── service.py         # LlmService：业务方法 + 缓存 + 审计 + 降级
├── cache.py           # llm_cache 读写（sha256 key）
└── providers/
    ├── openai_compat.py   # /chat/completions 兼容：DeepSeek/通义/Kimi/OpenAI/vLLM/Ollama
    └── null_provider.py   # 未配置时抛 LlmNotConfigured，由调用方降级为规则
```

```python
# llm/base.py
@dataclass(frozen=True)
class LlmMessage:
    role: str      # system | user | assistant
    content: str

@dataclass(frozen=True)
class LlmResult:
    text: str
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: int = 0

class LlmProvider(Protocol):
    name: str
    def is_available(self) -> bool: ...
    def chat(self, messages: list[LlmMessage], *, temperature: float = 0.2,
             max_tokens: int = 1024, json_mode: bool = False,
             timeout_seconds: float = 30.0) -> LlmResult: ...
```

### 6.2 业务方法（每个都有规则降级）

```python
class LlmService:
    def analyze_reviews(self, reviews: list[str]) -> dict:
        """→ {items:[{index,sentiment,score,topics,keywords}],
              summary:{positive,neutral,negative}, model:"llm"|"rule"}"""

    def name_topics(self, clusters: list[dict]) -> list[dict]:
        """→ [{topic_id,label,keywords}]；失败时回退 text[:8]"""

    def summarize_insights(self, metrics: dict, *, max_points: int = 4) -> str:
        """→ Markdown 3~4 条结论；失败时回退模板句"""

    def explain_recommendations(self, segment: str, items: list[dict]) -> list[str]:
        """→ 每条推荐的“为什么推荐”，失败时回退规则理由"""
```

### 6.2.1 与说明书第 4 章“情感模型下拉”的映射（避免“选项是假的”）

| 说明书选项 | 本地/规则模式 | LLM 模式 |
|---|---|---|
| SnowNLP 规则基线 | 首选 `snownlp`（轻依赖）；未装则用现有词袋 `sentiment_topic_service._POSITIVE_WORDS/_NEGATIVE_WORDS` | — |
| BERT-wwm-ext / RoBERTa-wwm-ext | 未安装 `transformers` 时下拉置灰，tooltip 提示安装命令 | 映射为 prompt 模板 + `LLM_MODEL` 别名；实际模型名写进 `sentiment_topic_mining.model_name` 与 `llm_call_log` |
| LSTM 融合模型 | 用“规则分 + 置信度加权”实现，UI 标注“实验性” | 同 BERT 行，温度更低、要求输出 JSON |

主题模型同理：`LDA`（sklearn LatentDirichletAllocation）、`NMF`（sklearn）必装可用；`BTM`（`biterm`）与 `BERTopic`（需 sentence-transformers）为可选依赖，未装即禁用。**任何不可用的选项都不允许静默回退成另一个算法**，必须让用户看见“未启用 + 原因”。

### 6.3 工程约束（答辩会被问）

- **脱敏**：发给 LLM 的只有评论正文/商户名/聚合指标，`user_identifier` 一律替换为 `u_<hash前8位>`。
- **成本**：单次 job 设 token 预算（默认 2 万），超预算截断并记 `llm_call_log`；批量 50~100 条/请求。
- **稳定性**：超时 30s、重试 2 次（指数退避）、`json_mode` 解析失败先修复再重试一次，仍失败→降级。
- **可观测**：每次调用写 `llm_call_log`（purpose/model/latency/token/ok/error/cache_hit），大屏“模型调用”卡片直接用这张表。
- **开关**：`LLM_ENABLED=false` 时全链路走规则，前端显示“基线模式（未配置大模型）”，保证答辩断网也能演示。

---

## 7. 前端改造要点

1. **去掉 CDN 依赖**：把 `echarts.min.js` 下载到 `web/vendor/echarts.min.js`，`<script src="vendor/echarts.min.js">`；`sichuan_geo.json` 已本地化，保留。
2. **四态渲染**：每个页面统一 `renderEmpty / renderLoading / renderError / renderData`；禁止再出现写死的 `124,419 条`。
3. **轮询器生命周期**：`renderPage()` 入口统一 `stopAllPollers()`；job 终态后刷新本页查询。
4. **进度组件**：`progress-fill` 宽度绑 `job.progress_percent`，下方显示 `stage` 中文名、`processed/total`、`eta_ms`、最近 5 条 `job_event`。
5. **空间页**：删除 `fetch('heatmap_data.json')`，改 `pywebview.api.get_spatial_payload(token)`；无数据时显示“请先接入数据并执行空间聚合”。
6. **大屏**：改 `get_dashboard_payload(token)`，ECharts 4 图（趋势/分层饼/热力/情感），数据为空时显示空态而不是假数字。
7. **设置页**：参数真正保存到 `data_source.config_json` / `settings` 表（新增 `app_setting(key,value)` 或在 `data_source` 存 JSON）；LLM 配置区显示“已启用/基线模式”。

---

## 8. 开发路线图（每步都可独立验收，按依赖排序）

> 原则：每步结束项目必须能 `python main.py` 正常打开并演示；不允许“半截状态”。每步产出后更新本文档「变更记录」。

| Step | 目标 | 主要产出 | 验收标准 |
|---|---|---|---|
| **S0** ✅ | Git 基线（2026-10-24 完成，基线提交 `ea55245`） | `.gitignore` 补 `*.db`/`store/unpacked/`/`.env`/`docs/_tmp/`；移除拼写错误的 `origih` remote；补 `README.md`；`git rm --cached` 清理已入库的 `__pycache__`、`local_consumption_insight.db`、`store/unpacked/`；提交 `docs/tasks/` | `git status` 干净 ✅；`git log` 有基线提交 ✅；GitHub master 与本地 HEAD 一致 ✅ |
| **S1** ✅ | 任务基础设施（2026-10-24 完成） | 新增 `pipeline/job_runner.py`（JobContext/线程池 `max_workers=2`/submit_job）、`pipeline/ingestion_job.py`（`ingestion.parse`、`ingestion.register` 两个真实 handler）、`schema_v2.sql`（`job_run`/`job_event`）、`store/job_store.py`、`bridge.start_job/get_job/list_jobs/cancel_job`；接入台进度改为 700ms 轮询 + 真实日志面板 | 点“导入数据/拖文件”后进度条由 `job_run` 驱动 ✅；日志面板出现真实阶段事件 ✅；`list_ingestion_records` 不再按时间伪造进度 ✅；演示任务/真实文件/失败/取消/权限五类路径自测通过 ✅（临时脚本已删） |
| **S2** | 文件真实解析 | `datasource/file_adapter.py`、`pipeline/ingestion_job.py`（写 `ingestion_row` + 校验报告） | 导入 CSV 后 `ingestion_row` 有 N 行；详情页显示 成功/失败/重复 行数；坏文件给出中文错误而非崩溃 |
| **S3** | 清洗融合接真实数据 | 改造 `cleaning_fusion_service` 读 `ingestion_row`；**7 阶段**（接入15/去重25/缺失15/对齐15/融合15/校验10/输出5）；写 `consumption_record`（含溯源列）；`cleaning_fusion_job` | 清洗 job 七阶段真实推进；`consumption_record` 行数 = 去重后行数；重复导入不重复入库；日志面板出现 INFO/WARN/ERROR |
| **S4** | 统计局适配器 | `datasource/nbs_adapter.py` + `spatial_job` 聚合（城市级）；`region_profile`（**默认四川 510000，17 市州**；保留上海 310000 可切换） | 能拉到 profile 对应城市月度社零/人均支出并落库；空间页 KPI/排名来自 DB；断网时优雅降级为本地已有数据 |
| **S5** | 高德适配器 | `datasource/amap_adapter.py`、`merchant_dim` 填充、配额/缓存 | 指定城市能拉到 POI 并落 `merchant_dim`；重复拉取命中缓存不耗配额；配额用尽有明确提示 |
| **S6** | RFM 真实化（第 5 章） | 修 `rfm_segmentation_store`（新表方案 A）、`rfm_batch/rfm_segment/rfm_user_snapshot`、`rfm_job`、`get_rfm_result`、前端绑定 | 点“执行”后按 `segment_count`（**UI 默认 5 层，内部 8 类全量落库**）出现真实分层占比与中位数；明细可按用户查 R/F/M 得分与原始 8 类标签；批次可回看；并发重复点击不会写坏表 |
| **S7** | 预测/异常真实化（第 6 章） | `forecast_job`（ARIMA 必装 + 基线兜底 + Prophet/LSTM/XGBoost 可选）、`anomaly_event` 表与 CRUD、`get_forecast_result`、前端图表 | 选窗口/模型/阈值后曲线与异常点来自真实序列；异常清单可新建/编辑/删除并审计；未装依赖的模型在 UI 置灰 |
| **S8** | LLM 层（第 4 章） | `llm/*` 全量 + `sentiment_job` 走 LLM/降级 + 摘要 8 指标/5 结论 + `get_sentiment_result` + 前端 | 无 key 时“基线模式”可演示；有 key 时评论情感/主题名/结论由模型生成并写 `llm_call_log`；未启用模型在下拉中置灰 |
| **S9** | 推荐工作台（第 8 章） | 召回三通道（协同过滤/内容标签/空间热点）+ 排序融合；`recommendation_job`、`recommendation_strategy`、`get_recommendation_payload` | 勾选通道/改召回数量/最低置信度后结果变化；覆盖率/命中率/人均条数/多样性真实计算；无历史用户走冷启动通道 |
| **S10** | 准实时 API 监测台（第 9 章） | 启动 `RealtimeApiProbe`、`service_health` 四组、5 分钟调用量与 QPS 聚合、运行日志页 | 读数随真实探测变化；服务标签出现 warning；日志可按 INFO/WARN/ERROR 过滤；断网时显示“暂无采样”而非假数字 |
| **S11** | 大屏 + 设置向导（第 10/11 章） | `get_dashboard_payload`、ECharts 四图（趋势/分层/热力/情感）、`app_setting` 十三参数、四步向导、CSV/PNG 导出 | 大屏六卡与四图来自 DB；空库显示空态；改参数后分析结果变化；导出文件可打开 |
| **S12** | 工程收尾 | 日志/审计补全、幂等与失败重试、`requirements-dev.txt`、答辩演示脚本 + 架构图/ER 图 | 全新机器按 README 能跑通 S1–S11 验收；`_full_verify.py` 扩展到 S12 全 PASS |

### 8.1 章节 ↔ Step 对照（方便答辩时按说明书讲）

| 说明书章节 | 对应 Step |
|---|---|
| 第 2 章 数据接入台 | S1（任务/进度）+ S2（真实解析/校验） |
| 第 3 章 清洗融合流水线 | S3 |
| 第 4 章 评论情感与主题挖掘 | S8（LLM 层 + 规则基线） |
| 第 5 章 RFM 分层画像 | S6 |
| 第 6 章 消费预测与异常检测 | S7 |
| 第 7 章 空间热点与区域分布 | S4（统计局）+ S5（高德）+ 空间聚合 |
| 第 8 章 个性化推荐 | S9 |
| 第 9 章 准实时 API 监测台 | S10 |
| 第 10 章 可视化大屏 | S11 |
| 第 11 章 系统初始化与参数设置 | S11（`app_setting` + 向导） |


### 8.2 立刻要修的 Bug（S1 前顺手修，避免踩坑）

1. `pipeline/hotspot_data_fetcher.py:172-266`：`generate_echarts_heatmap_option` / `save_heatmap_data_to_db` 缩进错误（定义在 `fetch_real_consumption_data()` 的 `return` 之后）→ 移到模块顶层。
2. `store/rfm_segmentation_store.py:206-224`：`rfm_segmentation_studio.id` 为 TEXT PRIMARY KEY 却按 segment 插多行 → 换新表（3.3 方案 A）。
3. `main.py:120-128`：启动强制“已完成 100%” → 删除，改为只修正 `progress` 超界的脏数据。
4. ✅ `bridge_api.py` 时间/ sleep 假进度（原 173-207、484-528 行）→ S1 已删除，改为 `job_run` 驱动。
5. `bridge_api.py:511-517`：清洗任务缺 `pipeline_task_identifier` 时默认“全部批次”，范围失控 → job 参数必填或显式传 `batch_identifiers`。
6. `web/index.html:6`：ECharts CDN → 本地 vendor（S9 前必须做，答辩断网）。
7. `store/account_store.py:53` 等用 `with get_connection() as connection:`：`sqlite3.Connection` 的上下文管理器只提交/回滚、**不关闭连接** → 长期运行泄漏连接（改为 `contextlib.closing` 或统一 `with closing(create_connection()) as conn`）。

---

## 9. 风险与答辩要点

- **数据真实性**：统计局是宏观汇总、高德是 POI 维度，二者都不能提供“单笔订单流水”。答辩口径要提前准备：**宏观指标 + 模拟/脱敏微观流水 + 融合口径说明**；演示数据必须标注来源与生成方式。
- **合规**：只调用公开 API/公开页面；遵守 robots.txt 与配额；不采集个人身份信息；评论数据若来自公开平台需注明用途与清洗方式。
- **断网演示**：ECharts 本地化、LLM 基线降级、统计局数据落库后离线可读——三条兜底。
- **性能**：SQLite 写入串行；`ingestion_row` 超过 50 万行时按批次归档（`store/archive/`），查询加索引；前端列表一律分页。

---

## 10. 变更记录（每步追加）

| 日期 | Step | 改动摘要 | 验证方式 |
|---|---|---|---|
| 2026-10-24 | 设计 | 通读代码，产出本架构文档；确认 3 项根因（无原始层/无任务运行器/无结果回读）与 7 个待修 Bug | 文档评审 |
| 2026-10-24 | 设计 v1.1 | 合并软著《用户说明书 V1.0》（11 章 49 页）：新增 1.4 功能基线逐章表、1.5 十项差异决策（D1 七阶段清洗/D2 四态/D3 五分层/D4 四预测模型/D5 上海默认 region profile/D6 真实采样/D7 周期可配/D8 增删改审计/D9 三通道召回/D10 数据源规模）；3.2.1 新增 8 张表与 4 组补列；4.2.1 job_type 注册表；6.2.1 情感模型下拉映射；路线图扩为 S0–S12 并加章节对照 | 说明书逐章对照本表；文档评审 |
| 2026-10-24 | 设计 v1.2 | 按用户拍板修正两处口径：**D5 演示区域改为四川（510000，17 市州），上海 profile 降为可切换对照项**；**D3/S6 明确 RFM 计算层 8 类全量落库、UI 默认展示 5 层归并结果（`app_setting.rfm.display_layers` 可切 5/8）** | 用户明确要求：演示区域用四川；RFM 默认 UI 展示 5 层 |
| 2026-10-24 | S0 | 仓库基线：重写 `.gitignore`（新增 `*.db`/`store/unpacked/`/`.env`/`docs/_tmp/`）；`git rm --cached` 清掉 45 个构建产物（`__pycache__` 27 个、`local_consumption_insight.db`、`store/unpacked` 17 个样例文件）；新增 `README.md`；删除拼错的 `origih` remote，仅保留 `origin`（SSH）；基线提交 `ea55245` 已推送到 GitHub。注：本机受限沙箱内 Git 经 msys `sh.exe` 调 ssh 会被拒，推送需一次性放宽权限执行 | `git status` 干净；本地 HEAD = 远端 master = `ea55245`；`gh repo view` 显示 pushedAt 已更新 |
| 2026-10-24 | S1 | 任务基础设施落地：新增 `pipeline/job_runner.py`（`JobContext.report/log/check_cancel`、节流 0.2s、`ThreadPoolExecutor(max_workers=2)`、`submit_job` 立即返回 `job_id`、失败写 `error_message` + ERROR 事件）、`pipeline/ingestion_job.py`（`ingestion.parse`：zip 解包/后缀校验/逐文件解析计数/真实进度；`ingestion.register`：演示批次字段校验）、`schema_v2.sql` 的 `job_run`/`job_event`、`store/job_store.py`；桥接新增 `start_job/get_job/list_jobs/cancel_job` 并补角色矩阵 `/api/job/start|read|cancel`；删除 3 处伪造进度（`list_ingestion_records` 时间推算、`_advance_batch_status_async` sleep 线程、`main.py` 启动强置完成）；`ingest_local_files`/`import_demo_batch` 改为提交后台任务并回传 `job_id`；`main.py` 拖拽回调改为通知前端 `trackIngestionJob(job_id)`；`web/index.html` 新增任务面板（真实进度条/阶段/已用与预计剩余/滚动日志）与 700ms 轮询器 | 自测：演示批次成功（4 条记录）、真实 CSV + zip 导入成功（2 个源、2 条记录、12 条事件）、坏路径任务 `failed` 带中文错误、已结束任务取消返回提示、未知 job_type 被拒、无 token 被拒（角色矩阵生效）；`get_job`/`list_jobs` 的 `elapsed_ms/eta_ms` 正常返回 |
