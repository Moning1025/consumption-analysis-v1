# 多源数据融合的本地消费分析系统

一个**完全跑在本机**的消费数据分析系统：桌面窗口打开，数据不出本地，从多来源数据导入 → 清洗融合 → RFM / 预测异常 / 空间热力 / 情感主题 / 推荐 → 可视化大屏。

> 毕业设计项目 · 当前处于重构阶段（S0 基线已完成，S1 任务运行器开发中）

---

## 1. 技术栈

| 层 | 选型 |
|---|---|
| 桌面外壳 | pywebview 6.2.1（Python ↔ 前端 JS 双向调用） |
| 前端 | 原生 HTML/CSS/JS 单页（`web/index.html`）+ ECharts |
| 后端 | Python 3.10.9，无 Web 框架，业务编排在 `pipeline/` |
| 存储 | SQLite（单文件 `local_consumption_insight.db`，WAL 模式） |
| 分析 | pandas / numpy / matplotlib / seaborn（scikit-learn、statsmodels 为后续新增） |

## 2. 环境要求

- Windows 10/11 x64
- Python **3.10.9**（`python --version` 核对）
- WebView2 Runtime（Win11 自带；Win10 若窗口打不开需装）
- 依赖见 `requirements.txt`

## 3. 快速开始

```bash
# 1) 建虚拟环境（只需一次）
python -m venv venv

# 2) 激活（PowerShell）
.\venv\Scripts\Activate.ps1
# 若提示禁止运行脚本：Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

# 3) 装依赖
pip install -r requirements.txt

# 4) 启动
python main.py
```

首次启动会自动建库（`schema.sql`）并创建默认管理员：

- 账号 `admin` / 密码 `admin123456`

## 4. 目录结构

```
main.py                 程序入口：建库、播种管理员、创建窗口、注册拖拽导入
bridge_api.py           前后端唯一门面（pywebview JS API）：鉴权 → 校验 → 派发任务
settings.py             配置（数据库路径、会话密钥、日志级别）
schema.sql              建表脚本（当前 13 张表）
logging_dict_config.py  日志配置
auth/                   账号、凭据（PBKDF2）、会话令牌（HMAC）、权限策略、登录审计
store/                  数据访问层：每张表一个 store 模块，统一走 db_connection
pipeline/               业务编排：接入、清洗融合、RFM、预测异常、空间热力、情感主题、推荐
extensions/             可选扩展：大屏 payload 组装、API 探测器（尚未接线）
web/                    前端单页 + 本地 geo 数据
docs/                   设计文档（ARCHITECTURE.md）、任务书、agent 规范
qa-results/             历史验收记录
store/unpacked/         导入文件解包目录（运行时生成，不入库）
```

## 5. 文档索引

- `docs/ARCHITECTURE.md` —— **设计基线**（v1.2）：目标架构、数据模型、真实数据源接入、LLM 层、路线图 S0–S12
- `docs/tasks/` —— 从零复现与真实数据接入任务书
- `AGENTS.md` —— 仓库级开发约定（Issue 流程、文档结构）
- `docs/agents/` —— issue tracker / triage / domain 规范

## 6. 开发路线（摘要）

| 阶段 | 内容 |
|---|---|
| S0 ✅ | Git 基线：清理构建产物、补 README、同步 GitHub |
| S1 | 任务运行器：真实的 job 表 + 进度上报 + 前端轮询（替换 sleep 假进度） |
| S2 | 文件真实解析：CSV/Excel/JSON → 原始层 `ingestion_row` + 校验报告 |
| S3 | 清洗融合接真实数据：7 阶段流水线写规范层 `consumption_record` |
| S4–S5 | 国家统计局适配器、高德 POI 适配器（真实外部数据） |
| S6–S9 | RFM、预测异常、LLM 情感主题、三通道推荐 |
| S10–S12 | API 监测台、大屏与设置向导、工程收尾 |

完整说明见 `docs/ARCHITECTURE.md` 第 8 章。

## 7. 数据与隐私

- 所有数据保存在本机 SQLite 文件中，不上传任何服务器。
- `*.db`、`store/unpacked/`、`.env` 已在 `.gitignore` 中排除，不进版本库。
- 后续接入国家统计局 / 高德等外部 API 时，密钥只放本地 `.env`，不写进代码。

## 8. 已知限制（重构中，按路线图逐步解决）

- 导入进度目前来自定时器/时间推算，不是真实任务进度（S1 修复）。
- 数据源目前只有内置演示样例，尚无真实外部数据接入（S4/S5 接入）。
- 前端 ECharts 仍走 CDN，离线需本地化 `web/vendor/`（S11 前处理）。
