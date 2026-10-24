# AGENTS.md

本文件是给编码 agent 的仓库级说明。技能读取的仓库配置在 `docs/agents/`。

## 开工必读（任何对话 / 任何 agent 动这个仓库前）

1. 读 `docs/ARCHITECTURE.md` —— **唯一设计基线（当前 v1.3）**。至少读：第 8 章路线图（当前进度在哪一步）、第 10 章变更记录（最近改了什么）、第 1.4 节软著说明书功能基线（验收口径）。
2. 看仓库现场：`git status`、`git log --oneline -5`，确认别人有没有未提交的改动。
3. 运行时记忆 `MEMORY.md` / `USER.md` 会自动注入，无需手动读；**若与 `docs/ARCHITECTURE.md` 冲突，以文档为准**。

改完必须做：在 `docs/ARCHITECTURE.md` 第 10 章「变更记录」追加一行（日期 / Step / 改动摘要 / 验证方式）；新增表或接口时同步更新第 3 / 4 章。

## 对话分工（两条工作线，别互相踩）

| 工作线 | 职责范围 | 允许改的路径 |
|---|---|---|
| **总修改** 对话 | 架构设计 + S 系列开发（数据层、任务系统、接入适配器、后端接口、路线图推进） | `docs/ARCHITECTURE.md`、`pipeline/`、`datasource/`、`store/`、`schema*.sql`、`bridge_api.py`、`main.py`、`auth/` |
| **其他对话**（样式 / Bug 类） | 前端样式与交互细节、单点 bug 修复、文档润色 | `web/index.html`（样式段与明确 bug 点）、`docs/` 非设计章节 |

纪律：

- **同一时间只允许一条工作线改同一个文件**；跨界改动先在 GitHub Issue 里留言，或写进 `docs/ARCHITECTURE.md` 变更记录再由主线认领。
- 交接只通过两个东西：**文档（第 10 章变更记录）+ git 提交**。不要依赖另一个对话的聊天上下文（对话之间不共享聊天记录）。
- 每条工作线收工即提交一次；`git push` 前必须先征得用户同意。

## Agent skills

### Issue tracker

Issue 与 spec 都放在本仓库的 GitHub Issues（`Moning1025/consumption-analysis-v1`），统一用 `gh` CLI 操作。See `docs/agents/issue-tracker.md`.

### Triage labels

Triage 沿用默认五角色，标签字符串与角色同名：`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`。See `docs/agents/triage-labels.md`.

### Domain docs

**multi-context** — 根目录一份 `GLOSSARY-MAP.md` 指向各子项目自己的 `GLOSSARY.md`（子项目各一份），ADR 分系统级与上下文级两处。See `docs/agents/domain.md`.
