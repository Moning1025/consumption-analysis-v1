# AGENTS.md

本文件是给编码 agent 的仓库级说明。技能读取的仓库配置在 `docs/agents/`。

## Agent skills

### Issue tracker

Issue 与 spec 都放在本仓库的 GitHub Issues（`Moning1025/consumption-analysis-v1`），统一用 `gh` CLI 操作。See `docs/agents/issue-tracker.md`.

### Triage labels

Triage 沿用默认五角色，标签字符串与角色同名：`needs-triage` / `needs-info` / `ready-for-agent` / `ready-for-human` / `wontfix`。See `docs/agents/triage-labels.md`.

### Domain docs

**multi-context** — 根目录一份 `GLOSSARY-MAP.md` 指向各子项目自己的 `GLOSSARY.md`（子项目各一份），ADR 分系统级与上下文级两处。See `docs/agents/domain.md`.
