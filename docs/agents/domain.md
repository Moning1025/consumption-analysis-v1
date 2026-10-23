# 域文档（Domain Docs）

工程技能在探索本仓库代码之前，按以下规则读取域文档。

## 先读这些

- **`GLOSSARY-MAP.md`**（仓库根目录，存在时）：本仓库采用 **multi-context** 布局，根目录这一份 `GLOSSARY-MAP.md` 指向各子项目自己的 `GLOSSARY.md`。先读它，再读与本次话题相关的那几个 `GLOSSARY.md`。
- 各子项目自己的 `GLOSSARY.md`（存在时，例如 `auth/GLOSSARY.md`、`pipeline/GLOSSARY.md`、`store/GLOSSARY.md`）。
- **`docs/adr/`**：读与即将改动的区域相关的 ADR。multi-context 仓库里还要看 `src/<context>/docs/adr/` 下该上下文专属的决策；本仓库的子项目直接放在仓库根下，所以是 `<context>/docs/adr/`。

**布局结论：multi-context**（本仓库有多个各自成篇的子项目，各有各的用语，不适合共用一份词表）。这一结论同时记录在 `docs/agents/workspace.yaml` 的 `layout: multi` 里。

目前 `GLOSSARY-MAP.md` 与各子项目的 `GLOSSARY.md` **都还没有创建**：初始化只记录布局结论，不在没有词条时先建空骨架。等第一次真正写下词条时再建，由根目录的 `GLOSSARY-MAP.md` 指向它们。

上述文件若不存在，**静默跳过**：不要提示缺失，也不要建议提前创建。`/domain-modeling` 技能（经 `/grill-with-docs` 与 `/improve-codebase-architecture` 到达）会在词条或决策真正定稿时按需创建它们。

## 文件结构

本仓库（multi-context）：

```
/
├── GLOSSARY-MAP.md                 ← 指向各子项目的 GLOSSARY.md
├── docs/adr/                       ← 系统级决策
├── auth/
│   ├── GLOSSARY.md                 ← 上下文级词表
│   └── docs/adr/                   ← 上下文级决策
├── pipeline/
│   ├── GLOSSARY.md
│   └── docs/adr/
├── store/
│   ├── GLOSSARY.md
│   └── docs/adr/
└── web/
    ├── GLOSSARY.md
    └── docs/adr/
```

（结构示意；目录按实际需要逐步长出来，不要为了对齐这张图提前建空目录。）

## 用词表里的词汇

当你的输出要命名一个领域概念时（issue 标题、重构提案、假设、测试名），用 `GLOSSARY.md` 里定义的词，不要漂移到词表明确避开的同义词。

若需要的概念还不在词表里，这本身是个信号：要么你在发明项目不用的说法（先重新考虑），要么确有缺口（记下来，留给 `/domain-modeling`）。

## 标记 ADR 冲突

若你的输出与既有 ADR 相矛盾，显式说出来，而不是悄悄覆盖：

> _与 ADR-0007（事件溯源的订单）冲突，但值得重开讨论，因为……_
