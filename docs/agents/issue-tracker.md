# Issue tracker: GitHub

本仓库的 issue 与 spec 都作为 GitHub issue 存放，仓库是 `Moning1025/consumption-analysis-v1`。所有操作一律用 `gh` CLI 完成；在克隆目录里运行时 `gh` 会自己从 `git remote -v` 推断仓库（当前 `origin` 指向 `Moning1025/consumption-analysis-v1`）。

## 约定

- **建 issue**：`gh issue create --title "..." --body "..."`，多行正文用 heredoc 或 `--body-file`。
- **读 issue**：`gh issue view <number> --comments`，用 `jq` 过滤评论，同时取回 labels。
- **列 issue**：`gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'`，按需加 `--label` 与 `--state` 过滤。
- **评论**：`gh issue comment <number> --body "..."`
- **打标 / 去标**：`gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **关闭**：`gh issue close <number> --comment "..."`

## 把 PR 当作 triage 入口

**PRs as a request surface: no.**（若本仓库要把外部 PR 也当作功能请求排队，把它改成 `yes`；`/triage` 会读这一行。）

设为 `yes` 时，PR 与 issue 走同一套标签与状态，命令换用对应的 `gh pr`：

- **读 PR**：`gh pr view <number> --comments`，看改动用 `gh pr diff <number>`。
- **列待 triage 的外部 PR**：`gh pr list --state open --json number,title,body,labels,author,authorAssociation,comments`，只保留 `authorAssociation` 为 `CONTRIBUTOR`、`FIRST_TIME_CONTRIBUTOR`、`NONE` 的条目（去掉 `OWNER`/`MEMBER`/`COLLABORATOR`）。
- **评论 / 打标 / 关闭**：`gh pr comment`、`gh pr edit --add-label`/`--remove-label`、`gh pr close`。

GitHub 的 issue 与 PR 共用一套编号，单独一个 `#42` 可能是两者之一：先用 `gh pr view 42` 解析，查不到再退回 `gh issue view 42`。

## 当技能说「publish to the issue tracker」

就是建一个 GitHub issue。

## 当技能说「fetch the relevant ticket」

就是跑 `gh issue view <number> --comments`。

## Wayfinding 操作

`/wayfinder` 使用。**地图（map）** 是一张 issue，它的**工单（ticket）** 是它的子 issue。

- **地图**：一张带 `wayfinder:map` 标签的 issue，承载 Notes / Decisions so far / Fog 三段正文。`gh issue create --label wayfinder:map`。
- **子工单**：作为 GitHub 原生 sub-issue 链到地图上的 issue（走 `gh api` 的 sub-issues 端点）。若当前仓库没开 sub-issue，就把子工单放进地图正文的任务清单，并在子工单正文顶部写 `Part of #<map>`。标签用 `wayfinder:<type>`（`research`/`prototype`/`grilling`/`task`）。认领后把工单指派给推进它的开发者。
- **阻塞关系**：用 GitHub 原生 issue dependencies，这是规范且界面上可见的表示。加一条边：`gh api --method POST repos/<owner>/<repo>/issues/<child>/dependencies/blocked_by -F issue_id=<blocker-db-id>`，其中 `<blocker-db-id>` 是阻塞方的数字**数据库 id**（`gh api repos/<owner>/<repo>/issues/<n> --jq .id`，不是 `#number`，也不是 `node_id`）。GitHub 用 `issue_dependencies_summary.blocked_by` 报告阻塞方（只算还开着的，是真正的闸门）。若当前仓库不支持 dependencies，就退回在子工单正文顶部写 `Blocked by: #<n>, #<n>`。阻塞方全部关闭时，这张工单才算解除阻塞。
- **前沿查询（frontier）**：列地图下所有开着的子工单（`gh issue list --state open`，范围限定在该地图的 sub-issue / 任务清单内），去掉还挂着一张开着的阻塞单的（`issue_dependencies_summary.blocked_by > 0`，或 `Blocked by` 行里还有开着的 issue）以及已经被指派的；地图顺序里的第一张优先。
- **认领**：`gh issue edit <n> --add-assignee @me`，这是本会话的第一次写操作。
- **解决**：`gh issue comment <n> --body "<答案>"`，然后 `gh issue close <n>`，最后把上下文指针（结论摘要 + 链接）追加到地图的 Decisions so far。
