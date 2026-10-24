-- =====================================================================
-- schema_v2.sql —— S1 任务基础设施：任务运行表 + 任务过程日志表
-- 与 schema.sql 一样是幂等脚本（CREATE TABLE IF NOT EXISTS），
-- 每次启动都会执行，已存在的表不会被破坏。
-- =====================================================================

-- ---------------------------------------------------------------------
-- job_run：一个任务一行，记录“此刻的快照”
--   前端轮询只看这一行：status / progress / stage_label / message
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_run (
    job_id           TEXT PRIMARY KEY,              -- job_xxxxx
    job_type         TEXT NOT NULL,                 -- ingestion.parse / cleaning_fusion / rfm.compute ...
    page_identifier  TEXT,                          -- 发起页面：data_ingestion_console / cleaning_fusion_pipeline ...
    status           TEXT NOT NULL DEFAULT 'queued',-- queued/running/succeeded/failed/canceled
    stage            TEXT,                          -- 当前阶段机器名：parse/validate/deduplicate ...
    stage_label      TEXT,                          -- 当前阶段中文名：格式校验 / 去重清洗 ...
    progress         INTEGER NOT NULL DEFAULT 0,    -- 0..100
    message          TEXT,                          -- 最近一条人话说明
    params_json      TEXT,                          -- 入参快照
    result_json      TEXT,                          -- 成功结果摘要（行数、产物 id 等）
    error_message    TEXT,                          -- 失败原因（中文）
    operator_name    TEXT,                          -- 发起人
    source_kind      TEXT,                          -- ui / drop / api / schedule
    cancel_requested INTEGER NOT NULL DEFAULT 0,    -- 1 = 用户点了取消
    created_at       TEXT NOT NULL,
    started_at       TEXT,
    finished_at      TEXT,
    updated_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_job_run_status_created
    ON job_run(status, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_job_run_type_created
    ON job_run(job_type, created_at DESC);

-- ---------------------------------------------------------------------
-- job_event：任务过程日志，一行一条，seq 在单任务内自增
--   前端用 after_seq 增量拉取，不会重复渲染旧日志
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS job_event (
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       TEXT NOT NULL,
    seq          INTEGER NOT NULL,                  -- 单任务内从 1 递增
    level        TEXT NOT NULL DEFAULT 'INFO',      -- INFO / WARN / ERROR
    stage        TEXT,                              -- 所属阶段机器名
    message      TEXT NOT NULL,
    payload_json TEXT,
    created_at   TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_job_event_job_seq
    ON job_event(job_id, seq);
CREATE INDEX IF NOT EXISTS idx_job_event_job
    ON job_event(job_id, event_id);
