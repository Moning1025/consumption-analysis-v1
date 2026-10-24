-- =====================================================================
-- schema_v2.sql —— S1 任务基础设施 + S2 原始数据层
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

-- =====================================================================
-- S2：原始数据层（清洗的上游）
--   ingestion_row 存“原文一行”，不清洗、不改写，保证可回溯；
--   清洗任务（S3）读这张表，写回清洗结果表，责任边界清楚。
-- =====================================================================

-- ---------------------------------------------------------------------
-- ingestion_row：接入时逐行落库的原始记录
--   batch_identifier 对应 data_ingestion_console.batch_identifier
--   row_hash = 原始行内容的 sha256，用于幂等（同一文件重复导入不重复入库）
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_row (
    row_id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_identifier TEXT NOT NULL,                 -- 所属接入批次
    source_kind      TEXT NOT NULL,                 -- catering/tourism/retail/nbs/amap/text
    line_no          INTEGER NOT NULL,              -- 源文件内行号（1 起；整文件失败记 0）
    row_hash         TEXT NOT NULL,                 -- 行内容 sha256，幂等去重用
    raw_json         TEXT NOT NULL,                 -- 原始行原文（不改写）
    parse_status     TEXT NOT NULL DEFAULT 'ok',    -- ok / error
    parse_error      TEXT,                          -- 坏行原因（中文）
    created_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingestion_row_batch
    ON ingestion_row(batch_identifier, parse_status);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ingestion_row_batch_hash
    ON ingestion_row(batch_identifier, row_hash);

-- ---------------------------------------------------------------------
-- ingestion_validation：一次接入的格式校验报告（一行一报告）
--   valid/error/warning 三个计数 + 明细 JSON，供接入台展示
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ingestion_validation (
    validation_code  TEXT PRIMARY KEY,              -- val_<batch>_<时间戳>
    batch_identifier TEXT NOT NULL,
    valid_rows       INTEGER NOT NULL DEFAULT 0,    -- 成功入库行数
    error_rows       INTEGER NOT NULL DEFAULT 0,    -- 坏行数（含无法解析的文件）
    warning_rows     INTEGER NOT NULL DEFAULT 0,    -- 重复跳过行数
    report_json      TEXT,                          -- 明细：逐文件统计、缺列清单
    created_at       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ingestion_validation_batch
    ON ingestion_validation(batch_identifier);
