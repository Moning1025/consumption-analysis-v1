-- =====================================================================
-- schema_v6.sql —— Olist 交易流水导入（S2-1c）新增表
-- 幂等：全部 CREATE ... IF NOT EXISTS，可重复执行
-- 注意：consumption_record 新增的两列（external_id / region_profile）
--       不写在本文件里，因为 SQLite 的 ALTER TABLE ADD COLUMN 不幂等，
--       第二次执行会报 "duplicate column name"。那两列由
--       scripts/import_olist.py 里的 ensure_consumption_record_columns()
--       用 PRAGMA table_info 检查后按需添加（与 grid_metrics 的 city_name 同款做法）。
-- =====================================================================

-- 每次导入一条账本，用来回答“这批数据是哪天、从哪些文件、导了多少、丢了多少”
CREATE TABLE IF NOT EXISTS olist_import_log (
    log_id                INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_identifier      TEXT    NOT NULL,
    data_dir              TEXT,                          -- 数据目录（相对项目根）
    granularity           TEXT,                          -- item / order
    rows_read             INTEGER NOT NULL DEFAULT 0,     -- 从 CSV 读到的原始行数
    rows_written          INTEGER NOT NULL DEFAULT 0,     -- 真正写进库的行数
    rows_skipped_status   INTEGER NOT NULL DEFAULT 0,     -- 因订单状态被跳过（canceled / unavailable）
    rows_skipped_invalid  INTEGER NOT NULL DEFAULT 0,     -- 因缺关键字段被跳过
    rows_skipped_dup      INTEGER NOT NULL DEFAULT 0,     -- 因唯一键已存在被跳过（幂等命中）
    distinct_orders       INTEGER NOT NULL DEFAULT 0,
    min_date              TEXT,
    max_date              TEXT,
    elapsed_ms            INTEGER,
    message               TEXT,
    created_at            TEXT    NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_olist_import_log_batch
    ON olist_import_log (batch_identifier);

-- 注意：consumption_record 的唯一索引也不写在这里。
-- 原因同上——索引依赖 external_id 列，而该列要等 ensure_consumption_record_columns()
-- 补上之后才能建索引；否则 initialize_schema() 执行本文件时会报
-- "no such column: external_id" 直接中断整个建库流程。
