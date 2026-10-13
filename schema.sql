-- ===================== 账号权限模块 user_account =====================
CREATE TABLE IF NOT EXISTS user_account (
    account_id INTEGER PRIMARY KEY AUTOINCREMENT,
    login_name TEXT NOT NULL UNIQUE,
    display_name TEXT,
    password_hash TEXT NOT NULL,
    password_salt TEXT NOT NULL,
    role_code TEXT NOT NULL DEFAULT 'guest',
    account_status TEXT NOT NULL DEFAULT 'active',
    created_at INTEGER,
    updated_at INTEGER,
    last_login_at INTEGER
);

CREATE TABLE IF NOT EXISTS password_reset_token (
    reset_token_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NOT NULL,
    token_digest TEXT NOT NULL,
    issued_at INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    consumed_at INTEGER NULL,
    FOREIGN KEY(account_id) REFERENCES user_account(account_id)
);

-- ===================== 审计日志 audit_log =====================
CREATE TABLE IF NOT EXISTS audit_log (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id INTEGER NULL,
    actor_username TEXT NOT NULL,
    actor_role TEXT NOT NULL,
    event_kind TEXT NOT NULL,
    action_name TEXT NOT NULL,
    source_page TEXT,
    client_ip TEXT,
    detail_payload TEXT,
    occurred_at TEXT NOT NULL
);

-- ===================== 数据接入控制台 data_ingestion_console =====================
CREATE TABLE IF NOT EXISTS data_ingestion_console (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_identifier TEXT UNIQUE NOT NULL,
    source_name TEXT,
    source_channel TEXT,
    source_type TEXT,
    endpoint_uri TEXT,
    region_code TEXT,
    payload_json TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    progress REAL DEFAULT 0.0,
    term_count INTEGER DEFAULT 0,
    operator TEXT,
    created_at TEXT,
    updated_at TEXT
);

-- ===================== 清洗融合任务表 cleaning_fusion_pipeline_task =====================
CREATE TABLE IF NOT EXISTS cleaning_fusion_pipeline_task (
    pipeline_task_identifier TEXT PRIMARY KEY,
    task_name TEXT,
    task_kind TEXT,
    source_scope TEXT,
    task_status TEXT NOT NULL DEFAULT 'pending',
    progress_percent INTEGER DEFAULT 0,
    current_stage TEXT,
    processed_record_count INTEGER DEFAULT 0,
    started_at TEXT,
    finished_at TEXT,
    execution_summary TEXT,
    created_at TEXT
);

-- ===================== RFM用户分层 rfm_segmentation_studio =====================
CREATE TABLE IF NOT EXISTS rfm_segmentation_studio (
    id TEXT PRIMARY KEY,
    analysis_window TEXT,
    user_name TEXT,
    segment_name TEXT,
    recency_score INTEGER,
    frequency_score INTEGER,
    monetary_score INTEGER,
    customer_identifier TEXT
);

-- ===================== 消费预测与异常检测 consumption_forecast_anomaly =====================
CREATE TABLE IF NOT EXISTS consumption_forecast_anomaly (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_code TEXT,
    model_type TEXT,
    window_days INTEGER,
    anomaly_threshold REAL,
    forecast_curve_json TEXT,
    summary_json TEXT,
    generated_at TEXT,
    status TEXT
);

-- ===================== 消费明细（预测/异常检测的输入序列） consumption_record =====================
CREATE TABLE IF NOT EXISTS consumption_record (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    region_code TEXT,
    consumption_amount REAL,
    occurred_date TEXT,
    occurred_at TEXT
);

CREATE TABLE IF NOT EXISTS forecast_anomaly_event (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    forecast_window_id INTEGER,
    region_code TEXT,
    occurred_date TEXT,
    consumption_amount REAL,
    z_score REAL,
    deviation_ratio REAL,
    anomaly_direction TEXT,
    detected_at TEXT,
    FOREIGN KEY(forecast_window_id) REFERENCES consumption_forecast_anomaly(id)
);

-- ===================== 空间消费热点 spatial_hotspot_map =====================
CREATE TABLE IF NOT EXISTS spatial_hotspot_map (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    grid_code TEXT,
    region_name TEXT NOT NULL,
    longitude REAL NULL,
    latitude REAL NULL,
    hotspot_score REAL DEFAULT 0.0,
    aggregation_count INTEGER DEFAULT 0,
    snapshot_time TEXT,
    detail_payload TEXT
);

-- ===================== 情感主题挖掘 sentiment_topic_mining =====================
CREATE TABLE IF NOT EXISTS sentiment_topic_mining (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_code TEXT,
    analysis_window TEXT,
    source_scope TEXT,
    total_reviews INTEGER DEFAULT 0,
    sentiment_distribution_json TEXT,
    topic_clusters_json TEXT,
    generated_at TEXT,
    status TEXT
);

-- ===================== 推荐工作台 recommendation_workbench =====================
CREATE TABLE IF NOT EXISTS recommendation_workbench (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    segment_label TEXT,
    top_n INTEGER DEFAULT 10,
    recommendations_json TEXT,
    source_pipeline_code TEXT,
    generated_at TEXT,
    status TEXT
);

-- ===================== API监控 realtime_api_monitor =====================
CREATE TABLE IF NOT EXISTS realtime_api_monitor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    endpoint_uri TEXT,
    source_name TEXT,
    region_code TEXT,
    latency_ms REAL,
    status_code INTEGER,
    response_sample_json TEXT,
    sampled_at TEXT,
    is_error INTEGER DEFAULT 0
);

-- 建立常用索引，提升大数据查询速度（答辩可以讲索引优化）
CREATE INDEX IF NOT EXISTS idx_audit_account ON audit_log(account_id);
CREATE INDEX IF NOT EXISTS idx_audit_time ON audit_log(occurred_at);
CREATE INDEX IF NOT EXISTS idx_ingest_status ON data_ingestion_console(status);
CREATE INDEX IF NOT EXISTS idx_rfm_id ON rfm_segmentation_studio(id);
CREATE INDEX IF NOT EXISTS idx_hotspot_region ON spatial_hotspot_map(region_name);
CREATE INDEX IF NOT EXISTS idx_forecast_region ON consumption_forecast_anomaly(region_code);
CREATE INDEX IF NOT EXISTS idx_forecast_event_region ON forecast_anomaly_event(region_code);
CREATE INDEX IF NOT EXISTS idx_sentiment_batch ON sentiment_topic_mining(batch_code);
CREATE INDEX IF NOT EXISTS idx_recommend_segment ON recommendation_workbench(segment_label);
CREATE INDEX IF NOT EXISTS idx_realtime_sampled ON realtime_api_monitor(sampled_at);
