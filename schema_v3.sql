-- =====================================================================
-- schema_v3.sql —— S2-1a 真实数据接入（高德 POI）
--
-- 和 schema.sql / schema_v2.sql 一样是**幂等**脚本（CREATE ... IF NOT EXISTS），
-- 每次启动都会整份执行一遍，已存在的表不会被破坏、不会丢数据。
-- 由 store/db_connection.py 的 initialize_schema() 按 sql → v2 → v3 顺序执行。
--
-- 三张表的分工：
--   poi_merchant    —— 实体供给清单：一个商户/设施一行（高德返回的一条 POI）
--   poi_fetch_log   —— 采集账本：一次请求一行（第几页、要回几条、入库几条、耗时多少）
--   poi_fetch_batch —— 采集批次：一次命令行运行一行，把上面那些请求串成一次“任务”
--
-- 为什么要拆“清单 / 账本 / 批次”三张表而不是一张大表：
--   清单是结果，账本是过程证据。答辩时老师问“你这 3 万条真是采的？”——
--   翻账本：某天某关键词某页要回 25 条、入库 23 条、重复 2 条、耗时 380ms。
--   这跟 job_event 留执行证据是同一个思路。
-- =====================================================================


-- ---------------------------------------------------------------------
-- poi_merchant：高德 POI 实体（供给侧）
--   主键用高德的 poi_id，所以“同一条 POI 重复采”不会产生重复行，
--   只会覆盖更新（见 scripts/fetch_amap_poi.py 的 ON CONFLICT 语句）。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS poi_merchant (
    poi_id        TEXT PRIMARY KEY,              -- 高德 POI 唯一 id（B0FF...），去重的依据
    name          TEXT NOT NULL,                 -- 商户/设施名称
    category_l1   TEXT,                          -- 一级业态：餐饮 / 购物 / 生活服务 ...（由 amap_type 切分得出）
    category_l2   TEXT,                          -- 二级业态：中餐厅 / 便利店 ...
    amap_type     TEXT,                          -- 高德原始 type 串："餐饮服务;中餐厅;火锅店"
    typecode      TEXT,                          -- 高德 POI 分类码："050500"
    address       TEXT,                          -- 详细地址（可能为空字符串）
    province      TEXT,                          -- 省：四川省
    city          TEXT,                          -- 市：成都市
    district      TEXT,                          -- 区县：武侯区
    adcode        TEXT,                          -- 行政区划码：510107（**地域归一化的关键**）
    longitude     REAL,                          -- 经度（从 location "104.06,30.67" 拆出来存数值，方便算距离）
    latitude      REAL,                          -- 纬度
    location_text TEXT,                          -- 高德原始经纬度串，留档对照
    tel           TEXT,                          -- 电话（可能为空）
    keyword       TEXT,                          -- 采集时用的关键词，便于回溯“这条是怎么来的”
    source        TEXT NOT NULL DEFAULT 'amap',  -- 数据来源标签：amap / manual / ...
    raw_json      TEXT,                          -- 原始 JSON 整条留档（字段被高德改版时还能救回来）
    fetched_at    TEXT NOT NULL,                 -- 首次入库时间
    updated_at    TEXT NOT NULL                  -- 最近一次被采集覆盖的时间
);

-- 索引：后面按区县、按城市、按业态做空间聚合，这三种查询最常用
CREATE INDEX IF NOT EXISTS idx_poi_merchant_adcode   ON poi_merchant (adcode);
CREATE INDEX IF NOT EXISTS idx_poi_merchant_city     ON poi_merchant (city);
CREATE INDEX IF NOT EXISTS idx_poi_merchant_category ON poi_merchant (category_l1);


-- ---------------------------------------------------------------------
-- poi_fetch_batch：一次命令行运行 = 一个批次
--   相当于 job_run 在采集场景下的轻量版：不必拉起线程池，但同样留痕。
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS poi_fetch_batch (
    batch_id      TEXT PRIMARY KEY,              -- amap_20261112_143015_8f3a 这种
    city          TEXT,                          -- 目标城市
    keywords      TEXT,                          -- 关键词（多个用逗号连）
    types         TEXT,                          -- 高德类型码过滤（可空）
    pages_planned INTEGER NOT NULL DEFAULT 0,    -- 计划抓几页
    pages_done    INTEGER NOT NULL DEFAULT 0,    -- 实际完成几页
    rows_returned INTEGER NOT NULL DEFAULT 0,    -- 高德一共返回几条
    rows_inserted INTEGER NOT NULL DEFAULT 0,    -- 新增入库几条
    rows_updated  INTEGER NOT NULL DEFAULT 0,    -- 覆盖更新几条
    status        TEXT NOT NULL DEFAULT 'running',-- running / succeeded / failed / canceled
    message       TEXT,                          -- 失败原因或备注
    elapsed_ms    INTEGER,                       -- 总耗时
    created_at    TEXT NOT NULL,
    finished_at   TEXT
);


-- ---------------------------------------------------------------------
-- poi_fetch_log：一次请求 = 一行（账本的最小刻度）
--   守恒关系（答辩可讲）：rows_returned = rows_inserted + rows_updated + rows_skipped
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS poi_fetch_log (
    log_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      TEXT,                          -- 归属批次
    keyword       TEXT,                          -- 本次请求的关键词
    page_no       INTEGER,                       -- 第几页（从 1 开始）
    http_status   INTEGER,                       -- HTTP 状态码
    amap_status   TEXT,                          -- 高德业务状态："1" 成功 / "0" 失败
    amap_infocode TEXT,                          -- 高德错误码：10001 / 10004 ...
    rows_returned INTEGER NOT NULL DEFAULT 0,    -- 本页返回条数
    rows_inserted INTEGER NOT NULL DEFAULT 0,    -- 本页新增
    rows_updated  INTEGER NOT NULL DEFAULT 0,    -- 本页覆盖
    rows_skipped  INTEGER NOT NULL DEFAULT 0,    -- 本页跳过（缺 poi_id / 缺经纬度等）
    elapsed_ms    INTEGER,                       -- 本页耗时（含节流等待）
    message       TEXT,                          -- 错误说明
    created_at    TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_poi_fetch_log_batch ON poi_fetch_log (batch_id);
