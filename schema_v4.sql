-- schema_v4.sql · S2-1b 宏观统计年鉴（年度口径）
-- 来源：成都统计年鉴（区县级）/ 四川统计年鉴（市州级）
-- 设计原则：原始单位原样落库（万元），换算只在导入时做一次并在 raw_row 留档，便于反查。

CREATE TABLE IF NOT EXISTS region_consumption_yearly (
    region_code      TEXT    NOT NULL,              -- 行政区划码：武侯区 510107 / 成都市 510100
    region_name      TEXT    NOT NULL,              -- 原文名（"武侯区" / "成都"）
    region_level     TEXT    NOT NULL,              -- city / district
    year             INTEGER NOT NULL,              -- 年度，从文件名解析
    retail_total     REAL,                          -- 社会消费品零售总额（万元）
    catering_revenue REAL,                          -- 餐饮收入（万元）
    urban_retail     REAL,                          -- 城镇零售额（万元）
    rural_retail     REAL,                          -- 乡村零售额（万元）
    goods_retail     REAL,                          -- 商品零售（万元）★扩展：成都表有、四川表也有
    retail_growth    REAL,                          -- 社零增速（小数，0.051 = 5.1%）★扩展：四川表有
    source_file      TEXT,                          -- 来源文件名（溯源）
    source_row_no    INTEGER,                       -- 原始行号（溯源：点数字能查到原表第几行）
    raw_row          TEXT,                          -- 原始整行 JSON（溯源）
    imported_at      TEXT    NOT NULL,
    PRIMARY KEY (region_level, region_code, year)
);

CREATE INDEX IF NOT EXISTS idx_rcy_year  ON region_consumption_yearly(year);
CREATE INDEX IF NOT EXISTS idx_rcy_level ON region_consumption_yearly(region_level);

-- 导入账本：一次导入一行，记录"读了多少行、剔了多少汇总行、入库多少"，与 poi_fetch_log 同构思路
CREATE TABLE IF NOT EXISTS yearbook_import_log (
    batch_id      TEXT    NOT NULL,
    source_file   TEXT    NOT NULL,
    file_format   TEXT,                             -- xlsx / xlsx_misnamed_as_csv / csv / zip
    sheet_name    TEXT,
    year          INTEGER,
    region_level  TEXT,
    rows_seen     INTEGER,                          -- 表里读到的数据行数（不含表头）
    rows_skipped  INTEGER,                          -- 剔掉的汇总行（全市/全省）与空行
    rows_upserted INTEGER,                          -- 真正写进库的行数
    unit_raw      TEXT,                             -- 原始单位（万元 / 亿元）
    unit_factor   REAL,                             -- 换算系数（1 / 10000）
    message       TEXT,
    created_at    TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_yil_batch ON yearbook_import_log(batch_id);
