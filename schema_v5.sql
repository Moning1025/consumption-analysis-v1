-- schema_v5.sql · S2-1d 空间网格底盘（人口 / 灯光 / POI 密度 / 路网密度）
-- 设计原则：一个网格一行，所有"分母"和"权重"都落在这里，后面算市场潜力只读这一张表。

CREATE TABLE IF NOT EXISTS grid_metrics (
    grid_id          TEXT PRIMARY KEY,      -- 形如 104.0625_30.7125_r0500（中心坐标 + 分辨率米数），可读且唯一
    lng              REAL NOT NULL,         -- 网格中心经度（WGS-84）
    lat              REAL NOT NULL,         -- 网格中心纬度（WGS-84）
    pop_count        REAL,                  -- 人口（人）：WorldPop 百米栅格在该网格内的像元值之和
    light_intensity  REAL,                  -- 灯光强度：VIIRS 该网格内的像元均值（nW/cm2/sr）
    poi_density_amap REAL,                  -- 高德 POI 密度（个/km2）：GCJ-02 转 WGS-84 后点落网格
    poi_density_baidu REAL,                 -- 百度 POI 密度（个/km2）★预留：AK 未批，先留空
    road_density     REAL,                  -- 路网密度（km/km2）：机动车道投影到米制后算长度
    -- 以下为溯源/复算用的扩展列（答辩被问"密度怎么算的"时直接给分母）
    area_km2         REAL,                  -- 网格实际面积（km2），纬度越高越小，不能一律填 1
    poi_count_amap   INTEGER,               -- 网格内高德 POI 计数（密度 = 计数 / area_km2）
    road_length_km   REAL,                  -- 网格内道路总长（km）
    city_code        TEXT,                  -- 所属城市行政区划码（成都 510100）
    city_name        TEXT,                  -- 所属城市名（成都市/眉山市…），多城市共存时用来筛选
    built_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gm_lnglat ON grid_metrics(lng, lat);
CREATE INDEX IF NOT EXISTS idx_gm_city   ON grid_metrics(city_code);
-- 注意：city_name 的索引不写在这里。老库的表创建时没有这一列，
-- 若在此处 CREATE INDEX 会在 initialize_schema 阶段直接报 "no such column"。
-- 由 grid_metrics_builder.py 的迁移块（先 ALTER 加列、再建索引）负责，新老库都能过。

-- 构建账本：一次构建一行，记录"网格为什么只有这么多个"（防 OOM 的举证）
CREATE TABLE IF NOT EXISTS grid_build_log (
    batch_id        TEXT    NOT NULL,
    bbox_lng_min    REAL, bbox_lat_min REAL, bbox_lng_max REAL, bbox_lat_max REAL,
    cell_size_m     REAL,                   -- 目标网格边长（米）
    grid_count      INTEGER,                -- 生成了多少个网格
    grid_limit      INTEGER,                -- 硬上限（超过则拒绝构建，防内存爆炸）
    poi_rows_used   INTEGER,                -- 参与统计的 POI 行数
    road_features   INTEGER,                -- 参与统计的道路要素数
    pop_tif         TEXT, light_tif TEXT,   -- 用到的栅格文件（溯源）
    elapsed_ms      INTEGER,
    status          TEXT,                   -- succeeded / failed / skipped
    message         TEXT,
    created_at      TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_gbl_batch ON grid_build_log(batch_id);
