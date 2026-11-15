"""S2-1d · 空间网格底盘构建器：人口 / 灯光 / POI 密度 / 路网密度 → grid_metrics

这张表是整个系统的"地基"
------------------------
后面所有"这个商圈是蓝海还是红海""市场潜力多高"的结论，都落在**同一张网格表**上：
    pop_count（有多少人） × light_intensity（有多热闹） vs poi_density（有多少店）
同样的 POI 数量，人口 2 万的街区是红海，人口 8 万的街区是蓝海——
**没有人口和灯光做分母，POI 数量本身说明不了任何事。**

★ 为什么必须限制区域（答辩必问，也是本脚本最重要的设计）
--------------------------------------------------------
1 km 网格铺满四川（48.6 万 km²）≈ **48.6 万个格子**，每格还要做栅格采样 + 路网叠加，
内存和耗时都会炸。所以：
- **只铺成都主城区**（默认 bbox 103.8–104.3E, 30.5–30.9N ≈ 0.5°×0.4°，约 4 万格？不——见下）；
- 还有一层硬保护：`--max-cells`（默认 20000），超过直接报错退出，**绝不"先跑起来再说"**；
- 实测：默认 bbox + 1000 m 网格 ≈ **约 1900 格**（因为 0.5° 经度在该纬度只有 48 km）。

内存账（答辩可以报数字）：每格 12 个字段 ≈ 1 KB Python 对象，2 万格 ≈ 20 MB；
栅格采样按窗口读，峰值 < 100 MB；路网只读 bbox 范围内的要素。

四个数据来源与对齐方式
--------------------
| 字段 | 来源 | 坐标系 | 处理 |
|---|---|---|---|
| pop_count | WorldPop 100 m TIF | WGS-84 | 格内像元**求和**（像元值=该像元的人数） |
| light_intensity | VIIRS 夜间灯光 TIF | WGS-84 | 格内像元**均值** |
| poi_density_amap | poi_merchant 表（高德） | **GCJ-02** | 先转 WGS-84 再落格，否则整体偏移 300–500 m |
| road_density | OSM 路网 shp | WGS-84 | 投到 EPSG:3857（米）算长度，按线段中点归格 |

用法
----
    # 先干跑：只统计不写库，看网格数和各层覆盖率
    python scripts/grid_metrics_builder.py --dry-run

    # 正式构建（只铺成都主城区）
    python scripts/grid_metrics_builder.py

    # 没有装地理库时，先只验 POI 一层
    python scripts/grid_metrics_builder.py --skip-raster --skip-roads
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datasource.coord_transform import gcj02_to_wgs84  # noqa: E402

DATA_RAW = PROJECT_ROOT / "data_raw"
DEFAULT_POP_TIF = DATA_RAW / "chn_pop_2026_CN_100m_R2025A_v1.tif"
DEFAULT_LIGHT_TIF = DATA_RAW / "2025_CNVIIRS.tif"
DEFAULT_ROAD_SHP = DATA_RAW / "osm" / "四川省道路2026.shp"

# 缺省区域：成都主城区（含近郊）：经度 103.8–104.3，纬度 30.5–30.9
# 注意：现在首选 --city 从 region_codes.SICHUAN_CITY_BBOXES 查 bbox，
#       DEFAULT_BBOX 只在既没传 --city 又没传 --bbox 时兜底（保持旧行为）。
DEFAULT_BBOX = (103.80, 30.50, 104.30, 30.90)
DEFAULT_CITY = "成都市"
DEFAULT_CELL_M = 1000.0
# 成都全域外包框约 6.6 万格；留到 15 万，为将来"两个市州一起铺"留余量
DEFAULT_MAX_CELLS = 150_000
CITY_CODE_CHENGDU = "510100"

# OSM 里算"机动车道"的取值（其余：footway/path/steps/cycleway/pedestrian 等一律不算）
MOTOR_ROAD_TYPES = {
    "motorway", "motorway_link", "trunk", "trunk_link",
    "primary", "primary_link", "secondary", "secondary_link",
    "tertiary", "tertiary_link", "unclassified", "residential",
    "living_street", "service", "road",
}
ROAD_TYPE_CANDIDATES = ("fclass", "highway", "type", "class", "TYPE", "道路等级", "等级")

UPSERT_SQL = """
INSERT INTO grid_metrics (
    grid_id, lng, lat, pop_count, light_intensity,
    poi_density_amap, poi_density_baidu, road_density,
    area_km2, poi_count_amap, road_length_km, city_code, city_name, built_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(grid_id) DO UPDATE SET
    pop_count        = excluded.pop_count,
    light_intensity  = excluded.light_intensity,
    poi_density_amap = excluded.poi_density_amap,
    road_density     = excluded.road_density,
    area_km2         = excluded.area_km2,
    poi_count_amap   = excluded.poi_count_amap,
    road_length_km   = excluded.road_length_km,
    city_code        = excluded.city_code,
    city_name        = excluded.city_name,
    built_at         = excluded.built_at
"""

LOG_SQL = """
INSERT INTO grid_build_log (
    batch_id, bbox_lng_min, bbox_lat_min, bbox_lng_max, bbox_lat_max,
    cell_size_m, grid_count, grid_limit, poi_rows_used, road_features,
    pop_tif, light_tif, elapsed_ms, status, message, created_at
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
"""


@dataclass(slots=True)
class Cell:
    """一个网格：id + 中心点 + bbox（WGS-84）。"""

    grid_id: str
    lng: float
    lat: float
    lng_min: float
    lat_min: float
    lng_max: float
    lat_max: float
    area_km2: float


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z")


# --------------------------------------------------------------------------- 网格生成
def meters_to_degrees(cell_m: float, lat: float) -> tuple[float, float]:
    """把"米"换算成经纬度跨度。

    纬度 1° ≈ 111320 m；经度 1° 要乘 cos(纬度)——这就是为什么同样 1 km，
    在高纬度看起来"经度跨度更大"。
    """
    dlat = cell_m / 111_320.0
    dlng = cell_m / (111_320.0 * math.cos(math.radians(lat)))
    return dlng, dlat


def build_cells(bbox: tuple[float, float, float, float], cell_m: float) -> list[Cell]:
    """在 bbox 内铺规则网格。返回的每个 Cell 都带真实面积（按中心纬度算）。"""
    lng_min, lat_min, lng_max, lat_max = bbox
    mid_lat = (lat_min + lat_max) / 2
    dlng, dlat = meters_to_degrees(cell_m, mid_lat)

    n_lng = max(1, int(round((lng_max - lng_min) / dlng)))
    n_lat = max(1, int(round((lat_max - lat_min) / dlat)))
    # 用"均分"而不是"按 dlng 递增"，避免最后一格残缺
    step_lng = (lng_max - lng_min) / n_lng
    step_lat = (lat_max - lat_min) / n_lat

    cells: list[Cell] = []
    for i in range(n_lat):
        for j in range(n_lng):
            x0 = lng_min + j * step_lng
            y0 = lat_min + i * step_lat
            x1, y1 = x0 + step_lng, y0 + step_lat
            clng, clat = (x0 + x1) / 2, (y0 + y1) / 2
            height_km = (y1 - y0) * 111.32
            width_km = (x1 - x0) * 111.32 * math.cos(math.radians(clat))
            cells.append(
                Cell(
                    grid_id=f"{clng:.5f}_{clat:.5f}_r{int(cell_m)}",
                    lng=clng, lat=clat,
                    lng_min=x0, lat_min=y0, lng_max=x1, lat_max=y1,
                    area_km2=round(height_km * width_km, 6),
                )
            )
    return cells


# --------------------------------------------------------------------------- POI 一层
def load_poi_points(conn: sqlite3.Connection) -> list[tuple[float, float]]:
    """从 poi_merchant 读 POI 经纬度，并**把 GCJ-02 转成 WGS-84**。

    这一步漏掉的话：所有 POI 会整体向东北偏 300–500 m，
    网格密度全部"平移一格"，而数据看起来完全正常——最危险的那种错。
    """
    try:
        rows = conn.execute(
            "SELECT longitude, latitude FROM poi_merchant "
            "WHERE longitude IS NOT NULL AND latitude IS NOT NULL"
        ).fetchall()
    except sqlite3.OperationalError:
        return []       # 表还不存在（没跑过 S2-1a）：返回空，不崩
    out: list[tuple[float, float]] = []
    for lng, lat in rows:
        try:
            w_lng, w_lat = gcj02_to_wgs84(float(lng), float(lat))
        except (TypeError, ValueError):
            continue
        out.append((w_lng, w_lat))
    return out


def count_poi_per_cell(
    points_wgs: list[tuple[float, float]],
    cells: list[Cell],
    bbox: tuple[float, float, float, float],
) -> dict[str, int]:
    """纯 Python 落格：先算行/列索引再查表，O(n)，不依赖 geopandas（1 万点级足够快）。

    用 dict 索引 (行, 列) → grid_id，避免每点遍历 2000 个格子。
    """
    lng_min, lat_min, lng_max, lat_max = bbox
    if len(cells) < 2:
        return {}
    # 从 cells 反推步长：cells 是"行优先"生成的，cells[0]→cells[1] 只变经度、不变纬度，
    # 所以必须用"去重后的最小纬度/经度集合"来求真实步长，否则会算出 0。
    lat_mins = sorted({round(c.lat_min, 9) for c in cells})
    lng_mins = sorted({round(c.lng_min, 9) for c in cells})
    step_lat = (lat_mins[1] - lat_mins[0]) if len(lat_mins) > 1 else 0.0
    step_lng = (lng_mins[1] - lng_mins[0]) if len(lng_mins) > 1 else 0.0
    if step_lat <= 0 or step_lng <= 0:
        return {}

    index: dict[tuple[int, int], str] = {}
    for cell in cells:
        i = int(round((cell.lat_min - lat_min) / step_lat))
        j = int(round((cell.lng_min - lng_min) / step_lng))
        index[(i, j)] = cell.grid_id

    counts: dict[str, int] = {}
    for lng, lat in points_wgs:
        if not (lng_min <= lng < lng_max and lat_min <= lat < lat_max):
            continue                        # bbox 外的 POI 不参与（不是错误，是范围外）
        i = int((lat - lat_min) / step_lat)
        j = int((lng - lng_min) / step_lng)
        grid_id = index.get((i, j))
        if grid_id:
            counts[grid_id] = counts.get(grid_id, 0) + 1
    return counts


# --------------------------------------------------------------------------- 路网一层
def _pick_road_type_column(gdf) -> str | None:
    for name in ROAD_TYPE_CANDIDATES:
        if name in gdf.columns:
            return name
    return None


def compute_road_length_per_cell(
    shp_path: Path,
    cells: list[Cell],
    bbox: tuple[float, float, float, float],
) -> tuple[dict[str, float], int]:
    """算每个网格内的机动车道长度（km）。返回 ({grid_id: km}, 参与计算的要素数)。

    做法（三句话）：
    1. 只读 bbox 范围内的要素（`bbox=` 参数），不把 34 万条全省道路读进内存；
    2. 过滤出机动车道（footway/path 这类不算"路网密度"）；
    3. 投到 EPSG:3857（单位是米）算长度，按**线段中点**归格——一条线段只算一次，
       不会被相邻两格重复计入。
    """
    try:
        import geopandas as gpd
        from shapely.geometry import box
    except ImportError as exc:
        raise ImportError(
            "缺少 geopandas/shapely：请执行 "
            "python.exe -m pip install --only-binary :all: geopandas shapely pyproj"
        ) from exc

    if not shp_path.exists():
        raise FileNotFoundError(f"路网 shp 不存在：{shp_path}")

    roads = gpd.read_file(shp_path, bbox=bbox)
    if roads.empty:
        return {}, 0

    type_col = _pick_road_type_column(roads)
    if type_col:
        before = len(roads)
        roads = roads[roads[type_col].astype(str).str.lower().isin(MOTOR_ROAD_TYPES)]
        print(f"    道路过滤：{before} → {len(roads)} 条（保留机动车道）")
    else:
        print(f"    [WARN] 找不到道路等级列（候选 {ROAD_TYPE_CANDIDATES}），按全部要素计算")

    roads = roads.to_crs(epsg=3857)                       # 投影到米制，长度才准

    # 网格多边形（WGS-84 → 3857）
    grid_geoms = [box(c.lng_min, c.lat_min, c.lng_max, c.lat_max) for c in cells]
    grid_gdf = gpd.GeoDataFrame(
        {"grid_id": [c.grid_id for c in cells]}, geometry=grid_geoms, crs="EPSG:4326"
    ).to_crs(epsg=3857)

    # 线段中点作为"归属点"（比 sjoin 两条边更不容易重复计数）
    midpoints = roads.geometry.interpolate(0.5, normalized=True)
    road_pts = gpd.GeoDataFrame(
        {"len_km": roads.geometry.length / 1000.0}, geometry=midpoints, crs="EPSG:3857"
    )
    joined = gpd.sjoin(road_pts, grid_gdf, how="inner", predicate="within")

    per_cell: dict[str, float] = {}
    for grid_id, group in joined.groupby("grid_id"):
        per_cell[str(grid_id)] = float(group["len_km"].sum())
    return per_cell, len(roads)


# --------------------------------------------------------------------------- 栅格一层
def sample_raster_layer(
    tif_path: Path,
    cells: list[Cell],
    mode: str,
    label: str,
) -> dict[str, float | None]:
    """调用 RasterSampler 按网格取值。缺库/缺文件时明确报错，不静默返回空。"""
    from datasource.raster_adapter import GridCell, RasterSampler

    if not tif_path.exists():
        raise FileNotFoundError(f"{label} 栅格不存在：{tif_path}")
    grid_cells = [
        GridCell(
            grid_id=c.grid_id, lng=c.lng, lat=c.lat,
            lng_min=c.lng_min, lat_min=c.lat_min, lng_max=c.lng_max, lat_max=c.lat_max,
        )
        for c in cells
    ]
    with RasterSampler(tif_path) as sampler:
        print(f"    {label}: {sampler.describe()}")
        return sampler.sample_cells(grid_cells, mode=mode)


# --------------------------------------------------------------------------- 主流程
def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1d 空间网格底盘构建器")
    parser.add_argument("--city", default=DEFAULT_CITY,
                        help="市州名（从 region_codes.SICHUAN_CITY_BBOXES 查 bbox），默认 成都市")
    parser.add_argument("--bbox", default=None,
                        help="手动指定 经度最小,纬度最小,经度最大,纬度最大；传了它就覆盖 --city")
    parser.add_argument("--cell-size", type=float, default=DEFAULT_CELL_M, help="网格边长（米），默认 1000")
    parser.add_argument("--max-cells", type=int, default=DEFAULT_MAX_CELLS, help="网格数硬上限，超出直接退出")
    parser.add_argument("--truncate", action="store_true",
                        help="先清空 grid_metrics 再写入（换城市/重跑全域时用，避免旧网格残留）")
    parser.add_argument("--db", default=None, help="数据库路径")
    parser.add_argument("--pop-tif", default=str(DEFAULT_POP_TIF))
    parser.add_argument("--light-tif", default=str(DEFAULT_LIGHT_TIF))
    parser.add_argument("--road-shp", default=str(DEFAULT_ROAD_SHP))
    parser.add_argument("--skip-raster", action="store_true", help="跳过人口/灯光两层")
    parser.add_argument("--skip-roads", action="store_true", help="跳过路网一层")
    parser.add_argument("--dry-run", action="store_true", help="不写库，只打印统计")
    args = parser.parse_args(argv)

    # ---- 区域解析：--bbox 优先；否则按 --city 查表；都没有才用 DEFAULT_BBOX ----
    from datasource.region_codes import SICHUAN_CITY_BBOXES, city_adcode, city_bbox

    city_name = (args.city or DEFAULT_CITY).strip()
    if args.bbox:
        try:
            bbox = tuple(float(x) for x in args.bbox.split(","))
            assert len(bbox) == 4
        except (ValueError, AssertionError):
            print("[FAIL] --bbox 必须是 4 个数字：经度最小,纬度最小,经度最大,纬度最大")
            return 2
        bbox_source = "--bbox 手工指定"
    else:
        found = city_bbox(city_name)
        if found is None:
            known = "、".join(sorted(SICHUAN_CITY_BBOXES))
            print(f"[FAIL] 不认识的城市：{city_name}")
            print(f"       已登记：{known}")
            print("       新增办法：在 datasource/region_codes.py 的 SICHUAN_CITY_BBOXES 里加一行")
            return 2
        bbox = found
        bbox_source = f"--city {city_name}（region_codes 查表）"

    city_code = city_adcode(city_name) or ""
    city_name_std = city_name

    t0 = time.perf_counter()
    cells = build_cells(bbox, args.cell_size)
    print("=" * 72)
    print(f"区域来源  : {bbox_source}")
    print(f"区域 bbox : {bbox}")
    print(f"归属城市  : {city_name_std}（city_code={city_code or '空'}）")
    print(f"网格边长 : {args.cell_size:g} m")
    print(f"网格数量 : {len(cells)}  （上限 {args.max_cells}）")
    print(f"示例 id  : {cells[0].grid_id}  面积 {cells[0].area_km2:.4f} km²")
    print("=" * 72)

    # ===== 防 OOM 第一道闸：网格数超限直接退出，不做任何后续计算 =====
    if len(cells) > args.max_cells:
        print(f"[FAIL] 网格数 {len(cells)} 超过上限 {args.max_cells}，已中止（防内存爆炸）。")
        print("       办法：缩小 --bbox，或把 --cell-size 调大（如 2000）。")
        return 3

    from store.db_connection import create_connection, initialize_schema

    db_path = Path(args.db) if args.db else PROJECT_ROOT / "local_consumption_insight.db"
    if args.dry_run:
        conn = sqlite3.connect(":memory:")
        conn.executescript((PROJECT_ROOT / "schema_v5.sql").read_text(encoding="utf-8"))
    else:
        initialize_schema()
        conn = create_connection(db_path)

    # 老库平滑升级：schema_v5.sql 里已有 city_name，但已建好的表不会自动补列 → 这里补
    cols = {row[1] for row in conn.execute("PRAGMA table_info(grid_metrics)")}
    if "city_name" not in cols:
        conn.execute("ALTER TABLE grid_metrics ADD COLUMN city_name TEXT")
        conn.commit()
        print("[MIGRATE] grid_metrics 已补列 city_name")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_gm_citynm ON grid_metrics(city_name)")
    conn.commit()

    if args.truncate:
        before = conn.execute("SELECT COUNT(*) FROM grid_metrics").fetchone()[0]
        conn.execute("DELETE FROM grid_metrics")
        conn.commit()
        print(f"[TRUNCATE] 已清空 grid_metrics（原有 {before} 行），本次将整体重铺")

    batch_id = "grid_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    status, message = "succeeded", ""
    poi_counts: dict[str, int] = {}
    road_km: dict[str, float] = {}
    pop_map: dict[str, float | None] = {}
    light_map: dict[str, float | None] = {}
    poi_rows = road_features = 0
    poi_density_baidu_placeholder = None   # 百度 AK 未批：整列留 NULL

    try:
        # ---- 第 1 层：POI 密度（不需要地理库，先跑这个最容易出结果）----
        print("\n[1/4] 高德 POI → 网格（GCJ-02 转 WGS-84 后落格）")
        points = load_poi_points(conn)
        poi_rows = len(points)
        if not points:
            print("    [WARN] poi_merchant 里没有可用经纬度，POI 密度列将为空（先跑 S2-1a）")
        else:
            poi_counts = count_poi_per_cell(points, cells, bbox)
            inside = sum(poi_counts.values())
            print(f"    POI 总数 {poi_rows}，落在本次 bbox 内 {inside} 条，覆盖 {len(poi_counts)} 个网格")

        # ---- 第 2 层：人口（求和）与灯光（均值）----
        if args.skip_raster:
            print("\n[2/4] 人口/灯光：已按 --skip-raster 跳过")
            print("\n[3/4] 灯光：已跳过")
        else:
            print("\n[2/4] 人口栅格（WorldPop 100m，格内求和 = 人口数）")
            pop_map = sample_raster_layer(Path(args.pop_tif), cells, mode="sum", label="人口")
            ok = sum(1 for v in pop_map.values() if v is not None)
            print(f"    有效网格 {ok}/{len(cells)}；总人口 {sum(v for v in pop_map.values() if v):,.0f} 人")

            print("\n[3/4] 灯光栅格（VIIRS，格内均值）")
            light_map = sample_raster_layer(Path(args.light_tif), cells, mode="mean", label="灯光")
            ok = sum(1 for v in light_map.values() if v is not None)
            vals = [v for v in light_map.values() if v is not None]
            print(f"    有效网格 {ok}/{len(cells)}；均值 {sum(vals)/len(vals):.3f}" if vals else "    全部为空")

        # ---- 第 4 层：路网密度 ----
        if args.skip_roads:
            print("\n[4/4] 路网：已按 --skip-roads 跳过")
        else:
            print("\n[4/4] 路网密度（OSM shp → 3857 算长度 → 按中点归格）")
            road_km, road_features = compute_road_length_per_cell(Path(args.road_shp), cells, bbox)
            total_km = sum(road_km.values())
            print(f"    覆盖 {len(road_km)} 个网格，道路总长 {total_km:,.1f} km")

        # ---- 写库 ----
        # 6.6 万格逐行 execute 会很慢（每行一次 Python↔SQLite 往返），改 executemany 批量提交；
        # 内存里只放"一行的元组"，6.6 万条约十几 MB，可控。
        now = _now()
        written = 0
        pending: list[tuple] = []
        for cell in cells:
            pop = pop_map.get(cell.grid_id)
            light = light_map.get(cell.grid_id)
            count = poi_counts.get(cell.grid_id, 0)
            poi_density = (count / cell.area_km2) if cell.area_km2 > 0 else None
            length = road_km.get(cell.grid_id, 0.0)
            road_density = (length / cell.area_km2) if cell.area_km2 > 0 else None

            if args.dry_run:
                written += 1
                continue
            pending.append((
                cell.grid_id, round(cell.lng, 6), round(cell.lat, 6),
                pop, light,
                None if poi_density is None else round(poi_density, 4),
                poi_density_baidu_placeholder,
                None if road_density is None else round(road_density, 4),
                cell.area_km2, count, round(length, 4),
                city_code, city_name_std, now,
            ))
            written += 1

        if pending:
            conn.executemany(UPSERT_SQL, pending)
            conn.commit()
            print(f"    已批量写入 {len(pending)} 行（executemany）")

        elapsed_ms = int((time.perf_counter() - t0) * 1000)
        print("\n" + "=" * 72)
        print(f"写入网格 {written} 行{'（dry-run 未落盘）' if args.dry_run else ''}，耗时 {elapsed_ms} ms")
        if not args.dry_run:
            conn.execute(
                LOG_SQL,
                (
                    batch_id, bbox[0], bbox[1], bbox[2], bbox[3], args.cell_size,
                    len(cells), args.max_cells, poi_rows, road_features,
                    str(Path(args.pop_tif).name), str(Path(args.light_tif).name),
                    elapsed_ms, status, message, now,
                ),
            )
            conn.commit()
            print(f"批次：{batch_id}")
            print("下一步：D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1d.py")
        else:
            print("[dry-run] 未写库；确认无误后去掉 --dry-run 再跑。")

    except (ImportError, FileNotFoundError, MemoryError) as exc:
        status, message = "failed", str(exc)
        print(f"\n[FAIL] {exc}")
        if not args.dry_run:
            conn.execute(
                LOG_SQL,
                (
                    batch_id, bbox[0], bbox[1], bbox[2], bbox[3], args.cell_size,
                    len(cells), args.max_cells, poi_rows, road_features,
                    str(Path(args.pop_tif).name), str(Path(args.light_tif).name),
                    int((time.perf_counter() - t0) * 1000), status, message, _now(),
                ),
            )
            conn.commit()
        conn.close()
        return 4

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
