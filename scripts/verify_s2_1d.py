"""S2-1d 验收：空间网格底盘是否可信、且没被"内存爆炸"偷偷截断。

9 项检查（每一项都在防一类特定的错）：

1. 网格数：≥ 1000 且 ≤ 构建上限（少了说明 bbox 填错，多了说明防 OOM 失效）
2. grid_id 唯一
3. 坐标在 bbox 内（防止坐标系没转就落库）
4. 面积合计 ≈ bbox 理论面积（误差 < 10%；对不上说明网格生成有重叠或缺口）
5. 人口非空率 ≥ 60%（龙门山脉等无人区天然为空，不是错误）
6. 灯光非空率 ≥ 60%（栅格边缘 nodata 天然为空）
7. POI 密度 = poi_count / area_km2（抽样 200 行复算，误差 < 1%）
8. 百度密度列全空（AK 未批，必须整列为 NULL；一旦有值就说明有人偷偷填了假数）
   + 账本 grid_build_log 有成功批次
9. 城市归属 city_name 全部标注（多城市共存时防止数据串城市）

关于 bbox：默认从 region_codes.SICHUAN_CITY_BBOXES 查"成都市"的外包框（102.5–105.0E,
29.0–31.5N），它**不是行政边界**——矩形里含周边市州地块，所以面积校验只要求 10% 以内，
而不是去对成都市域面积 14335 km²。

用法：
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1d.py
    ... --city 成都市 --min-pop-rate 0.5
    ... --db <临时库> --bbox 103.8,30.5,104.3,30.9
"""

from __future__ import annotations

import argparse
import math
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
# 直接 `python scripts\verify_s2_1d.py` 跑时，sys.path[0] 是 scripts\ 而不是项目根，
# 所以要手动把项目根塞进去，否则 import datasource.region_codes 会 ModuleNotFoundError。
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_CITY = "成都市"
DEFAULT_BBOX = (103.80, 30.50, 104.30, 30.90)   # 仅在 city 查不到时兜底


def bbox_area_km2(bbox: tuple[float, float, float, float]) -> float:
    """bbox 的球面近似面积（km²）：纬度跨度 × 经度跨度 × cos(中心纬度)。"""
    lng_min, lat_min, lng_max, lat_max = bbox
    height = (lat_max - lat_min) * 111.32
    width = (lng_max - lng_min) * 111.32 * math.cos(math.radians((lat_min + lat_max) / 2))
    return height * width


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1d 验收")
    parser.add_argument("--db", default=None)
    parser.add_argument("--city", default=DEFAULT_CITY,
                        help=f"按市州名取 bbox（与构建时的 --city 保持一致），默认 {DEFAULT_CITY}")
    parser.add_argument("--bbox", default=None, help="手动指定 bbox，优先级高于 --city")
    parser.add_argument("--min-rows", type=int, default=1000)
    parser.add_argument("--max-rows", type=int, default=200_000)
    parser.add_argument("--min-pop-rate", type=float, default=0.60,
                        help="人口非空率下限，默认 0.60（龙门山脉等无人区会天然为空）")
    parser.add_argument("--min-light-rate", type=float, default=0.60,
                        help="灯光非空率下限，默认 0.60（栅格边缘 nodata 会天然为空）")
    parser.add_argument("--area-tolerance", type=float, default=10.0,
                        help="面积偏差容忍百分比，默认 10（外包框含周边市州地块）")
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else PROJECT_ROOT / "local_consumption_insight.db"

    # bbox 解析顺序：--bbox > --city（默认成都市）查 region_codes 表 > 兜底主城区
    from datasource.region_codes import city_bbox

    if args.bbox:
        try:
            bbox = tuple(float(x) for x in args.bbox.split(","))
            assert len(bbox) == 4
        except (ValueError, AssertionError):
            print("[FAIL] --bbox 必须是 4 个数字：经度最小,纬度最小,经度最大,纬度最大")
            return 2
        bbox_source = "--bbox 手工指定"
    else:
        found = city_bbox(args.city) if args.city else None
        if found is None:
            print(f"[FAIL] 不认识的城市：{args.city}（在 region_codes.SICHUAN_CITY_BBOXES 里加一行即可）")
            return 2
        bbox = found
        bbox_source = f"--city {args.city}（region_codes 查表）"

    print(f"数据库：{db_path}")
    print(f"验收 bbox：{bbox}   来源：{bbox_source}")
    if not db_path.exists():
        print(f"[FAIL] 数据库不存在：{db_path}")
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    results: list[tuple[bool, str, str]] = []

    def add(ok: bool, no: int, title: str, detail: str) -> None:
        results.append((ok, f"[{'PASS' if ok else 'FAIL'}] {no}. {title}", detail))

    try:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(grid_metrics)")}
        if not cols:
            print("[FAIL] 表 grid_metrics 不存在：先跑 scripts/grid_metrics_builder.py")
            return 2

        rows = conn.execute("SELECT * FROM grid_metrics").fetchall()
        total = len(rows)
        print("=" * 68)

        # 1. 网格数
        add(args.min_rows <= total <= args.max_rows, 1, "网格数",
            f"{total} 行（要求 {args.min_rows}–{args.max_rows}）")

        # 2. 唯一性
        dup = conn.execute(
            "SELECT grid_id, COUNT(*) c FROM grid_metrics GROUP BY grid_id HAVING c > 1"
        ).fetchall()
        add(not dup, 2, "grid_id 唯一", f"重复 {len(dup)} 个")

        # 3. 坐标在 bbox 内
        lng_min, lat_min, lng_max, lat_max = bbox
        outside = [r["grid_id"] for r in rows
                   if not (lng_min <= (r["lng"] or 0) <= lng_max and lat_min <= (r["lat"] or 0) <= lat_max)]
        add(not outside, 3, "坐标落在 bbox 内", f"越界 {len(outside)} 个")

        # 4. 面积合计 vs 理论面积（外包框，容忍 10%）
        area_sum = sum(r["area_km2"] or 0 for r in rows)
        theory = bbox_area_km2(bbox)
        diff_pct = abs(area_sum - theory) / theory * 100 if theory else 999
        add(diff_pct < args.area_tolerance, 4, "面积合计 ≈ bbox 理论面积",
            f"网格合计 {area_sum:,.2f} km² vs 外包框理论 {theory:,.2f} km²，"
            f"偏差 {diff_pct:.2f}%（阈值 {args.area_tolerance:g}%；外包框含周边市州地块，故不用市域面积对账）")

        # 5/6. 栅格非空率（无人区/栅格边缘天然为空，阈值放宽到 60%）
        pop_ok = sum(1 for r in rows if r["pop_count"] is not None)
        light_ok = sum(1 for r in rows if r["light_intensity"] is not None)
        pop_rate = pop_ok / max(total, 1)
        light_rate = light_ok / max(total, 1)
        add(pop_rate >= args.min_pop_rate, 5, "人口非空率",
            f"{pop_rate * 100:.1f}% ({pop_ok}/{total}，阈值 {args.min_pop_rate * 100:.0f}%)"
            f"；空网格 {total - pop_ok} 个多为龙门山脉等无人区，属真实物理现状")
        add(light_rate >= args.min_light_rate, 6, "灯光非空率",
            f"{light_rate * 100:.1f}% ({light_ok}/{total}，阈值 {args.min_light_rate * 100:.0f}%)"
            f"；空网格 {total - light_ok} 个，多为栅格边缘 nodata")

        # 7. 密度复算（抽样 200 行）
        sample = conn.execute(
            "SELECT grid_id, area_km2, poi_count_amap, poi_density_amap FROM grid_metrics "
            "WHERE poi_density_amap IS NOT NULL LIMIT 200"
        ).fetchall()
        bad = []
        for r in sample:
            expect = (r["poi_count_amap"] or 0) / (r["area_km2"] or 1)
            got = r["poi_density_amap"] or 0
            if expect > 0 and abs(got - expect) / expect > 0.01:
                bad.append(r["grid_id"])
        add(sample and not bad, 7, "POI 密度复算",
            f"抽样 {len(sample)} 行，复算不一致 {len(bad)} 行（阈值 1%）")

        # 8. 百度列必须全空 + 账本存在
        baidu_filled = conn.execute(
            "SELECT COUNT(*) FROM grid_metrics WHERE poi_density_baidu IS NOT NULL"
        ).fetchone()[0]
        logs = conn.execute(
            "SELECT COUNT(*) FROM grid_build_log WHERE status = 'succeeded'"
        ).fetchone()[0]
        add(baidu_filled == 0 and logs > 0, 8, "百度列留空 + 构建账本",
            f"poi_density_baidu 非空 {baidu_filled} 行（必须为 0）；成功批次 {logs} 个")

        # 9. 城市归属一致（多城市共存时，防止"成都的网格混进眉山的数据"）
        if "city_name" in cols:
            dist = conn.execute(
                "SELECT city_name, COUNT(*) c FROM grid_metrics "
                "GROUP BY city_name ORDER BY c DESC"
            ).fetchall()
            named = [r for r in dist if r["city_name"]]
            nulls = sum(r["c"] for r in dist if not r["city_name"])
            detail = "、".join(f"{r['city_name']} {r['c']} 格" for r in named[:5]) or "（全为空）"
            if nulls:
                detail += f"；city_name 为空 {nulls} 行"
            add(bool(named) and nulls == 0, 9, "城市归属已标注（city_name）", detail)
        else:
            add(False, 9, "城市归属已标注（city_name）", "表里没有 city_name 列：请用新版构建器重跑")

        for ok, title, detail in results:
            print(title)
            print(f"      {detail}")

        print("=" * 68)
        passed = sum(1 for ok, _, _ in results if ok)
        print(f"结果：{passed}/{len(results)} 项通过")

        # 论文用分布
        print("\n--- 分布（论文可直接引用） ---")
        top_pop = conn.execute(
            "SELECT grid_id, lng, lat, pop_count, light_intensity, poi_density_amap, road_density "
            "FROM grid_metrics WHERE pop_count IS NOT NULL ORDER BY pop_count DESC LIMIT 5"
        ).fetchall()
        print("人口 TOP5 网格：")
        for r in top_pop:
            print(f"  {r['grid_id']:<24} 人口 {r['pop_count']:>10,.0f}  "
                  f"灯光 {r['light_intensity'] if r['light_intensity'] is None else round(r['light_intensity'], 3)}  "
                  f"POI 密度 {r['poi_density_amap']}  路网 {r['road_density']}")
        top_poi = conn.execute(
            "SELECT grid_id, pop_count, poi_density_amap FROM grid_metrics "
            "WHERE poi_density_amap IS NOT NULL ORDER BY poi_density_amap DESC LIMIT 5"
        ).fetchall()
        print("POI 密度 TOP5 网格（供给最密集）：")
        for r in top_poi:
            print(f"  {r['grid_id']:<24} POI 密度 {r['poi_density_amap']:>8}  "
                  f"人口 {r['pop_count'] if r['pop_count'] is None else round(r['pop_count'])}")

        if passed == len(results):
            print("\n[PASS] 结论：S2-1d 验收通过——网格底盘五列齐全，密度可复算")
            return 0
        print("\n[FAIL] 结论：存在未通过项，先看标 FAIL 的行")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
