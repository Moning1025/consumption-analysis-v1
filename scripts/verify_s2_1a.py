"""S2-1a 验收脚本：证明“真实数据真的进来了”（第一层代码，不用你改）。

为什么要有它
--------------------------------------------------------------------
答辩时老师不会听“我跑通了”，他要看证据。这个脚本只做一件事：
把 `poi_merchant` 表里的数据逐项体检，每项给出 PASS / FAIL，
最后一行给总判定。任何一项 FAIL 都会以退出码 1 结束（方便以后接 CI）。

检查 7 项
--------------------------------------------------------------------
1. 表存在、有数据          —— 最少 50 行，否则说明还没采或者采失败
2. poi_id 无重复           —— 主键保证，但重复采集逻辑写错时会出现（这里独立复核）
3. 经纬度非空率            —— ≥ 95%，缺经纬度的点做不了空间分箱
4. 经纬度落在合理范围      —— 中国 bbox（73~136, 3~54）内；四川数据还要落四川 bbox
5. 行政区划码覆盖率        —— adcode ≥ 90%，这是后面 S2-2 地域归一化的原料
6. 四川数据的 adcode 前缀  —— 四川是 51 开头，混进别的省说明 city 参数或 citylimit 有问题
7. 采集账本自洽            —— poi_fetch_log 里 返回 = 新增 + 覆盖 + 跳过

用法
--------------------------------------------------------------------
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1a.py
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1a.py --db 别的库.db
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import settings  # noqa: E402
from store.db_connection import fetch_all, fetch_one  # noqa: E402

# 中国大致经纬度范围（含南海诸岛），用来抓“经纬度写反了/单位错了”这类低级错误
CHINA_BBOX = (73.0, 3.0, 136.0, 54.0)      # (最小经度, 最小纬度, 最大经度, 最大纬度)
# 四川大致范围
SICHUAN_BBOX = (97.2, 26.0, 108.6, 34.4)

results: list[tuple[bool, str, str]] = []


def check(ok: bool, name: str, detail: str) -> None:
    results.append((ok, name, detail))
    print(f"  [{'PASS' if ok else 'FAIL'}] {name}：{detail}")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1a 高德 POI 数据验收")
    parser.add_argument("--db", default=None, help="自定义数据库路径")
    parser.add_argument("--min-rows", type=int, default=50, help="最少行数（默认 50）")
    args = parser.parse_args(argv)
    db = str(Path(args.db)) if args.db else None

    print(f"数据库: {db or settings.SQLITE_DATABASE_PATH}")
    print("=" * 64)

    # ---- 1. 表存在且有数据 -------------------------------------------
    table = fetch_one(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='poi_merchant'",
        database_path=db,
    )
    if not table:
        check(False, "表 poi_merchant", "不存在——先跑 scripts/fetch_amap_poi.py（它会建表）")
        summarize()
        return 1

    total = fetch_one("SELECT COUNT(*) AS c FROM poi_merchant", database_path=db)["c"]
    check(total >= args.min_rows, "行数", f"{total} 行（阈值 {args.min_rows}）")
    if total == 0:
        summarize()
        return 1

    # ---- 2. poi_id 无重复 --------------------------------------------
    dup = fetch_one(
        "SELECT COUNT(*) AS c FROM (SELECT poi_id FROM poi_merchant"
        " GROUP BY poi_id HAVING COUNT(*) > 1)",
        database_path=db,
    )["c"]
    check(dup == 0, "poi_id 唯一", f"重复 {dup} 个")

    # ---- 3. 经纬度非空率 ---------------------------------------------
    with_geo = fetch_one(
        "SELECT COUNT(*) AS c FROM poi_merchant"
        " WHERE longitude IS NOT NULL AND latitude IS NOT NULL",
        database_path=db,
    )["c"]
    ratio = with_geo / total if total else 0
    check(ratio >= 0.95, "经纬度非空率", f"{ratio:.1%}（{with_geo}/{total}，阈值 95%）")

    # ---- 4. 经纬度落在合理范围 ---------------------------------------
    lo_lng, lo_lat, hi_lng, hi_lat = CHINA_BBOX
    bad_geo = fetch_one(
        "SELECT COUNT(*) AS c FROM poi_merchant WHERE longitude IS NOT NULL AND ("
        " longitude < ? OR longitude > ? OR latitude < ? OR latitude > ?)",
        (lo_lng, hi_lng, lo_lat, hi_lat),
        database_path=db,
    )["c"]
    check(bad_geo == 0, "经纬度在中国范围内", f"越界 {bad_geo} 条（bbox {CHINA_BBOX}）")

    out_sichuan = fetch_one(
        "SELECT COUNT(*) AS c FROM poi_merchant WHERE province LIKE '%四川%' AND ("
        " longitude < ? OR longitude > ? OR latitude < ? OR latitude > ?)",
        (SICHUAN_BBOX[0], SICHUAN_BBOX[2], SICHUAN_BBOX[1], SICHUAN_BBOX[3]),
        database_path=db,
    )["c"]
    sichuan_total = fetch_one(
        "SELECT COUNT(*) AS c FROM poi_merchant WHERE province LIKE '%四川%'",
        database_path=db,
    )["c"]
    if sichuan_total:
        check(out_sichuan == 0, "四川数据落在四川范围",
              f"越界 {out_sichuan} 条（四川共 {sichuan_total} 条，bbox {SICHUAN_BBOX}）")

    # ---- 5. 行政区划码覆盖率 -----------------------------------------
    with_adcode = fetch_one(
        "SELECT COUNT(*) AS c FROM poi_merchant WHERE adcode IS NOT NULL AND adcode <> ''",
        database_path=db,
    )["c"]
    ratio = with_adcode / total if total else 0
    check(ratio >= 0.90, "adcode 覆盖率", f"{ratio:.1%}（{with_adcode}/{total}，阈值 90%）")

    # ---- 6. 四川数据 adcode 前缀 -------------------------------------
    if sichuan_total:
        wrong_prefix = fetch_one(
            "SELECT COUNT(*) AS c FROM poi_merchant WHERE province LIKE '%四川%'"
            " AND adcode NOT LIKE '51%'",
            database_path=db,
        )["c"]
        check(wrong_prefix == 0, "四川 adcode 前缀", f"非 51 开头的 {wrong_prefix} 条")

    # ---- 7. 采集账本自洽 ---------------------------------------------
    log_row = fetch_one(
        "SELECT COUNT(*) AS pages, COALESCE(SUM(rows_returned),0) AS ret,"
        " COALESCE(SUM(rows_inserted),0) AS ins, COALESCE(SUM(rows_updated),0) AS upd,"
        " COALESCE(SUM(rows_skipped),0) AS skp FROM poi_fetch_log",
        database_path=db,
    )
    if log_row["pages"]:
        diff = log_row["ret"] - (log_row["ins"] + log_row["upd"] + log_row["skp"])
        check(diff == 0, "账本守恒",
              f"返回 {log_row['ret']}，新增 {log_row['ins']} + 覆盖 {log_row['upd']}"
              f" + 跳过 {log_row['skp']}，差 {diff}（{log_row['pages']} 页记录）")
    else:
        check(False, "账本守恒", "poi_fetch_log 没有记录——说明数据不是用 fetch_amap_poi.py 采的")

    # ---- 附：分布速览（不计入判定） ----------------------------------
    print("-" * 64)
    print("业态分布 Top 8：")
    for row in fetch_all(
        "SELECT COALESCE(NULLIF(category_l1,''),'(未分类)') AS k, COUNT(*) AS c"
        " FROM poi_merchant GROUP BY k ORDER BY c DESC LIMIT 8",
        database_path=db,
    ):
        print(f"    {row['k']:<12} {row['c']:>6}")
    print("区县分布 Top 5：")
    for row in fetch_all(
        "SELECT COALESCE(NULLIF(district,''),'(空)') AS k, COUNT(*) AS c"
        " FROM poi_merchant GROUP BY k ORDER BY c DESC LIMIT 5",
        database_path=db,
    ):
        print(f"    {row['k']:<12} {row['c']:>6}")

    return summarize()


def summarize() -> int:
    passed = sum(1 for ok, _, _ in results if ok)
    failed = [name for ok, name, _ in results if not ok]
    print("=" * 64)
    print(f"结果：{passed}/{len(results)} 项通过")
    if failed:
        print(f"未通过：{', '.join(failed)}")
        print("[FAIL] 结论：S2-1a 验收未通过")
        return 1
    print("[PASS] 结论：S2-1a 验收通过——真实高德 POI 已落库且自洽")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
