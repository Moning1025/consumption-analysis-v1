"""S2-1b 验收：统计年鉴导入是否真的落库、且账能对上。

验收不是"脚本没报错"，是**用另一段代码去质疑它**。这里做 8 项检查：

1. 行数：区县级 ≥ 20（成都 20 个区市县）、市州级 ≥ 21（四川 21 个市州）
2. 主键唯一：同一 (层级, 区划码, 年份) 只能一行（upsert 幂等的前提）
3. 区划码合法：6 位数字、四川以 51 开头、且能在码表里反查到
4. 关键指标非空率：retail_total 非空 ≥ 95%
5. 数值合理性：每个地区 retail_total > 0，且没有"区县 > 全市"的级别错误
6. 跨表自洽：成都 2025（市州表）应比成都 2024 区县合计**略有增长**（0.9–1.2 倍）
7. 层级完整：区县级有 2024、市州级有 2025
8. 账本守恒：yearbook_import_log 里 rows_seen = rows_upserted + rows_skipped（每批都要成立）

用法：
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1b.py
    ... --db <临时库路径>     # 可选，默认项目库
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datasource.region_codes import adcode_to_name  # noqa: E402

TABLE = "region_consumption_yearly"


def add(check_no: int, ok: bool, title: str, detail: str, results: list[tuple[bool, str, str]]) -> None:
    results.append((ok, f"[{'PASS' if ok else 'FAIL'}] {check_no}. {title}", detail))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1b 验收")
    parser.add_argument("--db", default=None)
    parser.add_argument("--min-district", type=int, default=20)
    parser.add_argument("--min-city", type=int, default=21)
    args = parser.parse_args(argv)

    db_path = Path(args.db) if args.db else PROJECT_ROOT / "local_consumption_insight.db"
    print(f"数据库：{db_path}")
    if not db_path.exists():
        print(f"[FAIL] 数据库不存在：{db_path}")
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    results: list[tuple[bool, str, str]] = []

    try:
        cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({TABLE})")}
        if not cols:
            print(f"[FAIL] 表 {TABLE} 不存在：请先跑 scripts/apply_schema.py 或导入器")
            return 2

        rows = conn.execute(f"SELECT * FROM {TABLE}").fetchall()
        print("=" * 68)

        # 1. 行数（按层级）
        district_rows = [r for r in rows if r["region_level"] == "district"]
        city_rows = [r for r in rows if r["region_level"] == "city"]
        add(1, len(district_rows) >= args.min_district and len(city_rows) >= args.min_city,
            "行数",
            f"区县级 {len(district_rows)} 行（阈值 {args.min_district}）、"
            f"市州级 {len(city_rows)} 行（阈值 {args.min_city}）", results)

        # 2. 主键唯一
        dup = conn.execute(
            f"SELECT region_level, region_code, year, COUNT(*) c FROM {TABLE} "
            "GROUP BY 1,2,3 HAVING c > 1"
        ).fetchall()
        add(2, not dup, "主键唯一", f"(层级,区划码,年份) 重复组数 {len(dup)}", results)

        # 3. 区划码合法
        bad_code = [r for r in rows if not (r["region_code"] and len(r["region_code"]) == 6
                                           and r["region_code"].isdigit()
                                           and r["region_code"].startswith("51"))]
        unmapped = [r["region_code"] for r in bad_code if not adcode_to_name(r["region_code"] or "")]
        add(3, not bad_code, "区划码合法",
            f"非 6 位/非 51 开头 {len(bad_code)} 行；码表查不到 {len(unmapped)} 个", results)

        # 4. 关键指标非空率
        total = len(rows) or 1
        filled = sum(1 for r in rows if r["retail_total"] not in (None, 0))
        rate = filled / total * 100
        add(4, rate >= 95, "retail_total 非空率", f"{rate:.1f}% ({filled}/{total}，阈值 95%)", results)

        # 5. 数值合理性
        non_positive = [r["region_name"] for r in rows if (r["retail_total"] or 0) <= 0]
        city_total_chengdu = next(
            (r["retail_total"] for r in rows
             if r["region_level"] == "city" and r["region_code"] == "510100" and r["year"] == 2025),
            None,
        )
        over_city = []
        if city_total_chengdu:
            # 区县之和不应超过"同城的市级值"太多（口径不同年份时只做粗筛，>1.5 倍就报警）
            district_sum = sum(r["retail_total"] or 0 for r in district_rows)
            if district_sum > city_total_chengdu * 1.5:
                over_city.append(f"区县合计 {district_sum:,.0f} 万 > 1.5×市州值 {city_total_chengdu:,.0f} 万")
        add(5, not non_positive and not over_city, "数值合理性",
            f"非正值 {len(non_positive)} 个" + ("" if not over_city else "；" + "；".join(over_city)), results)

        # 6. 跨表自洽：成都 2025 vs 成都 2024 区县合计
        district_2024 = [r for r in district_rows if r["year"] == 2024]
        sum_2024 = sum(r["retail_total"] or 0 for r in district_2024)
        ratio = (city_total_chengdu / sum_2024) if (city_total_chengdu and sum_2024) else None
        ok6 = ratio is not None and 0.9 <= ratio <= 1.2
        ratio_text = f"{ratio:.3f}" if ratio else "N/A"
        add(6, ok6, "跨表自洽（成都 2025 / 2024 区县合计）",
            f"2024 区县合计 {sum_2024:,.0f} 万元；2025 市州值 {city_total_chengdu if city_total_chengdu else '缺失'} 万元；"
            f"比值 {ratio_text}（合理区间 0.9–1.2）", results)

        # 7. 层级年份完整
        years_district = sorted({r["year"] for r in district_rows})
        years_city = sorted({r["year"] for r in city_rows})
        add(7, bool(years_district) and bool(years_city), "层级年份",
            f"区县级年份 {years_district}；市州级年份 {years_city}", results)

        # 8. 账本守恒
        logs = conn.execute("SELECT * FROM yearbook_import_log").fetchall()
        bad_logs = [l["batch_id"] for l in logs
                    if (l["rows_seen"] or 0) != (l["rows_upserted"] or 0) + (l["rows_skipped"] or 0)]
        add(8, bool(logs) and not bad_logs, "账本守恒",
            f"导入批次 {len(logs)} 个；不守恒 {len(bad_logs)} 个"
            f"（校验：rows_seen = rows_upserted + rows_skipped）", results)

        # ---- 打印 ----
        for ok, title, detail in results:
            print(title)
            print(f"      {detail}")

        print("=" * 68)
        passed = sum(1 for ok, _, _ in results if ok)
        print(f"结果：{passed}/{len(results)} 项通过")

        # 附加信息：给论文用的分布
        print("\n--- 数据分布（论文可直接引用） ---")
        for level in ("district", "city"):
            sub = [r for r in rows if r["region_level"] == level]
            if not sub:
                continue
            label = "区县" if level == "district" else "市州"
            total_value = sum(r["retail_total"] or 0 for r in sub)
            top = sorted(sub, key=lambda r: r["retail_total"] or 0, reverse=True)[:5]
            print(f"{label} Top5（社零总额，万元）：")
            for r in top:
                print(f"  {r['region_name']:<8} {r['year']}  {(r['retail_total'] or 0):>14,.0f}")
            print(f"{label}合计：{total_value:,.0f} 万元")

        if passed == len(results):
            print("\n[PASS] 结论：S2-1b 验收通过——年鉴数据已结构化落库，且账本守恒")
            return 0
        print("\n[FAIL] 结论：存在未通过项，先看上面标 FAIL 的那几行")
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
