# -*- coding: utf-8 -*-
"""S2-1c 验收脚本 —— 第一层（沫宁鱼全写，老大只跑）

九项检查，任何一项 FAIL 就退出码 1：

 1. 行数达标          该 profile 下至少有 N 行（默认为 90000）
 2. 唯一键干净        external_id 无空值、无重复
 3. 数据源标签        全部 source_kind = 'olist'
 4. 区域隔离标签      全部 region_profile = 'olist-br'
 5. 不污染四川        region_code 必须全为 NULL（巴西城市不许套中国区划码）
 6. 金额合法          无 ≤0、无非数字，均值落在合理区间
 7. 日期范围          occurred_date 落在 Olist 真实区间 2016-09-01 ~ 2018-11-30
 8. 品类完整          category_label 无 NULL / 空串
 9. 账本对得上        olist_import_log 有本批次记录，且 rows_read = 写入 + 三类跳过

额外开关：
    --recheck-source   重新扫一遍 CSV 复算总金额，与库里的 SUM 对账（慢约 10 秒，最硬的证据）
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

DEFAULT_DB = PROJECT_ROOT / "local_consumption_insight.db"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data_raw" / "olist"

REGION_PROFILE = "olist-br"
SOURCE_KIND = "olist"
DATE_MIN = "2016-09-01"
DATE_MAX = "2018-11-30"


class Checker:
    """收集检查结果的小工具，统一打印格式与退出码。"""

    def __init__(self) -> None:
        self.total = 0
        self.failed: list[str] = []

    def check(self, name: str, ok: bool, detail: str) -> bool:
        self.total += 1
        print(f"[{'PASS' if ok else 'FAIL'}] {self.total}. {name}：{detail}")
        if not ok:
            self.failed.append(name)
        return ok

    def report(self) -> int:
        passed = self.total - len(self.failed)
        print("\n" + "=" * 58)
        print(f"结果：{passed}/{self.total} 项通过")
        if self.failed:
            print("未通过：" + "；".join(self.failed))
        print("=" * 58)
        return 0 if not self.failed else 1


def scalar(conn: sqlite3.Connection, sql: str, params: tuple = ()):
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S2-1c · Olist 导入验收")
    parser.add_argument("--db", default=str(DEFAULT_DB))
    parser.add_argument("--min-rows", type=int, default=90000, help="最少行数")
    parser.add_argument("--profile", default=REGION_PROFILE)
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR))
    parser.add_argument("--recheck-source", action="store_true", help="回扫 CSV 复算总金额对账")
    args = parser.parse_args(argv)

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"[错误] 库不存在：{db_path}")
        return 2

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    c = Checker()
    try:
        # 确认列存在（老库若没跑过 import_olist.py，这里会直接提示）
        cols = {row[1] for row in conn.execute("PRAGMA table_info(consumption_record)")}
        if "external_id" not in cols or "region_profile" not in cols:
            print("[错误] consumption_record 缺少 external_id / region_profile 列。")
            print("       请先运行一次：python scripts\\import_olist.py --dry-run")
            return 2

        where = "WHERE region_profile = ?"
        p = (args.profile,)

        rows_total = scalar(conn, f"SELECT COUNT(*) FROM consumption_record {where}", p) or 0
        c.check("行数达标", rows_total >= args.min_rows, f"{rows_total} 行（阈值 {args.min_rows}）")

        blank_ext = scalar(
            conn,
            f"SELECT COUNT(*) FROM consumption_record {where} AND (external_id IS NULL OR external_id = '')",
            p,
        )
        dup_ext = scalar(
            conn,
            f"SELECT COUNT(*) FROM (SELECT external_id FROM consumption_record {where} "
            f"GROUP BY external_id HAVING COUNT(*) > 1)",
            p,
        )
        c.check("唯一键干净", blank_ext == 0 and dup_ext == 0,
                f"空 {blank_ext}，重复 {dup_ext}")

        bad_source = scalar(
            conn,
            f"SELECT COUNT(*) FROM consumption_record {where} AND source_kind <> ?",
            (args.profile, SOURCE_KIND),
        )
        c.check("数据源标签", bad_source == 0, f"非 '{SOURCE_KIND}' 的行 {bad_source}")

        # 第 4 项按「数据源分域」校验：只约束 Olist 自己的行，不要求整张表都是 olist-br。
        # 理由：库里还有 5 条成都本地流水（source_kind='local'），它们的 profile 是本地口径，
        # 它们不是 olist-br 才叫正确。把整表都要求成 olist-br 是校验逻辑写错了，不是数据错。
        bad_profile = scalar(
            conn,
            "SELECT COUNT(*) FROM consumption_record "
            "WHERE source_kind = ? AND (region_profile IS NULL OR region_profile <> ?)",
            (SOURCE_KIND, args.profile),
        )
        local_rows = scalar(
            conn, "SELECT COUNT(*) FROM consumption_record WHERE source_kind <> ?", (SOURCE_KIND,)
        )
        c.check("区域隔离标签", bad_profile == 0,
                f"Olist 行全部 '{args.profile}'（违规 {bad_profile}）；"
                f"另有非 Olist 本地行 {local_rows} 条，不参与本项约束")

        # 第 5 项同理：只看 Olist 的行有没有被塞进中国区划码。
        # 巴西城市一旦被写了 region_code，就会在前端四川地图上冒出一个假点位——这是伪造数据。
        leaked = scalar(
            conn,
            "SELECT COUNT(*) FROM consumption_record WHERE source_kind = ? AND region_code IS NOT NULL",
            (SOURCE_KIND,),
        )
        c.check("不污染四川", leaked == 0, f"Olist 行被写了 region_code 的 {leaked} 条（必须为 0）")

        bad_amount = scalar(
            conn,
            f"SELECT COUNT(*) FROM consumption_record {where} AND (consumption_amount IS NULL OR consumption_amount <= 0)",
            p,
        )
        total_amount = scalar(conn, f"SELECT SUM(consumption_amount) FROM consumption_record {where}", p) or 0.0
        avg_amount = (total_amount / rows_total) if rows_total else 0.0
        c.check("金额合法", bad_amount == 0 and 1.0 <= avg_amount <= 1000.0,
                f"非法 {bad_amount}；总额 {total_amount:,.2f}，均价 {avg_amount:.2f}")

        out_of_range = scalar(
            conn,
            f"SELECT COUNT(*) FROM consumption_record {where} AND (occurred_date < ? OR occurred_date > ? OR occurred_date IS NULL)",
            (args.profile, DATE_MIN, DATE_MAX),
        )
        dmin = scalar(conn, f"SELECT MIN(occurred_date) FROM consumption_record {where}", p)
        dmax = scalar(conn, f"SELECT MAX(occurred_date) FROM consumption_record {where}", p)
        c.check("日期范围", out_of_range == 0,
                f"{dmin} ~ {dmax}，越界 {out_of_range} 行")

        bad_cat = scalar(
            conn,
            f"SELECT COUNT(*) FROM consumption_record {where} AND (category_label IS NULL OR TRIM(category_label) = '')",
            p,
        )
        c.check("品类完整", bad_cat == 0, f"空品类 {bad_cat} 行")

        log = conn.execute(
            "SELECT batch_identifier, rows_read, rows_written, rows_skipped_status, "
            "rows_skipped_invalid, rows_skipped_dup FROM olist_import_log "
            "ORDER BY log_id DESC LIMIT 1"
        ).fetchone()
        if log is None:
            c.check("账本对得上", False, "olist_import_log 无记录（没跑过 import_olist.py？）")
        else:
            batch, read_n, written, s_status, s_invalid, s_dup = log
            accounted = written + s_status + s_invalid + s_dup
            c.check("账本对得上", read_n == accounted,
                    f"批次 {batch}：{read_n} = {written} + {s_status} + {s_invalid} + {s_dup}")

        # 第十项（可选）：回扫源 CSV 复算总金额
        if args.recheck_source:
            from scripts.import_olist import (  # 延迟导入：不加 --recheck-source 时不做重活
                EXCLUDED_STATUS, build_row, iter_order_items,
                load_category_translation, load_customers, load_orders, load_products, load_sellers,
            )
            data_dir = Path(args.data_dir)
            orders = load_orders(data_dir)
            translation = load_category_translation(data_dir)
            products = load_products(data_dir, translation)
            sellers = load_sellers(data_dir)
            customers = load_customers(data_dir)

            recomputed = 0.0
            counted = 0
            for item in iter_order_items(data_dir):
                row, reason = build_row(item, orders, products, sellers, customers, "recheck", False)
                if row is None:
                    continue
                recomputed += row["consumption_amount"]
                counted += 1
            diff = abs(recomputed - total_amount)
            c.check("源表复算对账", diff < max(1.0, recomputed * 0.001),
                    f"源 {recomputed:,.2f} vs 库 {total_amount:,.2f}，差 {diff:.4f}"
                    f"（行数 源 {counted} vs 库 {rows_total}）")
        else:
            print("[跳过] 源表复算对账（加 --recheck-source 开启，慢约 10 秒）")
    finally:
        conn.close()

    return c.report()


if __name__ == "__main__":
    raise SystemExit(main())
