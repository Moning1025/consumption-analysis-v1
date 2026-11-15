"""S2-2 清洗脚本：把 consumption_record 的地域字段洗干净并写回区划码。

【分层标注 · 按工作区规则 v2】
- 本文件 = 第一层：沫宁鱼写完整版（命令行、落库、账本、打印），老大只跑不改。

【它做四件事】
1. 挑出所有"没洗干净"的记录（region_code 空 / city_region 是未知区域）
2. 逐条归一化（先看地域字段，再看门店名 —— 真正的地名藏在"成都春熙路店"里）
3. 回写 consumption_record.region_code 与 city_region（标准名）
4. 把这一轮的清洗结果写进 ingestion_validation（谁被改、怎么改的、置信度多少）

【为什么要写 ingestion_validation】
答辩会问"你怎么证明数据清洗过"。一张只增不改的账本表就是证据：
第几批、处理几条、命中几条、哪条是推断的、置信度多少，全在里面。
这跟 S2-1a 的 poi_fetch_log 是同一个设计思想——**留执行证据，不是打进度条**。

用法：
    chcp 65001
    # 1) 干跑，只看不写
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\clean_regions.py --dry-run
    # 2) 真写
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\clean_regions.py
    # 3) 验收
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_2.py
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from datasource.region_normalizer import NormalizeResult, resolve_record  # noqa: E402

DEFAULT_DB = PROJECT_ROOT / "local_consumption_insight.db"

# 判定"这条没洗干净"的条件。注意：只看 region_code 不够——
# 有码但 city_region 还写着"未知区域"的也算脏（字段之间不一致）。
DIRTY_WHERE = """
    region_code IS NULL
    OR TRIM(COALESCE(region_code, '')) = ''
    OR city_region IS NULL
    OR TRIM(COALESCE(city_region, '')) = ''
    OR city_region IN ('未知区域', '未知', '无', '其他', '其它')
"""


@dataclass
class CleanStat:
    """一轮清洗的计数器。三个字段必须满足 seen = updated + unmatched。"""

    seen: int = 0          # 进入清洗范围的记录数
    updated: int = 0       # 成功归一化并回写的条数
    unmatched: int = 0     # 认不出地名、原样保留的条数
    warning: int = 0       # 命中但置信度 < 0.95（推断/模糊），需要人工复核的条数


def fetch_dirty_rows(conn: sqlite3.Connection, limit: int | None) -> list[sqlite3.Row]:
    """取出所有待清洗记录。"""
    sql = (
        "SELECT id, batch_identifier, merchant_name, city_region, region_code "
        f"FROM consumption_record WHERE {DIRTY_WHERE} ORDER BY id"
    )
    if limit:
        sql += f" LIMIT {int(limit)}"
    return conn.execute(sql).fetchall()


def apply_result(conn: sqlite3.Connection, row_id: int, result: NormalizeResult) -> None:
    """把归一化结果回写到 consumption_record。

    为什么 city_region 也要改：只写码不改名，前端下拉框里还是"未知区域"，
    等于洗了个寂寞。码和名必须同时变成标准值。
    """
    conn.execute(
        "UPDATE consumption_record SET region_code = ?, city_region = ? WHERE id = ?",
        (result.code, result.name, row_id),
    )


def build_report(stat: CleanStat, details: list[dict]) -> dict:
    """组装写进 ingestion_validation.report_json 的报告。"""
    by_method: dict[str, int] = {}
    for d in details:
        by_method[d["method"]] = by_method.get(d["method"], 0) + 1
    return {
        "task": "region_normalization",
        "scope": "consumption_record",
        "stat": {
            "seen": stat.seen,
            "updated": stat.updated,
            "unmatched": stat.unmatched,
            "needs_review": stat.warning,
            "by_method": by_method,
        },
        "details": details,
    }


def write_validation(conn: sqlite3.Connection, stat: CleanStat, details: list[dict],
                     batch_identifier: str) -> str:
    """把清洗结果写成一行 ingestion_validation 账本，返回 validation_code。"""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    code = f"val_region_{stamp}"
    conn.execute(
        """
        INSERT INTO ingestion_validation
            (validation_code, batch_identifier, valid_rows, error_rows, warning_rows,
             report_json, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            code,
            batch_identifier,
            stat.updated,
            stat.unmatched,
            stat.warning,
            json.dumps(build_report(stat, details), ensure_ascii=False),
            datetime.now(timezone.utc).isoformat(timespec="seconds"),
        ),
    )
    return code


def main() -> int:
    parser = argparse.ArgumentParser(description="S2-2 地域归一化清洗")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="数据库路径")
    parser.add_argument("--dry-run", action="store_true", help="只打印，不写库")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少条（调试用）")
    parser.add_argument("--batch-id", default="region_clean_all",
                        help="写账本时用的批次标识（默认 region_clean_all）")
    args = parser.parse_args()

    if not Path(args.db).exists():
        print(f"[ERROR] 数据库不存在：{args.db}")
        return 2

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    stat = CleanStat()
    details: list[dict] = []

    try:
        rows = fetch_dirty_rows(conn, args.limit)
        print(f"数据库：{args.db}")
        print(f"待清洗：{len(rows)} 条{'（干跑模式，不写库）' if args.dry_run else ''}")
        if not rows:
            print("没有需要清洗的记录——可能是已经洗过了（幂等）或数据本身就是干净的。")
            return 0

        print()
        print(f"{'id':<6}{'门店':<18}{'原city_region':<12}{'→码':<9}{'→标准名':<9}{'方式':<10}{'置信':<6}说明")
        print("-" * 118)

        for row in rows:
            stat.seen += 1
            result = resolve_record(row["city_region"], row["merchant_name"])
            if result.ok:
                stat.updated += 1
                if result.confidence < 0.95:
                    stat.warning += 1
                if not args.dry_run:
                    apply_result(conn, row["id"], result)
            else:
                stat.unmatched += 1

            details.append({
                "id": row["id"],
                "merchant_name": row["merchant_name"],
                "city_region_before": row["city_region"],
                "region_code_before": row["region_code"],
                "region_code_after": result.code,
                "region_name_after": result.name,
                "method": result.method,
                "confidence": result.confidence,
                "note": result.note,
            })

            print(f"{row['id']:<6}{(row['merchant_name'] or '')[:16]:<18}"
                  f"{(row['city_region'] or '')[:10]:<12}"
                  f"{(result.code or '-'):<9}{(result.name or '-'):<9}"
                  f"{result.method:<10}{result.confidence:<6.2f}{result.note}")

        print()
        print("=" * 60)
        print(f"进入范围 : {stat.seen}")
        print(f"已归一化 : {stat.updated}")
        print(f"未命中   : {stat.unmatched}")
        print(f"待人工复核: {stat.warning}（置信度 < 0.95，多为门店名推断或模糊匹配）")
        # 守恒核对：处理数必须等于更新数+未命中数，否则说明有分支把记录吞了
        accounted = stat.updated + stat.unmatched
        print(f"账本核对 : {stat.seen} = {stat.updated} + {stat.unmatched} → "
              f"{'守恒' if accounted == stat.seen else '不守恒（有 Bug，先别写论文）'}")
        print("=" * 60)

        if args.dry_run:
            print("\n[干跑] 未写库。去掉 --dry-run 才会真正回写。")
            return 0 if accounted == stat.seen else 1

        code = write_validation(conn, stat, details, args.batch_id)
        conn.commit()
        print(f"\n已写库。账本编号：{code}（可查 ingestion_validation 表）")
        return 0 if accounted == stat.seen else 1

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
