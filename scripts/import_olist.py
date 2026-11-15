# -*- coding: utf-8 -*-
"""S2-1c · Olist（巴西电商）交易流水导入 —— 第一层脚本（沫宁鱼全写，老大只跑）

它干四件事
-----------
1. 把 9 个 CSV 里真正有用的 5 张表读进内存（products / translation / sellers / customers / orders）；
2. 以「订单项」为粒度把 orders × order_items 拼成消费流水（一条商品 = 一条流水）；
3. 打上 source_kind='olist' + region_profile='olist-br' 标签，**绝不污染四川/成都的本地数据**；
4. 用 (source_kind, external_id) 唯一键做幂等，重复跑不会多出一行。

为什么是「订单项」粒度而不是「订单」粒度
----------------------------------------
一个订单可以有多个商品、多个卖家、多个品类。按订单聚合就把品类和卖家信息糊成一团，
后面做「品类 × 区域」的交叉分析就无从下手。按订单项聚合，112,650 条流水每一行仍然
保留自己的品类和卖家，信息量最大。代价是行数比订单数多 13%，完全值得。

为什么 consumption_amount 只取 price、不含 freight_value
----------------------------------------------------------
price 是商品价格（消费者为「东西」付的钱），freight_value 是运费（物流履约成本）。
把运费算进「消费额」，会让「客单价」被平台物流策略污染——同一件商品换个包邮政策，
消费额就变了。所以本脚本只取 price，运费在源里保留、不参与本项目口径。

字段映射（Olist → 本项目 consumption_record）
--------------------------------------------
| consumption_record | 来源                                                        |
|---------------------|-------------------------------------------------------------|
| external_id         | f"{order_id}:{order_item_id}"（幂等唯一键）                  |
| consumption_amount  | order_items.price                                            |
| occurred_at         | orders.order_purchase_timestamp（'2017-10-02 10:56:33'，原样存）|
| occurred_date       | 上面这个时间戳的前 10 位                                      |
| merchant_name       | seller_city + '·' + seller_id 前 8 位（卖家 = 商户）         |
| category_label      | product_category_name（优先用英文译名，没有则用葡语原名）      |
| city_region         | customer_city + ', ' + customer_state（**巴西城市，不是四川**）|
| user_identifier     | orders.customer_id（平台已匿名，无个人信息）                  |
| source_kind         | 固定 'olist'                                                  |
| region_profile      | 固定 'olist-br'（前端按这个标签隔离，不画进四川地图）           |
| region_code         | 留 NULL —— 巴西城市套不进中国行政区划码，硬套就是伪造数据      |
| raw_row_id          | 留 NULL —— Olist 是 CSV 没有行号主键                          |

用法
----
    # 干跑：只读不写，打印 3 条映射后的流水给你肉眼验
    python scripts\\import_olist.py --dry-run

    # 正式导入（默认全量、跳过 canceled/unavailable）
    python scripts\\import_olist.py

    # 只导前 1000 条试试水
    python scripts\\import_olist.py --limit 1000

    # 验收
    python scripts\\verify_s2_1c.py
"""
from __future__ import annotations

import argparse
import csv
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from store.db_connection import create_connection, initialize_schema  # noqa: E402

DEFAULT_DB = PROJECT_ROOT / "local_consumption_insight.db"
DEFAULT_DATA_DIR = PROJECT_ROOT / "data_raw" / "olist"

SOURCE_KIND = "olist"
REGION_PROFILE = "olist-br"

# 这些状态的订单不构成「已完成的消费」，默认跳过。
# canceled = 已取消；unavailable = 下单时商品已不可售（平台侧拒绝）
EXCLUDED_STATUS = {"canceled", "unavailable"}

# 每攒多少行提交一次。SQLite 单写者，逐行 execute 112,000 次会慢到不可接受，
# 但一口气 executemany 全部又会让单个事务过长、占住写锁，5000 是折中值。
CHUNK_SIZE = 5000

# 数据文件（少一个就报错，早失败早发现，别导完才发现字段全空）
FILE_ORDERS = "olist_orders_dataset.csv"
FILE_ITEMS = "olist_order_items_dataset.csv"
FILE_PRODUCTS = "olist_products_dataset.csv"
FILE_SELLERS = "olist_sellers_dataset.csv"
FILE_CUSTOMERS = "olist_customers_dataset.csv"
FILE_TRANSLATION = "product_category_name_translation.csv"

UPSERT_SQL = """
INSERT INTO consumption_record (
    external_id, region_code, region_profile, consumption_amount,
    occurred_date, occurred_at, source_kind, batch_identifier, raw_row_id,
    merchant_name, user_identifier, category_label, city_region
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (source_kind, external_id) DO NOTHING
"""


# ---------------------------------------------------------------------------
# 一、建表/补列（幂等）
# ---------------------------------------------------------------------------
def ensure_consumption_record_columns(conn: sqlite3.Connection) -> list[str]:
    """给 consumption_record 补上 external_id / region_profile 两列（缺则补）。

    为什么不写进 schema_v6.sql：SQLite 的 `ALTER TABLE ADD COLUMN` 不幂等，
    第二次执行会抛 `duplicate column name`，直接让 initialize_schema() 崩掉。
    所以改成「先 PRAGMA 查有没有，没有才加」，跟 grid_metrics 补 city_name 同一套路。

    返回：本次真正补上的列名列表（空列表 = 之前就有）。
    """
    existing = {row[1] for row in conn.execute("PRAGMA table_info(consumption_record)")}
    added: list[str] = []

    if "external_id" not in existing:
        conn.execute("ALTER TABLE consumption_record ADD COLUMN external_id TEXT")
        added.append("external_id")
    if "region_profile" not in existing:
        conn.execute("ALTER TABLE consumption_record ADD COLUMN region_profile TEXT")
        added.append("region_profile")

    # 幂等基石：同一 source_kind 下 external_id 必须唯一。
    # 存量 5 条这列为 NULL，SQLite 唯一索引允许多个 NULL，不会被误伤。
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_consumption_record_external "
        "ON consumption_record (source_kind, external_id)"
    )
    # 前端按 region_profile 过滤数据源，建个普通索引省一次全表扫
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_consumption_record_profile "
        "ON consumption_record (region_profile)"
    )
    conn.commit()
    return added


# ---------------------------------------------------------------------------
# 二、读 CSV
# ---------------------------------------------------------------------------
def _open_csv(path: Path):
    """统一用 utf-8-sig 打开：Olist 的 CSV 带 BOM，不剥掉第一列名会变成 '\\ufefforder_id'。"""
    return path.open("r", encoding="utf-8-sig", newline="")


def load_category_translation(data_dir: Path) -> dict[str, str]:
    """葡语品类名 → 英语品类名。文件不存在就返回空字典（不是致命错误）。"""
    path = data_dir / FILE_TRANSLATION
    if not path.exists():
        return {}
    with _open_csv(path) as fh:
        return {
            row["product_category_name"]: row["product_category_name_english"]
            for row in csv.DictReader(fh)
            if row.get("product_category_name")
        }


def load_products(data_dir: Path, translation: dict[str, str]) -> dict[str, str]:
    """product_id → 可读品类名。

    610 个商品没有品类（源数据缺失）——给它们统一标 'unknown'，
    而不是留空：留空会让后面 groupby 把 None 单独算一类，报告中很显眼也很虚。
    """
    with _open_csv(data_dir / FILE_PRODUCTS) as fh:
        result = {}
        for row in csv.DictReader(fh):
            pt = (row.get("product_category_name") or "").strip()
            if not pt:
                result[row["product_id"]] = "unknown"
                continue
            en = translation.get(pt, "")
            # 英文译名有下划线（health_beauty），换成空格更好读
            result[row["product_id"]] = (en or pt).replace("_", " ")
    return result


def load_sellers(data_dir: Path) -> dict[str, tuple[str, str]]:
    """seller_id → (城市, 州)。卖家即「商户」，城市取卖家所在地。"""
    with _open_csv(data_dir / FILE_SELLERS) as fh:
        return {
            row["seller_id"]: (
                (row.get("seller_city") or "").strip(),
                (row.get("seller_state") or "").strip(),
            )
            for row in csv.DictReader(fh)
        }


def load_customers(data_dir: Path) -> dict[str, tuple[str, str]]:
    """customer_id → (城市, 州)。注意 customer_id 是「订单级」匿名 id，不是自然人 id。"""
    with _open_csv(data_dir / FILE_CUSTOMERS) as fh:
        return {
            row["customer_id"]: (
                (row.get("customer_city") or "").strip(),
                (row.get("customer_state") or "").strip(),
            )
            for row in csv.DictReader(fh)
        }


def load_orders(data_dir: Path) -> dict[str, dict]:
    """order_id → {customer_id, status, purchase_at}。

    为什么整表进内存：订单项要按 order_id 反查订单的时间和客户，
    99,441 条订单约占 30 MB 内存，换成逐条 SQL 查询会多出十几万次 I/O，得不偿失。
    """
    with _open_csv(data_dir / FILE_ORDERS) as fh:
        return {
            row["order_id"]: {
                "customer_id": row["customer_id"],
                "status": (row.get("order_status") or "").strip(),
                "purchase_at": (row.get("order_purchase_timestamp") or "").strip(),
            }
            for row in csv.DictReader(fh)
        }


def iter_order_items(data_dir: Path):
    """流式产出订单项——112,650 条不进内存，读一条处理一条。"""
    with _open_csv(data_dir / FILE_ITEMS) as fh:
        for row in csv.DictReader(fh):
            yield row


# ---------------------------------------------------------------------------
# 三、拼装一行 consumption_record
# ---------------------------------------------------------------------------
def build_row(
    item: dict,
    orders: dict[str, dict],
    products: dict[str, str],
    sellers: dict[str, tuple[str, str]],
    customers: dict[str, tuple[str, str]],
    batch_identifier: str,
    include_canceled: bool,
) -> tuple[dict | None, str]:
    """把一条订单项拼成 consumption_record 的一行。

    返回 (row, reason)：
      - row 非空、reason == 'ok'          → 正常入库
      - row 为空、reason == 'status'      → 订单状态被排除（canceled / unavailable）
      - row 为空、reason == 'invalid'     → 缺关键字段（孤儿订单项 / 价格非法 / 无下单时间）
    """
    order_id = (item.get("order_id") or "").strip()
    item_id = (item.get("order_item_id") or "").strip()

    order = orders.get(order_id)
    if order is None:
        # 订单项指向了 orders 表里不存在的订单（数据不完整），宁可跳过也不留半截记录
        return None, "invalid"

    if not include_canceled and order["status"] in EXCLUDED_STATUS:
        return None, "status"

    purchase_at = order["purchase_at"]
    if not purchase_at:
        return None, "invalid"

    # 价格必须是能转成数字的正数；空字符串、'0.00'、脏字符都算不合格
    raw_price = (item.get("price") or "").strip()
    try:
        price = float(raw_price)
    except ValueError:
        return None, "invalid"
    if price <= 0:
        return None, "invalid"

    seller_id = (item.get("seller_id") or "").strip()
    seller_city, _seller_state = sellers.get(seller_id, ("", ""))
    merchant_name = (
        f"{seller_city.title()}·{seller_id[:8]}" if seller_city else f"unknown·{seller_id[:8]}"
    )

    product_id = (item.get("product_id") or "").strip()
    category = products.get(product_id, "unknown")

    customer_city, customer_state = customers.get(order["customer_id"], ("", ""))
    city_region = (
        f"{customer_city.title()}, {customer_state}" if customer_city else "unknown"
    )

    row = {
        "external_id": f"{order_id}:{item_id}",
        "region_code": None,          # 巴西地址套不进中国区划码，留空是诚实的做法
        "region_profile": REGION_PROFILE,
        "consumption_amount": round(price, 2),
        "occurred_date": purchase_at[:10],
        "occurred_at": purchase_at,
        "source_kind": SOURCE_KIND,
        "batch_identifier": batch_identifier,
        "raw_row_id": None,
        "merchant_name": merchant_name,
        "user_identifier": order["customer_id"],
        "category_label": category,
        "city_region": city_region,
    }
    return row, "ok"


# ---------------------------------------------------------------------------
# 四、写库
# ---------------------------------------------------------------------------
@dataclass
class ImportStat:
    """一次导入的账本。恒等式：rows_read = rows_written + 三个 skipped 之和。"""
    rows_read: int = 0
    rows_written: int = 0
    rows_skipped_status: int = 0
    rows_skipped_invalid: int = 0
    rows_skipped_dup: int = 0
    distinct_orders: int = 0
    min_date: str = ""
    max_date: str = ""


def flush_chunk(conn: sqlite3.Connection, chunk: list[tuple], stat: ImportStat) -> None:
    """把一个 chunk 写进库，并用 total_changes 差值算出「真写入」与「被幂等挡掉」的行数。"""
    if not chunk:
        return
    before = conn.total_changes
    conn.executemany(UPSERT_SQL, chunk)
    conn.commit()
    inserted = conn.total_changes - before
    stat.rows_written += inserted
    stat.rows_skipped_dup += len(chunk) - inserted


def run_import(
    conn: sqlite3.Connection,
    data_dir: Path,
    batch_identifier: str,
    limit: int = 0,
    include_canceled: bool = False,
    dry_run: bool = False,
) -> ImportStat:
    """主流程：读 → 拼 → 写。dry_run 时只打印前 3 条映射结果，不写库。"""
    stat = ImportStat()
    orders = load_orders(data_dir)
    translation = load_category_translation(data_dir)
    products = load_products(data_dir, translation)
    sellers = load_sellers(data_dir)
    customers = load_customers(data_dir)
    print(
        f"载入字典：订单 {len(orders)} / 商品 {len(products)} / 卖家 {len(sellers)} "
        f"/ 客户 {len(customers)} / 品类译名 {len(translation)}"
    )

    chunk: list[tuple] = []
    seen_orders: set[str] = set()
    printed = 0
    started = time.perf_counter()

    for item in iter_order_items(data_dir):
        stat.rows_read += 1
        row, reason = build_row(
            item, orders, products, sellers, customers, batch_identifier, include_canceled
        )

        if row is None:
            if reason == "status":
                stat.rows_skipped_status += 1
            else:
                stat.rows_skipped_invalid += 1
            continue

        seen_orders.add(item["order_id"])
        date = row["occurred_date"]
        if not stat.min_date or date < stat.min_date:
            stat.min_date = date
        if not stat.max_date or date > stat.max_date:
            stat.max_date = date

        if dry_run:
            if printed < 3:
                print(f"  [干跑样本] {row}")
                printed += 1
            stat.rows_written += 1
        else:
            chunk.append(
                (
                    row["external_id"], row["region_code"], row["region_profile"],
                    row["consumption_amount"], row["occurred_date"], row["occurred_at"],
                    row["source_kind"], row["batch_identifier"], row["raw_row_id"],
                    row["merchant_name"], row["user_identifier"],
                    row["category_label"], row["city_region"],
                )
            )
            if len(chunk) >= CHUNK_SIZE:
                flush_chunk(conn, chunk, stat)
                chunk.clear()
                print(f"  ...已处理 {stat.rows_read} 条，写入 {stat.rows_written} 条")

        # --limit 限的是「读多少条」，不是「写多少条」，这样干跑和真跑口径一致
        if limit and stat.rows_read >= limit:
            break

    flush_chunk(conn, chunk, stat)

    stat.distinct_orders = len(seen_orders)
    stat.elapsed_ms = int((time.perf_counter() - started) * 1000)
    stat.min_date = stat.min_date or ""
    stat.max_date = stat.max_date or ""
    return stat


def write_log(conn: sqlite3.Connection, data_dir: Path, batch_identifier: str, stat: ImportStat) -> None:
    """写一条导入账本；同批次重跑用 REPLACE 覆盖，账本不会越滚越多。"""
    conn.execute(
        """
        INSERT INTO olist_import_log (
            batch_identifier, data_dir, granularity, rows_read, rows_written,
            rows_skipped_status, rows_skipped_invalid, rows_skipped_dup,
            distinct_orders, min_date, max_date, elapsed_ms, message, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (batch_identifier) DO UPDATE SET
            rows_read=excluded.rows_read,
            rows_written=excluded.rows_written,
            rows_skipped_status=excluded.rows_skipped_status,
            rows_skipped_invalid=excluded.rows_skipped_invalid,
            rows_skipped_dup=excluded.rows_skipped_dup,
            distinct_orders=excluded.distinct_orders,
            min_date=excluded.min_date,
            max_date=excluded.max_date,
            elapsed_ms=excluded.elapsed_ms,
            message=excluded.message,
            created_at=excluded.created_at
        """,
        (
            batch_identifier,
            str(data_dir.relative_to(PROJECT_ROOT)) if data_dir.is_relative_to(PROJECT_ROOT) else str(data_dir),
            "item",
            stat.rows_read, stat.rows_written,
            stat.rows_skipped_status, stat.rows_skipped_invalid, stat.rows_skipped_dup,
            stat.distinct_orders, stat.min_date, stat.max_date, stat.elapsed_ms,
            "dry-run 未写库" if stat.rows_skipped_dup == -1 else "ok",
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()


def print_stat(stat: ImportStat, dry_run: bool) -> None:
    """打印账本并当场核对守恒——数字对不上就说明有分支漏记账。"""
    total_skipped = stat.rows_skipped_status + stat.rows_skipped_invalid + stat.rows_skipped_dup
    accounted = stat.rows_written + total_skipped
    balanced = stat.rows_read == accounted
    print("\n" + "=" * 62)
    print(f"读到订单项        : {stat.rows_read}")
    print(f"  状态跳过        : {stat.rows_skipped_status}  (canceled / unavailable)")
    print(f"  字段不合格跳过  : {stat.rows_skipped_invalid}")
    print(f"  唯一键已存在跳过: {stat.rows_skipped_dup}  (幂等命中，不是错误)")
    print(f"写入/干跑计数     : {stat.rows_written}")
    print(f"覆盖订单数        : {stat.distinct_orders}")
    print(f"日期区间          : {stat.min_date} ~ {stat.max_date}")
    print(f"耗时              : {stat.elapsed_ms} ms")
    print(f"账本核对          : {stat.rows_read} = {stat.rows_written} + {total_skipped} → "
          f"{'守恒' if balanced else '不守恒！'}")
    if dry_run:
        print("[干跑] 未写库；去掉 --dry-run 即正式导入。")
    print("=" * 62)


# ---------------------------------------------------------------------------
# 五、命令行
# ---------------------------------------------------------------------------
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="S2-1c · Olist 交易流水导入")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="SQLite 库路径")
    parser.add_argument("--data-dir", default=str(DEFAULT_DATA_DIR), help="Olist CSV 所在目录")
    parser.add_argument("--batch", default="", help="批次号，缺省 olist_YYYYMMDD")
    parser.add_argument("--limit", type=int, default=0, help="最多读多少条订单项（0=全量）")
    parser.add_argument("--include-canceled", action="store_true", help="把 canceled/unavailable 订单也算进来")
    parser.add_argument("--dry-run", action="store_true", help="只读不写，打印 3 条映射样本")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_dir = Path(args.data_dir)
    if not data_dir.exists():
        print(f"[错误] 数据目录不存在：{data_dir}")
        return 2

    required = [FILE_ORDERS, FILE_ITEMS, FILE_PRODUCTS, FILE_SELLERS, FILE_CUSTOMERS]
    missing = [name for name in required if not (data_dir / name).exists()]
    if missing:
        print(f"[错误] 缺少必需文件：{', '.join(missing)}")
        return 2

    conn = create_connection(Path(args.db))
    try:
        initialize_schema(database_path=Path(args.db))
        added = ensure_consumption_record_columns(conn)
        if added:
            print(f"已为 consumption_record 补列：{', '.join(added)}")
        else:
            print("consumption_record 的 external_id / region_profile 列已存在")

        batch_identifier = args.batch or f"olist_{datetime.now():%Y%m%d}"
        print(f"批次号：{batch_identifier}")
        stat = run_import(
            conn, data_dir, batch_identifier,
            limit=args.limit,
            include_canceled=args.include_canceled,
            dry_run=args.dry_run,
        )
        if not args.dry_run:
            write_log(conn, data_dir, batch_identifier, stat)
        print_stat(stat, args.dry_run)
        return 0 if stat.rows_read == stat.rows_written + stat.rows_skipped_status + stat.rows_skipped_invalid + stat.rows_skipped_dup else 1
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
