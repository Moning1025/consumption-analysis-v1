"""S2-1a 采集入口：按关键词抓高德 POI，落进 poi_merchant 表（第一层代码）。

分层归属：第一层——沫宁鱼写完整、你不用改，只管跑。
真正“怎么请求高德”的逻辑在 `datasource/amap_adapter.py`（第二层，你要改一处）。

它做三件事
--------------------------------------------------------------------
1. 开批次：在 `poi_fetch_batch` 里写一行，记下这次要采什么、几页；
2. 抓 + 落库：逐页请求，按 `poi_id` 幂等写入（重复采同一条 POI 只更新不新增），
   每页往 `poi_fetch_log` 写一行账（要回几条 / 新增几条 / 覆盖几条 / 耗时）；
3. 收尾：把批次状态改成 succeeded 或 failed，并打印守恒核对。

用法（PowerShell，项目根目录）
--------------------------------------------------------------------
    # 先确认 Key 生效（setx 之后必须重开终端）
    echo $env:AMAP_WEB_KEY

    # 试跑：成都·餐饮，2 页（50 条左右），最省额度
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\fetch_amap_poi.py --city 成都 --keywords 餐饮 --pages 2

    # 正式跑：多关键词、20 页
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\fetch_amap_poi.py --city 成都 --keywords 餐饮,购物,休闲娱乐 --pages 20

    # 只看请求、不写库（验证 Key 和网络，不占数据库）
    D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\fetch_amap_poi.py --pages 1 --dry-run
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import settings  # noqa: E402
from datasource.amap_adapter import AmapClient, AmapError  # noqa: E402
from store.db_connection import (  # noqa: E402
    execute_many_write,
    execute_write,
    fetch_one,
    initialize_schema,
)

UPSERT_SQL = """
INSERT INTO poi_merchant (
    poi_id, name, category_l1, category_l2, amap_type, typecode, address,
    province, city, district, adcode, longitude, latitude, location_text,
    tel, keyword, source, raw_json, fetched_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'amap', ?, ?, ?)
ON CONFLICT(poi_id) DO UPDATE SET
    name          = excluded.name,
    category_l1   = excluded.category_l1,
    category_l2   = excluded.category_l2,
    amap_type     = excluded.amap_type,
    typecode      = excluded.typecode,
    address       = excluded.address,
    province      = excluded.province,
    city          = excluded.city,
    district      = excluded.district,
    adcode        = excluded.adcode,
    longitude     = excluded.longitude,
    latitude      = excluded.latitude,
    location_text = excluded.location_text,
    tel           = excluded.tel,
    keyword       = excluded.keyword,
    raw_json      = excluded.raw_json,
    updated_at    = excluded.updated_at
"""


def build_batch_id() -> str:
    """批次号：amap_20261112_143015_8f3a（时间 + 随机后缀，便于排序和肉眼识别）。"""
    import uuid

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"amap_{stamp}_{uuid.uuid4().hex[:4]}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="按关键词采集高德 POI 并落库（S2-1a）")
    parser.add_argument("--city", default="成都", help="城市名，如 成都 / 绵阳（默认 成都）")
    parser.add_argument(
        "--keywords",
        default="餐饮",
        help="关键词，多个用英文逗号分隔，如 餐饮,购物,休闲娱乐（默认 餐饮）",
    )
    parser.add_argument("--types", default="", help="高德 POI 类型码过滤（可空），如 050000")
    parser.add_argument("--pages", type=int, default=2, help="每个关键词翻几页（默认 2）")
    parser.add_argument("--offset", type=int, default=25, help="每页条数，最大 25（默认 25）")
    parser.add_argument(
        "--min-interval", type=float, default=0.35, help="请求间隔秒数（默认 0.35）"
    )
    parser.add_argument("--db", default=None, help="自定义数据库路径（默认用 settings 里的）")
    parser.add_argument("--dry-run", action="store_true", help="只请求不写库")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    keywords = [item.strip() for item in args.keywords.split(",") if item.strip()]
    if not keywords:
        print("[错误] --keywords 不能为空")
        return 2

    database_path = Path(args.db) if args.db else None
    print(f"数据库: {database_path or settings.SQLITE_DATABASE_PATH}")
    print(f"目标  : 城市={args.city} 关键词={keywords} 页数={args.pages} 每页={args.offset}")

    if not args.dry_run:
        initialize_schema(database_path=str(database_path) if database_path else None)
    else:
        print("[dry-run] 不建表、不写库")

    try:
        client = AmapClient(min_interval=args.min_interval)
    except AmapError as exc:            # 没有 Key
        print(f"[错误] {exc}")
        return 2

    batch_id = build_batch_id()
    started = time.perf_counter()

    if not args.dry_run:
        execute_write(
            "INSERT INTO poi_fetch_batch (batch_id, city, keywords, types, pages_planned,"
            " status, created_at) VALUES (?, ?, ?, ?, ?, 'running', ?)",
            (
                batch_id,
                args.city,
                ",".join(keywords),
                args.types,
                args.pages * len(keywords),
                datetime.now().isoformat(timespec="seconds"),
            ),
            database_path=str(database_path) if database_path else None,
        )

    total_returned = total_inserted = total_updated = total_skipped = 0
    pages_done = 0
    failure: str | None = None

    try:
        for keyword in keywords:
            for page in range(1, args.pages + 1):
                page_started = time.perf_counter()
                try:
                    pois = client.search_text(
                        keywords=keyword,
                        city=args.city,
                        types=args.types or None,
                        page=page,
                        offset=args.offset,
                    )
                except AmapError as exc:
                    # 配额/Key 类错误：整批停掉，不要继续烧额度
                    failure = str(exc)
                    print(f"\n[中断] {exc}")
                    break
                except RuntimeError as exc:
                    failure = str(exc)
                    print(f"\n[中断] 连续重试失败：{exc}")
                    break

                # 过滤掉没有 poi_id 或没有经纬度的脏数据，并计进 rows_skipped
                clean = [p for p in pois if p["poi_id"] and p["longitude"] is not None]
                skipped = len(pois) - len(clean)

                inserted = updated = 0
                if not args.dry_run and clean:
                    now = datetime.now().isoformat(timespec="seconds")
                    existing_ids = {
                        row["poi_id"]
                        for row in [
                            fetch_one(
                                "SELECT poi_id FROM poi_merchant WHERE poi_id = ?",
                                (p["poi_id"],),
                                database_path=str(database_path) if database_path else None,
                            )
                            for p in clean
                        ]
                        if row
                    }
                    inserted = sum(1 for p in clean if p["poi_id"] not in existing_ids)
                    updated = len(clean) - inserted
                    execute_many_write(
                        UPSERT_SQL,
                        [
                            (
                                p["poi_id"], p["name"], p["category_l1"], p["category_l2"],
                                p["amap_type"], p["typecode"], p["address"], p["province"],
                                p["city"], p["district"], p["adcode"], p["longitude"],
                                p["latitude"], p["location_text"], p["tel"], p["keyword"],
                                p["raw_json"], now, now,
                            )
                            for p in clean
                        ],
                        database_path=str(database_path) if database_path else None,
                    )

                elapsed_ms = int((time.perf_counter() - page_started) * 1000)
                pages_done += 1
                total_returned += len(pois)
                total_inserted += inserted
                total_updated += updated
                total_skipped += skipped

                if not args.dry_run:
                    execute_write(
                        "INSERT INTO poi_fetch_log (batch_id, keyword, page_no, http_status,"
                        " amap_status, rows_returned, rows_inserted, rows_updated, rows_skipped,"
                        " elapsed_ms, message, created_at)"
                        " VALUES (?, ?, ?, 200, '1', ?, ?, ?, ?, ?, ?, ?)",
                        (
                            batch_id, keyword, page, len(pois), inserted, updated,
                            skipped, elapsed_ms,
                            None if clean else "本页无有效数据",
                            datetime.now().isoformat(timespec="seconds"),
                        ),
                        database_path=str(database_path) if database_path else None,
                    )

                print(
                    f"  第 {page:>2}/{args.pages} 页 返回 {len(pois):>3} 条 "
                    f"新增 {inserted:>3} 覆盖 {updated:>3} 跳过 {skipped:>2} {elapsed_ms:>5} ms"
                )

                if not pois:
                    break            # 该关键词没有更多数据了
                if skipped == len(pois) and pois:
                    break            # 整页都是脏数据，继续翻也没意义

            if failure:
                break

    finally:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        status = "failed" if failure else "succeeded"
        if not args.dry_run:
            execute_write(
                "UPDATE poi_fetch_batch SET pages_done = ?, rows_returned = ?,"
                " rows_inserted = ?, rows_updated = ?, status = ?, message = ?,"
                " elapsed_ms = ?, finished_at = ? WHERE batch_id = ?",
                (
                    pages_done,
                    total_returned, total_inserted, total_updated, status,
                    failure, elapsed_ms,
                    datetime.now().isoformat(timespec="seconds"), batch_id,
                ),
                database_path=str(database_path) if database_path else None,
            )

        print("\n" + "=" * 64)
        print(f"批次      : {batch_id}")
        print(f"状态      : {status}")
        print(f"请求次数  : {client.request_count}（其中重试 {client.retry_count} 次）")
        print(f"返回合计  : {total_returned} 条")
        print(f"新增      : {total_inserted} 条")
        print(f"覆盖更新  : {total_updated} 条")
        print(f"跳过(脏)  : {total_skipped} 条")
        print(f"耗时      : {elapsed_ms} ms")
        if args.dry_run:
            print("守恒核对  : dry-run 不写库，跳过守恒核对")
        else:
            balanced = total_returned == total_inserted + total_updated + total_skipped
            print(
                f"守恒核对  : 返回 {total_returned} = 新增 {total_inserted}"
                f" + 覆盖 {total_updated} + 跳过 {total_skipped}"
                f" → {'✅ 守恒' if balanced else '❌ 不守恒（有数据没被记账，检查落库分支）'}"
            )
        print("=" * 64)

        if args.dry_run:
            print("[dry-run] 未写库，数据未落盘")
        else:
            row = fetch_one(
                "SELECT COUNT(*) AS c FROM poi_merchant",
                database_path=str(database_path) if database_path else None,
            )
            print(f"poi_merchant 现有 {row['c']} 行 → 下一步跑："
                  f"D:\\Python\\venv_p3\\Scripts\\python.exe scripts\\verify_s2_1a.py")

    return 2 if failure else 0


if __name__ == "__main__":
    raise SystemExit(main())
