"""幂等执行建表脚本（schema.sql + schema_v2.sql）并报告结果。

什么时候用：
- 拉取到新代码、加了新表之后，想在不打开界面的情况下先把本地库升级好；
- 数据库文件被删掉、想重建一个空库。

用法：
    python scripts/apply_schema.py
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import settings  # noqa: E402
from store.db_connection import fetch_all, initialize_schema  # noqa: E402


def main() -> int:
    print(f"数据库: {settings.SQLITE_DATABASE_PATH}")
    initialize_schema()

    tables = [
        row["name"]
        for row in fetch_all(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
            " AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
    ]
    print(f"共 {len(tables)} 张表：")
    for name in tables:
        print(f"  - {name}")

    job_tables = [name for name in tables if name.startswith("job_")]
    if {"job_run", "job_event"} <= set(job_tables):
        counts = {
            name: fetch_all(f"SELECT COUNT(*) AS c FROM {name}")[0]["c"]
            for name in ("job_run", "job_event")
        }
        print(f"S1 任务表就绪：{counts}")
        return 0

    print("S1 任务表缺失，请检查 schema_v2.sql 是否存在。")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
