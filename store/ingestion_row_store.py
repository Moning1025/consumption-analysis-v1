"""ingestion_row / ingestion_validation 两张原始数据表的读写（S2）。

为什么单独一个 store：
`data_ingestion_console` 记的是“批次”这一层的状态（进度条、状态、操作人），
而原始行是“数据”这一层，量大、只增不改。两层分开，前端列表页只查批次，
清洗任务才逐行扫原文，互不拖累。
"""

from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from store.db_connection import fetch_all, fetch_one, transaction_scope

INSERT_CHUNK_SIZE = 500


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def compute_row_hash(raw: Dict[str, Any]) -> str:
    """行指纹：对原文做规范序列化后取 sha256，用于“同一文件导两次不重复入库”。"""
    payload = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def fetch_batch_hashes(batch_identifier: str) -> set:
    """取某批次已入库的行指纹集合（幂等判断用）。"""
    rows = fetch_all(
        "SELECT row_hash FROM ingestion_row WHERE batch_identifier = ?",
        (str(batch_identifier),),
    )
    return {row["row_hash"] for row in rows}


def fetch_existing_hashes(limit: int = 500000) -> set:
    """取全库已入库的行指纹集合（跨批次全局判重用）。

    为什么是全局而不是“本批次”：
    每次导入都会新登记一个 batch_identifier，如果只在批次内判重，
    同一个文件导第二次仍然会全部插进去。指纹是行内容算出来的，
    所以正确口径是“这行内容以前入过库就不再入”。
    数据量很大时（>50 万行）再加独立的 hash 索引表，当前规模直接扫 row_hash 列足够快。
    """
    rows = fetch_all(
        "SELECT row_hash FROM ingestion_row LIMIT ?",
        (int(limit),),
    )
    return {row["row_hash"] for row in rows}


def insert_rows(
    batch_identifier: str, source_kind: str, rows: Iterable[Dict[str, Any]]
) -> int:
    """批量写入原始行，返回真正新增的行数。

    每行需要提供：line_no / row_hash / raw_json / parse_status / parse_error。
    `INSERT OR IGNORE` 配合唯一索引 (batch_identifier, row_hash) 兜底，
    即使上游判重漏了也不会插出重复行。
    """
    prepared = []
    now = utc_now()
    for row in rows:
        prepared.append(
            (
                str(batch_identifier),
                str(source_kind or "retail"),
                int(row.get("line_no") or 0),
                str(row["row_hash"]),
                str(row.get("raw_json") or "{}"),
                str(row.get("parse_status") or "ok"),
                row.get("parse_error") or None,
                row.get("created_at") or now,
            )
        )
    if not prepared:
        return 0

    inserted = 0
    with transaction_scope() as connection:
        for start in range(0, len(prepared), INSERT_CHUNK_SIZE):
            chunk = prepared[start : start + INSERT_CHUNK_SIZE]
            before = connection.total_changes
            connection.executemany(
                "INSERT OR IGNORE INTO ingestion_row ("
                "batch_identifier, source_kind, line_no, row_hash, raw_json, "
                "parse_status, parse_error, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                chunk,
            )
            inserted += connection.total_changes - before
    return inserted


def count_rows(batch_identifier: str) -> Dict[str, int]:
    """按状态统计某批次的行数。"""
    rows = fetch_all(
        "SELECT parse_status, COUNT(*) AS amount FROM ingestion_row "
        "WHERE batch_identifier = ? GROUP BY parse_status",
        (str(batch_identifier),),
    )
    counts = {"total": 0, "ok": 0, "error": 0}
    for row in rows:
        status = str(row["parse_status"] or "ok")
        amount = int(row["amount"] or 0)
        counts["total"] += amount
        if status in counts:
            counts[status] += amount
    return counts


def list_rows(
    batch_identifier: str, status: str = "", limit: int = 100, offset: int = 0
) -> List[Dict[str, Any]]:
    """列出某批次里的原始行（前端“看看导进来了什么”用）。"""
    sql = (
        "SELECT row_id, batch_identifier, source_kind, line_no, row_hash, raw_json, "
        "parse_status, parse_error, created_at FROM ingestion_row WHERE batch_identifier = ?"
    )
    parameters: List[Any] = [str(batch_identifier)]
    if status:
        sql += " AND parse_status = ?"
        parameters.append(str(status))
    sql += " ORDER BY row_id LIMIT ? OFFSET ?"
    parameters.extend([int(limit or 100), int(offset or 0)])

    rows = []
    for row in fetch_all(sql, tuple(parameters)):
        item = dict(row)
        try:
            item["raw"] = json.loads(item.pop("raw_json") or "{}")
        except ValueError:
            item["raw"] = {}
        rows.append(item)
    return rows


def save_validation(
    batch_identifier: str,
    valid_rows: int,
    error_rows: int,
    warning_rows: int,
    report: Optional[Dict[str, Any]] = None,
) -> str:
    """写入一份格式校验报告，返回 validation_code。"""
    validation_code = f"val_{batch_identifier}_{int(time.time() * 1000) % 1000000:06d}"
    with transaction_scope() as connection:
        connection.execute(
            "INSERT OR REPLACE INTO ingestion_validation ("
            "validation_code, batch_identifier, valid_rows, error_rows, warning_rows, "
            "report_json, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                validation_code,
                str(batch_identifier),
                int(valid_rows),
                int(error_rows),
                int(warning_rows),
                json.dumps(report or {}, ensure_ascii=False),
                utc_now(),
            ),
        )
    return validation_code


def fetch_validation(batch_identifier: str) -> Optional[Dict[str, Any]]:
    """取某批次最新一份校验报告。"""
    row = fetch_one(
        "SELECT validation_code, batch_identifier, valid_rows, error_rows, warning_rows, "
        "report_json, created_at FROM ingestion_validation "
        "WHERE batch_identifier = ? ORDER BY created_at DESC LIMIT 1",
        (str(batch_identifier),),
    )
    if not row:
        return None
    item = dict(row)
    try:
        item["report"] = json.loads(item.pop("report_json") or "{}")
    except ValueError:
        item["report"] = {}
    return item
