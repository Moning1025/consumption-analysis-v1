# -*- coding: utf-8 -*-
"""consumption_record_store —— S3 规范层落库与统计。

consumption_record 是清洗融合的结果表：一行 = 一条去重后的消费事件，
带溯源列（source_kind / batch_identifier / raw_row_id）可回溯到 ingestion_row。
唯一索引 uq_consumption_record_raw_row 保证「清洗重复跑也不重复入库」。
"""
import logging
from typing import Any, Dict, List, Optional

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)

_CONSUMPTION_RECORD_COLUMNS = (
    "region_code",
    "merchant_name",
    "user_identifier",
    "category_label",
    "city_region",
    "consumption_amount",
    "occurred_date",
    "occurred_at",
    "source_kind",
    "batch_identifier",
    "raw_row_id",
)


def insert_records(records: List[Dict[str, Any]]) -> int:
    """批量写入规范层，返回**实际新增**的行数。

    用 INSERT OR IGNORE + 唯一索引 raw_row_id 兜底：同一行原文即使被
    清洗多次，也只会留下一条，保证「重复导入/重复清洗不重复入库」。
    """
    if not records:
        return 0
    placeholders = ", ".join(["?"] * len(_CONSUMPTION_RECORD_COLUMNS))
    sql_statement = (
        f"INSERT OR IGNORE INTO consumption_record "
        f"({', '.join(_CONSUMPTION_RECORD_COLUMNS)}) VALUES ({placeholders})"
    )
    parameter_rows = [
        tuple(
            _normalize_value(record.get(column))
            for column in _CONSUMPTION_RECORD_COLUMNS
        )
        for record in records
    ]
    with transaction_scope() as connection:
        cursor = connection.executemany(sql_statement, parameter_rows)
        inserted = cursor.rowcount
    logger.info("consumption_record 写入：提交 %s 条，实际新增 %s 条", len(records), inserted)
    return inserted


def _normalize_value(value: Any) -> Any:
    """把规范记录里的值统一成 SQLite 可存形态。

    金额在清洗层是 Decimal（精度高），落库统一转 float；空字符串归 None。
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    from decimal import Decimal

    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return value


def count_records() -> int:
    connection = get_connection()
    try:
        row = connection.execute("SELECT COUNT(*) AS cnt FROM consumption_record").fetchone()
        return int(row["cnt"]) if row else 0
    finally:
        connection.close()


def count_by_source() -> List[Dict[str, Any]]:
    """按来源统计规范层行数与金额，供清洗页/验收核对「多源融合」结果。"""
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT source_kind, COUNT(*) AS record_count, "
            "COALESCE(SUM(consumption_amount), 0) AS total_amount "
            "FROM consumption_record GROUP BY source_kind ORDER BY record_count DESC"
        ).fetchall()
        return [
            {
                "source_kind": row["source_kind"],
                "record_count": row["record_count"],
                "total_amount": float(row["total_amount"]),
            }
            for row in rows
        ]
    finally:
        connection.close()


def list_records(limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
    """分页读规范层（下游 RFM/预测/空间页用）。"""
    connection = get_connection()
    try:
        rows = connection.execute(
            f"SELECT {', '.join(_CONSUMPTION_RECORD_COLUMNS)} "
            "FROM consumption_record ORDER BY occurred_at DESC, id DESC LIMIT ? OFFSET ?",
            (int(limit), int(offset)),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def clear_records(connection=None) -> int:
    """清空规范层（自测/重算用；生产流程不调用，保留给运维）。"""
    owns_connection = connection is None
    connection = connection or get_connection()
    try:
        cursor = connection.execute("DELETE FROM consumption_record")
        if owns_connection:
            connection.commit()
        return cursor.rowcount
    finally:
        if owns_connection:
            connection.close()
