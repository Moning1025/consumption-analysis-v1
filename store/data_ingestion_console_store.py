import logging
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Sequence

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)


def update_ingestion_record(
    record_id: int,
    fields: Dict[str, Any],
) -> bool:
    if not fields:
        return False
    set_clauses = []
    parameters: List[Any] = []
    for column, value in fields.items():
        set_clauses.append(f"{column} = ?")
        parameters.append(value)
    set_sql = ", ".join(set_clauses)
    parameters.append(record_id)
    with transaction_scope() as connection:
        cursor = connection.execute(
            f"UPDATE data_ingestion_console SET {set_sql} WHERE id = ?",
            parameters,
        )
        return cursor.rowcount > 0


def query_ingestion_records(
    batch_identifiers: Optional[Sequence[str]] = None,
    limit: int = 20000,
) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        if batch_identifiers is None:
            rows = connection.execute(
                "SELECT id, batch_identifier, source_name, source_type, endpoint_uri, region_code, "
                "source_channel, payload_json, status, progress, term_count, operator, created_at "
                "FROM data_ingestion_console "
                "ORDER BY id DESC "
                "LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            placeholders = ",".join("?" for _ in batch_identifiers)
            rows = connection.execute(
                f"SELECT id, batch_identifier, source_name, source_type, endpoint_uri, region_code, "
                f"source_channel, payload_json, status, progress, term_count, operator, created_at "
                f"FROM data_ingestion_console "
                f"WHERE batch_identifier IN ({placeholders}) "
                f"ORDER BY id DESC",
                list(batch_identifiers),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def get_ingestion_record(record_id: int) -> Optional[Dict[str, Any]]:
    connection = get_connection()
    try:
        row = connection.execute(
            "SELECT id, batch_identifier, source_name, source_type, endpoint_uri, region_code, "
            "source_channel, payload_json, status, progress, term_count, operator, created_at, updated_at "
            "FROM data_ingestion_console WHERE id = ?",
            (record_id,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        connection.close()


def delete_ingestion_record(record_id: int) -> bool:
    connection = get_connection()
    try:
        cursor = connection.execute(
            "DELETE FROM data_ingestion_console WHERE id = ?",
            (record_id,),
        )
        connection.commit()
        return cursor.rowcount > 0
    finally:
        connection.close()


def delete_ingestion_records(record_ids: Iterable[int]) -> int:
    ids = list(record_ids)
    if not ids:
        return 0
    connection = get_connection()
    try:
        placeholders = ",".join("?" for _ in ids)
        cursor = connection.execute(
            f"DELETE FROM data_ingestion_console WHERE id IN ({placeholders})",
            ids,
        )
        connection.commit()
        return cursor.rowcount
    finally:
        connection.close()


def fetch_active_ingestion_sources() -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT id, source_name, source_type, endpoint_uri, region_code, "
            "status, operator, created_at "
            "FROM data_ingestion_console "
            "WHERE status = 'active' "
            "ORDER BY id DESC"
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def summarize_ingestion_console() -> dict:
    with get_connection() as connection:
        status_rows = connection.execute(
            "SELECT status, COUNT(*) AS status_count, COALESCE(SUM(term_count), 0) AS term_total "
            "FROM data_ingestion_console "
            "GROUP BY status"
        ).fetchall()
        operator_rows = connection.execute(
            "SELECT operator, COUNT(*) AS operator_count "
            "FROM data_ingestion_console "
            "GROUP BY operator "
            "ORDER BY operator_count DESC "
            "LIMIT 10"
        ).fetchall()
        status_breakdown = {row["status"]: {"count": row["status_count"], "term_total": row["term_total"]} for row in status_rows}
        operator_breakdown = {row["operator"]: row["operator_count"] for row in operator_rows}
        total_records = sum(item["count"] for item in status_breakdown.values())
        total_terms = sum(item["term_total"] for item in status_breakdown.values())
        return {
            "total_records": total_records,
            "total_terms": total_terms,
            "status_breakdown": status_breakdown,
            "operator_breakdown": operator_breakdown,
        }


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class DataIngestionConsoleStore:
    def list_batches(self, *args, **kwargs):
        return None

    def create_batch(self, *args, **kwargs):
        return None
