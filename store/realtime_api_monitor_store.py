import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)


def record_api_sample(sample_payload: Dict[str, Any]) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO realtime_api_monitor ("
            "endpoint_uri, source_name, region_code, latency_ms, status_code, "
            "response_sample_json, sampled_at, is_error"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                sample_payload.get("endpoint_uri"),
                sample_payload.get("source_name"),
                sample_payload.get("region_code"),
                sample_payload.get("latency_ms"),
                sample_payload.get("status_code"),
                json.dumps(sample_payload.get("response_sample", {}), ensure_ascii=False),
                sample_payload.get("sampled_at") or datetime.now(timezone.utc).isoformat(timespec="seconds"),
                1 if (sample_payload.get("status_code") or 0) >= 400 else 0,
            ),
        )
        sample_id = cursor.lastrowid
        logger.info("realtime api sample recorded id=%s endpoint=%s", sample_id, sample_payload.get("endpoint_uri"))
        return sample_id


def query_api_samples(
    region_code: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        if region_code is not None:
            rows = connection.execute(
                "SELECT id, endpoint_uri, source_name, region_code, latency_ms, "
                "status_code, response_sample_json, sampled_at, is_error "
                "FROM realtime_api_monitor "
                "WHERE region_code = ? "
                "ORDER BY sampled_at DESC LIMIT ?",
                (region_code, limit),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT id, endpoint_uri, source_name, region_code, latency_ms, "
                "status_code, response_sample_json, sampled_at, is_error "
                "FROM realtime_api_monitor "
                "ORDER BY sampled_at DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def summarize_api_health(region_code: Optional[str] = None) -> Dict[str, Any]:
    connection = get_connection()
    try:
        if region_code is not None:
            row = connection.execute(
                "SELECT COUNT(*) AS sample_count, "
                "AVG(latency_ms) AS avg_latency_ms, "
                "SUM(is_error) AS error_count "
                "FROM realtime_api_monitor WHERE region_code = ?",
                (region_code,),
            ).fetchone()
        else:
            row = connection.execute(
                "SELECT COUNT(*) AS sample_count, "
                "AVG(latency_ms) AS avg_latency_ms, "
                "SUM(is_error) AS error_count "
                "FROM realtime_api_monitor"
            ).fetchone()
        sample_count = row["sample_count"] or 0
        error_count = row["error_count"] or 0
        return {
            "sample_count": sample_count,
            "avg_latency_ms": round(row["avg_latency_ms"] or 0.0, 4),
            "error_count": error_count,
            "error_ratio": round(error_count / sample_count, 4) if sample_count else 0.0,
        }
    finally:
        connection.close()


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class RealtimeApiMonitorStore:
    def list_samples(self, *args, **kwargs):
        return None

    def get_health_summary(self, *args, **kwargs):
        return None
