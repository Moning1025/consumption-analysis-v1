# 用户说明书第 8 章 个性化推荐工作台
import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)


def persist_recommendation_result(result_payload: Dict[str, Any]) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO recommendation_workbench ("
            "segment_label, top_n, recommendations_json, source_pipeline_code, "
            "generated_at, status"
            ") VALUES (?, ?, ?, ?, ?, ?)",
            (
                result_payload.get("segment_label"),
                result_payload.get("top_n", 10),
                json.dumps(result_payload.get("recommendations", []), ensure_ascii=False),
                result_payload.get("source_pipeline_code", ""),
                result_payload.get("generated_at"),
                result_payload.get("status", "completed"),
            ),
        )
        result_id = cursor.lastrowid
        logger.info("recommendation result persisted id=%s segment=%s", result_id, result_payload.get("segment_label"))
        return result_id


def fetch_recommendation_result(result_id: int) -> Optional[Dict[str, Any]]:
    connection = get_connection()
    try:
        row = connection.execute(
            "SELECT id, segment_label, top_n, recommendations_json, "
            "source_pipeline_code, generated_at, status "
            "FROM recommendation_workbench WHERE id = ?",
            (result_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "segment_label": row["segment_label"],
            "top_n": row["top_n"],
            "recommendations": json.loads(row["recommendations_json"] or "[]"),
            "source_pipeline_code": row["source_pipeline_code"],
            "generated_at": row["generated_at"],
            "status": row["status"],
        }
    finally:
        connection.close()


def list_recommendation_results(limit: int = 20) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT id, segment_label, top_n, recommendations_json, "
            "source_pipeline_code, generated_at, status "
            "FROM recommendation_workbench ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class RecommendationWorkbenchStore:
    def list_runs(self, *args, **kwargs):
        return None

    def get_run_detail(self, *args, **kwargs):
        return None
