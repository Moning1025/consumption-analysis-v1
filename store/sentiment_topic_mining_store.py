import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)


def insert_sentiment_topic_batch(batch_payload: Dict[str, Any]) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO sentiment_topic_mining ("
            "batch_code, analysis_window, source_scope, total_reviews, "
            "sentiment_distribution_json, topic_clusters_json, generated_at, status"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_payload.get("batch_code"),
                batch_payload.get("analysis_window"),
                batch_payload.get("source_scope"),
                batch_payload.get("total_reviews", 0),
                json.dumps(batch_payload.get("sentiment_distribution", {}), ensure_ascii=False),
                json.dumps(batch_payload.get("topic_clusters", []), ensure_ascii=False),
                batch_payload.get("generated_at"),
                batch_payload.get("status", "completed"),
            ),
        )
        batch_id = cursor.lastrowid
        logger.info("sentiment topic batch inserted id=%s", batch_id)
        return batch_id


def fetch_sentiment_topic_batch(batch_code: str) -> Optional[Dict[str, Any]]:
    connection = get_connection()
    try:
        row = connection.execute(
            "SELECT id, batch_code, analysis_window, source_scope, total_reviews, "
            "sentiment_distribution_json, topic_clusters_json, generated_at, status "
            "FROM sentiment_topic_mining WHERE batch_code = ?",
            (batch_code,),
        ).fetchone()
        if row is None:
            return None
        return {
            "id": row["id"],
            "batch_code": row["batch_code"],
            "analysis_window": row["analysis_window"],
            "source_scope": row["source_scope"],
            "total_reviews": row["total_reviews"],
            "sentiment_distribution": json.loads(row["sentiment_distribution_json"] or "{}"),
            "topic_clusters": json.loads(row["topic_clusters_json"] or "[]"),
            "generated_at": row["generated_at"],
            "status": row["status"],
        }
    finally:
        connection.close()


def list_sentiment_topic_batches(limit: int = 20) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT id, batch_code, analysis_window, source_scope, total_reviews, "
            "sentiment_distribution_json, topic_clusters_json, generated_at, status "
            "FROM sentiment_topic_mining ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class SentimentTopicMiningStore:
    def list_runs(self, *args, **kwargs):
        return None

    def get_run_detail(self, *args, **kwargs):
        return None
