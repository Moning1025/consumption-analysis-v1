import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional, Sequence

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)

_CLEANING_FUSION_TASK_COLUMNS = (
    "pipeline_task_identifier",
    "task_name",
    "task_kind",
    "task_status",
    "source_scope",
    "progress_percent",
    "current_stage",
    "processed_record_count",
    "started_at",
    "finished_at",
    "execution_summary",
    "created_at",
)


def _row_to_task(row: sqlite3.Row) -> Dict[str, Any]:
    execution_summary = row["execution_summary"]
    decoded = {}
    if execution_summary:
        try:
            decoded = json.loads(execution_summary)
        except (TypeError, ValueError):
            decoded = {}
    return {
        "pipeline_task_identifier": row["pipeline_task_identifier"],
        "task_name": row["task_name"],
        "task_kind": row["task_kind"],
        "task_status": row["task_status"],
        "source_scope": row["source_scope"],
        "progress_percent": row["progress_percent"],
        "current_stage": row["current_stage"],
        "processed_record_count": row["processed_record_count"],
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "execution_summary": decoded,
        "created_at": row["created_at"],
    }


def insert_cleaning_fusion_task(
    task_name: str,
    task_kind: str,
    source_scope: str = "all_sources",
    task_status: str = "pending",
    progress_percent: Optional[int] = None,
    meta_json: Optional[Dict[str, Any]] = None,
) -> str:
    import time
    import json as _json
    pipeline_task_identifier = f"cf_{int(time.time() * 1000)}"
    execution_summary = _json.dumps(meta_json, ensure_ascii=False) if meta_json else None
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO cleaning_fusion_pipeline_task ("
            "pipeline_task_identifier, task_name, task_kind, task_status, source_scope, "
            "progress_percent, current_stage, processed_record_count, execution_summary, created_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))",
            (
                pipeline_task_identifier,
                task_name,
                task_kind,
                task_status,
                source_scope,
                int(progress_percent or 0),
                None,
                0,
                execution_summary,
            ),
        )
        logger.info("cleaning fusion task inserted id=%s", pipeline_task_identifier)
        return pipeline_task_identifier


def delete_cleaning_fusion_task(pipeline_task_identifier: str) -> bool:
    connection = get_connection()
    try:
        cursor = connection.execute(
            "DELETE FROM cleaning_fusion_pipeline_task WHERE pipeline_task_identifier = ?",
            (pipeline_task_identifier,),
        )
        connection.commit()
        deleted = cursor.rowcount > 0
        if deleted:
            logger.info("cleaning fusion task deleted id=%s", pipeline_task_identifier)
        return deleted
    finally:
        connection.close()


def fetch_cleaning_fusion_task(pipeline_task_identifier: str) -> Optional[Dict[str, Any]]:
    connection = get_connection()
    try:
        row = connection.execute(
            "SELECT pipeline_task_identifier, task_name, task_kind, task_status, "
            "source_scope, progress_percent, current_stage, processed_record_count, "
            "started_at, finished_at, execution_summary, created_at "
            "FROM cleaning_fusion_pipeline_task "
            "WHERE pipeline_task_identifier = ?",
            (pipeline_task_identifier,),
        ).fetchone()
        if row is None:
            return None
        return _row_to_task(row)
    finally:
        connection.close()


def list_cleaning_fusion_tasks(
    task_status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        if task_status is not None:
            rows = connection.execute(
                "SELECT pipeline_task_identifier, task_name, task_kind, task_status, "
                "source_scope, progress_percent, current_stage, processed_record_count, "
                "started_at, finished_at, execution_summary, created_at "
                "FROM cleaning_fusion_pipeline_task "
                "WHERE task_status = ? "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (task_status, limit, offset),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT pipeline_task_identifier, task_name, task_kind, task_status, "
                "source_scope, progress_percent, current_stage, processed_record_count, "
                "started_at, finished_at, execution_summary, created_at "
                "FROM cleaning_fusion_pipeline_task "
                "ORDER BY created_at DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [_row_to_task(row) for row in rows]
    finally:
        connection.close()


def update_task_status_and_progress(
    pipeline_task_identifier: str,
    task_status: str,
    *,
    started_at: Optional[str] = None,
    finished_at: Optional[str] = None,
    execution_summary: Optional[str] = None,
    progress_percent: Optional[int] = None,
    processed_record_count: Optional[int] = None,
    current_stage: Optional[str] = None,
) -> bool:
    set_clauses = ["task_status = ?"]
    parameters: List[Any] = [task_status]
    if started_at is not None:
        set_clauses.append("started_at = ?")
        parameters.append(started_at)
    if finished_at is not None:
        set_clauses.append("finished_at = ?")
        parameters.append(finished_at)
    if execution_summary is not None:
        set_clauses.append("execution_summary = ?")
        parameters.append(execution_summary)
    if progress_percent is not None:
        set_clauses.append("progress_percent = ?")
        parameters.append(progress_percent)
    if processed_record_count is not None:
        set_clauses.append("processed_record_count = ?")
        parameters.append(processed_record_count)
    if current_stage is not None:
        set_clauses.append("current_stage = ?")
        parameters.append(current_stage)
    parameters.append(pipeline_task_identifier)
    with transaction_scope() as connection:
        cursor = connection.execute(
            f"UPDATE cleaning_fusion_pipeline_task SET {', '.join(set_clauses)} "
            "WHERE pipeline_task_identifier = ?",
            parameters,
        )
        return cursor.rowcount > 0


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class CleaningFusionPipelineStore:
    def list_runs(self, *args, **kwargs):
        return None

    def get_run_detail(self, *args, **kwargs):
        return None
