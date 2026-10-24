"""任务运行表（job_run / job_event）的读写层。

设计要点（为什么这么设计，答辩时能直接讲）：

1. job_run 存“快照”，job_event 存“过程”。
   前端每秒只查 job_run 一行就能画出进度条；日志面板按 seq 增量拉 event，
   不必把整个任务历史反复传。

2. 所有函数都是短事务。
   任务线程每次上报进度只写几十字节、立刻提交，不会长时间占着写锁，
   也就不会把 UI 线程或其它任务卡住（SQLite 单写者的经典坑）。

3. 状态只走固定枚举：
   queued → running → succeeded / failed / canceled
   取消是“协作式”的：用户点取消只置 cancel_requested=1，
   真正的停止由任务自己在阶段边界检查 is_cancel_requested() 决定，
   避免强杀线程造成写坏数据。
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

from store.db_connection import fetch_all, fetch_one, transaction

# ---------------------------------------------------------------- 常量

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"
STATUS_CANCELED = "canceled"

#: 仍在进行中的状态（用于“有任务在跑吗”这类判断）
ACTIVE_STATUSES = (STATUS_QUEUED, STATUS_RUNNING)

LEVEL_INFO = "INFO"
LEVEL_WARN = "WARN"
LEVEL_ERROR = "ERROR"


# ---------------------------------------------------------------- 工具

def _utc_now() -> str:
    """统一时间戳格式（UTC ISO，秒级，可直接字符串排序）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def new_job_id() -> str:
    """生成任务 id：job_ + 12 位十六进制。"""
    return "job_" + uuid.uuid4().hex[:12]


def _dumps(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _loads(value: Any) -> Any:
    if not value:
        return None
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value)
    except (TypeError, ValueError):
        return None


def _clamp_progress(progress: Optional[int]) -> Optional[int]:
    if progress is None:
        return None
    try:
        number = int(progress)
    except (TypeError, ValueError):
        return None
    return max(0, min(100, number))


def _row_to_job(row) -> Dict[str, Any]:
    job = dict(row)
    job["params"] = _loads(job.get("params_json"))
    job["result"] = _loads(job.get("result_json"))
    job["cancel_requested"] = bool(job.get("cancel_requested"))
    return job


def _row_to_event(row) -> Dict[str, Any]:
    event = dict(row)
    event["payload"] = _loads(event.get("payload_json"))
    return event


# ---------------------------------------------------------------- 建任务

def create_job(
    job_type: str,
    params: Any = None,
    operator_name: Optional[str] = None,
    page_identifier: Optional[str] = None,
    source_kind: str = "ui",
    job_id: Optional[str] = None,
    database_path: Optional[str] = None,
) -> str:
    """登记一个待执行任务，返回 job_id。此时 status=queued、progress=0。"""
    identifier = job_id or new_job_id()
    now = _utc_now()
    with transaction(database_path) as connection:
        connection.execute(
            "INSERT INTO job_run ("
            " job_id, job_type, page_identifier, status, progress,"
            " params_json, operator_name, source_kind,"
            " created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                identifier,
                job_type,
                page_identifier,
                STATUS_QUEUED,
                0,
                _dumps(params),
                operator_name,
                source_kind,
                now,
                now,
            ),
        )
    return identifier


# ---------------------------------------------------------------- 状态推进

def _update_fields(
    job_id: str,
    fields: Dict[str, Any],
    database_path: Optional[str] = None,
) -> bool:
    """按字段动态拼 SET，空字典直接返回 False（不发无意义的 SQL）。"""
    if not fields:
        return False
    fields = {key: value for key, value in fields.items() if value is not None or key in fields}
    fields["updated_at"] = _utc_now()
    assignments = ", ".join(f"{name} = ?" for name in fields)
    parameters: List[Any] = list(fields.values())
    parameters.append(job_id)
    with transaction(database_path) as connection:
        cursor = connection.execute(
            f"UPDATE job_run SET {assignments} WHERE job_id = ?", parameters
        )
        return cursor.rowcount > 0


def mark_running(job_id: str, database_path: Optional[str] = None) -> bool:
    """queued → running，记录开始时间。"""
    now = _utc_now()
    with transaction(database_path) as connection:
        cursor = connection.execute(
            "UPDATE job_run SET status = ?, started_at = COALESCE(started_at, ?),"
            " updated_at = ? WHERE job_id = ?",
            (STATUS_RUNNING, now, now, job_id),
        )
        return cursor.rowcount > 0


def update_progress(
    job_id: str,
    progress: Optional[int] = None,
    stage: Optional[str] = None,
    stage_label: Optional[str] = None,
    message: Optional[str] = None,
    database_path: Optional[str] = None,
) -> bool:
    """上报进度。只更新传入的字段，None 表示“这项不变”。"""
    fields: Dict[str, Any] = {}
    clamped = _clamp_progress(progress)
    if clamped is not None:
        fields["progress"] = clamped
    if stage is not None:
        fields["stage"] = stage
    if stage_label is not None:
        fields["stage_label"] = stage_label
    if message is not None:
        fields["message"] = message
    return _update_fields(job_id, fields, database_path)


def mark_succeeded(
    job_id: str,
    result: Any = None,
    message: Optional[str] = None,
    database_path: Optional[str] = None,
) -> bool:
    now = _utc_now()
    return _update_fields(
        job_id,
        {
            "status": STATUS_SUCCEEDED,
            "progress": 100,
            "result_json": _dumps(result) if result is not None else None,
            "message": message,
            "error_message": None,
            "finished_at": now,
        },
        database_path,
    )


def mark_failed(
    job_id: str,
    error_message: str,
    database_path: Optional[str] = None,
) -> bool:
    now = _utc_now()
    return _update_fields(
        job_id,
        {
            "status": STATUS_FAILED,
            "error_message": error_message,
            "message": error_message,
            "finished_at": now,
        },
        database_path,
    )


def mark_canceled(
    job_id: str,
    message: Optional[str] = None,
    database_path: Optional[str] = None,
) -> bool:
    now = _utc_now()
    return _update_fields(
        job_id,
        {
            "status": STATUS_CANCELED,
            "message": message or "任务已取消",
            "finished_at": now,
            "cancel_requested": 1,
        },
        database_path,
    )


def request_cancel(job_id: str, database_path: Optional[str] = None) -> bool:
    """用户点“取消”：只打标记。未开始的任务直接置为已取消。"""
    with transaction(database_path) as connection:
        row = connection.execute(
            "SELECT status FROM job_run WHERE job_id = ?", (job_id,)
        ).fetchone()
        if row is None:
            return False
        current = row["status"]
        now = _utc_now()
        if current == STATUS_QUEUED:
            connection.execute(
                "UPDATE job_run SET status = ?, cancel_requested = 1,"
                " message = ?, finished_at = ?, updated_at = ? WHERE job_id = ?",
                (STATUS_CANCELED, "任务在排队阶段被取消", now, now, job_id),
            )
            return True
        if current == STATUS_RUNNING:
            connection.execute(
                "UPDATE job_run SET cancel_requested = 1, message = ?, updated_at = ?"
                " WHERE job_id = ?",
                ("已请求取消，等待当前阶段结束", now, job_id),
            )
            return True
        # 已结束的任务不允许再取消
        return False


def is_cancel_requested(job_id: str, database_path: Optional[str] = None) -> bool:
    """任务线程在阶段边界调用：True 表示应尽快停手。"""
    row = fetch_one(
        "SELECT cancel_requested FROM job_run WHERE job_id = ?",
        (job_id,),
        database_path,
    )
    return bool(row and row["cancel_requested"])


# ---------------------------------------------------------------- 查询

def fetch_job(job_id: str, database_path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    row = fetch_one("SELECT * FROM job_run WHERE job_id = ?", (job_id,), database_path)
    return _row_to_job(row) if row else None


def list_jobs(
    job_type: Optional[str] = None,
    status: Optional[str] = None,
    limit: int = 20,
    database_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """按创建时间倒序列出任务，可按类型/状态过滤。"""
    clauses: List[str] = []
    parameters: List[Any] = []
    if job_type:
        clauses.append("job_type = ?")
        parameters.append(job_type)
    if status:
        if isinstance(status, (list, tuple, set)):
            placeholders = ", ".join("?" for _ in status)
            clauses.append(f"status IN ({placeholders})")
            parameters.extend(status)
        else:
            clauses.append("status = ?")
            parameters.append(status)
    where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
    parameters.append(max(1, min(200, int(limit))))
    rows = fetch_all(
        f"SELECT * FROM job_run{where} ORDER BY created_at DESC, job_id DESC LIMIT ?",
        tuple(parameters),
        database_path,
    )
    return [_row_to_job(row) for row in rows]


def has_active_job(
    job_type: Optional[str] = None, database_path: Optional[str] = None
) -> bool:
    """有没有排队中/运行中的任务（用于“同一批次不要重复提交”这类校验）。"""
    return bool(
        list_jobs(job_type=job_type, status=list(ACTIVE_STATUSES), limit=1, database_path=database_path)
    )


# ---------------------------------------------------------------- 过程日志

def append_event(
    job_id: str,
    message: str,
    level: str = LEVEL_INFO,
    stage: Optional[str] = None,
    payload: Any = None,
    database_path: Optional[str] = None,
) -> int:
    """追加一条任务日志，返回它的 seq。seq 在单任务内连续递增。"""
    now = _utc_now()
    with transaction(database_path) as connection:
        row = connection.execute(
            "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM job_event WHERE job_id = ?",
            (job_id,),
        ).fetchone()
        seq = int(row["max_seq"]) + 1
        connection.execute(
            "INSERT INTO job_event (job_id, seq, level, stage, message, payload_json, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?)",
            (job_id, seq, level, stage, message, _dumps(payload), now),
        )
    return seq


def list_events(
    job_id: str,
    after_seq: int = 0,
    limit: int = 200,
    database_path: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """增量拉取日志：只要 seq > after_seq 的部分。"""
    rows = fetch_all(
        "SELECT * FROM job_event WHERE job_id = ? AND seq > ?"
        " ORDER BY seq ASC LIMIT ?",
        (job_id, int(after_seq), max(1, min(1000, int(limit)))),
        database_path,
    )
    return [_row_to_event(row) for row in rows]


def list_all_events(
    job_id: str, limit: int = 500, database_path: Optional[str] = None
) -> List[Dict[str, Any]]:
    return list_events(job_id, after_seq=0, limit=limit, database_path=database_path)


# ---------------------------------------------------------------- 清理

def purge_finished_jobs(
    keep_days: int = 7, database_path: Optional[str] = None
) -> int:
    """清理超过保留天数的已结束任务（含其日志），返回删除的任务数。

    keep_days=0 表示“已结束的任务立刻可清”，便于自检与手动清理。
    时间戳是秒级 ISO 字符串，可直接按字符串比较，边界用 <= 以免卡在同一秒。
    """
    from datetime import timedelta

    cutoff = (datetime.now(timezone.utc) - timedelta(days=int(keep_days))).isoformat(
        timespec="seconds"
    )
    finished = (STATUS_SUCCEEDED, STATUS_FAILED, STATUS_CANCELED)
    placeholders = ", ".join("?" for _ in finished)
    with transaction(database_path) as connection:
        rows = connection.execute(
            f"SELECT job_id FROM job_run WHERE status IN ({placeholders})"
            " AND COALESCE(finished_at, updated_at) <= ?",
            (*finished, cutoff),
        ).fetchall()
        job_ids = [row["job_id"] for row in rows]
        if not job_ids:
            return 0
        id_placeholders = ", ".join("?" for _ in job_ids)
        connection.execute(
            f"DELETE FROM job_event WHERE job_id IN ({id_placeholders})", job_ids
        )
        connection.execute(
            f"DELETE FROM job_run WHERE job_id IN ({id_placeholders})", job_ids
        )
        return len(job_ids)
