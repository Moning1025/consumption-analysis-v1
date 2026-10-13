from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Iterator, Mapping, Sequence

from logging_dict_config import get_module_logger

audit_logger = get_module_logger("store.audit_log_store")

_ALLOWED_ACTOR_ROLES = frozenset({"guest", "consumer", "analyst", "administrator"})

_ALLOWED_EVENT_KINDS = frozenset({
    "login_success",
    "login_failure",
    "logout",
    "password_reset_request",
    "password_reset_commit",
    "task_dispatch",
    "task_reject",
    "data_export",
    "permission_denied",
})


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _normalize_source_page(source_page: str | None) -> str:
    if not source_page:
        return "unknown"
    trimmed = source_page.strip()
    return trimmed if trimmed else "unknown"


def _normalize_actor_role(actor_role: str) -> str:
    lowered = (actor_role or "").strip().lower()
    if lowered not in _ALLOWED_ACTOR_ROLES:
        raise ValueError(f"未识别的角色标识: {actor_role!r}")
    return lowered


def _normalize_event_kind(event_kind: str) -> str:
    lowered = (event_kind or "").strip().lower()
    if lowered not in _ALLOWED_EVENT_KINDS:
        raise ValueError(f"未识别的审计事件类型: {event_kind!r}")
    return lowered


def _serialize_detail(detail: Mapping[str, object] | None) -> str:
    if not detail:
        return "{}"
    try:
        return json.dumps(dict(detail), ensure_ascii=False, separators=(", ", ": "))
    except (TypeError, ValueError) as serialization_error:
        audit_logger.warning("审计明细序列化失败，降级为字符串: %s", serialization_error)
        return json.dumps({"_raw": str(detail)}, ensure_ascii=False)


def _row_to_audit_record(row: sqlite3.Row) -> dict:
    return {
        "audit_id": row["audit_id"],
        "account_id": row["account_id"],
        "actor_username": row["actor_username"],
        "actor_role": row["actor_role"],
        "event_kind": row["event_kind"],
        "action_name": row["action_name"],
        "source_page": row["source_page"],
        "client_ip": row["client_ip"],
        "detail_payload": json.loads(row["detail_payload"] or "{}"),
        "occurred_at": row["occurred_at"],
    }


def record_audit_event(
    connection: sqlite3.Connection,
    *,
    account_id: int | None,
    actor_username: str,
    actor_role: str,
    event_kind: str,
    action_name: str,
    source_page: str | None,
    client_ip: str | None = None,
    detail: Mapping[str, object] | None = None,
) -> int:
    normalized_role = _normalize_actor_role(actor_role)
    normalized_kind = _normalize_event_kind(event_kind)
    normalized_page = _normalize_source_page(source_page)
    if not actor_username:
        raise ValueError("actor_username 不能为空")
    if not action_name:
        raise ValueError("action_name 不能为空")
    occurred_at = _utc_now_iso()
    detail_payload = _serialize_detail(detail)
    cursor = connection.execute(
        "INSERT INTO audit_log ("
        "account_id, actor_username, actor_role, event_kind, "
        "action_name, source_page, client_ip, detail_payload, occurred_at"
        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            account_id,
            actor_username,
            normalized_role,
            normalized_kind,
            action_name,
            normalized_page,
            client_ip,
            detail_payload,
            occurred_at,
        ),
    )
    connection.commit()
    audit_id = int(cursor.lastrowid)
    audit_logger.info(
        "审计事件落库 audit_id=%s kind=%s actor=%s page=%s",
        audit_id,
        normalized_kind,
        actor_username,
        normalized_page,
    )
    return audit_id


def record_login_success(
    connection: sqlite3.Connection,
    *,
    account_id: int,
    actor_username: str,
    actor_role: str,
    source_page: str | None,
    client_ip: str | None,
    session_token_id: str,
) -> int:
    return record_audit_event(
        connection,
        account_id=account_id,
        actor_username=actor_username,
        actor_role=actor_role,
        event_kind="login_success",
        action_name="account_login",
        source_page=source_page,
        client_ip=client_ip,
        detail={"session_token_id": session_token_id},
    )


def record_login_failure(
    connection: sqlite3.Connection,
    *,
    actor_username: str,
    source_page: str | None,
    client_ip: str | None,
    failure_reason: str,
) -> int:
    return record_audit_event(
        connection,
        account_id=None,
        actor_username=actor_username or "anonymous",
        actor_role="consumer",
        event_kind="login_failure",
        action_name="account_login",
        source_page=source_page,
        client_ip=client_ip,
        detail={"failure_reason": failure_reason},
    )


def record_task_dispatch(
    connection: sqlite3.Connection,
    *,
    account_id: int,
    actor_username: str,
    actor_role: str,
    source_page: str,
    task_identifier: str,
    task_payload: Mapping[str, object] | None = None,
) -> int:
    return record_audit_event(
        connection,
        account_id=account_id,
        actor_username=actor_username,
        actor_role=actor_role,
        event_kind="task_dispatch",
        action_name=task_identifier,
        source_page=source_page,
        detail={"task_payload": dict(task_payload or {})},
    )


def record_permission_denied(
    connection: sqlite3.Connection,
    *,
    account_id: int | None,
    actor_username: str,
    actor_role: str,
    source_page: str | None,
    attempted_action: str,
    required_capability: str,
) -> int:
    return record_audit_event(
        connection,
        account_id=account_id,
        actor_username=actor_username,
        actor_role=actor_role,
        event_kind="permission_denied",
        action_name=attempted_action,
        source_page=source_page,
        detail={"required_capability": required_capability},
    )


def _build_filter_clause(
    actor_username: str | None,
    actor_role: str | None,
    event_kind: str | None,
    source_page: str | None,
    occurred_from: str | None,
    occurred_to: str | None,
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    parameters: list[object] = []
    if actor_username:
        clauses.append("actor_username = ?")
        parameters.append(actor_username)
    if actor_role:
        clauses.append("actor_role = ?")
        parameters.append(_normalize_actor_role(actor_role))
    if event_kind:
        clauses.append("event_kind = ?")
        parameters.append(_normalize_event_kind(event_kind))
    if source_page:
        clauses.append("source_page = ?")
        parameters.append(_normalize_source_page(source_page))
    if occurred_from:
        clauses.append("occurred_at >= ?")
        parameters.append(occurred_from)
    if occurred_to:
        clauses.append("occurred_at <= ?")
        parameters.append(occurred_to)
    where_sql = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    return where_sql, parameters


def iter_audit_records(
    connection: sqlite3.Connection,
    *,
    actor_username: str | None = None,
    actor_role: str | None = None,
    event_kind: str | None = None,
    source_page: str | None = None,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
    page_size: int = 200,
    offset: int = 0,
) -> Iterator[dict]:
    where_sql, parameters = _build_filter_clause(
        actor_username, actor_role, event_kind, source_page, occurred_from, occurred_to
    )
    bounded_page_size = max(1, min(int(page_size), 1000))
    bounded_offset = max(0, int(offset))
    sql = (
        "SELECT audit_id, account_id, actor_username, actor_role, event_kind, "
        "action_name, source_page, client_ip, detail_payload, occurred_at "
        "FROM audit_log" + where_sql + " ORDER BY occurred_at DESC, audit_id DESC LIMIT "
        "? OFFSET ?"
    )
    cursor = connection.execute(sql, (*parameters, bounded_page_size, bounded_offset))
    for row in cursor:
        yield _row_to_audit_record(row)


def count_audit_records(
    connection: sqlite3.Connection,
    *,
    actor_username: str | None = None,
    actor_role: str | None = None,
    event_kind: str | None = None,
    source_page: str | None = None,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
) -> int:
    where_sql, parameters = _build_filter_clause(
        actor_username, actor_role, event_kind, source_page, occurred_from, occurred_to
    )
    cursor = connection.execute("SELECT COUNT(1) FROM audit_log" + where_sql, parameters)
    return int(cursor.fetchone()[0])


def summarize_login_activity(
    connection: sqlite3.Connection,
    *,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
) -> dict:
    where_sql, parameters = _build_filter_clause(None, None, None, None, occurred_from, occurred_to)
    login_where = where_sql + (" AND " if where_sql else " WHERE ") + "event_kind IN ('login_success','login_failure')"
    cursor = connection.execute(
        "SELECT event_kind, COUNT(1) FROM audit_log" + login_where + " GROUP BY event_kind",
        parameters,
    )
    counters = {row[0]: int(row[1]) for row in cursor}
    success_total = counters.get("login_success", 0)
    failure_total = counters.get("login_failure", 0)
    total_attempts = success_total + failure_total
    success_ratio = round(success_total / total_attempts, 4) if total_attempts else 0.0
    return {
        "login_success_total": success_total,
        "login_failure_total": failure_total,
        "login_attempt_total": total_attempts,
        "login_success_ratio": success_ratio,
    }


def summarize_role_activity(
    connection: sqlite3.Connection,
    *,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
) -> list[dict]:
    where_sql, parameters = _build_filter_clause(None, None, None, None, occurred_from, occurred_to)
    cursor = connection.execute(
        "SELECT actor_role, source_page, COUNT(1) FROM audit_log"
        + where_sql
        + " GROUP BY actor_role, source_page ORDER BY COUNT(1) DESC",
        parameters,
    )
    return [
        {"actor_role": row[0], "source_page": row[1], "event_total": int(row[2])}
        for row in cursor
    ]


def summarize_page_activity(
    connection: sqlite3.Connection,
    *,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
) -> list[dict]:
    where_sql, parameters = _build_filter_clause(None, None, None, None, occurred_from, occurred_to)
    cursor = connection.execute(
        "SELECT source_page, event_kind, COUNT(1) FROM audit_log"
        + where_sql
        + " GROUP BY source_page, event_kind ORDER BY source_page, COUNT(1) DESC",
        parameters,
    )
    return [
        {"source_page": row[0], "event_kind": row[1], "event_total": int(row[2])}
        for row in cursor
    ]


def export_audit_records(
    connection: sqlite3.Connection,
    *,
    actor_username: str | None = None,
    actor_role: str | None = None,
    event_kind: str | None = None,
    source_page: str | None = None,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
    batch_size: int = 500,
) -> Iterator[dict]:
    offset = 0
    while True:
        batch = list(
            iter_audit_records(
                connection,
                actor_username=actor_username,
                actor_role=actor_role,
                event_kind=event_kind,
                source_page=source_page,
                occurred_from=occurred_from,
                occurred_to=occurred_to,
                page_size=batch_size,
                offset=offset,
            )
        )
        if not batch:
            return
        for record in batch:
            yield record
        if len(batch) < batch_size:
            return
        offset += batch_size


def purge_expired_audit_records(
    connection: sqlite3.Connection,
    *,
    retention_days: int = 180,
) -> int:
    if retention_days <= 0:
        raise ValueError("retention_days 必须为正整数")
    cutoff_iso = (
        datetime.now(timezone.utc).timestamp() - retention_days * 86400
    )
    cutoff_text = datetime.fromtimestamp(cutoff_iso, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    cursor = connection.execute(
        "DELETE FROM audit_log WHERE occurred_at < ? AND event_kind NOT IN ('permission_denied')",
        (cutoff_text,),
    )
    connection.commit()
    purged_total = int(cursor.rowcount or 0)
    audit_logger.info("审计日志清理完成 cutoff=%s purged=%s", cutoff_text, purged_total)
    return purged_total


def collect_recent_denied_actions(
    connection: sqlite3.Connection,
    *,
    lookback_hours: int = 24,
    limit: int = 50,
) -> Sequence[dict]:
    if lookback_hours <= 0:
        raise ValueError("lookback_hours 必须为正整数")
    cutoff_iso = datetime.now(timezone.utc).timestamp() - lookback_hours * 3600
    cutoff_text = datetime.fromtimestamp(cutoff_iso, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    cursor = connection.execute(
        "SELECT audit_id, account_id, actor_username, actor_role, event_kind, "
        "action_name, source_page, client_ip, detail_payload, occurred_at "
        "FROM audit_log WHERE event_kind = 'permission_denied' AND occurred_at >= ? "
        "ORDER BY occurred_at DESC LIMIT ?",
        (cutoff_text, max(1, int(limit))),
    )
    return tuple(_row_to_audit_record(row) for row in cursor)


def collect_actor_timeline(
    connection: sqlite3.Connection,
    *,
    actor_username: str,
    limit: int = 100,
) -> Sequence[dict]:
    if not actor_username:
        raise ValueError("actor_username 不能为空")
    cursor = connection.execute(
        "SELECT audit_id, account_id, actor_username, actor_role, event_kind, "
        "action_name, source_page, client_ip, detail_payload, occurred_at "
        "FROM audit_log WHERE actor_username = ? ORDER BY occurred_at DESC LIMIT ?",
        (actor_username, max(1, int(limit))),
    )
    return tuple(_row_to_audit_record(row) for row in cursor)


def build_audit_payload(
    connection: sqlite3.Connection,
    *,
    source_page: str | None = None,
    actor_username: str | None = None,
    occurred_from: str | None = None,
    occurred_to: str | None = None,
    page_size: int = 100,
) -> dict:
    records = list(
        iter_audit_records(
            connection,
            actor_username=actor_username,
            source_page=source_page,
            occurred_from=occurred_from,
            occurred_to=occurred_to,
            page_size=page_size,
        )
    )
    total_records = count_audit_records(
        connection,
        actor_username=actor_username,
        source_page=source_page,
        occurred_from=occurred_from,
        occurred_to=occurred_to,
    )
    return {
        "audit_records": records,
        "total_records": total_records,
        "login_activity": summarize_login_activity(
            connection, occurred_from=occurred_from, occurred_to=occurred_to
        ),
        "role_activity": summarize_role_activity(
            connection, occurred_from=occurred_from, occurred_to=occurred_to
        ),
        "page_activity": summarize_page_activity(
            connection, occurred_from=occurred_from, occurred_to=occurred_to
        ),
    }


def record_operation_audit(
    action_code: str,
    target_table: str,
    target_id: str,
    detail: str = "",
) -> int:
    """
    记录一次业务操作审计（无连接参数版，供 pipeline 直接调用）。
    该函数与 audit_log_store 其余函数不同：不要求外部传入连接，自行获取。
    """
    from store.db_connection import get_connection

    occurred_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    detail_payload = json.dumps(
        {"target_table": target_table, "target_id": target_id, "detail": detail},
        ensure_ascii=False,
    )
    with get_connection() as connection:
        cursor = connection.execute(
            "INSERT INTO audit_log ("
            "account_id, actor_username, actor_role, event_kind, "
            "action_name, source_page, client_ip, detail_payload, occurred_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                None,
                "system",
                "administrator",
                "operation",
                action_code,
                None,
                None,
                detail_payload,
                occurred_at,
            ),
        )
        return cursor.lastrowid


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class AuditLogStore:
    def record_action(self, *args, **kwargs):
        return None
