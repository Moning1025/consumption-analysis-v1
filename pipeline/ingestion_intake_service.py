import json
import logging
import time
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from store.db_connection import get_connection, transaction_scope
from store.data_ingestion_console_store import (
    query_ingestion_records,
    fetch_active_ingestion_sources,
    summarize_ingestion_console,
)

logger = logging.getLogger("pipeline.ingestion_intake_service")


def _normalize_source_type(source_type: str) -> str:
    candidate = (source_type or "api").strip().lower()
    allowed = frozenset({"api", "file", "database", "stream", "manual"})
    return candidate if candidate in allowed else "api"


def register_ingestion_source(payload: Dict[str, Any]) -> Dict[str, Any]:
    """注册/接入一个数据源，写入 data_ingestion_console 表。支持说明书新建记录的全部字段。"""
    source_name = (payload.get("source_name") or "").strip()
    source_type = _normalize_source_type(payload.get("source_type"))
    endpoint_uri = (payload.get("endpoint_uri") or "").strip()
    if not source_name:
        raise ValueError("source_name 不能为空")
    if source_type in ("api", "stream") and not endpoint_uri:
        raise ValueError(f"{source_type} 类型数据源必须提供 endpoint_uri")
    region_code = (payload.get("region_code") or payload.get("area_code") or "unknown").strip()
    operator = (payload.get("operator") or "anonymous").strip()
    status = (payload.get("status") or "active").strip() or "active"
    try:
        progress = int(payload.get("progress") or 0)
    except (TypeError, ValueError):
        progress = 0
    try:
        term_count = int(payload.get("term_count") or 0)
    except (TypeError, ValueError):
        term_count = 0
    payload_text = payload.get("payload_json") or payload.get("payload") or ""
    if payload_text and not isinstance(payload_text, str):
        payload_text = json.dumps(payload_text, ensure_ascii=False)
    meta: Dict[str, Any] = {}
    if payload.get("meta_primary"):
        meta["meta_primary"] = payload["meta_primary"]
    if payload.get("meta_secondary"):
        meta["meta_secondary"] = payload["meta_secondary"]
    if meta:
        try:
            parsed = json.loads(payload_text) if payload_text else {}
            parsed.update(meta)
            payload_text = json.dumps(parsed, ensure_ascii=False)
        except ValueError:
            if payload_text:
                payload_text = json.dumps({**meta, "raw": payload_text}, ensure_ascii=False)
            else:
                payload_text = json.dumps(meta, ensure_ascii=False)
    if not payload_text:
        payload_text = "{}"
    batch_identifier = f"src_{int(time.time() * 1000)}"
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    created_at = (payload.get("import_time") or now_iso).strip() or now_iso
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO data_ingestion_console ("
            "batch_identifier, source_name, source_type, endpoint_uri, region_code, status, "
            "operator, term_count, payload_json, progress, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_identifier,
                source_name,
                source_type,
                endpoint_uri,
                region_code,
                status,
                operator,
                term_count,
                payload_text,
                progress,
                created_at,
                now_iso,
            ),
        )
        source_id = cursor.lastrowid
        logger.info("ingestion source registered id=%s name=%s", source_id, source_name)
        return {
            "source_id": source_id,
            "batch_identifier": batch_identifier,
            "source_name": source_name,
            "source_type": source_type,
            "endpoint_uri": endpoint_uri,
            "region_code": region_code,
            "status": status,
        }


def submit_ingestion_batch(
    source_name: str,
    records: List[Dict[str, Any]],
    operator: str = "anonymous",
    file_ext: str = "",
) -> Dict[str, Any]:
    """提交一批原始消费记录（payload 落库），返回批次标识。file_ext 用于识别并记录文件格式。"""
    batch_identifier = f"ing_{int(time.time() * 1000)}"
    payload: Dict[str, Any] = {"records": records}
    if file_ext:
        payload["file_ext"] = file_ext
        payload["file_name"] = f"{source_name}_{batch_identifier[-6:]}.{file_ext}"
    payload_json = json.dumps(payload, ensure_ascii=False)
    now_iso = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO data_ingestion_console ("
            "batch_identifier, source_name, source_channel, payload_json, status, "
            "operator, term_count, created_at, updated_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                batch_identifier,
                source_name,
                source_name,
                payload_json,
                "pending",
                operator,
                len(records),
                now_iso,
                now_iso,
            ),
        )
        batch_id = cursor.lastrowid
        logger.info("ingestion batch submitted id=%s batch=%s records=%s", batch_id, batch_identifier, len(records))
        return {
            "batch_id": batch_id,
            "batch_identifier": batch_identifier,
            "record_count": len(records),
            "status": "pending",
        }


def list_active_sources() -> List[Dict[str, Any]]:
    return fetch_active_ingestion_sources()


def get_ingestion_overview() -> Dict[str, Any]:
    return summarize_ingestion_console()


class IngestionIntakeService:
    """数据接入服务门面，供任务调度层调用。"""

    def __init__(self, data_ingestion_console_store: Any):
        self._store = data_ingestion_console_store

    def register_source(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return register_ingestion_source(payload)

    def submit_batch(
        self,
        source_name: str,
        records: List[Dict[str, Any]],
        operator: str = "anonymous",
    ) -> Dict[str, Any]:
        return submit_ingestion_batch(source_name, records, operator)

    def list_active_sources(self) -> List[Dict[str, Any]]:
        return list_active_sources()

    def get_overview(self) -> Dict[str, Any]:
        return get_ingestion_overview()
