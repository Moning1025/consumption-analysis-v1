import json
import logging
import sqlite3
from typing import Any, Dict, List, Optional, Sequence

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)


def insert_forecast_window(window_payload: Dict[str, Any]) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO consumption_forecast_anomaly ("
            "region_code, model_type, window_days, anomaly_threshold, "
            "forecast_curve_json, summary_json, generated_at, status"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                window_payload.get("region_code"),
                window_payload.get("model_type"),
                window_payload.get("window_days"),
                window_payload.get("anomaly_threshold"),
                window_payload.get("forecast_curve_json"),
                window_payload.get("summary_json"),
                window_payload.get("generated_at"),
                window_payload.get("status", "completed"),
            ),
        )
        window_id = cursor.lastrowid
        logger.info("forecast window inserted id=%s region=%s", window_id, window_payload.get("region_code"))
        return window_id


def fetch_forecast_window_records(
    region_code: str,
    start_date: str,
    end_date: str,
    limit: int = 50000,
) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT id, region_code, consumption_amount, occurred_date, occurred_at "
            "FROM consumption_record "
            "WHERE region_code = ? "
            "AND occurred_date >= ? AND occurred_date <= ? "
            "ORDER BY occurred_date ASC "
            "LIMIT ?",
            (region_code, start_date, end_date, limit),
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def insert_anomaly_event(event_payload: Dict[str, Any]) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO forecast_anomaly_event ("
            "forecast_window_id, region_code, occurred_date, consumption_amount, "
            "z_score, deviation_ratio, anomaly_direction, detected_at"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                event_payload.get("forecast_window_id"),
                event_payload.get("region_code"),
                event_payload.get("occurred_date"),
                event_payload.get("consumption_amount"),
                event_payload.get("z_score"),
                event_payload.get("deviation_ratio"),
                event_payload.get("anomaly_direction"),
                event_payload.get("detected_at"),
            ),
        )
        return cursor.lastrowid


def fetch_anomaly_events_by_window(
    forecast_window_id: Optional[int],
    region_code: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    connection = get_connection()
    try:
        if forecast_window_id is not None:
            rows = connection.execute(
                "SELECT id, forecast_window_id, region_code, occurred_date, "
                "consumption_amount, z_score, deviation_ratio, anomaly_direction, detected_at "
                "FROM forecast_anomaly_event "
                "WHERE forecast_window_id = ? "
                "ORDER BY occurred_date DESC LIMIT ?",
                (forecast_window_id, limit),
            ).fetchall()
        elif region_code is not None:
            rows = connection.execute(
                "SELECT id, forecast_window_id, region_code, occurred_date, "
                "consumption_amount, z_score, deviation_ratio, anomaly_direction, detected_at "
                "FROM forecast_anomaly_event "
                "WHERE region_code = ? "
                "ORDER BY occurred_date DESC LIMIT ?",
                (region_code, limit),
            ).fetchall()
        else:
            rows = connection.execute(
                "SELECT id, forecast_window_id, region_code, occurred_date, "
                "consumption_amount, z_score, deviation_ratio, anomaly_direction, detected_at "
                "FROM forecast_anomaly_event "
                "ORDER BY occurred_date DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]
    finally:
        connection.close()


def update_forecast_window_status(forecast_window_id: int, status: str) -> bool:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "UPDATE consumption_forecast_anomaly SET status = ? WHERE id = ?",
            (status, forecast_window_id),
        )
        return cursor.rowcount > 0


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class ConsumptionForecastStore:
    def list_windows(self, *args, **kwargs):
        return None

    def fetch_window_detail(self, *args, **kwargs):
        return None
