import sqlite3
import json
import logging
from typing import Iterable, Iterator, Optional, Sequence

from store.db_connection import get_connection, transaction_scope

logger = logging.getLogger(__name__)

GRID_LEVEL_VALUES = frozenset({"coarse", "medium", "fine"})
HOTSPOT_STATUS_VALUES = frozenset({"pending", "aggregated", "published", "archived"})


def _normalize_grid_level(grid_level: str) -> str:
    if grid_level not in GRID_LEVEL_VALUES:
        raise ValueError(f"unsupported grid level: {grid_level}")
    return grid_level


def _encode_hotspot_coordinates(hotspot_points: Sequence[dict]) -> str:
    compact_points = [
        {
            "grid_code": str(point.get("grid_code", "")),
            "centroid_lng": round(float(point.get("centroid_lng", 0.0)), 6),
            "centroid_lat": round(float(point.get("centroid_lat", 0.0)), 6),
            "heat_score": round(float(point.get("heat_score", 0.0)), 4),
        }
        for point in hotspot_points
        if point.get("grid_code")
    ]
    return json.dumps(compact_points, ensure_ascii=False, separators=(",", ":"))


def _decode_hotspot_coordinates(raw_payload: Optional[str]) -> tuple:
    if not raw_payload:
        return ()
    try:
        decoded = json.loads(raw_payload)
    except (TypeError, ValueError):
        logger.warning("hotspot coordinate payload corrupted, fallback to empty tuple")
        return ()
    if not isinstance(decoded, list):
        return ()
    return tuple(decoded)


def _row_to_hotspot_record(row: sqlite3.Row) -> dict:
    detail_payload = row["detail_payload"]
    decoded_detail = {}
    if detail_payload:
        try:
            decoded_detail = json.loads(detail_payload)
        except (TypeError, ValueError):
            logger.warning("hotspot detail payload corrupted, fallback to empty dict")
            decoded_detail = {}
    if not isinstance(decoded_detail, dict):
        decoded_detail = {}
    return {
        "id": row["id"],
        "grid_code": row["grid_code"],
        "region_name": row["region_name"],
        "longitude": row["longitude"],
        "latitude": row["latitude"],
        "hotspot_score": row["hotspot_score"],
        "aggregation_count": row["aggregation_count"],
        "snapshot_time": row["snapshot_time"],
        "grid_level": decoded_detail.get("grid_level"),
        "grid_total_count": decoded_detail.get("grid_total_count", 0),
        "hotspot_points": _decode_hotspot_coordinates(decoded_detail.get("hotspot_coordinates")),
        "peak_heat_score": decoded_detail.get("peak_heat_score", row["hotspot_score"]),
        "status": decoded_detail.get("hotspot_status", "pending"),
        "created_at": decoded_detail.get("created_at"),
        "updated_at": decoded_detail.get("updated_at"),
    }


def insert_hotspot_result(
    id: int,
    region_name: str,
    grid_level: str,
    hotspot_points: Sequence[dict],
    peak_heat_score: float,
    status: str = "pending",
) -> int:
    normalized_level = _normalize_grid_level(grid_level)
    if status not in HOTSPOT_STATUS_VALUES:
        raise ValueError(f"unsupported hotspot status: {status}")
    if not region_name or not region_name.strip():
        raise ValueError("region_name must not be blank")
    encoded_points = _encode_hotspot_coordinates(hotspot_points)
    detail_payload = json.dumps(
        {
            "grid_level": normalized_level,
            "grid_total_count": len(hotspot_points),
            "hotspot_coordinates": encoded_points,
            "peak_heat_score": round(float(peak_heat_score), 4),
            "hotspot_status": status,
            "created_at": None,
            "updated_at": None,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO spatial_hotspot_map ("
            "id, grid_code, region_name, longitude, latitude, "
            "hotspot_score, aggregation_count, snapshot_time, detail_payload"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'), ?)",
            (
                id,
                None,
                region_name.strip(),
                None,
                None,
                round(float(peak_heat_score), 4),
                len(hotspot_points),
                detail_payload,
            ),
        )
        new_id = cursor.lastrowid
        logger.info("hotspot result persisted id=%s region=%s", new_id, region_name)
        return new_id


def update_hotspot_status(id: int, status: str) -> bool:
    if status not in HOTSPOT_STATUS_VALUES:
        raise ValueError(f"unsupported hotspot status: {status}")
    with transaction_scope() as connection:
        row = connection.execute(
            "SELECT detail_payload FROM spatial_hotspot_map WHERE id = ?",
            (id,),
        ).fetchone()
        if row is None:
            logger.warning("hotspot status update missed id=%s", id)
            return False
        detail_payload = row["detail_payload"]
        decoded_detail = {}
        if detail_payload:
            try:
                decoded_detail = json.loads(detail_payload)
            except (TypeError, ValueError):
                decoded_detail = {}
        if not isinstance(decoded_detail, dict):
            decoded_detail = {}
        decoded_detail["hotspot_status"] = status
        decoded_detail["updated_at"] = None
        cursor = connection.execute(
            "UPDATE spatial_hotspot_map "
            "SET detail_payload = ? "
            "WHERE id = ? ",
            (
                json.dumps(decoded_detail, ensure_ascii=False, separators=(",", ":")),
                id,
            ),
        )
        affected = cursor.rowcount
        if affected == 0:
            logger.warning("hotspot status update missed id=%s", id)
        return affected > 0


def replace_hotspot_points(id: int, hotspot_points: Sequence[dict], peak_heat_score: float) -> bool:
    encoded_points = _encode_hotspot_coordinates(hotspot_points)
    with transaction_scope() as connection:
        row = connection.execute(
            "SELECT detail_payload FROM spatial_hotspot_map WHERE id = ?",
            (id,),
        ).fetchone()
        if row is None:
            return False
        detail_payload = row["detail_payload"]
        decoded_detail = {}
        if detail_payload:
            try:
                decoded_detail = json.loads(detail_payload)
            except (TypeError, ValueError):
                decoded_detail = {}
        if not isinstance(decoded_detail, dict):
            decoded_detail = {}
        decoded_detail["hotspot_coordinates"] = encoded_points
        decoded_detail["grid_total_count"] = len(hotspot_points)
        decoded_detail["peak_heat_score"] = round(float(peak_heat_score), 4)
        cursor = connection.execute(
            "UPDATE spatial_hotspot_map "
            "SET detail_payload = ?, hotspot_score = ?, aggregation_count = ? "
            "WHERE id = ? ",
            (
                json.dumps(decoded_detail, ensure_ascii=False, separators=(",", ":")),
                round(float(peak_heat_score), 4),
                len(hotspot_points),
                id,
            ),
        )
        affected = cursor.rowcount
        return affected > 0


def fetch_hotspot_by_id(id: int) -> Optional[dict]:
    connection = get_connection()
    try:
        row = connection.execute(
            "SELECT * FROM spatial_hotspot_map WHERE id = ?",
            (id,),
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        return None
    return _row_to_hotspot_record(row)


def iter_hotspots_by_region(region_name: str, grid_level: Optional[str] = None) -> Iterator[dict]:
    connection = get_connection()
    try:
        if grid_level is None:
            cursor = connection.execute(
                "SELECT * FROM spatial_hotspot_map "
                "WHERE region_name = ? "
                "ORDER BY hotspot_score DESC, id DESC ",
                (region_name,),
            )
        else:
            normalized_level = _normalize_grid_level(grid_level)
            cursor = connection.execute(
                "SELECT * FROM spatial_hotspot_map "
                "WHERE region_name = ? "
                "AND json_extract(detail_payload, '$.grid_level') = ? "
                "ORDER BY hotspot_score DESC, id DESC ",
                (region_name, normalized_level),
            )
        for row in cursor:
            yield _row_to_hotspot_record(row)
    finally:
        connection.close()


def iter_hotspots_by_batch(id: int) -> Iterator[dict]:
    connection = get_connection()
    try:
        cursor = connection.execute(
            "SELECT * FROM spatial_hotspot_map "
            "WHERE id = ? "
            "ORDER BY id ASC ",
            (id,),
        )
        for row in cursor:
            yield _row_to_hotspot_record(row)
    finally:
        connection.close()


def aggregate_region_distribution(grid_level: str) -> tuple:
    normalized_level = _normalize_grid_level(grid_level)
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT region_name, "
            "COUNT(*) AS hotspot_count, "
            "SUM(aggregation_count) AS total_grid_count, "
            "MAX(hotspot_score) AS top_heat_score, "
            "AVG(hotspot_score) AS mean_heat_score "
            "FROM spatial_hotspot_map "
            "WHERE json_extract(detail_payload, '$.grid_level') = ? "
            "AND json_extract(detail_payload, '$.hotspot_status') IN ('aggregated', 'published') "
            "GROUP BY region_name "
            "ORDER BY top_heat_score DESC ",
            (normalized_level,),
        ).fetchall()
    finally:
        connection.close()
    return tuple(
        {
            "region_name": row["region_name"],
            "hotspot_count": row["hotspot_count"],
            "total_grid_count": row["total_grid_count"] or 0,
            "top_heat_score": round(row["top_heat_score"] or 0.0, 4),
            "mean_heat_score": round(row["mean_heat_score"] or 0.0, 4),
        }
        for row in rows
    )


def collect_top_hotspot_points(grid_level: str, limit: int = 20) -> tuple:
    normalized_level = _normalize_grid_level(grid_level)
    if limit <= 0:
        raise ValueError("limit must be positive")
    connection = get_connection()
    try:
        rows = connection.execute(
            "SELECT id, region_name, detail_payload, hotspot_score "
            "FROM spatial_hotspot_map "
            "WHERE json_extract(detail_payload, '$.grid_level') = ? "
            "AND json_extract(detail_payload, '$.hotspot_status') IN ('aggregated', 'published') "
            "ORDER BY hotspot_score DESC "
            "LIMIT ? ",
            (normalized_level, limit),
        ).fetchall()
    finally:
        connection.close()
    ranked_points = []
    for row in rows:
        detail_payload = row["detail_payload"]
        decoded_detail = {}
        if detail_payload:
            try:
                decoded_detail = json.loads(detail_payload)
            except (TypeError, ValueError):
                decoded_detail = {}
        if not isinstance(decoded_detail, dict):
            decoded_detail = {}
        for point in _decode_hotspot_coordinates(decoded_detail.get("hotspot_coordinates")):
            ranked_points.append(
                {
                    "id": row["id"],
                    "region_name": row["region_name"],
                    "grid_code": point.get("grid_code"),
                    "centroid_lng": point.get("centroid_lng"),
                    "centroid_lat": point.get("centroid_lat"),
                    "heat_score": point.get("heat_score"),
                }
            )
    ranked_points.sort(key=lambda item: item.get("heat_score") or 0.0, reverse=True)
    return tuple(ranked_points[:limit])


def purge_archived_hotspots(before_date: str) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "DELETE FROM spatial_hotspot_map "
            "WHERE json_extract(detail_payload, '$.hotspot_status') = 'archived' "
            "AND json_extract(detail_payload, '$.updated_at') < ? ",
            (before_date,),
        )
        removed = cursor.rowcount
        logger.info("archived hotspot purge removed=%s before=%s", removed, before_date)
        return removed
