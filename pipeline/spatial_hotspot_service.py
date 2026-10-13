# 说明书第 7 章【空间热点与区域分布】
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from store.spatial_hotspot_store import (
    insert_hotspot_result,
    update_hotspot_status,
    replace_hotspot_points,
    fetch_hotspot_by_id,
    iter_hotspots_by_region,
    iter_hotspots_by_batch,
    aggregate_region_distribution,
    collect_top_hotspot_points,
    purge_archived_hotspots,
)

logger = logging.getLogger("pipeline.spatial_hotspot_service")

GRID_LEVEL_VALUES = frozenset({"coarse", "medium", "fine"})


def _normalize_grid_level(grid_level: str) -> str:
    candidate = (grid_level or "medium").strip().lower()
    if candidate not in GRID_LEVEL_VALUES:
        raise ValueError(f"unsupported grid level: {grid_level}")
    return candidate


def _build_grid_points(records: List[Dict[str, Any]], grid_level: str) -> List[Dict[str, Any]]:
    """将消费记录映射为网格热点点（占位实现：按区域汇总热度）。"""
    region_bucket: Dict[str, Dict[str, Any]] = {}
    for record in records:
        region = str(record.get("city_region") or record.get("region") or "unknown")
        bucket = region_bucket.setdefault(region, {"heat_sum": 0.0, "count": 0})
        bucket["heat_sum"] += float(record.get("consumption_amount") or record.get("amount") or 0.0)
        bucket["count"] += 1
    points = []
    for region, bucket in region_bucket.items():
        points.append({
            "grid_code": f"{grid_level}_{region}",
            "centroid_lng": round(float(bucket.get("centroid_lng", 0.0)), 6),
            "centroid_lat": round(float(bucket.get("centroid_lat", 0.0)), 6),
            "heat_score": round(bucket["heat_sum"] / bucket["count"] if bucket["count"] else 0.0, 4),
        })
    return points


def aggregate_spatial_hotspot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """执行空间热点聚合并落库。"""
    records = payload.get("records") or payload.get("consumption_records") or []
    region_name = (payload.get("region_name") or payload.get("region_code") or "unknown").strip()
    grid_level = _normalize_grid_level(payload.get("grid_precision") or payload.get("grid_level"))
    hotspot_id = int(payload.get("hotspot_id") or int(datetime.now().timestamp()))
    hotspot_points = _build_grid_points(records, grid_level)
    peak_heat_score = max((p.get("heat_score") or 0.0) for p in hotspot_points) if hotspot_points else 0.0

    saved_id = insert_hotspot_result(
        id=hotspot_id,
        region_name=region_name,
        grid_level=grid_level,
        hotspot_points=hotspot_points,
        peak_heat_score=peak_heat_score,
        status="aggregated",
    )
    logger.info("spatial hotspot aggregated id=%s region=%s points=%s", saved_id, region_name, len(hotspot_points))
    return {
        "hotspot_id": saved_id,
        "region_name": region_name,
        "grid_level": grid_level,
        "point_count": len(hotspot_points),
        "peak_heat_score": round(peak_heat_score, 4),
        "hotspot_points": hotspot_points,
        "status": "aggregated",
    }


def publish_hotspot(hotspot_id: int) -> bool:
    return update_hotspot_status(int(hotspot_id), "published")


def archive_hotspot(hotspot_id: int) -> bool:
    return update_hotspot_status(int(hotspot_id), "archived")


def fetch_hotspot(hotspot_id: int) -> Optional[Dict[str, Any]]:
    return fetch_hotspot_by_id(int(hotspot_id))


def get_region_distribution(grid_level: str = "medium") -> tuple:
    normalized = _normalize_grid_level(grid_level)
    return aggregate_region_distribution(normalized)


def get_top_hotspot_points(grid_level: str = "medium", limit: int = 20) -> tuple:
    normalized = _normalize_grid_level(grid_level)
    return collect_top_hotspot_points(normalized, limit)


def cleanup_archived(before_date: str) -> int:
    return purge_archived_hotspots(before_date)


class SpatialHotspotService:
    """空间热点服务门面，供任务调度层调用。"""

    def __init__(self, spatial_hotspot_store: Any):
        self._store = spatial_hotspot_store

    def aggregate(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return aggregate_spatial_hotspot(payload)

    def publish(self, hotspot_id: int) -> bool:
        return publish_hotspot(hotspot_id)

    def archive(self, hotspot_id: int) -> bool:
        return archive_hotspot(hotspot_id)

    def fetch(self, hotspot_id: int) -> Optional[Dict[str, Any]]:
        return fetch_hotspot(hotspot_id)
