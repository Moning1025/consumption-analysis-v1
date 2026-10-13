# 说明书第 5 章【RFM 分层画像工作台】（调用rfm_segmentation_store）
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from store.rfm_segmentation_store import (
    persist_segmentation_batch,
    fetch_segmentation_batch,
    fetch_latest_segmentation_batch,
    list_segmentation_batches,
)

logger = logging.getLogger("pipeline.rfm_segmentation_service")

RFM_SCORE_BOUNDARY = (1, 2, 3, 4, 5)


def _bucketed_score(value: float, boundaries: tuple, reverse: bool = False) -> int:
    """将连续值映射到 1-5 分档。reverse=True 表示值越大分越低（如 recency）。"""
    score = 1
    for boundary in boundaries:
        if reverse:
            if value <= boundary:
                break
        else:
            if value < boundary:
                break
        score += 1
    return max(1, min(5, score))


def _compute_rfm_features(records: List[Dict[str, Any]], recency_days: int) -> List[Dict[str, Any]]:
    """为每条消费记录生成 RFM 特征（按用户聚合的简化实现）。"""
    user_map: Dict[str, List[Dict[str, Any]]] = {}
    for record in records:
        user_key = str(record.get("user_identifier") or record.get("customer_identifier") or "anonymous")
        user_map.setdefault(user_key, []).append(record)

    segments = []
    now_epoch = int(datetime.now(timezone.utc).timestamp())
    for user_key, user_records in user_map.items():
        monetary_total = sum(float(r.get("consumption_amount") or r.get("amount") or 0.0) for r in user_records)
        frequency = len(user_records)
        timestamps = [
            float(r.get("consumption_time_epoch") or 0.0)
            for r in user_records
            if r.get("consumption_time_epoch")
        ]
        last_ts = max(timestamps) if timestamps else now_epoch
        recency = max(0, (now_epoch - last_ts) // 86400)
        recency_score = _bucketed_score(recency, RFM_SCORE_BOUNDARY, reverse=True)
        frequency_score = _bucketed_score(frequency, RFM_SCORE_BOUNDARY)
        monetary_score = _bucketed_score(monetary_total, RFM_SCORE_BOUNDARY)
        segments.append({
            "customer_identifier": user_key,
            "recency_days": int(recency),
            "frequency_count": frequency,
            "monetary_amount": round(monetary_total, 4),
            "average_order_value": round(monetary_total / frequency, 4) if frequency else 0.0,
            "recency_score": recency_score,
            "frequency_score": frequency_score,
            "monetary_score": monetary_score,
            "preferred_channel": (user_records[-1].get("source_channel") or "unknown"),
            "region_code": (user_records[-1].get("city_region") or "UNKNOWN"),
        })
    return segments


def compute_rfm_segmentation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """执行 RFM 分层画像并落库。"""
    records = payload.get("records") or payload.get("consumption_records") or []
    recency_days = max(1, int(payload.get("recency_days") or 90))
    frequency_bins = payload.get("frequency_bins") or (1, 3, 6, 12)
    monetary_bins = payload.get("monetary_bins") or (100, 500, 2000, 8000)
    source_pipeline_code = (payload.get("source_pipeline_code") or "").strip()
    analysis_window = (payload.get("analysis_window") or "30d").strip()
    segmentation_batch_code = payload.get("segmentation_batch_code") or f"rfm_{int(__import__('time').time() * 1000)}"

    segments = _compute_rfm_features(records, recency_days)
    result = persist_segmentation_batch(
        segmentation_batch_code=segmentation_batch_code,
        analysis_window=analysis_window,
        segment_records=segments,
        source_pipeline_code=source_pipeline_code,
    )
    logger.info("RFM segmentation done batch=%s customers=%s", segmentation_batch_code, result["total_customers"])
    return result


def fetch_rfm_batch(batch_code: str) -> Dict[str, Any]:
    return fetch_segmentation_batch(batch_code)


def fetch_latest_rfm_batch() -> Dict[str, Any]:
    return fetch_latest_segmentation_batch()


def list_rfm_batches(limit: int = 20) -> tuple:
    return list_segmentation_batches(limit)


class RfmSegmentationService:
    """RFM 分层画像服务门面，供任务调度层调用。"""

    def __init__(self, rfm_segmentation_store: Any):
        self._store = rfm_segmentation_store

    def segment(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return compute_rfm_segmentation(payload)

    def fetch_batch(self, batch_code: str) -> Dict[str, Any]:
        return fetch_rfm_batch(batch_code)

    def fetch_latest(self) -> Dict[str, Any]:
        return fetch_latest_rfm_batch()

    def list_batches(self, limit: int = 20) -> tuple:
        return list_rfm_batches(limit)
