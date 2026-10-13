# 说明书第 8 章【个性化推荐工作台】
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from store.recommendation_workbench_store import (
    persist_recommendation_result,
    fetch_recommendation_result,
    list_recommendation_results,
)

logger = logging.getLogger("pipeline.recommendation_ranking_service")


def rank_personalized_recommendation(payload: Dict[str, Any]) -> Dict[str, Any]:
    """基于消费/画像特征生成个性化推荐排序并落库。"""
    segment_label = (payload.get("segment_label") or "general").strip()
    top_n = max(1, min(int(payload.get("top_n") or 10), 50))
    candidates = payload.get("candidates") or payload.get("merchants") or []
    preferences = payload.get("preferences") or payload.get("preferred_categories") or []

    ranked = _rank_candidates(candidates, preferences, top_n)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    source_pipeline_code = (payload.get("source_pipeline_code") or "").strip()
    result_id = persist_recommendation_result({
        "segment_label": segment_label,
        "top_n": top_n,
        "recommendations": ranked,
        "source_pipeline_code": source_pipeline_code,
        "generated_at": generated_at,
        "status": "completed",
    })
    logger.info("recommendation ranked id=%s segment=%s top=%s", result_id, segment_label, top_n)
    return {
        "recommendation_id": result_id,
        "segment_label": segment_label,
        "recommendations": ranked,
        "generated_at": generated_at,
    }


def _rank_candidates(
    candidates: List[Dict[str, Any]],
    preferences: List[str],
    top_n: int,
) -> List[Dict[str, Any]]:
    scored = []
    for candidate in candidates:
        category = str(candidate.get("category_label") or candidate.get("category") or "")
        order_count = float(candidate.get("order_count") or 0)
        total_amount = float(candidate.get("total_amount") or 0)
        preference_boost = 1.0 if category in preferences else 0.0
        score = order_count * 1.0 + total_amount * 0.001 + preference_boost * 100.0
        scored.append({
            "merchant_name": candidate.get("merchant_name") or candidate.get("name") or "unknown",
            "category_label": category,
            "order_count": int(order_count),
            "total_amount": round(total_amount, 2),
            "score": round(score, 4),
        })
    scored.sort(key=lambda item: item["score"], reverse=True)
    return scored[:top_n]


def fetch_recommendation(recommendation_id: int) -> Optional[Dict[str, Any]]:
    return fetch_recommendation_result(int(recommendation_id))


def list_recommendations(limit: int = 20) -> List[Dict[str, Any]]:
    return list_recommendation_results(limit)


class RecommendationRankingService:
    """个性化推荐排序服务门面，供任务调度层调用。"""

    def __init__(
        self,
        recommendation_workbench_store: Any,
        rfm_segmentation_store: Any,
        spatial_hotspot_store: Any,
    ):
        self._workbench_store = recommendation_workbench_store
        self._rfm_store = rfm_segmentation_store
        self._hotspot_store = spatial_hotspot_store

    def rank(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return rank_personalized_recommendation(payload)

    def fetch(self, recommendation_id: int) -> Optional[Dict[str, Any]]:
        return fetch_recommendation(recommendation_id)

    def list(self, limit: int = 20) -> List[Dict[str, Any]]:
        return list_recommendations(limit)
