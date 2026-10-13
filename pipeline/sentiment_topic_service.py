# 说明书第 4 章【评论情感与主题挖掘】
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from store.sentiment_topic_mining_store import (
    insert_sentiment_topic_batch,
    fetch_sentiment_topic_batch,
    list_sentiment_topic_batches,
)

logger = logging.getLogger("pipeline.sentiment_topic_service")

_POSITIVE_WORDS = frozenset({"好", "赞", "棒", "满意", "喜欢", "推荐", "实惠", "美味", "舒适", "干净"})
_NEGATIVE_WORDS = frozenset({"差", "贵", "失望", "难吃", "脏", "慢", "糟糕", "后悔", "不值", "态度差"})


def _simple_sentiment_score(text: str) -> float:
    """基于词袋的极简情感打分，返回 [-1.0, 1.0]。"""
    if not text:
        return 0.0
    positive_hits = sum(1 for word in _POSITIVE_WORDS if word in text)
    negative_hits = sum(1 for word in _NEGATIVE_WORDS if word in text)
    total = positive_hits + negative_hits
    if total == 0:
        return 0.0
    return round((positive_hits - negative_hits) / total, 4)


def _cluster_by_keyword(reviews: List[Dict[str, Any]], topic_count: int) -> List[Dict[str, Any]]:
    """按评论内容的高频词做简单主题聚类（占位实现）。"""
    clusters: Dict[str, Dict[str, Any]] = {}
    for review in reviews:
        text = review.get("content") or review.get("text") or ""
        keyword = (text[:8]) if text else "未知主题"
        bucket = clusters.setdefault(keyword, {"topic": keyword, "count": 0, "avg_score": 0.0, "score_sum": 0.0})
        bucket["count"] += 1
        bucket["score_sum"] += float(review.get("score", 0.0))
    result = []
    for bucket in clusters.values():
        bucket["avg_score"] = round(bucket["score_sum"] / bucket["count"], 4) if bucket["count"] else 0.0
        result.append({
            "topic": bucket["topic"],
            "count": bucket["count"],
            "avg_score": bucket["avg_score"],
        })
    result.sort(key=lambda item: item["count"], reverse=True)
    return result[:max(1, topic_count)]


def mine_sentiment_and_topics(payload: Dict[str, Any]) -> Dict[str, Any]:
    """对评论数据执行情感分析与主题挖掘。"""
    reviews = payload.get("reviews") or payload.get("comments") or []
    topic_count = max(1, int(payload.get("topic_count") or 5))
    source_scope = (payload.get("source_scope") or "review_sources").strip()
    analysis_window = (payload.get("analysis_window") or "30d").strip()
    batch_code = payload.get("batch_code") or f"st_{int(__import__('time').time() * 1000)}"

    sentiment_distribution = {"positive": 0, "neutral": 0, "negative": 0}
    for review in reviews:
        score = _simple_sentiment_score(review.get("content") or review.get("text") or "")
        if score > 0.1:
            sentiment_distribution["positive"] += 1
        elif score < -0.1:
            sentiment_distribution["negative"] += 1
        else:
            sentiment_distribution["neutral"] += 1

    topic_clusters = _cluster_by_keyword(reviews, topic_count)
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    batch_payload = {
        "batch_code": batch_code,
        "analysis_window": analysis_window,
        "source_scope": source_scope,
        "total_reviews": len(reviews),
        "sentiment_distribution": sentiment_distribution,
        "topic_clusters": topic_clusters,
        "generated_at": generated_at,
        "status": "completed",
    }
    batch_id = insert_sentiment_topic_batch(batch_payload)
    logger.info("sentiment topic mining done batch=%s reviews=%s", batch_code, len(reviews))
    return {
        "batch_id": batch_id,
        "batch_code": batch_code,
        "sentiment_distribution": sentiment_distribution,
        "topic_clusters": topic_clusters,
        "total_reviews": len(reviews),
        "generated_at": generated_at,
    }


def fetch_sentiment_batch(batch_code: str) -> Optional[Dict[str, Any]]:
    return fetch_sentiment_topic_batch(batch_code)


def list_sentiment_batches(limit: int = 20) -> List[Dict[str, Any]]:
    return list_sentiment_topic_batches(limit)


class SentimentTopicService:
    """情感与主题挖掘服务门面，供任务调度层调用。"""

    def __init__(self, sentiment_topic_mining_store: Any):
        self._store = sentiment_topic_mining_store

    def mine(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        return mine_sentiment_and_topics(payload)

    def fetch_batch(self, batch_code: str) -> Optional[Dict[str, Any]]:
        return fetch_sentiment_batch(batch_code)

    def list_batches(self, limit: int = 20) -> List[Dict[str, Any]]:
        return list_sentiment_batches(limit)
