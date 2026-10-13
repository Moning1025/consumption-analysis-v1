# RFM 用户分群存储层，对应说明书第 5 章 RFM 分层画像工作台
#- R (Recency) 最近消费间隔：距离上一次消费的天数，越小代表用户越活跃
#- F (Frequency) 消费频次：统计周期内消费订单数量
#- M (Monetary) 消费总金额：统计周期用户消费总金额
# 根据三项打分组合，把消费者划分 8 类群体，针对不同群体做差异化运营策略。

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Generator, Iterable, Mapping, Sequence

from store.db_connection import acquire_connection, transaction_scope

logger = logging.getLogger(__name__)

SEGMENT_LABELS = (
    "重要价值客户",
    "重要唤回客户",
    "重要深耕客户",
    "重要挽留客户",
    "潜力客户",
    "新客户",
    "一般客户",
    "流失客户",
)

RFM_SCORE_BOUNDARY = (1, 2, 3, 4, 5)


def _normalize_score(score_value: Any) -> int:
    try:
        numeric_score = int(score_value)
    except (TypeError, ValueError):
        return 1
    if numeric_score < 1:
        return 1
    if numeric_score > 5:
        return 5
    return numeric_score


def _compose_rfm_code(recency_score: int, frequency_score: int, monetary_score: int) -> str:
    return f"{recency_score}{frequency_score}{monetary_score}"


def _resolve_segment_label(recency_score: int, frequency_score: int, monetary_score: int) -> str:
    high_frequency = frequency_score >= 4
    high_monetary = monetary_score >= 4
    recent_active = recency_score >= 4
    if recent_active and high_frequency and high_monetary:
        return "重要价值客户"
    if not recent_active and high_frequency and high_monetary:
        return "重要挽留客户"
    if recent_active and not high_frequency and high_monetary:
        return "重要深耕客户"
    if recent_active and high_frequency and not high_monetary:
        return "重要唤回客户"
    if recent_active and frequency_score <= 2 and monetary_score <= 2:
        return "新客户"
    if not recent_active and frequency_score >= 3 and monetary_score >= 3:
        return "潜力客户"
    if recency_score <= 2 and frequency_score <= 2 and monetary_score <= 2:
        return "流失客户"
    return "一般客户"


def _coerce_audience_size(raw_value: Any) -> int:
    try:
        audience_size = int(raw_value)
    except (TypeError, ValueError):
        return 0
    return audience_size if audience_size >= 0 else 0


def _coerce_ratio(raw_value: Any) -> float:
    try:
        ratio_value = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    if ratio_value < 0.0:
        return 0.0
    if ratio_value > 1.0:
        return 1.0
    return round(ratio_value, 6)


def _coerce_monetary(raw_value: Any) -> float:
    try:
        monetary_amount = float(raw_value)
    except (TypeError, ValueError):
        return 0.0
    return round(monetary_amount, 4)


def _coerce_recency_days(raw_value: Any) -> int:
    try:
        recency_days = int(raw_value)
    except (TypeError, ValueError):
        return 0
    return recency_days if recency_days >= 0 else 0


def _coerce_frequency_count(raw_value: Any) -> int:
    try:
        frequency_count = int(raw_value)
    except (TypeError, ValueError):
        return 0
    return frequency_count if frequency_count >= 0 else 0


def _iter_segment_rows(segment_records: Iterable[Mapping[str, Any]]) -> Generator[tuple, None, None]:
    for segment_record in segment_records:
        recency_score = _normalize_score(segment_record.get("recency_score"))
        frequency_score = _normalize_score(segment_record.get("frequency_count"))
        monetary_score = _normalize_score(segment_record.get("monetary_score"))
        segment_label = segment_record.get("segment_label") or _resolve_segment_label(
            recency_score, frequency_score, monetary_score
        )
        yield (
            str(segment_record.get("customer_identifier") or ""),
            recency_score,
            frequency_score,
            monetary_score,
            _compose_rfm_code(recency_score, frequency_score, monetary_score),
            str(segment_label),
            _coerce_recency_days(segment_record.get("recency_days")),
            _coerce_frequency_count(segment_record.get("frequency_count")),
            _coerce_monetary(segment_record.get("monetary_amount")),
            _coerce_monetary(segment_record.get("average_order_value")),
            str(segment_record.get("preferred_channel") or "unknown"),
            str(segment_record.get("region_code") or "UNKNOWN"),
        )


def _aggregate_audience_scale(segment_rows: Sequence[tuple]) -> tuple:
    audience_counter: dict[str, int] = {}
    total_monetary = 0.0
    for segment_row in segment_rows:
        audience_counter[segment_row[5]] = audience_counter.get(segment_row[5], 0) + 1
        total_monetary += segment_row[8]
    total_customers = len(segment_rows)
    audience_scale = tuple(
        sorted(
            (
                (
                    segment_label,
                    audience_counter.get(segment_label, 0),
                    _coerce_ratio(audience_counter.get(segment_label, 0) / total_customers)
                    if total_customers
                    else 0.0,
                )
                for segment_label in SEGMENT_LABELS
            ),
            key=lambda scale_entry: (-scale_entry[1], scale_entry[0]),
        )
    )
    return audience_scale, total_customers, round(total_monetary, 4)


def _build_snapshot_payload(
    segmentation_batch_code: str,
    analysis_window: str,
    audience_scale: tuple,
    total_customers: int,
    total_monetary: float,
    source_pipeline_code: str,
) -> str:
    snapshot_payload = {
        "segmentation_batch_code": segmentation_batch_code,
        "analysis_window": analysis_window,
        "total_customers": total_customers,
        "total_monetary_amount": total_monetary,
        "source_pipeline_code": source_pipeline_code,
        "audience_scale": [
            {"segment_label": label, "audience_size": size, "audience_ratio": ratio}
            for label, size, ratio in audience_scale
        ],
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    return json.dumps(snapshot_payload, ensure_ascii=False)


def persist_segmentation_batch(
    segmentation_batch_code: str,
    analysis_window: str,
    segment_records: Iterable[Mapping[str, Any]],
    source_pipeline_code: str = "",
    user_name: str = "system",
) -> dict:
    segment_rows = tuple(_iter_segment_rows(segment_records))
    if not segment_rows:
        raise ValueError("RFM 分层画像结果为空，拒绝落库")
    audience_scale, total_customers, total_monetary = _aggregate_audience_scale(segment_rows)
    snapshot_payload = _build_snapshot_payload(
        segmentation_batch_code,
        analysis_window,
        audience_scale,
        total_customers,
        total_monetary,
        source_pipeline_code,
    )
    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with transaction_scope() as connection:
        connection.execute(
            "DELETE FROM rfm_segmentation_studio WHERE id = ?",
            (segmentation_batch_code,),
        )
        connection.executemany(
            "INSERT INTO rfm_segmentation_studio ("
            "    id, analysis_window, user_name, segment_name"
            ") VALUES (?, ?, ?, ?)",
            [
                (
                    segmentation_batch_code,
                    analysis_window,
                    user_name,
                    segment_row[5],
                )
                for segment_row in segment_rows
            ],
        )
    logger.info(
        "RFM 分层画像批次落库完成 batch=%s customers=%s segments=%s",
        segmentation_batch_code,
        total_customers,
        len(audience_scale),
    )
    return {
        "segmentation_batch_code": segmentation_batch_code,
        "total_customers": total_customers,
        "total_monetary_amount": total_monetary,
        "audience_scale": [
            {"segment_label": label, "audience_size": size, "audience_ratio": ratio}
            for label, size, ratio in audience_scale
        ],
        "generated_at": generated_at,
    }


def fetch_segmentation_batch(segmentation_batch_code: str) -> dict:
    with acquire_connection() as connection:
        batch_rows = connection.execute(
            "SELECT "
            "    id, "
            "    analysis_window, "
            "    user_name, "
            "    segment_name "
            "FROM rfm_segmentation_studio "
            "WHERE id = ? "
            "ORDER BY segment_name ASC ",
            (segmentation_batch_code,),
        ).fetchall()
    if not batch_rows:
        raise LookupError(f"未找到 RFM 分层画像批次: {segmentation_batch_code}")
    customer_profiles = [
        {
            "segment_name": batch_row["segment_name"],
        }
        for batch_row in batch_rows
    ]
    return {
        "segmentation_batch_code": segmentation_batch_code,
        "analysis_window": batch_rows[0]["analysis_window"],
        "user_name": batch_rows[0]["user_name"],
        "customer_profiles": customer_profiles,
    }


def fetch_latest_segmentation_batch() -> dict:
    with acquire_connection() as connection:
        latest_batch_row = connection.execute(
            "SELECT id "
            "FROM rfm_segmentation_studio "
            "ORDER BY id DESC "
            "LIMIT 1"
        ).fetchone()
    if not latest_batch_row:
        raise LookupError("RFM 分层画像批次表为空")
    return fetch_segmentation_batch(latest_batch_row["id"])


def fetch_audience_scale_summary(segmentation_batch_code: str) -> tuple:
    with acquire_connection() as connection:
        summary_row = connection.execute(
            "SELECT analysis_window, user_name, segment_name "
            "FROM rfm_segmentation_studio "
            "WHERE id = ? "
            "LIMIT 1 ",
            (segmentation_batch_code,),
        ).fetchone()
    if not summary_row:
        raise LookupError(f"未找到 RFM 分层画像批次: {segmentation_batch_code}")
    return (
        summary_row["analysis_window"],
        summary_row["user_name"],
        summary_row["segment_name"],
    )


def list_segmentation_batches(limit: int = 20) -> tuple:
    bounded_limit = max(1, min(int(limit), 200))
    with acquire_connection() as connection:
        batch_rows = connection.execute(
            "SELECT "
            "id, "
            "MIN(analysis_window) AS analysis_window, "
            "MAX(user_name) AS user_name, "
            "MAX(segment_name) AS segment_name "
            "FROM rfm_segmentation_studio "
            "GROUP BY id "
            "ORDER BY id DESC "
            "LIMIT ? ",
            (bounded_limit,),
        ).fetchall()
    return tuple(
        (
            batch_row["id"],
            batch_row["analysis_window"],
            batch_row["user_name"],
            batch_row["segment_name"],
        )
        for batch_row in batch_rows
    )


def purge_segmentation_batch(segmentation_batch_code: str) -> int:
    with transaction_scope() as connection:
        cursor = connection.execute(
            "DELETE FROM rfm_segmentation_studio WHERE id = ?",
            (segmentation_batch_code,),
        )
        deleted_count = cursor.rowcount or 0
        logger.info("RFM 分层画像批次清理 batch=%s deleted=%s", segmentation_batch_code, deleted_count)
        return deleted_count


# 中文注释：第三阶段自动补充桥接兼容类，保持统一接口命名不被导入断裂打断
class RfmSegmentationStore:
    def list_segmentation_runs(self, *args, **kwargs):
        return None

    def export_segment_report(self, *args, **kwargs):
        return None
