import json
import logging
import re
import time
import unicodedata
from datetime import datetime, timedelta
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from store.cleaning_fusion_pipeline_store import (
    fetch_cleaning_fusion_task,
    list_cleaning_fusion_tasks,
    insert_cleaning_fusion_task,
    update_task_status_and_progress,
)
from store.data_ingestion_console_store import query_ingestion_records

logger = logging.getLogger("pipeline.cleaning_fusion_service")

PIPELINE_STAGE_SEQUENCE: Tuple[str, ...] = (
    "normalize",
    "deduplicate",
    "align",
    "fuse",
    "persist",
)

STAGE_PROGRESS_WEIGHT: Dict[str, int] = {
    "normalize": 15,
    "deduplicate": 30,
    "align": 25,
    "fuse": 20,
    "persist": 10,
}

SOURCE_PRIORITY: Dict[str, int] = {
    "dianping": 90,
    "meituan": 85,
    "ctrip": 80,
    "fliggy": 78,
    "tmall": 75,
    "jd": 72,
    "offline_pos": 60,
}

FIELD_ALIAS_TABLE: Dict[str, Tuple[str, ...]] = {
    "merchant_name": ("shop_name", "store_name", "merchant", "商户名称", "门店"),
    "consumption_amount": ("amount", "total_fee", "pay_amount", "消费金额", "实付"),
    "consumption_time": ("trade_time", "order_time", "pay_time", "消费时间", "下单时间"),
    "user_identifier": ("user_id", "member_id", "customer_id", "用户标识", "会员号"),
    "category_label": ("category", "biz_category", "industry", "品类", "行业"),
    "city_region": ("city", "region", "area", "城市", "区域"),
}

DEDUP_SIMILARITY_THRESHOLD = 0.92
DEDUP_AMOUNT_TOLERANCE = Decimal("0.01")
DEDUP_TIME_WINDOW_SECONDS = 90
CURRENCY_NOISE_PATTERN = re.compile(r"[^\d.\-]")
WHITESPACE_COLLAPSE_PATTERN = re.compile(r"\s+")
CONTROL_CHAR_PATTERN = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
MAX_RECORDS_PER_BATCH = 20000


def _coerce_decimal(raw_value: Any) -> Optional[Decimal]:
    if raw_value is None:
        return None
    if isinstance(raw_value, Decimal):
        return raw_value
    if isinstance(raw_value, (int, float)):
        try:
            return Decimal(str(raw_value))
        except InvalidOperation:
            return None
    cleaned = CURRENCY_NOISE_PATTERN.sub("", str(raw_value).strip())
    if not cleaned or cleaned in ("-", ".", "-."):
        return None
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return None


def _coerce_timestamp(raw_value: Any) -> Optional[datetime]:
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value
    if isinstance(raw_value, (int, float)):
        numeric_value = float(raw_value)
        if numeric_value > 1e11:
            numeric_value = numeric_value / 1000.0
        try:
            return datetime.fromtimestamp(numeric_value)
        except (OverflowError, OSError, ValueError):
            return None
    textual = str(raw_value).strip()
    if not textual:
        return None
    candidate_formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y%m%d%H%M%S",
        "%Y%m%d",
    )
    for candidate_format in candidate_formats:
        try:
            return datetime.strptime(textual, candidate_format)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(textual.replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _sanitize_text(raw_value: Any) -> str:
    if raw_value is None:
        return ""
    normalized = unicodedata.normalize("NFKC", str(raw_value))
    normalized = CONTROL_CHAR_PATTERN.sub("", normalized)
    return WHITESPACE_COLLAPSE_PATTERN.sub(" ", normalized).strip()


def _resolve_field(record: Dict[str, Any], canonical_field: str) -> Any:
    if canonical_field in record and record[canonical_field] not in (None, ""):
        return record[canonical_field]
    for alias in FIELD_ALIAS_TABLE.get(canonical_field, ()):  # 兼容各源异构字段名
        if alias in record and record[alias] not in (None, ""):
            return record[alias]
    return None


def normalize_ingestion_record(raw_record: Dict[str, Any], source_channel: str) -> Optional[Dict[str, Any]]:
    merchant_name = _sanitize_text(_resolve_field(raw_record, "merchant_name"))
    if not merchant_name:
        return None
    consumption_amount = _coerce_decimal(_resolve_field(raw_record, "consumption_amount"))
    consumption_time = _coerce_timestamp(_resolve_field(raw_record, "consumption_time"))
    if consumption_amount is None or consumption_time is None:
        return None
    user_identifier = _sanitize_text(_resolve_field(raw_record, "user_identifier")) or "anonymous_consumer"
    category_label = _sanitize_text(_resolve_field(raw_record, "category_label")) or "未分类"
    city_region = _sanitize_text(_resolve_field(raw_record, "city_region")) or "未知区域"
    return {
        "merchant_name": merchant_name,
        "merchant_name_key": merchant_name.casefold(),
        "consumption_amount": consumption_amount,
        "consumption_time": consumption_time,
        "consumption_time_epoch": int(consumption_time.timestamp()),
        "user_identifier": user_identifier,
        "category_label": category_label,
        "city_region": city_region,
        "source_channel": source_channel,
        "source_priority": SOURCE_PRIORITY.get(source_channel, 50),
    }


def _iter_normalized_records(ingestion_batch_rows: Iterable[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
    for ingestion_row in ingestion_batch_rows:
        source_channel = _sanitize_text(ingestion_row.get("source_channel")) or "unknown_source"
        payload_text = ingestion_row.get("payload_json")
        if not payload_text:
            continue
        try:
            payload_records = json.loads(payload_text)
        except (TypeError, ValueError):
            logger.warning("接入批次 %s 载荷解析失败，跳过", ingestion_row.get("batch_identifier"))
            continue
        if isinstance(payload_records, dict):
            payload_records = payload_records.get("records", [])
        if not isinstance(payload_records, list):
            continue
        for raw_record in payload_records:
            if not isinstance(raw_record, dict):
                continue
            normalized_record = normalize_ingestion_record(raw_record, source_channel)
            if normalized_record is not None:
                yield normalized_record


def _merchant_similarity(left_name: str, right_name: str) -> float:
    if left_name == right_name:
        return 1.0
    if left_name in right_name or right_name in left_name:
        shorter_length = min(len(left_name), len(right_name))
        longer_length = max(len(left_name), len(right_name))
        if longer_length and shorter_length / longer_length >= 0.6:
            return 0.95
    return SequenceMatcher(None, left_name, right_name).ratio()


def deduplicate_records(normalized_records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
    ordered_records = sorted(
        normalized_records,
        key=lambda record: (
            record["merchant_name_key"],
            record["consumption_time_epoch"],
            -record["source_priority"],
        ),
    )
    retained_records: List[Dict[str, Any]] = []
    removed_counter = 0
    for candidate_record in ordered_records:
        duplicate_index = -1
        for retained_index in range(len(retained_records) - 1, -1, -1):
            retained_record = retained_records[retained_index]
            if abs(
                retained_record["consumption_time_epoch"]
                - candidate_record["consumption_time_epoch"]
            ) > DEDUP_TIME_WINDOW_SECONDS:
                continue
            if _merchant_similarity(
                retained_record["merchant_name_key"], candidate_record["merchant_name_key"]
            ) < DEDUP_SIMILARITY_THRESHOLD:
                continue
            if abs(
                retained_record["consumption_amount"] - candidate_record["consumption_amount"]
            ) > DEDUP_AMOUNT_TOLERANCE:
                continue
            if retained_record["user_identifier"] != candidate_record["user_identifier"]:
                continue
            duplicate_index = retained_index
            break
        if duplicate_index >= 0:
            removed_counter += 1
            continue
        retained_records.append(candidate_record)
    return retained_records, removed_counter


def align_records_to_canonical_schema(deduplicated_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    aligned_records: List[Dict[str, Any]] = []
    for record in deduplicated_records:
        aligned_records.append(
            {
                "merchant_name": record["merchant_name"],
                "consumption_amount": str(record["consumption_amount"].quantize(Decimal("0.01"))),
                "consumption_time": record["consumption_time"].strftime("%Y-%m-%d %H:%M:%S"),
                "consumption_date": record["consumption_time"].strftime("%Y-%m-%d"),
                "consumption_hour": record["consumption_time"].hour,
                "user_identifier": record["user_identifier"],
                "category_label": record["category_label"],
                "city_region": record["city_region"],
                "source_channel": record["source_channel"],
            }
        )
    return aligned_records


def fuse_aligned_records(aligned_records: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    merchant_aggregate: Dict[str, Dict[str, Any]] = {}
    category_aggregate: Dict[str, Dict[str, Any]] = {}
    region_aggregate: Dict[str, Dict[str, Any]] = {}
    hourly_distribution: Dict[int, int] = {hour: 0 for hour in range(24)}
    total_amount = Decimal("0.00")
    distinct_consumers = set()
    for record in aligned_records:
        amount_value = Decimal(record["consumption_amount"])
        total_amount += amount_value
        distinct_consumers.add(record["user_identifier"])
        merchant_bucket = merchant_aggregate.setdefault(
            record["merchant_name"],
            {"order_count": 0, "total_amount": Decimal("0.00"), "source_channels": set()},
        )
        merchant_bucket["order_count"] += 1
        merchant_bucket["total_amount"] += amount_value
        merchant_bucket["source_channels"].add(record["source_channel"])
        category_bucket = category_aggregate.setdefault(
            record["category_label"], {"order_count": 0, "total_amount": Decimal("0.00")}
        )
        category_bucket["order_count"] += 1
        category_bucket["total_amount"] += amount_value
        region_bucket = region_aggregate.setdefault(
            record["city_region"], {"order_count": 0, "total_amount": Decimal("0.00")}
        )
        region_bucket["order_count"] += 1
        region_bucket["total_amount"] += amount_value
        hourly_distribution[record["consumption_hour"]] += 1

    top_merchants = sorted(
        (
            {
                "merchant_name": merchant_name,
                "order_count": bucket["order_count"],
                "total_amount": str(bucket["total_amount"].quantize(Decimal("0.01"))),
                "source_channels": sorted(bucket["source_channels"]),
            }
            for merchant_name, bucket in merchant_aggregate.items()
        ),
        key=lambda merchant_entry: (-merchant_entry["order_count"], merchant_entry["merchant_name"]),
    )[:20]

    category_distribution = sorted(
        (
            {
                "category_label": category_label,
                "order_count": bucket["order_count"],
                "total_amount": str(bucket["total_amount"].quantize(Decimal("0.01"))),
            }
            for category_label, bucket in category_aggregate.items()
        ),
        key=lambda category_entry: -category_entry["order_count"],
    )

    region_distribution = sorted(
        (
            {
                "city_region": city_region,
                "order_count": bucket["order_count"],
                "total_amount": str(bucket["total_amount"].quantize(Decimal("0.01"))),
            }
            for city_region, bucket in region_aggregate.items()
        ),
        key=lambda region_entry: -region_entry["order_count"],
    )

    return {
        "record_count": len(aligned_records),
        "distinct_consumer": len(distinct_consumers),
        "total_amount": str(total_amount.quantize(Decimal("0.01"))),
        "average_amount": str(
            (total_amount / len(aligned_records)).quantize(Decimal("0.01")) if aligned_records else Decimal("0.00")
        ),
        "top_merchants": top_merchants,
        "category_distribution": category_distribution,
        "region_distribution": region_distribution,
        "hourly_distribution": [
            {"hour": hour, "order_count": count}
            for hour, count in sorted(hourly_distribution.items())
        ],
    }


def _advance_stage_progress(pipeline_task_identifier: str, stage_name: str, processed_count: int) -> None:
    update_task_status_and_progress(
        pipeline_task_identifier,
        "processing",
        progress_percent=STAGE_PROGRESS_WEIGHT.get(stage_name, 0),
        processed_record_count=processed_count,
        current_stage=stage_name,
    )


def execute_cleaning_fusion_pipeline(
    pipeline_task_identifier: str, ingestion_batch_identifiers: Sequence[str]
) -> Dict[str, Any]:
    pipeline_task = fetch_cleaning_fusion_task(pipeline_task_identifier)
    if pipeline_task is None:
        raise ValueError(f"清洗融合任务不存在: {pipeline_task_identifier}")
    if pipeline_task.get("task_status") == "running":
        raise RuntimeError(f"清洗融合任务正在执行: {pipeline_task_identifier}")
    update_task_status_and_progress(
        pipeline_task_identifier,
        "running",
        started_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    stage_timings: List[Dict[str, Any]] = []
    try:
        stage_started = time.perf_counter()
        ingestion_rows = list(query_ingestion_records(ingestion_batch_identifiers))
        normalized_records = list(_iter_normalized_records(ingestion_rows))
        if len(normalized_records) > MAX_RECORDS_PER_BATCH:
            normalized_records = normalized_records[:MAX_RECORDS_PER_BATCH]
        _advance_stage_progress(pipeline_task_identifier, "normalize", len(normalized_records))
        stage_timings.append(
            {"stage": "normalize", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        stage_started = time.perf_counter()
        deduplicated_records, removed_duplicate_count = deduplicate_records(normalized_records)
        _advance_stage_progress(pipeline_task_identifier, "deduplicate", len(deduplicated_records))
        stage_timings.append(
            {"stage": "deduplicate", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        stage_started = time.perf_counter()
        aligned_records = align_records_to_canonical_schema(deduplicated_records)
        _advance_stage_progress(pipeline_task_identifier, "align", len(aligned_records))
        stage_timings.append(
            {"stage": "align", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        stage_started = time.perf_counter()
        fusion_summary = fuse_aligned_records(aligned_records)
        _advance_stage_progress(pipeline_task_identifier, "fuse", len(aligned_records))
        stage_timings.append(
            {"stage": "fuse", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        stage_started = time.perf_counter()
        execution_summary = {
            "pipeline_task_identifier": pipeline_task_identifier,
            "ingestion_batch_identifiers": list(ingestion_batch_identifiers),
            "normalized_record_count": len(normalized_records),
            "removed_duplicate_count": removed_duplicate_count,
            "aligned_record_count": len(aligned_records),
            "fusion_summary": fusion_summary,
            "stage_timings": stage_timings,
            "finished_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        update_task_status_and_progress(
            pipeline_task_identifier,
            "succeeded",
            finished_at=execution_summary["finished_at"],
            execution_summary=json.dumps(execution_summary, ensure_ascii=False),
            progress_percent=100,
        )
        stage_timings.append(
            {"stage": "persist", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )
        execution_summary["stage_timings"] = stage_timings
        logger.info(
            "清洗融合任务完成 task=%s 记录=%s 去重=%s",
            pipeline_task_identifier,
            len(aligned_records),
            removed_duplicate_count,
        )
        return execution_summary
    except Exception as pipeline_error:
        logger.exception("清洗融合任务失败 task=%s", pipeline_task_identifier)
        update_task_status_and_progress(
            pipeline_task_identifier,
            "failed",
            finished_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            execution_summary=json.dumps({"error": str(pipeline_error)}, ensure_ascii=False),
        )
        raise


def run_task_cleaning_fusion_pipeline_5(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = (
        _sanitize_text(payload.get("pipeline_task_identifier")) or _sanitize_text(payload.get("task_identifier"))
    )
    if not pipeline_task_identifier:
        pipeline_task_identifier = insert_cleaning_fusion_task(
            task_name=_sanitize_text(payload.get("task_name")) or "消费预测与异常检测前置清洗",
            task_kind="consumption_forecast_anomaly",
            source_scope=_sanitize_text(payload.get("source_scope")) or "all_sources",
        )
    ingestion_batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers", [])))
    )
    if not ingestion_batch_identifiers:
        ingestion_batch_identifiers = [
            row["batch_identifier"]
            for row in query_ingestion_records(None)
            if row.get("batch_identifier")
        ]
    execution_summary = execute_cleaning_fusion_pipeline(pipeline_task_identifier, ingestion_batch_identifiers)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "task_status": "succeeded",
        "normalized_record_count": execution_summary["normalized_record_count"],
        "removed_duplicate_count": execution_summary["removed_duplicate_count"],
        "aligned_record_count": execution_summary["aligned_record_count"],
        "total_amount": execution_summary["fusion_summary"]["total_amount"],
        "finished_at": execution_summary["finished_at"],
    }


def run_task_cleaning_fusion_pipeline_9(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = (
        _sanitize_text(payload.get("pipeline_task_identifier")) or _sanitize_text(payload.get("task_identifier"))
    )
    if not pipeline_task_identifier:
        pipeline_task_identifier = insert_cleaning_fusion_task(
            task_name=_sanitize_text(payload.get("task_name")) or "消费分析可视化大数据融合",
            task_kind="consumption_dashboard_fusion",
            source_scope=_sanitize_text(payload.get("source_scope")) or "all_sources",
        )
    ingestion_batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers", [])))
    )
    if not ingestion_batch_identifiers:
        ingestion_batch_identifiers = [
            row["batch_identifier"]
            for row in query_ingestion_records(None)
            if row.get("batch_identifier")
        ]
    execution_summary = execute_cleaning_fusion_pipeline(pipeline_task_identifier, ingestion_batch_identifiers)
    fusion_summary = execution_summary["fusion_summary"]
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "task_status": "succeeded",
        "record_count": fusion_summary["record_count"],
        "distinct_consumer_count": fusion_summary["distinct_consumer"],
        "total_amount": fusion_summary["total_amount"],
        "top_merchants": fusion_summary["top_merchants"],
        "category_distribution": fusion_summary["category_distribution"],
        "region_distribution": fusion_summary["region_distribution"],
        "hourly_distribution": fusion_summary["hourly_distribution"],
        "finished_at": execution_summary["finished_at"],
    }


def run_task_cleaning_fusion_pipeline_15(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = (
        _sanitize_text(payload.get("pipeline_task_identifier")) or _sanitize_text(payload.get("task_identifier"))
    )
    if not pipeline_task_identifier:
        pipeline_task_identifier = insert_cleaning_fusion_task(
            task_name=_sanitize_text(payload.get("task_name")) or "评论情感与主题挖掘前置清洗",
            task_kind="sentiment_topic_preparation",
            source_scope=_sanitize_text(payload.get("source_scope")) or "review_sources",
        )
    ingestion_batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers", [])))
    )
    if not ingestion_batch_identifiers:
        ingestion_batch_identifiers = [
            row["batch_identifier"]
            for row in query_ingestion_records(None)
            if row.get("batch_identifier")
        ]
    execution_summary = execute_cleaning_fusion_pipeline(pipeline_task_identifier, ingestion_batch_identifiers)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "task_status": "succeeded",
        "aligned_record_count": execution_summary["aligned_record_count"],
        "removed_duplicate_count": execution_summary["removed_duplicate_count"],
        "stage_timings": execution_summary["stage_timings"],
        "finished_at": execution_summary["finished_at"],
    }


def run_task_cleaning_fusion_pipeline_18(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = (
        _sanitize_text(payload.get("pipeline_task_identifier")) or _sanitize_text(payload.get("task_identifier"))
    )
    if not pipeline_task_identifier:
        pipeline_task_identifier = insert_cleaning_fusion_task(
            task_name=_sanitize_text(payload.get("task_name")) or "RFM 分层画像前置清洗",
            task_kind="rfm_segmentation_preparation",
            source_scope=_sanitize_text(payload.get("source_scope")) or "transaction_sources",
        )
    ingestion_batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers", [])))
    )
    if not ingestion_batch_identifiers:
        ingestion_batch_identifiers = [
            row["batch_identifier"]
            for row in query_ingestion_records(None)
            if row.get("batch_identifier")
        ]
    execution_summary = execute_cleaning_fusion_pipeline(pipeline_task_identifier, ingestion_batch_identifiers)
    fusion_summary = execution_summary["fusion_summary"]
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "task_status": "succeeded",
        "distinct_consumer_count": fusion_summary["distinct_consumer"],
        "average_amount": fusion_summary["average_amount"],
        "top_merchants": fusion_summary["top_merchants"],
        "finished_at": execution_summary["finished_at"],
    }


def query_pipeline_task_overview(payload: Dict[str, Any]) -> Dict[str, Any]:
    task_status_filter = _sanitize_text(payload.get("task_status")) or None
    page_number = int(payload.get("page_number") or 1)
    page_size = int(payload.get("page_size") or 20)
    pipeline_task_rows = list_cleaning_fusion_tasks(
        task_status=task_status_filter,
        limit=page_size,
        offset=(page_number - 1) * page_size,
    )
    return {
        "page_number": page_number,
        "page_size": page_size,
        "task_rows": [
            {
                "pipeline_task_identifier": task_row.get("pipeline_task_identifier"),
                "task_name": task_row.get("task_name"),
                "task_kind": task_row.get("task_kind"),
                "task_status": task_row.get("task_status"),
                "progress_percent": task_row.get("progress_percent"),
                "current_stage": task_row.get("current_stage"),
                "processed_record_count": task_row.get("processed_record_count"),
                "started_at": task_row.get("started_at"),
                "finished_at": task_row.get("finished_at"),
            }
            for task_row in pipeline_task_rows
        ],
    }


def retry_pipeline_stage(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = _sanitize_text(payload.get("pipeline_task_identifier"))
    if not pipeline_task_identifier:
        raise ValueError("缺少 pipeline_task_identifier")
    pipeline_task = fetch_cleaning_fusion_task(pipeline_task_identifier)
    if pipeline_task is None:
        raise ValueError(f"清洗融合任务不存在: {pipeline_task_identifier}")
    if pipeline_task.get("task_status") == "running":
        raise RuntimeError(f"清洗融合任务正在执行: {pipeline_task_identifier}")
    ingestion_batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers", [])))
    )
    if not ingestion_batch_identifiers:
        ingestion_batch_identifiers = [
            row["batch_identifier"]
            for row in query_ingestion_records(None)
            if row.get("batch_identifier")
        ]
    execution_summary = execute_cleaning_fusion_pipeline(pipeline_task_identifier, ingestion_batch_identifiers)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "task_status": "succeeded",
        "aligned_record_count": execution_summary["aligned_record_count"],
        "finished_at": execution_summary["finished_at"],
    }
