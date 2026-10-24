# -*- coding: utf-8 -*-
"""清洗融合流水线（S3）：ingestion_row 真实行 → 7 阶段 → consumption_record。

S3 相对旧版（5 阶段、读演示载荷、不落库、同步卡界面）的四处真改造：

1. **数据源换真**：从 ``data_ingestion_console.payload_json``（演示假数据）
   改为 ``ingestion_row``（S2 真实解析出来的原始行，parse_status='ok'）。
2. **阶段扩为 7 个**（设计文档 D1）：数据接入→去重清洗→缺失值处理→字段对齐→
   多源融合→质量校验→结果输出，进度按累计权重推进 15/40/55/70/85/95/100。
3. **真落库**：新增「结果输出」阶段，把去重后的消费事件写入
   ``consumption_record``（带 source_kind / batch_identifier / raw_row_id 溯源列）。
4. **后台异步**：``submit_job("cleaning.fusion")`` 丢进 job_runner 线程池，
   进度与日志写 ``job_run`` / ``job_event``，前端轮询，不再同步卡住 UI。
"""

import json
import logging
import re
import time
import unicodedata
from datetime import datetime
from decimal import Decimal, InvalidOperation
from difflib import SequenceMatcher
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Tuple

from pipeline.job_runner import JobCanceled, JobContext, register_job_handler, submit_job
from store.cleaning_fusion_pipeline_store import (
    fetch_cleaning_fusion_task,
    insert_cleaning_fusion_task,
    list_cleaning_fusion_tasks,
    update_task_status_and_progress,
)
from store.consumption_record_store import insert_records
from store.ingestion_row_store import iter_ok_rows, list_distinct_batches

logger = logging.getLogger("pipeline.cleaning_fusion_service")

JOB_TYPE_CLEANING_FUSION = "cleaning.fusion"

# ---- 七阶段定义（设计文档 D1：接入15/去重25/缺失15/对齐15/融合15/校验10/输出5） ----

PIPELINE_STAGE_SEQUENCE: Tuple[str, ...] = (
    "ingest",
    "deduplicate",
    "impute",
    "align",
    "fuse",
    "validate",
    "output",
)

STAGE_LABELS: Dict[str, str] = {
    "ingest": "数据接入",
    "deduplicate": "去重清洗",
    "impute": "缺失值处理",
    "align": "字段对齐",
    "fuse": "多源融合",
    "validate": "质量校验",
    "output": "结果输出",
}

#: 每个阶段**结束时**的累计进度（前端进度条按这个推进）
STAGE_PROGRESS_TARGET: Dict[str, int] = {
    "ingest": 15,
    "deduplicate": 40,
    "impute": 55,
    "align": 70,
    "fuse": 85,
    "validate": 95,
    "output": 100,
}

#: 各阶段的权重（展示用，加起来 100）
STAGE_PROGRESS_WEIGHT: Dict[str, int] = {
    "ingest": 15,
    "deduplicate": 25,
    "impute": 15,
    "align": 15,
    "fuse": 15,
    "validate": 10,
    "output": 5,
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

# 注意：file_adapter.py 也 import 这张别名表（列名归一复用同一份），改动需两边一起看。
FIELD_ALIAS_TABLE: Dict[str, Tuple[str, ...]] = {
    "merchant_name": ("shop_name", "store_name", "merchant", "商户名称", "门店"),
    "consumption_amount": ("amount", "total_fee", "pay_amount", "消费金额", "实付"),
    "consumption_time": ("trade_time", "order_time", "pay_time", "消费时间", "下单时间"),
    "user_identifier": (
        "user_id",
        "member_id",
        "customer_id",
        "用户标识",
        "会员号",
        "用户id",
        "会员id",
        "用户编号",
    ),
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


# ------------------------------------------------------------------ 基础工具

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


#: 大小写不敏感的字段别名索引（与 file_adapter 的 lookup_canonical 同逻辑；
#: 因为清洗流水线读的是 ingestion_row.raw_json = 原始表头键，必须在这边再解一遍别名）
_FIELD_ALIAS_LOOKUP: Dict[str, str] = {}
for _canonical_field, _alias_tuple in FIELD_ALIAS_TABLE.items():
    _FIELD_ALIAS_LOOKUP[_canonical_field.strip().lower()] = _canonical_field
    for _alias in _alias_tuple:
        _FIELD_ALIAS_LOOKUP[_alias.strip().lower()] = _canonical_field
#: 长别名优先做包含匹配，避免“消费金额”被更短的“金额”抢走之类的误配
_FIELD_ALIAS_KEYS_BY_LENGTH = sorted(_FIELD_ALIAS_LOOKUP, key=len, reverse=True)


def _normalize_field_key(raw_key: Any) -> str:
    return str(raw_key or "").strip().strip("\ufeff").lower()


def lookup_canonical_field(raw_key: Any) -> str:
    """把任意键名归一到规范字段名，认不出返回空串（精确优先，再长别名包含匹配）。"""
    key = _normalize_field_key(raw_key)
    if not key:
        return ""
    direct = _FIELD_ALIAS_LOOKUP.get(key)
    if direct:
        return direct
    for alias in _FIELD_ALIAS_KEYS_BY_LENGTH:
        if len(alias) >= 2 and alias in key:
            return _FIELD_ALIAS_LOOKUP[alias]
    return ""


def _resolve_field(record: Dict[str, Any], canonical_field: str) -> Any:
    if canonical_field in record and record[canonical_field] not in (None, ""):
        return record[canonical_field]
    for raw_key, value in record.items():  # 兼容各源异构字段名（含中文表头、大小写差异）
        if lookup_canonical_field(raw_key) == canonical_field and value not in (None, ""):
            return value
    return None


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------ 规范化

def normalize_ingestion_record(
    raw_record: Dict[str, Any], source_channel: str, *, impute_defaults: bool = True
) -> Optional[Dict[str, Any]]:
    """把一条原始行规范成清洗用中间记录。

    三个必填字段（商户/金额/时间）缺失任一 → 返回 None（在接入阶段被计数跳过）。
    impute_defaults=True 时顺手把可选字段补默认值（老行为）；
    流水线里先传 False，把“补默认值”留给「缺失值处理」阶段做，让它成为真实阶段。
    """
    merchant_name = _sanitize_text(_resolve_field(raw_record, "merchant_name"))
    if not merchant_name:
        return None
    consumption_amount = _coerce_decimal(_resolve_field(raw_record, "consumption_amount"))
    consumption_time = _coerce_timestamp(_resolve_field(raw_record, "consumption_time"))
    if consumption_amount is None or consumption_time is None:
        return None
    user_identifier = _sanitize_text(_resolve_field(raw_record, "user_identifier"))
    category_label = _sanitize_text(_resolve_field(raw_record, "category_label"))
    city_region = _sanitize_text(_resolve_field(raw_record, "city_region"))
    if impute_defaults:
        user_identifier = user_identifier or "anonymous_consumer"
        category_label = category_label or "未分类"
        city_region = city_region or "未知区域"
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


def _classify_skip_reason(raw_record: Dict[str, Any]) -> str:
    """规范化失败时给出中文原因（写进 job 日志，答辩能讲清楚）。"""
    if not _sanitize_text(_resolve_field(raw_record, "merchant_name")):
        return "缺少商户名称"
    if _coerce_decimal(_resolve_field(raw_record, "consumption_amount")) is None:
        return "金额缺失或非法"
    return "时间缺失或非法"


# ------------------------------------------------------------------ 去重

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
    """90 秒时间窗 + 商户相似度 + 金额容差 的多源去重。

    “同一用户”判定：仅当两条记录**都有** user_identifier 时才比较；
    未识别用户的空值不参与合并判断（两条匿名记录不该因字段缺失被误并）。
    """
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
            if retained_record["user_identifier"] and candidate_record["user_identifier"]:
                if retained_record["user_identifier"] != candidate_record["user_identifier"]:
                    continue
            duplicate_index = retained_index
            break
        if duplicate_index >= 0:
            removed_counter += 1
            continue
        retained_records.append(candidate_record)
    return retained_records, removed_counter


# ------------------------------------------------------------------ 缺失值处理

def impute_defaults(records: Sequence[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    """给可选字段补默认值，返回 (补好的记录, 各类补齐条数)。"""
    imputed_records: List[Dict[str, Any]] = []
    impute_counts = {"user_identifier": 0, "category_label": 0, "city_region": 0}
    defaults = {
        "user_identifier": "anonymous_consumer",
        "category_label": "未分类",
        "city_region": "未知区域",
    }
    for record in records:
        for field, default_value in defaults.items():
            if not record.get(field):
                record[field] = default_value
                impute_counts[field] += 1
        imputed_records.append(record)
    return imputed_records, impute_counts


# ------------------------------------------------------------------ 字段对齐

def align_records_to_canonical_schema(deduplicated_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """对齐到规范 9 字段，并把溯源键（_raw_row_id/_batch_identifier/_source_kind）带下去。"""
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
                "_raw_row_id": record.get("_raw_row_id"),
                "_batch_identifier": record.get("_batch_identifier"),
                "_source_kind": record.get("_source_kind"),
            }
        )
    return aligned_records


# ------------------------------------------------------------------ 多源融合

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


# ------------------------------------------------------------------ 质量校验

def validate_aligned_records(
    aligned_records: Sequence[Dict[str, Any]]
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """质量校验：剔除金额 ≤ 0 等坏账，统计字段覆盖率，产出校验报告。"""
    validated_records: List[Dict[str, Any]] = []
    dropped_reasons: Dict[str, int] = {}
    amount_values: List[Decimal] = []
    for record in aligned_records:
        try:
            amount_value = Decimal(record["consumption_amount"])
        except (InvalidOperation, ValueError, TypeError, KeyError):
            dropped_reasons["金额缺失"] = dropped_reasons.get("金额缺失", 0) + 1
            continue
        if amount_value <= 0:
            dropped_reasons["金额≤0"] = dropped_reasons.get("金额≤0", 0) + 1
            continue
        amount_values.append(amount_value)
        validated_records.append(record)

    coverage = {
        "user_identifier": sum(
            1 for record in validated_records
            if record.get("user_identifier") and record["user_identifier"] != "anonymous_consumer"
        ),
        "category_label": sum(
            1 for record in validated_records
            if record.get("category_label") and record["category_label"] != "未分类"
        ),
        "city_region": sum(
            1 for record in validated_records
            if record.get("city_region") and record["city_region"] != "未知区域"
        ),
    }
    return validated_records, {
        "valid_count": len(validated_records),
        "dropped_count": len(aligned_records) - len(validated_records),
        "dropped_reasons": dropped_reasons,
        "field_coverage": coverage,
        "amount_min": str(min(amount_values).quantize(Decimal("0.01"))) if amount_values else None,
        "amount_max": str(max(amount_values).quantize(Decimal("0.01"))) if amount_values else None,
    }


# ------------------------------------------------------------------ 结果输出

def _build_output_records(validated_records: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """把校验后的对齐记录映射成 consumption_record 行（含溯源列）。"""
    output_records: List[Dict[str, Any]] = []
    for record in validated_records:
        output_records.append(
            {
                # 文件源暂无行政区划码；统计局/高德等宏观源接入后再填 region_code
                "region_code": None,
                "merchant_name": record.get("merchant_name"),
                "user_identifier": record.get("user_identifier"),
                "category_label": record.get("category_label"),
                "city_region": record.get("city_region"),
                "consumption_amount": record.get("consumption_amount"),
                "occurred_date": record.get("consumption_date"),
                "occurred_at": record.get("consumption_time"),
                "source_kind": record.get("_source_kind"),
                "batch_identifier": record.get("_batch_identifier"),
                "raw_row_id": record.get("_raw_row_id"),
            }
        )
    return output_records


# ------------------------------------------------------------------ 任务表进度

def _advance_stage_progress(pipeline_task_identifier: str, stage_name: str, processed_count: int) -> None:
    """同步更新 cleaning_fusion_pipeline_task 表（任务队列里看得到）。"""
    update_task_status_and_progress(
        pipeline_task_identifier,
        "processing",
        progress_percent=STAGE_PROGRESS_TARGET.get(stage_name, 0),
        processed_record_count=processed_count,
        current_stage=stage_name,
    )


# ------------------------------------------------------------------ 七阶段主流程（后台线程执行）

def execute_cleaning_fusion_pipeline(
    context: JobContext,
    pipeline_task_identifier: str,
    ingestion_batch_identifiers: Sequence[str],
) -> Dict[str, Any]:
    """七阶段清洗融合主流程。进度与日志写 job_run/job_event，结果写 consumption_record。"""
    pipeline_task = fetch_cleaning_fusion_task(pipeline_task_identifier)
    if pipeline_task is None:
        raise ValueError(f"清洗融合任务不存在: {pipeline_task_identifier}")
    if pipeline_task.get("task_status") == "running":
        raise RuntimeError(f"清洗融合任务正在执行: {pipeline_task_identifier}")
    update_task_status_and_progress(
        pipeline_task_identifier,
        "running",
        started_at=_now_text(),
    )
    stage_timings: List[Dict[str, Any]] = []

    try:
        # ---- 1. 数据接入：流式读 ingestion_row 好行 → 规范化 -----------------
        stage_started = time.perf_counter()
        context.report(
            progress=0,
            stage="ingest",
            stage_label=STAGE_LABELS["ingest"],
            message="开始读取原始行",
        )
        source_rows = list(iter_ok_rows(list(ingestion_batch_identifiers)))
        total_source = len(source_rows)
        normalized_records: List[Dict[str, Any]] = []
        skip_reasons: Dict[str, int] = {}
        for index, source_row in enumerate(source_rows, 1):
            raw_record = source_row.get("raw")
            source_kind = str(source_row.get("source_kind") or "retail")
            if not isinstance(raw_record, dict) or not raw_record:
                reason = "原始载荷为空"
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                continue
            record = normalize_ingestion_record(raw_record, source_kind, impute_defaults=False)
            if record is None:
                reason = _classify_skip_reason(raw_record)
                skip_reasons[reason] = skip_reasons.get(reason, 0) + 1
                continue
            record["_raw_row_id"] = source_row.get("row_id")
            record["_batch_identifier"] = source_row.get("batch_identifier")
            record["_source_kind"] = source_kind
            normalized_records.append(record)
            if index % 50 == 0 or index == total_source:
                context.report(
                    progress=int(STAGE_PROGRESS_TARGET["ingest"] * index / max(total_source, 1)),
                    stage="ingest",
                    stage_label=STAGE_LABELS["ingest"],
                    message=f"读取原始行 {index}/{total_source}",
                )
        context.check_cancel()
        if len(normalized_records) > MAX_RECORDS_PER_BATCH:
            context.log(
                f"数据接入：规范化成功 {len(normalized_records)} 条，超过单批上限 {MAX_RECORDS_PER_BATCH}，截断处理",
                level="WARN",
                stage="ingest",
            )
            normalized_records = normalized_records[:MAX_RECORDS_PER_BATCH]
        context.log(
            f"数据接入：读取 {total_source} 行，规范化成功 {len(normalized_records)} 条",
            stage="ingest",
        )
        for reason, count in skip_reasons.items():
            context.log(f"数据接入：跳过 {count} 条（{reason}）", level="WARN", stage="ingest")
        _advance_stage_progress(pipeline_task_identifier, "ingest", len(normalized_records))
        stage_timings.append(
            {"stage": "ingest", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 2. 去重清洗 ----------------------------------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["deduplicate"],
            stage="deduplicate",
            stage_label=STAGE_LABELS["deduplicate"],
            message="多源去重（90 秒窗 + 商户相似度 + 金额容差）",
        )
        deduplicated_records, removed_duplicate_count = deduplicate_records(normalized_records)
        context.check_cancel()
        context.log(
            f"去重清洗：{len(normalized_records)} 条输入，移除疑似重复 {removed_duplicate_count} 条，保留 {len(deduplicated_records)} 条",
            stage="deduplicate",
        )
        _advance_stage_progress(pipeline_task_identifier, "deduplicate", len(deduplicated_records))
        stage_timings.append(
            {"stage": "deduplicate", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 3. 缺失值处理 --------------------------------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["impute"],
            stage="impute",
            stage_label=STAGE_LABELS["impute"],
            message="补齐可选字段默认值",
        )
        imputed_records, impute_counts = impute_defaults(deduplicated_records)
        context.check_cancel()
        total_imputed = sum(impute_counts.values())
        if total_imputed:
            context.log(
                f"缺失值处理：user_identifier 补 {impute_counts['user_identifier']} 条 / "
                f"category_label 补 {impute_counts['category_label']} 条 / "
                f"city_region 补 {impute_counts['city_region']} 条",
                level="WARN",
                stage="impute",
            )
        else:
            context.log("缺失值处理：可选字段完整，无需补齐", stage="impute")
        _advance_stage_progress(pipeline_task_identifier, "impute", len(imputed_records))
        stage_timings.append(
            {"stage": "impute", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 4. 字段对齐 ----------------------------------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["align"],
            stage="align",
            stage_label=STAGE_LABELS["align"],
            message="对齐到规范 9 字段",
        )
        aligned_records = align_records_to_canonical_schema(imputed_records)
        context.check_cancel()
        context.log(f"字段对齐：{len(aligned_records)} 条记录对齐到规范模式", stage="align")
        _advance_stage_progress(pipeline_task_identifier, "align", len(aligned_records))
        stage_timings.append(
            {"stage": "align", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 5. 多源融合 ----------------------------------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["fuse"],
            stage="fuse",
            stage_label=STAGE_LABELS["fuse"],
            message="聚合商户 / 品类 / 区域 / 时段分布",
        )
        fusion_summary = fuse_aligned_records(aligned_records)
        context.check_cancel()
        context.log(
            f"多源融合：聚合 {fusion_summary['record_count']} 条事件，"
            f"去重消费者 {fusion_summary['distinct_consumer']} 人，"
            f"总金额 {fusion_summary['total_amount']} 元",
            stage="fuse",
        )
        _advance_stage_progress(pipeline_task_identifier, "fuse", len(aligned_records))
        stage_timings.append(
            {"stage": "fuse", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 6. 质量校验 ----------------------------------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["validate"],
            stage="validate",
            stage_label=STAGE_LABELS["validate"],
            message="剔除金额异常坏账并统计覆盖率",
        )
        validated_records, validation_report = validate_aligned_records(aligned_records)
        context.check_cancel()
        if validation_report["dropped_count"]:
            context.log(
                f"质量校验：剔除 {validation_report['dropped_count']} 条异常记录"
                f"（{validation_report['dropped_reasons']}），保留 {validation_report['valid_count']} 条",
                level="WARN",
                stage="validate",
            )
        else:
            context.log(f"质量校验：{validation_report['valid_count']} 条全部通过", stage="validate")
        _advance_stage_progress(pipeline_task_identifier, "validate", validation_report["valid_count"])
        stage_timings.append(
            {"stage": "validate", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        # ---- 7. 结果输出：写 consumption_record -----------------------------
        stage_started = time.perf_counter()
        context.report(
            progress=STAGE_PROGRESS_TARGET["output"],
            stage="output",
            stage_label=STAGE_LABELS["output"],
            message="写入规范层 consumption_record",
        )
        output_records = _build_output_records(validated_records)
        inserted_count = insert_records(output_records) if output_records else 0
        context.check_cancel()
        context.log(
            f"结果输出：提交 {len(output_records)} 条到 consumption_record，"
            f"实际新增 {inserted_count} 条，重复跳过 {len(output_records) - inserted_count} 条",
            stage="output",
        )
        _advance_stage_progress(pipeline_task_identifier, "output", inserted_count)
        stage_timings.append(
            {"stage": "output", "elapsed_ms": int((time.perf_counter() - stage_started) * 1000)}
        )

        execution_summary = {
            "pipeline_task_identifier": pipeline_task_identifier,
            "ingestion_batch_identifiers": list(ingestion_batch_identifiers),
            "source_row_count": total_source,
            "normalized_record_count": len(normalized_records),
            "removed_duplicate_count": removed_duplicate_count,
            "impute_counts": impute_counts,
            "aligned_record_count": len(aligned_records),
            "fusion_summary": fusion_summary,
            "validation_report": validation_report,
            "output_record_count": inserted_count,
            "stage_timings": stage_timings,
            "finished_at": _now_text(),
        }
        update_task_status_and_progress(
            pipeline_task_identifier,
            "succeeded",
            finished_at=execution_summary["finished_at"],
            execution_summary=json.dumps(execution_summary, ensure_ascii=False),
            progress_percent=100,
        )
        logger.info(
            "清洗融合任务完成 task=%s 输入=%s 去重后=%s 入库=%s",
            pipeline_task_identifier,
            total_source,
            len(deduplicated_records),
            inserted_count,
        )
        return execution_summary
    except JobCanceled:
        update_task_status_and_progress(
            pipeline_task_identifier,
            "canceled",
            finished_at=_now_text(),
        )
        raise
    except Exception as pipeline_error:
        logger.exception("清洗融合任务失败 task=%s", pipeline_task_identifier)
        update_task_status_and_progress(
            pipeline_task_identifier,
            "failed",
            finished_at=_now_text(),
            execution_summary=json.dumps({"error": str(pipeline_error)}, ensure_ascii=False),
        )
        raise


def handle_cleaning_fusion(context: JobContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """job_runner 的 cleaning.fusion 任务入口。"""
    pipeline_task_identifier = params.get("pipeline_task_identifier")
    if not pipeline_task_identifier:
        raise ValueError("缺少 pipeline_task_identifier")
    ingestion_batch_identifiers = list(
        filter(None, params.get("ingestion_batch_identifiers") or [])
    )
    return execute_cleaning_fusion_pipeline(
        context, pipeline_task_identifier, ingestion_batch_identifiers
    )


# ------------------------------------------------------------------ 对外入口（异步提交）

def _resolve_task_identifier(
    payload: Dict[str, Any], default_name: str, task_kind: str, default_scope: str
) -> str:
    """优先复用前端传来的任务 id；id 不存在（前端静态演示行）时补建真实任务。"""
    identifier = (
        _sanitize_text(payload.get("pipeline_task_identifier"))
        or _sanitize_text(payload.get("task_identifier"))
        or _sanitize_text(payload.get("pipeline_id"))
    )
    if identifier and fetch_cleaning_fusion_task(identifier) is not None:
        return identifier
    return insert_cleaning_fusion_task(
        task_name=_sanitize_text(payload.get("task_name")) or default_name,
        task_kind=task_kind,
        source_scope=_sanitize_text(payload.get("source_scope")) or default_scope,
    )


def _resolve_batch_identifiers(payload: Dict[str, Any]) -> List[str]:
    batch_identifiers = list(
        filter(None, (_sanitize_text(identifier) for identifier in payload.get("ingestion_batch_identifiers") or []))
    )
    if not batch_identifiers:
        batch_identifiers = list(list_distinct_batches())
    return batch_identifiers


def _submit_cleaning_job(
    pipeline_task_identifier: str, batch_identifiers: List[str], payload: Dict[str, Any]
) -> str:
    return submit_job(
        JOB_TYPE_CLEANING_FUSION,
        params={
            "pipeline_task_identifier": pipeline_task_identifier,
            "ingestion_batch_identifiers": batch_identifiers,
        },
        operator_name=_sanitize_text(payload.get("operator")) or "admin",
        page_identifier="cleaning_fusion_pipeline",
        source_kind="ui",
    )


def run_task_cleaning_fusion_pipeline_5(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = _resolve_task_identifier(
        payload, "消费预测与异常检测前置清洗", "consumption_forecast_anomaly", "all_sources"
    )
    batch_identifiers = _resolve_batch_identifiers(payload)
    job_id = _submit_cleaning_job(pipeline_task_identifier, batch_identifiers, payload)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "job_id": job_id,
        "task_status": "running",
        "ingestion_batch_identifiers": batch_identifiers,
    }


def run_task_cleaning_fusion_pipeline_9(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = _resolve_task_identifier(
        payload, "消费分析可视化大数据融合", "consumption_dashboard_fusion", "all_sources"
    )
    batch_identifiers = _resolve_batch_identifiers(payload)
    job_id = _submit_cleaning_job(pipeline_task_identifier, batch_identifiers, payload)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "job_id": job_id,
        "task_status": "running",
        "ingestion_batch_identifiers": batch_identifiers,
    }


def run_task_cleaning_fusion_pipeline_15(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = _resolve_task_identifier(
        payload, "评论情感与主题挖掘前置清洗", "sentiment_topic_preparation", "review_sources"
    )
    batch_identifiers = _resolve_batch_identifiers(payload)
    job_id = _submit_cleaning_job(pipeline_task_identifier, batch_identifiers, payload)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "job_id": job_id,
        "task_status": "running",
        "ingestion_batch_identifiers": batch_identifiers,
    }


def run_task_cleaning_fusion_pipeline_18(payload: Dict[str, Any]) -> Dict[str, Any]:
    pipeline_task_identifier = _resolve_task_identifier(
        payload, "RFM 分层画像前置清洗", "rfm_segmentation_preparation", "transaction_sources"
    )
    batch_identifiers = _resolve_batch_identifiers(payload)
    job_id = _submit_cleaning_job(pipeline_task_identifier, batch_identifiers, payload)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "job_id": job_id,
        "task_status": "running",
        "ingestion_batch_identifiers": batch_identifiers,
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
    batch_identifiers = _resolve_batch_identifiers(payload)
    job_id = _submit_cleaning_job(pipeline_task_identifier, batch_identifiers, payload)
    return {
        "pipeline_task_identifier": pipeline_task_identifier,
        "job_id": job_id,
        "task_status": "running",
        "ingestion_batch_identifiers": batch_identifiers,
    }


# 导入即注册：cleaning.fusion 成为 job_runner 可调度的后台任务类型
register_job_handler(JOB_TYPE_CLEANING_FUSION, handle_cleaning_fusion)
