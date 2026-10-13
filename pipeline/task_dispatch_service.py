import logging
import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Tuple

from pipeline.cleaning_fusion_service import (
    run_task_cleaning_fusion_pipeline_9 as cleaning_fusion_task_9,
    run_task_cleaning_fusion_pipeline_15 as cleaning_fusion_task_15,
)
from pipeline.sentiment_topic_service import mine_sentiment_and_topics
from pipeline.rfm_segmentation_service import compute_rfm_segmentation
from pipeline.forecast_anomaly_service import detect_consumption_forecast_anomaly
from pipeline.spatial_hotspot_service import aggregate_spatial_hotspot
from pipeline.recommendation_ranking_service import rank_personalized_recommendation
from pipeline.ingestion_intake_service import register_ingestion_source

task_logger = logging.getLogger("pipeline.task_dispatch_service")

_TASK_DISPATCH_TABLE: Dict[str, Tuple[str, Callable[[Mapping[str, Any]], Mapping[str, Any]]]] = {
    "run_task_cleaning_fusion_pipeline_5": ("consumption_forecast_anomaly", detect_consumption_forecast_anomaly),
    "run_task_cleaning_fusion_pipeline_9": ("cleaning_fusion_pipeline", cleaning_fusion_task_9),
    "run_task_cleaning_fusion_pipeline_15": ("cleaning_fusion_pipeline", cleaning_fusion_task_15),
    "run_task_cleaning_fusion_pipeline_18": ("sentiment_topic_mining", mine_sentiment_and_topics),
    "run_task_cleaning_fusion_pipeline_21": ("rfm_segmentation_studio", compute_rfm_segmentation),
    "run_task_cleaning_fusion_pipeline_24": ("spatial_hotspot_map", aggregate_spatial_hotspot),
    "run_task_data_ingestion_console_1": ("data_ingestion_console", register_ingestion_source),
    "run_task_recommendation_workbench_1": ("recommendation_workbench", rank_personalized_recommendation),
}

_TASK_PAGE_BINDING: Mapping[str, str] = {
    "run_task_cleaning_fusion_pipeline_5": "cleaning_fusion_pipeline",
    "run_task_cleaning_fusion_pipeline_9": "cleaning_fusion_pipeline",
    "run_task_cleaning_fusion_pipeline_15": "cleaning_fusion_pipeline",
    "run_task_cleaning_fusion_pipeline_18": "cleaning_fusion_pipeline",
    "run_task_cleaning_fusion_pipeline_21": "cleaning_fusion_pipeline",
    "run_task_cleaning_fusion_pipeline_24": "cleaning_fusion_pipeline",
    "run_task_data_ingestion_console_1": "data_ingestion_console",
    "run_task_recommendation_workbench_1": "recommendation_workbench",
}

_REQUIRED_PAYLOAD_KEYS: Mapping[str, frozenset] = {
    "run_task_cleaning_fusion_pipeline_5": frozenset({"pipeline_id", "forecast_window_days"}),
    "run_task_cleaning_fusion_pipeline_9": frozenset({"pipeline_id"}),
    "run_task_cleaning_fusion_pipeline_15": frozenset({"pipeline_id"}),
    "run_task_cleaning_fusion_pipeline_18": frozenset({"pipeline_id", "sentiment_model", "topic_count", "topic_model"}),
    "run_task_cleaning_fusion_pipeline_21": frozenset({"pipeline_id", "recency_days", "frequency_bins", "monetary_bins"}),
    "run_task_cleaning_fusion_pipeline_24": frozenset({"pipeline_id", "grid_precision"}),
    "run_task_data_ingestion_console_1": frozenset({"source_name", "source_type", "endpoint_uri"}),
    "run_task_recommendation_workbench_1": frozenset({"segment_label", "top_n"}),
}


class TaskDispatchError(RuntimeError):
    def __init__(self, task_identifier: str, reason: str) -> None:
        super().__init__(f"task_dispatch_failed:{task_identifier}:{reason}")
        self.task_identifier = task_identifier
        self.reason = reason


def _resolve_dispatch_entry(task_identifier: str) -> Tuple[str, Callable[[Mapping[str, Any]], Mapping[str, Any]]]:
    entry = _TASK_DISPATCH_TABLE.get(task_identifier)
    if entry is None:
        raise TaskDispatchError(task_identifier, "unknown_task_identifier")
    return entry


def _validate_payload(task_identifier: str, task_payload: Mapping[str, Any]) -> None:
    if not isinstance(task_payload, Mapping):
        raise TaskDispatchError(task_identifier, "payload_not_mapping")
    required_keys = _REQUIRED_PAYLOAD_KEYS.get(task_identifier, frozenset())
    missing_keys = tuple(
        key for key in required_keys
        if key not in task_payload or task_payload[key] in (None, "")
    )
    if missing_keys:
        raise TaskDispatchError(task_identifier, "missing_fields:" + ",".join(missing_keys))


def _normalize_payload(task_payload: Mapping[str, Any]) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for field_key, field_value in task_payload.items():
        if isinstance(field_value, str):
            normalized[field_key] = field_value.strip()
        else:
            normalized[field_key] = field_value
    return normalized


def _iter_dispatch_trace(task_identifier: str, page_identifier: str, operator_account: str) -> Iterable[Tuple[str, str]]:
    yield ("task_identifier", task_identifier)
    yield ("page_identifier", page_identifier)
    yield ("operator_account", operator_account)


def dispatch_page_task(task_identifier: str, task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    started_at = time.perf_counter()
    page_identifier = _TASK_PAGE_BINDING.get(task_identifier, "unknown_page")
    for trace_key, trace_value in _iter_dispatch_trace(task_identifier, page_identifier, operator_account):
        task_logger.debug("dispatch_trace %s=%s", trace_key, trace_value)
    _validate_payload(task_identifier, task_payload)
    normalized_payload = _normalize_payload(task_payload)
    target_table, task_handler = _resolve_dispatch_entry(task_identifier)
    try:
        handler_output = task_handler(normalized_payload)
    except TaskDispatchError:
        raise
    except Exception as handler_failure:
        task_logger.exception("task_handler_failed task=%s table=%s", task_identifier, target_table)
        raise TaskDispatchError(task_identifier, f"handler_exception:{handler_failure.__class__.__name__}") from handler_failure
    elapsed_ms = round((time.perf_counter() - started_at) * 1000.0, 3)
    dispatched_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    task_logger.info(
        "task_dispatched task=%s table=%s operator=%s elapsed_ms=%s",
        task_identifier,
        target_table,
        operator_account,
        elapsed_ms,
    )
    return {
        "task_identifier": task_identifier,
        "page_identifier": page_identifier,
        "target_table": target_table,
        "operator_account": operator_account,
        "dispatched_at": dispatched_at,
        "elapsed_ms": elapsed_ms,
        "handler_output": dict(handler_output) if isinstance(handler_output, Mapping) else {"value": handler_output},
    }


def run_task_cleaning_fusion_pipeline_5(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_5", task_payload, operator_account)


def run_task_cleaning_fusion_pipeline_9(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_9", task_payload, operator_account)


def run_task_cleaning_fusion_pipeline_15(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_15", task_payload, operator_account)


def run_task_cleaning_fusion_pipeline_18(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_18", task_payload, operator_account)


def run_task_cleaning_fusion_pipeline_21(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_21", task_payload, operator_account)


def run_task_cleaning_fusion_pipeline_24(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_cleaning_fusion_pipeline_24", task_payload, operator_account)


def run_task_data_ingestion_console_1(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_data_ingestion_console_1", task_payload, operator_account)


def run_task_recommendation_workbench_1(task_payload: Mapping[str, Any], operator_account: str = "anonymous") -> Dict[str, Any]:
    return dispatch_page_task("run_task_recommendation_workbench_1", task_payload, operator_account)


def list_registered_task_identifiers() -> Tuple[str, ...]:
    return tuple(sorted(_TASK_DISPATCH_TABLE.keys()))
