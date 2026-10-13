import json
import logging
import math
import sqlite3
import statistics
from datetime import datetime, timedelta
from typing import Generator

from store.consumption_forecast_store import (
    fetch_forecast_window_records,
    insert_forecast_window,
    insert_anomaly_event,
    fetch_anomaly_events_by_window,
    update_forecast_window_status,
)
from store.data_ingestion_console_store import fetch_active_ingestion_sources
from store.audit_log_store import record_operation_audit

logger = logging.getLogger("pipeline.forecast_anomaly")

SUPPORTED_MODEL_TYPES = frozenset({"moving_average", "exponential_smoothing", "linear_trend"})
DEFAULT_ANOMALY_THRESHOLD = 2.5
MIN_WINDOW_DAYS = 3
MAX_WINDOW_DAYS = 180


def _parse_window_days(prediction_window: str) -> int:
    normalized = (prediction_window or "").strip().lower()
    mapping = {
        "7d": 7, "14d": 14, "30d": 30, "60d": 60, "90d": 90,
        "7天": 7, "14天": 14, "30天": 30, "60天": 60, "90天": 90,
    }
    if normalized in mapping:
        return mapping[normalized]
    if normalized.endswith("d") and normalized[:-1].isdigit():
        return int(normalized[:-1])
    if normalized.endswith("天") and normalized[:-1].isdigit():
        return int(normalized[:-1])
    if normalized.isdigit():
        return int(normalized)
    raise ValueError(f"无法解析预测时间窗口: {prediction_window}")


def _validate_model_type(model_type: str) -> str:
    candidate = (model_type or "moving_average").strip().lower()
    if candidate not in SUPPORTED_MODEL_TYPES:
        raise ValueError(f"不支持的预测模型类型: {model_type}")
    return candidate


def _validate_anomaly_threshold(raw_threshold) -> float:
    try:
        threshold = float(raw_threshold)
    except (TypeError, ValueError):
        raise ValueError(f"异常检测阈值必须为数值: {raw_threshold}")
    if not (0.5 <= threshold <= 6.0):
        raise ValueError(f"异常检测阈值超出合理范围[0.5, 6.0]: {threshold}")
    return threshold


def _load_daily_consumption_series(region_code: str, window_days: int) -> list[tuple[str, float]]:
    lookback_days = window_days * 3
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=lookback_days)
    raw_rows = fetch_forecast_window_records(
        region_code=region_code,
        start_date=start_date.isoformat(),
        end_date=end_date.isoformat(),
    )
    daily_bucket: dict[str, float] = {}
    for record in raw_rows:
        occurred = record.get("occurred_date") or record.get("occurred_at", "")[:10]
        amount = float(record.get("consumption_amount") or 0.0)
        if not occurred:
            continue
        daily_bucket[occurred] = daily_bucket.get(occurred, 0.0) + amount
    ordered = sorted(daily_bucket.items(), key=lambda pair: pair[0])
    return ordered


def _moving_average_forecast(series: list[float], horizon: int, window: int = 7) -> list[float]:
    if not series:
        return [0.0] * horizon
    effective_window = max(1, min(window, len(series)))
    seed = sum(series[-effective_window:]) / effective_window
    return [round(seed, 4) for _ in range(horizon)]


def _exponential_smoothing_forecast(series: list[float], horizon: int, alpha: float = 0.35) -> list[float]:
    if not series:
        return [0.0] * horizon
    smoothed = series[0]
    for value in series[1:]:
        smoothed = alpha * value + (1 - alpha) * smoothed
    return [round(smoothed, 4) for _ in range(horizon)]


def _linear_trend_forecast(series: list[float], horizon: int) -> list[float]:
    if len(series) < 2:
        return [round(series[0] if series else 0.0, 4) for _ in range(horizon)]
    indices = list(range(len(series)))
    mean_x = sum(indices) / len(indices)
    mean_y = sum(series) / len(series)
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(indices, series))
    denominator = sum((x - mean_x) ** 2 for x in indices) or 1.0
    slope = numerator / denominator
    intercept = mean_y - slope * mean_x
    return [round(intercept + slope * (len(series) + step), 4) for step in range(horizon)]


def _dispatch_forecast_kernel(model_type: str, series: list[float], horizon: int) -> list[float]:
    if model_type == "moving_average":
        return _moving_average_forecast(series, horizon)
    if model_type == "exponential_smoothing":
        return _exponential_smoothing_forecast(series, horizon)
    return _linear_trend_forecast(series, horizon)


def _detect_anomaly_events(
    series: list[tuple[str, float]],
    threshold: float,
) -> Generator[dict, None, None]:
    if len(series) < 4:
        return
    values = [amount for _, amount in series]
    mean_value = statistics.fmean(values)
    stdev_value = statistics.pstdev(values) or 1e-6
    for occurred_date, amount in series:
        z_score = (amount - mean_value) / stdev_value
        if abs(z_score) >= threshold:
            yield {
                "occurred_date": occurred_date,
                "consumption_amount": round(amount, 4),
                "z_score": round(z_score, 4),
                "deviation_ratio": round((amount - mean_value) / mean_value, 4) if mean_value else 0.0,
                "anomaly_direction": "spike" if z_score > 0 else "drop",
            }


def _build_forecast_curve(series: list[tuple[str, float]], forecast_values: list[float]) -> list[dict]:
    last_date = datetime.fromisoformat(series[-1][0]).date() if series else datetime.now().date()
    curve = []
    for step, predicted in enumerate(forecast_values, start=1):
        projected_date = last_date + timedelta(days=step)
        curve.append({
            "forecast_date": projected_date.isoformat(),
            "predicted_amount": predicted,
            "step_index": step,
        })
    return curve


def _summarize_forecast(forecast_curve: list[dict], anomaly_events: list[dict]) -> dict:
    if not forecast_curve:
        return {"total_predicted": 0.0, "average_predicted": 0.0, "anomaly_count": len(anomaly_events)}
    predicted_values = [point["predicted_amount"] for point in forecast_curve]
    return {
        "total_predicted": round(sum(predicted_values), 4),
        "average_predicted": round(statistics.fmean(predicted_values), 4),
        "peak_predicted": round(max(predicted_values), 4),
        "trough_predicted": round(min(predicted_values), 4),
        "anomaly_count": len(anomaly_events),
    }


def _persist_forecast_artifact(
    region_code: str,
    model_type: str,
    window_days: int,
    threshold: float,
    forecast_curve: list[dict],
    anomaly_events: list[dict],
    summary: dict,
) -> int:
    window_payload = {
        "region_code": region_code,
        "model_type": model_type,
        "window_days": window_days,
        "anomaly_threshold": threshold,
        "forecast_curve_json": json.dumps(forecast_curve, ensure_ascii=False),
        "summary_json": json.dumps(summary, ensure_ascii=False),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "status": "completed",
    }
    window_id = insert_forecast_window(window_payload)
    for event in anomaly_events:
        insert_anomaly_event({
            "forecast_window_id": window_id,
            "region_code": region_code,
            "occurred_date": event["occurred_date"],
            "consumption_amount": event["consumption_amount"],
            "z_score": event["z_score"],
            "deviation_ratio": event["deviation_ratio"],
            "anomaly_direction": event["anomaly_direction"],
            "detected_at": datetime.now().isoformat(timespec="seconds"),
        })
    return window_id


def run_consumption_forecast_anomaly(payload: dict) -> dict:
    region_code = (payload.get("region_code") or payload.get("area_code") or "ALL").strip()
    prediction_window = payload.get("prediction_window") or payload.get("window") or "30d"
    model_type = _validate_model_type(payload.get("model_type") or payload.get("forecast_model"))
    anomaly_threshold = _validate_anomaly_threshold(payload.get("anomaly_threshold", DEFAULT_ANOMALY_THRESHOLD))
    window_days = _parse_window_days(prediction_window)
    if not (MIN_WINDOW_DAYS <= window_days <= MAX_WINDOW_DAYS):
        raise ValueError(f"预测时间窗口天数必须在[{MIN_WINDOW_DAYS}, {MAX_WINDOW_DAYS}]之间")

    consumption_series = _load_daily_consumption_series(region_code, window_days)
    if not consumption_series:
        logger.warning("区域 %s 无可用消费序列，返回空预测", region_code)
        empty_summary = {"total_predicted": 0.0, "average_predicted": 0.0, "anomaly_count": 0}
        window_id = _persist_forecast_artifact(
            region_code, model_type, window_days, anomaly_threshold, [], [], empty_summary
        )
        return {
            "forecast_window_id": window_id,
            "region_code": region_code,
            "model_type": model_type,
            "window_days": window_days,
            "anomaly_threshold": anomaly_threshold,
            "forecast_curve": [],
            "anomaly_events": [],
            "summary": empty_summary,
        }

    amounts = [amount for _, amount in consumption_series]
    forecast_values = _dispatch_forecast_kernel(model_type, amounts, window_days)
    forecast_curve = _build_forecast_curve(consumption_series, forecast_values)
    anomaly_events = list(_detect_anomaly_events(consumption_series, anomaly_threshold))
    summary = _summarize_forecast(forecast_curve, anomaly_events)
    window_id = _persist_forecast_artifact(
        region_code, model_type, window_days, anomaly_threshold,
        forecast_curve, anomaly_events, summary,
    )
    record_operation_audit(
        action_code="forecast_anomaly_run",
        target_table="consumption_forecast_anomaly",
        target_id=str(window_id),
        detail=f"region={region_code}:model={model_type}:window={window_days}:anomaly={len(anomaly_events)}",
    )
    logger.info(
        "消费预测完成 window_id=%s region=%s model=%s 异常事件=%d",
        window_id, region_code, model_type, len(anomaly_events),
    )
    return {
        "forecast_window_id": window_id,
        "region_code": region_code,
        "model_type": model_type,
        "window_days": window_days,
        "anomaly_threshold": anomaly_threshold,
        "forecast_curve": forecast_curve,
        "anomaly_events": anomaly_events,
        "summary": summary,
    }


def query_forecast_anomaly_events(payload: dict) -> dict:
    region_code = (payload.get("region_code") or "ALL").strip()
    window_id = payload.get("forecast_window_id")
    limit = int(payload.get("limit") or 200)
    if window_id is not None:
        events = fetch_anomaly_events_by_window(int(window_id))
    else:
        events = fetch_anomaly_events_by_window(None, region_code=region_code, limit=limit)
    return {
        "region_code": region_code,
        "forecast_window_id": window_id,
        "event_count": len(events),
        "events": events,
    }


def run_task_consumption_forecast_anomaly(payload: dict) -> dict:
    try:
        return run_consumption_forecast_anomaly(payload or {})
    except ValueError as validation_error:
        logger.warning("预测参数校验失败: %s", validation_error)
        return {"status": "rejected", "reason": str(validation_error)}
    except sqlite3.Error as persistence_error:
        logger.exception("预测结果落库失败")
        return {"status": "failed", "reason": f"持久化异常: {persistence_error}"}


def run_task_cleaning_fusion_pipeline_5(payload: dict) -> dict:
    return run_task_consumption_forecast_anomaly(payload)


def run_task_cleaning_fusion_pipeline_9(payload: dict) -> dict:
    return run_task_consumption_forecast_anomaly(payload)


def run_task_cleaning_fusion_pipeline_15(payload: dict) -> dict:
    return run_task_consumption_forecast_anomaly(payload)


def run_task_cleaning_fusion_pipeline_18(payload: dict) -> dict:
    return run_task_consumption_forecast_anomaly(payload)


def list_active_forecast_regions() -> list[dict]:
    sources = fetch_active_ingestion_sources()
    return [
        {"region_code": source.get("region_code"), "source_name": source.get("source_name")}
        for source in sources
        if source.get("region_code")
    ]


def mark_forecast_window_archived(forecast_window_id: int) -> dict:
    update_forecast_window_status(int(forecast_window_id), "archived")
    record_operation_audit(
        action_code="forecast_window_archive",
        target_table="consumption_forecast_anomaly",
        target_id=str(forecast_window_id),
        detail="archived by operator",
    )
    return {"forecast_window_id": int(forecast_window_id), "status": "archived"}


# 兼容别名：task_dispatch_service 的派发表以 detect_consumption_forecast_anomaly
# 作为入口，语义即"执行消费预测与异常检测"，等价于 run_consumption_forecast_anomaly。
detect_consumption_forecast_anomaly = run_consumption_forecast_anomaly
