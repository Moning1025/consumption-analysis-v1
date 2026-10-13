# 说明书第 10 章【消费分析可视化大屏】
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("extensions.echarts_dashboard_feed")


class EchartsDashboardFeed:
    """ECharts 仪表盘数据供给器：从各 store 聚合前端图表所需的数据集。"""

    def __init__(
        self,
        rfm_segmentation_store: Any,
        consumption_forecast_store: Any,
        spatial_hotspot_store: Any,
        sentiment_topic_mining_store: Any,
    ):
        self._rfm_store = rfm_segmentation_store
        self._forecast_store = consumption_forecast_store
        self._hotspot_store = spatial_hotspot_store
        self._sentiment_store = sentiment_topic_mining_store

    def build_dashboard_payload(self, region_code: str = "ALL") -> Dict[str, Any]:
        payload = {
            "region_code": region_code,
            "rfm": self._rfm_snapshot(),
            "forecast": self._forecast_snapshot(region_code),
            "hotspot": self._hotspot_snapshot(),
            "sentiment": self._sentiment_snapshot(),
            "generated_at": self._now_iso(),
        }
        logger.info("dashboard payload built region=%s", region_code)
        return payload

    def _rfm_snapshot(self) -> Dict[str, Any]:
        fetcher = getattr(self._rfm_store, "fetch_latest_segmentation_batch", None)
        if callable(fetcher):
            try:
                return fetcher()
            except Exception as exc:  # noqa: BLE001
                logger.warning("rfm snapshot unavailable: %s", exc)
                return {}
        return {}

    def _forecast_snapshot(self, region_code: str) -> Dict[str, Any]:
        fetcher = getattr(self._forecast_store, "fetch_forecast_window_records", None)
        if callable(fetcher):
            try:
                rows = fetcher(region_code=region_code, start_date="1970-01-01", end_date="2999-12-31")
                return {"window_records": list(rows) if rows else []}
            except Exception as exc:  # noqa: BLE001
                logger.warning("forecast snapshot unavailable: %s", exc)
                return {}
        return {}

    def _hotspot_snapshot(self) -> Dict[str, Any]:
        aggregator = getattr(self._hotspot_store, "aggregate_region_distribution", None)
        if callable(aggregator):
            try:
                return {"region_distribution": list(aggregator("medium"))}
            except Exception as exc:  # noqa: BLE001
                logger.warning("hotspot snapshot unavailable: %s", exc)
                return {}
        return {}

    def _sentiment_snapshot(self) -> Dict[str, Any]:
        lister = getattr(self._sentiment_store, "list_sentiment_topic_batches", None)
        if callable(lister):
            try:
                return {"batches": list(lister(5))}
            except Exception as exc:  # noqa: BLE001
                logger.warning("sentiment snapshot unavailable: %s", exc)
                return {}
        return {}

    def _now_iso(self) -> str:
        from datetime import datetime, timezone
        return datetime.now(timezone.utc).isoformat(timespec="seconds")

    def to_json(self, region_code: str = "ALL") -> str:
        return json.dumps(self.build_dashboard_payload(region_code), ensure_ascii=False)
