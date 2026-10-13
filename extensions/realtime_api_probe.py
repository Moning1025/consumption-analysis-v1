# 说明书第 9 章【准实时 API 监测台】
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger("extensions.realtime_api_probe")


class RealtimeApiProbe:
    """实时接口探测采样器：后台线程周期性采样各接入源 API 状态并落库。"""

    def __init__(self, realtime_api_monitor_store: Any):
        self._store = realtime_api_monitor_store
        self._sampling_interval_seconds = 60
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start_sampling_loop(self, interval_seconds: int = 60) -> None:
        if self._thread and self._thread.is_alive():
            logger.info("sampling loop already running")
            return
        self._sampling_interval_seconds = max(1, int(interval_seconds))
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._sampling_worker,
            name="realtime_api_probe_loop",
            daemon=True,
        )
        self._thread.start()
        logger.info("realtime api probe sampling loop started interval=%ss", self._sampling_interval_seconds)

    def stop_sampling_loop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=5)
            logger.info("realtime api probe sampling loop stopped")
        self._thread = None

    def _sampling_worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.sample_all()
            except Exception as exc:  # noqa: BLE001
                logger.warning("sampling iteration failed: %s", exc)
            self._stop_event.wait(self._sampling_interval_seconds)

    def sample_all(self) -> List[int]:
        sources = self._list_active_sources()
        recorded_ids = []
        for source in sources:
            endpoint = source.get("endpoint_uri")
            if not endpoint:
                continue
            sample_id = self._probe_and_record(source)
            if sample_id is not None:
                recorded_ids.append(sample_id)
        return recorded_ids

    def _list_active_sources(self) -> List[Dict[str, Any]]:
        fetcher = getattr(self._store, "fetch_active_ingestion_sources", None)
        if callable(fetcher):
            try:
                return fetcher()
            except Exception as exc:  # noqa: BLE001
                logger.warning("fetch active sources failed: %s", exc)
                return []
        return []

    def _probe_and_record(self, source: Dict[str, Any]) -> Optional[int]:
        endpoint = source.get("endpoint_uri")
        started = time.perf_counter()
        status_code = 0
        try:
            import urllib.request
            request = urllib.request.Request(endpoint, method="GET", headers={"User-Agent": "realtime-api-probe"})
            with urllib.request.urlopen(request, timeout=10) as response:
                status_code = int(response.status)
                body = response.read(2000).decode("utf-8", errors="ignore")
        except Exception as exc:  # noqa: BLE001
            status_code = 0
            body = f"probe_error: {exc}"
        elapsed_ms = round((time.perf_counter() - started) * 1000.0, 4)
        sample_payload = {
            "endpoint_uri": endpoint,
            "source_name": source.get("source_name") or source.get("id"),
            "region_code": source.get("region_code") or "unknown",
            "latency_ms": elapsed_ms,
            "status_code": status_code,
            "response_sample": {"body": body[:200]},
            "sampled_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        recorder = getattr(self._store, "record_api_sample", None)
        if callable(recorder):
            return recorder(sample_payload)
        logger.warning("realtime_api_monitor_store 缺少 record_api_sample 方法")
        return None
