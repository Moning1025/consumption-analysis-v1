"""后台任务运行器（S1 任务基础设施）。

为什么要有这一层（答辩可直接讲）：

1. pywebview 的 js_api 调用是在同一个线程里同步执行的。
   老代码里清洗任务直接跑在调用线程上，前端点一下按钮，整个窗口就卡住，
   进度条只能靠 bridge 里 time.sleep 硬编。真正的做法是：
   调接口 → 立刻返回 job_id → 重活丢到后台线程池 → 前端按 job_id 轮询。

2. 状态与日志落库（job_run / job_event 两张表），不是放在进程内存里。
   这样窗口关掉、进程重启，历史任务和日志还在，答辩时能翻出真实执行记录。

3. 取消是“协作式”的：用户点取消只置 cancel_requested=1，
   任务自己在阶段边界调 ctx.check_cancel() 决定何时停，
   不会强杀线程写坏数据（SQLite 单写者的经典坑）。

任务类型注册表是开放的：任何模块只要 import 并调用 register_job_handler，
就能把自己的业务函数挂成一个可轮询的后台任务，不需要改这一层。
"""

from __future__ import annotations

import logging
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, Optional

from store import job_store

logger = logging.getLogger("pipeline.job_runner")


class JobCanceled(Exception):
    """任务在阶段边界检查到 cancel_requested 时抛出，由运行器统一落“已取消”。"""


# ------------------------------------------------------------------ 注册表

JobHandler = Callable[["JobContext", Dict[str, Any]], Any]

_JOB_HANDLERS: Dict[str, JobHandler] = {}
_REGISTRY_LOCK = threading.Lock()

#: 任务结束回调（main.py 用它刷新前端），签名 (job_id, status)
_FINISHED_CALLBACK: Optional[Callable[[str, str], None]] = None

_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="job-worker")


def register_job_handler(job_type: str, handler: JobHandler) -> None:
    """注册一个任务类型。job_type 建议用“域.动作”形式，如 ingestion.parse。"""
    if not job_type or not callable(handler):
        raise ValueError("register_job_handler 需要合法的 job_type 与可调用对象")
    with _REGISTRY_LOCK:
        _JOB_HANDLERS[job_type] = handler
    logger.debug("已注册任务类型 %s", job_type)


def list_job_types() -> list:
    with _REGISTRY_LOCK:
        return sorted(_JOB_HANDLERS)


def set_finished_callback(callback: Optional[Callable[[str, str], None]]) -> None:
    global _FINISHED_CALLBACK
    _FINISHED_CALLBACK = callback


# ------------------------------------------------------------------ 执行上下文


class JobContext:
    """交给业务函数的“任务手柄”：上报进度、写日志、检查取消。"""

    #: 两次写库之间至少间隔多少秒（避免高频上报把 SQLite 写锁占满）
    MIN_WRITE_INTERVAL_SECONDS = 0.2

    def __init__(self, job_id: str, params: Dict[str, Any]):
        self.job_id = job_id
        self.params = params or {}
        self._started_at = time.monotonic()
        self._last_write_at = 0.0
        self._last_progress: Optional[int] = None
        self._last_stage: Optional[str] = None
        self._lock = threading.Lock()

    # -- 工具 ------------------------------------------------------
    @property
    def elapsed_ms(self) -> int:
        return int((time.monotonic() - self._started_at) * 1000)

    def _should_write(self, progress: Optional[int], stage: Optional[str]) -> bool:
        now = time.monotonic()
        if stage is not None and stage != self._last_stage:
            return True
        if progress is not None and self._last_progress is None:
            return True
        if progress is not None and progress != self._last_progress:
            return True
        return (now - self._last_write_at) >= self.MIN_WRITE_INTERVAL_SECONDS

    # -- 上报 ------------------------------------------------------
    def report(
        self,
        *,
        progress: Optional[int] = None,
        stage: Optional[str] = None,
        stage_label: Optional[str] = None,
        message: Optional[str] = None,
    ) -> None:
        """更新 job_run 里的快照。前端轮询的就是这里写进去的值。"""
        with self._lock:
            if not self._should_write(progress, stage):
                return
            job_store.update_progress(
                self.job_id,
                progress=progress,
                stage=stage,
                stage_label=stage_label,
                message=message,
            )
            self._last_write_at = time.monotonic()
            if progress is not None:
                self._last_progress = progress
            if stage is not None:
                self._last_stage = stage

    def log(
        self,
        message: str,
        level: str = job_store.LEVEL_INFO,
        stage: Optional[str] = None,
        payload: Any = None,
    ) -> int:
        """追加一条过程日志（前端按 seq 增量拉取）。"""
        return job_store.append_event(
            self.job_id, message=message, level=level, stage=stage, payload=payload
        )

    def check_cancel(self) -> None:
        """在阶段边界调用；用户点过取消就抛 JobCanceled，让任务干净地停下。"""
        if job_store.is_cancel_requested(self.job_id):
            raise JobCanceled("用户请求取消任务")


# ------------------------------------------------------------------ 提交与执行


def submit_job(
    job_type: str,
    params: Optional[Dict[str, Any]] = None,
    operator_name: Optional[str] = None,
    page_identifier: Optional[str] = None,
    source_kind: str = "ui",
) -> str:
    """登记任务并丢进后台线程池，立刻返回 job_id（毫秒级，不阻塞 UI）。"""
    with _REGISTRY_LOCK:
        handler = _JOB_HANDLERS.get(job_type)
    if handler is None:
        raise KeyError(f"未注册的任务类型：{job_type}（已注册：{list_job_types()}）")

    payload = dict(params or {})
    job_id = job_store.create_job(
        job_type=job_type,
        params=payload,
        operator_name=operator_name,
        page_identifier=page_identifier,
        source_kind=source_kind,
    )
    _EXECUTOR.submit(_run_job, job_id, job_type, payload, handler)
    logger.info("任务已提交 job_id=%s type=%s", job_id, job_type)
    return job_id


def _run_job(job_id: str, job_type: str, params: Dict[str, Any], handler: JobHandler) -> None:
    """后台线程入口：状态机 running → succeeded / failed / canceled。"""
    started = time.monotonic()
    context = JobContext(job_id, params)
    status = job_store.STATUS_FAILED
    try:
        job_store.mark_running(job_id)
        context.log(f"任务开始：{job_type}", stage="start")
        result = handler(context, params)
        elapsed_ms = int((time.monotonic() - started) * 1000)
        job_store.mark_succeeded(
            job_id, result or {}, message=f"任务完成，耗时 {elapsed_ms} 毫秒"
        )
        context.log(f"任务完成，耗时 {elapsed_ms} 毫秒", stage="done", payload=result or None)
        status = job_store.STATUS_SUCCEEDED
    except JobCanceled:
        job_store.mark_canceled(job_id)
        context.log("任务已取消", level=job_store.LEVEL_WARN, stage="canceled")
        status = job_store.STATUS_CANCELED
    except Exception as exc:  # noqa: BLE001 - 后台任务必须兜住一切异常
        logger.exception("任务 %s 执行失败", job_id)
        reason = f"{type(exc).__name__}: {exc}"
        job_store.mark_failed(job_id, reason)
        context.log(
            f"任务失败：{exc}",
            level=job_store.LEVEL_ERROR,
            stage="failed",
            payload={"traceback": traceback.format_exc()[-2000:]},
        )
        status = job_store.STATUS_FAILED
    finally:
        callback = _FINISHED_CALLBACK
        if callback is not None:
            try:
                callback(job_id, status)
            except Exception:  # noqa: BLE001 - 回调失败不能影响任务收尾
                logger.warning("任务结束回调执行失败 job_id=%s", job_id, exc_info=True)


def shutdown(wait: bool = False) -> None:
    """测试与退出时用：停掉线程池。"""
    _EXECUTOR.shutdown(wait=wait)
