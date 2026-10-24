import logging
from typing import Any, Dict, Optional

from auth.access_guard_middleware import AccessGuardMiddleware
from auth.session_token_issuer import SessionTokenIssuer
from auth.role_policy_registry import PolicyDecision
from pipeline import ingestion_job  # noqa: F401  导入即注册 ingestion.parse / ingestion.register
from pipeline import job_runner
from store import job_store
from pipeline.task_dispatch_service import (
    dispatch_page_task,
    list_registered_task_identifiers,
    TaskDispatchError,
)

logger = logging.getLogger(__name__)

# 演示用消费记录样例（字段与清洗融合服务的 canonical 字段对齐）
DEMO_SAMPLES = {
    "餐饮订单流水": [
        {"merchant_name": "蜀香园火锅", "consumption_amount": 268.00, "consumption_time": "2026-10-01 12:30:00",
         "user_identifier": "u10001", "category_label": "餐饮", "city_region": "成都"},
        {"merchant_name": "天府老妈蹄花", "consumption_amount": 86.50, "consumption_time": "2026-10-02 19:15:00",
         "user_identifier": "u10002", "category_label": "餐饮", "city_region": "成都"},
        {"merchant_name": "陈麻婆豆腐", "consumption_amount": 45.00, "consumption_time": "2026-10-03 12:05:00",
         "user_identifier": "u10003", "category_label": "餐饮", "city_region": "眉山"},
        {"merchant_name": "蜜雪冰城", "consumption_amount": 12.00, "consumption_time": "2026-10-04 15:40:00",
         "user_identifier": "u10004", "category_label": "饮品", "city_region": "眉山"},
    ],
    "旅游门票消费": [
        {"merchant_name": "青城山景区", "consumption_amount": 120.00, "consumption_time": "2026-10-01 10:20:00",
         "user_identifier": "u20001", "category_label": "门票", "city_region": "成都"},
        {"merchant_name": "宽窄巷子文创店", "consumption_amount": 158.00, "consumption_time": "2026-10-02 16:00:00",
         "user_identifier": "u20002", "category_label": "文创", "city_region": "成都"},
        {"merchant_name": "峨眉山金顶索道", "consumption_amount": 135.00, "consumption_time": "2026-10-03 09:45:00",
         "user_identifier": "u20003", "category_label": "交通", "city_region": "乐山"},
        {"merchant_name": "三苏祠门票", "consumption_amount": 45.00, "consumption_time": "2026-10-04 14:30:00",
         "user_identifier": "u20004", "category_label": "门票", "city_region": "眉山"},
    ],
    "零售POS交易": [
        {"merchant_name": "永辉超市", "consumption_amount": 236.80, "consumption_time": "2026-10-01 18:20:00",
         "user_identifier": "u30001", "category_label": "商超", "city_region": "成都"},
        {"merchant_name": "红旗连锁", "consumption_amount": 58.60, "consumption_time": "2026-10-02 09:10:00",
         "user_identifier": "u30002", "category_label": "便利店", "city_region": "眉山"},
        {"merchant_name": "京东到家", "consumption_amount": 199.00, "consumption_time": "2026-10-03 20:05:00",
         "user_identifier": "u30003", "category_label": "线上零售", "city_region": "成都"},
        {"merchant_name": "优衣库", "consumption_amount": 399.00, "consumption_time": "2026-10-04 17:50:00",
         "user_identifier": "u30004", "category_label": "服饰", "city_region": "成都"},
    ],
}


class BridgeApi:
    """pywebview 桥接 API：为前端 JS 暴露业务入口，统一鉴权与任务分发。"""

    def __init__(
        self,
        connection_factory: Any,
        settings: Optional[Dict[str, Any]] = None,
        runtime_dependencies: Optional[Dict[str, Any]] = None,
    ):
        self._connection_factory = connection_factory
        self._settings = settings or {}
        self._dependencies = runtime_dependencies or {}
        self._window = None
        self._last_token = ""
        self._last_username = "admin"

        session_secret = self._settings.get("session_secret") or "local-dev-secret"
        token_ttl = int(self._settings.get("token_ttl_seconds") or 3600)
        self._session_token_issuer = SessionTokenIssuer(
            secret_key=session_secret,
            token_validity_seconds=token_ttl,
        )
        self._access_guard = AccessGuardMiddleware(
            session_token_issuer=self._session_token_issuer,
            role_policy_registry=self._dependencies.get("role_policy_registry"),
            audit_log_store=self._dependencies.get("audit_log_store"),
        )

    def bind_window(self, window: Any) -> None:
        self._window = window
        logger.info("bridge window bound")

    # ---- 会话与鉴权 ----
    def login(self, username: str, password: str, source_page: str = "login") -> Dict[str, Any]:
        account_store = self._dependencies.get("account_store")
        credential_vault = self._dependencies.get("credential_vault")
        login_audit_recorder = self._dependencies.get("login_audit_recorder")
        if account_store is None or credential_vault is None:
            return {"success": False, "reason": "runtime_dependencies_missing"}
        account = account_store.find_account_by_identifier(username)
        if account is None:
            self._record_login_failure(username, source_page, "account_not_found")
            return {"success": False, "reason": "invalid_credentials"}
        verified = credential_vault.verify_password(
            password, account.get("password_hash"), account.get("password_salt")
        )
        if not verified:
            self._record_login_failure(username, source_page, "wrong_password")
            return {"success": False, "reason": "invalid_credentials"}
        token = self._session_token_issuer.issue_token(
            account_id=account["account_id"],
            login_name=account["login_name"],
            role_code=account.get("role_code") or "consumer",
        )
        if login_audit_recorder is not None:
            login_audit_recorder.record_login_success(
                account_id=account["account_id"],
                actor_username=account["login_name"],
                actor_role=account.get("role_code") or "consumer",
                source_page=source_page,
                session_token_id=token,
            )

        self._last_token = token
        self._last_username = account["login_name"]

        return {"success": True, "token": token, "role": account.get("role_code"), "username": account["login_name"]}

    def _record_login_failure(self, username: str, source_page: str, reason: str) -> None:
        recorder = self._dependencies.get("login_audit_recorder")
        if recorder is not None:
            recorder.record_login_failure(
                actor_username=username or "anonymous",
                source_page=source_page,
                failure_reason=reason,
            )

    # ---- 任务分发（前端调用） ----
    def run_task(self, task_identifier: str, payload: Dict[str, Any], token: str = "") -> Dict[str, Any]:
        try:
            decision = self._authorize(task_identifier, token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("run_task authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "reason": decision.denial_reason, "permitted": False}
        try:
            result = dispatch_page_task(task_identifier, payload, operator_account=self._operator_from_token(token))
            return {"success": True, "permitted": True, "data": result}
        except TaskDispatchError as dispatch_error:
            logger.warning("task dispatch failed: %s", dispatch_error)
            return {"success": False, "permitted": True, "reason": str(dispatch_error)}
        except Exception as handler_error:  # noqa: BLE001
            logger.exception("task handler raised")
            return {"success": False, "permitted": True, "reason": f"handler_error: {handler_error}"}

    def list_tasks(self) -> Dict[str, Any]:
        return {"success": True, "task_identifiers": list(list_registered_task_identifiers())}

    def _authorize(self, bridge_method_name: str, token: str) -> PolicyDecision:
        return self._access_guard.authorize_bridge_method(
            self._session_from_token(token),
            bridge_method_name,
        )

    def _session_from_token(self, token: str) -> Optional[Dict[str, Any]]:
        if not token:
            return None
        return self._session_token_issuer.verify_token(token)

    def _operator_from_token(self, token: str) -> str:
        session = self._session_from_token(token)
        return (session or {}).get("login_name") or "anonymous"

    # ---- 简单门户查询 ----
    def ping(self) -> Dict[str, Any]:
        return {"success": True, "message": "client_bridge_ready", "method": "ping"}

    def get_settings(self) -> Dict[str, Any]:
        return {"session_secret": "***", "token_ttl_seconds": self._session_token_issuer.get_token_validity_seconds()}

    # ---- 多源数据接入台 ----
    def list_ingestion_records(self, limit: int = 100) -> Dict[str, Any]:
        """查询最近导入记录（data_ingestion_console 表，按时间倒序）。

        S1 起这里只做“读”，不再按 created_at 秒数推算假进度；
        真实进度由后台任务写进 job_run，前端轮询 get_job 获取。
        """
        from store.data_ingestion_console_store import query_ingestion_records

        try:
            return {"success": True, "records": query_ingestion_records(limit=int(limit or 100))}
        except Exception as query_error:  # noqa: BLE001
            logger.exception("list_ingestion_records failed")
            return {"success": False, "reason": f"query_error: {query_error}"}

    def delete_ingestion_records(self, record_ids, token: str = "") -> Dict[str, Any]:
        """批量删除导入记录（接入台写权限鉴权，一次鉴权批量删除）。"""
        from store.data_ingestion_console_store import delete_ingestion_records

        try:
            decision = self._authorize("run_task_data_ingestion_console_1", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("delete_ingestion_records authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        try:
            ids = [int(i) for i in (record_ids or [])]
            if not ids:
                return {"success": False, "permitted": True, "reason": "未选择任何记录"}
            deleted = delete_ingestion_records(ids)
            return {"success": True, "permitted": True, "deleted": deleted}
        except Exception as delete_error:  # noqa: BLE001
            logger.exception("delete_ingestion_records failed")
            return {"success": False, "permitted": True, "reason": f"delete_error: {delete_error}"}

    def get_ingestion_record_detail(self, record_id: int) -> Dict[str, Any]:
        """查询单条导入记录完整字段（含原始载荷）。"""
        from store.data_ingestion_console_store import get_ingestion_record

        try:
            row = get_ingestion_record(int(record_id))
            if row is None:
                return {"success": False, "reason": f"记录不存在: {record_id}"}
            return {"success": True, "record": row}
        except Exception as detail_error:  # noqa: BLE001
            logger.exception("get_ingestion_record_detail failed")
            return {"success": False, "reason": f"detail_error: {detail_error}"}

    def delete_ingestion_record(self, record_id: int, token: str = "") -> Dict[str, Any]:
        """删除一条导入记录（接入台写权限鉴权）。"""
        from store.data_ingestion_console_store import delete_ingestion_record

        try:
            decision = self._authorize("run_task_data_ingestion_console_1", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("delete_ingestion_record authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        try:
            deleted = delete_ingestion_record(int(record_id))
            if not deleted:
                return {"success": False, "permitted": True, "reason": f"记录不存在或已删除: {record_id}"}
            return {"success": True, "permitted": True, "deleted": int(record_id)}
        except Exception as delete_error:  # noqa: BLE001
            logger.exception("delete_ingestion_record failed")
            return {"success": False, "permitted": True, "reason": f"delete_error: {delete_error}"}

    def get_ingestion_overview(self) -> Dict[str, Any]:
        """接入台概览：总记录数、总条数、状态分布。"""
        from pipeline.ingestion_intake_service import get_ingestion_overview

        try:
            return {"success": True, "data": get_ingestion_overview()}
        except Exception as overview_error:  # noqa: BLE001
            logger.exception("get_ingestion_overview failed")
            return {"success": False, "reason": f"overview_error: {overview_error}"}

    def import_demo_batch(self, source_type: str, token: str = "", file_ext: str = "") -> Dict[str, Any]:
        """导入一批演示消费记录（餐饮/旅游/零售）。

        S1 起不再同步写库，而是提交 ingestion.register 后台任务：
        真实做格式校验 / 落库 / 状态推进，前端凭 job_id 轮询进度。
        """
        try:
            decision = self._authorize("run_task_data_ingestion_console_1", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("import_demo_batch authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "reason": decision.denial_reason, "permitted": False}

        samples = DEMO_SAMPLES.get(source_type or "", DEMO_SAMPLES["餐饮订单流水"])
        try:
            job_id = job_runner.submit_job(
                job_type=ingestion_job.JOB_TYPE_INGESTION_REGISTER,
                params={
                    "source_name": source_type or "餐饮订单流水",
                    "source_type": "manual",
                    "records": samples,
                    "operator": self._operator_from_token(token),
                    "region_code": "510000",
                    "file_ext": (file_ext or ""),
                },
                operator_name=self._operator_from_token(token),
                page_identifier="data_ingestion_console",
                source_kind="ui",
            )
            return {"success": True, "permitted": True, "job_id": job_id,
                    "record_count": len(samples)}
        except Exception as batch_error:  # noqa: BLE001
            logger.exception("import_demo_batch failed")
            return {"success": False, "permitted": True, "reason": f"batch_error: {batch_error}"}

    def register_source(
        self,
        source_name: str,
        source_type: str,
        endpoint_uri: str = "",
        token: str = "",
        status: str = "",
        progress=None,
        operator: str = "",
        term_count=None,
        payload: str = "",
        import_time: str = "",
        meta_primary: str = "",
        meta_secondary: str = "",
    ) -> Dict[str, Any]:
        """登记一个新的数据源（走 run_task_data_ingestion_console_1 鉴权与审计）。
        支持说明书新建记录表单的全部字段。"""
        return self.run_task(
            "run_task_data_ingestion_console_1",
            {
                "source_name": source_name,
                "source_type": source_type,
                "endpoint_uri": endpoint_uri,
                "status": status,
                "progress": progress,
                "operator": operator,
                "term_count": term_count,
                "payload_json": payload,
                "import_time": import_time,
                "meta_primary": meta_primary,
                "meta_secondary": meta_secondary,
            },
            token,
        )

    # ---- 真文件导入（拖拽 / 系统文件选择框） ----
    def on_js_drop(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """接收 JS 侧 drop 事件（pywebview 会自动把每个 file 的绝对路径注入为 pywebviewFullPath）。"""
        files = (event or {}).get("files") or []
        paths = []
        for f in files:
            if not isinstance(f, dict):
                continue
            p = f.get("pywebviewFullPath") or f.get("path")
            if p:
                paths.append(p)
        if not paths:
            return {"success": False, "reason": "未拿到文件路径（pywebview 未注入 pywebviewFullPath）"}
        return self.ingest_local_files(paths)

    def ingest_local_files(self, file_paths) -> Dict[str, Any]:
        """拖入/选中的本地文件 → 提交 ingestion.parse 后台任务，立刻返回 job_id。

        真正的解包、逐行统计、批次登记、状态推进都在 pipeline/ingestion_job.py 的
        后台线程里完成；这个函数只做“后缀/存在性”的快速过滤。
        """
        import os

        if not file_paths:
            return {"success": False, "reason": "no_files"}
        accepted_ext = ingestion_job.ALLOWED_EXTENSIONS
        accepted, failed = [], []
        for fp in file_paths:
            if not fp or not os.path.isfile(fp):
                failed.append({"path": str(fp), "reason": "文件不存在"})
                continue
            ext = os.path.splitext(fp)[1].lower()
            if ext not in accepted_ext:
                failed.append({"path": str(fp), "reason": f"不支持的后缀 {ext}"})
                continue
            accepted.append(str(fp))
        if not accepted:
            return {"success": False, "reason": "没有可接入的文件", "failed": failed}

        job_id = job_runner.submit_job(
            job_type=ingestion_job.JOB_TYPE_INGESTION_PARSE,
            params={
                "file_paths": accepted,
                "operator": self._last_username,
                "region_code": "510000",
            },
            operator_name=self._last_username,
            page_identifier="data_ingestion_console",
            source_kind="drop",
        )
        logger.info("ingest_local_files 已提交任务 job_id=%s files=%d", job_id, len(accepted))
        return {
            "success": True,
            "job_id": job_id,
            "accepted": [os.path.basename(path) for path in accepted],
            "failed": failed,
            "summary": f"已提交后台任务，共 {len(accepted)} 个文件正在接入",
        }

    def pick_files_and_ingest(self) -> Dict[str, Any]:
        """弹原生文件选择框，选完后走真文件登记。"""
        import tkinter as tk
        from tkinter import filedialog

        root = tk.Tk()
        root.withdraw()
        root.attributes("-topmost", True)
        paths = filedialog.askopenfilenames(
            title="选择要导入的数据文件",
            filetypes=[
                ("数据文件", "*.csv *.xlsx *.xls *.json *.zip"),
                ("CSV", "*.csv"),
                ("Excel", "*.xlsx *.xls"),
                ("JSON", "*.json"),
                ("压缩包", "*.zip"),
                ("全部", "*.*"),
            ],
        )
        root.destroy()
        if not paths:
            return {"success": False, "reason": "已取消选择"}
        return self.ingest_local_files(list(paths))

    # ---- 后台任务（S1）：提交 / 轮询 / 取消 ----
    _SUPPORTED_JOB_TYPES = frozenset({
        ingestion_job.JOB_TYPE_INGESTION_PARSE,
        ingestion_job.JOB_TYPE_INGESTION_REGISTER,
    })

    @staticmethod
    def _job_elapsed_ms(job: Dict[str, Any]) -> int:
        from datetime import datetime, timezone

        started = job.get("started_at")
        if not started:
            return 0
        try:
            begin = datetime.fromisoformat(str(started))
        except ValueError:
            return 0
        if begin.tzinfo is None:
            begin = begin.replace(tzinfo=timezone.utc)
        return max(0, int((datetime.now(timezone.utc) - begin).total_seconds() * 1000))

    def start_job(
        self,
        job_type: str,
        params: Optional[Dict[str, Any]] = None,
        token: str = "",
        page_identifier: str = "",
        source_kind: str = "ui",
    ) -> Dict[str, Any]:
        """提交一个后台任务，立刻返回 job_id（前端随后用 get_job 轮询）。"""
        try:
            decision = self._authorize("start_job", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("start_job authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        if job_type not in self._SUPPORTED_JOB_TYPES:
            return {"success": False, "permitted": True,
                    "reason": f"未开放的任务类型：{job_type}"}
        try:
            active = job_store.list_jobs(
                job_type=job_type, status=list(job_store.ACTIVE_STATUSES), limit=1
            )
            if active:
                return {"success": True, "permitted": True, "job_id": active[0]["job_id"],
                        "reused": True, "message": "同类任务正在运行，已复用该任务的进度"}
            job_id = job_runner.submit_job(
                job_type=job_type,
                params=dict(params or {}),
                operator_name=self._operator_from_token(token),
                page_identifier=page_identifier or "unknown",
                source_kind=source_kind,
            )
            return {"success": True, "permitted": True, "job_id": job_id}
        except Exception as submit_error:  # noqa: BLE001
            logger.exception("start_job submit failed")
            return {"success": False, "permitted": True, "reason": f"submit_error: {submit_error}"}

    def get_job(self, job_id: str, after_seq: int = 0, token: str = "") -> Dict[str, Any]:
        """轮询任务快照 + 增量日志（after_seq 之后的部分）。"""
        try:
            decision = self._authorize("get_job", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("get_job authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        try:
            job = job_store.fetch_job(str(job_id or ""))
            if not job:
                return {"success": False, "permitted": True, "reason": "任务不存在"}
            events = job_store.list_events(str(job_id), after_seq=int(after_seq or 0))
            snapshot = {
                key: job.get(key)
                for key in (
                    "job_id", "job_type", "status", "stage", "stage_label",
                    "progress", "message", "error_message", "result",
                    "created_at", "started_at", "finished_at",
                )
            }
            snapshot["progress"] = int(job.get("progress") or 0)
            snapshot["elapsed_ms"] = self._job_elapsed_ms(job)
            progress = snapshot["progress"]
            if snapshot["status"] == job_store.STATUS_RUNNING and 0 < progress < 100:
                snapshot["eta_ms"] = int(snapshot["elapsed_ms"] * (100 - progress) / progress)
            else:
                snapshot["eta_ms"] = 0
            return {"success": True, "permitted": True, "job": snapshot, "events": events}
        except Exception as query_error:  # noqa: BLE001
            logger.exception("get_job failed")
            return {"success": False, "permitted": True, "reason": f"query_error: {query_error}"}

    def list_jobs(self, job_type: str = "", limit: int = 20, token: str = "") -> Dict[str, Any]:
        """列出最近的后台任务（任务中心用）。"""
        try:
            decision = self._authorize("list_jobs", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("list_jobs authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        try:
            rows = job_store.list_jobs(
                job_type=(job_type or None), limit=int(limit or 20)
            )
            for row in rows:
                elapsed = self._job_elapsed_ms(row)
                progress = int(row.get("progress") or 0)
                row["elapsed_ms"] = elapsed
                if row.get("status") == job_store.STATUS_RUNNING and 0 < progress < 100:
                    row["eta_ms"] = int(elapsed * (100 - progress) / progress)
                else:
                    row["eta_ms"] = 0
            return {"success": True, "permitted": True, "jobs": rows}
        except Exception as query_error:  # noqa: BLE001
            logger.exception("list_jobs failed")
            return {"success": False, "permitted": True, "reason": f"query_error: {query_error}"}

    def cancel_job(self, job_id: str, token: str = "") -> Dict[str, Any]:
        """请求取消：只打标记，任务在下一个阶段边界自行停下。"""
        try:
            decision = self._authorize("cancel_job", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("cancel_job authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        try:
            ok = job_store.request_cancel(str(job_id or ""))
            return {"success": bool(ok), "permitted": True,
                    "message": "已请求取消，等待当前阶段结束" if ok else "任务已结束或不存在"}
        except Exception as cancel_error:  # noqa: BLE001
            logger.exception("cancel_job failed")
            return {"success": False, "permitted": True, "reason": f"cancel_error: {cancel_error}"}

    # ---- 原始数据层（S2）：把真正落库的行和校验报告读回来 ----
    def list_ingestion_rows(
        self,
        batch_identifier: str,
        status: str = "",
        limit: int = 100,
        offset: int = 0,
        token: str = "",
    ) -> Dict[str, Any]:
        """列出某个接入批次落库的原始行（原件、不改写），用于“看看导进来了什么”。"""
        from store import ingestion_row_store

        try:
            decision = self._authorize("list_ingestion_rows", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("list_ingestion_rows authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        batch = str(batch_identifier or "").strip()
        if not batch:
            return {"success": False, "permitted": True, "reason": "batch_identifier 不能为空"}
        try:
            rows = ingestion_row_store.list_rows(
                batch, status=status.strip(), limit=int(limit or 100), offset=int(offset or 0)
            )
            counts = ingestion_row_store.count_rows(batch)
            return {
                "success": True,
                "permitted": True,
                "batch_identifier": batch,
                "rows": rows,
                "counts": counts,
                "limit": int(limit or 100),
                "offset": int(offset or 0),
            }
        except Exception as query_error:  # noqa: BLE001
            logger.exception("list_ingestion_rows failed")
            return {"success": False, "permitted": True, "reason": f"query_error: {query_error}"}

    def get_ingestion_validation(self, batch_identifier: str, token: str = "") -> Dict[str, Any]:
        """取某个接入批次的格式校验报告（好行/坏行/重复行 + 明细）。"""
        from store import ingestion_row_store

        try:
            decision = self._authorize("get_ingestion_validation", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("get_ingestion_validation authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "permitted": False, "reason": decision.denial_reason}
        batch = str(batch_identifier or "").strip()
        if not batch:
            return {"success": False, "permitted": True, "reason": "batch_identifier 不能为空"}
        try:
            report = ingestion_row_store.fetch_validation(batch)
            if report is None:
                return {"success": False, "permitted": True,
                        "reason": "该批次还没有校验报告（可能仍在解析中）"}
            report["counts"] = ingestion_row_store.count_rows(batch)
            return {"success": True, "permitted": True, "validation": report}
        except Exception as query_error:  # noqa: BLE001
            logger.exception("get_ingestion_validation failed")
            return {"success": False, "permitted": True, "reason": f"query_error: {query_error}"}

    # ---- 清洗融合流水线 ----
    def list_cleaning_tasks(self, task_status: str = "", limit: int = 50) -> Dict[str, Any]:
        """查询清洗融合任务队列（支持按状态过滤）。"""
        from store.cleaning_fusion_pipeline_store import list_cleaning_fusion_tasks

        try:
            rows = list_cleaning_fusion_tasks(
                task_status=task_status.strip() or None,
                limit=int(limit or 50),
            )
            return {"success": True, "tasks": rows}
        except Exception as query_error:  # noqa: BLE001
            logger.exception("list_cleaning_tasks failed")
            return {"success": False, "reason": f"query_error: {query_error}"}

    def get_cleaning_task_detail(self, task_identifier: str) -> Dict[str, Any]:
        """查询单个清洗任务的完整详情（含执行摘要）。"""
        from store.cleaning_fusion_pipeline_store import fetch_cleaning_fusion_task

        try:
            row = fetch_cleaning_fusion_task(task_identifier)
            if row is None:
                return {"success": False, "reason": f"任务不存在: {task_identifier}"}
            return {"success": True, "task": row}
        except Exception as detail_error:  # noqa: BLE001
            logger.exception("get_cleaning_task_detail failed")
            return {"success": False, "reason": f"detail_error: {detail_error}"}

    def create_cleaning_task(self, task_name: str, task_kind: str = "consumption_dashboard_fusion",
                             source_scope: str = "all_sources", task_status: str = "pending",
                             progress=None, priority: str = "", submitter: str = "",
                             operator: str = "", payload: str = "", meta_primary: str = "") -> Dict[str, Any]:
        """新建一条清洗融合任务记录（支持说明书新建记录的全部字段）。"""
        from store.cleaning_fusion_pipeline_store import insert_cleaning_fusion_task

        try:
            meta_json = {}
            if priority:
                meta_json["priority"] = priority
            if submitter:
                meta_json["submitter"] = submitter
            if operator:
                meta_json["operator"] = operator
            if payload:
                meta_json["payload"] = payload
            if meta_primary:
                meta_json["meta_primary"] = meta_primary
            identifier = insert_cleaning_fusion_task(
                task_name=task_name.strip() or "清洗融合任务",
                task_kind=task_kind.strip() or "consumption_dashboard_fusion",
                source_scope=source_scope.strip() or "all_sources",
                task_status=(task_status.strip() or "pending"),
                progress_percent=progress,
                meta_json=meta_json or None,
            )
            return {"success": True, "task_identifier": identifier, "task_status": task_status}
        except Exception as create_error:  # noqa: BLE001
            logger.exception("create_cleaning_task failed")
            return {"success": False, "reason": f"create_error: {create_error}"}

    def delete_cleaning_task(self, task_identifier: str) -> Dict[str, Any]:
        """删除一条清洗融合任务记录。"""
        from store.cleaning_fusion_pipeline_store import delete_cleaning_fusion_task

        try:
            deleted = delete_cleaning_fusion_task(task_identifier)
            if not deleted:
                return {"success": False, "reason": f"任务不存在或已删除: {task_identifier}"}
            return {"success": True, "deleted": task_identifier}
        except Exception as delete_error:  # noqa: BLE001
            logger.exception("delete_cleaning_task failed")
            return {"success": False, "reason": f"delete_error: {delete_error}"}
