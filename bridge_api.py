import logging
from typing import Any, Dict, Optional

from auth.access_guard_middleware import AccessGuardMiddleware
from auth.session_token_issuer import SessionTokenIssuer
from auth.role_policy_registry import PolicyDecision
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
        按记录创建时间模拟真实流程推进：<2s=已接入10% / 2~5s=预处理中50% / >5s=已完成100%。"""
        from store.data_ingestion_console_store import query_ingestion_records
        from store.db_connection import transaction_scope
        from datetime import datetime

        try:
            now = datetime.now().isoformat(timespec="seconds")
            with transaction_scope() as conn:
                rows = conn.execute(
                    "SELECT id, created_at FROM data_ingestion_console "
                    "WHERE NOT (status='已完成' AND progress=100)"
                ).fetchall()
                for r in rows:
                    rid, itime = r[0], r[1] or now
                    try:
                        t = datetime.fromisoformat(itime)
                        elapsed = (datetime.now() - t).total_seconds()
                    except Exception:
                        elapsed = 999
                    if elapsed < 2:
                        new_status, new_pct = "已接入", 10
                    elif elapsed < 5:
                        new_status, new_pct = "预处理中", 50
                    else:
                        new_status, new_pct = "已完成", 100
                    conn.execute(
                        "UPDATE data_ingestion_console SET status=?, progress=? WHERE id=?",
                        (new_status, new_pct, rid),
                    )
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
        """导入一批演示消费记录（餐饮/旅游/零售），走任务分发鉴权。file_ext 记录文件格式后缀。"""
        try:
            decision = self._authorize("run_task_data_ingestion_console_1", token)
        except Exception as guard_error:  # noqa: BLE001
            logger.exception("import_demo_batch authorize failed")
            return {"success": False, "permitted": False,
                    "reason": f"鉴权失败: {guard_error}（若提示会话过期请重新登录）"}
        if not decision.permitted:
            return {"success": False, "reason": decision.denial_reason, "permitted": False}
        from pipeline.ingestion_intake_service import submit_ingestion_batch

        samples = DEMO_SAMPLES.get(source_type or "", DEMO_SAMPLES["餐饮订单流水"])
        try:
            result = submit_ingestion_batch(
                source_name=source_type,
                records=samples,
                operator=self._operator_from_token(token),
                file_ext=(file_ext or ""),
            )
            return {"success": True, "permitted": True, "data": result}
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

    @staticmethod
    def _guess_source_type(filename: str) -> str:
        """按文件名猜来源类型：餐饮/旅游/零售。"""
        name = (filename or "").lower()
        if any(k in name for k in ["餐饮", "餐", "catering", "restaurant", "food", "meal"]):
            return "餐饮订单流水"
        if any(k in name for k in ["旅游", "门票", "travel", "ticket", "scenic", "hotel"]):
            return "旅游门票消费"
        if any(k in name for k in ["零售", "pos", "retail", "shop", "store", "supermarket", "mall"]):
            return "零售POS交易"
        return "餐饮订单流水"

    @staticmethod
    def _unpack_zip(zip_path: str) -> list:
        """解压 zip 里的 .csv/.xlsx/.xls/.json 到 store/unpacked/<zip名>/，返回解压出的文件绝对路径列表。"""
        import os
        import zipfile
        from datetime import datetime

        project_root = os.path.dirname(os.path.abspath(__file__))
        base = os.path.join(project_root, "store", "unpacked")
        os.makedirs(base, exist_ok=True)
        stem = os.path.splitext(os.path.basename(zip_path))[0]
        target = os.path.join(base, f"{stem}_{datetime.now().strftime('%H%M%S')}")
        os.makedirs(target, exist_ok=True)

        allowed = {".csv", ".xlsx", ".xls", ".json"}
        extracted = []
        with zipfile.ZipFile(zip_path) as zf:
            for name in zf.namelist():
                if name.endswith("/"):
                    continue
                ext = os.path.splitext(name)[1].lower()
                if ext not in allowed:
                    continue
                safe_name = os.path.basename(name)
                if not safe_name:
                    continue
                zf.extract(name, target)
                extracted.append(os.path.join(target, safe_name))
        return extracted

    def _register_one(self, fp: str, token: str, note: str = "") -> Dict[str, Any]:
        """登记单个文件到导入批次清单。"""
        import os
        from datetime import datetime

        fname = os.path.basename(fp)
        ext = os.path.splitext(fp)[1].lower()
        source_type = self._guess_source_type(fname)
        size_kb = os.path.getsize(fp) // 1024
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return self.register_source(
            source_name=fname,
            source_type=source_type,
            endpoint_uri=fp.replace("\\", "/"),
            token=token,
            status="已接入",
            progress=10,
            operator=self._last_username,
            term_count=0,
            payload=f'{{"file_ext":"{ext.lstrip(".")}","size_kb":{size_kb}}}',
            import_time=now,
            meta_primary=f"路径: {fp}",
            meta_secondary=(f"{source_type} · {ext}" + (f"　[{note}]" if note else "")),
        )

    def ingest_local_files(self, file_paths) -> Dict[str, Any]:
        """接收拖入窗口的本地文件绝对路径列表；.zip 自动解压并登记子文件。"""
        import os

        if not file_paths:
            return {"success": False, "reason": "no_files"}
        token = self._last_token
        accepted_ext = {".csv", ".xlsx", ".xls", ".json", ".zip"}
        ok, failed = [], []
        source_ids = []
        for fp in file_paths:
            try:
                if not fp or not os.path.isfile(fp):
                    failed.append({"path": str(fp), "reason": "文件不存在"})
                    continue
                ext = os.path.splitext(fp)[1].lower()
                if ext not in accepted_ext:
                    failed.append({"path": fp, "reason": f"不支持的后缀 {ext}"})
                    continue
                fname = os.path.basename(fp)

                def _collect_id(r):
                    sid = (r.get("data") or {}).get("source_id")
                    if sid:
                        source_ids.append(sid)

                if ext == ".zip":
                    extracted = self._unpack_zip(fp)
                    r = self._register_one(fp, token, note=f"压缩包已解压，含 {len(extracted)} 个数据文件")
                    if r.get("success"):
                        ok.append(f"{fname}（已解压 {len(extracted)} 个文件）")
                        _collect_id(r)
                    else:
                        failed.append({"path": fp, "reason": r.get("reason", "登记失败")})
                    for sub in extracted:
                        try:
                            rs = self._register_one(sub, token, note=f"解压自 {fname}")
                            if rs.get("success"):
                                ok.append("└ " + os.path.basename(sub))
                                _collect_id(rs)
                            else:
                                failed.append({"path": sub, "reason": rs.get("reason", "登记失败")})
                        except Exception as sub_error:  # noqa: BLE001
                            failed.append({"path": sub, "reason": str(sub_error)})
                else:
                    r = self._register_one(fp, token)
                    if r.get("success"):
                        ok.append(fname)
                        _collect_id(r)
                    else:
                        failed.append({"path": fp, "reason": r.get("reason", "登记失败")})
            except Exception as file_error:  # noqa: BLE001
                logger.exception("ingest_local_files one file failed: %s", fp)
                failed.append({"path": str(fp), "reason": str(file_error)})
        logger.info("ingest_local_files done: ok=%d failed=%d source_ids=%s", len(ok), len(failed), source_ids)

        # 后台自动推进批次状态：已接入(10%) → 预处理中(50%) → 触发融合 → 已完成(100%)
        if source_ids:
            self._advance_batch_status_async(source_ids)

        return {
            "success": len(ok) > 0,
            "accepted": ok,
            "failed": failed,
            "summary": f"成功登记 {len(ok)} 个文件" + (f"，失败 {len(failed)} 个" if failed else ""),
        }

    def _advance_batch_status_async(self, source_ids):
        """后台线程：模拟预处理进度推进 + 触发清洗融合 + 最终完成。"""
        import threading
        import time

        token = self._last_token

        def _refresh_ui():
            try:
                if self._window:
                    self._window.evaluate_js("if (typeof loadIngestionAll === 'function') loadIngestionAll();")
            except Exception:  # noqa: BLE001
                pass

        def _run():
            from store.data_ingestion_console_store import update_ingestion_record
            # 1. 1 秒后 → 预处理中 50%
            time.sleep(1)
            for sid in source_ids:
                try:
                    update_ingestion_record(sid, {"status": "预处理中", "progress": 50})
                except Exception:  # noqa: BLE001
                    logger.exception("update %s to processing failed", sid)
            _refresh_ui()
            logger.info("批次 %s 进入预处理中(50%)", source_ids)

            # 2. 3 秒后 → 触发清洗融合流水线
            time.sleep(2)
            try:
                self.run_task("run_task_cleaning_fusion_pipeline_9", {"pipeline_id": "PIPELINE_UI_001"}, token)
                logger.info("已自动触发清洗融合流水线")
            except Exception:  # noqa: BLE001
                logger.exception("auto trigger cleaning fusion failed")

            # 3. 6 秒后 → 已完成 100%
            time.sleep(3)
            for sid in source_ids:
                try:
                    update_ingestion_record(sid, {"status": "已完成", "progress": 100, "term_count": 12})
                except Exception:  # noqa: BLE001
                    logger.exception("update %s to done failed", sid)
            _refresh_ui()
            logger.info("批次 %s 已完成(100%)", source_ids)

        threading.Thread(target=_run, daemon=True).start()

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
