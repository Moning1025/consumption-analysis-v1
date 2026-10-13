import os
import sys
import logging
import logging.config
from pathlib import Path

# settings，完全匹配你现有的settings.py，没有BASE_DIR，直接用PROJECT_ROOT
from settings import (
    PROJECT_ROOT,
    SQLITE_DATABASE_PATH,
    SCHEMA_FILE_PATH,
    SESSION_SECRET_KEY,
    SESSION_TOKEN_VALIDITY_SECONDS,
    LOG_LEVEL
)
from logging_dict_config import build_logging_dict_config, get_module_logger

# ========== 导入模块 ==========
from store.db_connection import create_connection, transaction, get_connection, acquire_connection, transaction_scope
from store import account_store
from store import audit_log_store
from store import data_ingestion_console_store
from store import rfm_segmentation_store
from store import spatial_hotspot_store

from auth import credential_vault
from auth import session_token_issuer
from auth import role_policy_registry
from auth import access_guard_middleware
from auth import login_audit_recorder

from pipeline import task_dispatch_service
from pipeline import cleaning_fusion_service
from pipeline import forecast_anomaly_service
from pipeline import ingestion_intake_service
from pipeline import sentiment_topic_service
from pipeline import rfm_segmentation_service
from pipeline import spatial_hotspot_service
from pipeline import recommendation_ranking_service

from extensions import echarts_dashboard_feed
from extensions import realtime_api_probe

logger = logging.getLogger(__name__)


def ensure_database_initialized():
    """初始化SQLite数据库，幂等执行schema.sql建表"""
    db_file = Path(SQLITE_DATABASE_PATH)
    db_dir = db_file.parent
    db_dir.mkdir(parents=True, exist_ok=True)

    with create_connection() as conn:
        cursor = conn.cursor()
        with open(SCHEMA_FILE_PATH, "r", encoding="utf-8") as f:
            sql_script = f.read()
        cursor.executescript(sql_script)
        conn.commit()
        logger.info("✅数据库表初始化完成")


def seed_default_administrator():
    """初始化默认管理员账号 admin / admin123456"""
    user = account_store.find_account_by_login_name("admin")
    if user is None:
        account_store.register_account(
            login_name="admin",
            display_name="系统管理员",
            plain_password="admin123456",
            role_code="administrator"
        )
        logger.info("✅已创建默认管理员账号 admin")
    else:
        logger.info("ℹ️管理员账号admin已存在，跳过创建")


def main():
    # 加载日志配置
    log_cfg = build_logging_dict_config(LOG_LEVEL)
    logging.config.dictConfig(log_cfg)
    global logger
    logger = get_module_logger("main")

    logger.info("===== 多源数据融合的本地消费分析系统 V1.0 启动 =====")

    # 初始化数据库
    ensure_database_initialized()

    # 初始化默认管理员
    seed_default_administrator()

    # 组装运行时依赖对象
    vault = credential_vault.CredentialVault()
    login_auditor = login_audit_recorder.LoginAuditRecorder(audit_log_store)

    runtime_dependencies = {
        "connection_factory": create_connection,
        "account_store": account_store,
        "credential_vault": vault,
        "login_audit_recorder": login_auditor,
        "role_policy_registry": role_policy_registry,
        "audit_log_store": audit_log_store,
        "data_ingestion_console_store": data_ingestion_console_store,
        "rfm_segmentation_store": rfm_segmentation_store,
        "spatial_hotspot_store": spatial_hotspot_store,
    }

    # 初始化桥接API（桥接桌面窗口与后端服务）
    from bridge_api import BridgeApi
    bridge = BridgeApi(
        connection_factory=create_connection,
        settings={
            "session_secret": SESSION_SECRET_KEY,
            "token_ttl_seconds": SESSION_TOKEN_VALIDITY_SECONDS,
        },
        runtime_dependencies=runtime_dependencies,
    )

    # 启动pywebview桌面窗口
    # 批量修正：把历史遗留的"已接入/排队中/预处理中"记录全部标记为已完成 100%
    try:
        from store.db_connection import transaction_scope
        with transaction_scope() as conn:
            cur = conn.execute(
                "UPDATE data_ingestion_console SET status='已完成', progress=100 "
                "WHERE status IN ('已接入','排队中','预处理中')"
            )
            logger.info("批量修正历史批次状态：%d 条记录已置为已完成(100%%)", cur.rowcount)
    except Exception:  # noqa: BLE001
        logger.exception("批量修正历史批次状态失败")

    import webview
    window = webview.create_window(
        title="多源数据融合的本地消费分析系统 V1.0",
        url=str(PROJECT_ROOT / "web" / "index.html"),
        js_api=bridge,
        width=1400,
        height=900,
        resizable=True,
        easy_drag=False,
    )

    # 拖拽文件接收：pywebview 6.x DOM API，挂 body 的 drop 事件，
    # pywebview 会自动把每个文件的绝对路径注入为 pywebviewFullPath
    def on_drop(e):
        try:
            dt = (e or {}).get("dataTransfer", {}) if isinstance(e, dict) else {}
            files = dt.get("files", [])
            paths = [f.get("pywebviewFullPath") for f in files if f.get("pywebviewFullPath")]
            logger.info("DOM drop 触发，files=%d paths=%s", len(files), paths)
            if paths:
                result = bridge.ingest_local_files(paths)
                logger.info("拖拽文件登记结果: %s", result.get("summary", result))
                # 通知前端刷新批次清单
                try:
                    window.evaluate_js("if (typeof loadIngestionAll === 'function') loadIngestionAll();")
                except Exception:  # noqa: BLE001
                    logger.exception("前端刷新失败")
        except Exception:  # noqa: BLE001
            logger.exception("on_drop failed")

    def setup_dnd():
        try:
            from webview.dom import DOMEventHandler
            from webview.dom import _dnd_state
            window.dom.body.events.drop += DOMEventHandler(on_drop, prevent_default=True)
            logger.info("✅拖拽文件已挂 body.drop，num_listeners=%s", _dnd_state["num_listeners"])
        except Exception:  # noqa: BLE001
            logger.exception("DOM drop 注册失败")

    try:
        window.events.loaded += setup_dnd
        bridge.bind_window(window)
    except Exception:  # noqa: BLE001
        logger.exception("window bind 失败")

    logger.info("桌面窗口已创建，启动中……")
    webview.start()
    logger.info("系统正常退出")
    return 0


if __name__ == "__main__":
    sys.exit(main())
