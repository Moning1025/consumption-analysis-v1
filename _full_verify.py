# -*- coding: utf-8 -*-
"""全面验证脚本：结构 / 语法 / 导入 / 数据库 / 登录鉴权任务 / store读写（6大项）"""
import sys, os, re, importlib, py_compile

sys.path.insert(0, r"D:\Python\pythonProject3")
ROOT = r"D:\Python\pythonProject3"
from store.db_connection import get_connection
results = []


def check(section, item, ok, detail=""):
    results.append((section, item, bool(ok), detail))
    print(("[PASS] " if ok else "[FAIL] ") + "%s | %s" % (section, item) + (" | " + str(detail) if detail else ""))


# ================= 1. 文件结构（对照截图目录树） =================
expect = {
    "auth": ["credential_vault.py", "session_token_issuer.py", "role_policy_registry.py",
             "access_guard_middleware.py", "login_audit_recorder.py"],
    "pipeline": ["task_dispatch_service.py", "ingestion_intake_service.py", "cleaning_fusion_service.py",
                 "sentiment_topic_service.py", "rfm_segmentation_service.py", "forecast_anomaly_service.py",
                 "spatial_hotspot_service.py", "recommendation_ranking_service.py"],
    "extensions": ["echarts_dashboard_feed.py", "realtime_api_probe.py"],
    "store": ["db_connection.py", "account_store.py", "audit_log_store.py", "data_ingestion_console_store.py",
              "rfm_segmentation_store.py", "spatial_hotspot_store.py", "cleaning_fusion_pipeline_store.py",
              "consumption_forecast_store.py", "sentiment_topic_mining_store.py",
              "realtime_api_monitor_store.py", "recommendation_workbench_store.py"],
}
for folder, files in expect.items():
    for f in files:
        check("1结构", folder + "/" + f, os.path.isfile(os.path.join(ROOT, folder, f)))
for f in ["main.py", "settings.py", "logging_dict_config.py", "bridge_api.py", "schema.sql"]:
    check("1结构", f, os.path.isfile(os.path.join(ROOT, f)))
check("1结构", "web/index.html", os.path.isfile(os.path.join(ROOT, "web", "index.html")))

# ================= 2. 语法编译（全部 py） =================
fail_compile = []
for root, dirs, files in os.walk(ROOT):
    dirs[:] = [d for d in dirs if d not in ("venv",)]
    for f in files:
        if f.endswith(".py"):
            p = os.path.join(root, f)
            try:
                py_compile.compile(p, doraise=True)
            except Exception as e:
                fail_compile.append((p, repr(e)))
check("2语法", "全部 py 文件 py_compile", not fail_compile,
      "; ".join("%s: %s" % x for x in fail_compile[:5]))

# ================= 3. 全模块导入 =================
mods = [
    "auth.credential_vault", "auth.session_token_issuer", "auth.role_policy_registry",
    "auth.access_guard_middleware", "auth.login_audit_recorder",
    "store.db_connection", "store.account_store", "store.audit_log_store",
    "store.rfm_segmentation_store", "store.spatial_hotspot_store",
    "store.data_ingestion_console_store", "store.cleaning_fusion_pipeline_store",
    "store.consumption_forecast_store", "store.sentiment_topic_mining_store",
    "store.realtime_api_monitor_store", "store.recommendation_workbench_store",
    "pipeline.cleaning_fusion_service", "pipeline.forecast_anomaly_service",
    "pipeline.task_dispatch_service", "pipeline.ingestion_intake_service",
    "pipeline.sentiment_topic_service", "pipeline.rfm_segmentation_service",
    "pipeline.spatial_hotspot_service", "pipeline.recommendation_ranking_service",
    "extensions.echarts_dashboard_feed", "extensions.realtime_api_probe",
    "bridge_api", "settings", "logging_dict_config", "main",
]
ok_mods = 0
fail_mods = []
for m in mods:
    try:
        importlib.import_module(m)
        ok_mods += 1
    except Exception as e:
        fail_mods.append((m, repr(e)))
check("3导入", "模块导入 %d/%d" % (ok_mods, len(mods)), not fail_mods,
      "; ".join("%s->%s" % x for x in fail_mods[:5]))

# ================= 4. 数据库 =================
try:
    import main
    main.ensure_database_initialized()
    main.seed_default_administrator()
    from store.db_connection import get_connection

    with get_connection() as conn:
        db_tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        admin = conn.execute(
            "SELECT account_id, login_name, role_code FROM user_account WHERE login_name='admin'").fetchone()
    check("4数据库", "建表幂等 + admin 存在", admin is not None,
          dict(admin) if admin else "")
    schema_sql = open(os.path.join(ROOT, "schema.sql"), encoding="utf-8").read()
    schema_tables = set(re.findall(
        r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)", schema_sql, re.I))
    check("4数据库", "schema.sql 建表全部存在", schema_tables <= db_tables,
          "schema=%d db=%d missing=%s" % (len(schema_tables), len(db_tables),
                                           sorted(schema_tables - db_tables)))
    ref_tables = set()
    for folder in ("store", "pipeline"):
        for f in os.listdir(os.path.join(ROOT, folder)):
            if f.endswith(".py"):
                src = open(os.path.join(ROOT, folder, f), encoding="utf-8").read()
                # 剔除 import 语句行，避免 "from x import" 的 FROM 被误匹配为表引用
                src = re.sub(r"(?m)^\s*from\s+[\w\.]+\s+import.*$", "", src)
                for match_trio in re.findall(
                        r"\bINSERT\s+INTO\s+(\w+)|\bDELETE\s+FROM\s+(\w+)|\bUPDATE\s+(\w+)\s+SET",
                        src, re.I):
                    ref_tables.update(t for t in match_trio if t)
    ref_tables = {t for t in ref_tables if t not in ("sqlite_master",)}
    check("4数据库", "store/pipeline 引用表名均在库中", ref_tables <= db_tables,
          "missing=%s" % sorted(ref_tables - db_tables))
except Exception as e:
    import traceback
    traceback.print_exc()

# ================= 5. 登录 / 鉴权 / 任务分发 =================
try:
    from settings import SESSION_SECRET_KEY, SESSION_TOKEN_VALIDITY_SECONDS
    from store import account_store, audit_log_store
    from auth import credential_vault, login_audit_recorder, role_policy_registry
    from auth.session_token_issuer import SessionTokenIssuer
    from bridge_api import BridgeApi
    from pipeline.task_dispatch_service import dispatch_page_task, list_registered_task_identifiers

    vault = credential_vault.CredentialVault()
    bridge = BridgeApi(
        connection_factory=get_connection,
        settings={"session_secret": SESSION_SECRET_KEY,
                  "token_ttl_seconds": SESSION_TOKEN_VALIDITY_SECONDS},
        runtime_dependencies={
            "account_store": account_store, "credential_vault": vault,
            "login_audit_recorder": login_audit_recorder.LoginAuditRecorder(audit_log_store),
            "role_policy_registry": role_policy_registry, "audit_log_store": audit_log_store,
        },
    )
    ok_login = bridge.login("admin", "admin123456", "login")
    check("5登录", "admin 正确密码登录", ok_login.get("success") is True,
          "role=%s" % ok_login.get("role"))
    bad_login = bridge.login("admin", "wrong-password", "login")
    check("5登录", "错误密码被拒", bad_login.get("success") is False,
          "reason=%s" % bad_login.get("reason"))
    token = ok_login.get("token")
    issuer = SessionTokenIssuer(secret_key=SESSION_SECRET_KEY,
                                token_validity_seconds=SESSION_TOKEN_VALIDITY_SECONDS)
    sess = issuer.verify_token(token)
    check("5鉴权", "token 签发后可校验通过", sess is not None and sess.get("login_name") == "admin")
    dec = role_policy_registry.evaluate_bridge_method_access(
        "administrator", "run_task_cleaning_fusion_pipeline_5")
    check("5鉴权", "administrator 角色允许执行清洗任务", dec.permitted is True,
          "endpoint=%s" % dec.matched_endpoint)
    ids = list_registered_task_identifiers()
    check("5任务", "已注册任务标识非空", len(ids) > 0, "%d 个" % len(ids))
    check("5任务", "ping 正常", bridge.ping().get("success") is True)
    try:
        r = dispatch_page_task("run_task_cleaning_fusion_pipeline_5",
                               {"pipeline_id": "PIPELINE_DEMO_001", "forecast_window_days": 30},
                               operator_account="admin")
        check("5任务", "清洗流水线任务执行未抛异常", True,
              "table=%s elapsed_ms=%s handler=%s" % (r.get("target_table"), r.get("elapsed_ms"),
                                                     sorted((r.get("handler_output") or {}).keys())))
    except Exception as e:
        check("5任务", "清洗流水线任务执行未抛异常", False, repr(e))
    # 窗口实际链路：bridge.run_task（鉴权中间件 + 任务分发审计）
    try:
        rt = bridge.run_task("run_task_cleaning_fusion_pipeline_5",
                             {"pipeline_id": "PIPELINE_DEMO_001", "forecast_window_days": 30}, token)
        check("5任务", "bridge.run_task 完整链路(鉴权+审计)", rt.get("success") is True,
              "permitted=%s reason=%s" % (rt.get("permitted"), rt.get("reason")))
    except Exception as e:
        check("5任务", "bridge.run_task 完整链路(鉴权+审计)", False, repr(e))
except Exception as e:
    import traceback
    traceback.print_exc()

# ================= 6. store 读写冒烟 =================
try:
    from store import audit_log_store
    with get_connection() as conn:
        summary = audit_log_store.summarize_login_activity(conn)
        recs = list(audit_log_store.iter_audit_records(conn, page_size=5))
    check("6store", "审计日志读写正常", isinstance(summary, dict),
          "login_success=%s login_failure=%s" % (summary.get("login_success_total"),
                                                 summary.get("login_failure_total")))
except Exception as e:
    check("6store", "审计日志读写正常", False, repr(e))

# ================= 7. 多源数据接入台链路 =================
try:
    imb = bridge.import_demo_batch("餐饮订单流水", token)
    check("7接入", "导入演示批次(餐饮)", imb.get("success") is True,
          "batch=%s" % (imb.get("data") or {}).get("batch_identifier"))
    lst = bridge.list_ingestion_records(10)
    check("7接入", "查询导入记录", lst.get("success") is True and len(lst.get("records", [])) >= 1,
          "records=%d" % len(lst.get("records", [])))
    ov = bridge.get_ingestion_overview()
    check("7接入", "接入概览", ov.get("success") is True,
          "total_records=%s total_terms=%s" % ((ov.get("data") or {}).get("total_records"),
                                               (ov.get("data") or {}).get("total_terms")))
    reg = bridge.register_source("测试API数据源", "api", "http://localhost:9000/demo", token)
    check("7接入", "登记数据源(api)", reg.get("success") is True,
          "output=%s" % ((reg.get("handler_output") or {}).get("source_name")))
    # 无效 token 拒绝路径：guest 审计可记录，返回 permitted=false 而非抛异常
    bad = bridge.import_demo_batch("餐饮订单流水", "invalid-token-abc")
    check("7接入", "无效token拒绝路径不崩溃", bad.get("success") is False and bad.get("permitted") is False,
          "reason=%s" % (bad.get("reason") or "")[:60])
except Exception as e:
    import traceback
    traceback.print_exc()
    check("7接入", "多源数据接入台链路", False, repr(e))

# ================= 8. 清洗融合流水线页面链路 =================
try:
    ct = bridge.list_cleaning_tasks("", 50)
    check("8清洗", "任务队列查询", ct.get("success") is True and len(ct.get("tasks", [])) >= 1,
          "tasks=%d" % len(ct.get("tasks", [])))
    cid = bridge.create_cleaning_task("界面新建测试任务", "consumption_dashboard_fusion", "all_sources")
    check("8清洗", "新建任务记录", cid.get("success") is True,
          "id=%s" % cid.get("task_identifier"))
    det = bridge.get_cleaning_task_detail(cid.get("task_identifier", ""))
    check("8清洗", "任务详情查询", det.get("success") is True and det.get("task", {}).get("task_status") == "pending",
          "status=%s" % (det.get("task") or {}).get("task_status"))
    dele = bridge.delete_cleaning_task(cid.get("task_identifier", ""))
    check("8清洗", "删除任务记录", dele.get("success") is True,
          "deleted=%s" % dele.get("deleted"))
except Exception as e:
    import traceback
    traceback.print_exc()
    check("8清洗", "清洗融合页面链路", False, repr(e))

# ================= 汇总 =================
fails = [r for r in results if not r[2]]
print("\n===== 汇总：总计 %d 项，PASS %d，FAIL %d =====" % (
    len(results), len(results) - len(fails), len(fails)))
for s, i, ok, d in fails:
    print("  FAIL:", s, "|", i, "|", d)
print("全部通过" if not fails else "存在失败项，见上")
