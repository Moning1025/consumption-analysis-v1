# -*- coding: utf-8 -*-
"""S3 自测脚本：七阶段清洗融合流水线（在独立临时库上运行，绝不碰开发库）。

用法：
    PYTHONIOENCODING=utf-8 python qa-results/s3_selfcheck.py

覆盖：
    1. schema 初始化幂等
    2. 三列硬性必填放宽（缺用户/品类/城市的好行照常入库）
    3. 中文表头别名（用户ID -> user_identifier）
    4. 七阶段全部触发 + 终态 succeeded
    5. 缺失值补默认值（anonymous_consumer / 未分类 / 未知区域）
    6. 结果写入 consumption_record（含溯源列）
    7. 幂等（同批次再跑一遍不重复入库）
"""
import json
import os
import sys
import tempfile
import time

# 必须先设临时库，再 import 业务模块（settings 在 import 时读环境变量）
_TMP_DIR = tempfile.mkdtemp(prefix="s3_selfcheck_")
os.environ["SQLITE_DATABASE_PATH"] = os.path.join(_TMP_DIR, "s3_check.db")
os.environ["LOG_LEVEL"] = "WARNING"

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from store.db_connection import initialize_schema  # noqa: E402
from store.ingestion_row_store import compute_row_hash, insert_rows  # noqa: E402
from store import job_store  # noqa: E402
from store.consumption_record_store import count_records, list_records  # noqa: E402
from pipeline.cleaning_fusion_service import run_task_cleaning_fusion_pipeline_9  # noqa: E402

RESULTS = []


def check(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    print(("PASS  " if ok else "FAIL  ") + name + (("   " + detail) if detail else ""))


def seed_rows() -> None:
    rows = [
        # 完整中文行：用户ID / 消费品类 / 城市 三列齐全
        {"商户名称": "春熙路店", "消费金额": "45.60", "消费时间": "2026-10-04 18:20:00",
         "用户ID": "u1", "消费品类": "餐饮", "城市": "成都"},
        # 缺 用户/品类/城市 三列（只保留硬性必填三列）—— 应靠缺失值处理补默认
        {"商户名称": "锦里店", "消费金额": "88.00", "消费时间": "2026-10-03 21:40:00"},
        {"商户名称": "太古里店", "消费金额": "22.50", "消费时间": "2026-10-02 09:05:00", "城市": "成都"},
        {"商户名称": "天府广场店", "消费金额": "129.90", "消费时间": "2026-10-01 19:15:00"},
        {"商户名称": "成都春熙路店", "消费金额": "58.00", "消费时间": "2026-10-01 12:30:00"},
    ]
    payload = [
        {
            "line_no": idx + 1,
            "row_hash": compute_row_hash(raw),
            "raw_json": json.dumps(raw, ensure_ascii=False),
            "parse_status": "ok",
            "parse_error": None,
        }
        for idx, raw in enumerate(rows)
    ]
    inserted = insert_rows("src_s3check", "retail", payload)
    check("种子 5 条好行写入原始层", inserted == 5, f"inserted={inserted}")

    bad = {"商户名称": "缺时间店", "消费金额": "10.00"}
    insert_rows("src_s3check", "retail", [
        {
            "line_no": 99,
            "row_hash": compute_row_hash(bad),
            "raw_json": json.dumps(bad, ensure_ascii=False),
            "parse_status": "error",
            "parse_error": "缺少必填字段: consumption_time",
        }
    ])


def wait_job(job_id: str) -> dict:
    for _ in range(160):
        time.sleep(0.25)
        job = job_store.fetch_job(job_id)
        if job and job["status"] in ("succeeded", "failed", "canceled"):
            return job
    return job_store.fetch_job(job_id)


def main() -> int:
    initialize_schema()
    initialize_schema()  # 再跑一遍验证幂等
    check("schema 初始化幂等（连跑两遍不报错）", True)

    seed_rows()

    first = run_task_cleaning_fusion_pipeline_9({})
    job_id = first.get("job_id")
    check("提交清洗融合任务返回 job_id", bool(job_id), str(job_id))

    job = wait_job(job_id)
    check("任务终态 succeeded", bool(job) and job["status"] == "succeeded", job and job["status"])

    events = job_store.list_events(job_id)
    stages = [e["stage"] for e in events if e.get("stage")]
    stage_set = list(dict.fromkeys(stages))
    check(
        "七阶段全部触发",
        all(s in stage_set for s in ("ingest", "deduplicate", "impute", "align", "fuse", "validate", "output")),
        " -> ".join(stage_set),
    )

    total = count_records()
    check("consumption_record 写入 5 条", total == 5, f"total={total}")

    records = list_records(limit=100, offset=0)
    by_merchant = {r["merchant_name"]: r for r in records}
    chunxi = by_merchant.get("春熙路店")
    check("别名 用户ID -> user_identifier 生效", bool(chunxi) and chunxi["user_identifier"] == "u1",
          chunxi and chunxi.get("user_identifier"))

    jinli = by_merchant.get("锦里店")
    check(
        "缺失值补默认（锦里店缺三列）",
        bool(jinli) and jinli["user_identifier"] == "anonymous_consumer"
        and jinli["category_label"] == "未分类" and jinli["city_region"] == "未知区域",
    )

    has_lineage = all(r.get("batch_identifier") == "src_s3check" and r.get("raw_row_id") for r in records)
    check("溯源列 batch_identifier / raw_row_id 齐全", has_lineage)

    # 幂等：同数据再跑一遍，结果输出阶段应 0 新增
    second = run_task_cleaning_fusion_pipeline_9({})
    job2 = wait_job(second["job_id"])
    out_events = [e for e in job_store.list_events(second["job_id"]) if e.get("stage") == "output"]
    out_msg = " ".join(e["message"] for e in out_events)
    check("幂等（再跑一遍 0 新增）", "实际新增 0" in out_msg, out_msg)

    failed = [r for r in RESULTS if not r[1]]
    print("\n==== 结果 ====  PASS: %d   FAIL: %d" % (len(RESULTS) - len(failed), len(failed)))
    if failed:
        for name, _, detail in failed:
            print("  失败:", name, detail)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
