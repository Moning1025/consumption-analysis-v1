"""S1-1 自检脚本：验证任务表（job_run / job_event）与 job_store 读写是否正常。

用法（PyCharm 里右键运行，或终端）：
    python scripts/verify_job_store.py

特点：
- 全程使用**系统临时库**，不碰你的 local_consumption_insight.db；
- 每一步都打印实际值，最后给出 PASS / FAIL 汇总；
- 退出码 0 = 全通过，1 = 有失败项。
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from store import job_store  # noqa: E402
from store.db_connection import fetch_one, initialize_schema  # noqa: E402

results = []


def check(label: str, condition: bool, actual=None) -> None:
    results.append((label, bool(condition)))
    mark = "PASS" if condition else "FAIL"
    detail = "" if actual is None else f"  → 实际: {actual}"
    print(f"  [{mark}] {label}{detail}")


def main() -> int:
    temp_dir = Path(tempfile.mkdtemp(prefix="jobstore_verify_"))
    temp_db = temp_dir / "verify.db"
    print("=" * 70)
    print("S1-1 自检：任务表 + job_store")
    print(f"临时库: {temp_db}")
    print("=" * 70)

    # ---------- 1. 建表 ----------
    print("\n[1] 执行 schema.sql + schema_v2.sql 建表")
    initialize_schema(database_path=str(temp_db))
    tables = {
        row["name"]
        for row in [
            fetch_one(
                "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
                (name,),
                str(temp_db),
            )
            for name in ("job_run", "job_event")
        ]
        if row is not None
    }
    check("job_run / job_event 两张表已创建", tables == {"job_run", "job_event"}, sorted(tables))

    # ---------- 2. 建任务 ----------
    print("\n[2] 创建一个任务（应处于 queued / 0%）")
    job_id = job_store.create_job(
        job_type="ingestion.parse",
        params={"file": "样例.csv", "rows": 1200},
        operator_name="admin",
        page_identifier="data_ingestion_console",
        source_kind="drop",
        database_path=str(temp_db),
    )
    job = job_store.fetch_job(job_id, str(temp_db))
    check("job_id 以 job_ 开头", job_id.startswith("job_"), job_id)
    check("初始状态 = queued", job["status"] == job_store.STATUS_QUEUED, job["status"])
    check("初始进度 = 0", job["progress"] == 0, job["progress"])
    check("入参已序列化可读", job["params"] == {"file": "样例.csv", "rows": 1200}, job["params"])

    # ---------- 3. 进度上报 ----------
    print("\n[3] 开始执行并上报两次进度")
    job_store.mark_running(job_id, str(temp_db))
    job_store.update_progress(
        job_id,
        progress=35,
        stage="parse",
        stage_label="格式校验",
        message="已解析 420 / 1200 行",
        database_path=str(temp_db),
    )
    job = job_store.fetch_job(job_id, str(temp_db))
    check("状态 = running", job["status"] == job_store.STATUS_RUNNING, job["status"])
    check("进度 = 35", job["progress"] == 35, job["progress"])
    check("阶段中文名写入", job["stage_label"] == "格式校验", job["stage_label"])
    check("started_at 已记录", bool(job["started_at"]), job["started_at"])

    job_store.update_progress(job_id, progress=999, database_path=str(temp_db))
    job = job_store.fetch_job(job_id, str(temp_db))
    check("进度越界被夹到 100", job["progress"] == 100, job["progress"])

    # ---------- 4. 过程日志 ----------
    print("\n[4] 追加 3 条日志，并测试增量拉取")
    for index, text in enumerate(["开始解析文件", "发现 12 行缺字段", "解析完成"], start=1):
        job_store.append_event(
            job_id,
            text,
            level=job_store.LEVEL_WARN if index == 2 else job_store.LEVEL_INFO,
            stage="parse",
            database_path=str(temp_db),
        )
    events = job_store.list_all_events(job_id, database_path=str(temp_db))
    check("共 3 条日志", len(events) == 3, len(events))
    check("seq 连续 1,2,3", [e["seq"] for e in events] == [1, 2, 3], [e["seq"] for e in events])
    check("第 2 条是 WARN", events[1]["level"] == "WARN", events[1]["level"])

    incremental = job_store.list_events(job_id, after_seq=1, database_path=str(temp_db))
    check("after_seq=1 只返回后 2 条", [e["seq"] for e in incremental] == [2, 3], [e["seq"] for e in incremental])

    # ---------- 5. 成功收尾 ----------
    print("\n[5] 标记成功并读取结果")
    job_store.mark_succeeded(
        job_id,
        result={"rows_ok": 1188, "rows_failed": 12},
        message="导入完成",
        database_path=str(temp_db),
    )
    job = job_store.fetch_job(job_id, str(temp_db))
    check("状态 = succeeded", job["status"] == job_store.STATUS_SUCCEEDED, job["status"])
    check("进度 = 100", job["progress"] == 100, job["progress"])
    check("结果可反序列化", job["result"] == {"rows_ok": 1188, "rows_failed": 12}, job["result"])
    check("finished_at 已记录", bool(job["finished_at"]), job["finished_at"])

    # ---------- 6. 列表与过滤 ----------
    print("\n[6] 列表查询与状态过滤")
    jobs = job_store.list_jobs(database_path=str(temp_db))
    check("列表返回 1 条", len(jobs) == 1, len(jobs))
    done = job_store.list_jobs(status=job_store.STATUS_SUCCEEDED, database_path=str(temp_db))
    check("按状态过滤命中 1 条", len(done) == 1, len(done))
    check("不存在排队中任务", not job_store.has_active_job(database_path=str(temp_db)))

    # ---------- 7. 取消语义 ----------
    print("\n[7] 取消语义：排队中直接取消 / 运行中只打标记")
    queued_job = job_store.create_job(job_type="rfm.compute", database_path=str(temp_db))
    check("排队中任务可取消", job_store.request_cancel(queued_job, str(temp_db)))
    check(
        "取消后状态 = canceled",
        job_store.fetch_job(queued_job, str(temp_db))["status"] == job_store.STATUS_CANCELED,
    )

    running_job = job_store.create_job(job_type="cleaning_fusion", database_path=str(temp_db))
    job_store.mark_running(running_job, str(temp_db))
    job_store.request_cancel(running_job, str(temp_db))
    running_view = job_store.fetch_job(running_job, str(temp_db))
    check("运行中任务状态仍 running", running_view["status"] == job_store.STATUS_RUNNING, running_view["status"])
    check("但 cancel_requested = True", running_view["cancel_requested"] is True, running_view["cancel_requested"])
    check("is_cancel_requested() 为真", job_store.is_cancel_requested(running_job, str(temp_db)))
    check("已结束任务不可再取消", not job_store.request_cancel(job_id, str(temp_db)))

    # ---------- 8. 清理 ----------
    print("\n[8] 清理过期任务")
    removed = job_store.purge_finished_jobs(keep_days=0, database_path=str(temp_db))
    check("清理掉 2 个已结束任务（1 成功 + 1 排队取消）", removed == 2, removed)
    check("剩余 1 个运行中任务", len(job_store.list_jobs(database_path=str(temp_db))) == 1)

    # ---------- 汇总 ----------
    passed = sum(1 for _, ok in results if ok)
    total = len(results)
    print("\n" + "=" * 70)
    print(f"结果：{passed}/{total} 项通过")
    if passed != total:
        print("失败项：")
        for label, ok in results:
            if not ok:
                print(f"  - {label}")
    print("=" * 70)

    # 收尾：删临时库
    try:
        for path in temp_dir.glob("*"):
            path.unlink()
        temp_dir.rmdir()
    except OSError:
        pass

    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(main())
