"""接入台的两类后台任务：本地文件解析、结构化批次登记。

这两个函数由 pipeline.job_runner 在后台线程里调用，
每一次 ctx.report() 都会落到 job_run 表，前端轮询拿到的就是这里的真实数值，
不再有 time.sleep 伪造的 10% / 50% / 100%。

S2 起，`ingestion.parse` 不再只是“数行数登记一个批次”，而是真的把文件读开：
    csv/json/xlsx → datasource.LocalFileAdapter 逐行吐出
    → 列名归一 + 必填校验（缺字段的行标 error 照样落库）
    → 行指纹 sha256 判重（同一个文件导两次不会重复入库）
    → 写入 ingestion_row（原文层），并写一份 ingestion_validation 报告
清洗任务（S3）直接从 ingestion_row 读原文，责任边界清楚。
"""

from __future__ import annotations

import json
import os
import time
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from datasource.base import DataSourceError
from datasource.file_adapter import LocalFileAdapter
from pipeline.ingestion_intake_service import register_ingestion_source
from pipeline.job_runner import JobContext, register_job_handler
from settings import PROJECT_ROOT
from store.data_ingestion_console_store import update_ingestion_record
from store.ingestion_row_store import (
    compute_row_hash,
    count_rows,
    fetch_existing_hashes,
    insert_rows,
    save_validation,
)

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".zip"}

#: 说明书里“格式校验”阶段要求的列。S3 起放宽为三列硬性必填（商户/金额/时间），
#: 用户标识 / 消费品类 / 城市区域 允许缺，交给清洗阶段“缺失值处理”补默认值。
REQUIRED_RECORD_FIELDS = (
    "merchant_name",
    "consumption_amount",
    "consumption_time",
)

UNPACK_ROOT = PROJECT_ROOT / "store" / "unpacked"

JOB_TYPE_INGESTION_PARSE = "ingestion.parse"
JOB_TYPE_INGESTION_REGISTER = "ingestion.register"

#: 每攒够这么多行落一次库，避免大文件把内存顶满
ROW_BUFFER_SIZE = 200


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


#: 文件名关键词 → 业务口径（写进 ingestion_row.source_kind）
_SOURCE_KIND_KEYWORDS: Tuple[Tuple[Tuple[str, ...], str], ...] = (
    (("餐饮", "catering", "order", "meal", "food"), "catering"),
    (("旅游", "tourism", "trip", "travel", "ticket", "门票", "景区"), "tourism"),
    (("零售", "retail", "pos", "store", "门店"), "retail"),
    (("统计", "nbs", "macro", "gdp"), "nbs"),
    (("poi", "amap", "高德", "地理"), "amap"),
    (("评论", "review", "hot", "热榜", "text"), "text"),
)


def guess_source_kind(filename: str) -> str:
    """按文件名猜业务口径，猜不出按零售流水处理。"""
    lowered = (filename or "").lower()
    for keywords, kind in _SOURCE_KIND_KEYWORDS:
        if any(keyword in lowered for keyword in keywords):
            return kind
    return "retail"


def guess_source_type(filename: str) -> str:
    """按文件名猜来源类型（和首页三条业务线对齐）。"""
    kind = guess_source_kind(filename)
    return {
        "catering": "餐饮订单流水",
        "tourism": "旅游门票消费",
        "retail": "零售POS交易",
        "nbs": "统计宏观指标",
        "amap": "高德地理数据",
        "text": "文本数据集",
    }.get(kind, "零售POS交易")


# ------------------------------------------------------------------ 文件解包


def _unpack_zip(zip_path: str, stamp: str) -> List[str]:
    """解压 zip 到 store/unpacked/<文件名>_<时间戳>/，只挑出可接入的数据文件。"""
    stem = os.path.splitext(os.path.basename(zip_path))[0]
    target = UNPACK_ROOT / f"{stem}_{stamp}"
    target.mkdir(parents=True, exist_ok=True)
    extracted: List[str] = []
    with zipfile.ZipFile(zip_path) as archive:
        for member in archive.namelist():
            if member.endswith("/"):
                continue
            if os.path.splitext(member)[1].lower() not in ALLOWED_EXTENSIONS - {".zip"}:
                continue
            destination = target / os.path.basename(member)
            with archive.open(member) as source, open(destination, "wb") as sink:
                sink.write(source.read())
            extracted.append(str(destination))
    return extracted


def _expand_units(file_paths: List[str], stamp: str) -> List[str]:
    """把输入路径展开成“真正要解析的文件”列表（zip 拆成内部文件）。"""
    units: List[str] = []
    for path in file_paths:
        if os.path.splitext(path)[1].lower() == ".zip":
            units.extend(_unpack_zip(path, stamp))
        else:
            units.append(path)
    return units


def _register_file_unit(
    path: str, operator: str, region_code: str, adapter: Optional[LocalFileAdapter] = None
) -> Dict[str, Any]:
    """把一个文件登记成一条接入批次（状态：预处理中），返回含 batch_identifier 的登记结果。"""
    adapter = adapter or LocalFileAdapter(path)
    name = os.path.basename(path)
    ext = os.path.splitext(name)[1].lstrip(".").lower()
    payload_json = json.dumps(
        {
            "file_name": name,
            "file_ext": ext,
            "size_kb": round(os.path.getsize(path) / 1024, 1) if os.path.exists(path) else 0,
            "adapter": adapter.describe(),
        },
        ensure_ascii=False,
    )
    result = register_ingestion_source(
        {
            "source_name": name,
            "source_type": "file",
            "endpoint_uri": path,
            "region_code": region_code,
            "operator": operator,
            "status": "预处理中",
            "progress": 0,
            "term_count": 0,
            "payload_json": payload_json,
            "import_time": _utc_now(),
            "meta_primary": f"路径: {path}",
            "meta_secondary": f"{guess_source_type(name)} / .{ext}",
        }
    )
    result["source_kind"] = adapter.source_kind
    return result


def _build_row_payload(source_row: Any) -> Dict[str, Any]:
    """把 SourceRow 转换成 ingestion_row 需要的一行参数。"""
    raw_text = json.dumps(source_row.raw or {}, ensure_ascii=False, default=str)
    return {
        "line_no": int(source_row.line_no or 0),
        "row_hash": compute_row_hash(source_row.raw or {}),
        "raw_json": raw_text,
        "parse_status": "error" if source_row.parse_error else "ok",
        "parse_error": source_row.parse_error or None,
    }


def _flush_rows(batch_identifier: str, source_kind: str, buffer: List[Dict[str, Any]]) -> int:
    if not buffer:
        return 0
    inserted = insert_rows(batch_identifier, source_kind, buffer)
    buffer.clear()
    return inserted


def handle_ingestion_parse(context: JobContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """job_type = ingestion.parse：真实解析本地文件，按文件+读取进度推进进度条。"""
    raw_paths = params.get("file_paths") or []
    file_paths = [str(item) for item in raw_paths if str(item).strip()]
    if not file_paths:
        raise ValueError("没有收到任何文件路径")

    missing = [path for path in file_paths if not os.path.isfile(path)]
    if missing:
        raise ValueError(f"以下文件不存在：{', '.join(os.path.basename(p) for p in missing)}")

    operator = str(params.get("operator") or "anonymous")
    region_code = str(params.get("region_code") or "510000")
    stamp = time.strftime("%H%M%S")

    context.report(progress=2, stage="expand", stage_label="解包展开", message="正在展开压缩包…")
    units = _expand_units(file_paths, stamp)
    context.check_cancel()
    if not units:
        raise ValueError("压缩包里没有可接入的 .csv / .xlsx / .xls / .json 文件")

    total = len(units)
    file_reports: List[Dict[str, Any]] = []
    batch_identifiers: List[str] = []
    validation_codes: List[str] = []
    sum_ok = sum_error = sum_duplicate = 0

    # 幂等口径：按“行内容指纹”全局判重（同一个文件导第二次不会再入库）
    known_hashes = fetch_existing_hashes()

    for index, path in enumerate(units, start=1):
        context.check_cancel()
        name = os.path.basename(path)
        span_start = 5 + int((index - 1) / total * 85)
        span_end = 5 + int(index / total * 85)
        summary = {
            "file_name": name,
            "source_kind": guess_source_kind(name),
            "batch_identifier": "",
            "ok": 0,
            "error": 0,
            "duplicate": 0,
            "note": "",
        }

        adapter = LocalFileAdapter(path, source_kind=guess_source_kind(name))
        batch = _register_file_unit(path, operator, region_code, adapter=adapter)
        batch_identifier = str(batch.get("batch_identifier") or "")
        summary["batch_identifier"] = batch_identifier
        batch_identifiers.append(batch_identifier)

        context.report(
            progress=span_start,
            stage="parse",
            stage_label="文件解析",
            message=f"正在解析 {name}（{index}/{total}）",
        )

        # 幂等：全库行指纹里已有的内容直接跳过（重复行只计数、不入库）
        # 用同一个全局集合（不是复制），本次任务内跨文件的重复也会被挡住
        seen_hashes = known_hashes
        buffer: List[Dict[str, Any]] = []
        ok = error = duplicate = 0
        parsed = 0

        try:
            adapter.validate()
            for source_row in adapter.fetch():
                context.check_cancel()
                payload = _build_row_payload(source_row)
                if payload["row_hash"] in seen_hashes:
                    duplicate += 1
                    continue
                seen_hashes.add(payload["row_hash"])
                buffer.append(payload)
                parsed += 1
                if payload["parse_status"] == "ok":
                    ok += 1
                else:
                    error += 1
                if len(buffer) >= ROW_BUFFER_SIZE:
                    _flush_rows(batch_identifier, adapter.source_kind, buffer)
                    if adapter.progress_ratio:
                        context.report(
                            progress=min(
                                span_end,
                                span_start + int((span_end - span_start) * adapter.progress_ratio),
                            ),
                            stage="parse",
                            stage_label="文件解析",
                            message=f"{name}：已读 {parsed} 行（好行 {ok} / 坏行 {error}）",
                        )
        except DataSourceError as adapter_error:
            # 整个文件不可解析：留一条 line_no=0 的坏行做证据，任务继续处理下一个文件
            error += 1
            buffer.append(
                {
                    "line_no": 0,
                    "row_hash": compute_row_hash({"__file__": name, "__error__": str(adapter_error)}),
                    "raw_json": json.dumps(
                        {"file_name": name, "error": str(adapter_error)}, ensure_ascii=False
                    ),
                    "parse_status": "error",
                    "parse_error": str(adapter_error),
                }
            )
            summary["note"] = str(adapter_error)
            context.log(f"{name} 解析失败：{adapter_error}", level="WARN", stage="parse")

        _flush_rows(batch_identifier, adapter.source_kind, buffer)
        stored = count_rows(batch_identifier)

        report = {
            "file_name": name,
            "source_kind": adapter.source_kind,
            "adapter": adapter.describe(),
            "parsed_rows": parsed,
            "valid_rows": stored["ok"],
            "error_rows": stored["error"],
            "duplicate_rows": duplicate,
            "missing_field_hint": "商户名称/消费金额/消费时间 三列必须齐全（用户/品类/城市缺失时清洗阶段自动补默认值）",
        }
        validation_codes.append(
            save_validation(
                batch_identifier,
                valid_rows=stored["ok"],
                error_rows=stored["error"],
                warning_rows=duplicate,
                report=report,
            )
        )

        update_ingestion_record(
            int(batch["source_id"]),
            {"term_count": stored["ok"], "status": "已完成", "progress": 100, "updated_at": _utc_now()},
        )

        summary.update({"ok": stored["ok"], "error": stored["error"], "duplicate": duplicate})
        file_reports.append(summary)
        sum_ok += stored["ok"]
        sum_error += stored["error"]
        sum_duplicate += duplicate

        context.log(
            f"{name}：入库 {stored['ok']} 行，坏行 {stored['error']}，重复跳过 {duplicate}"
            f"（批次 {batch_identifier}）",
            level="INFO" if stored["error"] == 0 else "WARN",
            stage="parse",
        )
        context.report(
            progress=span_end,
            stage="parse",
            stage_label="文件解析",
            message=f"{name} 完成：入库 {stored['ok']} 行",
        )

    context.report(progress=92, stage="persist", stage_label="结果落库", message="正在写校验报告…")
    all_error = sum_error

    context.check_cancel()
    done_message = (
        f"共 {total} 个文件，入库 {sum_ok} 行"
        + (f"，坏行 {all_error}" if all_error else "")
        + (f"，重复跳过 {sum_duplicate}" if sum_duplicate else "")
    )
    context.report(progress=100, stage="done", stage_label="已完成", message=done_message)
    return {
        "total_files": total,
        "total_rows": sum_ok + sum_error,
        "valid_rows": sum_ok,
        "error_rows": all_error,
        "duplicate_rows": sum_duplicate,
        "batch_identifiers": batch_identifiers,
        "validation_codes": validation_codes,
        "files": file_reports,
    }


# ------------------------------------------------------------------ 结构化批次


def _validate_records(records: Any) -> Tuple[List[Dict[str, Any]], int]:
    """逐行校验必填列，返回 (合法记录, 非法条数)。"""
    if not isinstance(records, list):
        raise ValueError("records 必须是数组")
    valid: List[Dict[str, Any]] = []
    invalid = 0
    for item in records:
        if not isinstance(item, dict):
            invalid += 1
            continue
        if all(str(item.get(field, "")).strip() for field in REQUIRED_RECORD_FIELDS):
            valid.append(item)
        else:
            invalid += 1
    return valid, invalid


def handle_ingestion_register(context: JobContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """job_type = ingestion.register：演示批次 / 手工登记，做真实的格式校验。"""
    source_name = str(params.get("source_name") or "演示批次").strip()
    source_type = str(params.get("source_type") or "manual")
    records = params.get("records")
    operator = str(params.get("operator") or "anonymous")
    region_code = str(params.get("region_code") or "510000")

    context.report(progress=8, stage="validate", stage_label="格式校验", message="正在校验字段完整性…")
    context.check_cancel()
    valid, invalid = _validate_records(records)
    if not valid:
        raise ValueError(f"格式校验未通过：{invalid} 条记录全部缺少必填字段")

    batch = register_ingestion_source(
        {
            "source_name": source_name,
            "source_type": source_type,
            "endpoint_uri": str(params.get("endpoint_uri") or "demo://batch"),
            "region_code": region_code,
            "operator": operator,
            "status": "预处理中",
            "progress": 60,
            "term_count": len(valid),
            "payload_json": json.dumps(
                {
                    "records": valid[:200],
                    "truncated": len(valid) > 200,
                    "invalid_rows": invalid,
                },
                ensure_ascii=False,
            ),
            "import_time": _utc_now(),
            "meta_primary": f"演示批次 {source_name}",
            "meta_secondary": f"合法 {len(valid)} 条 / 非法 {invalid} 条",
        }
    )
    batch_identifier = str(batch.get("batch_identifier") or "")

    context.report(progress=55, stage="persist", stage_label="结果落库", message="正在写入原始行…")
    context.check_cancel()
    rows = []
    for line_no, record in enumerate(valid, start=1):
        rows.append(
            {
                "line_no": line_no,
                "row_hash": compute_row_hash(record),
                "raw_json": json.dumps(record, ensure_ascii=False, default=str),
                "parse_status": "ok",
                "parse_error": None,
            }
        )
    inserted = insert_rows(batch_identifier, guess_source_kind(source_name), rows)
    validation_code = save_validation(
        batch_identifier,
        valid_rows=inserted,
        error_rows=invalid,
        warning_rows=0,
        report={
            "source_name": source_name,
            "source_kind": guess_source_kind(source_name),
            "parsed_rows": len(valid),
            "valid_rows": inserted,
            "error_rows": invalid,
            "mode": "manual_batch",
        },
    )

    context.report(progress=85, stage="persist", stage_label="结果落库", message="正在刷新批次状态…")
    update_ingestion_record(
        int(batch["source_id"]),
        {"status": "已完成", "progress": 100, "term_count": inserted, "updated_at": _utc_now()},
    )
    context.report(progress=100, stage="done", stage_label="已完成", message=f"批次 {batch['source_id']} 接入完成")
    return {
        "source_id": batch["source_id"],
        "batch_identifier": batch_identifier,
        "valid": inserted,
        "invalid": invalid,
        "validation_code": validation_code,
    }


register_job_handler(JOB_TYPE_INGESTION_PARSE, handle_ingestion_parse)
register_job_handler(JOB_TYPE_INGESTION_REGISTER, handle_ingestion_register)
