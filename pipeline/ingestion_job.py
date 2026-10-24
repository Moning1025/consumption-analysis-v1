"""接入台的两类后台任务（S1）：本地文件解析、结构化批次登记。

这两个函数由 pipeline.job_runner 在后台线程里调用，
每一次 ctx.report() 都会落到 job_run 表，前端轮询拿到的就是这里的真实数值，
不再有 time.sleep 伪造的 10% / 50% / 100%。
"""

from __future__ import annotations

import csv
import json
import os
import time
import zipfile
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pipeline.ingestion_intake_service import register_ingestion_source
from pipeline.job_runner import JobContext, register_job_handler
from settings import PROJECT_ROOT
from store.data_ingestion_console_store import update_ingestion_record

ALLOWED_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".zip"}

#: 说明书里“格式校验”阶段要求的列；缺列的行会被计数，但不会阻断整批
REQUIRED_RECORD_FIELDS = (
    "merchant_name",
    "consumption_amount",
    "consumption_time",
    "user_identifier",
    "category_label",
    "city_region",
)

UNPACK_ROOT = PROJECT_ROOT / "store" / "unpacked"

JOB_TYPE_INGESTION_PARSE = "ingestion.parse"
JOB_TYPE_INGESTION_REGISTER = "ingestion.register"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def guess_source_type(filename: str) -> str:
    """按文件名猜来源类型（和首页三条业务线对齐）。"""
    lowered = (filename or "").lower()
    if any(key in lowered for key in ("餐饮", "catering", "order", "meal", "food")):
        return "餐饮订单流水"
    if any(key in lowered for key in ("旅游", "trip", "travel", "ticket")):
        return "旅游门票消费"
    if any(key in lowered for key in ("零售", "retail", "pos", "store")):
        return "零售POS交易"
    return "餐饮订单流水"


# ------------------------------------------------------------------ 文件解析


def _read_text_rows(path: str) -> List[List[str]]:
    """读 CSV：先按 UTF-8（带 BOM）读，失败再按 GBK 读——国内导出文件常见编码。"""
    for encoding in ("utf-8-sig", "gbk"):
        try:
            with open(path, "r", encoding=encoding, newline="") as handle:
                sample = handle.read(4096)
                handle.seek(0)
                try:
                    dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
                except csv.Error:
                    dialect = csv.excel
                return [row for row in csv.reader(handle, dialect) if any(cell.strip() for cell in row)]
        except (UnicodeDecodeError, LookupError):
            continue
    raise ValueError(f"无法识别文件编码（已尝试 UTF-8 / GBK）：{os.path.basename(path)}")


def _count_records(path: str) -> Tuple[int, str]:
    """返回 (记录条数, 说明)。条数是真实解析出来的行数，不是编的。"""
    ext = os.path.splitext(path)[1].lower()
    if ext == ".csv":
        rows = _read_text_rows(path)
        if not rows:
            return 0, "空文件"
        header = [cell.strip().lower() for cell in rows[0]]
        has_header = any("金额" in cell or "merchant" in cell or "amount" in cell for cell in header)
        count = len(rows) - 1 if has_header else len(rows)
        return max(0, count), f"CSV 解析 {len(rows)} 行"
    if ext == ".json":
        with open(path, "r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
        if isinstance(data, list):
            return len(data), "JSON 数组"
        if isinstance(data, dict):
            for key in ("records", "data", "rows", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    return len(value), f"JSON 字段 {key}"
            return 1, "JSON 对象"
        return 0, "JSON 不是数组或对象"
    if ext in (".xlsx", ".xls"):
        return 0, "未安装 openpyxl，本次只登记文件、不统计行数"
    return 0, "不支持统计行数的文件类型"


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
    path: str, operator: str, region_code: str
) -> Tuple[Dict[str, Any], int]:
    """把一个文件登记成一条接入批次，返回 (登记结果, 真实解析出的记录条数)。"""
    count, note = _count_records(path)
    name = os.path.basename(path)
    ext = os.path.splitext(name)[1].lstrip(".").lower()
    payload_json = json.dumps(
        {
            "file_name": name,
            "file_ext": ext,
            "records": count,
            "note": note,
            "size_kb": round(os.path.getsize(path) / 1024, 1),
        },
        ensure_ascii=False,
    )
    return register_ingestion_source(
        {
            "source_name": name,
            "source_type": "file",
            "endpoint_uri": path,
            "region_code": region_code,
            "operator": operator,
            "status": "预处理中",
            "progress": 50,
            "term_count": count,
            "payload_json": payload_json,
            "import_time": _utc_now(),
            "meta_primary": f"路径: {path}",
            "meta_secondary": f"{guess_source_type(name)} / .{ext}",
        }
    ), count


def handle_ingestion_parse(context: JobContext, params: Dict[str, Any]) -> Dict[str, Any]:
    """job_type = ingestion.parse：真实解析本地文件，按“文件个数”推进进度。"""
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
    registered: List[Dict[str, Any]] = []
    record_total = 0
    for index, path in enumerate(units, start=1):
        context.check_cancel()
        context.report(
            progress=5 + int((index - 1) / total * 85),
            stage="parse",
            stage_label="文件解析",
            message=f"正在解析 {os.path.basename(path)}（{index}/{total}）",
        )
        result, count = _register_file_unit(path, operator, region_code)
        registered.append(result)
        record_total += count
        context.log(
            f"已登记 {os.path.basename(path)}：{count} 条记录（批次 {result['source_id']}）",
            stage="parse",
        )

    context.report(progress=92, stage="persist", stage_label="结果落库", message="正在刷新批次状态…")
    for result in registered:
        update_ingestion_record(
            int(result["source_id"]),
            {"status": "已完成", "progress": 100, "updated_at": _utc_now()},
        )

    context.report(progress=100, stage="done", stage_label="已完成", message=f"共接入 {total} 个文件")
    return {
        "total_files": total,
        "total_records": record_total,
        "source_ids": [item["source_id"] for item in registered],
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

    context.report(progress=75, stage="persist", stage_label="结果落库", message="正在写入批次记录…")
    context.check_cancel()
    update_ingestion_record(
        int(batch["source_id"]),
        {"status": "已完成", "progress": 100, "updated_at": _utc_now()},
    )
    context.report(progress=100, stage="done", stage_label="已完成", message=f"批次 {batch['source_id']} 接入完成")
    return {"source_id": batch["source_id"], "valid": len(valid), "invalid": invalid}


register_job_handler(JOB_TYPE_INGESTION_PARSE, handle_ingestion_parse)
register_job_handler(JOB_TYPE_INGESTION_REGISTER, handle_ingestion_register)
