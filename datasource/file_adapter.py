"""本地文件适配器（S2）：把 csv / json / xlsx 读成统一的 SourceRow 序列。

S2 之前接入台的“解析”只数了个大概行数，没有留下任何原始数据，后面清洗、分析
全是空中楼阁。这个适配器负责把文件真正读开：

- 编码：先 UTF-8（带 BOM），失败退 GBK —— 国内导出文件最常见两种情况；
- 分隔符：csv.Sniffer 猜，猜不出用逗号；
- 表头：能认出“商户名称/金额”这类列名就当时表头，认不出就按 col_1..col_N 处理；
- 列名归一：复用清洗层同一张 FIELD_ALIAS_TABLE，避免两套口径打架；
- 坏行不抛出，而是标 parse_error 照样返回 —— 坏行也要留证据。

读的时候是惰性的（生成器），10 万行也不会一次性进内存。
"""

from __future__ import annotations

import csv
import io
import json
import os
from typing import Any, Dict, Iterable, Iterator, List, Optional

from datasource.base import DataSourceError, FetchRequest, SourceRow
from pipeline.cleaning_fusion_service import FIELD_ALIAS_TABLE

try:  # openpyxl 是可选依赖：没装时 xlsx 会给出中文提示而不是崩掉
    from openpyxl import load_workbook

    OPENPYXL_AVAILABLE = True
except ImportError:  # pragma: no cover - 依赖缺失分支
    load_workbook = None
    OPENPYXL_AVAILABLE = False

SUPPORTED_EXTENSIONS = (".csv", ".json", ".xlsx", ".xls")

#: 说明书“格式校验”阶段要求六个字段齐全
REQUIRED_FIELDS = (
    "merchant_name",
    "consumption_amount",
    "consumption_time",
    "user_identifier",
    "category_label",
    "city_region",
)

#: 字段的中文说明，坏行提示用
FIELD_LABELS = {
    "merchant_name": "商户名称",
    "consumption_amount": "消费金额",
    "consumption_time": "消费时间",
    "user_identifier": "用户标识",
    "category_label": "消费品类",
    "city_region": "城市区域",
}

_ALIAS_LOOKUP: Dict[str, str] = {}
for _canonical, _aliases in FIELD_ALIAS_TABLE.items():
    _ALIAS_LOOKUP[_canonical.strip().lower()] = _canonical
    for _alias in _aliases:
        _ALIAS_LOOKUP[_alias.strip().lower()] = _canonical

#: 长别名优先做包含匹配，避免“消费金额”被更短的“金额”抢走之类的误配
_ALIAS_KEYS_BY_LENGTH = sorted(_ALIAS_LOOKUP, key=len, reverse=True)


def _normalize_key(raw_key: Any) -> str:
    return str(raw_key or "").strip().strip("\ufeff").lower()


def lookup_canonical(raw_key: Any) -> str:
    """把任意表头/键名归一到规范字段名，认不出返回空串。

    匹配顺序：
    1. 精确命中别名表（amount → consumption_amount）
    2. 表头里包含某个别名（“消费品类”包含“品类”、“城市区域”包含“城市”）
       只对长度 >= 2 的别名做包含匹配，避免单字误伤。
    """
    key = _normalize_key(raw_key)
    if not key:
        return ""
    direct = _ALIAS_LOOKUP.get(key)
    if direct:
        return direct
    for alias in _ALIAS_KEYS_BY_LENGTH:
        if len(alias) >= 2 and alias in key:
            return _ALIAS_LOOKUP[alias]
    return ""


def resolve_canonical(raw: Dict[str, Any]) -> Dict[str, Any]:
    """把一行原始字典按别名表映射成规范字段名。认不出的列直接丢掉（原文仍在）。"""
    canonical: Dict[str, Any] = {}
    for key, value in raw.items():
        target = lookup_canonical(key)
        if target and target not in canonical:
            canonical[target] = value
    return canonical


def describe_missing(canonical: Dict[str, Any]) -> str:
    """返回缺失/空值的必填字段中文清单，空字符串表示这行合法。"""
    missing = [
        FIELD_LABELS.get(field, field)
        for field in REQUIRED_FIELDS
        if not str(canonical.get(field, "") or "").strip()
    ]
    if not missing:
        return ""
    return "缺少必填字段：" + "、".join(missing)


def looks_like_header(cells: List[Any]) -> bool:
    """表头判定：任意一格能对上别名表，就认为第一行是表头。"""
    for cell in cells:
        if lookup_canonical(cell):
            return True
    return False


class LocalFileAdapter:
    """单个本地文件的适配器（zip 由任务层先解包，再逐个文件 new 一个适配器）。"""

    kind = "file"

    def __init__(self, path: str, source_kind: str = "retail") -> None:
        self.path = str(path)
        self.source_kind = source_kind
        self.bytes_total = 0
        try:
            self.bytes_total = os.path.getsize(self.path)
        except OSError:
            self.bytes_total = 0
        #: 读取进度（0..1），只有 csv 能真实给出；其它格式保持 0
        self.progress_ratio = 0.0

    # ------------------------------------------------------------- 契约
    @property
    def file_name(self) -> str:
        return os.path.basename(self.path)

    @property
    def extension(self) -> str:
        return os.path.splitext(self.path)[1].lower()

    def validate(self) -> None:
        if not os.path.isfile(self.path):
            raise DataSourceError(f"文件不存在：{self.file_name}")
        if self.extension not in SUPPORTED_EXTENSIONS:
            raise DataSourceError(
                f"不支持的文件类型：{self.extension or '(无后缀)'}，"
                f"目前支持 {', '.join(SUPPORTED_EXTENSIONS)}"
            )
        if self.extension == ".xls":
            raise DataSourceError(".xls 旧格式暂不支持，请在 Excel 里另存为 .xlsx 后再导入")
        if self.extension == ".xlsx" and not OPENPYXL_AVAILABLE:
            raise DataSourceError("缺少 openpyxl 依赖，暂时无法解析 .xlsx")
        if os.path.getsize(self.path) == 0:
            raise DataSourceError(f"文件是空的：{self.file_name}")

    def describe(self) -> Dict[str, Any]:
        return {
            "adapter": "local_file",
            "kind": self.source_kind,
            "file_name": self.file_name,
            "file_ext": self.extension.lstrip("."),
            "size_kb": round(self.bytes_total / 1024, 1),
            "supports_progress": self.extension == ".csv",
        }

    def fetch(self, request: Optional[FetchRequest] = None) -> Iterable[SourceRow]:
        if self.extension == ".csv":
            return self._iter_csv()
        if self.extension == ".json":
            return self._iter_json()
        if self.extension == ".xlsx":
            return self._iter_xlsx()
        raise DataSourceError(f"不支持的文件类型：{self.extension}")

    # ------------------------------------------------------------- 具体格式
    def _iter_csv(self) -> Iterator[SourceRow]:
        handle = self._open_text()
        try:
            sample = handle.read(65536)
            handle.seek(0)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel
            reader = csv.reader(handle, dialect)
            header: Optional[List[str]] = None
            line_no = 0
            for cells in reader:
                line_no += 1
                if not any(str(cell).strip() for cell in cells):
                    continue  # 跳过空行，不算错
                if header is None:
                    if looks_like_header(cells):
                        header = [str(cell).strip() for cell in cells]
                        continue
                    header = [f"col_{index}" for index in range(1, len(cells) + 1)]
                self._update_ratio(handle)
                raw = self._zip_row(header, cells)
                if raw is None:
                    yield SourceRow(
                        line_no=line_no,
                        raw={"cells": [str(cell) for cell in cells]},
                        parse_error=f"列数与表头不一致（表头 {len(header)} 列，本行 {len(cells)} 列）",
                    )
                    continue
                yield self._build_row(line_no, raw)
            if header is None:
                raise DataSourceError(f"文件里没有可读内容：{self.file_name}")
        finally:
            handle.close()
        self.progress_ratio = 1.0

    def _iter_json(self) -> Iterator[SourceRow]:
        try:
            with open(self.path, "r", encoding="utf-8-sig") as handle:
                data = json.load(handle)
        except json.JSONDecodeError as decode_error:
            raise DataSourceError(f"JSON 格式错误：{decode_error}") from decode_error
        items: Any = data
        if isinstance(data, dict):
            for key in ("records", "data", "rows", "items"):
                value = data.get(key)
                if isinstance(value, list):
                    items = value
                    break
            else:
                items = [data]
        if not isinstance(items, list):
            raise DataSourceError("JSON 顶层既不是数组也不是对象，无法解析成记录")
        for index, item in enumerate(items, start=1):
            if isinstance(item, dict):
                yield self._build_row(index, item)
            else:
                yield SourceRow(
                    line_no=index,
                    raw={"value": item},
                    parse_error="JSON 元素不是对象，无法当成一条消费记录",
                )
        self.progress_ratio = 1.0

    def _iter_xlsx(self) -> Iterator[SourceRow]:
        if not OPENPYXL_AVAILABLE:
            raise DataSourceError("缺少 openpyxl 依赖，暂时无法解析 .xlsx")
        workbook = load_workbook(self.path, read_only=True, data_only=True)
        try:
            sheet = workbook[workbook.sheetnames[0]]
            header: Optional[List[str]] = None
            for line_no, row in enumerate(sheet.iter_rows(values_only=True), start=1):
                cells = ["" if cell is None else str(cell) for cell in row]
                if not any(cell.strip() for cell in cells):
                    continue
                if header is None:
                    if looks_like_header(cells):
                        header = [cell.strip() for cell in cells]
                        continue
                    header = [f"col_{index}" for index in range(1, len(cells) + 1)]
                raw = self._zip_row(header, cells)
                if raw is None:
                    yield SourceRow(
                        line_no=line_no,
                        raw={"cells": cells},
                        parse_error=f"列数与表头不一致（表头 {len(header)} 列，本行 {len(cells)} 列）",
                    )
                    continue
                yield self._build_row(line_no, raw)
            if header is None:
                raise DataSourceError(f"工作表里没有可读内容：{self.file_name}")
        finally:
            workbook.close()
        self.progress_ratio = 1.0

    # ------------------------------------------------------------- 小工具
    def _open_text(self) -> io.TextIOWrapper:
        for encoding in ("utf-8-sig", "gbk"):
            handle = None
            try:
                handle = open(self.path, "r", encoding=encoding, newline="")
                handle.read(1)
                handle.seek(0)
                return handle
            except (UnicodeDecodeError, LookupError):
                if handle is not None:
                    try:
                        handle.close()
                    except Exception:  # noqa: BLE001 - 关闭失败不影响换编码重试
                        pass
                continue
        raise DataSourceError(f"无法识别文件编码（已尝试 UTF-8 / GBK）：{self.file_name}")

    def _update_ratio(self, handle: Any) -> None:
        if not self.bytes_total:
            return
        try:
            position = handle.tell()
        except (OSError, ValueError):
            return
        ratio = float(position) / float(self.bytes_total)
        self.progress_ratio = min(1.0, max(0.0, ratio))

    @staticmethod
    def _zip_row(header: List[str], cells: List[Any]) -> Optional[Dict[str, Any]]:
        if len(cells) > len(header):
            return None
        padded = list(cells) + [""] * (len(header) - len(cells))
        return {name or f"col_{index}": value for index, (name, value) in enumerate(zip(header, padded), start=1)}

    def _build_row(self, line_no: int, raw: Dict[str, Any]) -> SourceRow:
        canonical = resolve_canonical(raw)
        return SourceRow(
            line_no=line_no,
            raw=raw,
            canonical=canonical,
            parse_error=describe_missing(canonical),
        )
