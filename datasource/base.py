"""数据源适配器契约（S2 建立，S11 联网复用）。

设计意图
--------
接入台后面要接的源会越来越多：本地文件、四川统计局、高德 POI、Open-Meteo、
节假日日历、热榜文本……如果每接一个源就改一遍任务层和前端，代码会烂掉。

所以这里定一个**最小契约**：任何数据源只要实现 `DataSourceAdapter`，
就能被 `pipeline/ingestion_job.py` 统一调度：
    validate()  →  检查这个源此刻能不能用（文件在不在、Key 配没配）
    fetch(req)  →  吐出一行一行的原始记录（SourceRow）
    describe()  →  交代自己是谁（给前端和血缘看）

任务层只认这三件事，不认 HTTP 还是文件，所以：
- S2 的 `LocalFileAdapter`（本地 csv/json/xlsx）
- S11 的 `NbsStatAdapter`（国家统计局）、`AmapPoiAdapter`（高德）
实现同一个契约，进度上报、幂等、落库全部复用同一套代码。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Protocol, runtime_checkable

#: 业务口径的来源分类，写进 ingestion_row.source_kind
SOURCE_KINDS = (
    "catering",   # 餐饮订单流水
    "tourism",    # 旅游门票消费
    "retail",     # 零售 POS 交易
    "nbs",        # 国家/地方统计局宏观指标
    "amap",       # 高德 POI / 地理围栏
    "text",       # 热榜、评论等文本数据集
)


class DataSourceError(Exception):
    """数据源不可用或解析失败（消息直接展示给用户，用中文写）。"""


@dataclass(frozen=True)
class FetchRequest:
    """一次取数的参数。S2 的本地文件只看 extra；联网源才会用到城市与期次。"""

    city_codes: List[str] = field(default_factory=list)
    start_period: str = ""
    end_period: str = ""
    page: int = 1
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceRow:
    """源里的一行原始记录。

    - line_no：源内行号/页码，用于定位坏行
    - raw：原文（原样落库到 ingestion_row.raw_json，不改写）
    - canonical：按 FIELD_ALIAS_TABLE 归一后的字段（校验用，不落库）
    - parse_error：非空表示这行是坏行（照样落库，便于排查）
    """

    line_no: int
    raw: Dict[str, Any] = field(default_factory=dict)
    canonical: Dict[str, Any] = field(default_factory=dict)
    parse_error: str = ""


@runtime_checkable
class DataSourceAdapter(Protocol):
    """所有数据源的统一形状。"""

    #: 写入 ingestion_row.source_kind 的取值，应属于 SOURCE_KINDS
    kind: str

    def validate(self) -> None:
        """不可用时抛 DataSourceError（中文提示）。"""

    def fetch(self, request: Optional[FetchRequest] = None) -> Iterable[SourceRow]:
        """逐行吐出原始记录；实现方应尽量做惰性读取，避免大文件占满内存。"""

    def describe(self) -> Dict[str, Any]:
        """返回身份信息，用于日志、血缘与前端展示。"""
