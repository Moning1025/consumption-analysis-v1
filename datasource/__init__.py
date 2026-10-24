"""datasource 包：数据源适配器。

对外只暴露契约与本地文件实现，联网源（S11）后续加在同一目录下。
"""

from datasource.base import (  # noqa: F401
    SOURCE_KINDS,
    DataSourceAdapter,
    DataSourceError,
    FetchRequest,
    SourceRow,
)
from datasource.file_adapter import LocalFileAdapter  # noqa: F401

__all__ = [
    "SOURCE_KINDS",
    "DataSourceAdapter",
    "DataSourceError",
    "FetchRequest",
    "SourceRow",
    "LocalFileAdapter",
]
