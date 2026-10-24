import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path

from settings import SQLITE_DATABASE_PATH, SQLITE_BUSY_TIMEOUT_SECONDS

logger = logging.getLogger(__name__)

_WAL_PRECONDITION_CACHE = {}


def _resolve_database_path(database_path=None):
    if database_path is None:
        return Path(SQLITE_DATABASE_PATH)
    return Path(database_path)


def _apply_connection_pragmas(connection, database_path):
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(f"PRAGMA busy_timeout = {int(SQLITE_BUSY_TIMEOUT_SECONDS * 1000)}")
    journal_mode_row = connection.execute("PRAGMA journal_mode = WAL").fetchone()
    if journal_mode_row is None or str(journal_mode_row["journal_mode"]).lower() != "wal":
        # 某些只读挂载或旧版 sqlite 不支持 WAL，降级为 DELETE 模式保证可写
        connection.execute("PRAGMA journal_mode = DELETE")
    connection.execute("PRAGMA synchronous = NORMAL")
    _WAL_PRECONDITION_CACHE[str(database_path)] = True


def _build_row_factory(cursor, row):
    # 使用 sqlite3.Row：同时支持 row[0] 下标访问与 row["col"] 列名访问，
    # 兼容 PDF 源码中两种行访问写法（dict 工厂只支持列名，曾导致 row[0] 崩溃）
    return sqlite3.Row(cursor, row)


def create_connection(database_path=None, read_only=False):
    resolved_path = _resolve_database_path(database_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    if read_only:
        connection_uri = f"{resolved_path}?mode=ro"
        connection = sqlite3.connect(connection_uri, uri=True, timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
    else:
        connection = sqlite3.connect(str(resolved_path), timeout=SQLITE_BUSY_TIMEOUT_SECONDS)
    connection.row_factory = _build_row_factory
    _apply_connection_pragmas(connection, resolved_path)
    return connection


# S3：consumption_record 补的规范层字段（老库缺哪列补哪列，幂等）
#   字段对齐步骤产出的 9 个规范字段要能完整落库，否则下游 RFM/预测/空间
#   拿不到 user_identifier / merchant_name / category_label 会断链。
_S3_CONSUMPTION_RECORD_COLUMNS = (
    ("merchant_name", "TEXT"),     # 商户名（对齐后的规范名）
    ("user_identifier", "TEXT"),   # 用户标识（RFM 依赖）
    ("category_label", "TEXT"),    # 品类标签（融合展示依赖）
    ("city_region", "TEXT"),       # 城市/区域文本（宏观源 adcode 未接入前的区域维度）
    ("source_kind", "TEXT"),       # 来源：catering/tourism/retail/nbs/amap/text
    ("batch_identifier", "TEXT"),  # 来自哪个接入批次（对 data_ingestion_console.batch_identifier）
    ("raw_row_id", "INTEGER"),     # 来自 ingestion_row.row_id（哪一行原文，可回溯）
)


def _apply_migrations(connection):
    """启动时给老库补新列（幂等：先查 PRAGMA table_info，缺哪列补哪列）。

    ALTER TABLE ADD COLUMN 重复执行会报 duplicate column，所以不能像建表脚本
    那样直接 executescript；这里先读表结构，只对缺失的列执行 ALTER。
    """
    try:
        existing_columns = {
            row["name"]
            for row in connection.execute("PRAGMA table_info(consumption_record)").fetchall()
        }
    except sqlite3.Error:
        # consumption_record 尚未创建（例如只跑某个 schema 文件的场景），跳过迁移
        return
    for column_name, column_type in _S3_CONSUMPTION_RECORD_COLUMNS:
        if column_name not in existing_columns:
            connection.execute(
                f"ALTER TABLE consumption_record ADD COLUMN {column_name} {column_type}"
            )
            logger.info("迁移: consumption_record 补列 %s %s", column_name, column_type)
    # raw_row_id 全局唯一 → 清洗重复跑也不会重复入库（INSERT OR IGNORE 兜底）
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_consumption_record_raw_row "
        "ON consumption_record(raw_row_id)"
    )


def initialize_schema(schema_file_path=None, database_path=None):
    """建库建表。

    - 不传 schema_file_path 时，按顺序执行 schema.sql（基础表）、schema_v2.sql
      （S1 任务表 + S2 原始数据层），两者都是幂等脚本；随后跑 _apply_migrations
      给老库补 S3 起的溯源列；
    - 显式传 schema_file_path 时只执行该文件（自检脚本用临时库时走这条路）。
    """
    project_root = Path(__file__).resolve().parent.parent
    if schema_file_path:
        schema_paths = [Path(schema_file_path)]
    else:
        schema_paths = [project_root / "schema.sql", project_root / "schema_v2.sql"]

    existing = [path for path in schema_paths if path.exists()]
    if not existing:
        raise FileNotFoundError(f"schema 文件缺失: {schema_paths[0]}")
    if schema_file_path and not Path(schema_file_path).exists():
        raise FileNotFoundError(f"schema 文件缺失: {schema_file_path}")

    connection = create_connection(database_path)
    try:
        with connection:
            for path in existing:
                connection.executescript(path.read_text(encoding="utf-8"))
                logger.info("数据库 schema 已执行: %s", path.name)
            _apply_migrations(connection)
    finally:
        connection.close()


@contextmanager
def transaction(database_path=None):
    connection = create_connection(database_path)
    try:
        connection.execute("BEGIN IMMEDIATE")
        yield connection
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()


@contextmanager
def read_session(database_path=None):
    connection = create_connection(database_path, read_only=False)
    try:
        yield connection
    finally:
        connection.close()


def execute_write(sql_statement, parameters=(), database_path=None):
    with transaction(database_path) as connection:
        cursor = connection.execute(sql_statement, parameters)
        return cursor.lastrowid


def execute_many_write(sql_statement, parameter_sequence, database_path=None):
    with transaction(database_path) as connection:
        cursor = connection.executemany(sql_statement, parameter_sequence)
        return cursor.rowcount


def fetch_one(sql_statement, parameters=(), database_path=None):
    with read_session(database_path) as connection:
        return connection.execute(sql_statement, parameters).fetchone()


def fetch_all(sql_statement, parameters=(), database_path=None):
    with read_session(database_path) as connection:
        return connection.execute(sql_statement, parameters).fetchall()


def stream_rows(sql_statement, parameters=(), batch_size=512, database_path=None):
    connection = create_connection(database_path)
    try:
        cursor = connection.execute(sql_statement, parameters)
        while True:
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            for row in batch:
                yield row
    finally:
        connection.close()


def health_probe(database_path=None):
    resolved_path = _resolve_database_path(database_path)
    try:
        with read_session(resolved_path) as connection:
            integrity_row = connection.execute("PRAGMA integrity_check").fetchone()
            table_rows = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE "
                "'sqlite_%'"
            ).fetchall()
            table_names = tuple(sorted(row["name"] for row in table_rows))
    except sqlite3.Error as database_error:
        logger.error("数据库健康检查失败: %s", database_error)
        return {
            "database_path": str(resolved_path),
            "integrity": "error",
            "table_names": (),
            "table_count": 0,
            "error": str(database_error),
        }
    return {
        "database_path": str(resolved_path),
        "integrity": integrity_row["integrity_check"] if integrity_row else "unknown",
        "table_names": table_names,
        "table_count": len(table_names),
    }
# ========== 兼容别名（PDF源码接口不一致，添加这3行，解决全部导入报错） ==========
get_connection = create_connection
acquire_connection = create_connection
transaction_scope = transaction
