import os
from pathlib import Path

# ---- 项目根目录 ----
PROJECT_ROOT = Path(__file__).resolve().parent

# ---- SQLite 数据库 ----
SQLITE_DATABASE_PATH = os.getenv(
    "SQLITE_DATABASE_PATH",
    str(PROJECT_ROOT / "local_consumption_insight.db"),
)
SQLITE_BUSY_TIMEOUT_SECONDS = float(os.getenv("SQLITE_BUSY_TIMEOUT_SECONDS", "5.0"))

# ---- 会话安全 ----
SESSION_SECRET_KEY = os.getenv("SESSION_SECRET_KEY", "local‑consumer‑insight‑dev‑secret")
SESSION_TOKEN_VALIDITY_SECONDS = int(os.getenv("SESSION_TOKEN_VALIDITY_SECONDS", "3600"))

# ---- 日志 ----
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# ---- Schema ----
SCHEMA_FILE_PATH = os.getenv("SCHEMA_FILE_PATH", str(PROJECT_ROOT / "schema.sql"))
