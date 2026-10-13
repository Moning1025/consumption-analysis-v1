import logging
from typing import Any, Dict


def build_logging_dict_config(level: str = "INFO") -> Dict[str, Any]:
    """构建 logging.dictConfig 配置字典。"""
    normalized_level = str(level or "INFO").upper()
    if normalized_level not in ("DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL"):
        normalized_level = "INFO"
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
            },
        },
        "handlers": {
            "console": {
                "class": "logging.StreamHandler",
                "level": normalized_level,
                "formatter": "standard",
                "stream": "ext://sys.stdout",
            },
        },
        "root": {
            "level": normalized_level,
            "handlers": ["console"],
        },
        "loggers": {
            "consumer_insight": {"level": normalized_level, "handlers": ["console"], "propagate": False},
            "pipeline": {"level": normalized_level, "handlers": ["console"], "propagate": False},
            "store": {"level": normalized_level, "handlers": ["console"], "propagate": False},
            "auth": {"level": normalized_level, "handlers": ["console"], "propagate": False},
            "extensions": {"level": normalized_level, "handlers": ["console"], "propagate": False},
        },
    }


def get_module_logger(name: str) -> logging.Logger:
    """按名称获取/配置模块级 logger（兼容 store.audit_log_store 的调用）。"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
        logger.addHandler(handler)
        logger.propagate = False
    return logger
