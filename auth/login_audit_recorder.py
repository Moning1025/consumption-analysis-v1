import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


class LoginAuditRecorder:
    """登录审计记录器：封装对审计日志的登录成功/失败记录。"""

    def __init__(self, audit_log_store: Any):
        self._audit_log_store = audit_log_store

    def record_login_success(
        self,
        account_id: int,
        actor_username: str,
        actor_role: str,
        source_page: Optional[str] = None,
        client_ip: Optional[str] = None,
        session_token_id: str = "",
    ) -> Optional[int]:
        recorder = getattr(self._audit_log_store, "record_login_success", None)
        if not callable(recorder):
            logger.warning("audit_log_store 缺少 record_login_success 方法")
            return None
        from store.db_connection import get_connection

        with get_connection() as connection:
            return recorder(
                connection,
                account_id=account_id,
                actor_username=actor_username,
                actor_role=actor_role,
                source_page=source_page,
                client_ip=client_ip,
                session_token_id=session_token_id,
            )

    def record_login_failure(
        self,
        actor_username: str,
        source_page: Optional[str] = None,
        client_ip: Optional[str] = None,
        failure_reason: str = "",
    ) -> Optional[int]:
        recorder = getattr(self._audit_log_store, "record_login_failure", None)
        if not callable(recorder):
            logger.warning("audit_log_store 缺少 record_login_failure 方法")
            return None
        from store.db_connection import get_connection

        with get_connection() as connection:
            return recorder(
                connection,
                actor_username=actor_username,
                source_page=source_page,
                client_ip=client_ip,
                failure_reason=failure_reason,
            )
