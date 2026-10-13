import logging
from typing import Any, Dict, Optional

from auth.session_token_issuer import SessionTokenIssuer
from auth.role_policy_registry import (
    evaluate_bridge_method_access,
    PolicyDecision,
)
from store.audit_log_store import record_permission_denied, record_task_dispatch

logger = logging.getLogger(__name__)


class AccessGuardMiddleware:
    """访问守卫中间件：校验会话令牌、执行基于角色的端点/桥接方法权限判定，并记录审计。"""

    def __init__(
        self,
        session_token_issuer: SessionTokenIssuer,
        role_policy_registry: Any,
        audit_log_store: Any,
    ):
        self._session_token_issuer = session_token_issuer
        self._role_policy_registry = role_policy_registry
        self._audit_log_store = audit_log_store

    def authenticate(self, raw_token: str) -> Optional[Dict[str, Any]]:
        """校验令牌并返回会话上下文（含角色与账号信息）。"""
        payload = self._session_token_issuer.verify_token(raw_token)
        if payload is None:
            return None
        return {
            "account_id": payload.get("account_id"),
            "login_name": payload.get("login_name"),
            "role_code": payload.get("role_code"),
            "token_payload": payload,
        }

    def authorize_bridge_method(
        self,
        session_context: Optional[Dict[str, Any]],
        bridge_method_name: str,
        source_page: str = "unknown",
        client_ip: Optional[str] = None,
    ) -> PolicyDecision:
        role_code = (session_context or {}).get("role_code") or "guest"
        account_id = (session_context or {}).get("account_id")
        login_name = (session_context or {}).get("login_name") or "anonymous"
        decision = evaluate_bridge_method_access(role_code, bridge_method_name)
        if not decision.permitted:
            record_permission_denied(
                self._get_connection(),
                account_id=account_id,
                actor_username=login_name,
                actor_role=role_code,
                source_page=source_page,
                attempted_action=bridge_method_name,
                required_capability=decision.required_action,
            )
            logger.warning(
                "access denied method=%s role=%s user=%s reason=%s",
                bridge_method_name, role_code, login_name, decision.denial_reason,
            )
        else:
            record_task_dispatch(
                self._get_connection(),
                account_id=account_id,
                actor_username=login_name,
                actor_role=role_code,
                source_page=source_page,
                task_identifier=bridge_method_name,
            )
        return decision

    def _get_connection(self):
        # 审计 store 的写函数均要求外部传入连接；中间件自取数据库连接
        from store.db_connection import get_connection

        return get_connection()

    def guard(self, raw_token: str, bridge_method_name: str) -> PolicyDecision:
        session_context = self.authenticate(raw_token)
        return self.authorize_bridge_method(session_context, bridge_method_name)
