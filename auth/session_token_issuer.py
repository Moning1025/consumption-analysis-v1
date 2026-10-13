import hashlib
import hmac
import json
import secrets
import time
import logging
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class SessionTokenIssuer:
    """会话令牌签发与校验器，基于 HMAC-SHA256 签名 + 时间戳有效期的无状态令牌。"""

    def __init__(self, secret_key: str, token_validity_seconds: int = 3600):
        if not secret_key:
            raise ValueError("secret_key 不能为空")
        self._secret_key = secret_key.encode("utf-8")
        self._token_validity_seconds = int(token_validity_seconds)

    def _sign_payload(self, payload_json: str) -> str:
        return hmac.new(self._secret_key, payload_json.encode("utf-8"), hashlib.sha256).hexdigest()

    def issue_token(
        self,
        account_id: int,
        login_name: str,
        role_code: str,
        extra: Optional[Dict[str, Any]] = None,
    ) -> str:
        issued_at = int(time.time())
        payload = {
            "account_id": account_id,
            "login_name": login_name,
            "role_code": role_code,
            "issued_at": issued_at,
            "expires_at": issued_at + self._token_validity_seconds,
            "extra": extra or {},
        }
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        signature = self._sign_payload(payload_json)
        return f"{payload_json}.{signature}"

    def verify_token(self, raw_token: str) -> Optional[Dict[str, Any]]:
        if not raw_token or "." not in raw_token:
            return None
        payload_json, signature = raw_token.rsplit(".", 1)
        expected = self._sign_payload(payload_json)
        if not hmac.compare_digest(expected, signature):
            logger.warning("session token signature mismatch")
            return None
        try:
            payload = json.loads(payload_json)
        except (TypeError, ValueError):
            return None
        expires_at = payload.get("expires_at", 0)
        if int(time.time()) > int(expires_at):
            logger.info("session token expired")
            return None
        return payload

    def get_token_validity_seconds(self) -> int:
        return self._token_validity_seconds

    def revoke_token(self, raw_token: str) -> bool:
        # 无状态令牌不支持本地即时吊销；如需强吊销可结合会话黑名单表。
        return True
