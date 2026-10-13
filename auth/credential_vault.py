import hashlib
import hmac
import secrets
import logging
from typing import Tuple

logger = logging.getLogger(__name__)

_PBKDF2_ITERATIONS = 120000
_SALT_BYTES = 16
_HASH_BYTES = 32


def generate_salt() -> str:
    """生成随机盐值，返回十六进制字符串。"""
    return secrets.token_hex(_SALT_BYTES)


def hash_password(plain_password: str, salt: str) -> str:
    """基于 PBKDF2-HMAC-SHA256 计算口令摘要。"""
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        plain_password.encode("utf-8"),
        salt.encode("utf-8"),
        _PBKDF2_ITERATIONS,
        _HASH_BYTES,
    )
    return digest.hex()


def verify_password(plain_password: str, password_digest: str, salt: str) -> bool:
    """校验明文口令与存储摘要是否匹配（恒定时间比较）。"""
    expected = hash_password(plain_password, salt)
    return hmac.compare_digest(expected, password_digest)


def hash_password_with_salt(plain_password: str) -> Tuple[str, str]:
    """生成新盐并返回 (password_hash, password_salt)。"""
    salt = generate_salt()
    password_hash = hash_password(plain_password, salt)
    return password_hash, salt


def verify_password_against_hash(plain_password: str, password_hash: str, password_salt: str) -> bool:
    """兼容 account_store 使用的校验函数签名。"""
    return verify_password(plain_password, password_hash, password_salt)


class CredentialVault:
    """本地凭证保险库，负责口令加盐哈希与校验。"""

    def generate_salt(self) -> str:
        return generate_salt()

    def hash_password(self, plain_password: str, salt: str) -> str:
        return hash_password(plain_password, salt)

    def verify_password(self, plain_password: str, password_digest: str, salt: str) -> bool:
        return verify_password(plain_password, password_digest, salt)
