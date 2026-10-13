import sqlite3
import secrets
import hashlib
import hmac
import time
import logging
from typing import Optional, Tuple, Dict, Any

from store.db_connection import get_connection, transaction_scope
from auth.credential_vault import hash_password_with_salt, verify_password_against_hash

logger = logging.getLogger(__name__)

ACCOUNT_TABLE = "user_account"
PASSWORD_RESET_TABLE = "password_reset_token"

_ACCOUNT_COLUMNS = (
    "account_id",
    "login_name",
    "display_name",
    "password_hash",
    "password_salt",
    "role_code",
    "account_status",
    "created_at",
    "updated_at",
    "last_login_at",
)

_RESET_COLUMNS = (
    "reset_token_id",
    "account_id",
    "token_digest",
    "issued_at",
    "expires_at",
    "consumed_at",
)


def _row_to_account(record: sqlite3.Row) -> Dict[str, Any]:
    return {
        "account_id": record["account_id"],
        "login_name": record["login_name"],
        "display_name": record["display_name"],
        "role_code": record["role_code"],
        "account_status": record["account_status"],
        "created_at": record["created_at"],
        "updated_at": record["updated_at"],
        "last_login_at": record["last_login_at"],
    }


def find_account_by_login_name(login_name: str) -> Optional[Dict[str, Any]]:
    with get_connection() as connection:
        cursor = connection.execute(
            "SELECT account_id, login_name, display_name, password_hash, "
            "password_salt, role_code, account_status, created_at, updated_at, last_login_at "
            "FROM user_account WHERE login_name = ? LIMIT 1",
            (login_name,),
        )
        record = cursor.fetchone()
        if record is None:
            return None
        account_view = _row_to_account(record)
        account_view["password_hash"] = record["password_hash"]
        account_view["password_salt"] = record["password_salt"]
        return account_view


def find_account_by_identifier(identifier: str) -> Optional[Dict[str, Any]]:
    account = find_account_by_login_name(identifier)
    if account is not None:
        return account
    with get_connection() as connection:
        cursor = connection.execute(
            "SELECT account_id, login_name, display_name, password_hash, "
            "password_salt, role_code, account_status, created_at, updated_at, last_login_at "
            "FROM user_account WHERE account_id = ? LIMIT 1",
            (identifier,),
        )
        record = cursor.fetchone()
        if record is None:
            return None
        account_view = _row_to_account(record)
        account_view["password_hash"] = record["password_hash"]
        account_view["password_salt"] = record["password_salt"]
        return account_view


def register_account(
    login_name: str,
    display_name: str,
    plain_password: str,
    role_code: str = "standard_user",
) -> Dict[str, Any]:
    with get_connection() as connection:
        cursor = connection.execute(
            "SELECT login_name FROM user_account WHERE login_name = ?",
            (login_name,),
        )
        login_taken = False
        for record in cursor.fetchall():
            if record["login_name"] == login_name:
                login_taken = True
        if login_taken:
            raise ValueError("登录名已被占用")
    password_hash, password_salt = hash_password_with_salt(plain_password)
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        cursor = connection.execute(
            "INSERT INTO user_account "
            "(login_name, display_name, password_hash, password_salt, "
            "role_code, account_status, created_at, updated_at, last_login_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                login_name,
                display_name,
                password_hash,
                password_salt,
                role_code,
                "active",
                now_epoch,
                now_epoch,
                None,
            ),
        )
        new_account_id = cursor.lastrowid
        logger.info("account registered account_id=%s login_name=%s", new_account_id, login_name)
        return {
            "account_id": new_account_id,
            "login_name": login_name,
            "display_name": display_name,
            "role_code": role_code,
            "account_status": "active",
        }


def authenticate_account(login_name: str, plain_password: str) -> Optional[Dict[str, Any]]:
    account = find_account_by_login_name(login_name)
    if account is None:
        return None
    if account["account_status"] != "active":
        logger.warning("account disabled login_name=%s status=%s", login_name, account["account_status"])
        return None
    password_matched = verify_password_against_hash(
        plain_password,
        account["password_hash"],
        account["password_salt"],
    )
    if not password_matched:
        return None
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        connection.execute(
            "UPDATE user_account SET last_login_at = ?, updated_at = ? WHERE account_id = ?",
            (now_epoch, now_epoch, account["account_id"]),
        )
    account.pop("password_hash", None)
    account.pop("password_salt", None)
    account["last_login_at"] = now_epoch
    return account


def update_account_password(account_id: int, new_plain_password: str) -> bool:
    password_hash, password_salt = hash_password_with_salt(new_plain_password)
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        cursor = connection.execute(
            "UPDATE user_account SET password_hash = ?, password_salt = ?, updated_at = ? "
            "WHERE account_id = ?",
            (password_hash, password_salt, now_epoch, account_id),
        )
        updated = cursor.rowcount > 0
        if updated:
            logger.info("account password updated account_id=%s", account_id)
        return updated


def issue_password_reset_token(login_name: str, ttl_seconds: int = 3600) -> Optional[Dict[str, Any]]:
    account = find_account_by_login_name(login_name)
    if account is None:
        return None
    raw_token = secrets.token_urlsafe(32)
    token_digest = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    now_epoch = int(time.time())
    expires_epoch = now_epoch + ttl_seconds
    with transaction_scope() as connection:
        connection.execute(
            "UPDATE password_reset_token SET consumed_at = ? "
            "WHERE account_id = ? AND consumed_at IS NULL",
            (now_epoch, account["account_id"]),
        )
        connection.execute(
            "INSERT INTO password_reset_token "
            "(account_id, token_digest, issued_at, expires_at, consumed_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (account["account_id"], token_digest, now_epoch, expires_epoch, None),
        )
        return {
            "account_id": account["account_id"],
            "reset_token": raw_token,
            "expires_at": expires_epoch,
        }


def consume_password_reset_token(raw_token: str, new_plain_password: str) -> bool:
    token_digest = hashlib.sha256(raw_token.encode("utf-8")).hexdigest()
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        cursor = connection.execute(
            "SELECT reset_token_id, account_id, expires_at, consumed_at "
            "FROM password_reset_token WHERE token_digest = ? LIMIT 1",
            (token_digest,),
        )
        record = cursor.fetchone()
        if record is None:
            return False
        if record["consumed_at"] is not None:
            return False
        if record["expires_at"] < now_epoch:
            return False
        password_hash, password_salt = hash_password_with_salt(new_plain_password)
        connection.execute(
            "UPDATE user_account SET password_hash = ?, password_salt = ?, updated_at = ? "
            "WHERE account_id = ?",
            (password_hash, password_salt, now_epoch, record["account_id"]),
        )
        connection.execute(
            "UPDATE password_reset_token SET consumed_at = ? WHERE reset_token_id = ?",
            (now_epoch, record["reset_token_id"]),
        )
        logger.info("password reset consumed account_id=%s", record["account_id"])
        return True


def purge_expired_reset_tokens(retention_seconds: int = 86400) -> int:
    cutoff_epoch = int(time.time()) - retention_seconds
    with transaction_scope() as connection:
        cursor = connection.execute(
            "DELETE FROM password_reset_token WHERE expires_at < ?",
            (cutoff_epoch,),
        )
        removed_count = cursor.rowcount
        if removed_count:
            logger.debug("purged expired reset tokens count=%s", removed_count)
        return removed_count


def list_accounts_for_admin(role_filter: Optional[str] = None) -> Tuple[Dict[str, Any], ...]:
    with get_connection() as connection:
        if role_filter:
            cursor = connection.execute(
                "SELECT account_id, login_name, display_name, role_code, "
                "account_status, created_at, updated_at, last_login_at "
                "FROM user_account WHERE role_code = ? ORDER BY account_id ASC",
                (role_filter,),
            )
        else:
            cursor = connection.execute(
                "SELECT account_id, login_name, display_name, role_code, "
                "account_status, created_at, updated_at, last_login_at "
                "FROM user_account ORDER BY account_id ASC"
            )
        return tuple(_row_to_account(record) for record in cursor.fetchall())


def change_account_status(account_id: int, target_status: str) -> bool:
    if target_status not in ("active", "suspended", "locked"):
        raise ValueError("非法的账户状态")
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        cursor = connection.execute(
            "UPDATE user_account SET account_status = ?, updated_at = ? WHERE account_id = ?",
            (target_status, now_epoch, account_id),
        )
        return cursor.rowcount > 0


def change_account_role(account_id: int, target_role: str) -> bool:
    if target_role not in ("standard_user", "analyst", "administrator"):
        raise ValueError("非法的角色编码")
    now_epoch = int(time.time())
    with transaction_scope() as connection:
        cursor = connection.execute(
            "UPDATE user_account SET role_code = ?, updated_at = ? WHERE account_id = ?",
            (target_role, now_epoch, account_id),
        )
        return cursor.rowcount > 0
