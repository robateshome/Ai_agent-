# TAG=0xD707;MODULE=DB
# CRC32=0x1B208203; BITS=00011011001000001000001000000011
# DESCRIPTION: SQLite schema and secure API key storage with AES-256 encryption.
#   Use passphrase from environment `DIVERGENCEBOTX_KEY` to encrypt/decrypt keys.
#   Provide DB migration & manifest verification endpoints.

from __future__ import annotations
import os
import sqlite3
from typing import Optional, Tuple
from hashlib import sha256
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
import base64
import secrets

DB_PATH = os.path.join(os.getcwd(), "divergencebotx.sqlite3")


def get_db_path() -> str:
    return DB_PATH


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


def init_db() -> None:
    conn = _get_conn()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts_unix_ms INTEGER NOT NULL,
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                dtype TEXT NOT NULL,
                score INTEGER NOT NULL,
                sig_bit INTEGER NOT NULL,
                payload_json TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS secrets (
                name TEXT PRIMARY KEY,
                salt BLOB NOT NULL,
                nonce BLOB NOT NULL,
                ciphertext BLOB NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
            """
        )
        # manifest version
        conn.execute("INSERT OR IGNORE INTO meta(key, value) VALUES('schema_version','1')")
        conn.commit()
    finally:
        conn.close()


def _derive_key(passphrase: str, salt: bytes) -> bytes:
    kdf = PBKDF2HMAC(algorithm=hashes.SHA256(), length=32, salt=salt, iterations=200_000)
    return kdf.derive(passphrase.encode("utf-8"))


def save_api_key(name: str, api_key: str) -> None:
    init_db()
    passphrase = os.environ.get("DIVERGENCEBOTX_KEY")
    if not passphrase:
        raise RuntimeError("DIVERGENCEBOTX_KEY not set in environment")
    salt = secrets.token_bytes(16)
    key = _derive_key(passphrase, salt)
    aesgcm = AESGCM(key)
    nonce = secrets.token_bytes(12)
    ct = aesgcm.encrypt(nonce, api_key.encode("utf-8"), None)
    conn = _get_conn()
    try:
        conn.execute(
            "REPLACE INTO secrets(name, salt, nonce, ciphertext) VALUES(?,?,?,?)",
            (name, salt, nonce, ct),
        )
        conn.commit()
    finally:
        conn.close()


def get_api_key(name: str) -> Optional[str]:
    init_db()
    passphrase = os.environ.get("DIVERGENCEBOTX_KEY")
    if not passphrase:
        return None
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT salt, nonce, ciphertext FROM secrets WHERE name=?", (name,))
        row = cur.fetchone()
        if not row:
            return None
        salt, nonce, ct = row
        key = _derive_key(passphrase, salt)
        aesgcm = AESGCM(key)
        pt = aesgcm.decrypt(nonce, ct, None)
        return pt.decode("utf-8")
    finally:
        conn.close()


def manifest_info() -> Tuple[str, str]:
    conn = _get_conn()
    try:
        cur = conn.execute("SELECT key, value FROM meta")
        kv = {k: v for (k, v) in cur.fetchall()}
        return kv.get("schema_version", "1"), DB_PATH
    finally:
        conn.close()
