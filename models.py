from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class SQLiteCache:
    def __init__(self, db_path: str) -> None:
        self.db_path = str(Path(db_path))
        self._lock = threading.Lock()
        self._initialized = False

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        with self._lock:
            if self._initialized:
                return
            Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cache_entries (
                        namespace TEXT NOT NULL,
                        cache_key TEXT NOT NULL,
                        json_value TEXT NOT NULL,
                        created_at REAL NOT NULL,
                        expires_at REAL,
                        PRIMARY KEY(namespace, cache_key)
                    )
                    """
                )
                conn.commit()
            self._initialized = True

    def get(self, namespace: str, key: str) -> Any | None:
        self.initialize()
        now = time.time()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT json_value, expires_at FROM cache_entries WHERE namespace=? AND cache_key=?",
                (namespace, key),
            ).fetchone()
            if row is None:
                return None
            expires_at = row["expires_at"]
            if expires_at is not None and float(expires_at) < now:
                conn.execute("DELETE FROM cache_entries WHERE namespace=? AND cache_key=?", (namespace, key))
                conn.commit()
                return None
            return json.loads(row["json_value"])

    def set(self, namespace: str, key: str, value: Any, ttl_seconds: int | None = None) -> None:
        self.initialize()
        now = time.time()
        expires_at = (now + ttl_seconds) if ttl_seconds else None
        payload = json.dumps(value, ensure_ascii=True)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO cache_entries(namespace, cache_key, json_value, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(namespace, cache_key) DO UPDATE SET
                    json_value=excluded.json_value,
                    created_at=excluded.created_at,
                    expires_at=excluded.expires_at
                """,
                (namespace, key, payload, now, expires_at),
            )
            conn.commit()

    def clear(self, namespace: str | None = None) -> int:
        self.initialize()
        with self._connect() as conn:
            if namespace is None:
                cursor = conn.execute("DELETE FROM cache_entries")
            else:
                cursor = conn.execute("DELETE FROM cache_entries WHERE namespace=?", (namespace,))
            conn.commit()
            return int(cursor.rowcount or 0)
