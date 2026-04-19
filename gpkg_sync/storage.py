from __future__ import annotations

import json
import sqlite3
from dataclasses import asdict
from pathlib import Path
from typing import Iterable, List, Optional

import keyring
import keyring.errors

from .models import APP_NAME, SyncProfile


CONFIG_VERSION = 1


class ConfigError(RuntimeError):
    """Raised when local config cannot be loaded safely."""


class SecretStoreError(RuntimeError):
    """Raised when keyring operations fail."""


class SecretStore:
    def __init__(self, service_name: str = APP_NAME):
        self.service_name = service_name

    def secret_key(self, profile: SyncProfile) -> str:
        return f"{profile.name}:{profile.protocol.lower()}:{profile.host}:{profile.username}"

    def _ensure_backend(self) -> None:
        backend = keyring.get_keyring()
        priority = getattr(backend, "priority", 0)
        if priority <= 0:
            raise SecretStoreError("No usable OS keychain backend is available.")

    def get_password(self, profile: SyncProfile) -> str:
        self._ensure_backend()
        try:
            return keyring.get_password(self.service_name, self.secret_key(profile)) or ""
        except keyring.errors.KeyringError as exc:
            raise SecretStoreError(f"Unable to read password from OS keychain: {exc}") from exc

    def set_password(self, profile: SyncProfile, password: str) -> None:
        self._ensure_backend()
        try:
            keyring.set_password(self.service_name, self.secret_key(profile), password)
        except keyring.errors.KeyringError as exc:
            raise SecretStoreError(f"Unable to store password in OS keychain: {exc}") from exc

    def delete_password(self, profile: SyncProfile) -> None:
        self._ensure_backend()
        try:
            keyring.delete_password(self.service_name, self.secret_key(profile))
        except keyring.errors.PasswordDeleteError:
            return
        except keyring.errors.KeyringError as exc:
            raise SecretStoreError(f"Unable to delete password from OS keychain: {exc}") from exc


class SettingsStore:
    def __init__(self, config_path: Path, secret_store: SecretStore):
        self.config_path = config_path
        self.secret_store = secret_store
        self.config_path.parent.mkdir(parents=True, exist_ok=True)

    def load_profiles(self) -> List[SyncProfile]:
        if not self.config_path.exists():
            return []
        with self.config_path.open("r", encoding="utf-8") as handle:
            raw = json.load(handle)

        if isinstance(raw, list):
            profiles = self._migrate_legacy_profiles(raw)
            self.save_profiles(profiles)
            return profiles

        version = raw.get("version")
        if version != CONFIG_VERSION:
            raise ConfigError(f"Unsupported config version: {version}")

        profiles: List[SyncProfile] = []
        for item in raw.get("profiles", []):
            password = ""
            base_profile = SyncProfile.from_metadata(item)
            if item.get("has_saved_password"):
                password = self.secret_store.get_password(base_profile)
            profiles.append(SyncProfile.from_metadata(item, password=password))
        return profiles

    def _migrate_legacy_profiles(self, raw_profiles: Iterable[dict]) -> List[SyncProfile]:
        profiles: List[SyncProfile] = []
        for item in raw_profiles:
            password = item.pop("password", "") or ""
            profile = SyncProfile.from_metadata(item, password=password)
            if password:
                self.secret_store.set_password(profile, password)
                profile.has_saved_password = True
            profiles.append(profile)
        return profiles

    def save_profiles(self, profiles: List[SyncProfile]) -> None:
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "version": CONFIG_VERSION,
            "profiles": [],
        }
        for profile in profiles:
            if profile.password:
                self.secret_store.set_password(profile, profile.password)
            else:
                self.secret_store.delete_password(profile)
            payload["profiles"].append(profile.to_metadata())
        with self.config_path.open("w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)


class StateDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS sync_files (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    profile_name TEXT NOT NULL,
                    local_path TEXT NOT NULL,
                    remote_path TEXT NOT NULL,
                    local_mtime REAL,
                    remote_mtime REAL,
                    local_size INTEGER,
                    remote_size INTEGER,
                    last_synced_hash TEXT,
                    last_sync_time REAL,
                    status TEXT,
                    last_error TEXT,
                    UNIQUE(profile_name, local_path, remote_path)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS app_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts REAL NOT NULL,
                    level TEXT NOT NULL,
                    code TEXT NOT NULL DEFAULT '',
                    message TEXT NOT NULL
                )
                """
            )
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(app_log)").fetchall()
            }
            if "code" not in columns:
                conn.execute("ALTER TABLE app_log ADD COLUMN code TEXT NOT NULL DEFAULT ''")
            conn.commit()

    def upsert_file_state(
        self,
        profile_name: str,
        local_path: str,
        remote_path: str,
        local_mtime: Optional[float],
        remote_mtime: Optional[float],
        local_size: Optional[int],
        remote_size: Optional[int],
        file_hash: Optional[str],
        status: str,
        last_error: str = "",
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO sync_files (
                    profile_name, local_path, remote_path,
                    local_mtime, remote_mtime, local_size, remote_size,
                    last_synced_hash, last_sync_time, status, last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(profile_name, local_path, remote_path)
                DO UPDATE SET
                    local_mtime=excluded.local_mtime,
                    remote_mtime=excluded.remote_mtime,
                    local_size=excluded.local_size,
                    remote_size=excluded.remote_size,
                    last_synced_hash=excluded.last_synced_hash,
                    last_sync_time=excluded.last_sync_time,
                    status=excluded.status,
                    last_error=excluded.last_error
                """,
                (
                    profile_name,
                    local_path,
                    remote_path,
                    local_mtime,
                    remote_mtime,
                    local_size,
                    remote_size,
                    file_hash,
                    now_ts(),
                    status,
                    last_error,
                ),
            )
            conn.commit()

    def get_file_state(self, profile_name: str, local_path: str, remote_path: str) -> Optional[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                """
                SELECT * FROM sync_files
                WHERE profile_name=? AND local_path=? AND remote_path=?
                """,
                (profile_name, local_path, remote_path),
            ).fetchone()

    def get_states_for_profile(self, profile_name: str) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM sync_files WHERE profile_name=? ORDER BY last_sync_time DESC",
                (profile_name,),
            ).fetchall()

    def add_log(self, ts: float, level: str, code: str, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO app_log(ts, level, code, message) VALUES (?, ?, ?, ?)",
                (ts, level, code, message),
            )
            conn.commit()

    def get_recent_logs(self, limit: int = 200) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM app_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()


def now_ts() -> float:
    import time

    return time.time()
