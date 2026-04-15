#!/usr/bin/env python3
"""
gpkg sync
---------
Cross-platform GeoPackage sync desktop app for Windows and Linux.

Features
- PySide6 desktop UI
- SFTP sync using Paramiko
- Watches local folders for .gpkg changes using watchdog
- Safe delayed sync after file stability check
- Upload-only / Download-only / Two-way modes
- Conflict detection with timestamped conflict copies
- Backup before overwrite
- SQLite state tracking
- System tray support
- Optional auto-start at app launch
- Designed as a single-file starter app for easier deployment

Requirements:
    pip install PySide6 paramiko watchdog

Run:
    python gpkg_sync_app.py

Notes:
- This app syncs whole .gpkg files, not row-level GIS edits.
- For reliability, it waits for files to become stable before syncing.
- It avoids silent overwrite when both local and remote changed.
"""

from __future__ import annotations

import contextlib
import ftplib
import hashlib
import json
import os
import posixpath
import shutil
import sqlite3
import stat
import sys
import threading
import time
import traceback
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple

import paramiko
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from PySide6.QtCore import QMutex, QObject, QPoint, Qt, QThread, QTimer, Signal
from PySide6.QtGui import QAction, QCloseEvent, QIcon, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QSpinBox,
    QSystemTrayIcon,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

APP_NAME = "gpkg sync"
APP_VERSION = "0.1.0"
APP_DIR = Path.home() / ".gpkg_sync"
DB_PATH = APP_DIR / "gpkg_sync.db"
CONFIG_PATH = APP_DIR / "profiles.json"
LOG_MAX_LINES = 1500
SYNC_EXTENSIONS = {".gpkg"}


# ----------------------------- Utility Helpers ----------------------------- #


def now_ts() -> float:
    return time.time()



def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")



def ensure_app_dir() -> None:
    APP_DIR.mkdir(parents=True, exist_ok=True)



def sha1_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    h = hashlib.sha1()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()



def is_gpkg(path: Path) -> bool:
    return path.suffix.lower() in SYNC_EXTENSIONS



def safe_relpath(file_path: Path, root: Path) -> str:
    return str(file_path.resolve().relative_to(root.resolve())).replace("\\", "/")



def make_conflict_name(path: Path, device_label: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return path.with_name(f"{path.stem}.conflict-{device_label}-{stamp}{path.suffix}")



def make_backup_name(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return path.with_name(f"{path.stem}.backup-{stamp}{path.suffix}")



def file_snapshot(path: Path) -> Optional[Tuple[int, float]]:
    try:
        st = path.stat()
        return st.st_size, st.st_mtime
    except FileNotFoundError:
        return None



def is_file_stable(path: Path, checks: int = 3, delay: float = 1.5) -> bool:
    last = None
    for _ in range(checks):
        snap = file_snapshot(path)
        if snap is None:
            return False
        if last is not None and snap != last:
            last = snap
            time.sleep(delay)
            continue
        last = snap
        time.sleep(delay)
    final_snap = file_snapshot(path)
    return final_snap == last and final_snap is not None



def local_file_accessible(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except OSError:
        return False



def normalize_remote_path(remote_root: str, rel: str) -> str:
    pure = PurePosixPath(remote_root) / PurePosixPath(rel)
    return str(pure).replace("\\", "/")


# ----------------------------- Data Models -------------------------------- #


@dataclass
class SyncProfile:
    name: str
    host: str
    port: int
    username: str
    password: str = ""
    key_path: str = ""
    protocol: str = "sftp"
    local_dir: str = ""
    remote_dir: str = ""
    direction: str = "two-way"  # upload-only, download-only, two-way
    auto_start: bool = False
    backup_before_overwrite: bool = True
    delete_missing: bool = False
    device_label: str = "device"
    stability_wait_seconds: int = 5

    def validate(self) -> Tuple[bool, str]:
        if not self.name.strip():
            return False, "Profile name is required."
        if not self.host.strip():
            return False, "Host is required."
        if not self.username.strip():
            return False, "Username is required."
        if not self.local_dir.strip():
            return False, "Local directory is required."
        if not self.remote_dir.strip():
            return False, "Remote directory is required."
        if not Path(self.local_dir).exists():
            return False, "Local directory does not exist."
        protocol = self.protocol.lower()
        if protocol not in {"sftp", "ftp", "ftps"}:
            return False, "Invalid protocol."
        if self.direction not in {"upload-only", "download-only", "two-way"}:
            return False, "Invalid sync direction."
        if protocol == "sftp":
            if not self.password and not self.key_path:
                return False, "Provide either password or SSH key path."
        else:
            if not self.password and self.username.lower() != "anonymous":
                return False, "Password is required for FTP/FTPS."
        return True, ""


# ----------------------------- Persistence -------------------------------- #


class SettingsStore:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        ensure_app_dir()

    def load_profiles(self) -> List[SyncProfile]:
        if not self.config_path.exists():
            return []
        with self.config_path.open("r", encoding="utf-8") as f:
            raw = json.load(f)
        return [SyncProfile(**item) for item in raw]

    def save_profiles(self, profiles: List[SyncProfile]) -> None:
        ensure_app_dir()
        with self.config_path.open("w", encoding="utf-8") as f:
            json.dump([asdict(p) for p in profiles], f, indent=2)


class StateDB:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        ensure_app_dir()
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
                    message TEXT NOT NULL
                )
                """
            )
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
            row = conn.execute(
                """
                SELECT * FROM sync_files
                WHERE profile_name=? AND local_path=? AND remote_path=?
                """,
                (profile_name, local_path, remote_path),
            ).fetchone()
            return row

    def get_states_for_profile(self, profile_name: str) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM sync_files WHERE profile_name=? ORDER BY last_sync_time DESC",
                (profile_name,),
            ).fetchall()

    def add_log(self, level: str, message: str) -> None:
        with self._connect() as conn:
            conn.execute(
                "INSERT INTO app_log(ts, level, message) VALUES (?, ?, ?)",
                (now_ts(), level, message),
            )
            conn.commit()

    def get_recent_logs(self, limit: int = 200) -> List[sqlite3.Row]:
        with self._connect() as conn:
            return conn.execute(
                "SELECT * FROM app_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()


# ----------------------------- SFTP Layer --------------------------------- #


class SFTPManager:
    def __init__(self, profile: SyncProfile):
        self.profile = profile
        self.transport: Optional[paramiko.Transport] = None
        self.sftp: Optional[paramiko.SFTPClient] = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            if self.sftp is not None:
                return
            transport = paramiko.Transport((self.profile.host, self.profile.port))
            if self.profile.key_path:
                key = paramiko.RSAKey.from_private_key_file(self.profile.key_path)
                transport.connect(username=self.profile.username, pkey=key)
            else:
                transport.connect(username=self.profile.username, password=self.profile.password)
            self.transport = transport
            self.sftp = paramiko.SFTPClient.from_transport(transport)

    def close(self) -> None:
        with self._lock:
            with contextlib.suppress(Exception):
                if self.sftp:
                    self.sftp.close()
            with contextlib.suppress(Exception):
                if self.transport:
                    self.transport.close()
            self.sftp = None
            self.transport = None

    def ensure_connected(self) -> None:
        if self.sftp is None:
            self.connect()

    def stat(self, remote_path: str):
        self.ensure_connected()
        assert self.sftp is not None
        return self.sftp.stat(remote_path)

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        self.ensure_connected()
        assert self.sftp is not None
        current = "/"
        parts = [p for p in PurePosixPath(remote_dir).parts if p not in ("/", "")]
        for part in parts:
            current = posixpath.join(current, part)
            try:
                self.sftp.stat(current)
            except FileNotFoundError:
                self.sftp.mkdir(current)

    def upload(self, local_path: Path, remote_path: str, callback: Optional[Callable[[int, int], None]] = None) -> None:
        self.ensure_connected()
        assert self.sftp is not None
        self.mkdirs(str(PurePosixPath(remote_path).parent))
        self.sftp.put(str(local_path), remote_path, callback=callback)

    def download(self, remote_path: str, local_path: Path, callback: Optional[Callable[[int, int], None]] = None) -> None:
        self.ensure_connected()
        assert self.sftp is not None
        local_path.parent.mkdir(parents=True, exist_ok=True)
        self.sftp.get(remote_path, str(local_path), callback=callback)

    def remove(self, remote_path: str) -> None:
        self.ensure_connected()
        assert self.sftp is not None
        self.sftp.remove(remote_path)

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        self.ensure_connected()
        assert self.sftp is not None
        found: List[Dict[str, object]] = []

        def _walk(path: str) -> None:
            for entry in self.sftp.listdir_attr(path):
                child = posixpath.join(path, entry.filename)
                if stat.S_ISDIR(entry.st_mode):
                    _walk(child)
                else:
                    if Path(entry.filename).suffix.lower() in SYNC_EXTENSIONS:
                        found.append({
                            "path": child,
                            "size": entry.st_size,
                            "mtime": entry.st_mtime,
                        })

        try:
            _walk(remote_root)
        except FileNotFoundError:
            self.mkdirs(remote_root)
        return found


class FTPManager:
    def __init__(self, profile: SyncProfile):
        self.profile = profile
        self.ftp: Optional[ftplib.FTP] = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            if self.ftp is not None:
                return
            if self.profile.protocol.lower() == "ftps":
                ftp = ftplib.FTP_TLS()
            else:
                ftp = ftplib.FTP()
            ftp.connect(self.profile.host, self.profile.port, timeout=30)
            ftp.login(self.profile.username, self.profile.password or "")
            if self.profile.protocol.lower() == "ftps":
                ftp.prot_p()
            self.ftp = ftp

    def close(self) -> None:
        with self._lock:
            with contextlib.suppress(Exception):
                if self.ftp:
                    self.ftp.quit()
            with contextlib.suppress(Exception):
                if self.ftp:
                    self.ftp.close()
            self.ftp = None

    def ensure_connected(self) -> None:
        if self.ftp is None:
            self.connect()

    def stat(self, remote_path: str):
        self.ensure_connected()
        assert self.ftp is not None
        try:
            size = self.ftp.size(remote_path)
        except Exception as exc:
            raise FileNotFoundError from exc
        mtime = 0.0
        try:
            response = self.ftp.sendcmd(f"MDTM {remote_path}")
            if response.startswith("213 "):
                stamp = response[4:].strip()
                mtime = time.mktime(time.strptime(stamp, "%Y%m%d%H%M%S"))
        except Exception:
            pass
        return SimpleNamespace(st_size=size, st_mtime=mtime)

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        self.ensure_connected()
        assert self.ftp is not None
        path = str(PurePosixPath(remote_dir))
        if not path or path == ".":
            return
        parts = [p for p in PurePosixPath(path).parts if p not in ("/", "")]
        current = "" if not path.startswith("/") else "/"
        for part in parts:
            current = posixpath.join(current, part) if current else part
            try:
                self.ftp.cwd(current)
            except Exception:
                with contextlib.suppress(Exception):
                    self.ftp.mkd(current)
                self.ftp.cwd(current)

    def upload(
        self,
        local_path: Path,
        remote_path: str,
        callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.ensure_connected()
        assert self.ftp is not None
        self.mkdirs(str(PurePosixPath(remote_path).parent))
        total = max(1, local_path.stat().st_size)
        sent = 0
        with local_path.open("rb") as f:
            def store_cb(data: bytes) -> None:
                nonlocal sent
                sent += len(data)
                if callback:
                    callback(sent, total)
            self.ftp.storbinary(f"STOR {remote_path}", f, 8192, callback=store_cb)

    def download(
        self,
        remote_path: str,
        local_path: Path,
        callback: Optional[Callable[[int, int], None]] = None,
    ) -> None:
        self.ensure_connected()
        assert self.ftp is not None
        local_path.parent.mkdir(parents=True, exist_ok=True)
        total = max(1, self.ftp.size(remote_path) or 1)
        received = 0
        with local_path.open("wb") as f:
            def retr_cb(data: bytes) -> None:
                nonlocal received
                f.write(data)
                received += len(data)
                if callback:
                    callback(received, total)
            self.ftp.retrbinary(f"RETR {remote_path}", retr_cb, 8192)

    def remove(self, remote_path: str) -> None:
        self.ensure_connected()
        assert self.ftp is not None
        self.ftp.delete(remote_path)

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        self.ensure_connected()
        assert self.ftp is not None
        found: List[Dict[str, object]] = []
        try:
            entries = list(self.ftp.mlsd(remote_root))
        except Exception:
            entries = None

        if entries is not None:
            for name, facts in entries:
                if name in (".", ".."):
                    continue
                path = posixpath.join(remote_root, name)
                if facts.get("type") == "dir":
                    found.extend(self.walk_remote_files(path))
                elif facts.get("type") == "file" and Path(name).suffix.lower() in SYNC_EXTENSIONS:
                    size = int(facts.get("size", 0))
                    mtime = 0.0
                    modify = facts.get("modify")
                    if modify:
                        try:
                            mtime = time.mktime(time.strptime(modify, "%Y%m%d%H%M%S"))
                        except Exception:
                            pass
                    found.append({"path": path, "size": size, "mtime": mtime})
            return found

        def _walk(path: str) -> None:
            try:
                entries = self.ftp.nlst(path)
            except Exception:
                return
            for name in entries:
                if name in (".", ".."):
                    continue
                candidate = name if name.startswith("/") else posixpath.join(path, name)
                try:
                    self.ftp.cwd(candidate)
                    _walk(candidate)
                    self.ftp.cwd(path)
                except Exception:
                    if Path(name).suffix.lower() in SYNC_EXTENSIONS:
                        try:
                            size = self.ftp.size(candidate)
                        except Exception:
                            size = 0
                        mtime = 0.0
                        try:
                            response = self.ftp.sendcmd(f"MDTM {candidate}")
                            if response.startswith("213 "):
                                stamp = response[4:].strip()
                                mtime = time.mktime(time.strptime(stamp, "%Y%m%d%H%M%S"))
                        except Exception:
                            pass
                        found.append({"path": candidate, "size": size, "mtime": mtime})

        try:
            _walk(remote_root)
        except Exception:
            self.mkdirs(remote_root)
        return found

# ----------------------------- Watcher ------------------------------------ #


class LocalWatcherHandler(FileSystemEventHandler):
    def __init__(self, on_change: Callable[[Path], None]):
        super().__init__()
        self.on_change = on_change

    def _handle(self, path_str: str) -> None:
        path = Path(path_str)
        if is_gpkg(path):
            self.on_change(path)

    def on_created(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._handle(event.src_path)

    def on_moved(self, event):
        if not event.is_directory:
            self._handle(event.dest_path)

    def on_deleted(self, event):
        if not event.is_directory:
            self._handle(Path(event.src_path))


class FolderWatcher(QObject):
    file_changed = Signal(str)

    def __init__(self, root: Path):
        super().__init__()
        self.root = root
        self.observer: Optional[Observer] = None

    def start(self) -> None:
        if self.observer is not None:
            return
        handler = LocalWatcherHandler(lambda p: self.file_changed.emit(str(p)))
        self.observer = Observer()
        self.observer.schedule(handler, str(self.root), recursive=True)
        self.observer.start()

    def stop(self) -> None:
        if self.observer is None:
            return
        self.observer.stop()
        self.observer.join(timeout=5)
        self.observer = None


# ----------------------------- Sync Engine -------------------------------- #


class SyncEngine(QObject):
    log = Signal(str, str)
    status_changed = Signal(str)
    progress_changed = Signal(str, int)
    file_synced = Signal(dict)

    def __init__(self, profile: SyncProfile, db: StateDB):
        super().__init__()
        self.profile = profile
        self.db = db
        if profile.protocol.lower() in {"ftp", "ftps"}:
            self.sftp = FTPManager(profile)
        else:
            self.sftp = SFTPManager(profile)
        self.watcher: Optional[FolderWatcher] = None
        self.running = False
        self.scan_timer: Optional[QTimer] = None
        self.pending_files: Dict[str, float] = {}
        self.pending_lock = threading.Lock()
        self.mutex = QMutex()
        self._last_full_remote_scan = 0.0

    def emit_log(self, level: str, message: str) -> None:
        self.db.add_log(level, f"[{self.profile.name}] {message}")
        self.log.emit(level, message)

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.emit_log("INFO", "Starting sync engine...")
        self.status_changed.emit("Connecting")
        try:
            self.sftp.connect()
            self.status_changed.emit("Watching")
            self.emit_log("INFO", "Connected to remote server.")
        except Exception as e:
            self.status_changed.emit("Connection failed")
            self.emit_log("ERROR", f"Failed to connect: {e}")
            self.running = False
            return

        self.watcher = FolderWatcher(Path(self.profile.local_dir))
        self.watcher.file_changed.connect(self.on_local_file_event)
        self.watcher.start()

        self.scan_timer = QTimer()
        self.scan_timer.timeout.connect(self.process_pending_and_poll_remote)
        self.scan_timer.start(3000)

        self.full_sync()

    def stop(self) -> None:
        self.running = False
        if self.scan_timer is not None:
            self.scan_timer.stop()
            self.scan_timer.deleteLater()
            self.scan_timer = None
        if self.watcher is not None:
            self.watcher.stop()
            self.watcher.deleteLater()
            self.watcher = None
        self.sftp.close()
        self.status_changed.emit("Stopped")
        self.emit_log("INFO", "Sync engine stopped.")

    def on_local_file_event(self, path_str: str) -> None:
        if not self.running:
            return
        path = Path(path_str)
        rel = None
        try:
            rel = safe_relpath(path, Path(self.profile.local_dir))
        except Exception:
            if path.exists() and Path(self.profile.local_dir) not in path.parents and path != Path(self.profile.local_dir):
                return
        with self.pending_lock:
            self.pending_files[str(path)] = now_ts()
        if rel:
            self.emit_log("INFO", f"Queued local change: {rel}")

    def process_pending_and_poll_remote(self) -> None:
        if not self.running:
            return
        self.process_pending_local_changes()
        if now_ts() - self._last_full_remote_scan >= 15:
            self.poll_remote_changes()
            self._last_full_remote_scan = now_ts()

    def process_pending_local_changes(self) -> None:
        cutoff = now_ts() - max(2, self.profile.stability_wait_seconds)
        ready: List[str] = []
        with self.pending_lock:
            for path_str, queued_at in list(self.pending_files.items()):
                if queued_at <= cutoff:
                    ready.append(path_str)
                    self.pending_files.pop(path_str, None)

        for path_str in ready:
            path = Path(path_str)
            if self.profile.direction == "download-only":
                continue
            try:
                self.sync_local_change(path)
            except Exception as e:
                self.emit_log("ERROR", f"Local sync failed for {path.name}: {e}")
                self.emit_log("DEBUG", traceback.format_exc())

    def full_sync(self) -> None:
        self.emit_log("INFO", "Running full sync...")
        self.status_changed.emit("Full sync")
        local_root = Path(self.profile.local_dir)
        local_files = [p for p in local_root.rglob("*") if p.is_file() and is_gpkg(p)]
        remote_files = self.sftp.walk_remote_files(self.profile.remote_dir)
        remote_map = {
            posixpath.relpath(r["path"], self.profile.remote_dir): r for r in remote_files
        }
        local_map = {safe_relpath(p, local_root): p for p in local_files}

        # Process local files
        for rel, local_path in local_map.items():
            remote_path = normalize_remote_path(self.profile.remote_dir, rel)
            remote_info = remote_map.get(rel)
            self.reconcile_single(local_path, remote_path, remote_info)

        # Process remote-only files
        if self.profile.direction in {"download-only", "two-way"}:
            for rel, remote_info in remote_map.items():
                if rel not in local_map:
                    local_path = local_root / rel
                    self.handle_remote_only(local_path, remote_info)

        self.status_changed.emit("Watching")
        self.emit_log("INFO", "Full sync complete.")

    def poll_remote_changes(self) -> None:
        if self.profile.direction == "upload-only":
            return
        self.emit_log("DEBUG", "Polling remote changes...")
        local_root = Path(self.profile.local_dir)
        remote_files = self.sftp.walk_remote_files(self.profile.remote_dir)
        for remote_info in remote_files:
            rel = posixpath.relpath(remote_info["path"], self.profile.remote_dir)
            local_path = local_root / rel
            if not local_path.exists():
                self.handle_remote_only(local_path, remote_info)
                continue
            remote_path = str(remote_info["path"])
            self.reconcile_single(local_path, remote_path, remote_info)

    def reconcile_single(self, local_path: Path, remote_path: str, remote_info: Optional[Dict[str, object]] = None) -> None:
        local_stat = local_path.stat()
        local_mtime = float(local_stat.st_mtime)
        local_size = int(local_stat.st_size)
        rel = safe_relpath(local_path, Path(self.profile.local_dir))

        if remote_info is None:
            try:
                rstat = self.sftp.stat(remote_path)
                remote_info = {"path": remote_path, "mtime": float(rstat.st_mtime), "size": int(rstat.st_size)}
            except FileNotFoundError:
                remote_info = None

        if remote_info is None:
            if self.profile.direction in {"upload-only", "two-way"}:
                self.upload_local_file(local_path, remote_path, reason="new local file")
            return

        remote_mtime = float(remote_info["mtime"])
        remote_size = int(remote_info["size"])
        state = self.db.get_file_state(self.profile.name, str(local_path), remote_path)
        last_local = float(state["local_mtime"]) if state and state["local_mtime"] else None
        last_remote = float(state["remote_mtime"]) if state and state["remote_mtime"] else None

        local_changed = last_local is None or local_mtime != last_local or local_size != int(state["local_size"] or 0)
        remote_changed = last_remote is None or remote_mtime != last_remote or remote_size != int(state["remote_size"] or 0)

        if not local_changed and not remote_changed:
            return

        if local_changed and remote_changed:
            # Conflict-safe resolution
            self.resolve_conflict(local_path, remote_path, remote_mtime, remote_size)
            return

        if local_changed:
            if self.profile.direction in {"upload-only", "two-way"}:
                self.upload_local_file(local_path, remote_path, reason="local newer")
        elif remote_changed:
            if self.profile.direction in {"download-only", "two-way"}:
                self.download_remote_file(remote_path, local_path, remote_mtime, remote_size, reason="remote newer")

        self.emit_log("DEBUG", f"Checked: {rel}")

    def handle_remote_only(self, local_path: Path, remote_info: Dict[str, object]) -> None:
        remote_path = str(remote_info["path"])
        if self.profile.direction in {"download-only", "two-way"}:
            self.download_remote_file(
                remote_path,
                local_path,
                float(remote_info["mtime"]),
                int(remote_info["size"]),
                reason="remote-only file",
            )

    def sync_local_change(self, local_path: Path) -> None:
        if not local_path.exists():
            # Optional delete propagation
            if not self.profile.delete_missing:
                return
            rel = safe_relpath(local_path.parent / local_path.name, Path(self.profile.local_dir))
            remote_path = normalize_remote_path(self.profile.remote_dir, rel)
            if self.profile.direction in {"upload-only", "two-way"} and self.sftp.exists(remote_path):
                self.emit_log("INFO", f"Deleting remote file: {rel}")
                self.sftp.remove(remote_path)
            return

        if not is_file_stable(local_path, delay=1.0):
            self.emit_log("INFO", f"Waiting for stable file: {local_path.name}")
            with self.pending_lock:
                self.pending_files[str(local_path)] = now_ts()
            return

        if not local_file_accessible(local_path):
            self.emit_log("INFO", f"File still in use: {local_path.name}")
            with self.pending_lock:
                self.pending_files[str(local_path)] = now_ts()
            return

        rel = safe_relpath(local_path, Path(self.profile.local_dir))
        remote_path = normalize_remote_path(self.profile.remote_dir, rel)
        try:
            rstat = self.sftp.stat(remote_path)
            remote_info = {"path": remote_path, "mtime": float(rstat.st_mtime), "size": int(rstat.st_size)}
        except FileNotFoundError:
            remote_info = None
        self.reconcile_single(local_path, remote_path, remote_info)

    def resolve_conflict(self, local_path: Path, remote_path: str, remote_mtime: float, remote_size: int) -> None:
        rel = safe_relpath(local_path, Path(self.profile.local_dir))
        conflict_path = make_conflict_name(local_path, self.profile.device_label)
        self.emit_log("WARNING", f"Conflict detected for {rel}. Downloading remote as conflict copy.")
        self.download_remote_file(remote_path, conflict_path, remote_mtime, remote_size, reason="conflict copy")
        # Keep local file as-is; update state using current local and remote metadata.
        local_stat = local_path.stat()
        self.db.upsert_file_state(
            self.profile.name,
            str(local_path),
            remote_path,
            float(local_stat.st_mtime),
            remote_mtime,
            int(local_stat.st_size),
            remote_size,
            None,
            "conflict",
            "Conflict copy created",
        )

    def upload_local_file(self, local_path: Path, remote_path: str, reason: str) -> None:
        rel = safe_relpath(local_path, Path(self.profile.local_dir))
        self.status_changed.emit(f"Uploading {local_path.name}")
        self.emit_log("INFO", f"Uploading {rel} ({reason})")

        total = max(1, local_path.stat().st_size)

        def cb(sent: int, _total: int) -> None:
            pct = max(0, min(100, int((sent / total) * 100)))
            self.progress_changed.emit(rel, pct)

        self.sftp.upload(local_path, remote_path, callback=cb)
        rstat = self.sftp.stat(remote_path)
        file_hash = sha1_file(local_path)
        lst = local_path.stat()
        self.db.upsert_file_state(
            self.profile.name,
            str(local_path),
            remote_path,
            float(lst.st_mtime),
            float(rstat.st_mtime),
            int(lst.st_size),
            int(rstat.st_size),
            file_hash,
            "synced",
            "",
        )
        self.progress_changed.emit(rel, 100)
        self.file_synced.emit({
            "profile": self.profile.name,
            "file": rel,
            "direction": "upload",
            "time": fmt_ts(now_ts()),
            "status": "synced",
        })
        self.status_changed.emit("Watching")
        self.emit_log("INFO", f"Upload complete: {rel}")

    def download_remote_file(self, remote_path: str, local_path: Path, remote_mtime: float, remote_size: int, reason: str) -> None:
        rel = str(local_path.relative_to(Path(self.profile.local_dir))) if str(local_path).startswith(self.profile.local_dir) else local_path.name
        self.status_changed.emit(f"Downloading {local_path.name}")
        self.emit_log("INFO", f"Downloading {rel} ({reason})")

        if local_path.exists() and self.profile.backup_before_overwrite and reason != "conflict copy":
            backup_path = make_backup_name(local_path)
            shutil.copy2(local_path, backup_path)
            self.emit_log("INFO", f"Backup created: {backup_path.name}")

        total = max(1, remote_size)

        def cb(done: int, _total: int) -> None:
            pct = max(0, min(100, int((done / total) * 100)))
            self.progress_changed.emit(rel, pct)

        self.sftp.download(remote_path, local_path, callback=cb)
        with contextlib.suppress(Exception):
            os.utime(local_path, (remote_mtime, remote_mtime))
        lst = local_path.stat()
        file_hash = sha1_file(local_path)
        self.db.upsert_file_state(
            self.profile.name,
            str(local_path),
            remote_path,
            float(lst.st_mtime),
            remote_mtime,
            int(lst.st_size),
            remote_size,
            file_hash,
            "synced" if reason != "conflict copy" else "conflict-copy",
            "",
        )
        self.progress_changed.emit(rel, 100)
        self.file_synced.emit({
            "profile": self.profile.name,
            "file": rel,
            "direction": "download",
            "time": fmt_ts(now_ts()),
            "status": "synced",
        })
        self.status_changed.emit("Watching")
        self.emit_log("INFO", f"Download complete: {rel}")

    def test_connection(self) -> Tuple[bool, str]:
        try:
            self.sftp.connect()
            self.sftp.mkdirs(self.profile.remote_dir)
            self.sftp.close()
            return True, "Connection successful."
        except Exception as e:
            return False, str(e)


# ----------------------------- Profile Dialog ----------------------------- #


class ProfileDialog(QDialog):
    def __init__(self, parent: Optional[QWidget] = None, profile: Optional[SyncProfile] = None):
        super().__init__(parent)
        self.setWindowTitle("Profile")
        self.setModal(True)
        self.profile = profile
        self.result_profile: Optional[SyncProfile] = None
        self._build_ui()
        if profile:
            self._load(profile)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.host_edit = QLineEdit()
        self.port_spin = QSpinBox()
        self.protocol_combo = QComboBox()
        self.protocol_combo.addItems(["sftp", "ftp", "ftps"])
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(22)
        self.username_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.key_path_edit = QLineEdit()
        self.local_dir_edit = QLineEdit()
        self.remote_dir_edit = QLineEdit()
        self.direction_combo = QComboBox()
        self.direction_combo.addItems(["upload-only", "download-only", "two-way"])
        self.auto_start_check = QCheckBox("Start syncing when app opens")
        self.backup_check = QCheckBox("Backup before local overwrite")
        self.backup_check.setChecked(True)
        self.delete_missing_check = QCheckBox("Delete remote file when local file is deleted")
        self.device_label_edit = QLineEdit(os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "device")
        self.stability_spin = QSpinBox()
        self.stability_spin.setRange(2, 120)
        self.stability_spin.setValue(5)

        browse_key_btn = QPushButton("Browse")
        browse_key_btn.clicked.connect(self._browse_key)
        browse_local_btn = QPushButton("Browse")
        browse_local_btn.clicked.connect(self._browse_local)

        key_layout = QHBoxLayout()
        key_layout.addWidget(self.key_path_edit)
        key_layout.addWidget(browse_key_btn)

        local_layout = QHBoxLayout()
        local_layout.addWidget(self.local_dir_edit)
        local_layout.addWidget(browse_local_btn)

        form.addRow("Profile name", self.name_edit)
        form.addRow("Host", self.host_edit)
        form.addRow("Protocol", self.protocol_combo)
        form.addRow("Port", self.port_spin)
        form.addRow("Username", self.username_edit)
        form.addRow("Password", self.password_edit)
        form.addRow("SSH key", self._wrap_layout(key_layout))
        form.addRow("Local folder", self._wrap_layout(local_layout))
        form.addRow("Remote folder", self.remote_dir_edit)
        form.addRow("Direction", self.direction_combo)
        form.addRow("Device label", self.device_label_edit)
        form.addRow("Stability wait (sec)", self.stability_spin)
        form.addRow("", self.auto_start_check)
        form.addRow("", self.backup_check)
        form.addRow("", self.delete_missing_check)

        layout.addLayout(form)

        btns = QHBoxLayout()
        test_btn = QPushButton("Test connection")
        test_btn.clicked.connect(self._test_connection)
        save_btn = QPushButton("Save")
        save_btn.clicked.connect(self._save)
        cancel_btn = QPushButton("Cancel")
        cancel_btn.clicked.connect(self.reject)
        btns.addWidget(test_btn)
        btns.addStretch(1)
        btns.addWidget(save_btn)
        btns.addWidget(cancel_btn)
        layout.addLayout(btns)

        self.resize(640, 420)

    def _wrap_layout(self, layout: QHBoxLayout) -> QWidget:
        w = QWidget()
        w.setLayout(layout)
        return w

    def _browse_key(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Choose SSH Private Key")
        if path:
            self.key_path_edit.setText(path)

    def _browse_local(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Choose Local Folder")
        if path:
            self.local_dir_edit.setText(path)

    def _load(self, p: SyncProfile) -> None:
        self.name_edit.setText(p.name)
        self.host_edit.setText(p.host)
        self.protocol_combo.setCurrentText(p.protocol)
        self.port_spin.setValue(p.port)
        self.username_edit.setText(p.username)
        self.password_edit.setText(p.password)
        self.key_path_edit.setText(p.key_path)
        self.local_dir_edit.setText(p.local_dir)
        self.remote_dir_edit.setText(p.remote_dir)
        self.direction_combo.setCurrentText(p.direction)
        self.auto_start_check.setChecked(p.auto_start)
        self.backup_check.setChecked(p.backup_before_overwrite)
        self.delete_missing_check.setChecked(p.delete_missing)
        self.device_label_edit.setText(p.device_label)
        self.stability_spin.setValue(p.stability_wait_seconds)

    def _collect(self) -> SyncProfile:
        return SyncProfile(
            name=self.name_edit.text().strip(),
            host=self.host_edit.text().strip(),
            protocol=self.protocol_combo.currentText(),
            port=int(self.port_spin.value()),
            username=self.username_edit.text().strip(),
            password=self.password_edit.text(),
            key_path=self.key_path_edit.text().strip(),
            local_dir=self.local_dir_edit.text().strip(),
            remote_dir=self.remote_dir_edit.text().strip(),
            direction=self.direction_combo.currentText(),
            auto_start=self.auto_start_check.isChecked(),
            backup_before_overwrite=self.backup_check.isChecked(),
            delete_missing=self.delete_missing_check.isChecked(),
            device_label=self.device_label_edit.text().strip() or "device",
            stability_wait_seconds=int(self.stability_spin.value()),
        )

    def _test_connection(self) -> None:
        profile = self._collect()
        ok, msg = profile.validate()
        if not ok:
            QMessageBox.warning(self, APP_NAME, msg)
            return
        engine = SyncEngine(profile, StateDB(DB_PATH))
        success, message = engine.test_connection()
        if success:
            QMessageBox.information(self, APP_NAME, message)
        else:
            QMessageBox.critical(self, APP_NAME, message)

    def _save(self) -> None:
        profile = self._collect()
        ok, msg = profile.validate()
        if not ok:
            QMessageBox.warning(self, APP_NAME, msg)
            return
        self.result_profile = profile
        self.accept()


# ----------------------------- Main Window -------------------------------- #


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        ensure_app_dir()
        self.store = SettingsStore(CONFIG_PATH)
        self.db = StateDB(DB_PATH)
        self.profiles: List[SyncProfile] = self.store.load_profiles()
        self.engines: Dict[str, SyncEngine] = {}
        self.threads: Dict[str, QThread] = {}
        self.current_profile_name: Optional[str] = None
        self.tray: Optional[QSystemTrayIcon] = None
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(1100, 760)
        self._build_ui()
        self._build_tray()
        self._load_profiles_ui()
        self._load_logs()
        self._auto_start_profiles()

    def _build_ui(self) -> None:
        central = QWidget()
        root = QHBoxLayout(central)

        left_box = QVBoxLayout()
        profile_group = QGroupBox("Profiles")
        profile_layout = QVBoxLayout(profile_group)
        self.profile_list = QListWidget()
        self.profile_list.currentItemChanged.connect(self.on_profile_selected)
        profile_layout.addWidget(self.profile_list)

        btn_grid = QGridLayout()
        self.add_btn = QPushButton("Add")
        self.edit_btn = QPushButton("Edit")
        self.delete_btn = QPushButton("Delete")
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.sync_now_btn = QPushButton("Sync now")

        self.add_btn.clicked.connect(self.add_profile)
        self.edit_btn.clicked.connect(self.edit_profile)
        self.delete_btn.clicked.connect(self.delete_profile)
        self.start_btn.clicked.connect(self.start_selected_profile)
        self.stop_btn.clicked.connect(self.stop_selected_profile)
        self.sync_now_btn.clicked.connect(self.sync_now_selected)

        btn_grid.addWidget(self.add_btn, 0, 0)
        btn_grid.addWidget(self.edit_btn, 0, 1)
        btn_grid.addWidget(self.delete_btn, 1, 0)
        btn_grid.addWidget(self.start_btn, 1, 1)
        btn_grid.addWidget(self.stop_btn, 2, 0)
        btn_grid.addWidget(self.sync_now_btn, 2, 1)
        profile_layout.addLayout(btn_grid)
        left_box.addWidget(profile_group)

        right_box = QVBoxLayout()

        status_group = QGroupBox("Status")
        status_layout = QFormLayout(status_group)
        self.status_value = QLabel("Idle")
        self.profile_name_value = QLabel("-")
        self.local_dir_value = QLabel("-")
        self.remote_dir_value = QLabel("-")
        self.direction_value = QLabel("-")
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        status_layout.addRow("Profile", self.profile_name_value)
        status_layout.addRow("Local folder", self.local_dir_value)
        status_layout.addRow("Remote folder", self.remote_dir_value)
        status_layout.addRow("Direction", self.direction_value)
        status_layout.addRow("Engine status", self.status_value)
        status_layout.addRow("Progress", self.progress_bar)
        right_box.addWidget(status_group)

        history_group = QGroupBox("Recent synced files")
        history_layout = QVBoxLayout(history_group)
        self.history_table = QTableWidget(0, 4)
        self.history_table.setHorizontalHeaderLabels(["Time", "File", "Direction", "Status"])
        self.history_table.horizontalHeader().setStretchLastSection(True)
        history_layout.addWidget(self.history_table)
        right_box.addWidget(history_group, 2)

        log_group = QGroupBox("Logs")
        log_layout = QVBoxLayout(log_group)
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        log_layout.addWidget(self.log_edit)
        right_box.addWidget(log_group, 3)

        root.addLayout(left_box, 1)
        root.addLayout(right_box, 3)
        self.setCentralWidget(central)

    def _build_tray(self) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        self.tray = QSystemTrayIcon(self)
        self.tray.setToolTip(APP_NAME)
        menu = QMenu()
        open_action = QAction("Open", self)
        open_action.triggered.connect(self.showNormal)
        quit_action = QAction("Quit", self)
        quit_action.triggered.connect(self.quit_app)
        menu.addAction(open_action)
        menu.addSeparator()
        menu.addAction(quit_action)
        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._tray_activated)
        self.tray.show()

    def _tray_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:
            self.showNormal()
            self.raise_()
            self.activateWindow()

    def _load_profiles_ui(self) -> None:
        self.profile_list.clear()
        for profile in self.profiles:
            item = QListWidgetItem(profile.name)
            self.profile_list.addItem(item)
        if self.profiles:
            self.profile_list.setCurrentRow(0)

    def _load_logs(self) -> None:
        self.log_edit.clear()
        rows = list(reversed(self.db.get_recent_logs(200)))
        for row in rows:
            self.append_log(row["level"], row["message"], row["ts"])

    def append_log(self, level: str, message: str, ts: Optional[float] = None) -> None:
        prefix = fmt_ts(ts or now_ts())
        line = f"[{prefix}] [{level}] {message}"
        self.log_edit.appendPlainText(line)
        # Trim lines if needed
        text = self.log_edit.toPlainText().splitlines()
        if len(text) > LOG_MAX_LINES:
            self.log_edit.setPlainText("\n".join(text[-LOG_MAX_LINES:]))
        self.log_edit.moveCursor(QTextCursor.End)
        if self.tray and level in {"WARNING", "ERROR"}:
            self.tray.showMessage(APP_NAME, message)

    def get_selected_profile(self) -> Optional[SyncProfile]:
        row = self.profile_list.currentRow()
        if row < 0 or row >= len(self.profiles):
            return None
        return self.profiles[row]

    def on_profile_selected(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:
        profile = self.get_selected_profile()
        if not profile:
            self.current_profile_name = None
            return
        self.current_profile_name = profile.name
        self.profile_name_value.setText(profile.name)
        self.local_dir_value.setText(profile.local_dir)
        self.remote_dir_value.setText(profile.remote_dir)
        self.direction_value.setText(profile.direction)
        self.status_value.setText("Running" if profile.name in self.engines else "Stopped")
        self.load_history(profile.name)

    def load_history(self, profile_name: str) -> None:
        rows = self.db.get_states_for_profile(profile_name)
        self.history_table.setRowCount(0)
        for row in rows[:100]:
            insert_at = self.history_table.rowCount()
            self.history_table.insertRow(insert_at)
            local_path = Path(row["local_path"]).name if row["local_path"] else "-"
            remote_path = row["remote_path"] or "-"
            file_name = local_path if local_path != "." else remote_path
            self.history_table.setItem(insert_at, 0, QTableWidgetItem(fmt_ts(row["last_sync_time"])))
            self.history_table.setItem(insert_at, 1, QTableWidgetItem(file_name))
            self.history_table.setItem(insert_at, 2, QTableWidgetItem(self._infer_direction(row)))
            self.history_table.setItem(insert_at, 3, QTableWidgetItem(row["status"] or "-"))

    def _infer_direction(self, row: sqlite3.Row) -> str:
        local_m = row["local_mtime"] or 0
        remote_m = row["remote_mtime"] or 0
        if local_m >= remote_m:
            return "upload/check"
        return "download/check"

    def add_profile(self) -> None:
        dlg = ProfileDialog(self)
        if dlg.exec() == QDialog.Accepted and dlg.result_profile:
            new_profile = dlg.result_profile
            if any(p.name == new_profile.name for p in self.profiles):
                QMessageBox.warning(self, APP_NAME, "A profile with that name already exists.")
                return
            self.profiles.append(new_profile)
            self.store.save_profiles(self.profiles)
            self._load_profiles_ui()
            self.append_log("INFO", f"Profile added: {new_profile.name}")

    def edit_profile(self) -> None:
        profile = self.get_selected_profile()
        if not profile:
            return
        if profile.name in self.engines:
            QMessageBox.warning(self, APP_NAME, "Stop the profile before editing it.")
            return
        dlg = ProfileDialog(self, profile)
        if dlg.exec() == QDialog.Accepted and dlg.result_profile:
            row = self.profile_list.currentRow()
            self.profiles[row] = dlg.result_profile
            self.store.save_profiles(self.profiles)
            self._load_profiles_ui()
            self.profile_list.setCurrentRow(row)
            self.append_log("INFO", f"Profile updated: {dlg.result_profile.name}")

    def delete_profile(self) -> None:
        profile = self.get_selected_profile()
        if not profile:
            return
        if profile.name in self.engines:
            QMessageBox.warning(self, APP_NAME, "Stop the profile before deleting it.")
            return
        if QMessageBox.question(self, APP_NAME, f"Delete profile '{profile.name}'?") != QMessageBox.Yes:
            return
        row = self.profile_list.currentRow()
        self.profiles.pop(row)
        self.store.save_profiles(self.profiles)
        self._load_profiles_ui()
        self.append_log("INFO", f"Profile deleted: {profile.name}")

    def start_selected_profile(self) -> None:
        profile = self.get_selected_profile()
        if profile:
            self.start_profile(profile)

    def stop_selected_profile(self) -> None:
        profile = self.get_selected_profile()
        if profile:
            self.stop_profile(profile.name)

    def sync_now_selected(self) -> None:
        profile = self.get_selected_profile()
        if not profile or profile.name not in self.engines:
            QMessageBox.information(self, APP_NAME, "Start the profile first.")
            return
        engine = self.engines[profile.name]
        engine.full_sync()

    def start_profile(self, profile: SyncProfile) -> None:
        if profile.name in self.engines:
            QMessageBox.information(self, APP_NAME, "Profile is already running.")
            return

        thread = QThread(self)
        engine = SyncEngine(profile, self.db)
        engine.moveToThread(thread)

        thread.started.connect(engine.start)
        engine.log.connect(self.on_engine_log)
        engine.status_changed.connect(self.on_engine_status)
        engine.progress_changed.connect(self.on_engine_progress)
        engine.file_synced.connect(self.on_file_synced)

        thread.finished.connect(thread.deleteLater)

        self.engines[profile.name] = engine
        self.threads[profile.name] = thread
        thread.start()
        self.append_log("INFO", f"Started profile: {profile.name}")
        self.on_profile_selected(None, None)

    def stop_profile(self, profile_name: str) -> None:
        engine = self.engines.get(profile_name)
        thread = self.threads.get(profile_name)
        if not engine or not thread:
            return
        engine.stop()
        thread.quit()
        thread.wait(5000)
        self.engines.pop(profile_name, None)
        self.threads.pop(profile_name, None)
        self.append_log("INFO", f"Stopped profile: {profile_name}")
        self.on_profile_selected(None, None)

    def on_engine_log(self, level: str, message: str) -> None:
        profile = self.sender().profile.name if hasattr(self.sender(), "profile") else "profile"
        self.append_log(level, f"[{profile}] {message}")

    def on_engine_status(self, status: str) -> None:
        sender = self.sender()
        if not hasattr(sender, "profile"):
            return
        profile_name = sender.profile.name
        if self.current_profile_name == profile_name:
            self.status_value.setText(status)

    def on_engine_progress(self, file_rel: str, pct: int) -> None:
        self.progress_bar.setValue(pct)

    def on_file_synced(self, info: dict) -> None:
        profile_name = info.get("profile")
        if self.current_profile_name == profile_name:
            self.load_history(profile_name)

    def _auto_start_profiles(self) -> None:
        for profile in self.profiles:
            if profile.auto_start:
                QTimer.singleShot(500, lambda p=profile: self.start_profile(p))

    def closeEvent(self, event: QCloseEvent) -> None:
        if self.tray and self.tray.isVisible():
            self.hide()
            event.ignore()
            self.tray.showMessage(APP_NAME, "App minimized to tray and continues syncing.")
            return
        super().closeEvent(event)

    def quit_app(self) -> None:
        for profile_name in list(self.engines.keys()):
            self.stop_profile(profile_name)
        QApplication.instance().quit()


# ----------------------------- Main Entrypoint ---------------------------- #


def main() -> int:
    ensure_app_dir()
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setOrganizationName("Appzter")
    window = MainWindow()
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
