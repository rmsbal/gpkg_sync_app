from __future__ import annotations

import contextlib
import os
import shutil
import threading
import traceback
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Callable, Dict, List, Optional, Type

from PySide6.QtCore import QObject, QMutex, QThread, QTimer, Qt, Signal, Slot
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from .logging_utils import AppLogger
from .models import SyncProfile
from .storage import StateDB, now_ts
from .transports import FileTransport, transport_for_profile


def fmt_ts(ts: Optional[float]) -> str:
    if not ts:
        return "-"
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def sha1_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    import hashlib

    digest = hashlib.sha1()
    with path.open("rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def safe_relpath(file_path: Path, root: Path) -> str:
    return str(file_path.resolve().relative_to(root.resolve())).replace("\\", "/")


def make_conflict_name(path: Path, device_label: str) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return path.with_name(f"{path.stem}.conflict-{device_label}-{stamp}{path.suffix}")


def make_backup_name(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    return path.with_name(f"{path.stem}.backup-{stamp}{path.suffix}")


def normalize_remote_path(remote_root: str, rel: str) -> str:
    return str(PurePosixPath(remote_root) / PurePosixPath(rel)).replace("\\", "/")


def remote_relpath(remote_root: str, remote_path: str) -> str:
    return str(PurePosixPath(remote_path).relative_to(PurePosixPath(remote_root))).replace("\\", "/")


def file_snapshot(path: Path) -> Optional[tuple[int, float]]:
    try:
        stat_result = path.stat()
    except FileNotFoundError:
        return None
    return stat_result.st_size, stat_result.st_mtime


def is_file_stable(path: Path, checks: int = 3, delay: float = 1.5) -> bool:
    import time

    last = None
    for _ in range(checks):
        snapshot = file_snapshot(path)
        if snapshot is None:
            return False
        if last is not None and snapshot != last:
            last = snapshot
            time.sleep(delay)
            continue
        last = snapshot
        time.sleep(delay)
    return file_snapshot(path) == last and last is not None


def local_file_accessible(path: Path) -> bool:
    try:
        with path.open("rb"):
            return True
    except OSError:
        return False


class LocalWatcherHandler(FileSystemEventHandler):
    def __init__(self, on_change: Callable[[Path], None]):
        super().__init__()
        self.on_change = on_change

    def _handle(self, path_str: str) -> None:
        path = Path(path_str)
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
            self._handle(event.src_path)


class FolderWatcher(QObject):
    file_changed = Signal(str)

    def __init__(self, root: Path):
        super().__init__()
        self.root = root
        self.observer: Optional[Observer] = None

    def start(self) -> None:
        if self.observer is not None:
            return
        handler = LocalWatcherHandler(lambda path: self.file_changed.emit(str(path)))
        self.observer = Observer()
        self.observer.schedule(handler, str(self.root), recursive=True)
        self.observer.start()

    def stop(self) -> None:
        if self.observer is None:
            return
        self.observer.stop()
        self.observer.join(timeout=5)
        self.observer = None


class SyncEngine(QObject):
    status_changed = Signal(str, str)
    progress_changed = Signal(str, str, int)
    file_synced = Signal(dict)
    stopped = Signal(str)

    def __init__(
        self,
        profile: SyncProfile,
        db: StateDB,
        app_logger: AppLogger,
        transport: Optional[FileTransport] = None,
        watcher_factory: Type[FolderWatcher] = FolderWatcher,
    ):
        super().__init__()
        self.profile = profile
        self.db = db
        self.app_logger = app_logger
        self.transport = transport or transport_for_profile(profile)
        self.watcher_factory = watcher_factory
        self.watcher: Optional[FolderWatcher] = None
        self.scan_timer: Optional[QTimer] = None
        self.running = False
        self.pending_files: Dict[str, float] = {}
        self.pending_lock = threading.Lock()
        self.mutex = QMutex()
        self._last_full_remote_scan = 0.0

    def emit_log(self, level: str, code: str, message: str) -> None:
        self.app_logger.log(level, code, f"[{self.profile.name}] {message}")

    @Slot()
    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self.emit_log("INFO", "ENGINE_START", "Starting sync engine.")
        self.status_changed.emit(self.profile.name, "Connecting")
        try:
            self.transport.connect()
            self.emit_log("INFO", "TRANSPORT_CONNECTED", "Connected to remote server.")
            self.status_changed.emit(self.profile.name, "Watching")
            self.watcher = self.watcher_factory(Path(self.profile.local_dir))
            self.watcher.file_changed.connect(self.on_local_file_event)
            self.watcher.start()
            self.scan_timer = QTimer(self)
            self.scan_timer.setInterval(3000)
            self.scan_timer.timeout.connect(self.process_pending_and_poll_remote)
            self.scan_timer.start()
            self.request_full_sync()
        except Exception as exc:
            self.emit_log("ERROR", "CONNECTION_FAILED", f"Failed to connect: {exc}")
            self.status_changed.emit(self.profile.name, "Connection failed")
            self.running = False
            self._shutdown()

    @Slot()
    def request_stop(self) -> None:
        if not self.running and self.scan_timer is None and self.watcher is None:
            self._shutdown()
            return
        self.running = False
        self.emit_log("INFO", "ENGINE_STOP", "Stopping sync engine.")
        self._shutdown()

    def _shutdown(self) -> None:
        if self.scan_timer is not None:
            self.scan_timer.stop()
            self.scan_timer.deleteLater()
            self.scan_timer = None
        if self.watcher is not None:
            self.watcher.stop()
            self.watcher.deleteLater()
            self.watcher = None
        self.transport.close()
        self.status_changed.emit(self.profile.name, "Stopped")
        self.stopped.emit(self.profile.name)

    @Slot()
    def request_full_sync(self) -> None:
        if not self.running:
            return
        try:
            self.full_sync()
        except Exception as exc:
            self.emit_log("ERROR", "FULL_SYNC_FAILED", f"Full sync failed: {exc}")
            self.emit_log("ERROR", "TRACEBACK", traceback.format_exc())

    @Slot(str)
    def on_local_file_event(self, path_str: str) -> None:
        if not self.running:
            return
        path = Path(path_str)
        try:
            rel = safe_relpath(path, Path(self.profile.local_dir))
            self.emit_log("INFO", "LOCAL_FILE_QUEUED", f"Queued local change: {rel}")
        except ValueError:
            if path.exists():
                return
        with self.pending_lock:
            self.pending_files[str(path)] = now_ts()

    @Slot()
    def process_pending_and_poll_remote(self) -> None:
        if not self.running:
            return
        try:
            self.process_pending_local_changes()
            if now_ts() - self._last_full_remote_scan >= 15:
                self.poll_remote_changes()
                self._last_full_remote_scan = now_ts()
        except Exception as exc:
            self.emit_log("ERROR", "WORK_CYCLE_FAILED", f"Work cycle failed: {exc}")
            self.emit_log("ERROR", "TRACEBACK", traceback.format_exc())

    def process_pending_local_changes(self) -> None:
        cutoff = now_ts() - max(2, self.profile.stability_wait_seconds)
        ready: List[str] = []
        with self.pending_lock:
            for path_str, queued_at in list(self.pending_files.items()):
                if queued_at <= cutoff:
                    ready.append(path_str)
                    self.pending_files.pop(path_str, None)

        for path_str in ready:
            if self.profile.direction == "download-only":
                continue
            self.sync_local_change(Path(path_str))

    def full_sync(self) -> None:
        self.emit_log("INFO", "FULL_SYNC_START", "Running full sync.")
        self.status_changed.emit(self.profile.name, "Full sync")
        local_root = Path(self.profile.local_dir)
        local_files = [path for path in local_root.rglob("*") if path.is_file()]
        remote_files = self.transport.walk_remote_files(self.profile.remote_dir)
        remote_map = {remote_relpath(self.profile.remote_dir, str(item["path"])): item for item in remote_files}
        local_map = {safe_relpath(path, local_root): path for path in local_files}

        for rel, local_path in local_map.items():
            self.reconcile_single(local_path, normalize_remote_path(self.profile.remote_dir, rel), remote_map.get(rel))

        if self.profile.direction in {"download-only", "two-way"}:
            for rel, remote_info in remote_map.items():
                if rel not in local_map:
                    self.handle_remote_only(local_root / rel, remote_info)

        self.status_changed.emit(self.profile.name, "Watching")
        self.emit_log("INFO", "FULL_SYNC_DONE", "Full sync complete.")

    def poll_remote_changes(self) -> None:
        if self.profile.direction == "upload-only":
            return
        self.emit_log("INFO", "REMOTE_POLL", "Polling remote changes.")
        local_root = Path(self.profile.local_dir)
        for remote_info in self.transport.walk_remote_files(self.profile.remote_dir):
            rel = remote_relpath(self.profile.remote_dir, str(remote_info["path"]))
            local_path = local_root / rel
            if not local_path.exists():
                self.handle_remote_only(local_path, remote_info)
                continue
            self.reconcile_single(local_path, str(remote_info["path"]), remote_info)

    def reconcile_single(self, local_path: Path, remote_path: str, remote_info: Optional[Dict[str, object]] = None) -> None:
        local_stat = local_path.stat()
        local_mtime = float(local_stat.st_mtime)
        local_size = int(local_stat.st_size)
        rel = safe_relpath(local_path, Path(self.profile.local_dir))

        if remote_info is None:
            try:
                remote_stat = self.transport.stat(remote_path)
                remote_info = {"path": remote_path, "mtime": float(remote_stat.st_mtime), "size": int(remote_stat.st_size)}
            except FileNotFoundError:
                remote_info = None

        if remote_info is None:
            if self.profile.direction in {"upload-only", "two-way"}:
                self.upload_local_file(local_path, remote_path, "new local file")
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
            self.resolve_conflict(local_path, remote_path, remote_mtime, remote_size)
            return
        if local_changed and self.profile.direction in {"upload-only", "two-way"}:
            self.upload_local_file(local_path, remote_path, "local newer")
        elif remote_changed and self.profile.direction in {"download-only", "two-way"}:
            self.download_remote_file(remote_path, local_path, remote_mtime, remote_size, "remote newer")
        self.emit_log("INFO", "FILE_RECONCILED", f"Checked: {rel}")

    def handle_remote_only(self, local_path: Path, remote_info: Dict[str, object]) -> None:
        if self.profile.direction in {"download-only", "two-way"}:
            self.download_remote_file(
                str(remote_info["path"]),
                local_path,
                float(remote_info["mtime"]),
                int(remote_info["size"]),
                "remote-only file",
            )

    def sync_local_change(self, local_path: Path) -> None:
        if not local_path.exists():
            if not self.profile.delete_missing:
                return
            rel = safe_relpath(local_path, Path(self.profile.local_dir))
            remote_path = normalize_remote_path(self.profile.remote_dir, rel)
            if self.profile.direction in {"upload-only", "two-way"} and self.transport.exists(remote_path):
                self.emit_log("WARNING", "REMOTE_DELETE", f"Deleting remote file: {rel}")
                self.transport.remove(remote_path)
            return

        if not is_file_stable(local_path, delay=1.0):
            self.emit_log("INFO", "FILE_UNSTABLE", f"Waiting for stable file: {local_path.name}")
            with self.pending_lock:
                self.pending_files[str(local_path)] = now_ts()
            return

        if not local_file_accessible(local_path):
            self.emit_log("INFO", "FILE_LOCKED", f"File still in use: {local_path.name}")
            with self.pending_lock:
                self.pending_files[str(local_path)] = now_ts()
            return

        rel = safe_relpath(local_path, Path(self.profile.local_dir))
        remote_path = normalize_remote_path(self.profile.remote_dir, rel)
        try:
            remote_stat = self.transport.stat(remote_path)
            remote_info = {"path": remote_path, "mtime": float(remote_stat.st_mtime), "size": int(remote_stat.st_size)}
        except FileNotFoundError:
            remote_info = None
        self.reconcile_single(local_path, remote_path, remote_info)

    def resolve_conflict(self, local_path: Path, remote_path: str, remote_mtime: float, remote_size: int) -> None:
        rel = safe_relpath(local_path, Path(self.profile.local_dir))
        conflict_path = make_conflict_name(local_path, self.profile.device_label)
        while conflict_path.exists():
            conflict_path = make_conflict_name(conflict_path, self.profile.device_label)
        self.emit_log("WARNING", "SYNC_CONFLICT", f"Conflict detected for {rel}. Downloading remote as conflict copy.")
        self.download_remote_file(remote_path, conflict_path, remote_mtime, remote_size, "conflict copy")
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
        self.status_changed.emit(self.profile.name, f"Uploading {local_path.name}")
        self.emit_log("INFO", "UPLOAD_START", f"Uploading {rel} ({reason})")
        total = max(1, local_path.stat().st_size)

        def callback(sent: int, _total: int) -> None:
            pct = max(0, min(100, int((sent / total) * 100)))
            self.progress_changed.emit(self.profile.name, rel, pct)

        self.transport.upload(local_path, remote_path, callback=callback)
        remote_stat = self.transport.stat(remote_path)
        local_stat = local_path.stat()
        self.db.upsert_file_state(
            self.profile.name,
            str(local_path),
            remote_path,
            float(local_stat.st_mtime),
            float(remote_stat.st_mtime),
            int(local_stat.st_size),
            int(remote_stat.st_size),
            sha1_file(local_path),
            "synced",
            "",
        )
        self.progress_changed.emit(self.profile.name, rel, 100)
        self.file_synced.emit({"profile": self.profile.name, "file": rel, "direction": "upload", "time": fmt_ts(now_ts()), "status": "synced"})
        self.status_changed.emit(self.profile.name, "Watching")
        self.emit_log("INFO", "UPLOAD_DONE", f"Upload complete: {rel}")

    def _download_to_temp(self, remote_path: str, temp_path: Path, remote_size: int, rel: str) -> None:
        total = max(1, remote_size)

        def callback(done: int, _total: int) -> None:
            pct = max(0, min(100, int((done / total) * 100)))
            self.progress_changed.emit(self.profile.name, rel, pct)

        self.transport.download(remote_path, temp_path, callback=callback)

    def download_remote_file(self, remote_path: str, local_path: Path, remote_mtime: float, remote_size: int, reason: str) -> None:
        rel = local_path.name
        with contextlib.suppress(ValueError):
            rel = safe_relpath(local_path, Path(self.profile.local_dir))
        self.status_changed.emit(self.profile.name, f"Downloading {local_path.name}")
        self.emit_log("INFO", "DOWNLOAD_START", f"Downloading {rel} ({reason})")

        if local_path.exists() and self.profile.backup_before_overwrite and reason != "conflict copy":
            backup_path = make_backup_name(local_path)
            while backup_path.exists():
                backup_path = make_backup_name(backup_path)
            shutil.copy2(local_path, backup_path)
            self.emit_log("INFO", "BACKUP_CREATED", f"Backup created: {backup_path.name}")

        local_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = local_path.with_name(f".{local_path.name}.part")
        try:
            self._download_to_temp(remote_path, temp_path, remote_size, rel)
            temp_path.replace(local_path)
        finally:
            with contextlib.suppress(FileNotFoundError):
                if temp_path.exists():
                    temp_path.unlink()

        with contextlib.suppress(Exception):
            os.utime(local_path, (remote_mtime, remote_mtime))
        local_stat = local_path.stat()
        self.db.upsert_file_state(
            self.profile.name,
            str(local_path),
            remote_path,
            float(local_stat.st_mtime),
            remote_mtime,
            int(local_stat.st_size),
            remote_size,
            sha1_file(local_path),
            "synced" if reason != "conflict copy" else "conflict-copy",
            "",
        )
        self.progress_changed.emit(self.profile.name, rel, 100)
        self.file_synced.emit({"profile": self.profile.name, "file": rel, "direction": "download", "time": fmt_ts(now_ts()), "status": "synced"})
        self.status_changed.emit(self.profile.name, "Watching")
        self.emit_log("INFO", "DOWNLOAD_DONE", f"Download complete: {rel}")

    def test_connection(self) -> tuple[bool, str]:
        try:
            self.transport.connect()
            self.transport.mkdirs(self.profile.remote_dir)
            self.transport.close()
            return True, "Connection successful."
        except Exception as exc:
            return False, str(exc)
