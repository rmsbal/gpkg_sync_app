from __future__ import annotations

import contextlib
import ftplib
import os
import posixpath
import socket
import stat
import threading
import time
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Protocol

import paramiko

from .models import SYNC_EXTENSIONS, SyncProfile


Callback = Optional[Callable[[int, int], None]]


class FileTransport(Protocol):
    def connect(self) -> None: ...
    def close(self) -> None: ...
    def stat(self, remote_path: str): ...
    def exists(self, remote_path: str) -> bool: ...
    def mkdirs(self, remote_dir: str) -> None: ...
    def upload(self, local_path: Path, remote_path: str, callback: Callback = None) -> None: ...
    def download(self, remote_path: str, local_path: Path, callback: Callback = None) -> None: ...
    def remove(self, remote_path: str) -> None: ...
    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]: ...


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
            transport.banner_timeout = 15
            transport.auth_timeout = 15
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

    def _run(self, operation):
        try:
            if self.sftp is None:
                self.connect()
            return operation()
        except (EOFError, OSError, socket.error, paramiko.SSHException):
            self.close()
            self.connect()
            return operation()

    def stat(self, remote_path: str):
        return self._run(lambda: self.sftp.stat(remote_path))  # type: ignore[union-attr]

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        def op() -> None:
            current = "/"
            parts = [p for p in PurePosixPath(remote_dir).parts if p not in ("/", "")]
            for part in parts:
                current = posixpath.join(current, part)
                try:
                    self.sftp.stat(current)  # type: ignore[union-attr]
                except FileNotFoundError:
                    self.sftp.mkdir(current)  # type: ignore[union-attr]

        self._run(op)

    def upload(self, local_path: Path, remote_path: str, callback: Callback = None) -> None:
        def op() -> None:
            self.mkdirs(str(PurePosixPath(remote_path).parent))
            self.sftp.put(str(local_path), remote_path, callback=callback)  # type: ignore[union-attr]

        self._run(op)

    def download(self, remote_path: str, local_path: Path, callback: Callback = None) -> None:
        def op() -> None:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            self.sftp.get(remote_path, str(local_path), callback=callback)  # type: ignore[union-attr]

        self._run(op)

    def remove(self, remote_path: str) -> None:
        self._run(lambda: self.sftp.remove(remote_path))  # type: ignore[union-attr]

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        found: List[Dict[str, object]] = []

        def _walk(path: str) -> None:
            for entry in self.sftp.listdir_attr(path):  # type: ignore[union-attr]
                child = posixpath.join(path, entry.filename)
                if stat.S_ISDIR(entry.st_mode):
                    _walk(child)
                elif Path(entry.filename).suffix.lower() in SYNC_EXTENSIONS:
                    found.append({"path": child, "size": entry.st_size, "mtime": entry.st_mtime})

        try:
            self._run(lambda: _walk(remote_root))
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
            ftp = ftplib.FTP_TLS(timeout=30) if self.profile.protocol.lower() == "ftps" else ftplib.FTP(timeout=30)
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

    def _run(self, operation):
        try:
            if self.ftp is None:
                self.connect()
            return operation()
        except ftplib.all_errors:
            self.close()
            self.connect()
            return operation()

    def stat(self, remote_path: str):
        def op():
            try:
                size = self.ftp.size(remote_path)  # type: ignore[union-attr]
            except ftplib.all_errors as exc:
                raise FileNotFoundError from exc
            mtime = 0.0
            try:
                response = self.ftp.sendcmd(f"MDTM {remote_path}")  # type: ignore[union-attr]
                if response.startswith("213 "):
                    stamp = response[4:].strip()
                    mtime = time.mktime(time.strptime(stamp, "%Y%m%d%H%M%S"))
            except ftplib.all_errors:
                pass
            return SimpleNamespace(st_size=size, st_mtime=mtime)

        return self._run(op)

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        def op() -> None:
            path = str(PurePosixPath(remote_dir))
            if not path or path == ".":
                return
            parts = [p for p in PurePosixPath(path).parts if p not in ("/", "")]
            current = "" if not path.startswith("/") else "/"
            for part in parts:
                current = posixpath.join(current, part) if current else part
                try:
                    self.ftp.cwd(current)  # type: ignore[union-attr]
                except ftplib.all_errors:
                    with contextlib.suppress(ftplib.all_errors):
                        self.ftp.mkd(current)  # type: ignore[union-attr]
                    self.ftp.cwd(current)  # type: ignore[union-attr]

        self._run(op)

    def upload(self, local_path: Path, remote_path: str, callback: Callback = None) -> None:
        def op() -> None:
            self.mkdirs(str(PurePosixPath(remote_path).parent))
            total = max(1, local_path.stat().st_size)
            sent = 0
            with local_path.open("rb") as handle:
                def store_cb(data: bytes) -> None:
                    nonlocal sent
                    sent += len(data)
                    if callback:
                        callback(sent, total)

                self.ftp.storbinary(f"STOR {remote_path}", handle, 8192, callback=store_cb)  # type: ignore[union-attr]

        self._run(op)

    def download(self, remote_path: str, local_path: Path, callback: Callback = None) -> None:
        def op() -> None:
            local_path.parent.mkdir(parents=True, exist_ok=True)
            total = max(1, self.ftp.size(remote_path) or 1)  # type: ignore[union-attr]
            received = 0
            with local_path.open("wb") as handle:
                def retr_cb(data: bytes) -> None:
                    nonlocal received
                    handle.write(data)
                    received += len(data)
                    if callback:
                        callback(received, total)

                self.ftp.retrbinary(f"RETR {remote_path}", retr_cb, 8192)  # type: ignore[union-attr]

        self._run(op)

    def remove(self, remote_path: str) -> None:
        self._run(lambda: self.ftp.delete(remote_path))  # type: ignore[union-attr]

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        found: List[Dict[str, object]] = []

        def op() -> None:
            try:
                entries = list(self.ftp.mlsd(remote_root))  # type: ignore[union-attr]
            except ftplib.all_errors:
                entries = None

            if entries is not None:
                for name, facts in entries:
                    if name in (".", ".."):
                        continue
                    path = posixpath.join(remote_root, name)
                    if facts.get("type") == "dir":
                        found.extend(self.walk_remote_files(path))
                    elif facts.get("type") == "file" and Path(name).suffix.lower() in SYNC_EXTENSIONS:
                        modify = facts.get("modify")
                        mtime = 0.0
                        if modify:
                            try:
                                mtime = time.mktime(time.strptime(modify, "%Y%m%d%H%M%S"))
                            except ValueError:
                                mtime = 0.0
                        found.append({"path": path, "size": int(facts.get("size", 0)), "mtime": mtime})
                return

            def _walk(path: str) -> None:
                try:
                    names = self.ftp.nlst(path)  # type: ignore[union-attr]
                except ftplib.all_errors:
                    return
                for name in names:
                    if name in (".", ".."):
                        continue
                    candidate = name if name.startswith("/") else posixpath.join(path, name)
                    try:
                        self.ftp.cwd(candidate)  # type: ignore[union-attr]
                        _walk(candidate)
                        self.ftp.cwd(path)  # type: ignore[union-attr]
                    except ftplib.all_errors:
                        if Path(name).suffix.lower() in SYNC_EXTENSIONS:
                            size = 0
                            with contextlib.suppress(ftplib.all_errors):
                                size = self.ftp.size(candidate) or 0  # type: ignore[union-attr]
                            found.append({"path": candidate, "size": size, "mtime": 0.0})

            _walk(remote_root)

        try:
            self._run(op)
        except ftplib.all_errors:
            self.mkdirs(remote_root)
        return found


def transport_for_profile(profile: SyncProfile) -> FileTransport:
    if profile.protocol.lower() in {"ftp", "ftps"}:
        return FTPManager(profile)
    return SFTPManager(profile)
