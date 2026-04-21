from __future__ import annotations

import contextlib
import ftplib
import mimetypes
import posixpath
import socket
import stat
import threading
import time
from datetime import datetime
from pathlib import Path, PurePosixPath
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Protocol
from urllib.parse import quote

import paramiko

from .models import SyncProfile
from .oauth import google_oauth_setup_hint, load_google_client_config


Callback = Optional[Callable[[int, int], None]]
FTP_BLOCK_SIZE = 1024 * 1024
GOOGLE_DRIVE_CHUNK_SIZE = 8 * 1024 * 1024
GOOGLE_DRIVE_MAX_RETRIES = 4
ONEDRIVE_SIMPLE_UPLOAD_LIMIT = 4 * 1024 * 1024
ONEDRIVE_UPLOAD_CHUNK_SIZE = 5 * 320 * 1024


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
                else:
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

    @staticmethod
    def _is_missing_error(exc: BaseException) -> bool:
        if isinstance(exc, FileNotFoundError):
            return True
        if isinstance(exc, ftplib.error_perm):
            message = str(exc).strip()
            return message.startswith("550") or message.startswith("553")
        return False

    def _run(self, operation):
        had_connection = self.ftp is not None
        try:
            if self.ftp is None:
                self.connect()
            return operation()
        except ftplib.all_errors:
            if not had_connection:
                raise
            self.close()
            self.connect()
            return operation()

    def stat(self, remote_path: str):
        def op():
            try:
                size = self.ftp.size(remote_path)  # type: ignore[union-attr]
            except ftplib.all_errors as exc:
                if self._is_missing_error(exc):
                    raise FileNotFoundError from exc
                raise
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
                self._cwd_or_create(current)

        self._run(op)

    def _cwd_or_create(self, remote_dir: str) -> None:
        try:
            self.ftp.cwd(remote_dir)  # type: ignore[union-attr]
            return
        except ftplib.all_errors as exc:
            if not self._is_missing_error(exc):
                raise
        with contextlib.suppress(ftplib.all_errors):
            self.ftp.mkd(remote_dir)  # type: ignore[union-attr]
        self.ftp.cwd(remote_dir)  # type: ignore[union-attr]

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

                self.ftp.storbinary(f"STOR {remote_path}", handle, FTP_BLOCK_SIZE, callback=store_cb)  # type: ignore[union-attr]

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

                self.ftp.retrbinary(f"RETR {remote_path}", retr_cb, FTP_BLOCK_SIZE)  # type: ignore[union-attr]

        self._run(op)

    def remove(self, remote_path: str) -> None:
        self._run(lambda: self.ftp.delete(remote_path))  # type: ignore[union-attr]

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        found: List[Dict[str, object]] = []

        def op() -> None:
            try:
                entries = list(self.ftp.mlsd(remote_root))  # type: ignore[union-attr]
            except ftplib.all_errors as exc:
                if self._is_missing_error(exc):
                    self.mkdirs(remote_root)
                    return
                raise
            if entries is not None:
                for name, facts in entries:
                    if name in (".", ".."):
                        continue
                    path = posixpath.join(remote_root, name)
                    if facts.get("type") == "dir":
                        found.extend(self.walk_remote_files(path))
                    elif facts.get("type") == "file":
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
                except ftplib.all_errors as exc:
                    if self._is_missing_error(exc):
                        return
                    raise
                for name in names:
                    if name in (".", ".."):
                        continue
                    candidate = name if name.startswith("/") else posixpath.join(path, name)
                    try:
                        self.ftp.cwd(candidate)  # type: ignore[union-attr]
                        _walk(candidate)
                        self.ftp.cwd(path)  # type: ignore[union-attr]
                    except ftplib.all_errors as exc:
                        if not self._is_missing_error(exc):
                            raise
                        size = 0
                        with contextlib.suppress(ftplib.all_errors):
                            size = self.ftp.size(candidate) or 0  # type: ignore[union-attr]
                        found.append({"path": candidate, "size": size, "mtime": 0.0})

            _walk(remote_root)
        self._run(op)
        return found


class CloudPathMixin:
    @staticmethod
    def _normalize_remote_path(remote_path: str) -> str:
        normalized = str(PurePosixPath(remote_path)).replace("\\", "/")
        if normalized in ("", "."):
            return "/"
        return normalized if normalized.startswith("/") else f"/{normalized}"

    def _split_path(self, remote_path: str) -> List[str]:
        normalized = self._normalize_remote_path(remote_path)
        return [part for part in PurePosixPath(normalized).parts if part not in ("/", "")]

    @staticmethod
    def _to_timestamp(value: Optional[str]) -> float:
        if not value:
            return 0.0
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0.0


class GoogleDriveManager(CloudPathMixin):
    SCOPES = ["https://www.googleapis.com/auth/drive"]

    def __init__(self, profile: SyncProfile):
        self.profile = profile
        self.service = None
        self.creds = None
        self._lock = threading.Lock()
        self._folder_cache: Dict[str, str] = {"/": "root"}

    @property
    def _token_path(self) -> Path:
        safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in self.profile.name)
        return Path.home() / ".gpkg_sync" / f"google-drive-{safe_name}-token.json"

    def connect(self) -> None:
        with self._lock:
            if self.service is not None:
                return
            from google.auth.transport.requests import Request
            from google.oauth2.credentials import Credentials
            from googleapiclient.discovery import build
            from google_auth_oauthlib.flow import InstalledAppFlow

            client_config = load_google_client_config()
            if client_config is None:
                raise RuntimeError(google_oauth_setup_hint())
            creds = None
            token_path = self._token_path
            if token_path.exists():
                creds = Credentials.from_authorized_user_file(str(token_path), self.SCOPES)
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            if not creds or not creds.valid:
                flow = InstalledAppFlow.from_client_config(client_config, self.SCOPES)
                creds = flow.run_local_server(port=0)
                token_path.parent.mkdir(parents=True, exist_ok=True)
                token_path.write_text(creds.to_json(), encoding="utf-8")
            self.creds = creds
            self.service = build("drive", "v3", credentials=creds, cache_discovery=False)
            self._folder_cache = {"/": "root"}

    def close(self) -> None:
        self.service = None
        self.creds = None
        self._folder_cache = {"/": "root"}

    def _run(self, operation):
        if self.service is None:
            self.connect()
        return operation()

    def _next_chunk_with_retry(self, chunk_op):
        last_exc = None
        for attempt in range(GOOGLE_DRIVE_MAX_RETRIES):
            try:
                return chunk_op()
            except Exception as exc:
                status = getattr(getattr(exc, "resp", None), "status", None)
                retryable = status in {408, 429, 500, 502, 503, 504} or isinstance(exc, (OSError, TimeoutError))
                if not retryable or attempt == GOOGLE_DRIVE_MAX_RETRIES - 1:
                    raise
                last_exc = exc
                time.sleep(min(8, 2**attempt))
        if last_exc is not None:
            raise last_exc

    def _query_child(self, parent_id: str, name: str, mime_type: Optional[str] = None) -> Optional[dict]:
        escaped_name = name.replace("'", "\\'")
        parts = [
            f"name = '{escaped_name}'",
            f"'{parent_id}' in parents",
            "trashed = false",
        ]
        if mime_type:
            parts.append(f"mimeType = '{mime_type}'")
        response = self.service.files().list(  # type: ignore[union-attr]
            q=" and ".join(parts),
            spaces="drive",
            fields="files(id,name,mimeType,modifiedTime,size)",
            pageSize=10,
        ).execute()
        files = response.get("files", [])
        return files[0] if files else None

    def _ensure_folder(self, remote_dir: str) -> str:
        normalized = self._normalize_remote_path(remote_dir)
        if normalized in self._folder_cache:
            return self._folder_cache[normalized]
        current_path = "/"
        current_id = "root"
        for segment in self._split_path(normalized):
            current_path = str(PurePosixPath(current_path) / segment).replace("\\", "/")
            if not current_path.startswith("/"):
                current_path = f"/{current_path}"
            cached = self._folder_cache.get(current_path)
            if cached:
                current_id = cached
                continue
            folder = self._query_child(current_id, segment, "application/vnd.google-apps.folder")
            if folder is None:
                folder = self.service.files().create(  # type: ignore[union-attr]
                    body={
                        "name": segment,
                        "parents": [current_id],
                        "mimeType": "application/vnd.google-apps.folder",
                    },
                    fields="id,name",
                ).execute()
            current_id = folder["id"]
            self._folder_cache[current_path] = current_id
        return current_id

    def _find_file(self, remote_path: str) -> dict:
        parent = str(PurePosixPath(self._normalize_remote_path(remote_path)).parent)
        name = PurePosixPath(remote_path).name
        parent_id = self._ensure_folder(parent)
        file_item = self._query_child(parent_id, name)
        if file_item is None or file_item.get("mimeType") == "application/vnd.google-apps.folder":
            raise FileNotFoundError(remote_path)
        return file_item

    def stat(self, remote_path: str):
        item = self._run(lambda: self._find_file(remote_path))
        return SimpleNamespace(
            st_size=int(item.get("size", 0) or 0),
            st_mtime=self._to_timestamp(item.get("modifiedTime")),
        )

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        self._run(lambda: self._ensure_folder(remote_dir))

    def upload(self, local_path: Path, remote_path: str, callback: Callback = None) -> None:
        from googleapiclient.http import MediaFileUpload

        def op() -> None:
            parent = str(PurePosixPath(self._normalize_remote_path(remote_path)).parent)
            name = PurePosixPath(remote_path).name
            parent_id = self._ensure_folder(parent)
            existing = None
            with contextlib.suppress(FileNotFoundError):
                existing = self._find_file(remote_path)
            mime_type = mimetypes.guess_type(local_path.name)[0] or "application/octet-stream"
            media = MediaFileUpload(
                str(local_path),
                mimetype=mime_type,
                resumable=True,
                chunksize=GOOGLE_DRIVE_CHUNK_SIZE,
            )
            body = {"name": name, "parents": [parent_id]}
            request = (
                self.service.files().update(fileId=existing["id"], media_body=media, fields="id,size,modifiedTime")
                if existing
                else self.service.files().create(body=body, media_body=media, fields="id,size,modifiedTime")
            )
            response = None
            while response is None:
                status, response = self._next_chunk_with_retry(request.next_chunk)
                if status and callback:
                    callback(int(status.resumable_progress), max(1, local_path.stat().st_size))
            if callback:
                callback(local_path.stat().st_size, max(1, local_path.stat().st_size))

        self._run(op)

    def download(self, remote_path: str, local_path: Path, callback: Callback = None) -> None:
        from googleapiclient.http import MediaIoBaseDownload

        def op() -> None:
            item = self._find_file(remote_path)
            total = max(1, int(item.get("size", 0) or 1))
            local_path.parent.mkdir(parents=True, exist_ok=True)
            request = self.service.files().get_media(fileId=item["id"])  # type: ignore[union-attr]
            with local_path.open("wb") as handle:
                downloader = MediaIoBaseDownload(handle, request, chunksize=GOOGLE_DRIVE_CHUNK_SIZE)
                done = False
                while not done:
                    status, done = self._next_chunk_with_retry(downloader.next_chunk)
                    if status and callback:
                        callback(int(status.resumable_progress), total)
            if callback:
                callback(total, total)

        self._run(op)

    def remove(self, remote_path: str) -> None:
        def op() -> None:
            item = self._find_file(remote_path)
            self.service.files().delete(fileId=item["id"]).execute()  # type: ignore[union-attr]

        self._run(op)

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        found: List[Dict[str, object]] = []

        def walk(folder_path: str, folder_id: str) -> None:
            page_token = None
            while True:
                response = self.service.files().list(  # type: ignore[union-attr]
                    q=f"'{folder_id}' in parents and trashed = false",
                    spaces="drive",
                    fields="nextPageToken,files(id,name,mimeType,modifiedTime,size)",
                    pageSize=1000,
                    pageToken=page_token,
                ).execute()
                for item in response.get("files", []):
                    item_path = str(PurePosixPath(folder_path) / item["name"]).replace("\\", "/")
                    if item.get("mimeType") == "application/vnd.google-apps.folder":
                        if not item_path.startswith("/"):
                            item_path = f"/{item_path}"
                        self._folder_cache[item_path] = item["id"]
                        walk(item_path, item["id"])
                    else:
                        if not item_path.startswith("/"):
                            item_path = f"/{item_path}"
                        found.append(
                            {
                                "path": item_path,
                                "size": int(item.get("size", 0) or 0),
                                "mtime": self._to_timestamp(item.get("modifiedTime")),
                            }
                        )
                page_token = response.get("nextPageToken")
                if not page_token:
                    break

        def op() -> None:
            folder_id = self._ensure_folder(remote_root)
            walk(self._normalize_remote_path(remote_root), folder_id)

        self._run(op)
        return found


class OneDriveManager(CloudPathMixin):
    SCOPES = ["Files.ReadWrite.All", "offline_access", "User.Read"]
    GRAPH_ROOT = "https://graph.microsoft.com/v1.0/me/drive"

    def __init__(self, profile: SyncProfile):
        self.profile = profile
        self.app = None
        self.token = None
        self.session = None
        self._lock = threading.Lock()

    @property
    def _cache_path(self) -> Path:
        safe_name = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in self.profile.name)
        return Path.home() / ".gpkg_sync" / f"onedrive-{safe_name}-token.bin"

    def connect(self) -> None:
        with self._lock:
            if self.session is not None:
                return
            import msal
            import requests

            cache = msal.SerializableTokenCache()
            cache_path = self._cache_path
            if cache_path.exists():
                cache.deserialize(cache_path.read_text(encoding="utf-8"))
            authority = f"https://login.microsoftonline.com/{self.profile.tenant_id}"
            app = msal.PublicClientApplication(self.profile.client_id, authority=authority, token_cache=cache)
            accounts = app.get_accounts()
            result = app.acquire_token_silent(self.SCOPES, account=accounts[0] if accounts else None)
            if not result:
                result = app.acquire_token_interactive(scopes=self.SCOPES)
            if "access_token" not in result:
                error = result.get("error_description") or result.get("error") or "OneDrive authentication failed."
                raise RuntimeError(error)
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(cache.serialize(), encoding="utf-8")
            session = requests.Session()
            session.headers.update({"Authorization": f"Bearer {result['access_token']}"})
            self.app = app
            self.token = result
            self.session = session

    def close(self) -> None:
        if self.session is not None:
            with contextlib.suppress(Exception):
                self.session.close()
        self.session = None
        self.app = None
        self.token = None

    def _run(self, operation):
        if self.session is None:
            self.connect()
        return operation()

    def _api(self, method: str, path: str, **kwargs):
        response = self.session.request(method, f"{self.GRAPH_ROOT}{path}", **kwargs)  # type: ignore[union-attr]
        if response.status_code == 404:
            raise FileNotFoundError(path)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return response.json()

    def _path_url(self, remote_path: str, suffix: str = "") -> str:
        normalized = self._normalize_remote_path(remote_path)
        quoted = quote(normalized, safe="/")
        return f"/root:{quoted}:{suffix}"

    def stat(self, remote_path: str):
        item = self._run(lambda: self._api("GET", self._path_url(remote_path)))
        if item.get("folder"):
            raise FileNotFoundError(remote_path)
        return SimpleNamespace(
            st_size=int(item.get("size", 0) or 0),
            st_mtime=self._to_timestamp(item.get("lastModifiedDateTime")),
        )

    def exists(self, remote_path: str) -> bool:
        try:
            self.stat(remote_path)
            return True
        except FileNotFoundError:
            return False

    def mkdirs(self, remote_dir: str) -> None:
        def op() -> None:
            current = "/"
            for segment in self._split_path(remote_dir):
                current = str(PurePosixPath(current) / segment).replace("\\", "/")
                parent = str(PurePosixPath(current).parent)
                if parent == ".":
                    parent = "/"
                try:
                    item = self._api("GET", self._path_url(current))
                    if not item.get("folder"):
                        raise RuntimeError(f"Remote path is a file, not a folder: {current}")
                    continue
                except FileNotFoundError:
                    self._api(
                        "POST",
                        self._path_url(parent, "/children"),
                        json={"name": segment, "folder": {}, "@microsoft.graph.conflictBehavior": "replace"},
                    )

        self._run(op)

    def upload(self, local_path: Path, remote_path: str, callback: Callback = None) -> None:
        def op() -> None:
            parent = str(PurePosixPath(self._normalize_remote_path(remote_path)).parent)
            self.mkdirs(parent)
            total = max(1, local_path.stat().st_size)
            if total <= ONEDRIVE_SIMPLE_UPLOAD_LIMIT:
                with local_path.open("rb") as handle:
                    response = self.session.put(  # type: ignore[union-attr]
                        f"{self.GRAPH_ROOT}{self._path_url(remote_path, '/content')}",
                        data=handle,
                        headers={"Content-Type": "application/octet-stream"},
                    )
                response.raise_for_status()
                if callback:
                    callback(total, total)
                return

            session_info = self._api(
                "POST",
                self._path_url(remote_path, "/createUploadSession"),
                json={"item": {"@microsoft.graph.conflictBehavior": "replace"}},
            )
            upload_url = session_info.get("uploadUrl")
            if not upload_url:
                raise RuntimeError("OneDrive did not return an upload session URL.")
            sent = 0
            with local_path.open("rb") as handle:
                while sent < total:
                    chunk = handle.read(min(ONEDRIVE_UPLOAD_CHUNK_SIZE, total - sent))
                    if not chunk:
                        break
                    end = sent + len(chunk) - 1
                    response = self.session.put(  # type: ignore[union-attr]
                        upload_url,
                        data=chunk,
                        headers={
                            "Content-Length": str(len(chunk)),
                            "Content-Range": f"bytes {sent}-{end}/{total}",
                        },
                    )
                    response.raise_for_status()
                    sent += len(chunk)
                    if callback:
                        callback(sent, total)

        self._run(op)

    def download(self, remote_path: str, local_path: Path, callback: Callback = None) -> None:
        def op() -> None:
            item = self._api("GET", self._path_url(remote_path))
            total = max(1, int(item.get("size", 0) or 1))
            download_url = item.get("@microsoft.graph.downloadUrl")
            if not download_url:
                raise RuntimeError("OneDrive did not return a download URL.")
            local_path.parent.mkdir(parents=True, exist_ok=True)
            response = self.session.get(download_url, stream=True)  # type: ignore[union-attr]
            response.raise_for_status()
            received = 0
            with local_path.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if not chunk:
                        continue
                    handle.write(chunk)
                    received += len(chunk)
                    if callback:
                        callback(received, total)
            if callback:
                callback(total, total)

        self._run(op)

    def remove(self, remote_path: str) -> None:
        self._run(lambda: self._api("DELETE", self._path_url(remote_path)))

    def walk_remote_files(self, remote_root: str) -> List[Dict[str, object]]:
        found: List[Dict[str, object]] = []

        def walk(folder_path: str) -> None:
            response = self._api("GET", self._path_url(folder_path, "/children"))
            for item in response.get("value", []):
                item_path = str(PurePosixPath(folder_path) / item["name"]).replace("\\", "/")
                if not item_path.startswith("/"):
                    item_path = f"/{item_path}"
                if item.get("folder"):
                    walk(item_path)
                else:
                    found.append(
                        {
                            "path": item_path,
                            "size": int(item.get("size", 0) or 0),
                            "mtime": self._to_timestamp(item.get("lastModifiedDateTime")),
                        }
                    )

        def op() -> None:
            self.mkdirs(remote_root)
            walk(self._normalize_remote_path(remote_root))

        self._run(op)
        return found


def transport_for_profile(profile: SyncProfile) -> FileTransport:
    protocol = profile.protocol.lower()
    if protocol in {"ftp", "ftps"}:
        return FTPManager(profile)
    if protocol == "google-drive":
        return GoogleDriveManager(profile)
    if protocol == "onedrive":
        return OneDriveManager(profile)
    return SFTPManager(profile)
