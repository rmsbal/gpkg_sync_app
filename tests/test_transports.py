from __future__ import annotations

import ftplib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock

from gpkg_sync.models import SyncProfile
from gpkg_sync.transports import (
    FTPManager,
    GoogleDriveManager,
    ONEDRIVE_SIMPLE_UPLOAD_LIMIT,
    OneDriveManager,
    SFTPManager,
    transport_for_profile,
)


class _ClosableFTP:
    def quit(self):
        return None

    def close(self):
        return None


class _TimeoutMLSDFtp(_ClosableFTP):
    def mlsd(self, remote_root):
        raise TimeoutError("timed out")


class _MissingRootFtp(_ClosableFTP):
    def mlsd(self, remote_root):
        raise ftplib.error_perm("550 No such file or directory")


class FTPManagerTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        local_dir = Path(self.tempdir.name)
        self.profile = SyncProfile(
            name="prod",
            host="example.com",
            port=21,
            username="alice",
            password="secret",
            protocol="ftp",
            local_dir=str(local_dir),
            remote_dir="/remote",
            device_label="device",
        )

    def tearDown(self):
        self.tempdir.cleanup()

    def test_run_does_not_retry_initial_connect_timeout(self):
        manager = FTPManager(self.profile)
        manager.connect = Mock(side_effect=TimeoutError("timed out"))
        operation = Mock(return_value="ok")

        with self.assertRaises(TimeoutError):
            manager._run(operation)

        self.assertEqual(manager.connect.call_count, 1)
        operation.assert_not_called()

    def test_run_retries_once_when_existing_connection_times_out(self):
        manager = FTPManager(self.profile)
        manager.ftp = _ClosableFTP()
        operation = Mock(side_effect=[TimeoutError("timed out"), "ok"])

        def reconnect():
            manager.ftp = _ClosableFTP()

        manager.connect = Mock(side_effect=reconnect)

        result = manager._run(operation)

        self.assertEqual(result, "ok")
        self.assertEqual(manager.connect.call_count, 1)
        self.assertEqual(operation.call_count, 2)

    def test_walk_remote_files_timeout_does_not_create_root(self):
        manager = FTPManager(self.profile)
        manager.ftp = _TimeoutMLSDFtp()
        manager.connect = Mock(side_effect=TimeoutError("timed out"))
        manager.mkdirs = Mock()

        with self.assertRaises(TimeoutError):
            manager.walk_remote_files("/remote")

        manager.mkdirs.assert_not_called()

    def test_walk_remote_files_creates_missing_root(self):
        manager = FTPManager(self.profile)
        manager.ftp = _MissingRootFtp()
        manager.mkdirs = Mock()

        result = manager.walk_remote_files("/remote")

        self.assertEqual(result, [])
        manager.mkdirs.assert_called_once_with("/remote")

    def test_transport_factory_returns_google_drive_manager(self):
        self.profile.protocol = "google-drive"

        manager = transport_for_profile(self.profile)

        self.assertIsInstance(manager, GoogleDriveManager)

    def test_transport_factory_returns_onedrive_manager(self):
        self.profile.protocol = "onedrive"
        self.profile.client_id = "client-id"
        self.profile.tenant_id = "tenant-id"

        manager = transport_for_profile(self.profile)

        self.assertIsInstance(manager, OneDriveManager)

    def test_transport_factory_returns_sftp_manager_by_default(self):
        self.profile.protocol = "sftp"
        self.profile.port = 22
        self.profile.key_path = ""

        manager = transport_for_profile(self.profile)

        self.assertIsInstance(manager, SFTPManager)

    def test_onedrive_large_upload_uses_chunked_session(self):
        self.profile.protocol = "onedrive"
        self.profile.client_id = "client-id"
        self.profile.tenant_id = "tenant-id"
        manager = OneDriveManager(self.profile)
        chunks = []

        class _Response:
            def raise_for_status(self):
                return None

        class _Session:
            def put(self, url, data=None, headers=None):
                payload = data if isinstance(data, bytes) else data.read()
                chunks.append((url, len(payload), headers or {}))
                return _Response()

        local_path = Path(self.tempdir.name) / "large.bin"
        local_path.write_bytes(b"a" * (ONEDRIVE_SIMPLE_UPLOAD_LIMIT + 1024))
        manager.session = _Session()
        manager.mkdirs = Mock()
        manager._api = Mock(return_value={"uploadUrl": "https://upload.example/session"})
        progress = []

        manager.upload(local_path, "/remote/large.bin", callback=lambda sent, total: progress.append((sent, total)))

        self.assertGreater(len(chunks), 1)
        self.assertEqual(progress[-1], (local_path.stat().st_size, local_path.stat().st_size))

    def test_google_drive_chunk_retry_retries_transient_errors(self):
        manager = GoogleDriveManager(self.profile)

        class _Resp:
            status = 503

        class _HttpError(Exception):
            def __init__(self):
                self.resp = _Resp()

        attempts = {"count": 0}

        def flaky():
            attempts["count"] += 1
            if attempts["count"] == 1:
                raise _HttpError()
            return ("status", "response")

        result = manager._next_chunk_with_retry(flaky)

        self.assertEqual(result, ("status", "response"))
        self.assertEqual(attempts["count"], 2)
