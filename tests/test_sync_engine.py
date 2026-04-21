from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from PySide6.QtCore import QCoreApplication

from gpkg_sync.logging_utils import AppLogger
from gpkg_sync.models import SyncProfile
from gpkg_sync.storage import StateDB
from gpkg_sync.sync_engine import SyncEngine, normalize_remote_path, remote_relpath, safe_relpath, sha1_file


class FakeTransport:
    def __init__(self):
        self.connected = False
        self.files = {}
        self.download_fail = False
        self.removed = []

    def connect(self):
        self.connected = True

    def close(self):
        self.connected = False

    def stat(self, remote_path: str):
        if remote_path not in self.files:
            raise FileNotFoundError(remote_path)
        size, mtime = self.files[remote_path]
        return type("Stat", (), {"st_size": size, "st_mtime": mtime})()

    def exists(self, remote_path: str):
        return remote_path in self.files

    def mkdirs(self, remote_dir: str):
        return None

    def upload(self, local_path: Path, remote_path: str, callback=None):
        data = local_path.read_bytes()
        self.files[remote_path] = (len(data), 100.0)
        if callback:
            callback(len(data), len(data))

    def download(self, remote_path: str, local_path: Path, callback=None):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"remote-bytes")
        if callback:
            callback(len(b"remote-bytes"), len(b"remote-bytes"))
        if self.download_fail:
            raise RuntimeError("download failed")

    def remove(self, remote_path: str):
        self.removed.append(remote_path)
        self.files.pop(remote_path, None)

    def walk_remote_files(self, remote_root: str):
        found = []
        for path, (size, mtime) in self.files.items():
            found.append({"path": path, "size": size, "mtime": mtime})
        return found


class FakeWatcher:
    def __init__(self, root: Path):
        self.root = root
        self.started = False
        self.stopped = False
        self.file_changed = type("SignalStub", (), {"connect": lambda self, func: None})()

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True

    def deleteLater(self):
        return None


class SyncEngineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.qt_app = QCoreApplication.instance() or QCoreApplication([])

    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory()
        base = Path(self.tempdir.name)
        self.base = base
        self.local_dir = base / "local"
        self.local_dir.mkdir()
        self.db = StateDB(base / "state.db")
        self.logger = AppLogger(self.db)
        self.profile = SyncProfile(
            name="prod",
            host="example.com",
            port=22,
            username="alice",
            password="secret",
            protocol="sftp",
            local_dir=str(self.local_dir),
            remote_dir="/remote",
            device_label="device",
        )
        self.transport = FakeTransport()
        self.engine = SyncEngine(self.profile, self.db, self.logger, transport=self.transport, watcher_factory=FakeWatcher)

    def tearDown(self):
        self.tempdir.cleanup()

    def test_path_helpers(self):
        nested = self.local_dir / "a" / "b.gpkg"
        nested.parent.mkdir(parents=True)
        nested.write_text("x", encoding="utf-8")
        self.assertEqual(safe_relpath(nested, self.local_dir), "a/b.gpkg")
        self.assertEqual(normalize_remote_path("/remote", "a/b.gpkg"), "/remote/a/b.gpkg")
        self.assertEqual(remote_relpath("/remote", "/remote/a/b.gpkg"), "a/b.gpkg")

    def test_download_is_atomic(self):
        target = self.local_dir / "remote.gpkg"
        self.transport.files["/remote/remote.gpkg"] = (12, 10.0)
        self.engine.download_remote_file("/remote/remote.gpkg", target, 10.0, 12, "remote newer")
        self.assertTrue(target.exists())
        self.assertEqual(target.read_bytes(), b"remote-bytes")
        self.assertFalse((self.local_dir / ".remote.gpkg.part").exists())

    def test_download_failure_does_not_leave_partial_file(self):
        target = self.local_dir / "remote.gpkg"
        self.transport.files["/remote/remote.gpkg"] = (12, 10.0)
        self.transport.download_fail = True
        with self.assertRaises(RuntimeError):
            self.engine.download_remote_file("/remote/remote.gpkg", target, 10.0, 12, "remote newer")
        self.assertFalse((self.local_dir / ".remote.gpkg.part").exists())

    def test_delete_propagation(self):
        target = self.local_dir / "gone.gpkg"
        target.write_text("old", encoding="utf-8")
        remote = "/remote/gone.gpkg"
        self.transport.files[remote] = (3, 10.0)
        self.profile.delete_missing = True
        target.unlink()
        self.engine.sync_local_change(target)
        self.assertIn(remote, self.transport.removed)

    def test_start_stop_are_idempotent(self):
        self.engine.start()
        self.engine.start()
        self.assertTrue(self.engine.running)
        self.engine.request_stop()
        self.engine.request_stop()
        self.assertFalse(self.engine.running)

    def test_full_sync_uploads_nested_local_file(self):
        nested = self.local_dir / "projects" / "roads.gpkg"
        nested.parent.mkdir(parents=True)
        nested.write_text("nested", encoding="utf-8")

        self.engine.full_sync()

        self.assertIn("/remote/projects/roads.gpkg", self.transport.files)

    def test_full_sync_downloads_nested_remote_file(self):
        self.transport.files["/remote/projects/roads.gpkg"] = (12, 10.0)

        self.engine.full_sync()

        target = self.local_dir / "projects" / "roads.gpkg"
        self.assertTrue(target.exists())
        self.assertEqual(target.read_bytes(), b"remote-bytes")

    def test_full_sync_uploads_non_gpkg_file(self):
        nested = self.local_dir / "docs" / "notes.txt"
        nested.parent.mkdir(parents=True)
        nested.write_text("hello", encoding="utf-8")

        self.engine.full_sync()

        self.assertIn("/remote/docs/notes.txt", self.transport.files)

    def test_full_sync_downloads_file_without_extension(self):
        self.transport.files["/remote/config/README"] = (12, 10.0)

        self.engine.full_sync()

        target = self.local_dir / "config" / "README"
        self.assertTrue(target.exists())
        self.assertEqual(target.read_bytes(), b"remote-bytes")

    def test_full_sync_with_multiple_watch_folders_namespaces_remote_paths(self):
        photos_dir = self.base / "photos"
        docs_dir = self.base / "docs"
        photos_dir.mkdir()
        docs_dir.mkdir()
        profile = SyncProfile(
            name="multi",
            host="example.com",
            port=22,
            username="alice",
            password="secret",
            protocol="sftp",
            local_dir=str(photos_dir),
            watch_dirs=[str(photos_dir), str(docs_dir)],
            remote_dir="/remote",
            device_label="device",
        )
        engine = SyncEngine(profile, self.db, self.logger, transport=self.transport, watcher_factory=FakeWatcher)
        (photos_dir / "img.jpg").write_text("a", encoding="utf-8")
        (docs_dir / "notes.txt").write_text("b", encoding="utf-8")

        engine.full_sync()

        self.assertIn("/remote/photos/img.jpg", self.transport.files)
        self.assertIn("/remote/docs/notes.txt", self.transport.files)

    def test_full_sync_downloads_into_matching_watch_folder_namespace(self):
        photos_dir = self.base / "photos"
        docs_dir = self.base / "docs"
        photos_dir.mkdir()
        docs_dir.mkdir()
        profile = SyncProfile(
            name="multi",
            host="example.com",
            port=22,
            username="alice",
            password="secret",
            protocol="sftp",
            local_dir=str(photos_dir),
            watch_dirs=[str(photos_dir), str(docs_dir)],
            remote_dir="/remote",
            device_label="device",
        )
        engine = SyncEngine(profile, self.db, self.logger, transport=self.transport, watcher_factory=FakeWatcher)
        self.transport.files["/remote/docs/notes.txt"] = (12, 10.0)

        engine.full_sync()

        self.assertTrue((docs_dir / "notes.txt").exists())
        self.assertFalse((photos_dir / "notes.txt").exists())

    def test_full_sync_skips_managed_artifact_files(self):
        artifact = self.local_dir / ".notes.txt.part"
        artifact.write_text("temp", encoding="utf-8")

        self.engine.full_sync()

        self.assertNotIn("/remote/.notes.txt.part", self.transport.files)

    def test_conflict_is_not_created_when_local_content_matches_last_sync(self):
        target = self.local_dir / "report.txt"
        target.write_text("same", encoding="utf-8")
        local_stat = target.stat()

        self.db.upsert_file_state(
            self.profile.name,
            str(target),
            "/remote/report.txt",
            float(local_stat.st_mtime),
            5.0,
            int(local_stat.st_size),
            int(local_stat.st_size),
            sha1_file(target),
            "synced",
            "",
        )
        self.transport.files["/remote/report.txt"] = (len(b"remote-bytes"), 20.0)
        target.touch()

        self.engine.reconcile_single(
            target,
            "/remote/report.txt",
            {"path": "/remote/report.txt", "mtime": 20.0, "size": len(b"remote-bytes")},
        )

        self.assertEqual(target.read_bytes(), b"remote-bytes")
        conflict_files = list(self.local_dir.glob("report.conflict-*"))
        self.assertEqual(conflict_files, [])

    def test_sha1_file_reports_progress(self):
        target = self.local_dir / "big.bin"
        target.write_bytes(b"a" * (2 * 1024 * 1024 + 10))
        updates = []

        sha1_file(target, chunk_size=1024 * 1024, progress=lambda done, total: updates.append((done, total)))

        self.assertTrue(updates)
        self.assertEqual(updates[-1], (target.stat().st_size, target.stat().st_size))
