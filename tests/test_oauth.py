from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gpkg_sync import oauth


class OAuthTests(unittest.TestCase):
    def test_load_google_client_config_finds_package_bundled_client_json(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            package_dir = base / "gpkg_sync"
            package_dir.mkdir()
            client_file = package_dir / "google_oauth_client.json"
            client_file.write_text(
                json.dumps({"installed": {"client_id": "client", "client_secret": "secret"}}),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(oauth, "__file__", str(package_dir / "oauth.py")):
                    config = oauth.load_google_client_config()

            self.assertEqual(config["installed"]["client_id"], "client")

    def test_load_google_client_config_finds_client_json_next_to_frozen_exe(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            package_dir = base / "_internal" / "gpkg_sync"
            package_dir.mkdir(parents=True)
            exe_path = base / "gpkgSyncApp.exe"
            client_file = base / "google_oauth_client.json"
            client_file.write_text(
                json.dumps({"installed": {"client_id": "portable-client", "client_secret": "secret"}}),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(oauth, "__file__", str(package_dir / "oauth.py")):
                    with mock.patch.object(oauth.sys, "frozen", True, create=True):
                        with mock.patch.object(oauth.sys, "executable", str(exe_path)):
                            config = oauth.load_google_client_config()

            self.assertEqual(config["installed"]["client_id"], "portable-client")

    def test_load_dotenv_sets_missing_google_env_vars(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            package_dir = base / "gpkg_sync"
            package_dir.mkdir()
            env_file = base / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "GPKG_SYNC_GOOGLE_CLIENT_ID=test-client-id",
                        "GPKG_SYNC_GOOGLE_CLIENT_SECRET=test-client-secret",
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(oauth, "__file__", str(package_dir / "oauth.py")):
                    oauth._load_dotenv()
                self.assertEqual(os.environ.get("GPKG_SYNC_GOOGLE_CLIENT_ID"), "test-client-id")
                self.assertEqual(os.environ.get("GPKG_SYNC_GOOGLE_CLIENT_SECRET"), "test-client-secret")

    def test_load_dotenv_finds_env_next_to_frozen_exe(self):
        with tempfile.TemporaryDirectory() as tempdir:
            base = Path(tempdir)
            package_dir = base / "_internal" / "gpkg_sync"
            package_dir.mkdir(parents=True)
            exe_path = base / "gpkgSyncApp.exe"
            env_file = base / ".env"
            env_file.write_text(
                "\n".join(
                    [
                        "GPKG_SYNC_GOOGLE_CLIENT_ID=frozen-client-id",
                        "GPKG_SYNC_GOOGLE_CLIENT_SECRET=frozen-client-secret",
                    ]
                ),
                encoding="utf-8",
            )

            with mock.patch.dict(os.environ, {}, clear=True):
                with mock.patch.object(oauth, "__file__", str(package_dir / "oauth.py")):
                    with mock.patch.object(oauth.sys, "frozen", True, create=True):
                        with mock.patch.object(oauth.sys, "executable", str(exe_path)):
                            oauth._load_dotenv()
                self.assertEqual(os.environ.get("GPKG_SYNC_GOOGLE_CLIENT_ID"), "frozen-client-id")
                self.assertEqual(os.environ.get("GPKG_SYNC_GOOGLE_CLIENT_SECRET"), "frozen-client-secret")
