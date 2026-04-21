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
