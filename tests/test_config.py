from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import keyring
from keyring.backend import KeyringBackend

from gpkg_sync.models import SyncProfile
from gpkg_sync.storage import CONFIG_VERSION, SecretStore, SecretStoreError, SettingsStore


class MemoryKeyring(KeyringBackend):
    priority = 1

    def __init__(self):
        self.values = {}

    def get_password(self, service, username):
        return self.values.get((service, username))

    def set_password(self, service, username, password):
        self.values[(service, username)] = password

    def delete_password(self, service, username):
        self.values.pop((service, username), None)


class NullKeyring(KeyringBackend):
    priority = 0

    def get_password(self, service, username):
        return None

    def set_password(self, service, username, password):
        raise RuntimeError("unsupported")

    def delete_password(self, service, username):
        raise RuntimeError("unsupported")


class ConfigTests(unittest.TestCase):
    def setUp(self):
        self.keyring = MemoryKeyring()
        keyring.set_keyring(self.keyring)
        self.tempdir = tempfile.TemporaryDirectory()
        self.base = Path(self.tempdir.name)
        self.local_dir = self.base / "local"
        self.local_dir.mkdir()
        self.config_path = self.base / "profiles.json"
        self.store = SettingsStore(self.config_path, SecretStore("test-gpkg"))

    def tearDown(self):
        self.tempdir.cleanup()

    def make_profile(self, password: str = "secret") -> SyncProfile:
        return SyncProfile(
            name="prod",
            host="example.com",
            port=22,
            username="alice",
            password=password,
            protocol="sftp",
            local_dir=str(self.local_dir),
            remote_dir="/data",
            device_label="device",
        )

    def test_profile_validation(self):
        ok, msg = self.make_profile().validate()
        self.assertTrue(ok, msg)
        invalid = self.make_profile(password="")
        ok, msg = invalid.validate()
        self.assertFalse(ok)

    def test_save_profiles_moves_password_to_keyring(self):
        profile = self.make_profile()
        profile.enabled = False
        profile.watch_dirs = [str(self.local_dir), str(self.base / "archive")]
        (self.base / "archive").mkdir()
        self.store.save_profiles([profile])
        payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["version"], CONFIG_VERSION)
        self.assertNotIn("password", payload["profiles"][0])
        self.assertTrue(payload["profiles"][0]["has_saved_password"])
        self.assertFalse(payload["profiles"][0]["enabled"])
        self.assertEqual(payload["profiles"][0]["watch_dirs"], [str(self.local_dir), str(self.base / "archive")])
        loaded = self.store.load_profiles()
        self.assertEqual(loaded[0].password, "secret")
        self.assertFalse(loaded[0].enabled)
        self.assertEqual(loaded[0].effective_watch_dirs(), [str(self.local_dir), str(self.base / "archive")])

    def test_legacy_config_is_migrated(self):
        legacy = [
            {
                "name": "prod",
                "host": "example.com",
                "port": 22,
                "username": "alice",
                "password": "legacy-secret",
                "protocol": "sftp",
                "local_dir": str(self.local_dir),
                "remote_dir": "/data",
                "direction": "two-way",
                "device_label": "device",
                "stability_wait_seconds": 5,
            }
        ]
        self.config_path.write_text(json.dumps(legacy), encoding="utf-8")
        loaded = self.store.load_profiles()
        self.assertEqual(loaded[0].password, "legacy-secret")
        migrated = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(migrated["version"], CONFIG_VERSION)
        self.assertNotIn("password", migrated["profiles"][0])

    def test_missing_keyring_backend_fails_closed(self):
        keyring.set_keyring(NullKeyring())
        store = SettingsStore(self.config_path, SecretStore("test-gpkg"))
        with self.assertRaises(SecretStoreError):
            store.save_profiles([self.make_profile()])

    def test_profile_validation_rejects_duplicate_watch_folder_names(self):
        left = self.base / "one" / "shared"
        right = self.base / "two" / "shared"
        left.mkdir(parents=True)
        right.mkdir(parents=True)
        profile = self.make_profile()
        profile.watch_dirs = [str(left), str(right)]
        ok, message = profile.validate()
        self.assertFalse(ok)
        self.assertIn("unique folder names", message)

    def test_google_drive_profile_validation_uses_app_oauth_config(self):
        profile = self.make_profile(password="")
        profile.protocol = "google-drive"
        profile.host = ""
        profile.port = 0
        profile.username = ""
        profile.key_path = ""
        with mock.patch("gpkg_sync.models.has_google_oauth_config", return_value=True):
            ok, msg = profile.validate()

        self.assertTrue(ok, msg)

    def test_google_drive_profile_validation_fails_when_app_oauth_missing(self):
        profile = self.make_profile(password="")
        profile.protocol = "google-drive"
        profile.host = ""
        profile.port = 0
        profile.username = ""
        profile.key_path = ""

        with mock.patch("gpkg_sync.models.has_google_oauth_config", return_value=False):
            ok, msg = profile.validate()

        self.assertFalse(ok)
        self.assertIn("Google Drive sign-in is not configured", msg)

    def test_profile_defaults_to_enabled_when_not_present_in_metadata(self):
        profile = SyncProfile.from_metadata(
            {
                "name": "prod",
                "host": "example.com",
                "port": 22,
                "username": "alice",
                "protocol": "sftp",
                "local_dir": str(self.local_dir),
                "remote_dir": "/data",
                "direction": "two-way",
                "device_label": "device",
                "stability_wait_seconds": 5,
            }
        )

        self.assertTrue(profile.enabled)

    def test_save_profiles_preserves_profile_order(self):
        first = self.make_profile()
        second = self.make_profile(password="other")
        second.name = "staging"

        self.store.save_profiles([second, first])
        loaded = self.store.load_profiles()

        self.assertEqual([profile.name for profile in loaded], ["staging", "prod"])

    def test_onedrive_profile_validation_requires_client_and_tenant(self):
        profile = self.make_profile(password="")
        profile.protocol = "onedrive"
        profile.host = ""
        profile.port = 0
        profile.username = ""
        profile.key_path = ""
        profile.client_id = "client-id"
        profile.tenant_id = "tenant-id"

        ok, msg = profile.validate()

        self.assertTrue(ok, msg)
