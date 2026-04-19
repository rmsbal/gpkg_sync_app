from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

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
        self.store.save_profiles([profile])
        payload = json.loads(self.config_path.read_text(encoding="utf-8"))
        self.assertEqual(payload["version"], CONFIG_VERSION)
        self.assertNotIn("password", payload["profiles"][0])
        self.assertTrue(payload["profiles"][0]["has_saved_password"])
        loaded = self.store.load_profiles()
        self.assertEqual(loaded[0].password, "secret")

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
