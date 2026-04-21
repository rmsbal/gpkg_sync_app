from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from gpkg_sync.startup import StartupManager, current_launch_command


class StartupTests(unittest.TestCase):
    def test_linux_autostart_desktop_file_is_created_and_removed(self):
        with tempfile.TemporaryDirectory() as tempdir:
            manager = StartupManager(
                app_name="gpkg sync",
                command=["/opt/gpkg sync/gpkgSyncApp.AppImage"],
                autostart_dir=Path(tempdir),
            )
            with mock.patch("sys.platform", "linux"):
                manager.set_enabled(True)

                desktop_path = Path(tempdir) / "gpkg-sync.desktop"
                text = desktop_path.read_text(encoding="utf-8")
                self.assertIn("[Desktop Entry]", text)
                self.assertIn("Name=gpkg sync", text)
                self.assertIn('Exec="/opt/gpkg sync/gpkgSyncApp.AppImage"', text)
                self.assertTrue(manager.is_enabled())

                manager.set_enabled(False)
                self.assertFalse(desktop_path.exists())

    def test_current_launch_command_prefers_appimage_path(self):
        with mock.patch.dict("os.environ", {"APPIMAGE": "/apps/gpkg_sync.AppImage"}):
            self.assertEqual(current_launch_command(), ["/apps/gpkg_sync.AppImage"])

    def test_current_launch_command_uses_frozen_executable(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with mock.patch.object(sys, "frozen", True, create=True):
                with mock.patch("sys.executable", "/opt/gpkgSyncApp"):
                    self.assertEqual(current_launch_command(), ["/opt/gpkgSyncApp"])

    def test_windows_startup_uses_run_registry_key(self):
        fake_key = object()
        fake_winreg = mock.Mock()
        fake_winreg.HKEY_CURRENT_USER = object()
        fake_winreg.KEY_SET_VALUE = 2
        fake_winreg.REG_SZ = 1
        fake_winreg.CreateKeyEx = mock.MagicMock()
        fake_winreg.CreateKeyEx.return_value.__enter__.return_value = fake_key

        manager = StartupManager(app_name="gpkg sync", command=[r"C:\Program Files\gpkg sync\gpkgSyncApp.exe"])

        with mock.patch("sys.platform", "win32"):
            with mock.patch.dict("sys.modules", {"winreg": fake_winreg}):
                manager.set_enabled(True)

        fake_winreg.SetValueEx.assert_called_once_with(
            fake_key,
            "gpkg sync",
            0,
            fake_winreg.REG_SZ,
            '"C:\\Program Files\\gpkg sync\\gpkgSyncApp.exe"',
        )
