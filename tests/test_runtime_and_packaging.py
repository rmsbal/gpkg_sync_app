from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path
from unittest import mock

from gpkg_sync.app import runtime_preflight


ROOT = Path(__file__).resolve().parent.parent


class RuntimeAndPackagingTests(unittest.TestCase):
    def test_preflight_reports_missing_display(self):
        with mock.patch.dict(os.environ, {}, clear=True):
            with mock.patch("sys.platform", "linux"):
                self.assertIn("graphical display", runtime_preflight())

    def test_preflight_reports_missing_xcb_cursor(self):
        env = {"DISPLAY": ":0"}
        with mock.patch.dict(os.environ, env, clear=True):
            with mock.patch("sys.platform", "linux"):
                with mock.patch("ctypes.util.find_library", return_value=None):
                    self.assertIn("libxcb-cursor0", runtime_preflight())

    def test_packaging_script_has_expected_build_hooks(self):
        cmd_text = (ROOT / "cmd").read_text(encoding="utf-8")
        spec_text = (ROOT / "gpkgSyncApp.spec").read_text(encoding="utf-8")
        self.assertIn("AppRun", cmd_text)
        self.assertIn('SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"', cmd_text)
        self.assertIn('APP_VERSION="${APP_VERSION:-1.2}"', cmd_text)
        self.assertIn('ARTIFACT_NAME="${ARTIFACT_NAME:-gpkg_sync-${APP_VERSION}-${APPIMAGE_ARCH}.AppImage}"', cmd_text)
        self.assertIn('find "$TMP_OUTPUT_DIR" -maxdepth 1 -type f -name \'*.AppImage\'', cmd_text)
        self.assertIn("collect_all('PySide6')", spec_text)
        subprocess.run(["bash", "-n", str(ROOT / "cmd")], check=True)

    def test_windows_packaging_assets_exist(self):
        build_script = (ROOT / "build_windows.ps1").read_text(encoding="utf-8")
        windows_spec = (ROOT / "gpkgSyncApp.windows.spec").read_text(encoding="utf-8")
        installer_script = (ROOT / "windows-installer" / "gpkgSyncApp.iss").read_text(encoding="utf-8")
        self.assertIn("pyinstaller --noconfirm", build_script)
        self.assertIn("ISCC", build_script)
        self.assertIn("('PySide6', 'keyring')", windows_spec)
        self.assertIn("collect_all(package_name)", windows_spec)
        self.assertIn("OutputBaseFilename=gpkg_sync_setup", installer_script)
