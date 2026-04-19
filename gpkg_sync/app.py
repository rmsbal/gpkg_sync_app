from __future__ import annotations

import ctypes.util
import os
import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication, QMessageBox

from .models import APP_NAME
from .ui import MainWindow


def ensure_app_dir() -> None:
    (Path.home() / ".gpkg_sync").mkdir(parents=True, exist_ok=True)


def runtime_preflight() -> str | None:
    if sys.platform != "linux":
        return None
    if not os.environ.get("DISPLAY") and not os.environ.get("WAYLAND_DISPLAY"):
        return "No graphical display is available. Start the app from a desktop session."
    if ctypes.util.find_library("xcb-cursor") is None:
        return "Missing Qt runtime dependency: libxcb-cursor0. Install it before launching the app."
    return None


def main() -> int:
    ensure_app_dir()
    failure = runtime_preflight()
    if failure:
        app = QApplication(sys.argv)
        QMessageBox.critical(None, APP_NAME, failure)
        return 1
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setOrganizationName("Appzter")
    window = MainWindow()
    window.show()
    return app.exec()
