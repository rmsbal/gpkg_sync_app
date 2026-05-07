from __future__ import annotations

import os
import sys
from pathlib import Path


APP_DIR_NAME = ".gpkg_sync"
APP_DIR_ENV = "GPKG_SYNC_APP_DIR"


def app_data_dir() -> Path:
    configured = os.environ.get(APP_DIR_ENV, "").strip()
    if configured:
        return Path(configured).expanduser()
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent / APP_DIR_NAME
    return Path.home() / APP_DIR_NAME
