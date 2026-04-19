from __future__ import annotations

import logging
import time

from PySide6.QtCore import QObject, Signal

from .storage import StateDB


class AppLogger(QObject):
    log_emitted = Signal(str, str, str, float)

    def __init__(self, db: StateDB):
        super().__init__()
        self.db = db
        self._logger = logging.getLogger("gpkg_sync")
        if not self._logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
            self._logger.addHandler(handler)
        self._logger.setLevel(logging.INFO)

    def log(self, level: str, code: str, message: str) -> None:
        ts = time.time()
        self.db.add_log(ts, level, code, message)
        self._logger.log(getattr(logging, level.upper(), logging.INFO), "[%s] %s", code, message)
        self.log_emitted.emit(level, code, message, ts)
