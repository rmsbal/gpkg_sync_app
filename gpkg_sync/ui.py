from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from PySide6.QtCore import QMetaObject, QObject, Qt, QThread, QTimer, Signal
from PySide6.QtGui import QAction, QCloseEvent, QIcon, QTextCursor
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QGridLayout,
    QGroupBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QMenu,
    QMessageBox,
    QPushButton,
    QPlainTextEdit,
    QProgressBar,
    QSpinBox,
    QSystemTrayIcon,
    QTableWidget,
    QTableWidgetItem,
    QVBoxLayout,
    QWidget,
)

from .logging_utils import AppLogger
from .models import APP_NAME, APP_VERSION, DEFAULT_PORTS, SyncProfile, default_device_label
from .storage import ConfigError, SecretStore, SecretStoreError, SettingsStore, StateDB
from .sync_engine import SyncEngine, fmt_ts


LOG_MAX_LINES = 1500
APP_DIR = Path.home() / ".gpkg_sync"
DB_PATH = APP_DIR / "gpkg_sync.db"
CONFIG_PATH = APP_DIR / "profiles.json"


class ProfileDialog(QDialog):
    def __init__(self, parent: Optional[QWidget] = None, profile: Optional[SyncProfile] = None):
        super().__init__(parent)
        self.setWindowTitle("Profile")
        self.setModal(True)
        self.result_profile: Optional[SyncProfile] = None
        self._build_ui()
        if profile:
            self._load(profile)

    def _build_ui(self) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.name_edit = QLineEdit()
        self.host_edit = QLineEdit()
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.port_spin.setValue(22)
        self.protocol_combo = QComboBox()
        self.protocol_combo.addItems(["sftp", "ftp", "ftps"])
        self.protocol_combo.currentTextChanged.connect(self._apply_protocol_default)
        self.username_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.key_path_edit = QLineEdit()
        self.watch_dirs_list = QListWidget()
        self.watch_dirs_list.setSelectionMode(QListWidget.SingleSelection)
        self.remote_dir_edit = QLineEdit()
        self.direction_combo = QComboBox()
        self.direction_combo.addItems(["upload-only", "download-only", "two-way"])
        self.auto_start_check = QCheckBox("Start syncing when app opens")
        self.backup_check = QCheckBox("Backup before local overwrite")
        self.backup_check.setChecked(True)
        self.delete_missing_check = QCheckBox("Delete remote file when local file is deleted")
        self.device_label_edit = QLineEdit(default_device_label())
        self.stability_spin = QSpinBox()
        self.stability_spin.setRange(2, 120)
        self.stability_spin.setValue(5)

        browse_key_btn = QPushButton("Browse")
        browse_key_btn.clicked.connect(self._browse_key)
        add_watch_btn = QPushButton("Add folder")
        add_watch_btn.clicked.connect(self._browse_local)
        remove_watch_btn = QPushButton("Remove selected")
        remove_watch_btn.clicked.connect(self._remove_selected_watch_dir)

        key_layout = QHBoxLayout()
        key_layout.addWidget(self.key_path_edit)
        key_layout.addWidget(browse_key_btn)
        watch_dir_buttons = QHBoxLayout()
        watch_dir_buttons.addWidget(add_watch_btn)
        watch_dir_buttons.addWidget(remove_watch_btn)
        watch_dir_buttons.addStretch(1)
        watch_dirs_layout = QVBoxLayout()
        watch_dirs_layout.addWidget(self.watch_dirs_list)
        watch_dirs_layout.addLayout(watch_dir_buttons)

        form.addRow("Profile name", self.name_edit)
        form.addRow("Host", self.host_edit)
        form.addRow("Protocol", self.protocol_combo)
        form.addRow("Port", self.port_spin)
        form.addRow("Username", self.username_edit)
        form.addRow("Password", self.password_edit)
        form.addRow("SSH key", self._wrap_layout(key_layout))
        form.addRow("Watch folders", self._wrap_layout(watch_dirs_layout))
        form.addRow("Remote folder", self.remote_dir_edit)
        form.addRow("Direction", self.direction_combo)
        form.addRow("Device label", self.device_label_edit)
        form.addRow("Stability wait (sec)", self.stability_spin)
        form.addRow("", self.auto_start_check)
        form.addRow("", self.backup_check)
        form.addRow("", self.delete_missing_check)

        layout.addLayout(form)
        buttons = QHBoxLayout()
        test_btn = QPushButton("Test connection")
        save_btn = QPushButton("Save")
        cancel_btn = QPushButton("Cancel")
        test_btn.clicked.connect(self._test_connection)
        save_btn.clicked.connect(self._save)
        cancel_btn.clicked.connect(self.reject)
        buttons.addWidget(test_btn)
        buttons.addStretch(1)
        buttons.addWidget(save_btn)
        buttons.addWidget(cancel_btn)
        layout.addLayout(buttons)
        self.resize(640, 420)

    def _wrap_layout(self, layout) -> QWidget:
        widget = QWidget()
        widget.setLayout(layout)
        return widget

    def _apply_protocol_default(self, protocol: str) -> None:
        default_port = DEFAULT_PORTS.get(protocol)
        if default_port:
            self.port_spin.setValue(default_port)

    def _browse_key(self) -> None:
        path, _ = QFileDialog.getOpenFileName(self, "Choose SSH Private Key")
        if path:
            self.key_path_edit.setText(path)

    def _browse_local(self) -> None:
        path = QFileDialog.getExistingDirectory(self, "Choose Local Folder")
        if path and not self._has_watch_dir(path):
            self.watch_dirs_list.addItem(path)

    def _remove_selected_watch_dir(self) -> None:
        row = self.watch_dirs_list.currentRow()
        if row >= 0:
            self.watch_dirs_list.takeItem(row)

    def _load(self, profile: SyncProfile) -> None:
        self.name_edit.setText(profile.name)
        self.host_edit.setText(profile.host)
        self.protocol_combo.setCurrentText(profile.protocol)
        self.port_spin.setValue(profile.port)
        self.username_edit.setText(profile.username)
        self.password_edit.setText(profile.password)
        self.key_path_edit.setText(profile.key_path)
        self._set_watch_dirs(profile.effective_watch_dirs())
        self.remote_dir_edit.setText(profile.remote_dir)
        self.direction_combo.setCurrentText(profile.direction)
        self.auto_start_check.setChecked(profile.auto_start)
        self.backup_check.setChecked(profile.backup_before_overwrite)
        self.delete_missing_check.setChecked(profile.delete_missing)
        self.device_label_edit.setText(profile.device_label)
        self.stability_spin.setValue(profile.stability_wait_seconds)

    def _collect(self) -> SyncProfile:
        watch_dirs = self._watch_dirs()
        return SyncProfile(
            name=self.name_edit.text().strip(),
            host=self.host_edit.text().strip(),
            protocol=self.protocol_combo.currentText(),
            port=int(self.port_spin.value()),
            username=self.username_edit.text().strip(),
            password=self.password_edit.text(),
            key_path=self.key_path_edit.text().strip(),
            local_dir=watch_dirs[0] if watch_dirs else "",
            watch_dirs=watch_dirs,
            remote_dir=self.remote_dir_edit.text().strip(),
            direction=self.direction_combo.currentText(),
            auto_start=self.auto_start_check.isChecked(),
            backup_before_overwrite=self.backup_check.isChecked(),
            delete_missing=self.delete_missing_check.isChecked(),
            device_label=self.device_label_edit.text().strip() or "device",
            stability_wait_seconds=int(self.stability_spin.value()),
        )

    def _watch_dirs(self) -> List[str]:
        return [self.watch_dirs_list.item(index).text().strip() for index in range(self.watch_dirs_list.count()) if self.watch_dirs_list.item(index).text().strip()]

    def _set_watch_dirs(self, watch_dirs: List[str]) -> None:
        self.watch_dirs_list.clear()
        for watch_dir in watch_dirs:
            self.watch_dirs_list.addItem(watch_dir)

    def _has_watch_dir(self, path: str) -> bool:
        normalized = str(Path(path).resolve())
        for index in range(self.watch_dirs_list.count()):
            item = self.watch_dirs_list.item(index)
            if str(Path(item.text()).resolve()) == normalized:
                return True
        return False

    def _test_connection(self) -> None:
        profile = self._collect()
        ok, message = profile.validate()
        if not ok:
            QMessageBox.warning(self, APP_NAME, message)
            return
        temp_logger = AppLogger(StateDB(DB_PATH))
        engine = SyncEngine(profile, StateDB(DB_PATH), temp_logger)
        success, detail = engine.test_connection()
        box = QMessageBox.information if success else QMessageBox.critical
        box(self, APP_NAME, detail)

    def _save(self) -> None:
        profile = self._collect()
        ok, message = profile.validate()
        if not ok:
            QMessageBox.warning(self, APP_NAME, message)
            return
        self.result_profile = profile
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        APP_DIR.mkdir(parents=True, exist_ok=True)
        self.secret_store = SecretStore()
        self.db = StateDB(DB_PATH)
        self.logger = AppLogger(self.db)
        self.logger.log_emitted.connect(self.append_log_entry)
        self.store = SettingsStore(CONFIG_PATH, self.secret_store)
        self.profiles: List[SyncProfile] = []
        self.engines: Dict[str, SyncEngine] = {}
        self.threads: Dict[str, QThread] = {}
        self.current_profile_name: Optional[str] = None
        self.tray: Optional[QSystemTrayIcon] = None
        self.setWindowTitle(f"{APP_NAME} {APP_VERSION}")
        self.resize(1100, 760)
        self._build_ui()
        self._build_tray()
        self._load_profiles()
        self._load_profiles_ui()
        self._load_logs()
        self._auto_start_profiles()

    def _load_profiles(self) -> None:
        try:
            self.profiles = self.store.load_profiles()
        except (ConfigError, SecretStoreError) as exc:
            QMessageBox.critical(self, APP_NAME, str(exc))
            self.profiles = []

    def _build_ui(self) -> None:
        central = QWidget()
        root = QHBoxLayout(central)

        left_box = QVBoxLayout()
        profile_group = QGroupBox("Profiles")
        profile_layout = QVBoxLayout(profile_group)
        self.profile_list = QListWidget()
        self.profile_list.currentItemChanged.connect(self.on_profile_selected)
        profile_layout.addWidget(self.profile_list)
        button_grid = QGridLayout()
        self.add_btn = QPushButton("Add")
        self.edit_btn = QPushButton("Edit")
        self.delete_btn = QPushButton("Delete")
        self.start_btn = QPushButton("Start")
        self.stop_btn = QPushButton("Stop")
        self.sync_now_btn = QPushButton("Sync now")
        self.add_btn.clicked.connect(self.add_profile)
        self.edit_btn.clicked.connect(self.edit_profile)
        self.delete_btn.clicked.connect(self.delete_profile)
        self.start_btn.clicked.connect(self.start_selected_profile)
        self.stop_btn.clicked.connect(self.stop_selected_profile)
        self.sync_now_btn.clicked.connect(self.sync_now_selected)
        button_grid.addWidget(self.add_btn, 0, 0)
        button_grid.addWidget(self.edit_btn, 0, 1)
        button_grid.addWidget(self.delete_btn, 1, 0)
        button_grid.addWidget(self.start_btn, 1, 1)
        button_grid.addWidget(self.stop_btn, 2, 0)
        button_grid.addWidget(self.sync_now_btn, 2, 1)
        profile_layout.addLayout(button_grid)
        left_box.addWidget(profile_group)

        right_box = QVBoxLayout()
        status_group = QGroupBox("Status")
        status_layout = QFormLayout(status_group)
        self.status_value = QLabel("Idle")
        self.profile_name_value = QLabel("-")
        self.local_dir_value = QLabel("-")
        self.local_dir_value.setWordWrap(True)
        self.remote_dir_value = QLabel("-")
        self.direction_value = QLabel("-")
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        status_layout.addRow("Profile", self.profile_name_value)
        status_layout.addRow("Watch folders", self.local_dir_value)
        status_layout.addRow("Remote folder", self.remote_dir_value)
        status_layout.addRow("Direction", self.direction_value)
        status_layout.addRow("Engine status", self.status_value)
        status_layout.addRow("Progress", self.progress_bar)
        right_box.addWidget(status_group)

        history_group = QGroupBox("Recent synced files")
        history_layout = QVBoxLayout(history_group)
        self.history_table = QTableWidget(0, 4)
        self.history_table.setHorizontalHeaderLabels(["Time", "File", "Direction", "Status"])
        self.history_table.horizontalHeader().setStretchLastSection(True)
        history_layout.addWidget(self.history_table)
        right_box.addWidget(history_group, 2)

        log_group = QGroupBox("Logs")
        log_layout = QVBoxLayout(log_group)
        self.log_edit = QPlainTextEdit()
        self.log_edit.setReadOnly(True)
        log_layout.addWidget(self.log_edit)
        right_box.addWidget(log_group, 3)

        root.addLayout(left_box, 1)
        root.addLayout(right_box, 3)
        self.setCentralWidget(central)

    def _build_tray(self) -> None:
        if not QSystemTrayIcon.isSystemTrayAvailable():
            return
        self.tray = QSystemTrayIcon(self)
        self.tray.setToolTip(APP_NAME)
        self.tray.setIcon(QIcon(str(Path(__file__).resolve().parent.parent / "gpkgSyncApp.png")))
        menu = QMenu()
        open_action = QAction("Open", self)
        open_action.triggered.connect(self.showNormal)
        quit_action = QAction("Quit", self)
        quit_action.triggered.connect(self.quit_app)
        menu.addAction(open_action)
        menu.addSeparator()
        menu.addAction(quit_action)
        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._tray_activated)
        self.tray.show()

    def _tray_activated(self, reason):
        if reason == QSystemTrayIcon.Trigger:
            self.showNormal()
            self.raise_()
            self.activateWindow()

    def _load_profiles_ui(self) -> None:
        self.profile_list.clear()
        for profile in self.profiles:
            self.profile_list.addItem(QListWidgetItem(profile.name))
        if self.profiles:
            self.profile_list.setCurrentRow(0)

    def _load_logs(self) -> None:
        self.log_edit.clear()
        for row in reversed(self.db.get_recent_logs(200)):
            self.append_log_entry(row["level"], row["code"], row["message"], row["ts"])

    def append_log_entry(self, level: str, code: str, message: str, ts: float) -> None:
        line = f"[{fmt_ts(ts)}] [{level}] [{code}] {message}"
        self.log_edit.appendPlainText(line)
        lines = self.log_edit.toPlainText().splitlines()
        if len(lines) > LOG_MAX_LINES:
            self.log_edit.setPlainText("\n".join(lines[-LOG_MAX_LINES:]))
        self.log_edit.moveCursor(QTextCursor.End)
        if self.tray and level in {"WARNING", "ERROR"}:
            self.tray.showMessage(APP_NAME, message)

    def get_selected_profile(self) -> Optional[SyncProfile]:
        row = self.profile_list.currentRow()
        if row < 0 or row >= len(self.profiles):
            return None
        return self.profiles[row]

    def on_profile_selected(self, current: Optional[QListWidgetItem], previous: Optional[QListWidgetItem]) -> None:
        profile = self.get_selected_profile()
        if not profile:
            self.current_profile_name = None
            return
        self.current_profile_name = profile.name
        self.profile_name_value.setText(profile.name)
        self.local_dir_value.setText("\n".join(profile.effective_watch_dirs()) or "-")
        self.remote_dir_value.setText(profile.remote_dir)
        self.direction_value.setText(profile.direction)
        self.status_value.setText("Running" if profile.name in self.engines else "Stopped")
        self.load_history(profile.name)

    def load_history(self, profile_name: str) -> None:
        rows = self.db.get_states_for_profile(profile_name)
        self.history_table.setRowCount(0)
        for row in rows[:100]:
            index = self.history_table.rowCount()
            self.history_table.insertRow(index)
            local_name = Path(row["local_path"]).name if row["local_path"] else "-"
            remote_path = row["remote_path"] or "-"
            file_name = local_name if local_name != "." else remote_path
            self.history_table.setItem(index, 0, QTableWidgetItem(fmt_ts(row["last_sync_time"])))
            self.history_table.setItem(index, 1, QTableWidgetItem(file_name))
            self.history_table.setItem(index, 2, QTableWidgetItem(self._infer_direction(row)))
            self.history_table.setItem(index, 3, QTableWidgetItem(row["status"] or "-"))

    def _infer_direction(self, row: sqlite3.Row) -> str:
        local_mtime = row["local_mtime"] or 0
        remote_mtime = row["remote_mtime"] or 0
        return "upload/check" if local_mtime >= remote_mtime else "download/check"

    def _save_profiles(self) -> bool:
        try:
            self.store.save_profiles(self.profiles)
            return True
        except (ConfigError, SecretStoreError) as exc:
            QMessageBox.critical(self, APP_NAME, str(exc))
            return False

    def add_profile(self) -> None:
        dialog = ProfileDialog(self)
        if dialog.exec() == QDialog.Accepted and dialog.result_profile:
            if any(profile.name == dialog.result_profile.name for profile in self.profiles):
                QMessageBox.warning(self, APP_NAME, "A profile with that name already exists.")
                return
            self.profiles.append(dialog.result_profile)
            if self._save_profiles():
                self._load_profiles_ui()

    def edit_profile(self) -> None:
        profile = self.get_selected_profile()
        if not profile:
            return
        if profile.name in self.engines:
            QMessageBox.warning(self, APP_NAME, "Stop the profile before editing it.")
            return
        row = self.profile_list.currentRow()
        dialog = ProfileDialog(self, profile)
        if dialog.exec() == QDialog.Accepted and dialog.result_profile:
            self.profiles[row] = dialog.result_profile
            if self._save_profiles():
                self._load_profiles_ui()
                self.profile_list.setCurrentRow(row)

    def delete_profile(self) -> None:
        profile = self.get_selected_profile()
        if not profile:
            return
        if profile.name in self.engines:
            QMessageBox.warning(self, APP_NAME, "Stop the profile before deleting it.")
            return
        if QMessageBox.question(self, APP_NAME, f"Delete profile '{profile.name}'?") != QMessageBox.Yes:
            return
        self.profiles.pop(self.profile_list.currentRow())
        if self._save_profiles():
            self._load_profiles_ui()

    def start_selected_profile(self) -> None:
        profile = self.get_selected_profile()
        if profile:
            self.start_profile(profile)

    def stop_selected_profile(self) -> None:
        profile = self.get_selected_profile()
        if profile:
            self.stop_profile(profile.name)

    def sync_now_selected(self) -> None:
        profile = self.get_selected_profile()
        if not profile or profile.name not in self.engines:
            QMessageBox.information(self, APP_NAME, "Start the profile first.")
            return
        QMetaObject.invokeMethod(self.engines[profile.name], "request_full_sync", Qt.QueuedConnection)

    def start_profile(self, profile: SyncProfile) -> None:
        if profile.name in self.engines:
            QMessageBox.information(self, APP_NAME, "Profile is already running.")
            return
        thread = QThread(self)
        engine = SyncEngine(profile, self.db, self.logger)
        engine.moveToThread(thread)
        thread.started.connect(engine.start)
        engine.status_changed.connect(self.on_engine_status)
        engine.progress_changed.connect(self.on_engine_progress)
        engine.file_synced.connect(self.on_file_synced)
        engine.stopped.connect(thread.quit)
        engine.stopped.connect(self.on_engine_stopped)
        thread.finished.connect(thread.deleteLater)
        self.engines[profile.name] = engine
        self.threads[profile.name] = thread
        thread.start()
        self.on_profile_selected(None, None)

    def stop_profile(self, profile_name: str) -> None:
        engine = self.engines.get(profile_name)
        thread = self.threads.get(profile_name)
        if not engine or not thread:
            return
        QMetaObject.invokeMethod(engine, "request_stop", Qt.QueuedConnection)

    def on_engine_status(self, profile_name: str, status: str) -> None:
        if self.current_profile_name == profile_name:
            self.status_value.setText(status)

    def on_engine_progress(self, profile_name: str, file_rel: str, pct: int) -> None:
        if self.current_profile_name == profile_name:
            self.progress_bar.setValue(pct)

    def on_file_synced(self, info: dict) -> None:
        if self.current_profile_name == info.get("profile"):
            self.load_history(info["profile"])

    def on_engine_stopped(self, profile_name: str) -> None:
        thread = self.threads.pop(profile_name, None)
        self.engines.pop(profile_name, None)
        if thread is not None:
            thread.wait(5000)
        self.on_profile_selected(None, None)

    def _auto_start_profiles(self) -> None:
        for profile in self.profiles:
            if profile.auto_start:
                QTimer.singleShot(500, lambda p=profile: self.start_profile(p))

    def closeEvent(self, event: QCloseEvent) -> None:
        if self.tray and self.tray.isVisible():
            self.hide()
            event.ignore()
            self.tray.showMessage(APP_NAME, "App minimized to tray and continues syncing.")
            return
        super().closeEvent(event)

    def quit_app(self) -> None:
        for name in list(self.engines.keys()):
            self.stop_profile(name)
        for thread in list(self.threads.values()):
            thread.quit()
            thread.wait(5000)
        QApplication.instance().quit()
