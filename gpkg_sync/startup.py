from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from .models import APP_NAME


RUN_KEY = r"Software\Microsoft\Windows\CurrentVersion\Run"


class StartupError(RuntimeError):
    """Raised when the app cannot update OS login startup settings."""


def current_launch_command() -> List[str]:
    appimage_path = os.environ.get("APPIMAGE")
    if appimage_path:
        return [appimage_path]
    if getattr(sys, "frozen", False):
        return [sys.executable]
    script_path = Path(sys.argv[0]).resolve()
    return [sys.executable, str(script_path)]


class StartupManager:
    def __init__(
        self,
        app_name: str = APP_NAME,
        command: Optional[List[str]] = None,
        autostart_dir: Optional[Path] = None,
    ):
        self.app_name = app_name
        self.command = command or current_launch_command()
        self.autostart_dir = autostart_dir

    def is_supported(self) -> bool:
        return sys.platform.startswith("linux") or sys.platform == "win32"

    def is_enabled(self) -> bool:
        if sys.platform.startswith("linux"):
            return self._linux_is_enabled()
        if sys.platform == "win32":
            return self._windows_is_enabled()
        return False

    def set_enabled(self, enabled: bool) -> None:
        if not self.is_supported():
            raise StartupError("Run on startup is only supported on Linux and Windows.")
        if sys.platform.startswith("linux"):
            self._linux_set_enabled(enabled)
            return
        if sys.platform == "win32":
            self._windows_set_enabled(enabled)

    def _linux_desktop_path(self) -> Path:
        autostart_dir = self.autostart_dir or Path.home() / ".config" / "autostart"
        return autostart_dir / "gpkg-sync.desktop"

    def _linux_is_enabled(self) -> bool:
        desktop_path = self._linux_desktop_path()
        if not desktop_path.exists():
            return False
        text = desktop_path.read_text(encoding="utf-8")
        return "X-GNOME-Autostart-enabled=false" not in text

    def _linux_set_enabled(self, enabled: bool) -> None:
        desktop_path = self._linux_desktop_path()
        if not enabled:
            desktop_path.unlink(missing_ok=True)
            return
        desktop_path.parent.mkdir(parents=True, exist_ok=True)
        desktop_path.write_text(self._desktop_entry(), encoding="utf-8")

    def _desktop_entry(self) -> str:
        return "\n".join(
            [
                "[Desktop Entry]",
                "Type=Application",
                f"Name={self.app_name}",
                f"Exec={self._desktop_exec()}",
                "Terminal=false",
                "X-GNOME-Autostart-enabled=true",
                "",
            ]
        )

    def _desktop_exec(self) -> str:
        return " ".join(_quote_desktop_arg(part) for part in self.command)

    def _windows_is_enabled(self) -> bool:
        try:
            import winreg

            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, RUN_KEY, 0, winreg.KEY_READ) as key:
                value, _ = winreg.QueryValueEx(key, self.app_name)
            return bool(value)
        except FileNotFoundError:
            return False
        except OSError as exc:
            raise StartupError(f"Unable to read Windows startup setting: {exc}") from exc

    def _windows_set_enabled(self, enabled: bool) -> None:
        try:
            import winreg

            with winreg.CreateKeyEx(winreg.HKEY_CURRENT_USER, RUN_KEY, 0, winreg.KEY_SET_VALUE) as key:
                if enabled:
                    winreg.SetValueEx(key, self.app_name, 0, winreg.REG_SZ, subprocess.list2cmdline(self.command))
                else:
                    try:
                        winreg.DeleteValue(key, self.app_name)
                    except FileNotFoundError:
                        pass
        except OSError as exc:
            raise StartupError(f"Unable to update Windows startup setting: {exc}") from exc


def _quote_desktop_arg(value: str) -> str:
    if not value:
        return '""'
    if all(char not in value for char in (' ', "\t", "\n", '"', "\\", "$", "`")):
        return value
    escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("`", "\\`").replace("$", "\\$")
    return f'"{escaped}"'
