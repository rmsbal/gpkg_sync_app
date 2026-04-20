from __future__ import annotations

import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


APP_NAME = "gpkg sync"
APP_VERSION = "0.1.0"
DEFAULT_PORTS = {
    "sftp": 22,
    "ftp": 21,
    "ftps": 21,
}


def default_device_label() -> str:
    return os.environ.get("COMPUTERNAME") or os.environ.get("HOSTNAME") or "device"


@dataclass
class SyncProfile:
    name: str
    host: str
    port: int
    username: str
    password: str = ""
    key_path: str = ""
    protocol: str = "sftp"
    local_dir: str = ""
    watch_dirs: List[str] = field(default_factory=list)
    remote_dir: str = ""
    direction: str = "two-way"
    auto_start: bool = False
    backup_before_overwrite: bool = True
    delete_missing: bool = False
    device_label: str = "device"
    stability_wait_seconds: int = 5
    has_saved_password: bool = False

    def validate(self) -> Tuple[bool, str]:
        if not self.name.strip():
            return False, "Profile name is required."
        if not self.host.strip():
            return False, "Host is required."
        if not self.username.strip():
            return False, "Username is required."
        watch_dirs = self.effective_watch_dirs()
        if not watch_dirs:
            return False, "At least one local watch folder is required."
        if not self.remote_dir.strip():
            return False, "Remote directory is required."
        folder_names: List[str] = []
        for watch_dir in watch_dirs:
            path = Path(watch_dir)
            if not path.exists():
                return False, f"Local watch folder does not exist: {watch_dir}"
            if not path.is_dir():
                return False, f"Local watch folder is not a directory: {watch_dir}"
            folder_names.append(path.resolve().name)
        if len(folder_names) > 1 and len(set(folder_names)) != len(folder_names):
            return False, "Watch folders must have unique folder names."
        protocol = self.protocol.lower()
        if protocol not in {"sftp", "ftp", "ftps"}:
            return False, "Invalid protocol."
        if self.direction not in {"upload-only", "download-only", "two-way"}:
            return False, "Invalid sync direction."
        if self.stability_wait_seconds < 2:
            return False, "Stability wait must be at least 2 seconds."
        if protocol == "sftp":
            if not self.password and not self.key_path:
                return False, "Provide either password or SSH key path."
            if self.key_path and not Path(self.key_path).exists():
                return False, "SSH key path does not exist."
        elif not self.password and self.username.lower() != "anonymous":
            return False, "Password is required for FTP/FTPS."
        return True, ""

    def effective_watch_dirs(self) -> List[str]:
        if self.watch_dirs:
            return [path.strip() for path in self.watch_dirs if path.strip()]
        if self.local_dir.strip():
            return [self.local_dir.strip()]
        return []

    @classmethod
    def from_metadata(cls, raw: Dict[str, Any], password: str = "") -> "SyncProfile":
        data = dict(raw)
        data["password"] = password
        data.setdefault("device_label", default_device_label())
        data.setdefault("stability_wait_seconds", 5)
        data.setdefault("protocol", "sftp")
        if not data.get("port"):
            data["port"] = DEFAULT_PORTS.get(data["protocol"], 22)
        watch_dirs = data.get("watch_dirs") or []
        if not watch_dirs and data.get("local_dir"):
            watch_dirs = [data["local_dir"]]
        data["watch_dirs"] = watch_dirs
        if watch_dirs:
            data["local_dir"] = watch_dirs[0]
        data.setdefault("has_saved_password", bool(raw.get("has_saved_password")))
        return cls(**data)

    def to_metadata(self) -> Dict[str, Any]:
        data = asdict(self)
        data.pop("password", None)
        effective_watch_dirs = self.effective_watch_dirs()
        data["watch_dirs"] = effective_watch_dirs
        data["local_dir"] = effective_watch_dirs[0] if effective_watch_dirs else ""
        data["has_saved_password"] = bool(self.password)
        return data
