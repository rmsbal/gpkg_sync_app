from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional


GOOGLE_CLIENT_JSON_ENV = "GPKG_SYNC_GOOGLE_CLIENT_JSON"
GOOGLE_CLIENT_ID_ENV = "GPKG_SYNC_GOOGLE_CLIENT_ID"
GOOGLE_CLIENT_SECRET_ENV = "GPKG_SYNC_GOOGLE_CLIENT_SECRET"
APP_DIR_NAME = ".gpkg_sync"
APP_CLIENT_JSON = "google_oauth_client.json"
PROJECT_ENV_FILE = ".env"


def _load_dotenv() -> None:
    env_path = Path(__file__).resolve().parent.parent / PROJECT_ENV_FILE
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv()


def _candidate_google_client_paths() -> list[Path]:
    paths: list[Path] = []
    env_path = os.environ.get(GOOGLE_CLIENT_JSON_ENV, "").strip()
    if env_path:
        paths.append(Path(env_path).expanduser())
    paths.append(Path.home() / APP_DIR_NAME / APP_CLIENT_JSON)
    paths.append(Path(__file__).resolve().parent / APP_CLIENT_JSON)
    paths.append(Path(__file__).resolve().parent.parent / APP_CLIENT_JSON)
    return paths


def load_google_client_config() -> Optional[Dict[str, Any]]:
    for path in _candidate_google_client_paths():
        if not path.exists():
            continue
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and payload.get("installed"):
            return payload
    client_id = os.environ.get(GOOGLE_CLIENT_ID_ENV, "").strip()
    client_secret = os.environ.get(GOOGLE_CLIENT_SECRET_ENV, "").strip()
    if client_id and client_secret:
        return {
            "installed": {
                "client_id": client_id,
                "client_secret": client_secret,
                "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                "token_uri": "https://oauth2.googleapis.com/token",
                "redirect_uris": ["http://localhost"],
            }
        }
    return None


def has_google_oauth_config() -> bool:
    return load_google_client_config() is not None


def google_oauth_setup_hint() -> str:
    return (
        "Google Drive sign-in is not configured for this app yet. "
        "Add a desktop OAuth client in ~/.gpkg_sync/google_oauth_client.json, "
        "or set GPKG_SYNC_GOOGLE_CLIENT_ID and GPKG_SYNC_GOOGLE_CLIENT_SECRET in .env."
    )
