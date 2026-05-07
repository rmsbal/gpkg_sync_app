# -*- mode: python ; coding: utf-8 -*-
import sys
from pathlib import Path

from PyInstaller.utils.hooks import collect_all, collect_submodules, copy_metadata


repo_root = Path(SPECPATH).resolve()
for site_packages in [*repo_root.glob('.venv/lib/python*/site-packages'), repo_root / '.venv' / 'Lib' / 'site-packages']:
    if site_packages.exists():
        sys.path.insert(0, str(site_packages))


def collect_python_sqlite_binaries():
    search_roots = [Path(sys.base_prefix), Path(sys.prefix), Path(sys.executable).resolve().parent]
    for root in list(search_roots):
        search_roots.extend(root.parents[:3])

    candidates = []
    for root in dict.fromkeys(search_roots):
        candidates.extend([
            root / 'DLLs' / 'sqlite3.dll',
            root / 'bin' / 'sqlite3.dll',
            root / 'sqlite3.dll',
        ])

    for candidate in candidates:
        if candidate.exists():
            return [(str(candidate), '.')]
    return []


datas = [('gpkgSyncApp.png', '.')]
google_client_json = Path('gpkg_sync') / 'google_oauth_client.json'
if google_client_json.exists():
    datas.append((str(google_client_json), '.'))
    datas.append((str(google_client_json), 'gpkg_sync'))
binaries = collect_python_sqlite_binaries()
hiddenimports = ['_sqlite3', 'paramiko', 'watchdog', 'keyring', 'msal', 'requests']
tmp_ret = collect_all('PySide6')
datas += tmp_ret[0]
binaries += tmp_ret[1]
hiddenimports += tmp_ret[2]
for package_name in ('keyring', 'googleapiclient', 'google_auth_oauthlib', 'google.auth', 'msal'):
    tmp_ret = collect_all(package_name)
    datas += tmp_ret[0]
    binaries += tmp_ret[1]
    hiddenimports += tmp_ret[2]
for distribution_name in ('google-api-python-client', 'google-auth', 'google-auth-oauthlib', 'google-auth-httplib2'):
    try:
        datas += copy_metadata(distribution_name)
    except Exception:
        pass
for package_name in ('google.auth', 'google.oauth2', 'googleapiclient', 'google_auth_oauthlib', 'google_auth_httplib2'):
    hiddenimports += collect_submodules(package_name)
hiddenimports += [
    'google',
    'googleapiclient.discovery',
    'googleapiclient.http',
    'google_auth_oauthlib.flow',
    'google_auth_httplib2',
]


a = Analysis(
    ['app.py'],
    pathex=[],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
drive_discovery_doc = 'googleapiclient/discovery_cache/documents/drive.v3.json'
a.datas = [
    item for item in a.datas
    if not (
        item[0].startswith('googleapiclient/discovery_cache/documents/')
        and item[0] != drive_discovery_doc
    )
]
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='gpkgSyncApp',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['gpkgSyncApp.png'],
)
