#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR"
cd "$REPO_ROOT"

APP_NAME="gpkgSyncApp"
APP_VERSION="${APP_VERSION:-1.2}"
APPDIR="${APP_NAME}.AppDir"
APPIMAGE_TOOL="${APPIMAGE_TOOL:-$HOME/Applications/tools/appimagetool-x86_64.AppImage}"
APPIMAGE_ARCH="${APPIMAGE_ARCH:-$(uname -m)}"
ARTIFACT_NAME="${ARTIFACT_NAME:-gpkg_sync-${APP_VERSION}-${APPIMAGE_ARCH}.AppImage}"
TMP_ROOT="${TMP_ROOT:-/tmp}"
TMP_APPIMAGETOOL_DIR=""
TMP_OUTPUT_DIR=""

# Build a Linux AppImage from the PySide6 desktop app.
# Override APPIMAGE_TOOL if appimagetool lives somewhere else.

cleanup() {
  if [ -n "$TMP_APPIMAGETOOL_DIR" ] && [ -d "$TMP_APPIMAGETOOL_DIR" ]; then
    rm -rf "$TMP_APPIMAGETOOL_DIR"
  fi
  if [ -n "$TMP_OUTPUT_DIR" ] && [ -d "$TMP_OUTPUT_DIR" ]; then
    rm -rf "$TMP_OUTPUT_DIR"
  fi
}
trap cleanup EXIT

if ! command -v pyinstaller >/dev/null 2>&1; then
  echo "pyinstaller is required but was not found in PATH" >&2
  exit 1
fi

if [ ! -x "$APPIMAGE_TOOL" ]; then
  echo "appimagetool is required at $APPIMAGE_TOOL" >&2
  exit 1
fi

rm -rf build dist "$APPDIR"
rm -f "$ARTIFACT_NAME"

pyinstaller --noconfirm "${APP_NAME}.spec"

mkdir -p "$APPDIR/usr/bin"
mkdir -p "$APPDIR/usr/share/applications"
mkdir -p "$APPDIR/usr/share/icons/hicolor/256x256/apps"

cp "dist/${APP_NAME}" "$APPDIR/usr/bin/${APP_NAME}"
cp gpkgSyncApp.png "$APPDIR/usr/share/icons/hicolor/256x256/apps/${APP_NAME}.png"
cp gpkgSyncApp.png "$APPDIR/.DirIcon"
cp gpkgSyncApp.png "$APPDIR/${APP_NAME}.png"

cat > "$APPDIR/AppRun" <<'EOF'
#!/bin/sh
HERE="$(dirname "$(readlink -f "$0")")"
exec "$HERE/usr/bin/gpkgSyncApp" "$@"
EOF
chmod +x "$APPDIR/AppRun"

cat > "$APPDIR/${APP_NAME}.desktop" <<'EOF'
[Desktop Entry]
Type=Application
Name=gpkg sync
Exec=gpkgSyncApp
Icon=gpkgSyncApp
Categories=Utility;
Comment=Auto sync gpkg for QGIS
Terminal=false
EOF
cp "$APPDIR/${APP_NAME}.desktop" "$APPDIR/usr/share/applications/${APP_NAME}.desktop"

TMP_OUTPUT_DIR="$(mktemp -d "${TMP_ROOT%/}/appimage-out.XXXXXX")"

if [ -e /dev/fuse ]; then
  (
    cd "$TMP_OUTPUT_DIR"
    "$APPIMAGE_TOOL" "$REPO_ROOT/$APPDIR"
  )
else
  TMP_APPIMAGETOOL_DIR="$(mktemp -d "${TMP_ROOT%/}/appimagetool-root.XXXXXX")"
  (
    cd "$TMP_APPIMAGETOOL_DIR"
    "$APPIMAGE_TOOL" --appimage-extract >/dev/null
    (
      cd "$TMP_OUTPUT_DIR"
      "$TMP_APPIMAGETOOL_DIR/squashfs-root/AppRun" "$REPO_ROOT/$APPDIR"
    )
  )
fi

APPIMAGE_OUTPUT="$(find "$TMP_OUTPUT_DIR" -maxdepth 1 -type f -name '*.AppImage' | head -n 1)"
if [ -z "$APPIMAGE_OUTPUT" ]; then
  echo "appimagetool did not produce an AppImage artifact" >&2
  exit 1
fi

mv "$APPIMAGE_OUTPUT" "$ARTIFACT_NAME"
