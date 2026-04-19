#!/usr/bin/env bash
set -euo pipefail

APP_NAME="gpkgSyncApp"
APPDIR="${APP_NAME}.AppDir"
APPIMAGE_TOOL="${APPIMAGE_TOOL:-$HOME/Applications/tools/appimagetool-x86_64.AppImage}"
ARTIFACT_NAME="${ARTIFACT_NAME:-gpkg_sync-x86_64.AppImage}"
TMP_APPIMAGETOOL_DIR="${TMP_APPIMAGETOOL_DIR:-/tmp/appimagetool-root}"
REPO_ROOT="$(pwd)"

# Build a Linux AppImage from the PySide6 desktop app.
# Override APPIMAGE_TOOL if appimagetool lives somewhere else.

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

if [ -e /dev/fuse ]; then
  "$APPIMAGE_TOOL" "$APPDIR"
else
  rm -rf "$TMP_APPIMAGETOOL_DIR"
  mkdir -p "$TMP_APPIMAGETOOL_DIR"
  (
    cd "$TMP_APPIMAGETOOL_DIR"
    "$APPIMAGE_TOOL" --appimage-extract >/dev/null
    ./squashfs-root/AppRun "$REPO_ROOT/$APPDIR"
  )
  cp "$TMP_APPIMAGETOOL_DIR/$ARTIFACT_NAME" "$ARTIFACT_NAME"
fi
