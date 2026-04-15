binary - linux

pyinstaller --noconfirm --onefile --windowed  --name gpkgSyncApp  --add-data "gpkgSyncApp.png:." app.py

mkdir -p gpkgSyncApp.AppDir/usr/bin
mkdir -p gpkgSyncApp.AppDir/usr/share/applications
mkdir -p gpkgSyncApp.AppDir/usr/share/icons/hicolor/256x256/apps

cp dist/gpkgSyncApp gpkgSyncApp.AppDir/usr/bin/
cp gpkgSyncApp.png gpkgSyncApp.AppDir/usr/share/icons/hicolor/256x256/apps/gpkgSyncApp.png
cp gpkgSyncApp.png gpkgSyncApp.AppDir/.DirIcon

====================================================================
nano gpkgSyncApp.AppDir/gpkgSyncApp.desktop

[Desktop Entry]
Type=Application
Name=gpkgSyncApp-1.0
Exec=gpkgSyncApp
Icon=gpkgSyncApp
Categories=Utility;
Comment=Auto sync gpkg for qgis
Terminal=false
======================================================================

#make sure you download appimagetool-x86_64.AppImage

~/Applications/tools/appimagetool-x86_64.AppImage gpkgSyncApp.AppDir

----------------------------------------------------------------------
windows

pyinstaller --noconfirm --windowed --name "gpkg sync" --icon "gpkgSyncApp.png" --collect-all PySide6 --hidden-import paramiko --hidden-import watchdog app.py
