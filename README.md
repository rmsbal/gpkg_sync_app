# gpkg sync

Desktop app for syncing files between one or more local watch folders and a remote SFTP, FTP, FTPS, Google Drive, or OneDrive location.

Current app version: `1.2`

## Install on Linux

### Recommended package

Use the AppImage:

`gpkg_sync-1.2-x86_64.AppImage`

### Run from source

Create or activate a virtual environment, then install the runtime dependencies:

```bash
python3 -m pip install -r requirements.txt
```

### Install steps

1. Download or copy `gpkg_sync-1.2-x86_64.AppImage` to your Linux machine.
2. Make it executable:

   ```bash
   chmod +x gpkg_sync-1.2-x86_64.AppImage
   ```

3. Run it:

   ```bash
   ./gpkg_sync-1.2-x86_64.AppImage
   ```

### Linux runtime requirements

- A desktop session is required. The app will not start from a headless shell with no GUI display.
- On Debian/Ubuntu-based systems, install the Qt runtime dependency if needed:

  ```bash
  sudo apt install libxcb-cursor0
  ```

- If your Linux system cannot use FUSE for AppImage mounting, extract-and-run support may be needed depending on the distro setup.

### Linux app data

The app stores local state under:

`~/.gpkg_sync`

That folder contains:

- `profiles.json` for non-secret profile settings
- `gpkg_sync.db` for sync state and logs

Passwords are stored in the OS keychain, not in `profiles.json`.

OAuth token caches for Google Drive and OneDrive are stored in `~/.gpkg_sync` after the first successful sign-in.

### Cloud drive setup

- `google-drive`: choose Google Drive in the app, then sign in through your browser the first time the profile connects.
- The app-level Google OAuth client can be provided in `~/.gpkg_sync/google_oauth_client.json` or in a project `.env` file with `GPKG_SYNC_GOOGLE_CLIENT_ID` and `GPKG_SYNC_GOOGLE_CLIENT_SECRET`.
- `onedrive`: provide an Azure app `Client ID` and `Tenant ID`, then sign in with Microsoft when prompted.
- For cloud profiles, use a path-like remote folder such as `/Apps/gpkg-sync`.

### Sync behavior

- A profile can watch multiple local folders.
- When a profile has one watch folder, files sync directly into the configured remote folder.
- When a profile has multiple watch folders, each local folder is namespaced on the remote side by its folder name to avoid collisions.
- App-managed temporary and conflict artifacts such as `.part`, `.backup-*`, and `.conflict-*` files are excluded from sync to prevent feedback loops.
- Enable **Run app when I sign in** in the app to launch gpkg sync automatically after desktop login. Profile-level **Start syncing when app opens** still controls which profiles begin syncing after launch.

## Install on Windows

### Recommended package

Use the Windows installer:

`gpkg_sync_setup.exe`

### Install steps

1. Run `gpkg_sync_setup.exe`.
2. Accept the installer prompts.
3. Choose the install folder if you do not want the default location.
4. Optionally enable the desktop shortcut during setup.
5. Launch **gpkg sync** from the Start menu or desktop shortcut.

### Windows app data

The app stores profile settings and state in the user profile directory managed by the app at runtime.

Passwords are stored in the Windows credential/keychain backend through `keyring`, not in the JSON config file.

## Build packages

### Build Linux AppImage

Run:

```bash
./cmd
```

Output:

- `gpkg_sync-1.2-x86_64.AppImage`

### Build Windows installer

Run on a Windows machine with:

- Python
- `pyinstaller`
- Inno Setup 6

Command:

```powershell
.\build_windows.ps1
```

If Inno Setup is installed in a non-standard location:

```powershell
.\build_windows.ps1 -IsccPath "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
```

Output:

- `dist\windows\gpkg_sync_setup.exe`

Important:

- Running the normal Linux build does not create a Windows `.exe` installer.
- If you build this project on Linux, you will only see Linux artifacts such as `gpkg_sync-1.2-x86_64.AppImage` or `dist/gpkgSyncApp`.
- The Windows installer is only generated after running `build_windows.ps1` on a Windows machine with PyInstaller and Inno Setup installed.

## Notes

- Linux is currently the primary packaged target.
- The Windows installer assets are included in this repo, but the installer itself must be built on Windows.
