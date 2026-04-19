#define MyAppName "gpkg sync"
#ifndef MyAppVersion
  #define MyAppVersion "0.1.0"
#endif
#ifndef MyAppSourceDir
  #error MyAppSourceDir must be passed to ISCC.
#endif
#ifndef MyOutputDir
  #define MyOutputDir "."
#endif

[Setup]
AppId={{A9D5AA5A-6D5A-4A42-9857-6B05F0D5449C}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher=Appzter
DefaultDirName={autopf}\gpkg sync
DefaultGroupName=gpkg sync
DisableProgramGroupPage=yes
OutputDir={#MyOutputDir}
OutputBaseFilename=gpkg_sync_setup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
ArchitecturesInstallIn64BitMode=x64
UninstallDisplayIcon={app}\gpkgSyncApp.exe
SetupIconFile=..\gpkgSyncApp.png

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; GroupDescription: "Additional icons:"; Flags: unchecked

[Files]
Source: "{#MyAppSourceDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\gpkg sync"; Filename: "{app}\gpkgSyncApp.exe"
Name: "{autodesktop}\gpkg sync"; Filename: "{app}\gpkgSyncApp.exe"; Tasks: desktopicon

[Run]
Filename: "{app}\gpkgSyncApp.exe"; Description: "Launch gpkg sync"; Flags: nowait postinstall skipifsilent
