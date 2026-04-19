param(
    [string]$AppName = "gpkgSyncApp",
    [string]$Version = "0.1.0",
    [string]$SpecPath = "gpkgSyncApp.windows.spec",
    [string]$InstallerScript = "windows-installer\gpkgSyncApp.iss",
    [string]$OutputDir = "dist\windows",
    [string]$IsccPath = ""
)

$ErrorActionPreference = "Stop"

function Require-Command {
    param([string]$Name)
    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "$Name is required but was not found in PATH."
    }
}

function Resolve-Iscc {
    param([string]$RequestedPath)

    if ($RequestedPath) {
        if (-not (Test-Path $RequestedPath)) {
            throw "Inno Setup compiler not found at $RequestedPath"
        }
        return (Resolve-Path $RequestedPath).Path
    }

    $candidatePaths = @(
        "C:\Program Files (x86)\Inno Setup 6\ISCC.exe",
        "C:\Program Files\Inno Setup 6\ISCC.exe"
    )

    foreach ($candidate in $candidatePaths) {
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    $command = Get-Command ISCC.exe -ErrorAction SilentlyContinue
    if ($command) {
        return $command.Path
    }

    throw "ISCC.exe was not found. Install Inno Setup 6 or pass -IsccPath."
}

Require-Command pyinstaller
$iscc = Resolve-Iscc -RequestedPath $IsccPath

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$outputRoot = Join-Path $repoRoot $OutputDir
$pyinstallerDist = Join-Path $repoRoot "dist"
$pyinstallerBuild = Join-Path $repoRoot "build"
$appDistDir = Join-Path $pyinstallerDist $AppName

if (Test-Path $pyinstallerBuild) {
    Remove-Item -Recurse -Force $pyinstallerBuild
}
if (Test-Path $appDistDir) {
    Remove-Item -Recurse -Force $appDistDir
}
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

Push-Location $repoRoot
try {
    pyinstaller --noconfirm $SpecPath

    if (-not (Test-Path $appDistDir)) {
        throw "PyInstaller output directory not found at $appDistDir"
    }

    & $iscc `
        "/DMyAppVersion=$Version" `
        "/DMyAppSourceDir=$appDistDir" `
        "/DMyOutputDir=$outputRoot" `
        (Join-Path $repoRoot $InstallerScript)
}
finally {
    Pop-Location
}
