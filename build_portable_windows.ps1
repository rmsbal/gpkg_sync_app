param(
    [string]$AppName = "gpkgSyncApp",
    [string]$Version = "1.2",
    [string]$SpecPath = "gpkgSyncApp.windows.spec",
    [string]$OutputDir = "dist\portable",
    [string]$Python = "",
    [switch]$SkipZip,
    [switch]$IncludeAccountData
)

$ErrorActionPreference = "Stop"

function Resolve-Python {
    param([string]$RequestedPath)

    function Test-PythonCommand {
        param([string]$Path)
        try {
            & $Path --version *> $null
            return $LASTEXITCODE -eq 0
        }
        catch {
            return $false
        }
    }

    if ($RequestedPath) {
        if (-not (Test-Path $RequestedPath)) {
            throw "Python was not found at $RequestedPath"
        }
        $resolvedPath = (Resolve-Path $RequestedPath).Path
        if (-not (Test-PythonCommand -Path $resolvedPath)) {
            throw "Python at $resolvedPath could not be executed."
        }
        return $resolvedPath
    }

    $pythonCommand = Get-Command python -ErrorAction SilentlyContinue
    if ($pythonCommand -and (Test-PythonCommand -Path $pythonCommand.Source)) {
        return $pythonCommand.Source
    }

    $pyCommand = Get-Command py -ErrorAction SilentlyContinue
    if ($pyCommand -and (Test-PythonCommand -Path $pyCommand.Source)) {
        return $pyCommand.Source
    }

    throw "Python is required but no working python or py command was found in PATH."
}

$repoRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$outputRoot = Join-Path $repoRoot $OutputDir
$pyinstallerDist = Join-Path $repoRoot "dist"
$pyinstallerBuild = Join-Path $repoRoot "build"
$appDistDir = Join-Path $pyinstallerDist $AppName
$portableZip = Join-Path $outputRoot "$AppName-$Version-windows-x64-portable.zip"
$pythonExe = Resolve-Python -RequestedPath $Python

if (Test-Path $pyinstallerBuild) {
    Remove-Item -Recurse -Force $pyinstallerBuild
}
if (Test-Path $appDistDir) {
    Remove-Item -Recurse -Force $appDistDir
}
New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null

Push-Location $repoRoot
try {
    & $pythonExe -m PyInstaller --noconfirm $SpecPath

    if (-not (Test-Path $appDistDir)) {
        throw "PyInstaller output directory not found at $appDistDir"
    }

    $googleClientJson = Join-Path $repoRoot "gpkg_sync\google_oauth_client.json"
    if (Test-Path $googleClientJson) {
        Copy-Item -Force -Path $googleClientJson -Destination (Join-Path $appDistDir "google_oauth_client.json")
    }

    if ($IncludeAccountData) {
        $sourceAppDir = Join-Path $HOME ".gpkg_sync"
        $portableAppDir = Join-Path $appDistDir ".gpkg_sync"
        if (Test-Path $sourceAppDir) {
            New-Item -ItemType Directory -Force -Path $portableAppDir | Out-Null
            $accountFiles = @(
                "google-drive-default-token.json",
                "google-drive-*-token.json",
                "onedrive-default-*-token.bin",
                "onedrive-*-token.bin",
                "profiles.json"
            )
            foreach ($pattern in $accountFiles) {
                Get-ChildItem -Path $sourceAppDir -Filter $pattern -File -ErrorAction SilentlyContinue |
                    Copy-Item -Force -Destination $portableAppDir
            }
            Write-Host "Portable account data copied to $portableAppDir"
        }
        else {
            Write-Host "No account data directory found at $sourceAppDir"
        }
    }

    if (-not $SkipZip) {
        if (Test-Path $portableZip) {
            Remove-Item -Force $portableZip
        }
        Compress-Archive -Path $appDistDir -DestinationPath $portableZip -Force
        Write-Host "Portable app package created at $portableZip"
    }
    else {
        Write-Host "Portable app folder created at $appDistDir"
    }
}
finally {
    Pop-Location
}
