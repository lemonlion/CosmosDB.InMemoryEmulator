<#
.SYNOPSIS
    One-command parity validation: starts emulator, runs both test suites, compares results.
.PARAMETER Filter
    Optional dotnet test filter to narrow the test scope.
.PARAMETER Framework
    Target framework. Default net8.0.
.PARAMETER SkipBuild
    Skip the build step (if you've already built).
.PARAMETER SkipEmulatorStop
    Don't stop the emulator after the run (useful for debugging).
.PARAMETER EmulatorTarget
    Which emulator to run against: 'emulator-linux' (default) or 'emulator-windows'.
    emulator-windows requires Docker Desktop in Windows containers mode.
.EXAMPLE
    .\scripts\validate-parity.ps1
    .\scripts\validate-parity.ps1 -Filter "FullyQualifiedName~Crud"
    .\scripts\validate-parity.ps1 -EmulatorTarget emulator-windows
    .\scripts\validate-parity.ps1 -SkipBuild -SkipEmulatorStop
#>
param(
    [string]$Filter,
    [string]$Framework = 'net8.0',
    [switch]$SkipBuild,
    [switch]$SkipEmulatorStop,
    [ValidateSet('emulator-linux', 'emulator-windows')]
    [string]$EmulatorTarget = 'emulator-linux'
)

$ErrorActionPreference = 'Stop'
$scriptsDir = $PSScriptRoot

# Clean previous results
$resultsDir = './test-results'
if (Test-Path $resultsDir) {
    Remove-Item $resultsDir -Recurse -Force
}

# Step 0: Build (once, shared by both runs)
if (-not $SkipBuild) {
    Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Step 0: Building" -ForegroundColor Cyan
    Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
    dotnet build CosmosDB.InMemoryEmulator.sln --configuration Release --framework $Framework
    if ($LASTEXITCODE -ne 0) { Write-Error "Build failed"; exit 1 }
}

# Step 1: Run in-memory baseline
Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Step 1: Running in-memory tests" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
$runArgs = @{ Target = 'inmemory'; Framework = $Framework; OutputDir = $resultsDir }
if ($Filter) { $runArgs.Filter = $Filter }
& "$scriptsDir/run-tests.ps1" @runArgs
$inMemoryExit = $LASTEXITCODE

# Step 2: Start emulator (if not already running)
# For Docker-based targets: check docker ps. For Windows local: probe the HTTP endpoint.
$emulatorReady = $false
if ($EmulatorTarget -eq 'emulator-windows') {
    try {
        $probe = Invoke-WebRequest -Uri "https://localhost:8081/" `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 3 -ErrorAction Stop
        $emulatorReady = $probe.StatusCode -in 200, 401
    } catch { $emulatorReady = $false }
} else {
    $emulatorReady = [bool](docker ps --filter name=cosmosdb-emulator --format '{{.Names}}' 2>$null)
}

if (-not $emulatorReady) {
    Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Step 2: Starting emulator ($EmulatorTarget)" -ForegroundColor Cyan
    Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
    if ($EmulatorTarget -eq 'emulator-windows') {
        & "$scriptsDir/start-emulator-windows-local.ps1"
    } else {
        & "$scriptsDir/start-emulator.ps1"
    }
}

# Step 3: Run emulator tests
Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Step 3: Running emulator tests ($EmulatorTarget)" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
$runArgs.Target = $EmulatorTarget
& "$scriptsDir/run-tests.ps1" @runArgs
$emulatorExit = $LASTEXITCODE

# Step 4: Compare results
Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Step 4: Parity comparison" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
& "$scriptsDir/compare-trx.ps1" -ResultsDir $resultsDir -EmulatorTarget $EmulatorTarget

# Step 5: Cleanup
if (-not $SkipEmulatorStop) {
    Write-Host "`nStopping emulator..." -ForegroundColor DarkGray
    if ($EmulatorTarget -eq 'emulator-windows') {
        $emulatorExe = "C:\Program Files\Azure Cosmos DB Emulator\Microsoft.Azure.Cosmos.Emulator.exe"
        if (-not (Test-Path $emulatorExe)) { $emulatorExe = "C:\Program Files\Azure Cosmos DB Emulator\CosmosDB.Emulator.exe" }
        Start-Process -FilePath $emulatorExe -ArgumentList "/Shutdown" -Verb RunAs -Wait -ErrorAction SilentlyContinue
    } else {
        docker rm -f cosmosdb-emulator 2>$null | Out-Null
    }
}

Write-Host "`nDone." -ForegroundColor Green
