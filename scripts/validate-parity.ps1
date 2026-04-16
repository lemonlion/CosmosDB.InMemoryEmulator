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
.EXAMPLE
    .\scripts\validate-parity.ps1
    .\scripts\validate-parity.ps1 -Filter "FullyQualifiedName~Crud"
    .\scripts\validate-parity.ps1 -SkipBuild -SkipEmulatorStop
#>
param(
    [string]$Filter,
    [string]$Framework = 'net8.0',
    [switch]$SkipBuild,
    [switch]$SkipEmulatorStop
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
$emulatorRunning = docker ps --filter name=cosmosdb-emulator --format '{{.Names}}' 2>$null
if (-not $emulatorRunning) {
    Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Step 2: Starting emulator" -ForegroundColor Cyan
    Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
    & "$scriptsDir/start-emulator.ps1"
}

# Step 3: Run emulator tests
Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Step 3: Running emulator tests" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
$runArgs.Target = 'emulator-linux'
& "$scriptsDir/run-tests.ps1" @runArgs
$emulatorExit = $LASTEXITCODE

# Step 4: Compare results
Write-Host "`n══════════════════════════════════════" -ForegroundColor Cyan
Write-Host "  Step 4: Parity comparison" -ForegroundColor Cyan
Write-Host "══════════════════════════════════════" -ForegroundColor Cyan
& "$scriptsDir/compare-trx.ps1" -ResultsDir $resultsDir

# Step 5: Cleanup
if (-not $SkipEmulatorStop) {
    Write-Host "`nStopping emulator..." -ForegroundColor DarkGray
    docker rm -f cosmosdb-emulator 2>$null | Out-Null
}

Write-Host "`nDone." -ForegroundColor Green
