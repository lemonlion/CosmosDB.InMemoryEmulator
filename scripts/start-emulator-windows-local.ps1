<#
.SYNOPSIS
    Starts the locally installed Azure Cosmos DB Emulator (Windows) and waits for readiness.
.DESCRIPTION
    Uses the installed CosmosDB.Emulator.exe at its default location.
    If the emulator is already running and responsive, this is a no-op.
    Requires elevation — a UAC prompt will appear if the script is not already running as admin.
.PARAMETER TimeoutSeconds
    Max seconds to wait for readiness. Default 300.
.EXAMPLE
    .\scripts\start-emulator-windows-local.ps1
#>
param(
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'

$emulatorExe = "C:\Program Files\Azure Cosmos DB Emulator\Microsoft.Azure.Cosmos.Emulator.exe"
if (-not (Test-Path $emulatorExe)) {
    # Fallback to legacy name
    $emulatorExe = "C:\Program Files\Azure Cosmos DB Emulator\CosmosDB.Emulator.exe"
}
if (-not (Test-Path $emulatorExe)) {
    Write-Error "Azure Cosmos DB Emulator not found. Install it from https://aka.ms/cosmosdb-emulator"
    exit 1
}

function Test-EmulatorReady {
    try {
        $r = Invoke-WebRequest -Uri "https://localhost:8081/" `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 3 -ErrorAction Stop
        return $r.StatusCode -in 200, 401
    } catch {
        return $false
    }
}

# If already running, nothing to do
if (Test-EmulatorReady) {
    Write-Host "Emulator already running at https://localhost:8081" -ForegroundColor Yellow
    return
}

Write-Host "Starting local Windows Cosmos DB Emulator..." -ForegroundColor Cyan
Write-Host "  (A UAC elevation prompt may appear)" -ForegroundColor DarkGray

Start-Process -FilePath $emulatorExe `
    -ArgumentList "/NoExplorer", "/PartitionCount=3" `
    -Verb RunAs

# Wait for readiness
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    if (Test-EmulatorReady) {
        Write-Host "Emulator ready after ${elapsed}s" -ForegroundColor Green
        return
    }
    Start-Sleep 5
    $elapsed += 5
    Write-Host "  Waiting for emulator... (${elapsed}s / ${TimeoutSeconds}s)"
}

Write-Error "Emulator did not become ready within ${TimeoutSeconds}s"
exit 1
