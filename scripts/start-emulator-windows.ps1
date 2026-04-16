<#
.SYNOPSIS
    Starts the Cosmos DB Windows Docker emulator and waits for readiness.
.DESCRIPTION
    Uses mcr.microsoft.com/cosmosdb/windows/azure-cosmos-emulator which runs in
    Windows containers mode. Docker Desktop must be switched to Windows containers
    before running this script.
.PARAMETER ContainerName
    Docker container name. Default 'cosmosdb-emulator'.
.PARAMETER Port
    Host port to map to emulator port 8081. Default 8081.
.PARAMETER TimeoutSeconds
    Max seconds to wait for readiness. Default 480 (Windows emulator is slower to start).
.EXAMPLE
    .\scripts\start-emulator-windows.ps1
#>
param(
    [string]$ContainerName = 'cosmosdb-emulator',
    [int]$Port = 8081,
    [int]$TimeoutSeconds = 480
)

$ErrorActionPreference = 'Stop'

# Verify Docker is in Windows containers mode
$dockerInfo = docker info --format '{{.OSType}}' 2>$null
if ($dockerInfo -ne 'windows') {
    Write-Error @"
Docker is not in Windows containers mode (current OS type: '$dockerInfo').
Switch Docker Desktop to Windows containers first:
  Right-click the Docker tray icon → 'Switch to Windows containers...'
Or run: & "$env:ProgramFiles\Docker\Docker\DockerCli.exe" -SwitchDaemon
"@
    exit 1
}

# Check if already running
$existing = docker ps --filter "name=$ContainerName" --format '{{.Names}}' 2>$null
if ($existing -eq $ContainerName) {
    Write-Host "Emulator container '$ContainerName' is already running." -ForegroundColor Yellow
    return
}

# Remove stopped container with same name
docker rm -f $ContainerName 2>$null | Out-Null

$image = 'mcr.microsoft.com/cosmosdb/windows/azure-cosmos-emulator'
Write-Host "Starting Windows emulator ($image)..." -ForegroundColor Cyan
Write-Host "  Note: Windows emulator typically takes 2-5 minutes to start." -ForegroundColor DarkGray

docker run --detach --name $ContainerName `
    --publish "${Port}:8081" `
    --publish "10251:10251" `
    --publish "10252:10252" `
    --publish "10253:10253" `
    --publish "10254:10254" `
    --memory 3g `
    --env AZURE_COSMOS_EMULATOR_PARTITION_COUNT=3 `
    --env AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false `
    $image | Out-Null

# Wait for readiness — Windows emulator signals ready via HTTP 200 on /
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    try {
        $response = Invoke-WebRequest -Uri "https://localhost:${Port}/" `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 5 -ErrorAction Stop
        if ($response.StatusCode -in 200, 401) {
            Write-Host "Emulator ready after ${elapsed}s (HTTP $($response.StatusCode))" -ForegroundColor Green
            Start-Sleep 5
            return
        }
    } catch {
        # Expected while emulator is starting
    }
    Start-Sleep 10
    $elapsed += 10
    Write-Host "  Waiting for emulator... (${elapsed}s / ${TimeoutSeconds}s)"
}

Write-Error "Emulator did not become ready within ${TimeoutSeconds}s"
exit 1
