<#
.SYNOPSIS
    Starts the Cosmos DB emulator in Docker and waits for readiness.
.PARAMETER Image
    Docker image to use. Defaults to legacy emulator (HTTPS).
    Use 'mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview' for the vnext HTTP variant.
.PARAMETER ContainerName
    Docker container name. Default 'cosmosdb-emulator'.
.PARAMETER Port
    Host port to map. Default 8081.
.PARAMETER TimeoutSeconds
    Max seconds to wait for readiness. Default 300.
.EXAMPLE
    .\scripts\start-emulator.ps1
    .\scripts\start-emulator.ps1 -Image mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview
#>
param(
    [string]$Image = 'mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator',
    [string]$ContainerName = 'cosmosdb-emulator',
    [int]$Port = 8081,
    [int]$TimeoutSeconds = 300
)

$ErrorActionPreference = 'Stop'

# Check if already running
$existing = docker ps --filter "name=$ContainerName" --format '{{.Names}}' 2>$null
if ($existing -eq $ContainerName) {
    Write-Host "Emulator container '$ContainerName' is already running." -ForegroundColor Yellow
    return
}

# Remove stopped container with same name
docker rm -f $ContainerName 2>$null | Out-Null

Write-Host "Starting emulator ($Image)..." -ForegroundColor Cyan
docker run --detach --name $ContainerName `
    --publish "${Port}:8081" `
    --publish "20250-20256:10250-10256" `
    --env AZURE_COSMOS_EMULATOR_PARTITION_COUNT=3 `
    --env AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false `
    --env AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1 `
    $Image | Out-Null

# Wait for readiness — detect protocol from image name
$protocol = if ($Image -like '*vnext*') { 'http' } else { 'https' }
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    try {
        $response = Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/" `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 3 -ErrorAction Stop
        if ($response.StatusCode -in 200, 401) {
            Write-Host "Emulator ready after ${elapsed}s (HTTP $($response.StatusCode))" -ForegroundColor Green
            Start-Sleep 5  # Small buffer for internal initialization
            return
        }
    } catch {
        # Expected while emulator is starting
    }
    Start-Sleep 5
    $elapsed += 5
    Write-Host "  Waiting for emulator... (${elapsed}s / ${TimeoutSeconds}s)"
}

Write-Error "Emulator did not become ready within ${TimeoutSeconds}s"
exit 1
