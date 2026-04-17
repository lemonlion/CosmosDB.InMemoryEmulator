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

# Remove stopped container with same name (include -v to remove anonymous volumes)
docker rm -f -v $ContainerName 2>$null | Out-Null

Write-Host "Starting emulator ($Image)..." -ForegroundColor Cyan
docker run --detach --name $ContainerName `
    --publish "${Port}:8081" `
    --publish "20250-20256:10250-10256" `
    --env AZURE_COSMOS_EMULATOR_PARTITION_COUNT=10 `
    --env AZURE_COSMOS_EMULATOR_ENABLE_DATA_PERSISTENCE=false `
    --env AZURE_COSMOS_EMULATOR_IP_ADDRESS_OVERRIDE=127.0.0.1 `
    $Image | Out-Null

# Wait for HTTP endpoint readiness — detect protocol from image name
$protocol = if ($Image -like '*vnext*') { 'http' } else { 'https' }
$elapsed = 0
while ($elapsed -lt $TimeoutSeconds) {
    try {
        $response = Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/" `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 3 -ErrorAction Stop
        if ($response.StatusCode -in 200, 401) {
            Write-Host "HTTP endpoint ready after ${elapsed}s (HTTP $($response.StatusCode))" -ForegroundColor Green
            break
        }
    } catch {
        # Expected while emulator is starting
    }
    Start-Sleep 5
    $elapsed += 5
    Write-Host "  Waiting for emulator... (${elapsed}s / ${TimeoutSeconds}s)"
}

if ($elapsed -ge $TimeoutSeconds) {
    Write-Error "Emulator HTTP endpoint did not become ready within ${TimeoutSeconds}s"
    exit 1
}

# Wait for data-plane readiness by probing database AND container creation.
# Database creation succeeds quickly, but container creation requires partition
# services and is the actual bottleneck (returns 503 until ready).
$key = 'C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=='

function Get-CosmosAuthHeader([string]$verb, [string]$resourceType, [string]$resourceLink, [string]$date) {
    $hmac = [System.Security.Cryptography.HMACSHA256]::new([Convert]::FromBase64String($key))
    $payload = "${verb}`n${resourceType}`n${resourceLink}`n${date}`n`n".ToLowerInvariant()
    $sig = [Convert]::ToBase64String($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes($payload)))
    return [Uri]::EscapeDataString("type=master&ver=1.0&sig=$sig")
}

Write-Host "Waiting for data-plane readiness..." -ForegroundColor Cyan

# Phase 1: Create the warmup database
$dbReady = $false
while ($elapsed -lt $TimeoutSeconds -and -not $dbReady) {
    try {
        $date = [DateTime]::UtcNow.ToString('r')
        $auth = Get-CosmosAuthHeader 'post' 'dbs' '' $date
        $headers = @{ 'Authorization' = $auth; 'x-ms-date' = $date; 'x-ms-version' = '2018-12-31' }
        $r = Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/dbs" `
            -Method POST -Headers $headers -Body '{"id":"_warmup"}' -ContentType 'application/json' `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 5 -ErrorAction Stop
        if ($r.StatusCode -in 200, 201, 409) {
            Write-Host "  Database ready after ${elapsed}s (HTTP $($r.StatusCode))" -ForegroundColor DarkGreen
            $dbReady = $true
        } else {
            Write-Host "  Database not ready (HTTP $($r.StatusCode)), retrying..." -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  Database probe failed ($($_.Exception.Message)), retrying..." -ForegroundColor DarkGray
    }
    if (-not $dbReady) { Start-Sleep 5; $elapsed += 5 }
}

if (-not $dbReady) {
    Write-Error "Emulator database creation did not succeed within ${TimeoutSeconds}s"
    exit 1
}

# Phase 1b: Delete all stale databases (data can persist across container restarts).
# Stale databases consume partition slots and cause 503 on new container creation.
try {
    $date = [DateTime]::UtcNow.ToString('r')
    $auth = Get-CosmosAuthHeader 'get' 'dbs' '' $date
    $headers = @{ 'Authorization' = $auth; 'x-ms-date' = $date; 'x-ms-version' = '2018-12-31' }
    $r = Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/dbs" `
        -Method GET -Headers $headers -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 10 -ErrorAction Stop
    if ($r.StatusCode -eq 200) {
        $dbs = ($r.Content | ConvertFrom-Json).Databases
        $stale = $dbs | Where-Object { $_.id -ne '_warmup' }
        if ($stale.Count -gt 0) {
            Write-Host "  Cleaning up $($stale.Count) stale database(s)..." -ForegroundColor Yellow
            foreach ($db in $stale) {
                try {
                    $date = [DateTime]::UtcNow.ToString('r')
                    $resLink = "dbs/$($db.id)"
                    $auth = Get-CosmosAuthHeader 'delete' 'dbs' $resLink $date
                    $headers = @{ 'Authorization' = $auth; 'x-ms-date' = $date; 'x-ms-version' = '2018-12-31' }
                    Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/${resLink}" `
                        -Method DELETE -Headers $headers -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 10 | Out-Null
                    Write-Host "    Deleted database '$($db.id)'" -ForegroundColor DarkGray
                } catch {
                    Write-Host "    Failed to delete '$($db.id)': $($_.Exception.Message)" -ForegroundColor DarkGray
                }
            }
        }
    }
} catch {
    Write-Host "  Database cleanup skipped ($($_.Exception.Message))" -ForegroundColor DarkGray
}

# Phase 2: Create a container — this is the real test (needs partition services)
$containerReady = $false
while ($elapsed -lt $TimeoutSeconds -and -not $containerReady) {
    try {
        $date = [DateTime]::UtcNow.ToString('r')
        $auth = Get-CosmosAuthHeader 'post' 'colls' 'dbs/_warmup' $date
        $headers = @{ 'Authorization' = $auth; 'x-ms-date' = $date; 'x-ms-version' = '2018-12-31' }
        $body = '{"id":"_warmup_coll","partitionKey":{"paths":["/id"],"kind":"Hash","version":2}}'
        $r = Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/dbs/_warmup/colls" `
            -Method POST -Headers $headers -Body $body -ContentType 'application/json' `
            -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 10 -ErrorAction Stop
        if ($r.StatusCode -in 200, 201, 409) {
            Write-Host "Data-plane ready after ${elapsed}s (container HTTP $($r.StatusCode))" -ForegroundColor Green
            $containerReady = $true
        } else {
            Write-Host "  Container not ready (HTTP $($r.StatusCode)), retrying..." -ForegroundColor DarkGray
        }
    } catch {
        Write-Host "  Container probe failed ($($_.Exception.Message)), retrying..." -ForegroundColor DarkGray
    }
    if (-not $containerReady) { Start-Sleep 5; $elapsed += 5 }
}

if (-not $containerReady) {
    Write-Error "Emulator container creation did not succeed within ${TimeoutSeconds}s"
    exit 1
}

# Cleanup warmup resources (best-effort)
try {
    $date = [DateTime]::UtcNow.ToString('r')
    $auth = Get-CosmosAuthHeader 'delete' 'dbs' 'dbs/_warmup' $date
    $headers = @{ 'Authorization' = $auth; 'x-ms-date' = $date; 'x-ms-version' = '2018-12-31' }
    Invoke-WebRequest -Uri "${protocol}://localhost:${Port}/dbs/_warmup" `
        -Method DELETE -Headers $headers -SkipCertificateCheck -SkipHttpErrorCheck -TimeoutSec 5 | Out-Null
} catch { }

return
