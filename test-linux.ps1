<#
.SYNOPSIS
    Runs the test suite inside a Linux Docker container to catch platform-specific
    failures (e.g. Cosmos SDK gateway query plan path) before pushing to CI.

.DESCRIPTION
    The Cosmos SDK uses a native ServiceInterop DLL on Windows for local query plan
    computation. On Linux (including CI), it falls back to a gateway HTTP endpoint,
    which exercises different code paths in FakeCosmosHandler. This script reproduces
    that Linux behavior locally via Docker.

    NuGet packages are restored offline from the host's package cache to avoid
    network dependencies.

.PARAMETER Filter
    Optional xUnit test filter expression (e.g. "FullyQualifiedName~OrderBy").

.EXAMPLE
    .\test-linux.ps1
    # Runs all tests

.EXAMPLE
    .\test-linux.ps1 -Filter "FullyQualifiedName~RealToFeedIteratorTests"
    # Runs only matching tests
#>
param(
    [string]$Filter
)

$ErrorActionPreference = 'Stop'

# Verify Docker is available
$dockerVersion = docker version --format '{{.Server.Version}}' 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Error "Docker is not running. Start Rancher Desktop (or Docker Desktop) and try again."
    return
}
Write-Host "Docker $dockerVersion detected" -ForegroundColor Cyan

$repoRoot = $PSScriptRoot
$nugetCache = Join-Path $env:USERPROFILE '.nuget\packages'

if (-not (Test-Path $nugetCache)) {
    Write-Error "NuGet package cache not found at $nugetCache"
    return
}

$filterArg = if ($Filter) { "--filter '$Filter'" } else { "" }

$testCmd = @(
    "dotnet nuget add source /root/.nuget/packages --name local-cache 2>/dev/null;"
    "find /src -name 'project.assets.json' -delete 2>/dev/null;"
    "dotnet restore tests/CosmosDB.InMemoryEmulator.Tests -p:TargetFrameworks=net8.0 --source /root/.nuget/packages -p:TreatWarningsAsErrors=false 2>/dev/null;"
    "dotnet test tests/CosmosDB.InMemoryEmulator.Tests --nologo -c Release -f net8.0 --no-restore -p:TreatWarningsAsErrors=false $filterArg"
) -join ' '

Write-Host "Running tests on Linux (mcr.microsoft.com/dotnet/sdk:8.0)..." -ForegroundColor Cyan

docker run --rm `
    -v "${repoRoot}:/src" `
    -v "${nugetCache}:/root/.nuget/packages" `
    -w /src `
    mcr.microsoft.com/dotnet/sdk:8.0 `
    bash -c $testCmd

if ($LASTEXITCODE -ne 0) {
    Write-Host "`nLinux tests failed." -ForegroundColor Red
} else {
    Write-Host "`nLinux tests passed." -ForegroundColor Green
}

exit $LASTEXITCODE
