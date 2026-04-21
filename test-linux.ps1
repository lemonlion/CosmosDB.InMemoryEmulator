<#
.SYNOPSIS
    Runs the integration test suite inside a Linux Docker container to catch
    platform-specific failures (e.g. Cosmos SDK gateway query plan path) before
    pushing to CI.

.DESCRIPTION
    The Cosmos SDK uses a native ServiceInterop DLL on Windows for local query plan
    computation. On Linux (including CI), it falls back to a gateway HTTP endpoint,
    which exercises different code paths in FakeCosmosHandler. This script reproduces
    that Linux behavior locally via Docker.

    By default this script runs only the integration test classes that the
    Emulator Parity workflow validates (Crud, Ttl, Batch, QueryAdvanced) — the
    other integration test classes (PartitionKey, Linq, CrudHardening, Bulk)
    overload the Linux Cosmos emulator under per-test container churn and are
    excluded from CI parity. Pass -Filter to override.

    NuGet packages are restored offline from the host's package cache to avoid
    network dependencies.

.PARAMETER Filter
    xUnit test filter expression. Defaults to the parity-clean integration
    classes; pass an explicit value to run a different set
    (e.g. "FullyQualifiedName~OrderBy").

.EXAMPLE
    .\test-linux.ps1
    # Runs the parity-clean integration test classes

.EXAMPLE
    .\test-linux.ps1 -Filter "FullyQualifiedName~FakeCosmosHandlerLinqTests"
    # Runs only the Linq integration tests
#>
param(
    [string]$Filter = "FullyQualifiedName~FakeCosmosHandlerCrudTests|FullyQualifiedName~FakeCosmosHandlerTtlTests|FullyQualifiedName~FakeCosmosHandlerBatchTests|FullyQualifiedName~FakeCosmosHandlerQueryAdvancedTests"
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

$testProject = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'
$filterArg = if ($Filter) { "--filter '$Filter'" } else { "" }

$testCmd = @(
    "dotnet nuget add source /root/.nuget/packages --name local-cache 2>/dev/null;"
    "find /src -name 'project.assets.json' -delete 2>/dev/null;"
    "dotnet restore $testProject -p:TargetFrameworks=net8.0 --source /root/.nuget/packages -p:TreatWarningsAsErrors=false 2>/dev/null;"
    "dotnet test $testProject --nologo -c Release -f net8.0 --no-restore -p:TreatWarningsAsErrors=false $filterArg"
) -join ' '

Write-Host "Running integration tests on Linux (mcr.microsoft.com/dotnet/sdk:8.0)..." -ForegroundColor Cyan
Write-Host "  Filter: $Filter" -ForegroundColor DarkGray

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
