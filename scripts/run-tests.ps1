<#
.SYNOPSIS
    Runs the test suite against a specified target backend.
.PARAMETER Target
    Backend to test against: inmemory, emulator-linux, or emulator-windows.
.PARAMETER Framework
    Target framework. Default net8.0.
.PARAMETER Filter
    Additional dotnet test filter expression.
.PARAMETER OutputDir
    Directory for TRX output files. Default ./test-results.
.PARAMETER EmulatorEndpoint
    Override the emulator endpoint URL. When unset, defaults to https://localhost:8081
    for emulator targets. Use http://localhost:8081 for the vnext-preview image.
.EXAMPLE
    .\scripts\run-tests.ps1 -Target inmemory
    .\scripts\run-tests.ps1 -Target emulator-linux
    .\scripts\run-tests.ps1 -Target emulator-linux -EmulatorEndpoint http://localhost:8081
    .\scripts\run-tests.ps1 -Target emulator-linux -Filter "FullyQualifiedName~Crud"
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('inmemory', 'emulator-linux', 'emulator-windows')]
    [string]$Target,

    [string]$Framework = 'net8.0',
    [string]$Filter,
    [string]$OutputDir = './test-results',
    [string]$EmulatorEndpoint
)

$ErrorActionPreference = 'Stop'
$env:COSMOS_TEST_TARGET = $Target

# Set emulator endpoint — default is HTTPS (legacy emulator). Pass -EmulatorEndpoint to override.
if ($Target -ne 'inmemory') {
    $env:COSMOS_EMULATOR_ENDPOINT = if ($EmulatorEndpoint) { $EmulatorEndpoint } else { 'https://localhost:8081' }
} else {
    Remove-Item Env:COSMOS_EMULATOR_ENDPOINT -ErrorAction SilentlyContinue
}

# Build filter: exclude InMemoryOnly tests when targeting emulator
$filterExpr = ''
if ($Target -ne 'inmemory') {
    $filterExpr = 'Target!=InMemoryOnly'
}
if ($Filter) {
    $filterExpr = if ($filterExpr) { "$filterExpr&$Filter" } else { $Filter }
}

New-Item -ItemType Directory -Path $OutputDir -Force | Out-Null
$trxFile = "$Target-results.trx"

Write-Host "Running tests against '$Target' (framework: $Framework)..." -ForegroundColor Cyan
if ($filterExpr) { Write-Host "  Filter: $filterExpr" -ForegroundColor DarkGray }

$testArgs = @(
    'test', 'tests/CosmosDB.InMemoryEmulator.Tests',
    '--configuration', 'Release',
    '--framework', $Framework,
    '--no-build',
    '--logger', "trx;LogFileName=$trxFile",
    '--results-directory', $OutputDir
)
if ($filterExpr) {
    $testArgs += '--filter'
    $testArgs += $filterExpr
}

& dotnet @testArgs
$exitCode = $LASTEXITCODE

Write-Host "Results: $OutputDir/$trxFile" -ForegroundColor Cyan
exit $exitCode
