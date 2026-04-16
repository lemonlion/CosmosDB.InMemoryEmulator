<#
.SYNOPSIS
    Runs the test suite against a specified target backend.
.PARAMETER Target
    Backend to test against: inmemory or emulator-linux.
.PARAMETER Framework
    Target framework. Default net8.0.
.PARAMETER Filter
    Additional dotnet test filter expression.
.PARAMETER OutputDir
    Directory for TRX output files. Default ./test-results.
.EXAMPLE
    .\scripts\run-tests.ps1 -Target inmemory
    .\scripts\run-tests.ps1 -Target emulator-linux
    .\scripts\run-tests.ps1 -Target emulator-linux -Filter "FullyQualifiedName~Crud"
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('inmemory', 'emulator-linux')]
    [string]$Target,

    [string]$Framework = 'net8.0',
    [string]$Filter,
    [string]$OutputDir = './test-results'
)

$ErrorActionPreference = 'Stop'
$env:COSMOS_TEST_TARGET = $Target

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
