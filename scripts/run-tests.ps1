<#
.SYNOPSIS
    Runs the test suite against a specified target backend.
.PARAMETER Target
    Backend to test against: inmemory, emulator-linux, or emulator-windows.
.PARAMETER Project
    Which test project(s) to run: unit, integration, or both. Default: both.
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
    .\scripts\run-tests.ps1 -Target emulator-linux -Project integration
    .\scripts\run-tests.ps1 -Target emulator-linux -EmulatorEndpoint http://localhost:8081
    .\scripts\run-tests.ps1 -Target inmemory -Project unit -Filter "FullyQualifiedName~Crud"
#>
param(
    [Parameter(Mandatory)]
    [ValidateSet('inmemory', 'emulator-linux', 'emulator-windows')]
    [string]$Target,

    [ValidateSet('unit', 'integration', 'both')]
    [string]$Project = 'both',

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

# Determine which projects to run
$projects = switch ($Project) {
    'unit'        { @(@{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Unit';        Label = 'unit' }) }
    'integration' { @(@{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'; Label = 'integration' }) }
    'both'        { @(
        @{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Unit';        Label = 'unit' }
        @{ Path = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'; Label = 'integration' }
    )}
}

$overallExit = 0
foreach ($proj in $projects) {
    $trxFile = "$Target-$($proj.Label)-results.trx"

    Write-Host "`nRunning $($proj.Label) tests against '$Target' (framework: $Framework)..." -ForegroundColor Cyan
    if ($filterExpr) { Write-Host "  Filter: $filterExpr" -ForegroundColor DarkGray }

    $testArgs = @(
        'test', $proj.Path,
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

    # Disable xUnit test-collection parallelism for emulator targets.
    # The emulator (especially Linux) can crash when many test classes
    # simultaneously create containers during startup. Running collections
    # sequentially avoids this burst while tests within each class still
    # run normally. In-memory targets keep full parallelism.
    if ($Target -ne 'inmemory') {
        $testArgs += '--'
        $testArgs += 'xunit.parallelizeTestCollections=false'
    }

    & dotnet @testArgs
    if ($LASTEXITCODE -ne 0) { $overallExit = $LASTEXITCODE }

    Write-Host "Results: $OutputDir/$trxFile" -ForegroundColor Cyan
}

exit $overallExit
