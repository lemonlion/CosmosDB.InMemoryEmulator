<#
.SYNOPSIS
    Validates that Integration test files don't use internal-only APIs.
.DESCRIPTION
    Scans tests/CosmosDB.InMemoryEmulator.Tests.Integration/*.cs for patterns
    that indicate usage of internal APIs (InMemoryContainer constructor,
    InMemoryCosmosClient constructor, FaultInjector, FaultInjection).

    FakeCosmosHandler with InMemoryContainer backing is allowed since both are public APIs.
    Direct `new InMemoryContainer(...)` passed to FakeCosmosHandler is the expected
    integration test pattern.

    This script checks for patterns that indicate a test should be in Unit, not Integration:
    - Direct use of FaultInjector / FaultInjection (internal fault injection APIs)
    - Using InternalVisibleTo-dependent APIs
.EXAMPLE
    .\scripts\check-test-classification.ps1
#>
$ErrorActionPreference = 'Stop'

$integrationDir = 'tests/CosmosDB.InMemoryEmulator.Tests.Integration'
if (-not (Test-Path $integrationDir)) {
    Write-Error "Integration test directory not found: $integrationDir"
    exit 1
}

$violations = @()
$files = Get-ChildItem $integrationDir -Filter '*.cs'

foreach ($f in $files) {
    $content = Get-Content $f.FullName -Raw
    $issues = @()

    # Internal FaultInjector class (not FakeCosmosHandler.FaultInjector property which is public)
    if ($content -match '\bnew\s+FaultInjector\b') { $issues += 'Uses new FaultInjector() (internal)' }
    if ($content -match '\bFaultInjection\b') { $issues += 'Uses FaultInjection (internal)' }

    # Direct InMemoryCosmosClient construction (unit test pattern)
    if ($content -match 'new\s+InMemoryCosmosClient\b') { $issues += 'Uses new InMemoryCosmosClient() (unit test pattern)' }

    if ($issues.Count -gt 0) {
        $violations += [PSCustomObject]@{
            File   = $f.Name
            Issues = $issues -join '; '
        }
    }
}

if ($violations.Count -gt 0) {
    Write-Host "❌ Integration test classification violations found:" -ForegroundColor Red
    foreach ($v in $violations) {
        Write-Host "  $($v.File): $($v.Issues)" -ForegroundColor Red
    }
    Write-Host ""
    Write-Host "These files use internal-only APIs and should be in Tests.Unit instead." -ForegroundColor Yellow
    exit 1
} else {
    Write-Host "✅ All integration tests pass classification checks." -ForegroundColor Green
    exit 0
}
