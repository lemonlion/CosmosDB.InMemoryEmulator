<#
.SYNOPSIS
    Compares TRX files from in-memory and emulator test runs to produce a parity report.
.PARAMETER ResultsDir
    Directory containing TRX files. Default ./test-results.
.PARAMETER OutputFormat
    Output format: 'console' (default) or 'markdown' (for GitHub Step Summary).
.PARAMETER EmulatorTarget
    Which emulator TRX to compare against: 'emulator-linux' (default) or 'emulator-windows'.
.EXAMPLE
    .\scripts\compare-trx.ps1
    .\scripts\compare-trx.ps1 -EmulatorTarget emulator-windows
    .\scripts\compare-trx.ps1 -OutputFormat markdown >> $env:GITHUB_STEP_SUMMARY
#>
param(
    [string]$ResultsDir = './test-results',
    [ValidateSet('console', 'markdown')]
    [string]$OutputFormat = 'console',
    [ValidateSet('emulator-linux', 'emulator-windows')]
    [string]$EmulatorTarget = 'emulator-linux'
)

$ErrorActionPreference = 'Stop'

function Parse-TrxFile([string]$Path) {
    [xml]$xml = Get-Content $Path -Raw
    $ns = @{ t = 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010' }
    $results = @{}
    $xml | Select-Xml '//t:UnitTestResult' -Namespace $ns | ForEach-Object {
        $node = $_.Node
        $testName = $node.testName
        $outcome = $node.outcome  # Passed, Failed, NotExecuted
        $results[$testName] = $outcome
    }
    return $results
}

# Find TRX files
$inmemoryTrx = Get-ChildItem $ResultsDir -Filter 'inmemory-results.trx' -ErrorAction SilentlyContinue
$emulatorTrx = Get-ChildItem $ResultsDir -Filter "$EmulatorTarget-results.trx" -ErrorAction SilentlyContinue

if (-not $inmemoryTrx) { Write-Error "No inmemory-results.trx found in $ResultsDir"; exit 1 }
if (-not $emulatorTrx) { Write-Error "No $EmulatorTarget-results.trx found in $ResultsDir"; exit 1 }

$inmemoryResults = Parse-TrxFile $inmemoryTrx.FullName
$emulatorResults = Parse-TrxFile $emulatorTrx.FullName

# Build comparison matrix
$allTests = ($inmemoryResults.Keys + $emulatorResults.Keys) | Sort-Object -Unique

$parity = @()
$suspect = @()
$emulatorGap = @()
$bothFail = @()
$onlyInMemory = @()
$onlyEmulator = @()

foreach ($test in $allTests) {
    $im = $inmemoryResults[$test]
    $em = $emulatorResults[$test]

    if (-not $im) {
        $onlyEmulator += [PSCustomObject]@{ Test = $test; InMemory = '-'; Emulator = $em }
    } elseif (-not $em) {
        $onlyInMemory += [PSCustomObject]@{ Test = $test; InMemory = $im; Emulator = '-' }
    } elseif ($im -eq $em) {
        $parity += [PSCustomObject]@{ Test = $test; InMemory = $im; Emulator = $em }
    } elseif ($im -eq 'Passed' -and $em -ne 'Passed') {
        $suspect += [PSCustomObject]@{ Test = $test; InMemory = $im; Emulator = $em }
    } elseif ($im -ne 'Passed' -and $em -eq 'Passed') {
        $emulatorGap += [PSCustomObject]@{ Test = $test; InMemory = $im; Emulator = $em }
    } else {
        $bothFail += [PSCustomObject]@{ Test = $test; InMemory = $im; Emulator = $em }
    }
}

# Output
$totalCompared = $allTests.Count
$parityPct = if ($totalCompared -gt 0) { [math]::Round(($parity.Count / $totalCompared) * 100, 1) } else { 0 }

if ($OutputFormat -eq 'markdown') {
    Write-Output "## Parity Report"
    Write-Output ""
    Write-Output "| Metric | Count |"
    Write-Output "|--------|-------|"
    Write-Output "| Total tests compared | $totalCompared |"
    Write-Output "| ✅ Parity (same result) | $($parity.Count) ($parityPct%) |"
    Write-Output "| 🔍 Suspect (in-memory passes, emulator fails) | $($suspect.Count) |"
    Write-Output "| ⚠️ Emulator gap (emulator passes, in-memory fails) | $($emulatorGap.Count) |"
    Write-Output "| ❌ Both fail | $($bothFail.Count) |"
    Write-Output "| 📋 Only in in-memory run | $($onlyInMemory.Count) |"
    Write-Output "| 📋 Only in emulator run | $($onlyEmulator.Count) |"

    if ($suspect.Count -gt 0) {
        Write-Output ""
        Write-Output "### 🔍 Suspect — In-Memory Passes, Emulator Fails"
        Write-Output ""
        Write-Output "| Test | InMemory | Emulator |"
        Write-Output "|------|----------|----------|"
        foreach ($s in $suspect) {
            Write-Output "| $($s.Test) | $($s.InMemory) | $($s.Emulator) |"
        }
    }

    if ($emulatorGap.Count -gt 0) {
        Write-Output ""
        Write-Output "### ⚠️ Emulator Gap — Emulator Passes, In-Memory Fails"
        Write-Output ""
        Write-Output "| Test | InMemory | Emulator |"
        Write-Output "|------|----------|----------|"
        foreach ($g in $emulatorGap) {
            Write-Output "| $($g.Test) | $($g.InMemory) | $($g.Emulator) |"
        }
    }
} else {
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  PARITY REPORT" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "  Total tests compared:   $totalCompared"
    Write-Host "  ✅ Parity:              $($parity.Count) ($parityPct%)" -ForegroundColor Green
    Write-Host "  🔍 Suspect:             $($suspect.Count)" -ForegroundColor $(if ($suspect.Count -gt 0) { 'Red' } else { 'Green' })
    Write-Host "  ⚠️  Emulator gap:       $($emulatorGap.Count)" -ForegroundColor $(if ($emulatorGap.Count -gt 0) { 'Yellow' } else { 'Green' })
    Write-Host "  ❌ Both fail:           $($bothFail.Count)" -ForegroundColor $(if ($bothFail.Count -gt 0) { 'Yellow' } else { 'Green' })
    Write-Host "  📋 Only in-memory:      $($onlyInMemory.Count)"
    Write-Host "  📋 Only emulator:       $($onlyEmulator.Count)"

    if ($suspect.Count -gt 0) {
        Write-Host ""
        Write-Host "  🔍 SUSPECT (in-memory passes, emulator fails):" -ForegroundColor Red
        foreach ($s in $suspect) {
            Write-Host "    - $($s.Test)" -ForegroundColor Red
        }
    }

    if ($emulatorGap.Count -gt 0) {
        Write-Host ""
        Write-Host "  ⚠️  EMULATOR GAP (emulator passes, in-memory fails):" -ForegroundColor Yellow
        foreach ($g in $emulatorGap) {
            Write-Host "    - $($g.Test)" -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

# Exit with non-zero if there are suspects (potential bugs in our implementation)
if ($suspect.Count -gt 0) {
    exit 1
}
