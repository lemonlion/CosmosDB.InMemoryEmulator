<#
.SYNOPSIS
    Compares TRX files from in-memory and emulator test runs to produce a parity report.
.DESCRIPTION
    When EmulatorTarget is not specified, auto-discovers all emulator TRX files in ResultsDir
    and produces a separate comparison for each (e.g. emulator-linux, emulator-windows).
.PARAMETER ResultsDir
    Directory containing TRX files. Default ./test-results.
.PARAMETER OutputFormat
    Output format: 'console' (default) or 'markdown' (for GitHub Step Summary).
.PARAMETER EmulatorTarget
    Compare against a specific emulator only. If omitted, compares against all found emulator TRX files.
.EXAMPLE
    .\scripts\compare-trx.ps1
    .\scripts\compare-trx.ps1 -EmulatorTarget emulator-linux
    .\scripts\compare-trx.ps1 -OutputFormat markdown >> $env:GITHUB_STEP_SUMMARY
#>
param(
    [string]$ResultsDir = './test-results',
    [ValidateSet('console', 'markdown')]
    [string]$OutputFormat = 'console',
    [string]$EmulatorTarget
)

$ErrorActionPreference = 'Stop'

function Parse-TrxFile([string]$Path) {
    [xml]$xml = Get-Content $Path -Raw
    $ns = @{ t = 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010' }
    $results = @{}
    $xml | Select-Xml '//t:UnitTestResult' -Namespace $ns | ForEach-Object {
        $node = $_.Node
        $results[$node.testName] = $node.outcome
    }
    return $results
}

function Compare-Results([hashtable]$InMemory, [hashtable]$Emulator) {
    $allTests = ($InMemory.Keys + $Emulator.Keys) | Sort-Object -Unique
    $parity = @(); $suspect = @(); $emulatorGap = @(); $bothFail = @()
    $onlyInMemory = @(); $onlyEmulator = @()

    foreach ($test in $allTests) {
        $im = $InMemory[$test]; $em = $Emulator[$test]
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

    return @{
        Total        = $allTests.Count
        Parity       = $parity
        Suspect      = $suspect
        EmulatorGap  = $emulatorGap
        BothFail     = $bothFail
        OnlyInMemory = $onlyInMemory
        OnlyEmulator = $onlyEmulator
    }
}

function Write-Report([string]$Label, [hashtable]$Data, [string]$Format) {
    $total = $Data.Total
    $parityPct = if ($total -gt 0) { [math]::Round(($Data.Parity.Count / $total) * 100, 1) } else { 0 }

    if ($Format -eq 'markdown') {
        Write-Output "## Parity Report — $Label"
        Write-Output ""
        Write-Output "| Metric | Count |"
        Write-Output "|--------|-------|"
        Write-Output "| Total tests compared | $total |"
        Write-Output "| ✅ Parity (same result) | $($Data.Parity.Count) ($parityPct%) |"
        Write-Output "| 🔍 Suspect (in-memory passes, emulator fails) | $($Data.Suspect.Count) |"
        Write-Output "| ⚠️ Emulator gap (emulator passes, in-memory fails) | $($Data.EmulatorGap.Count) |"
        Write-Output "| ❌ Both fail | $($Data.BothFail.Count) |"
        Write-Output "| 📋 Only in in-memory run | $($Data.OnlyInMemory.Count) |"
        Write-Output "| 📋 Only in emulator run | $($Data.OnlyEmulator.Count) |"

        if ($Data.Suspect.Count -gt 0) {
            Write-Output ""
            Write-Output "### 🔍 Suspect — In-Memory Passes, Emulator Fails"
            Write-Output ""
            Write-Output "| Test | InMemory | Emulator |"
            Write-Output "|------|----------|----------|"
            foreach ($s in $Data.Suspect) {
                Write-Output "| $($s.Test) | $($s.InMemory) | $($s.Emulator) |"
            }
        }
        if ($Data.EmulatorGap.Count -gt 0) {
            Write-Output ""
            Write-Output "### ⚠️ Emulator Gap — Emulator Passes, In-Memory Fails"
            Write-Output ""
            Write-Output "| Test | InMemory | Emulator |"
            Write-Output "|------|----------|----------|"
            foreach ($g in $Data.EmulatorGap) {
                Write-Output "| $($g.Test) | $($g.InMemory) | $($g.Emulator) |"
            }
        }
        Write-Output ""
    } else {
        Write-Host ""
        Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host "  PARITY REPORT — $Label" -ForegroundColor Cyan
        Write-Host "═══════════════════════════════════════════════════════" -ForegroundColor Cyan
        Write-Host ""
        Write-Host "  Total tests compared:   $total"
        Write-Host "  ✅ Parity:              $($Data.Parity.Count) ($parityPct%)" -ForegroundColor Green
        Write-Host "  🔍 Suspect:             $($Data.Suspect.Count)" -ForegroundColor $(if ($Data.Suspect.Count -gt 0) { 'Red' } else { 'Green' })
        Write-Host "  ⚠️  Emulator gap:       $($Data.EmulatorGap.Count)" -ForegroundColor $(if ($Data.EmulatorGap.Count -gt 0) { 'Yellow' } else { 'Green' })
        Write-Host "  ❌ Both fail:           $($Data.BothFail.Count)" -ForegroundColor $(if ($Data.BothFail.Count -gt 0) { 'Yellow' } else { 'Green' })
        Write-Host "  📋 Only in-memory:      $($Data.OnlyInMemory.Count)"
        Write-Host "  📋 Only emulator:       $($Data.OnlyEmulator.Count)"

        if ($Data.Suspect.Count -gt 0) {
            Write-Host ""
            Write-Host "  🔍 SUSPECT (in-memory passes, emulator fails):" -ForegroundColor Red
            foreach ($s in $Data.Suspect) { Write-Host "    - $($s.Test)" -ForegroundColor Red }
        }
        if ($Data.EmulatorGap.Count -gt 0) {
            Write-Host ""
            Write-Host "  ⚠️  EMULATOR GAP (emulator passes, in-memory fails):" -ForegroundColor Yellow
            foreach ($g in $Data.EmulatorGap) { Write-Host "    - $($g.Test)" -ForegroundColor Yellow }
        }
        Write-Host ""
    }
}

# Find in-memory baseline
$inmemoryTrx = Get-ChildItem $ResultsDir -Filter 'inmemory-results.trx' -ErrorAction SilentlyContinue
if (-not $inmemoryTrx) { Write-Error "No inmemory-results.trx found in $ResultsDir"; exit 1 }
$inmemoryResults = Parse-TrxFile $inmemoryTrx.FullName

# Discover emulator TRX files
if ($EmulatorTarget) {
    $emulatorTrxFiles = @(Get-ChildItem $ResultsDir -Filter "$EmulatorTarget-results.trx" -ErrorAction SilentlyContinue)
} else {
    $emulatorTrxFiles = @(Get-ChildItem $ResultsDir -Filter 'emulator-*-results.trx' -ErrorAction SilentlyContinue)
}

if ($emulatorTrxFiles.Count -eq 0) {
    $target = if ($EmulatorTarget) { $EmulatorTarget } else { "emulator-*" }
    Write-Error "No $target-results.trx found in $ResultsDir"
    exit 1
}

# Compare against each emulator and track suspects
$anySuspects = $false
foreach ($trxFile in $emulatorTrxFiles) {
    $label = $trxFile.BaseName -replace '-results$', ''
    $emulatorResults = Parse-TrxFile $trxFile.FullName
    $data = Compare-Results -InMemory $inmemoryResults -Emulator $emulatorResults
    Write-Report -Label $label -Data $data -Format $OutputFormat
    if ($data.Suspect.Count -gt 0) { $anySuspects = $true }
}

if ($anySuspects) { exit 1 }
