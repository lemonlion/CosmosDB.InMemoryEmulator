<#
.SYNOPSIS
    Compares all TRX files in the results directory and produces a unified cross-platform parity report.
.DESCRIPTION
    Auto-discovers every *-results.trx file in ResultsDir. The 'inmemory' file is the baseline;
    all others are emulator targets. Produces a single N-way comparison showing every test's
    outcome across all targets, plus per-target summary stats and a cross-platform divergence table.
.PARAMETER ResultsDir
    Directory containing TRX files. Default ./test-results.
.PARAMETER OutputFormat
    Output format: 'console' (default) or 'markdown' (for GitHub Step Summary).
.EXAMPLE
    .\scripts\compare-trx.ps1
    .\scripts\compare-trx.ps1 -ResultsDir ./test-results -OutputFormat markdown >> $env:GITHUB_STEP_SUMMARY
#>
param(
    [string]$ResultsDir = './test-results',
    [ValidateSet('console', 'markdown')]
    [string]$OutputFormat = 'console'
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

# --- Discover all TRX files ---
$allTrxFiles = @(Get-ChildItem $ResultsDir -Filter '*-results.trx' -ErrorAction SilentlyContinue)
if ($allTrxFiles.Count -eq 0) { Write-Error "No *-results.trx files found in $ResultsDir"; exit 1 }

# Parse all files into a hashtable keyed by target name
$targets = [ordered]@{}
foreach ($f in $allTrxFiles | Sort-Object Name) {
    $name = $f.BaseName -replace '-results$', ''
    $targets[$name] = Parse-TrxFile $f.FullName
}

# Identify baseline and emulator targets
$baselineName = 'inmemory'
if (-not $targets.Contains($baselineName)) {
    Write-Error "No inmemory-results.trx found in $ResultsDir (required as baseline)"
    exit 1
}
$baseline = $targets[$baselineName]
$emulatorNames = @($targets.Keys | Where-Object { $_ -ne $baselineName })

if ($emulatorNames.Count -eq 0) {
    Write-Error "No emulator TRX files found (only inmemory-results.trx present)"
    exit 1
}

# --- Build unified test matrix ---
$allTestNames = @()
foreach ($t in $targets.Values) { $allTestNames += $t.Keys }
$allTestNames = $allTestNames | Sort-Object -Unique

# Build per-test row: { Test, inmemory, emulator-linux, emulator-windows, ... }
$rows = @()
foreach ($test in $allTestNames) {
    $row = [ordered]@{ Test = $test }
    foreach ($name in $targets.Keys) {
        $row[$name] = if ($targets[$name].ContainsKey($test)) { $targets[$name][$test] } else { '-' }
    }
    $rows += [PSCustomObject]$row
}

# --- Classify each test ---
$fullParity = @()       # same outcome across ALL targets
$suspects = @()         # inmemory passes, at least one emulator fails
$emulatorGaps = @()     # inmemory fails, at least one emulator passes
$platformDiverge = @()  # emulators disagree with each other (but not suspect/gap)
$bothFail = @()         # fails everywhere

foreach ($row in $rows) {
    $im = $row.$baselineName
    $emOutcomes = @()
    foreach ($eName in $emulatorNames) { $emOutcomes += $row.$eName }

    $allSame = ($emOutcomes + $im | Sort-Object -Unique).Count -eq 1
    $anyEmFail = $emOutcomes | Where-Object { $_ -ne 'Passed' -and $_ -ne '-' }
    $anyEmPass = $emOutcomes | Where-Object { $_ -eq 'Passed' }
    $emulatorsDisagree = ($emOutcomes | Where-Object { $_ -ne '-' } | Sort-Object -Unique).Count -gt 1

    if ($allSame) {
        $fullParity += $row
    } elseif ($im -eq 'Passed' -and $anyEmFail) {
        $suspects += $row
    } elseif ($im -ne 'Passed' -and $im -ne '-' -and $anyEmPass) {
        $emulatorGaps += $row
    } elseif ($emulatorsDisagree) {
        $platformDiverge += $row
    } else {
        $bothFail += $row
    }
}

$totalTests = $allTestNames.Count
$parityPct = if ($totalTests -gt 0) { [math]::Round(($fullParity.Count / $totalTests) * 100, 1) } else { 0 }

# --- Helper to format a row for the divergence table ---
function Format-Outcome([string]$outcome) {
    switch ($outcome) {
        'Passed'      { '✅ Passed' }
        'Failed'      { '❌ Failed' }
        'NotExecuted' { '⏭️ Skipped' }
        '-'           { '—' }
        default       { $outcome }
    }
}

# --- Output ---
if ($OutputFormat -eq 'markdown') {
    # Header columns for tables
    $emulatorCols = ($emulatorNames | ForEach-Object { $_ }) -join ' | '
    $emulatorSep  = ($emulatorNames | ForEach-Object { '---' }) -join ' | '

    Write-Output "# Parity Report"
    Write-Output ""
    Write-Output "**Targets:** $($targets.Keys -join ', ')"
    Write-Output ""

    # Summary table
    Write-Output "## Summary"
    Write-Output ""
    Write-Output "| Metric | Count |"
    Write-Output "|--------|-------|"
    Write-Output "| Total tests | $totalTests |"
    Write-Output "| ✅ Full parity (all targets agree) | $($fullParity.Count) ($parityPct%) |"
    Write-Output "| 🔍 Suspects (in-memory passes, emulator fails) | $($suspects.Count) |"
    Write-Output "| ⚠️ Emulator gaps (emulator passes, in-memory fails) | $($emulatorGaps.Count) |"
    Write-Output "| 🔀 Platform divergence (emulators disagree) | $($platformDiverge.Count) |"
    Write-Output "| ❌ All fail | $($bothFail.Count) |"
    Write-Output ""

    # Per-target breakdown
    Write-Output "## Per-Target Breakdown"
    Write-Output ""
    Write-Output "| Target | Total | Passed | Failed | Skipped |"
    Write-Output "|--------|-------|--------|--------|---------|"
    foreach ($name in $targets.Keys) {
        $data = $targets[$name]
        $passed  = @($data.Values | Where-Object { $_ -eq 'Passed' }).Count
        $failed  = @($data.Values | Where-Object { $_ -eq 'Failed' }).Count
        $skipped = @($data.Values | Where-Object { $_ -eq 'NotExecuted' }).Count
        Write-Output "| $name | $($data.Count) | $passed | $failed | $skipped |"
    }
    Write-Output ""

    # Suspects detail
    if ($suspects.Count -gt 0) {
        Write-Output "## 🔍 Suspects — In-Memory Passes, Emulator Fails"
        Write-Output ""
        Write-Output "| Test | inmemory | $emulatorCols |"
        Write-Output "|------|----------|$emulatorSep|"
        foreach ($r in $suspects) {
            $vals = ($emulatorNames | ForEach-Object { Format-Outcome $r.$_ }) -join ' | '
            Write-Output "| $($r.Test) | $(Format-Outcome $r.$baselineName) | $vals |"
        }
        Write-Output ""
    }

    # Emulator gaps detail
    if ($emulatorGaps.Count -gt 0) {
        Write-Output "## ⚠️ Emulator Gaps — Emulator Passes, In-Memory Fails"
        Write-Output ""
        Write-Output "| Test | inmemory | $emulatorCols |"
        Write-Output "|------|----------|$emulatorSep|"
        foreach ($r in $emulatorGaps) {
            $vals = ($emulatorNames | ForEach-Object { Format-Outcome $r.$_ }) -join ' | '
            Write-Output "| $($r.Test) | $(Format-Outcome $r.$baselineName) | $vals |"
        }
        Write-Output ""
    }

    # Platform divergence detail
    if ($platformDiverge.Count -gt 0) {
        Write-Output "## 🔀 Platform Divergence — Emulators Disagree"
        Write-Output ""
        Write-Output "| Test | inmemory | $emulatorCols |"
        Write-Output "|------|----------|$emulatorSep|"
        foreach ($r in $platformDiverge) {
            $vals = ($emulatorNames | ForEach-Object { Format-Outcome $r.$_ }) -join ' | '
            Write-Output "| $($r.Test) | $(Format-Outcome $r.$baselineName) | $vals |"
        }
        Write-Output ""
    }

} else {
    # Console output
    Write-Host ""
    Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  PARITY REPORT" -ForegroundColor Cyan
    Write-Host "═══════════════════════════════════════════════════════════════" -ForegroundColor Cyan
    Write-Host "  Targets: $($targets.Keys -join ', ')" -ForegroundColor DarkGray
    Write-Host ""
    Write-Host "  Total tests:            $totalTests"
    Write-Host "  ✅ Full parity:          $($fullParity.Count) ($parityPct%)" -ForegroundColor Green
    Write-Host "  🔍 Suspects:             $($suspects.Count)" -ForegroundColor $(if ($suspects.Count -gt 0) { 'Red' } else { 'Green' })
    Write-Host "  ⚠️  Emulator gaps:       $($emulatorGaps.Count)" -ForegroundColor $(if ($emulatorGaps.Count -gt 0) { 'Yellow' } else { 'Green' })
    Write-Host "  🔀 Platform divergence:  $($platformDiverge.Count)" -ForegroundColor $(if ($platformDiverge.Count -gt 0) { 'Yellow' } else { 'Green' })
    Write-Host "  ❌ All fail:             $($bothFail.Count)" -ForegroundColor $(if ($bothFail.Count -gt 0) { 'Yellow' } else { 'Green' })

    # Per-target breakdown
    Write-Host ""
    Write-Host "  Per-Target:" -ForegroundColor Cyan
    foreach ($name in $targets.Keys) {
        $data = $targets[$name]
        $passed  = @($data.Values | Where-Object { $_ -eq 'Passed' }).Count
        $failed  = @($data.Values | Where-Object { $_ -eq 'Failed' }).Count
        $skipped = @($data.Values | Where-Object { $_ -eq 'NotExecuted' }).Count
        $color = if ($failed -gt 0) { 'Yellow' } else { 'Green' }
        Write-Host "    $($name.PadRight(20)) $passed passed, $failed failed, $skipped skipped" -ForegroundColor $color
    }

    if ($suspects.Count -gt 0) {
        Write-Host ""
        Write-Host "  🔍 SUSPECTS (in-memory passes, emulator fails):" -ForegroundColor Red
        foreach ($r in $suspects) {
            $parts = @()
            foreach ($eName in $emulatorNames) {
                $o = $r.$eName; if ($o -ne 'Passed') { $parts += "$eName=$o" }
            }
            Write-Host "    - $($r.Test)  [$($parts -join ', ')]" -ForegroundColor Red
        }
    }

    if ($emulatorGaps.Count -gt 0) {
        Write-Host ""
        Write-Host "  ⚠️  EMULATOR GAPS (emulator passes, in-memory fails):" -ForegroundColor Yellow
        foreach ($r in $emulatorGaps) {
            $parts = @()
            foreach ($eName in $emulatorNames) {
                $o = $r.$eName; if ($o -eq 'Passed') { $parts += $eName }
            }
            Write-Host "    - $($r.Test)  [passes on: $($parts -join ', ')]" -ForegroundColor Yellow
        }
    }

    if ($platformDiverge.Count -gt 0) {
        Write-Host ""
        Write-Host "  🔀 PLATFORM DIVERGENCE (emulators disagree):" -ForegroundColor Yellow
        foreach ($r in $platformDiverge) {
            $parts = @()
            foreach ($eName in $emulatorNames) { $parts += "$eName=$($r.$eName)" }
            Write-Host "    - $($r.Test)  [$($parts -join ', ')]" -ForegroundColor Yellow
        }
    }
    Write-Host ""
}

if ($suspects.Count -gt 0) { exit 1 }
