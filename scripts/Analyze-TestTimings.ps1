#Requires -Version 7.0
<#
.SYNOPSIS
    Analyses TRX test result files and writes a time-balanced shard profile.

.DESCRIPTION
    Parses one or more TRX files produced by `dotnet test --logger trx`,
    aggregates execution time per test class, then uses greedy bin-packing
    to split classes into N time-balanced shards.

    Writes scripts/shard-profile.json containing the --filter expression for
    each shard, consumed by .github/workflows/_build-and-test.yml at runtime.

    The final shard always uses a negative filter so any future test class is
    automatically captured without needing to re-run this script.

.PARAMETER TrxFiles
    One or more TRX result file paths. Glob patterns accepted.
    Timings are merged across all supplied files (e.g. multiple frameworks).

.PARAMETER Shards
    Number of shards to produce. Default: 3.

.PARAMETER DryRun
    Print the shard profile summary but do not write shard-profile.json.

.EXAMPLE
    .\scripts\Analyze-TestTimings.ps1 .\test-results\unit-timings.trx

.EXAMPLE
    .\scripts\Analyze-TestTimings.ps1 .\test-results\*.trx -Shards 4

.NOTES
    To regenerate shard-profile.json after adding new test classes:

        dotnet test tests/CosmosDB.InMemoryEmulator.Tests.Unit `
            --logger "trx;LogFileName=unit-timings.trx" `
            --results-directory ./test-results

        .\scripts\Analyze-TestTimings.ps1 .\test-results\unit-timings.trx
#>
[CmdletBinding(SupportsShouldProcess)]
param (
    [Parameter(Mandatory, Position = 0)]
    [string[]] $TrxFiles,

    [ValidateRange(2, 20)]
    [int] $Shards = 3,

    [switch] $DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# ── Helpers ──────────────────────────────────────────────────────────────────

function ConvertTo-TotalSeconds ([string] $Duration) {
    # TRX duration is HH:MM:SS.fffffff — TimeSpan.Parse handles this natively
    return [TimeSpan]::Parse($Duration).TotalSeconds
}

function Format-Duration ([double] $Seconds) {
    # Use Truncate (not [int] cast) — [int] uses banker's rounding which floors to ceiling
    $mins = [Math]::Truncate($Seconds / 60)
    $secs = [Math]::Truncate($Seconds % 60)
    return '{0}m{1:00}s' -f $mins, $secs
}

function Get-FilterSegment ([string] $FullyQualifiedName) {
    $parts = $FullyQualifiedName -split '\.'
    $tail  = if ($parts.Count -ge 2) { '.' + ($parts[-2..-1] -join '.') } else { $FullyQualifiedName }
    return "FullyQualifiedName~$tail"
}

# ── TRX parsing ──────────────────────────────────────────────────────────────

function Read-TrxDurations ([string] $Path) {
    $xml = [xml](Get-Content -Path $Path -Raw -Encoding UTF8)
    $ns  = New-Object System.Xml.XmlNamespaceManager($xml.NameTable)
    $ns.AddNamespace('t', 'http://microsoft.com/schemas/VisualStudio/TeamTest/2010')

    # Build testId → className from <TestDefinitions>
    $idToClass = @{}
    foreach ($test in $xml.SelectNodes('//t:UnitTest', $ns)) {
        $id     = $test.GetAttribute('id')
        $method = $test.SelectSingleNode('t:TestMethod', $ns)
        if ($id -and $method) {
            $className = $method.GetAttribute('className')
            if ($className) { $idToClass[$id] = $className }
        }
    }

    # Aggregate total seconds per class from <Results>
    $durations = @{}
    foreach ($result in $xml.SelectNodes('//t:UnitTestResult', $ns)) {
        $testId      = $result.GetAttribute('testId')
        $durationStr = $result.GetAttribute('duration')
        if ($durationStr -and $idToClass.ContainsKey($testId)) {
            $cls             = $idToClass[$testId]
            $durations[$cls] = ($durations[$cls] ?? 0.0) + (ConvertTo-TotalSeconds $durationStr)
        }
    }

    return $durations
}

# ── Greedy bin-packing ───────────────────────────────────────────────────────

function Invoke-BinPack ([hashtable] $ClassDurations, [int] $N) {
    $sorted = $ClassDurations.GetEnumerator() |
        Sort-Object @{E='Value'; D=$true}, @{E='Key'; D=$false}

    $shardLists = [object[]]::new($N)
    for ($j = 0; $j -lt $N; $j++) {
        $shardLists[$j] = [System.Collections.Generic.List[PSCustomObject]]::new()
    }
    $totals = [double[]]::new($N)

    foreach ($entry in $sorted) {
        $lightest = 0
        for ($i = 1; $i -lt $N; $i++) {
            if ($totals[$i] -lt $totals[$lightest]) { $lightest = $i }
        }
        $shardLists[$lightest].Add([PSCustomObject]@{
            Class    = $entry.Key
            Duration = $entry.Value
        })
        $totals[$lightest] += $entry.Value
    }

    return [PSCustomObject]@{ ShardLists = $shardLists; Totals = $totals }
}

# ── Main ─────────────────────────────────────────────────────────────────────

# Resolve globs and validate
$resolvedFiles = $TrxFiles | ForEach-Object { Resolve-Path $_ -ErrorAction Stop } |
    Select-Object -ExpandProperty Path

if (-not $resolvedFiles) {
    Write-Error 'No TRX files found matching the supplied paths.'
    exit 1
}

# Merge timings across all files (e.g. net8.0 + net10.0 runs)
$merged = @{}
foreach ($file in $resolvedFiles) {
    Write-Verbose "Parsing: $file"
    foreach ($entry in (Read-TrxDurations $file).GetEnumerator()) {
        $merged[$entry.Key] = ($merged[$entry.Key] ?? 0.0) + $entry.Value
    }
}

if ($merged.Count -eq 0) {
    Write-Error 'No test timings found in the supplied TRX file(s).'
    exit 1
}

$total  = ($merged.Values | Measure-Object -Sum).Sum
$ideal  = $total / $Shards
$packed = Invoke-BinPack -ClassDurations $merged -N $Shards

# Build list of all positive filter segments from all-but-last shards
# (used to construct the final shard's negative filter)
$positiveFilters = [System.Collections.Generic.List[string]]::new()
for ($i = 0; $i -lt $Shards - 1; $i++) {
    foreach ($entry in $packed.ShardLists[$i]) {
        $positiveFilters.Add((Get-FilterSegment $entry.Class))
    }
}

# ── Summary ──────────────────────────────────────────────────────────────────

$separator = '=' * 64
Write-Host $separator
Write-Host "  Shard profile  ($Shards shards, measured total: $(Format-Duration $total))"
Write-Host $separator
Write-Host "  Ideal per shard: $(Format-Duration $ideal)"
Write-Host ''

$shardProfiles = for ($i = 0; $i -lt $Shards; $i++) {
    $shard      = $packed.ShardLists[$i]
    $shardTotal = $packed.Totals[$i]
    $isLast     = $i -eq ($Shards - 1)
    $deviation  = [Math]::Abs($shardTotal - $ideal) / $ideal * 100

    $sortedByTime = $shard | Sort-Object Duration -Descending
    $preview      = ($sortedByTime | Select-Object -First 4 |
                        ForEach-Object { "$($_.Class.Split('.')[-1]) ($(Format-Duration $_.Duration))" }) -join ', '
    if ($shard.Count -gt 4) { $preview += ", +$($shard.Count - 4) more" }

    $filter = if ($isLast) {
        ($positiveFilters | ForEach-Object { $_ -replace '~', '!~' }) -join '&'
    } else {
        ($shard | ForEach-Object { Get-FilterSegment $_.Class }) -join '|'
    }

    Write-Host "  Shard $i  $(Format-Duration $shardTotal)  (+-$([int]$deviation)% from ideal)  $($shard.Count) classes"
    Write-Host "    $preview"
    Write-Host ''

    [PSCustomObject]@{
        id          = $i
        estimate    = Format-Duration $shardTotal
        classes     = $shard.Count
        top_classes = @($sortedByTime | Select-Object -First 5 | ForEach-Object { $_.Class.Split('.')[-1] })
        filter      = $filter
    }
}

# ── Write shard-profile.json ─────────────────────────────────────────────────

$profile = [ordered]@{
    '_comment'      = 'Auto-generated by scripts/Analyze-TestTimings.ps1 — do not edit by hand'
    generated       = [datetime]::UtcNow.ToString('yyyy-MM-ddTHH:mm:ssZ')
    shards          = $Shards
    total_estimate  = Format-Duration $total
    ideal_per_shard = Format-Duration $ideal
    shard_profiles  = @($shardProfiles)
}

$outputPath = Join-Path $PSScriptRoot 'shard-profile.json'
$json       = $profile | ConvertTo-Json -Depth 5

if ($DryRun) {
    Write-Host '(dry-run: skipping write)'
} else {
    $json | Set-Content -Path $outputPath -Encoding UTF8NoBOM
    Write-Host "Written: $outputPath"
    Write-Host ''
    Write-Host "Configure _build-and-test.yml with shard_index: [$(0..($Shards - 1) -join ', ')]"
}
