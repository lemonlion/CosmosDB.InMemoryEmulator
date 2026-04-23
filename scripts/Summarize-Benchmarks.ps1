<#
.SYNOPSIS
    Summarize benchmark results from multiple runs into an averaged markdown table.
.DESCRIPTION
    Reads test-output.txt files from the results directory (downloaded artifacts)
    and produces a per-scenario summary with averaged metrics across runs.
.PARAMETER ResultsDir
    Path to the directory containing benchmark result subdirectories.
.EXAMPLE
    ./scripts/Summarize-Benchmarks.ps1 -ResultsDir results
#>
[CmdletBinding()]
param(
    [string]$ResultsDir = 'results'
)

if (-not (Test-Path $ResultsDir -PathType Container)) {
    Write-Error "Results directory '$ResultsDir' not found"
    exit 1
}

$scenarioPattern = [regex]'║\s+(.+?(?:\(\d+/\d+\)))\s+║'
$metricsPattern  = [regex]'║\s+(Throughput|Mean latency|P50 latency|P95 latency|P99 latency|Max latency)\s*:\s*([\d.]+)'
$artifactPattern = [regex]'^bench-(?:emulator-)?(?<framework>net[\d.]+)-(?<rps>\d+)rps-(?<duration>\d+)s-run(?<run>\d+)$'
$metricOrder     = @('Throughput', 'Mean latency', 'P50 latency', 'P95 latency', 'P99 latency', 'Max latency')

# Parse all run artifacts into $configs: key = "configKey|scenario" → hashtable metric→List[double]
$configs = @{}

foreach ($artifactDir in (Get-ChildItem $ResultsDir -Directory | Sort-Object Name)) {
    $m = $artifactPattern.Match($artifactDir.Name)
    if (-not $m.Success) { continue }

    $framework = $m.Groups['framework'].Value
    $rps       = $m.Groups['rps'].Value
    $duration  = $m.Groups['duration'].Value
    $configKey = "$framework / ${rps}rps / ${duration}s"

    $filepath = Join-Path $artifactDir.FullName 'test-output.txt'
    if (-not (Test-Path $filepath)) {
        Write-Warning "Missing $filepath"
        continue
    }

    $currentScenario = 'Unknown'
    foreach ($line in (Get-Content $filepath)) {
        $sm = $scenarioPattern.Match($line)
        if ($sm.Success) { $currentScenario = $sm.Groups[1].Value.Trim() }

        $mm = $metricsPattern.Match($line)
        if ($mm.Success) {
            $metricName = $mm.Groups[1].Value.Trim()
            $value      = [double]$mm.Groups[2].Value
            $dictKey    = "$configKey|$currentScenario"

            if (-not $configs.ContainsKey($dictKey)) { $configs[$dictKey] = @{} }
            if (-not $configs[$dictKey].ContainsKey($metricName)) {
                $configs[$dictKey][$metricName] = [System.Collections.Generic.List[double]]::new()
            }
            $configs[$dictKey][$metricName].Add($value)
        }
    }
}

if ($configs.Count -eq 0) {
    Write-Warning "No benchmark results found to summarize"
    exit 0
}

# Determine run count from first entry
$runCount = 0
foreach ($entry in $configs.GetEnumerator()) {
    foreach ($vals in $entry.Value.GetEnumerator()) { $runCount = [Math]::Max($runCount, $vals.Value.Count); break }
    break
}

# Output averaged summary tables per scenario
$scenarios = $configs.Keys | ForEach-Object { ($_ -split '\|', 2)[1] } | Sort-Object -Unique

foreach ($scenario in $scenarios) {
    $plural = if ($runCount -ne 1) { 's' } else { '' }
    "# $scenario (Averaged over $runCount run$plural)"
    ""
    "| Config | Throughput (ops/s) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) | Max (ms) |"
    "|--------|-------------------|-----------|----------|----------|----------|----------|"

    foreach ($key in ($configs.Keys | Sort-Object)) {
        $parts = $key -split '\|', 2
        if ($parts[1] -ne $scenario) { continue }

        $row = @($parts[0])
        foreach ($metric in $metricOrder) {
            if ($configs[$key].ContainsKey($metric) -and $configs[$key][$metric].Count -gt 0) {
                $avg = ($configs[$key][$metric] | Measure-Object -Average).Average
                $row += if ($metric -match 'latency') { '{0:F3}' -f $avg } else { '{0:F1}' -f $avg }
            } else {
                $row += 'N/A'
            }
        }
        "| $($row -join ' | ') |"
    }
    ""
}

# Per-run details
"## Per-Run Details"
""

foreach ($artifactDir in (Get-ChildItem $ResultsDir -Directory | Sort-Object Name)) {
    $m = $artifactPattern.Match($artifactDir.Name)
    if (-not $m.Success) { continue }

    $framework = $m.Groups['framework'].Value
    $rps       = $m.Groups['rps'].Value
    $duration  = $m.Groups['duration'].Value
    $run       = $m.Groups['run'].Value
    $filepath  = Join-Path $artifactDir.FullName 'test-output.txt'
    if (-not (Test-Path $filepath)) { continue }

    $currentScenario = 'Unknown'
    $scenarioMetrics = @{}

    foreach ($line in (Get-Content $filepath)) {
        $sm = $scenarioPattern.Match($line)
        if ($sm.Success) {
            if ($scenarioMetrics.Count -gt 0) {
                $detail = ($scenarioMetrics.GetEnumerator() | ForEach-Object { "$($_.Key): $($_.Value)" }) -join ', '
                "- **$framework ${rps}rps ${duration}s run${run} — $currentScenario**: $detail"
            }
            $currentScenario = $sm.Groups[1].Value.Trim()
            $scenarioMetrics = @{}
        }
        $mm = $metricsPattern.Match($line)
        if ($mm.Success) {
            $scenarioMetrics[$mm.Groups[1].Value.Trim()] = [double]$mm.Groups[2].Value
        }
    }

    if ($scenarioMetrics.Count -gt 0) {
        $detail = ($scenarioMetrics.GetEnumerator() | ForEach-Object { "$($_.Key): $($_.Value)" }) -join ', '
        "- **$framework ${rps}rps ${duration}s run${run} — $currentScenario**: $detail"
    }
}
