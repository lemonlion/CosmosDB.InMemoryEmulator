$base = "c:\git\CosmosDB.InMemoryEmulator\plans\diagrams"
$svgDir = "$base\abbreviated\svg"
$outFile = "$base\query-flow-comparison-inline.md"

$a1sql     = [System.IO.File]::ReadAllText("$svgDir\approach1-sql-abbreviated.svg").Trim()
$a1linq    = [System.IO.File]::ReadAllText("$svgDir\approach1-linq-abbreviated.svg").Trim()
$a2seq     = [System.IO.File]::ReadAllText("$svgDir\approach2-abbreviated.svg").Trim()
$a3sql     = [System.IO.File]::ReadAllText("$svgDir\approach3-sql-abbreviated.svg").Trim()
$a3linq    = [System.IO.File]::ReadAllText("$svgDir\approach3-linq-abbreviated.svg").Trim()
$a1sqlAct  = [System.IO.File]::ReadAllText("$svgDir\approach1-sql-abbreviated-activity.svg").Trim()
$a1linqAct = [System.IO.File]::ReadAllText("$svgDir\approach1-linq-abbreviated-activity.svg").Trim()
$a2act     = [System.IO.File]::ReadAllText("$svgDir\approach2-abbreviated-activity.svg").Trim()
$a3sqlAct  = [System.IO.File]::ReadAllText("$svgDir\approach3-sql-abbreviated-activity.svg").Trim()
$a3linqAct = [System.IO.File]::ReadAllText("$svgDir\approach3-linq-abbreviated-activity.svg").Trim()

$bt = '`'  # backtick for inline code in markdown

$md = @"
# Query Flow Comparison — Integration Approaches

This page compares the query execution flow across the three integration approaches described in [Integration Approaches](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Integration-Approaches). SQL queries are shown first (simpler pattern), then LINQ queries.

> **Approach 1** (InMemoryContainer) is included standalone — Approaches 2 and 3 are shown side-by-side for comparison since they both involve a ${bt}CosmosClient${bt}.

---

## Sequence Diagrams

### SQL Query Flow

#### Approach 1: InMemoryContainer (standalone)

<div>
$a1sql
</div>

#### Approach 2 vs Approach 3 — SQL (side-by-side)

<table>
<tr>
<th>Approach 2: InMemoryCosmosClient + InMemoryContainer</th>
<th>Approach 3: CosmosClient + FakeCosmosHandler</th>
</tr>
<tr>
<td>

$a2seq

</td>
<td>

$a3sql

</td>
</tr>
</table>

### LINQ Query Flow

#### Approach 1: InMemoryContainer (standalone)

<div>
$a1linq
</div>

#### Approach 2 vs Approach 3 — LINQ (side-by-side)

<table>
<tr>
<th>Approach 2: InMemoryCosmosClient + InMemoryContainer</th>
<th>Approach 3: CosmosClient + FakeCosmosHandler</th>
</tr>
<tr>
<td>

$a2seq

</td>
<td>

$a3linq

</td>
</tr>
</table>

| Feature | Approach 1 | Approach 2 | Approach 3 |
|---|---|---|---|
| Setup | ${bt}new InMemoryContainer(...)${bt} | ${bt}new InMemoryCosmosClient(...)${bt} then ${bt}GetContainer()${bt} | ${bt}new InMemoryContainer${bt} + ${bt}new FakeCosmosHandler${bt} + ${bt}new CosmosClient${bt} then ${bt}GetContainer()${bt} |
| Query parsing | ${bt}CosmosSqlParser${bt} | ${bt}CosmosSqlParser${bt} | ${bt}CosmosSqlParser${bt} (via ${bt}FakeCosmosHandler${bt}) |
| HTTP layer | None | None | Real SDK HTTP pipeline |
| FeedIterator | ${bt}InMemoryFeedIterator<T>${bt} | ${bt}InMemoryFeedIterator<T>${bt} | Real SDK ${bt}FeedIterator<T>${bt} |
| LINQ path | ${bt}LINQ-to-Objects${bt} | ${bt}LINQ-to-Objects${bt} | Real ${bt}CosmosLinqQueryProvider${bt} → SQL → ${bt}CosmosSqlParser${bt} |

---

## Activity Diagrams

### SQL Query Flow

#### Approach 1: InMemoryContainer (standalone)

<div>
$a1sqlAct
</div>

#### Approach 2 vs Approach 3 — SQL (side-by-side)

<table>
<tr>
<th>Approach 2: InMemoryCosmosClient + InMemoryContainer</th>
<th>Approach 3: CosmosClient + FakeCosmosHandler</th>
</tr>
<tr>
<td>

$a2act

</td>
<td>

$a3sqlAct

</td>
</tr>
</table>

### LINQ Query Flow

#### Approach 1: InMemoryContainer (standalone)

<div>
$a1linqAct
</div>

#### Approach 2 vs Approach 3 — LINQ (side-by-side)

<table>
<tr>
<th>Approach 2: InMemoryCosmosClient + InMemoryContainer</th>
<th>Approach 3: CosmosClient + FakeCosmosHandler</th>
</tr>
<tr>
<td>

$a2act

</td>
<td>

$a3linqAct

</td>
</tr>
</table>
"@

[System.IO.File]::WriteAllText($outFile, $md, [System.Text.UTF8Encoding]::new($false))

$f = Get-Item $outFile
$content = [System.IO.File]::ReadAllText($outFile)
Write-Output "Size: $([Math]::Round($f.Length/1KB,1)) KB"
Write-Output "Has Setup in a2: $($content.Contains('>Setup<'))"
Write-Output "Has Get Container: $($content.Contains('Get Container'))"
Write-Output "Has Container Resolution: $($content.Contains('Container Resolution'))"
Write-Output "GetContainer count: $(([regex]::Matches($content, 'GetContainer')).Count)"
