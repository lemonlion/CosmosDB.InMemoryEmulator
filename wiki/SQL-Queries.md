The emulator includes a full SQL query engine built with [Superpower](https://github.com/datalust/superpower) parser combinators. It parses and executes Cosmos DB SQL queries against in-memory data, supporting **120+ built-in functions**.

> **See also:** [Features](Features) for higher-level feature descriptions · [API Reference](API-Reference) for query method signatures · [Known Limitations](Known-Limitations) for behavioural differences from real Cosmos DB

## Clauses

| Clause | Examples |
|--------|----------|
| `SELECT` | `SELECT *`, `SELECT c.name, c.age`, `SELECT VALUE c.name` |
| `SELECT DISTINCT` | `SELECT DISTINCT c.category` |
| `SELECT TOP` | `SELECT TOP 10 * FROM c` |
| `FROM` | `FROM c`, `FROM s IN c.scores` (top-level array iteration) |
| `WHERE` | `c.age > 30 AND c.active = true` |
| `ORDER BY` | `ORDER BY c.name ASC, c.date DESC` |
| `ORDER BY RANK` | `ORDER BY RANK FullTextScore(c.text, ['term'])` (for [full-text search](#full-text-search-functions)) |
| `GROUP BY` / `HAVING` | `GROUP BY c.category HAVING COUNT(1) > 5` |
| `OFFSET` / `LIMIT` | `OFFSET 10 LIMIT 20` |
| `JOIN` | `JOIN t IN c.tags` (array expansion, multiple JOINs) |

## Operators

| Category | Supported |
|----------|-----------|
| Comparison | `=`, `!=`, `<>`, `<`, `>`, `<=`, `>=` |
| Logical | `AND`, `OR`, `NOT` |
| Arithmetic | `+`, `-`, `*`, `/`, `%` |
| String concat | `\|\|` |
| Null coalesce | `??` |
| Ternary | `condition ? ifTrue : ifFalse` |
| Bitwise | `&`, `\|`, `^`, `~` |
| Range | `BETWEEN low AND high`, `NOT BETWEEN low AND high` |
| Membership | `IN ('a', 'b', 'c')`, `NOT IN (...)` |
| Pattern | `LIKE '%pattern%'`, `NOT LIKE`, `LIKE ... ESCAPE` (with `%` and `_` wildcards) |
| Null checks | `IS NULL`, `IS NOT NULL` |

## Built-in Functions

### String Functions

| Function | Example |
|----------|---------|
| `UPPER(str)` | `SELECT VALUE UPPER(c.name) FROM c` |
| `LOWER(str)` | `SELECT VALUE LOWER(c.name) FROM c` |
| `LTRIM(str)` | `SELECT VALUE LTRIM(c.name) FROM c` |
| `RTRIM(str)` | `SELECT VALUE RTRIM(c.name) FROM c` |
| `TRIM(str)` | `SELECT VALUE TRIM(c.name) FROM c` |
| `SUBSTRING(str, start, length)` | `SELECT VALUE SUBSTRING(c.name, 0, 3) FROM c` |
| `LENGTH(str)` | `SELECT VALUE LENGTH(c.name) FROM c` |
| `CONCAT(str1, str2, ...)` | `SELECT VALUE CONCAT(c.first, ' ', c.last) FROM c` |
| `CONTAINS(str, substr [, ignoreCase])` | `SELECT * FROM c WHERE CONTAINS(c.name, 'ali', true)` |
| `STARTSWITH(str, prefix [, ignoreCase])` | `SELECT * FROM c WHERE STARTSWITH(c.name, 'A', true)` |
| `ENDSWITH(str, suffix [, ignoreCase])` | `SELECT * FROM c WHERE ENDSWITH(c.name, 'e', true)` |
| `INDEX_OF(str, substr)` | `SELECT VALUE INDEX_OF(c.name, 'l') FROM c` |
| `REPLACE(str, old, new)` | `SELECT VALUE REPLACE(c.name, 'old', 'new') FROM c` |
| `REVERSE(str)` | `SELECT VALUE REVERSE(c.name) FROM c` |
| `LEFT(str, count)` | `SELECT VALUE LEFT(c.name, 3) FROM c` |
| `RIGHT(str, count)` | `SELECT VALUE RIGHT(c.name, 3) FROM c` |
| `REPLICATE(str, count)` | `SELECT VALUE REPLICATE('x', 5) FROM c` |
| `REGEXMATCH(str, pattern [, modifiers])` | `SELECT * FROM c WHERE REGEXMATCH(c.email, '^[a-z]+@', 'i')` |
| `StringEquals(str1, str2 [, ignoreCase])` | `SELECT * FROM c WHERE StringEquals(c.name, 'JOHN', true)` |
| `StringJoin(separator, array)` | `SELECT VALUE StringJoin(',', c.tags) FROM c` |
| `StringSplit(str, delimiter)` | `SELECT VALUE StringSplit(c.csv, ',') FROM c` |

> **Optional arguments:** `CONTAINS`, `STARTSWITH`, and `ENDSWITH` accept an optional boolean 3rd argument for case-insensitive matching. `REGEXMATCH` accepts an optional 3rd argument with modifier flags: `'i'` (ignore case), `'m'` (multiline), `'s'` (single-line), `'x'` (ignore whitespace).

### Type-Checking Functions

| Function | Returns `true` when... |
|----------|----------------------|
| `IS_ARRAY(expr)` | Value is an array |
| `IS_BOOL(expr)` | Value is a boolean |
| `IS_NULL(expr)` | Value is null |
| `IS_DEFINED(expr)` | Property exists on the document |
| `IS_NUMBER(expr)` | Value is a number |
| `IS_OBJECT(expr)` | Value is a JSON object |
| `IS_STRING(expr)` | Value is a string |
| `IS_PRIMITIVE(expr)` | Value is string, number, boolean, or null |
| `IS_FINITE_NUMBER(expr)` | Value is a finite number (not NaN/Infinity) |
| `IS_INTEGER(expr)` | Value is an integer |

### Math Functions

| Function | Description |
|----------|-------------|
| `ABS(num)` | Absolute value |
| `CEILING(num)` | Ceiling (round up) |
| `FLOOR(num)` | Floor (round down) |
| `ROUND(num)` | Round to nearest integer |
| `SQRT(num)` | Square root |
| `SQUARE(num)` | Square (num²) |
| `POWER(base, exp)` | Exponentiation |
| `EXP(num)` | e^num |
| `LOG(num)` | Natural logarithm |
| `LOG10(num)` | Base-10 logarithm |
| `SIGN(num)` | Sign (-1, 0, or 1) |
| `TRUNC(num)` | Truncate to integer |
| `PI()` | π constant |
| `SIN(num)`, `COS(num)`, `TAN(num)` | Trigonometric functions |
| `COT(num)` | Cotangent (1/tan) |
| `ASIN(num)`, `ACOS(num)`, `ATAN(num)` | Inverse trigonometric functions |
| `ATN2(y, x)` | Two-argument arctangent |
| `DEGREES(radians)` | Radians to degrees |
| `RADIANS(degrees)` | Degrees to radians |
| `RAND()` | Random number [0, 1) |
| `NumberBin(num, binSize)` | Bin a number to the nearest multiple |

### Integer Math Functions

| Function | Description |
|----------|-------------|
| `IntAdd(a, b)` | Integer addition |
| `IntSub(a, b)` | Integer subtraction |
| `IntMul(a, b)` | Integer multiplication |
| `IntDiv(a, b)` | Integer division |
| `IntMod(a, b)` | Integer modulo |
| `IntBitAnd(a, b)` | Bitwise AND |
| `IntBitOr(a, b)` | Bitwise OR |
| `IntBitXor(a, b)` | Bitwise XOR |
| `IntBitNot(a)` | Bitwise NOT |
| `IntBitLeftShift(a, b)` | Left shift |
| `IntBitRightShift(a, b)` | Right shift |

### Array Functions

| Function | Example |
|----------|---------|
| `ARRAY_CONTAINS(arr, val)` | `SELECT * FROM c WHERE ARRAY_CONTAINS(c.tags, 'urgent')` |
| `ARRAY_CONTAINS_ANY(arr, searchArr)` | `true` if any element of searchArr exists in arr |
| `ARRAY_CONTAINS_ALL(arr, searchArr)` | `true` if all elements of searchArr exist in arr |
| `ARRAY_LENGTH(arr)` | `SELECT VALUE ARRAY_LENGTH(c.items) FROM c` |
| `ARRAY_SLICE(arr, start, length)` | `SELECT VALUE ARRAY_SLICE(c.items, 0, 3) FROM c` |
| `ARRAY_CONCAT(arr1, arr2)` | `SELECT VALUE ARRAY_CONCAT(c.tags, c.labels) FROM c` |
| `SetIntersect(arr1, arr2)` | Set intersection |
| `SetUnion(arr1, arr2)` | Set union |
| `SetDifference(arr1, arr2)` | Set difference (elements in arr1 not in arr2) |
| `CHOOSE(index, val1, val2, ...)` | 1-based index selection from argument list |
| `ObjectToArray(object)` | Convert `{a:1, b:2}` to `[{k:"a", v:1}, ...]` |
| `ArrayToObject(array)` | Convert `[{k:"a", v:1}, ...]` to `{a:1, b:2}` (inverse of `ObjectToArray`) |

### Conversion Functions

| Function | Description |
|----------|-------------|
| `TOSTRING(value)` | Convert to string |
| `TONUMBER(value)` | Convert to number |
| `TOBOOLEAN(value)` | Convert to boolean |
| `StringToArray(str)` | Parse JSON array string |
| `StringToBoolean(str)` | Parse boolean string |
| `StringToNull(str)` | Parse "null" string |
| `StringToNumber(str)` | Parse number string |
| `StringToObject(str)` | Parse JSON object string |

### Date/Time Functions

| Function | Description |
|----------|-------------|
| `GetCurrentDateTime()` | Current UTC datetime as ISO string |
| `GetCurrentTimestamp()` | Current UTC as Unix timestamp (ms) |
| `GetCurrentTicks()` | Current UTC as .NET ticks |
| `GetCurrentDateTimeStatic()` | Same datetime for all items in a query |
| `GetCurrentTimestampStatic()` | Same timestamp for all items in a query |
| `GetCurrentTicksStatic()` | Same ticks for all items in a query |
| `DateTimeAdd(part, num, datetime)` | Add to a datetime |
| `DateTimePart(part, datetime)` | Extract part of a datetime |
| `DateTimeDiff(part, start, end)` | Difference between two datetimes |
| `DateTimeBin(datetime, part, binSize, [origin])` | Bin a datetime to intervals |
| `DateTimeFromParts(year, month, day, hour, min, sec, ms)` | Construct a datetime |
| `DateTimeToTicks(datetime)` | Convert ISO datetime to .NET ticks |
| `DateTimeToTimestamp(datetime)` | Convert ISO datetime to Unix timestamp (ms) |
| `TicksToDateTime(ticks)` | Convert .NET ticks to ISO datetime |
| `TimestampToDateTime(timestamp)` | Convert Unix timestamp (ms) to ISO datetime |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(expr)`, `COUNT(1)`, or `COUNT(*)` | Count items |
| `SUM(expr)` | Sum numeric values |
| `AVG(expr)` | Average of numeric values |
| `MIN(expr)` | Minimum value |
| `MAX(expr)` | Maximum value |

### Conditional Functions

| Function | Description |
|----------|-------------|
| `IIF(condition, ifTrue, ifFalse)` | Inline conditional |
| `COALESCE(expr1, expr2, ...)` | First non-null/undefined value |

### Item Functions

| Function | Description |
|----------|-------------|
| `DOCUMENTID(item)` | Returns the document's resource ID |

### Geospatial Functions

| Function | Implementation |
|----------|---------------|
| `ST_DISTANCE(point1, point2)` | Haversine formula (metres) |
| `ST_WITHIN(point, region)` | Point-in-polygon (ray casting) + point-in-circle (haversine radius) |
| `ST_INTERSECTS(geo1, geo2)` | Point-point, point-polygon, polygon-polygon overlap |
| `ST_ISVALID(geojson)` | Full GeoJSON validation (Point, Polygon, LineString, MultiPoint) |
| `ST_ISVALIDDETAILED(geojson)` | Returns `{ valid, reason }` with specific error messages |
| `ST_AREA(polygon)` | Spherical excess formula |

> All geospatial functions use real geometric calculations. Results may differ slightly from Cosmos DB's exact values. See [Known Limitations](Known-Limitations) for details on precision differences.

### Vector Functions

| Function | Implementation |
|----------|---------------|
| `VECTORDISTANCE(vec1, vec2)` | Cosine similarity (default). Returns -1 to +1 |
| `VECTORDISTANCE(vec1, vec2, bruteForce)` | 3rd bool arg accepted but ignored (always brute-force) |
| `VECTORDISTANCE(vec1, vec2, false, {distanceFunction:'cosine'})` | Cosine similarity: `dot(a,b) / (|a| × |b|)` |
| `VECTORDISTANCE(vec1, vec2, false, {distanceFunction:'dotproduct'})` | Dot product: `Σ(a[i] × b[i])` |
| `VECTORDISTANCE(vec1, vec2, false, {distanceFunction:'euclidean'})` | Euclidean distance: `√Σ(a[i] - b[i])²` |

Works in `SELECT` projections, `WHERE` filters, and `ORDER BY` clauses. Supports vectors of any dimensionality (tested up to 2000). Returns `null` for mismatched dimensions, missing vector properties, zero-magnitude vectors (cosine), non-numeric elements, multi-dimensional arrays, or Infinity/NaN overflow results. Additional options (`dataType`, `searchListSizeMultiplier`, `filterPriority`) are accepted but ignored. Unknown distance functions silently fall back to cosine. Extra arguments beyond the 4th are silently ignored. Works with arithmetic expressions (`VectorDistance(...) * 100`), function composition (`ABS(VectorDistance(...))`), and conditional logic (`IIF(VectorDistance(...) > 0.5, ...)`).

```sql
-- Find the 10 most similar documents by cosine similarity
SELECT TOP 10 c.title, VectorDistance(c.embedding, [0.1, 0.2, 0.3]) AS score
FROM c
ORDER BY VectorDistance(c.embedding, [0.1, 0.2, 0.3]) DESC

-- Filter by similarity threshold
SELECT c.id FROM c
WHERE VectorDistance(c.embedding, [0.1, 0.2, 0.3]) > 0.5

-- Arithmetic on vector scores
SELECT c.id, VectorDistance(c.embedding, [0.1, 0.2, 0.3]) * 100 AS pctScore
FROM c
```

> No vector index policy or container configuration is required. The emulator always performs exact (brute-force) distance computation, which is correct for testing but does not simulate ANN index behaviour (DiskANN, quantizedFlat, flat). See [Known Limitations](Known-Limitations) for details.

### Full-Text Search Functions

The emulator provides approximate implementations of all four Cosmos DB full-text search functions using case-insensitive substring matching. Real Cosmos DB uses NLP tokenisation and BM25 scoring.

| Function | Description |
|----------|-------------|
| `FullTextContains(field, term)` | `true` if the field contains the search term |
| `FullTextContainsAll(field, term1, term2, ...)` | `true` if the field contains **all** search terms |
| `FullTextContainsAny(field, term1, term2, ...)` | `true` if the field contains **any** search term |
| `FullTextScore(field, [term1, term2, ...])` | Returns a relevance score (naive term-frequency count) |

Use `ORDER BY RANK FullTextScore(...)` to sort results by relevance:

```sql
-- Find documents containing 'database', sorted by relevance
SELECT * FROM c
WHERE FullTextContains(c.description, 'database')
ORDER BY RANK FullTextScore(c.description, ['database', 'cosmos'])
```

> **Note:** Real Cosmos DB requires a full-text indexing policy. The emulator skips this validation — queries work on any container without configuration. Matching is approximate (case-insensitive substring, no stemming, no BM25 scoring). See [Known Limitations](Known-Limitations) for details.

## Parameterised Queries

```csharp
var query = new QueryDefinition(
        "SELECT * FROM c WHERE c.status = @status AND c.age > @minAge")
    .WithParameter("@status", "active")
    .WithParameter("@minAge", 21);

var iterator = container.GetItemQueryIterator<MyDoc>(query);
```

## Subqueries

```sql
-- EXISTS
SELECT * FROM c WHERE EXISTS(
    SELECT VALUE 1 FROM t IN c.tags WHERE t = 'important')

-- ARRAY()
SELECT c.id, ARRAY(
    SELECT VALUE t FROM t IN c.tags WHERE t != 'draft') AS filteredTags
FROM c

-- Scalar subqueries in SELECT and WHERE
SELECT (SELECT VALUE COUNT(1) FROM t IN c.items) AS itemCount FROM c
```

### ORDER BY, OFFSET, and LIMIT in Subqueries

Subqueries support `ORDER BY`, `OFFSET`, and `LIMIT` clauses, evaluated within the subquery scope:

```sql
-- Sorted subquery
SELECT ARRAY(SELECT VALUE s FROM s IN c.scores ORDER BY s DESC) AS sorted FROM c

-- Paginated subquery (skip 1, take 2)
SELECT ARRAY(SELECT VALUE s FROM s IN c.scores OFFSET 1 LIMIT 2) AS page FROM c

-- Combined: sort, then paginate
SELECT ARRAY(SELECT VALUE s FROM s IN c.scores ORDER BY s ASC OFFSET 1 LIMIT 3) AS page FROM c
```

## User-Defined Functions (UDFs)

Register C# functions callable as `udf.name()` in SQL. See [API Reference](API-Reference) for the full `RegisterUdf` signature.

```csharp
container.RegisterUdf("IsEven", args =>
{
    if (args[0] is not long num) return false;
    return num % 2 == 0;
});

var iterator = container.GetItemQueryIterator<dynamic>(
    "SELECT * FROM c WHERE udf.IsEven(c.value)");
```

## Parser API

The SQL parser is also available directly for advanced scenarios. See [API Reference](API-Reference#cosmossqlparser) for the full class reference.

```csharp
// Parse a query
var parsed = CosmosSqlParser.Parse("SELECT c.name FROM c WHERE c.age > 30");

// Inspect the AST
Console.WriteLine(parsed.FromAlias);      // "c"
Console.WriteLine(parsed.Fields[0].Alias); // "name"

// Try-parse (no exception on failure)
if (CosmosSqlParser.TryParse(sql, out var result))
{
    // Use result
}
```
