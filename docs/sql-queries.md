# SQL Query Support

The emulator includes a full SQL query engine built with [Superpower](https://github.com/datalust/superpower) parser combinators. It parses and executes Cosmos DB SQL queries against in-memory data.

## Clauses

| Clause | Examples |
|--------|----------|
| `SELECT` | `SELECT *`, `SELECT c.name, c.age`, `SELECT VALUE c.name` |
| `SELECT DISTINCT` | `SELECT DISTINCT c.category` |
| `SELECT TOP` | `SELECT TOP 10 * FROM c` |
| `WHERE` | `c.age > 30 AND c.active = true` |
| `ORDER BY` | `ORDER BY c.name ASC, c.date DESC` |
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
| Range | `BETWEEN low AND high` |
| Membership | `IN ('a', 'b', 'c')` |
| Pattern | `LIKE '%pattern%'` (with `%` and `_` wildcards) |
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
| `CONTAINS(str, substr)` | `SELECT * FROM c WHERE CONTAINS(c.name, 'ali')` |
| `STARTSWITH(str, prefix)` | `SELECT * FROM c WHERE STARTSWITH(c.name, 'A')` |
| `ENDSWITH(str, suffix)` | `SELECT * FROM c WHERE ENDSWITH(c.name, 'e')` |
| `INDEX_OF(str, substr)` | `SELECT VALUE INDEX_OF(c.name, 'l') FROM c` |
| `REPLACE(str, old, new)` | `SELECT VALUE REPLACE(c.name, 'old', 'new') FROM c` |
| `REVERSE(str)` | `SELECT VALUE REVERSE(c.name) FROM c` |
| `LEFT(str, count)` | `SELECT VALUE LEFT(c.name, 3) FROM c` |
| `RIGHT(str, count)` | `SELECT VALUE RIGHT(c.name, 3) FROM c` |
| `REPLICATE(str, count)` | `SELECT VALUE REPLICATE('x', 5) FROM c` |
| `REGEXMATCH(str, pattern)` | `SELECT * FROM c WHERE REGEXMATCH(c.email, '^[a-z]+@')` |
| `StringEquals(str1, str2, ignoreCase)` | `SELECT * FROM c WHERE StringEquals(c.name, 'JOHN', true)` |
| `TOSTRING(value)` | `SELECT VALUE TOSTRING(c.age) FROM c` |

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
| `ARRAY_LENGTH(arr)` | `SELECT VALUE ARRAY_LENGTH(c.items) FROM c` |
| `ARRAY_SLICE(arr, start, length)` | `SELECT VALUE ARRAY_SLICE(c.items, 0, 3) FROM c` |
| `ARRAY_CONCAT(arr1, arr2)` | `SELECT VALUE ARRAY_CONCAT(c.tags, c.labels) FROM c` |
| `SetIntersect(arr1, arr2)` | Set intersection |
| `SetUnion(arr1, arr2)` | Set union |

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
| `DateTimeAdd(part, num, datetime)` | Add to a datetime |
| `DateTimePart(part, datetime)` | Extract part of a datetime |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `COUNT(expr)` or `COUNT(1)` | Count items |
| `SUM(expr)` | Sum numeric values |
| `AVG(expr)` | Average of numeric values |
| `MIN(expr)` | Minimum value |
| `MAX(expr)` | Maximum value |

### Conditional Functions

| Function | Description |
|----------|-------------|
| `IIF(condition, ifTrue, ifFalse)` | Inline conditional |
| `COALESCE(expr1, expr2, ...)` | First non-null/undefined value |

### Geospatial Functions (Stub)

| Function | Status |
|----------|--------|
| `ST_DISTANCE(point1, point2)` | Returns synthetic distance |
| `ST_WITHIN(point, polygon)` | Returns synthetic result |
| `ST_INTERSECTS(geo1, geo2)` | Returns synthetic result |
| `ST_ISVALID(geojson)` | Returns synthetic result |
| `ST_ISVALIDDETAILED(geojson)` | Returns synthetic result |

> Geospatial functions use a simplified haversine approximation. For precision-critical geospatial testing, use the real Cosmos DB or the official emulator.

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

## User-Defined Functions (UDFs)

Register C# functions callable as `udf.name()` in SQL:

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

The SQL parser is also available directly for advanced scenarios:

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
