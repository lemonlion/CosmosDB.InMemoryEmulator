# CosmosDB.InMemoryEmulator

A high-fidelity, in-memory implementation of the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

> **Drop-in replacement for `Microsoft.Azure.Cosmos.Container`** — no network, no emulator process, no Docker, no Azure subscription required.

### Zero Production Code Changes Required

Using the `FakeCosmosHandler` approach, **your production code stays completely untouched** — including calls to `.ToFeedIterator()` and `GetItemLinqQueryable<T>()` (which returns `IOrderedQueryable<T>`). The handler intercepts HTTP traffic at the SDK level, so LINQ queries, SQL queries, and all other Cosmos operations work exactly as written. No wrapper interfaces, no test-specific abstractions, no code changes at all.

```csharp
// Your production code — works against real Cosmos DB AND the in-memory emulator, unmodified
var results = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();  // ← works as-is, no changes needed
```

## Why This Exists

Testing code that depends on Azure Cosmos DB is painful. The available options each have significant drawbacks:

| Approach | Problem |
|----------|---------|
| **[Official Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator)** | Heavy process, slow startup, port conflicts, unreliable in CI, Windows/Docker-only, poor performance under heavy test load |
| **Mocking `CosmosClient` directly** | Fragile, doesn't test query logic, misses serialization bugs, very low fidelity |
| **Real Azure Cosmos DB** | Slow, costly, requires network, shared state between test runs |

**CosmosDB.InMemoryEmulator** fills the gap: a comprehensive in-process fake that supports the full breadth of the Cosmos DB SDK surface area — SQL queries with a real parser, change feeds, transactional batches, patch operations, TTL, ETags, LINQ, stream APIs, and more — all running at in-memory speed with zero external dependencies.

## Features

- **Full CRUD** — `CreateItemAsync`, `ReadItemAsync`, `UpsertItemAsync`, `ReplaceItemAsync`, `DeleteItemAsync` (typed and stream variants)
- **Rich SQL query engine** — Parser built with [Superpower](https://github.com/datalust/superpower) supporting `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `TOP`, `DISTINCT`, `OFFSET/LIMIT`, subqueries, and 40+ built-in functions
- **LINQ support** — `GetItemLinqQueryable<T>()` with two integration approaches for `.ToFeedIterator()`
- **Transactional batches** — Atomic multi-operation execution with rollback on failure
- **Change feed** — Iterators, checkpoints, and `ChangeFeedProcessor` with polling
- **Patch operations** — `Set`, `Replace`, `Add`, `Remove`, `Increment` with deep nested path support
- **ETag / optimistic concurrency** — `IfMatchEtag`, `IfNoneMatchEtag`, wildcard `*` support
- **Partition keys** — Single and composite, auto-extraction from documents, partition-scoped queries
- **TTL / expiration** — Container-level and per-item `_ttl` with lazy eviction
- **ReadMany** — Batch reads via `ReadManyItemsAsync`
- **Stream API** — Full `*StreamAsync` variants returning `ResponseMessage`
- **State persistence** — Export/import container state as JSON
- **Stored procedures & UDFs** — Register C# handlers callable via `Scripts.ExecuteStoredProcedureAsync` and `udf.*()` in SQL
- **Container management** — `ReadContainerAsync`, `ReplaceContainerAsync`, `DeleteContainerAsync`, `DeleteAllItemsByPartitionKeyStreamAsync`
- **HTTP-level interception** — `FakeCosmosHandler` intercepts SDK HTTP traffic for zero-code-change integration
- **Fault injection** — Simulate 429 throttling, 503 errors, and timeouts
- **Multi-container routing** — Single `CosmosClient` instance backed by multiple in-memory containers
- **SDK compatibility verification** — Detect breaking changes in the Cosmos SDK early
- **800+ tests** covering CRUD, queries, batches, change feed, patch, TTL, ETags, LINQ, streams, and edge cases

## Installation

```
dotnet add package CosmosDB.InMemoryEmulator
```

**Target framework:** .NET 8.0  
**Cosmos SDK:** Microsoft.Azure.Cosmos 3.57.1+

## Quick Start

### Approach 1: Direct `InMemoryContainer` Usage

The simplest approach — use `InMemoryContainer` directly in place of `Container`. Requires a one-token change in production code: `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`.

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

// Create an in-memory container
var container = new InMemoryContainer("my-container", "/partitionKey");

// CRUD works exactly like the real SDK
var item = new { id = "1", partitionKey = "pk1", name = "Alice", age = 30 };
var response = await container.CreateItemAsync(item, new PartitionKey("pk1"));
// response.StatusCode == HttpStatusCode.Created

var read = await container.ReadItemAsync<dynamic>("1", new PartitionKey("pk1"));
// read.Resource.name == "Alice"

// SQL queries work out of the box
var query = new QueryDefinition("SELECT * FROM c WHERE c.age > @age")
    .WithParameter("@age", 25);
var iterator = container.GetItemQueryIterator<dynamic>(query);

while (iterator.HasMoreResults)
{
    var page = await iterator.ReadNextAsync();
    foreach (var doc in page)
        Console.WriteLine(doc);
}

// LINQ queries use .ToFeedIteratorOverridable() instead of .ToFeedIterator()
var linqIterator = container
    .GetItemLinqQueryable<dynamic>()
    .Where(x => x.age > 25)
    .ToFeedIteratorOverridable();
```

### Approach 2: `FakeCosmosHandler` (Zero Production Code Changes)

Intercepts HTTP traffic at the SDK level. Production code stays completely untouched — including `.ToFeedIterator()`.

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

// Setup: wire the handler into CosmosClient
var inMemoryContainer = new InMemoryContainer("orders", "/partitionKey");
var handler = new FakeCosmosHandler(inMemoryContainer);

var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdA==;",
    new CosmosClientOptions
    {
        HttpClientFactory = () => new HttpClient(handler)
    });

// Seed test data
await inMemoryContainer.CreateItemAsync(
    new { id = "1", partitionKey = "pk1", name = "Widget", price = 9.99 },
    new PartitionKey("pk1"));

// Production code — completely unmodified
var container = client.GetContainer("db", "orders");
var iterator = container
    .GetItemLinqQueryable<dynamic>()
    .Where(x => x.price > 5)
    .ToFeedIterator();  // Works unchanged — intercepted by FakeCosmosHandler

while (iterator.HasMoreResults)
{
    var page = await iterator.ReadNextAsync();
    // process results
}
```

### Approach 2b: Multi-Container Routing

```csharp
var orders = new InMemoryContainer("orders", "/partitionKey");
var customers = new InMemoryContainer("customers", "/partitionKey");

var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
{
    ["orders"] = new FakeCosmosHandler(orders),
    ["customers"] = new FakeCosmosHandler(customers)
});

var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdA==;",
    new CosmosClientOptions
    {
        HttpClientFactory = () => new HttpClient(router)
    });

// Both containers accessible through the same client
var ordersContainer = client.GetContainer("db", "orders");
var customersContainer = client.GetContainer("db", "customers");
```

### Using `InMemoryCosmosClient`

For tests that need `CosmosClient`-level operations (database/container creation):

```csharp
var client = new InMemoryCosmosClient();

var database = client.GetDatabase("myDb");
var container = database.GetContainer("myColl");

// Or use the create-if-not-exists pattern
var dbResponse = await client.CreateDatabaseIfNotExistsAsync("myDb");
var containerResponse = await dbResponse.Database.CreateContainerIfNotExistsAsync("myColl", "/id");
```

## Choosing an Approach

| | Approach 1: `ToFeedIteratorOverridable()` | Approach 2: `FakeCosmosHandler` |
|---|---|---|
| **Production code change** | One token per LINQ call site | None |
| **Query execution** | LINQ-to-Objects | Full SQL parser via HTTP interception |
| **Setup complexity** | Minimal | Moderate (handler wiring) |
| **Fault injection** | No | Yes (429s, 503s, timeouts) |
| **Pagination** | Basic | Realistic with continuation tokens |
| **Query logging** | No | Yes |
| **SDK fidelity** | Lower (bypasses SDK pipeline) | Higher (exercises SDK HTTP pipeline) |
| **Best for** | Unit tests, simple integration tests | Component tests, acceptance tests, CI pipelines |

Both approaches can be used in the same project. They are complementary, not mutually exclusive.

## SQL Query Support

The built-in SQL engine parses and executes Cosmos DB SQL queries against in-memory data. The parser is built with [Superpower](https://github.com/datalust/superpower) parser combinators.

### Clauses

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

### Operators

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

### Built-in Functions

<details>
<summary><strong>String functions</strong></summary>

`UPPER`, `LOWER`, `LTRIM`, `RTRIM`, `TRIM`, `SUBSTRING`, `LENGTH`, `CONCAT`, `CONTAINS`, `STARTSWITH`, `ENDSWITH`, `INDEX_OF`, `REPLACE`, `REVERSE`, `REGEX_MATCH`

</details>

<details>
<summary><strong>Type-checking functions</strong></summary>

`IS_ARRAY`, `IS_BOOL`, `IS_NULL`, `IS_DEFINED`, `IS_NUMBER`, `IS_OBJECT`, `IS_STRING`, `IS_PRIMITIVE`

</details>

<details>
<summary><strong>Math functions</strong></summary>

`ABS`, `CEILING`, `FLOOR`, `ROUND`, `SQRT`, `POWER`, `EXP`, `LOG`, `LOG10`, `SIGN`, `PI`, `ACOS`, `ASIN`, `ATAN`, `TAN`

</details>

<details>
<summary><strong>Array functions</strong></summary>

`ARRAY_CONTAINS`, `ARRAY_SLICE`, `ARRAY_CONCAT`, `ARRAY_LENGTH`

</details>

<details>
<summary><strong>Date/Time functions</strong></summary>

`GetCurrentTimestamp`, `DateTimeBin`, `GetCurrentDateTime`

</details>

<details>
<summary><strong>Conversion functions</strong></summary>

`TOSTRING`, `TONUMBER`, `STRINGTOARRAY`, `STRINGTOBINARY`, `STRINGTOOBJECT`

</details>

<details>
<summary><strong>Aggregate functions</strong></summary>

`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`

</details>

<details>
<summary><strong>Conditional functions</strong></summary>

`IIF`

</details>

### Subqueries

```sql
-- EXISTS
SELECT * FROM c WHERE EXISTS(SELECT VALUE 1 FROM t IN c.tags WHERE t = 'important')

-- ARRAY()
SELECT c.id, ARRAY(SELECT VALUE t FROM t IN c.tags WHERE t != 'draft') AS filteredTags FROM c

-- Scalar subqueries in SELECT and WHERE
SELECT (SELECT VALUE COUNT(1) FROM t IN c.items) AS itemCount FROM c
```

### User-Defined Functions

```csharp
container.RegisterUdf("IsEven", args =>
{
    if (args[0] is not long num) return false;
    return num % 2 == 0;
});

var iterator = container.GetItemQueryIterator<dynamic>(
    new QueryDefinition("SELECT * FROM c WHERE udf.IsEven(c.value)"));
```

### Parameterised Queries

```csharp
var query = new QueryDefinition("SELECT * FROM c WHERE c.status = @status AND c.age > @minAge")
    .WithParameter("@status", "active")
    .WithParameter("@minAge", 21);
```

## Patch Operations

```csharp
var patchOps = new List<PatchOperation>
{
    PatchOperation.Set("/status", "active"),
    PatchOperation.Replace("/name", "Updated Name"),
    PatchOperation.Add("/tags/-", "new-tag"),        // Append to array
    PatchOperation.Remove("/tempField"),
    PatchOperation.Increment("/version", 1)
};

var response = await container.PatchItemAsync<MyDocument>(
    "doc-id", new PartitionKey("pk"), patchOps);
```

Features:
- Deep nested path support (`/address/city/zipCode`)
- Intermediate object creation for deeply nested paths
- ETag-based conditional patching via `IfMatchEtag`
- Filter predicate support via `FilterPredicate`
- Stream variant: `PatchItemStreamAsync`

## Transactional Batches

Atomic multi-operation execution with automatic rollback on failure:

```csharp
var batch = container.CreateTransactionalBatch(new PartitionKey("pk"))
    .CreateItem(new { id = "1", partitionKey = "pk", name = "Alice" })
    .CreateItem(new { id = "2", partitionKey = "pk", name = "Bob" })
    .ReplaceItem("3", updatedItem)
    .PatchItem("4", new[] { PatchOperation.Set("/status", "done") })
    .DeleteItem("5");

var response = await batch.ExecuteAsync();
if (response.IsSuccessStatusCode)
{
    // All operations succeeded atomically
    var alice = response.GetOperationResultAtIndex<dynamic>(0);
}
else
{
    // All operations rolled back — nothing was persisted
}
```

Supported operations: `CreateItem`, `UpsertItem`, `ReplaceItem`, `DeleteItem`, `ReadItem`, `PatchItem`, and stream variants. Enforces Cosmos DB limits (100 operations, 2 MB total size).

## Change Feed

### Change Feed Iterators

```csharp
// Checkpoint-based (recommended)
long checkpoint = container.GetChangeFeedCheckpoint();

await container.CreateItemAsync(item1, new PartitionKey("pk"));
await container.CreateItemAsync(item2, new PartitionKey("pk"));

var iterator = container.GetChangeFeedIterator<MyDocument>(checkpoint);
while (iterator.HasMoreResults)
{
    var changes = await iterator.ReadNextAsync();
    foreach (var change in changes)
        Console.WriteLine(change.Id);
}
```

### Change Feed Processor

```csharp
var processor = container
    .GetChangeFeedProcessorBuilder<MyDocument>(
        "myProcessor",
        async (context, changes, ct) =>
        {
            foreach (var doc in changes)
                Console.WriteLine($"Changed: {doc.Id}");
        })
    .Build();

await processor.StartAsync();
// ... changes will be delivered to the handler ...
await processor.StopAsync();
```

### Start Positions

- `ChangeFeedStartFrom.Beginning()` — all changes since container creation
- `ChangeFeedStartFrom.Now()` — only changes after iterator creation
- `ChangeFeedStartFrom.Time(DateTime)` — changes after a specific timestamp

## ETag & Optimistic Concurrency

```csharp
// Read and capture ETag
var response = await container.ReadItemAsync<MyDoc>("id", new PartitionKey("pk"));
var etag = response.ETag;

// Conditional update — fails with 412 if another write happened
await container.ReplaceItemAsync(updatedDoc, "id", new PartitionKey("pk"),
    new ItemRequestOptions { IfMatchEtag = etag });

// Conditional read — returns 304 if unchanged
await container.ReadItemAsync<MyDoc>("id", new PartitionKey("pk"),
    new ItemRequestOptions { IfNoneMatchEtag = etag });
```

## TTL / Expiration

```csharp
// Container-level default (seconds)
container.DefaultTimeToLive = 3600; // 1 hour

// Per-item override via _ttl property
var item = new { id = "1", partitionKey = "pk", _ttl = 7200, data = "..." };
await container.CreateItemAsync(item, new PartitionKey("pk"));

// Disable TTL
container.DefaultTimeToLive = null;
```

Items are lazily evicted — expired items are removed on the next read attempt.

## Stored Procedures

```csharp
container.RegisterStoredProcedure("bulkDelete", (pk, args) =>
{
    // Custom logic with access to partition key and arguments
    return JsonConvert.SerializeObject(new { deleted = 42 });
});

var response = await container.Scripts.ExecuteStoredProcedureAsync<string>(
    "bulkDelete", new PartitionKey("pk"), new dynamic[] { "arg1" });
```

## State Persistence

Export and import container state for test data seeding, debugging, or snapshots:

```csharp
// Export
string json = container.ExportState();
container.ExportStateToFile("test-data/snapshot.json");

// Import (replaces all data)
container.ImportState(json);
container.ImportStateFromFile("test-data/snapshot.json");

// Utilities
int count = container.ItemCount;
container.ClearItems();
```

## Fault Injection

Simulate transient failures and throttling (Approach 2 only):

```csharp
var handler = new FakeCosmosHandler(container);

handler.FaultInjector = request =>
{
    // Simulate 429 throttling on write operations
    if (request.Method == HttpMethod.Post)
    {
        return new HttpResponseMessage((HttpStatusCode)429)
        {
            Headers = { RetryAfter = new RetryConditionHeaderValue(TimeSpan.FromMilliseconds(1)) }
        };
    }
    return null; // No fault — proceed normally
};

// Don't inject faults on SDK metadata requests
handler.FaultInjectorIncludesMetadata = false;
```

## FakeCosmosHandler Options

```csharp
var options = new FakeCosmosHandlerOptions
{
    CacheTtl = TimeSpan.FromMinutes(5),    // Query result cache TTL
    CacheMaxEntries = 100,                  // Max cached result sets
    PartitionKeyRangeCount = 4              // Simulated partition ranges
};

var handler = new FakeCosmosHandler(container, options);
```

## SDK Compatibility Verification

Detect breaking changes in the Cosmos SDK early in your CI pipeline:

```csharp
// Call once during test suite setup
await FakeCosmosHandler.VerifySdkCompatibilityAsync();
```

## Comparison with Official Alternatives

| Feature | CosmosDB.InMemoryEmulator | [Official Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator) | Real Azure Cosmos DB |
|---------|:---:|:---:|:---:|
| Pricing | ✅ Free | ✅ Free | ❌ Pay-per-RU + storage |
| In-process (no external deps) | ✅ | ❌ | ❌ Requires network |
| SQL query parser | ✅ (40+ functions) | ✅ | ✅ Full |
| `GROUP BY` / `HAVING` | ✅ | ✅ | ✅ |
| Subqueries / `EXISTS` | ✅ | ✅ | ✅ |
| `JOIN` (array expansion) | ✅ Unlimited | ⚠️ Max 5 per query | ✅ |
| Transactional batches | ✅ | ✅ | ✅ |
| Change feed | ✅ | ✅ | ✅ |
| Patch operations | ✅ | ✅ | ✅ |
| ETag concurrency | ✅ | ✅ | ✅ |
| TTL / expiration | ✅ | ✅ | ✅ |
| Stored procedures / UDFs | ✅ (C# handlers) | ✅ (JavaScript) | ✅ (JavaScript) |
| Fault injection | ✅ | ❌ | ❌ |
| Stream API | ✅ | ✅ | ✅ |
| LINQ integration | ✅ | ✅ | ✅ |
| ReadMany | ✅ | ✅ | ✅ |
| Composite partition keys | ✅ | ✅ | ✅ |
| State export/import | ✅ | ❌ | ❌ |
| Multi-container routing | ✅ | ✅ | ✅ |
| Unlimited containers | ✅ | ⚠️ Degrades past 10 fixed / 5 unlimited | ✅ |
| All consistency levels | ✅ (simulated) | ⚠️ Session & Strong only | ✅ |
| Serverless throughput mode | ✅ (no RU enforcement) | ❌ Provisioned only | ✅ |
| Multiple instances / parallel | ✅ One per test | ❌ Single instance only | ⚠️ Shared state; needs cleanup |
| Custom auth keys | ✅ Any / none | ⚠️ Well-known key; restart to change | ✅ Managed keys |
| Item ID length | ✅ Unrestricted | ⚠️ Max 254 characters | ✅ |
| Fast startup | ✅ Instant | ❌ 10-30s | ❌ Provisioning minutes |
| CI-friendly | ✅ | ⚠️ Flaky | ⚠️ Needs secrets, network, costs |
| Test isolation | ✅ New instance per test | ⚠️ Shared state | ❌ Shared; risk of data leakage |
| Stable under load | ✅ No sockets/network | ⚠️ Socket exhaustion, 407s, hangs under high throughput | ✅ (at cost) |
| No CPU/corruption issues | ✅ Pure in-memory | ⚠️ Can spike CPU, enter corrupted "Service Unavailable" state | ✅ |
| Linux / Docker reliable | ✅ Any platform | ⚠️ Docker image fails on some CPUs; high CPU; no bulk execution on Linux | ✅ Any platform |
| No special reset needed | ✅ Dispose and recreate | ⚠️ May need "Reset Data" or `/DisableRIO` flag to recover | ⚠️ Manual cleanup between tests |
| Works offline | ✅ | ✅ | ❌ Requires internet |
| Actively maintained | ✅ | ✅ | ✅ |

## Comparison with Community Alternatives

| Feature | CosmosDB.InMemoryEmulator | [FakeCosmosDb](https://github.com/timabell/FakeCosmosDb) | [FakeCosmosEasy](https://github.com/rentready/fake-cosmos-easy) |
|---------|:---:|:---:|:---:|
| Pricing | ✅ Free | ✅ Free | ✅ Free |
| In-process (no external deps) | ✅ | ✅ | ✅ |
| SQL query parser | ✅ (40+ functions) | ⚠️ Basic | ⚠️ Basic |
| `GROUP BY` / `HAVING` | ✅ | ❌ | ❌ |
| Subqueries / `EXISTS` | ✅ | ❌ | ❌ |
| `JOIN` (array expansion) | ✅ Unlimited | ❌ | ❌ |
| Transactional batches | ✅ | ❌ | ❌ |
| Change feed | ✅ | ❌ | ❌ |
| Patch operations | ✅ | ❌ | ❌ |
| ETag concurrency | ✅ | ❌ | ❌ |
| TTL / expiration | ✅ | ❌ | ❌ |
| Stored procedures / UDFs | ✅ (C# handlers) | ❌ | ❌ |
| Fault injection | ✅ | ❌ | ❌ |
| Stream API | ✅ | ❌ | ❌ |
| LINQ integration | ✅ | ❌ | ❌ |
| ReadMany | ✅ | ❌ | ❌ |
| Composite partition keys | ✅ | ❌ | ❌ |
| State export/import | ✅ | ❌ | ❌ |
| Multi-container routing | ✅ | ❌ | ❌ |
| Unlimited containers | ✅ | ✅ | ✅ |
| All consistency levels | ✅ (simulated) | ❌ | ❌ |
| Serverless throughput mode | ✅ (no RU enforcement) | ✅ | ✅ |
| Multiple instances / parallel | ✅ One per test | ✅ | ✅ |
| Custom auth keys | ✅ Any / none | ✅ | ✅ |
| Item ID length | ✅ Unrestricted | ✅ | ✅ |
| Fast startup | ✅ Instant | ✅ Instant | ✅ Instant |
| CI-friendly | ✅ | ✅ | ✅ |
| Test isolation | ✅ New instance per test | ✅ | ✅ |
| Stable under load | ✅ No sockets/network | ✅ | ✅ |
| No CPU/corruption issues | ✅ Pure in-memory | ✅ | ✅ |
| Linux / Docker reliable | ✅ Any platform | ✅ | ✅ |
| No special reset needed | ✅ Dispose and recreate | ✅ | ✅ |
| Works offline | ✅ | ✅ | ✅ |
| Actively maintained | ✅ | ✅ | ❌ (3 years stale) |

## Known Limitations & Behavioral Differences

| Area | Status | Notes |
|------|--------|-------|
| Deletes in change feed | ❌ Not recorded | Deletions don't appear in the change feed (known gap) |
| Spatial functions | ❌ Not implemented | `ST_DISTANCE`, `ST_WITHIN`, etc. not available |
| Cross-partition transactions | ❌ Not supported | Batches are single-partition only (matches Cosmos DB) |
| Analytical store (Synapse) | ❌ Not simulated | OLAP context not available |
| Continuous backup / PITR | ❌ Not simulated | Point-in-time restore not available |
| IndexingPolicy | ⚠️ Stub | Accepted and stored but doesn't affect query performance |
| TTL eviction | ⚠️ Lazy | Expired items removed on next read, not proactively |
| Resource IDs | ⚠️ Synthetic | Valid format but doesn't match real Cosmos RIDs |
| Throughput (RU/s) | ⚠️ Synthetic | Returns 400 RU/s; doesn't affect behavior |
| `FeedRange` filtering | ⚠️ Not implemented | Accepted but currently ignored |

## Project Structure

```
src/
  CosmosDB.InMemoryEmulator/
    InMemoryContainer.cs              Core Container implementation
    InMemoryCosmosClient.cs           CosmosClient implementation
    InMemoryDatabase.cs               Database implementation
    FakeCosmosHandler.cs              HTTP-level SDK interception
    CosmosSqlParser.cs                SQL parser (Superpower-based)
    InMemoryTransactionalBatch.cs     Transactional batch support
    InMemoryChangeFeedProcessor.cs    Change feed processor
    InMemoryFeedIterator.cs           Paginated result iterator
    InMemoryFeedIteratorSetup.cs      Feed iterator factory registration
    CosmosOverridableFeedIteratorExtensions.cs  .ToFeedIteratorOverridable()
    JsonParseHelpers.cs               JSON utility methods
tests/
  CosmosDB.InMemoryEmulator.Tests/    800+ tests
  CosmosDB.InMemoryEmulator.Tests.Performance/  Load/performance tests
```

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos) | 3.57.1 | Azure Cosmos DB SDK |
| [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json) | 13.0.3 | JSON serialization |
| [NSubstitute](https://www.nuget.org/packages/NSubstitute) | 5.3.0 | Internal mocking |
| [Superpower](https://www.nuget.org/packages/Superpower) | 3.1.0 | Parser combinators for SQL engine |

## License

[Apache License 2.0](LICENSE)
