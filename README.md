# CosmosDB.InMemoryEmulator

A high-fidelity, in-memory implementation of the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

> **Drop-in replacement for `Microsoft.Azure.Cosmos.Container`** — no network, no emulator process, no Docker, no Azure subscription required.

## Zero Production Code Changes Required

Using the `FakeCosmosHandler` approach, **your production code stays completely untouched** — including calls to `.ToFeedIterator()` and `GetItemLinqQueryable<T>()`. The handler intercepts HTTP traffic at the SDK level, so LINQ queries, SQL queries, and all other Cosmos operations work exactly as written.

```csharp
// Your production code — works against real Cosmos DB AND the in-memory emulator, unmodified
var results = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();  // ← works as-is, no changes needed
```

## Why This Exists

| Approach | Problem |
|----------|---------|
| **[Official Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator)** | Heavy process, slow startup, port conflicts, unreliable in CI |
| **Mocking `CosmosClient` directly** | Fragile, doesn't test query logic, misses serialization bugs |
| **Real Azure Cosmos DB** | Slow, costly, requires network, shared state between test runs |

**CosmosDB.InMemoryEmulator** fills the gap with in-memory speed and zero external dependencies.

## Features

- **Full CRUD** — typed and stream variants with proper status codes and ETags
- **Rich SQL query engine** — 40+ built-in functions, `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `TOP`, `DISTINCT`, `OFFSET/LIMIT`, subqueries
- **LINQ support** — `GetItemLinqQueryable<T>()` with `.ToFeedIterator()` interception
- **Transactional batches** — atomic execution with rollback on failure
- **Change feed** — iterators, checkpoints, and `ChangeFeedProcessor`
- **Patch operations** — all 5 types with deep nested paths and filter predicates
- **ETag / optimistic concurrency** — `IfMatchEtag`, `IfNoneMatchEtag`, wildcard `*`
- **Partition keys** — single and composite, auto-extraction from documents
- **TTL / expiration** — container-level and per-item with lazy eviction
- **Fault injection** — simulate 429 throttling, 503 errors, timeouts
- **DI integration** — `UseInMemoryCosmosDB()` extension methods for `IServiceCollection`
- **State persistence** — export/import container state as JSON
- **HTTP-level interception** — `FakeCosmosHandler` for zero-code-change integration
- **900+ tests** covering all features

## Installation

```
dotnet add package CosmosDB.InMemoryEmulator
```

**Target framework:** .NET 8.0 | **Cosmos SDK:** Microsoft.Azure.Cosmos 3.58.0+

## Quick Start

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

var container = new InMemoryContainer("my-container", "/partitionKey");

// Create
await container.CreateItemAsync(
    new { id = "1", partitionKey = "pk1", name = "Alice" },
    new PartitionKey("pk1"));

// Query
var iterator = container.GetItemQueryIterator<dynamic>(
    "SELECT * FROM c WHERE c.name = 'Alice'");
var page = await iterator.ReadNextAsync();
```

## Integration Approaches

| | Direct `InMemoryContainer` | `FakeCosmosHandler` | DI Extensions |
|---|---|---|---|
| **Production code changes** | One token per LINQ call site¹ | **None** | None |
| **Fidelity** | Good | **Highest** | Good |
| **Fault injection** | No | **Yes** | No |
| **Best for** | Unit tests | Component/acceptance tests | Integration tests with DI |

¹ `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`. Not needed if you don't use LINQ `.ToFeedIterator()`.

## Documentation

| Guide | Description |
|-------|-------------|
| **[Getting Started](docs/getting-started.md)** | Installation, quick start, first test |
| **[Integration Approaches](docs/integration-approaches.md)** | Detailed comparison of all three approaches with pros/cons |
| **[Dependency Injection](docs/dependency-injection.md)** | Step-by-step DI setup for all patterns |
| **[SQL Queries](docs/sql-queries.md)** | Full SQL reference — clauses, operators, 40+ functions |
| **[Features](docs/features.md)** | Patch, batches, change feed, ETags, TTL, stored procs, state persistence |
| **[API Reference](docs/api-reference.md)** | Complete class and method reference |
| **[Known Limitations](docs/known-limitations.md)** | Behavioural differences from real Cosmos DB |
| **[Comparison](docs/comparison.md)** | vs Official Emulator, vs community alternatives |

## Dependencies

| Package | Purpose |
|---------|---------|
| [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos) | Azure Cosmos DB SDK |
| [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json) | JSON serialization |
| [NSubstitute](https://www.nuget.org/packages/NSubstitute) | Internal mocking for SDK response types |
| [Superpower](https://www.nuget.org/packages/Superpower) | Parser combinators for SQL engine |

## License

[Apache License 2.0](LICENSE)