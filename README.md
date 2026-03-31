# CosmosDB.InMemoryEmulator

[![NuGet](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator.svg)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator)
[![NuGet Downloads](https://img.shields.io/nuget/dt/CosmosDB.InMemoryEmulator.svg)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator)
[![CI](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/actions/workflows/ci.yml/badge.svg)](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/actions/workflows/ci.yml)

A high-fidelity, in-memory implementation of the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

> **Drop-in replacements for `CosmosClient`, `Database`, and `Container`** — full CRUD, SQL queries, LINQ, patch, batches, change feed, and more. No network, no emulator process, no Docker, no Azure subscription required.
<br>

## Full Support For `.ToFeedIterator()` — Zero Production Code Changes

Using the [`FakeCosmosHandler`](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Integration-Approaches#fakecosmoshandler-high-fidelity) approach, **your production code stays completely untouched** — including calls to `.ToFeedIterator()` and `GetItemLinqQueryable<T>()`. The handler intercepts HTTP traffic at the SDK level, so LINQ queries, SQL queries, CRUD operations, and all other Cosmos operations work exactly as written.

```csharp
// Your production code — works against real Cosmos DB AND the in-memory emulator, unmodified
var results = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();  // ← works as-is, no changes needed
```

> **How?** `FakeCosmosHandler` is a custom `HttpMessageHandler` that intercepts the Cosmos SDK's HTTP pipeline. The SDK translates your LINQ expression into Cosmos SQL, sends it over HTTP, and the handler executes that SQL against an in-memory store. Your production code never knows the difference. See the [Feed Iterator Usage Guide](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feed-Iterator-Usage-Guide) for details.
<br>

## Why This Exists

| Approach | Problem |
|----------|---------|
| **[Official Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator)** | Heavy process, slow startup, port conflicts, unreliable in CI |
| **Mocking `CosmosClient` directly** | Fragile, doesn't test query logic, misses serialization bugs |
| **Real Azure Cosmos DB** | Slow, costly, requires network, shared state between test runs |

**CosmosDB.InMemoryEmulator** fills the gap with in-memory speed and zero external dependencies.

## Features

- **Full CRUD** — typed and stream variants with proper status codes and ETags
- **System metadata** — `_ts` and `_etag` injected into stored items, matching real Cosmos DB
- **Rich SQL query engine** — 100+ built-in functions, `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `TOP`, `DISTINCT`, `OFFSET/LIMIT`, subqueries, full-text search
- **LINQ support** — `GetItemLinqQueryable<T>()` with `.ToFeedIterator()` interception
- **Transactional batches** — atomic execution with rollback on failure
- **Change feed** — iterators, checkpoints, delete tombstones, and `ChangeFeedProcessor`
- **Patch operations** — all 5 types with deep nested paths and filter predicates
- **ETag / optimistic concurrency** — `IfMatchEtag`, `IfNoneMatchEtag`, wildcard `*`
- **Partition keys** — single and composite, auto-extraction from documents
- **TTL / expiration** — container-level and per-item with lazy eviction
- **Fault injection** — simulate 429 throttling, 503 errors, timeouts
- **DI integration** — `UseInMemoryCosmosDB()` extension methods for `IServiceCollection`
- **State persistence** — export/import container state as JSON
- **Point-in-time restore** — restore a container to any previous point in time via change feed replay
- **HTTP-level interception** — `FakeCosmosHandler` for zero-code-change integration
- **Unique key policies** — constraint enforcement on Create, Upsert, Replace, and Patch (typed and stream)
- **Triggers** — pre-trigger and post-trigger execution via C# handlers
- **1237 tests** covering all features (28 skipped — see [Known Limitations](../../wiki/Known-Limitations))

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
| **CRUD operations** | Yes | **Yes** | Yes |
| **Fidelity** | Good | **Highest** | Good |
| **Fault injection** | No | **Yes** | No |
| **Best for** | Unit tests | Component/acceptance tests | Integration tests with DI |

¹ `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`. Not needed if you don't use LINQ `.ToFeedIterator()`.

## Documentation

| Guide | Description |
|-------|-------------|
| **[Getting Started](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Getting-Started)** | Installation, quick start, first test |
| **[Integration Approaches](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Integration-Approaches)** | Detailed comparison of all three approaches with pros/cons |
| **[Dependency Injection](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Dependency-Injection)** | Step-by-step DI setup for all patterns |
| **[Feed Iterator Usage](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feed-Iterator-Usage-Guide)** | Making `.ToFeedIterator()` work — `FakeCosmosHandler` vs `ToFeedIteratorOverridable()` |
| **[SQL Queries](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/SQL-Queries)** | Full SQL reference — clauses, operators, 100+ functions |
| **[Features](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Features)** | Patch, batches, change feed, ETags, TTL, stored procs, state persistence, PITR |
| **[API Reference](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/API-Reference)** | Complete class and method reference |
| **[Known Limitations](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Known-Limitations)** | Behavioural differences from real Cosmos DB |
| **[Troubleshooting](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Troubleshooting)** | Common errors and how to fix them |
| **[Comparison](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Comparison)** | vs Official Emulator, vs community alternatives |

## Dependencies

| Package | Purpose |
|---------|---------|
| [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos) | Azure Cosmos DB SDK |
| [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json) | JSON serialization |
| [NSubstitute](https://www.nuget.org/packages/NSubstitute) | Internal mocking for SDK response types |
| [Superpower](https://www.nuget.org/packages/Superpower) | Parser combinators for SQL engine |

## License

[Apache License 2.0](LICENSE)
