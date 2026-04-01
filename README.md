# CosmosDB.InMemoryEmulator

[![NuGet](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator.svg)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator)
[![NuGet Downloads](https://img.shields.io/nuget/dt/CosmosDB.InMemoryEmulator.svg)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator)
[![CI](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/actions/workflows/ci.yml/badge.svg)](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/actions/workflows/ci.yml)

A fully featured, in-process, fake for Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.  Runs in memory on the fly for the lifetime of your test run (although persistence between runs is available as a feature).

Has full support for *all* Cosmos CRUD and SQL querying, including raw querying, functions, LINQ querying (including `GetItemLinqQueryable<T>()` with `.ToFeedIterator()` support - no production code changes necessary).

Includes all the other features of CosmosDB like Triggers, Stored Procedures, Change Feed, Transactional Batches, Bulk Operations, Point-in-time restore, TTL...

Client encryption keys are deliberately not implemented as they require Azure Key Vault integration and are out of scope for a testing fake.

## Usage

Works by replacing either `Microsoft.Azure.Cosmos.Container` or `Microsoft.Azure.Cosmos.CosmosClient`.

### Dependency Injection

In your `ConfigureTestServices()` method in your `WebApplicationFactory()`:

```csharp
serviceCollection.UseInMemoryCosmosDB(); // Replaces all Cosmos Clients and Containers — highest fidelity, zero production code changes
```
OR
```csharp
serviceCollection.UseInMemoryCosmosContainers(); // Replaces only Cosmos Containers (lower fidelity, needs .ToFeedIteratorOverridable() for LINQ)
```

### Direct Instantiation

```csharp
var cosmosClient = new InMemoryCosmosClient(); // Fully functional In-Memory Cosmos Client Emulator
```
OR
```csharp
var cosmosClient = new InMemoryContainer(); // Fully functional In-Memory Cosmos Container Emulator
```

## Motivation

Designed for super fast feedback from your Integration/Component tests in a local or CI environment, to avoid relying completely on the official Cosmos emulator or official Cosmos DB or inaccurate high level abstractions. 

| Traditional Approach | Problem |
|----------|---------|
| **[Official Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator)** | Heavy process, slow startup, poor performance, limited feature set, unreliable in CI |
| **Real Azure Cosmos DB** | Slower, costly, requires network, authentication overhead, shared state between test runs |
| **Repository Abstraction Layer** | Fragile, doesn't test query logic, misses serialization bugs |

Recommendation is to use **CosmosDB.InMemoryEmulator** for integration/component testing locally and in CI for quick feedback and iteration, while still having the integration/component tests *additionally* running in CI against the official out of process emulator for (10x) slower feedback.

## Features

- **Full CRUD** — typed and stream variants with proper status codes and ETags
- **Feature Complete SQL query engine** — 100+ built-in functions, `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `TOP`, `DISTINCT`, `OFFSET/LIMIT`, subqueries, full-text search, vector search
- **Full LINQ support** — `GetItemLinqQueryable<T>()` with `.ToFeedIterator()` interception
- **Triggers** — pre-trigger and post-trigger execution via C# handlers, with optional JavaScript body interpretation via the `CosmosDB.InMemoryEmulator.JsTriggers` package
- **Bulk operations** — `AllowBulkExecution = true` with concurrent `Task.WhenAll` patterns
- **Transactional batches** — atomic execution with rollback on failure
- **Change feed** — iterators, checkpoints, delete tombstones, and `ChangeFeedProcessor`
- **Point-in-time restore** — restore a container to any previous point in time via change feed replay
- **Partition keys** — single and composite, auto-extraction from documents
- **State persistence** — export/import container state as JSON
- **TTL / expiration** — container-level and per-item with lazy eviction
- **ETag / optimistic concurrency** — `IfMatchEtag`, `IfNoneMatchEtag`, wildcard `*`
- **System metadata** — `_ts` and `_etag` injected into stored items, matching real Cosmos DB
- **Patch operations** — all 5 types with deep nested paths and filter predicates
- **Fault injection** — simulate 429 throttling, 503 errors, timeouts
- **DI integration** — `UseInMemoryCosmosDB()` extension methods for `IServiceCollection`
- **HTTP-level interception** — `FakeCosmosHandler` for zero-code-change integration
- **Unique key policies** — constraint enforcement on Create, Upsert, Replace, and Patch (typed and stream)
- **FeedRange support** — configurable `FeedRangeCount` with scoped queries and change feed iterators
- **Vector search** — `VECTORDISTANCE` with cosine, dot product, and Euclidean distance; works in `SELECT`, `WHERE`, and `ORDER BY`
- **Users & permissions** — stub user/permission CRUD with synthetic tokens (no authorization enforced)
- **1350+ tests** covering all features and performance

For behavioural differences from a real CosmosDB see [Known Limitations](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Known-Limitations)

## NuGet Packages

| Framework | Package | Description | NuGet |
|---|---|---|---|
| **Core library** | `CosmosDB.InMemoryEmulator` | Primary Features | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator) |
| **JavaScript Triggers** | `CosmosDB.InMemoryEmulator.JsTriggers` | Support for JS Triggers | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator.JsTriggers) |
| **Production Extensions** | `CosmosDB.InMemoryEmulator.ProductionExtensions` | Support for use of the *optional* `.ToFeedIteratorOverridable()` alternative to the native `.ToFeedIterator()`* | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator.ProductionExtensions) |

* Native `.ToFeedIterator()` method works without any problems, there are just occasionally some advantages to using `.ToFeedIteratorOverridable()`, hence why this optional package is supplied.  See [Feed Iterator Usage](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feed-Iterator-Usage-Guide).

All packages support .NET 8.0+.  .NET 10 specific packages will be created before .NET 8.0 support ends, but it is being deliberately held off to avoid having to fully maintain multiple packages targeting different .NET frameworks, as until then there is little benefit having both.

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

[MIT License](LICENSE)