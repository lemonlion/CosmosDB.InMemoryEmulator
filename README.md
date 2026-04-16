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

Ideal with highest fidelity:

```csharp
// 1. Create an InMemoryContainer as the backing store
var container = new InMemoryContainer("my-container", "/partitionKey");

// 2. Wire up FakeCosmosHandler → CosmosClient
using var handler = new FakeCosmosHandler(container);
using var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        LimitToEndpoint = true,
        HttpClientFactory = () => new HttpClient(handler)
    });

// 3. Use the real SDK — all calls are intercepted by the handler
var cosmosContainer = client.GetContainer("db", "my-container");
```

Alternatively - the following to are slightly more limited usages, but still fully functional.
They require the use of `.ToFeedIteratorOverrideable()` wherever `.ToFeedIterator()` is used, 
and have some minor differences (e.g. use of LINQ to objects for querying) - [see here for more details](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Integration-Approaches).

```csharp
var cosmosClient = new InMemoryCosmosClient();
```
OR
```csharp
var cosmosContainer = new InMemoryContainer();
```

## Motivation

Designed for super fast feedback from your Integration/Component tests in a local or CI environment, to avoid relying completely on the official Cosmos emulator or official Cosmos DB or inaccurate high level abstractions. 

| Traditional Approach | Problem |
|----------|---------|
| **[Official Cosmos DB Emulator](https://learn.microsoft.com/en-us/azure/cosmos-db/emulator)** | Heavy process, slow startup, poor performance, limited feature set, unreliable in CI |
| **Real Azure Cosmos DB** | Slower, costly, requires network, authentication overhead, shared state between test runs |
| **Repository Abstraction Layer** | Fragile, doesn't test query logic, misses serialization bugs |

Recommendation is to use **CosmosDB.InMemoryEmulator** for integration/component testing locally and in CI for quick feedback and iteration, while still having the integration/component tests *additionally* running in CI against the official out of process emulator for (10x) slower feedback.

See the **[Feature Comparison With Alternatives](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feature-Comparison-With-Alternatives)** for a detailed side-by-side breakdown.

## Features

- **Full CRUD** — typed and stream variants with proper status codes and ETags
- **Feature Complete SQL query engine** — 125+ built-in functions, `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `HAVING`, `JOIN`, `TOP`, `DISTINCT`, `OFFSET/LIMIT`, subqueries, full-text search, vector search
- **Full LINQ support** — `GetItemLinqQueryable<T>()` with `.ToFeedIterator()` interception
- **Triggers** — pre-trigger and post-trigger execution via C# handlers, with optional JavaScript body interpretation via the `CosmosDB.InMemoryEmulator.JsTriggers` package
- **Bulk operations** — `AllowBulkExecution = true` with concurrent `Task.WhenAll` patterns
- **Transactional batches** — atomic execution with rollback on failure
- **Change feed** — iterators, checkpoints, delete tombstones, `ChangeFeedProcessor`, and manual checkpoint processor
- **Point-in-time restore** — restore a container to any previous point in time via change feed replay
- **Partition keys** — single and composite, auto-extraction from documents
- **State persistence** — export/import container state as JSON; automatic save/restore between test runs via `StatePersistenceDirectory`
- **TTL / expiration** — container-level and per-item with lazy eviction
- **ETag / optimistic concurrency** — `IfMatchEtag`, `IfNoneMatchEtag`, wildcard `*`
- **System metadata** — `_ts` and `_etag` injected into stored items, matching real Cosmos DB
- **Patch operations** — all 6 types with deep nested paths and filter predicates
- **Fault injection** — simulate 429 throttling, 503 errors, timeouts
- **DI integration** — `UseInMemoryCosmosDB()` extension methods for `IServiceCollection`
- **HTTP-level interception** — `FakeCosmosHandler` for zero-code-change integration
- **Custom handler wrapping** — insert `DelegatingHandler` middleware (logging, tracking, metrics) via `HttpMessageHandlerWrapper`
- **Unique key policies** — constraint enforcement on Create, Upsert, Replace, and Patch (typed and stream)
- **FeedRange support** — configurable `FeedRangeCount` with scoped queries and change feed iterators
- **Vector search** — `VECTORDISTANCE` with cosine, dot product, and Euclidean distance; works in `SELECT`, `WHERE`, and `ORDER BY`
- **Users & permissions** — stub user/permission CRUD with synthetic tokens (no authorization enforced)
- **Computed properties** — virtual container-level properties evaluated at query time; usable in `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `DISTINCT`, `HAVING`, with aliases, aggregates, and all expression types
- **7500+ tests** covering all features and performance, ensuring feature consistency and parity with real CosmosDB

For the full feature list see [Features](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Features). For a side-by-side comparison with the official Microsoft emulator see [Feature Comparison With Alternatives](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feature-Comparison-With-Alternatives). For behavioural differences from a real CosmosDB see [Known Limitations](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Known-Limitations)

## NuGet Packages

| Framework | Package | Description | NuGet |
|---|---|---|---|
| **Core library** | `CosmosDB.InMemoryEmulator` | Primary Features | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator) |
| **JavaScript Triggers** | `CosmosDB.InMemoryEmulator.JsTriggers` | Support for JS Triggers | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator.JsTriggers) |
| **Production Extensions** | `CosmosDB.InMemoryEmulator.ProductionExtensions` | Support for use of the *optional* `.ToFeedIteratorOverridable()` alternative to the native `.ToFeedIterator()`* | [![NuGet Version](https://img.shields.io/nuget/v/CosmosDB.InMemoryEmulator)](https://www.nuget.org/packages/CosmosDB.InMemoryEmulator.ProductionExtensions) |

* Native `.ToFeedIterator()` method works without any problems, there are just occasionally some advantages to using `.ToFeedIteratorOverridable()`, hence why this optional package is supplied.  See [Feed Iterator Usage](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki/Feed-Iterator-Usage-Guide).

## Documentation

Full documentation is available on the **[Wiki](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki)**.

## Emulator Parity Validation

The test suite includes infrastructure to validate that the in-memory implementation produces identical results to the real Cosmos DB emulator. Integration tests use the same SDK HTTP pipeline as a real emulator, making comparison meaningful.

### Test Project Structure

| Project | Description |
|---------|-------------|
| `Tests.Unit` | Direct `InMemoryContainer`/`InMemoryCosmosClient` tests — fast, in-memory only |
| `Tests.Integration` | `FakeCosmosHandler` tests via `ITestContainerFixture` — run against in-memory, Linux emulator, or Windows emulator |
| `Tests.Shared` | Shared infrastructure (fixtures, traits, models) used by both projects |

### Quick Start

```powershell
# Run full parity validation (starts emulator, runs integration tests, compares)
.\scripts\validate-parity.ps1

# Run only CRUD tests
.\scripts\validate-parity.ps1 -Filter "FullyQualifiedName~Crud"

# Run unit tests only
.\scripts\run-tests.ps1 -Target inmemory -Project unit

# Run integration tests against emulator
.\scripts\run-tests.ps1 -Target emulator-linux -Project integration
```

### Environment Variable

Set `COSMOS_TEST_TARGET` to switch test backends:

| Value | Backend |
|-------|---------|
| `inmemory` (default) | FakeCosmosHandler + InMemoryContainer |
| `emulator-linux` | Docker legacy `azure-cosmos-emulator:latest` |

### Test Traits

Tests are categorized with xUnit traits:

| Trait | Meaning |
|-------|---------|
| `Target=InMemoryOnly` | Uses in-memory-only APIs (BackingContainer, FaultInjector, etc.) — skipped on emulator |
| `Target=KnownDivergence` | Documents known behavioral differences |
| *(no Target trait)* | Parity test — runs against both backends |

### Scripts

| Script | Purpose |
|--------|---------|
| `scripts/validate-parity.ps1` | One-command orchestrator |
| `scripts/start-emulator.ps1` | Start Docker emulator |
| `scripts/run-tests.ps1` | Run tests with a given target and project |
| `scripts/compare-trx.ps1` | Compare TRX files and output parity report |


### CI

The `emulator-parity.yml` workflow runs weekly (Monday 6am UTC) or on manual trigger, executing integration tests against all backends and producing a parity report in the GitHub Actions step summary.

## Dependencies

| Package | Purpose |
|---------|---------|
| [Microsoft.Azure.Cosmos](https://www.nuget.org/packages/Microsoft.Azure.Cosmos) | Azure Cosmos DB SDK |
| [Newtonsoft.Json](https://www.nuget.org/packages/Newtonsoft.Json) | JSON serialization |
| [NSubstitute](https://www.nuget.org/packages/NSubstitute) | Internal mocking for SDK response types |
| [Superpower](https://www.nuget.org/packages/Superpower) | Parser combinators for SQL engine |

## License

[MIT License](LICENSE)
