# Feed Iterator Usage Guide

This guide explains the two approaches for making `.ToFeedIterator()` work with `InMemoryContainer` in component tests, how they work internally, their trade-offs, and when to use each.

---

## The problem

The Cosmos SDK's `.ToFeedIterator()` only works on queryables created by the SDK's own LINQ provider (`CosmosLinqQueryProvider`). When you use `InMemoryContainer` in component tests, `GetItemLinqQueryable<T>()` returns a standard LINQ-to-Objects queryable (`EnumerableQuery<T>`). Calling `.ToFeedIterator()` on that throws:

```
ArgumentOutOfRangeException: ToFeedIterator is only supported on Cosmos LINQ query operations
```

There are two independent approaches to solve this. They can be used separately or together.

---

## Approach 1: `.ToFeedIterator()` via `FakeCosmosHandler`

**Production code uses the standard SDK `.ToFeedIterator()` — no changes required.**

### How it works

1. The SDK translates the LINQ expression tree into SQL.
2. The SDK serialises the SQL as an HTTP POST to the Cosmos endpoint.
3. `FakeCosmosHandler` — wired in via `CosmosClientOptions.HttpClientFactory` — intercepts all HTTP traffic.
4. The handler parses the SQL (via `CosmosSqlParser`), executes it against `InMemoryContainer`, and returns paged JSON responses with continuation tokens, request charges, and partition key range headers.

The interception is at the HTTP level, bound to the `CosmosClient` instance. There is no ambient state, no `AsyncLocal`, and no `ExecutionContext` dependency.

### Test wiring (~10 lines)

```csharp
var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
var handler = new FakeCosmosHandler(inMemoryContainer);
var cosmosClient = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        LimitToEndpoint = true,
        MaxRetryAttemptsOnRateLimitedRequests = 0,
        RequestTimeout = TimeSpan.FromSeconds(5),
        HttpClientFactory = () => new HttpClient(handler)
        {
            Timeout = TimeSpan.FromSeconds(10)
        }
    });
var container = cosmosClient.GetContainer("fakeDb", "fakeContainer");
```

Critical settings:
- **`ConnectionMode.Gateway`** — forces all traffic through HTTP (not TCP direct mode).
- **`LimitToEndpoint = true`** — prevents the SDK from trying to discover other endpoints.
- **`MaxRetryAttemptsOnRateLimitedRequests = 0`** — disables retries for clean fault injection testing.

### Production code (unchanged)

```csharp
var iterator = container
    .GetItemLinqQueryable<MyEntity>()
    .Where(item => item.IsActive)
    .ToFeedIterator();  // standard SDK method — no changes

while (iterator.HasMoreResults)
{
    var response = await iterator.ReadNextAsync();
    // process response
}
```

### What the handler does with queries

When the SDK calls `.ToFeedIterator()`, the LINQ expression tree is translated to SQL internally, then sent as a POST to `/docs` with the SQL in the request body. `FakeCosmosHandler.HandleQueryAsync` then:

1. Reads the SQL from the POST body.
2. Extracts partition key, max item count, continuation token, and range ID from HTTP headers.
3. Checks the query result cache (for pagination continuations).
4. Parses via `CosmosSqlParser.TryParse()`:
   - **ORDER BY queries**: Detected structurally (the SDK wraps ORDER BY in a special `[{"item": <expr>}]` array format). Handled by `HandleOrderByQueryAsync` which re-wraps results with `orderByItems` and `payload` aliases.
   - **Regular queries**: Simplified via `CosmosSqlParser.SimplifySdkQuery()` to strip SDK-internal decorations, then delegated to `InMemoryContainer.GetItemQueryIterator<JToken>()`.
5. Filters by partition key range (for multi-range fan-out testing).
6. Builds a paged HTTP response via `BuildPagedResponse` with `x-ms-continuation` headers.

### Key files

- `src/CosmosDB.InMemoryEmulator/FakeCosmosHandler.cs` — ~1100 lines handling account metadata, partition key ranges, collection metadata, query execution, read feeds, pagination, caching, and fault injection.
- `src/CosmosDB.InMemoryEmulator/CosmosSqlParser.cs` — parses and simplifies SDK-internal SQL format.

---

## Approach 2: `.ToFeedIteratorOverridable()` via dual-factory interception

**Production code calls `.ToFeedIteratorOverridable()` instead of `.ToFeedIterator()`. This is the only production-side modification required.**

### How it works

`ToFeedIteratorOverridable` checks two factory delegates before falling back to the real SDK:

1. **AsyncLocal factory (`FeedIteratorFactory`)** — per-async-flow, test-isolated. Flows through `await`, `Task.Run`, `ThreadPool.QueueUserWorkItem`, and anything else that captures `ExecutionContext`.
2. **Static volatile fallback (`StaticFallbackFactory`)** — catches `new Thread()` where `ExecutionContext` doesn't flow. The factory is stateless (materialises whatever `IQueryable<T>` it receives), so different tests using different in-memory data don't cross-contaminate.
3. **Real SDK (`.ToFeedIterator()`)** — production path, taken when neither factory is set.

`InMemoryFeedIteratorSetup.Register()` sets **both** factories to the same delegate. `Deregister()` clears both.

### Test wiring (1 line)

```csharp
InMemoryFeedIteratorSetup.Register();
```

Teardown (optional):

```csharp
InMemoryFeedIteratorSetup.Deregister();
```

### Production code (one-token change)

```csharp
using CosmosDB.InMemoryEmulator.ProductionExtensions;

// Before:
var iterator = container
    .GetItemLinqQueryable<MyEntity>()
    .Where(item => item.IsActive)
    .ToFeedIterator();                   // ← fails with in-memory container

// After:
var iterator = container
    .GetItemLinqQueryable<MyEntity>()
    .Where(item => item.IsActive)
    .ToFeedIteratorOverridable();         // ← works in both production and tests
```

### Why two factories?

`AsyncLocal<T>` stores a value that flows with `ExecutionContext`. Every async continuation (`await`, `Task.Run`, etc.) inherits the calling flow's value. This means parallel xUnit tests — each running in their own async flow — see their own factory without cross-talk.

However, `new Thread()` does **not** capture `ExecutionContext`, so the `AsyncLocal` value will be null on a bare thread. The `volatile static` fallback catches this case.

Why the static fallback is safe:
- The factory delegate is stateless — it receives an `IQueryable<T>` and materialises it into an `InMemoryFeedIterator<T>`.
- Each test's queryable points at that test's in-memory data.
- Even though the static field is globally shared, there's no data cross-talk.

Edge case: if test A calls `Deregister()` while test B's `new Thread()` is mid-flight, B's thread could hit `.ToFeedIterator()` and throw. This is a pre-existing test isolation issue — not caused by the fallback mechanism.

### What InMemoryFeedIterator does

`InMemoryFeedIterator<T>` wraps a materialised list of items and provides `FeedIterator<T>` semantics:

- **Pagination**: Tracks `_offset` and `_maxItemCount`. `HasMoreResults` is `_offset < items.Count`. `ReadNextAsync` pages through with `Skip`/`Take`.
- **Continuation tokens**: Simple offset-as-string (`_offset.ToString()`).
- **Lazy factory**: Optional `Func<IReadOnlyList<T>>` constructor for change feeds.
- **`InMemoryFeedResponse<T>`**: Nested class implementing `FeedResponse<T>` with `NSubstitute` for `CosmosDiagnostics`.

### Key files

- `src/CosmosDB.InMemoryEmulator.ProductionExtensions/CosmosOverridableFeedIteratorExtensions.cs` — the extension method, heavily documented. Only dependency: `Microsoft.Azure.Cosmos`.
- `src/CosmosDB.InMemoryEmulator/InMemoryFeedIteratorSetup.cs` — test infrastructure that sets/clears both factories.
- `src/CosmosDB.InMemoryEmulator/InMemoryFeedIterator.cs` — the in-memory `FeedIterator<T>` implementation.

### Namespace

The extension method lives in the `CosmosDB.InMemoryEmulator.ProductionExtensions` namespace. Production code that calls `.ToFeedIteratorOverridable()` needs:

```csharp
using CosmosDB.InMemoryEmulator.ProductionExtensions;
```

### Production dependency

The `CosmosDB.InMemoryEmulator.ProductionExtensions` NuGet package contains only the extension method and a reference to `Microsoft.Azure.Cosmos`. No test libraries, no emulator code, no `Newtonsoft.Json`.

---

## Side-by-side comparison

| Dimension | Approach 1: `.ToFeedIterator()` | Approach 2: `.ToFeedIteratorOverridable()` |
|---|---|---|
| **Production code change** | None | Required (every `.ToFeedIterator()` call site) |
| **Test setup cost** | ~10 lines of boilerplate | 1 line |
| **`new Thread()` safety** | **Safe** — handler is bound to `CosmosClient` instance, no ambient state | **Safe** — `AsyncLocal` (primary) + `volatile static` fallback catches bare threads |
| **`Task.Run` / `await` safety** | Safe — same reason | Safe — `AsyncLocal` flows via `ExecutionContext` |
| **Test fidelity** | **High** — exercises real SDK LINQ→SQL translation, HTTP serialisation, partition key range fan-out, ORDER BY merge-sort, continuation token pagination, request charges | **Lower** — runs LINQ-to-Objects. No SQL translation, no HTTP layer, no fan-out, returns all results in one page by default |
| **LINQ-to-SQL gap risk** | **Catches bugs** — a LINQ expression that generates invalid Cosmos SQL will fail in tests | **Misses bugs** — a LINQ expression that works in-memory but would generate invalid SQL won't be caught |
| **SDK upgrade risk** | **Medium-high** — `FakeCosmosHandler` must understand the SDK's undocumented internal SQL format. SDK version changes can silently break parsing. | **None** — interception happens before the SDK is involved |
| **Fault injection** | Yes — `FakeCosmosHandler.FaultInjector` can simulate 429s, 503s, timeouts | No — there's no HTTP layer to inject faults into |
| **Multi-container** | Yes — `FakeCosmosHandler.CreateRouter()` dispatches by URL path | N/A — works directly on the `IQueryable` |
| **Emulator complexity** | `FakeCosmosHandler` is ~1100 lines (account metadata, partition key ranges, query execution, pagination, caching, fault injection) | Extension method is ~200 lines (mostly documentation); setup is ~75 lines |
| **Production dependency** | None — just `Microsoft.Azure.Cosmos` | `CosmosDB.InMemoryEmulator.ProductionExtensions` NuGet (contains only the extension + Cosmos SDK ref). Requires `using CosmosDB.InMemoryEmulator.ProductionExtensions;` |
| **Compile-time safety** | N/A — using standard SDK method | No enforcement — miss a `.ToFeedIterator()` and it throws at runtime in tests |

---

## The `new Thread()` scenario in detail

| Scenario | Approach 1 | Approach 2 |
|---|---|---|
| **Mechanism** | Instance-bound `HttpClient` — no ambient state involved | `AsyncLocal` (primary) + `volatile static` (fallback) |
| **`new Thread()` without `ExecutionContext`** | Works — handler is on the `CosmosClient` | Works — static fallback catches it |
| **`Thread.Start()` after `Deregister()`** | Works — handler is still on the `CosmosClient` | **Fails** — both factories are null, hits real SDK |
| **Parallel test isolation on bare threads** | Full — each test has its own `CosmosClient`/handler | Partial — static fallback is shared, but factory is stateless so data doesn't cross-contaminate. Ordering of `Register`/`Deregister` across tests could matter. |

---

## When to prefer which

### Prefer Approach 1 (`.ToFeedIterator()`) when:

- You can't or won't change production code.
- You need high-fidelity testing (SQL translation, pagination, ORDER BY).
- You want fault injection (429s, timeouts, 503s).
- Production code spawns raw threads and test lifecycle is complex (many parallel tests calling `Register`/`Deregister`).
- You want to catch LINQ expressions that would generate invalid Cosmos SQL.

### Prefer Approach 2 (`.ToFeedIteratorOverridable()`) when:

- You want minimal test boilerplate (1 line setup).
- You're OK with LINQ-to-Objects fidelity (e.g., you also have integration tests against real Cosmos).
- You want zero risk from SDK internal format changes.
- You want the production dependency to stay tiny with no emulator references.

### Using both together

They can coexist. A given call site uses one or the other; they don't conflict. You could use Approach 2 for most LINQ queries (fast, simple) and wire up `FakeCosmosHandler` for a few high-fidelity scenarios that need SQL translation testing, fault injection, or ORDER BY verification.

---

## Quick reference

### Approach 1 setup

```csharp
// Test fixture
var container = new InMemoryContainer("my-container", "/partitionKey");
var handler = new FakeCosmosHandler(container);
var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        LimitToEndpoint = true,
        MaxRetryAttemptsOnRateLimitedRequests = 0,
        HttpClientFactory = () => new HttpClient(handler)
    });
var realContainer = client.GetContainer("fakeDb", "fakeContainer");

// Production code (unchanged)
var iterator = realContainer.GetItemLinqQueryable<MyEntity>()
    .Where(e => e.IsActive)
    .ToFeedIterator();
```

### Approach 2 setup

```csharp
using CosmosDB.InMemoryEmulator.ProductionExtensions;

// Test fixture (one line)
InMemoryFeedIteratorSetup.Register();

// Production code (one-token change + using directive)
var iterator = container.GetItemLinqQueryable<MyEntity>()
    .Where(e => e.IsActive)
    .ToFeedIteratorOverridable();

// Teardown (optional)
InMemoryFeedIteratorSetup.Deregister();
```
