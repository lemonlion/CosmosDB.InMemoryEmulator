There are three ways to integrate CosmosDB.InMemoryEmulator into your tests. Each has different trade-offs around production code changes, fidelity, and setup complexity.

> **Custom factory interface?** If your production code accesses Cosmos through a custom factory interface (e.g. `IDatabaseClientFactory`) rather than registering `CosmosClient`/`Container` directly in DI, see [Pattern 5 (Direct)](Dependency-Injection#pattern-5-custom-factory-interface--direct-inmemoryclient) and [Pattern 6 (FakeCosmosHandler)](Dependency-Injection#pattern-6-custom-factory-interface--fakecosmoshandler-zero-production-changes) in the DI guide. Pattern 6 requires **zero production code changes**.

## At a Glance

There are two independent decisions when integrating the emulator. You can mix and match freely — each column in one table can be combined with any column in the other.

### Decision 1: How You Wire It Up

| | DI Extensions (Recommended) | Direct Instantiation |
|---|---|---|
| **Requires DI** | Yes | No |
| **Setup** | `services.UseInMemoryCosmosDB()` — one line | `new InMemoryContainer(...)` / `new InMemoryCosmosClient()` / `new FakeCosmosHandler(...)` |
| **Auto-detects registrations** | Yes — replaces existing `CosmosClient`/`Container` bindings | N/A — you construct and pass objects yourself |
| **Best for** | Integration tests with `WebApplicationFactory`, any DI-based system | Unit tests, simple component tests, projects without DI |

### Decision 2: What You Replace

| | Container (`InMemoryContainer`) | CosmosClient (`InMemoryCosmosClient` + `InMemoryContainer`) | CosmosClient (`CosmosClient` + `FakeCosmosHandler`) |
|---|---|---|---|
| **What it replaces** | `Container` | `CosmosClient` + `CosmosClient`'s `Container`s | `CosmosClient` + `CosmosClient`'s `HttpMessageHandler` |
| **Production code changes** | `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`¹ | `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`¹ | **None** |
| **LINQ queries** | LINQ-to-Objects | LINQ-to-Objects | SDK translates LINQ → SQL → `CosmosSqlParser`² |
| **SDK fidelity** | Lower — bypasses SDK pipeline entirely | Medium — fake `CosmosClient` subclass whose methods route directly to in-memory storage, bypassing the SDK's HTTP serialization and request pipeline | **Highest** — real `CosmosClient` builds and serializes HTTP requests exactly as in production; `FakeCosmosHandler` intercepts them before they leave the process |
| **Multi-container** | One container per instance | `client.GetContainer()` returns different containers | `FakeCosmosHandler.CreateRouter()` for multi-container |
| **Fault injection** | No | No | Yes (429s, 503s, timeouts) |
| **Query / request logging** | No | No | Yes |
| **Pagination fidelity** | Basic | Basic | Realistic continuation tokens |
| **Setup complexity** | **Minimal** | Minimal | Moderate (connection string + `CosmosClientOptions`) |
| **Best for** | Simplest setup; code that depends on `Container` directly; unit tests without DI | Multi-container scenarios; code that calls `CosmosClient.GetContainer()`; custom factory interfaces | Maximum confidence; zero production code changes; testing retry/throttle handling; LINQ-to-SQL translation fidelity |

All three approaches support CRUD operations (create, read, upsert, replace, delete, patch) and SQL queries via `CosmosSqlParser`. All run entirely in-process — nothing goes over the network.

### Combining the Two Decisions

| Wiring ↓ \ Replacement → | Container (`InMemoryContainer`) | CosmosClient (`InMemoryCosmosClient` + `InMemoryContainer`) | CosmosClient (`CosmosClient` + `FakeCosmosHandler`) |
|---|---|---|---|
| **DI Extensions** | `UseInMemoryCosmosContainers()` | `UseInMemoryCosmosDB<TClient>()` — typed client subclasses only | `UseInMemoryCosmosDB()` — **recommended** |
| **Direct Instantiation** | `new InMemoryContainer(...)` passed to code under test | `new InMemoryCosmosClient()` — multi-container via `GetContainer()` | `new FakeCosmosHandler(...)` → `new CosmosClient(...)` |

¹ Not needed if you don't use LINQ `.ToFeedIterator()`.

² **Why LINQ execution differs:** With `InMemoryContainer` and `InMemoryCosmosClient` + `InMemoryContainer` replacement, `GetItemLinqQueryable<T>()` returns a LINQ-to-Objects queryable — `.Where()` and `.OrderBy()` execute as C# delegates. With `CosmosClient` + `FakeCosmosHandler`, the real SDK's `CosmosLinqQueryProvider` translates your LINQ expression tree into Cosmos DB SQL, and `FakeCosmosHandler` executes that SQL through `CosmosSqlParser`. This means `FakeCosmosHandler` is the only approach that tests the SDK's LINQ-to-SQL translation.

With `CosmosClient` + `FakeCosmosHandler`, the SDK's HTTP pipeline executes but the handler intercepts requests before they leave the process.

---

## DI Extensions (Recommended)

**Start here.** Covers the vast majority of test scenarios with one line of setup. Full guide: [Dependency Injection](Dependency-Injection).

### Quick Example

```csharp
// Replaces both CosmosClient and Container registrations with in-memory equivalents
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB();
});
```

Or, to replace only `Container` registrations (leaving any `CosmosClient` registration untouched):

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosContainers();
});
```

Existing registrations are automatically replaced with in-memory equivalents. See the [DI guide](Dependency-Injection) for zero-config auto-detect, explicit container configuration, typed clients, and more.

### Pros

- ✅ One-liner setup
- ✅ Zero-config auto-detect of existing registrations
- ✅ Automatically matches existing service lifetimes

### Cons

- ❌ Requires DI in the system under test

### Cons (Only applies to usage of `UseInMemoryCosmosContainers()`)

- ❌ Needs `.ToFeedIteratorOverridable()` for LINQ `.ToFeedIterator()` support (not an issue with `UseInMemoryCosmosDB()`, which uses `FakeCosmosHandler` internally)

### When to Use

- Integration tests with `WebApplicationFactory`
- Any system that uses DI for Cosmos registrations
- **Default choice for most projects**

---

## `InMemoryContainer`

**Best for:** Simplest setup; code that depends on `Container` directly; unit tests.

### How It Works

`InMemoryContainer` is a fake `Container` subclass backed by in-memory storage. Pass it anywhere your production code expects a `Container` — either directly or via DI with `UseInMemoryCosmosContainers()`.

### Step-by-Step

**1. Create the container:**

```csharp
var container = new InMemoryContainer("orders", "/customerId");
```

**2. Pass it to your production code:**

```csharp
// Your production repository (unchanged)
public class OrderRepository
{
    private readonly Container _container;
    public OrderRepository(Container container) => _container = container;

    public async Task<Order> GetOrderAsync(string id, string customerId)
    {
        var response = await _container.ReadItemAsync<Order>(
            id, new PartitionKey(customerId));
        return response.Resource;
    }
}

// In your test
var repo = new OrderRepository(container);
```

**3. If production code uses LINQ `.ToFeedIterator()`:**

Change `.ToFeedIterator()` to `.ToFeedIteratorOverridable()` in production code (one-token change per call site), and call `InMemoryFeedIteratorSetup.Register()` once in your test setup:

```csharp
// Production code change (the only one):
var iterator = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIteratorOverridable();  // was: .ToFeedIterator()

// Test setup (once per test fixture):
InMemoryFeedIteratorSetup.Register();
```

This requires adding the `CosmosDB.InMemoryEmulator.ProductionExtensions` NuGet package to your production project.

### Pros

- ✅ Simplest setup — one line to create the container
- ✅ Fastest execution — no SDK pipeline overhead
- ✅ Works with any test framework, with or without DI
- ✅ No `CosmosClient` required

### Cons

- ❌ Requires a one-token change per LINQ `.ToFeedIterator()` call site (if using LINQ)
- ❌ Requires the `ProductionExtensions` NuGet package in the production project (if using LINQ)
- ❌ Bypasses the SDK pipeline entirely — lower fidelity
- ❌ No fault injection or query logging
- ❌ One container per instance — no multi-container support

### When to Use

- Code that depends on `Container` directly
- Unit tests for individual repositories or services
- When you want the absolute simplest setup

---

## `InMemoryCosmosClient` + `InMemoryContainer`

**Best for:** Multi-container scenarios; code that calls `CosmosClient.GetContainer()`; typed client subclasses; custom factory interfaces.

### How It Works

`InMemoryCosmosClient` is a fake `CosmosClient` subclass. When you call `client.GetContainer()`, it returns `InMemoryContainer` instances backed by in-memory storage. Containers are created lazily on first access. Use it via DI with `UseInMemoryCosmosDB<TClient>()` (typed client pattern) or by direct instantiation.

### Step-by-Step

**1. Create the client:**

```csharp
var client = new InMemoryCosmosClient();
```

**2. Use it exactly like a real `CosmosClient`:**

```csharp
var container = client.GetContainer("my-database", "orders");

await container.CreateItemAsync(
    new Order { Id = "1", CustomerId = "cust-1", Status = "active" },
    new PartitionKey("cust-1"));

var response = await container.ReadItemAsync<Order>("1", new PartitionKey("cust-1"));
```

Multiple `GetContainer()` calls with the same database and container name return the same container instance — data is shared.

**3. If production code uses LINQ `.ToFeedIterator()`:**

Same as `InMemoryContainer` — change `.ToFeedIterator()` to `.ToFeedIteratorOverridable()` and call `InMemoryFeedIteratorSetup.Register()` once in your test setup.

### Pros

- ✅ Multi-container — `GetContainer()` returns different containers from a single client
- ✅ Drop-in replacement for code that depends on `CosmosClient`
- ✅ Containers created lazily on first access
- ✅ Simple setup — one line to create the client

### Cons

- ❌ Requires a one-token change per LINQ `.ToFeedIterator()` call site (if using LINQ)
- ❌ Requires the `ProductionExtensions` NuGet package in the production project (if using LINQ)
- ❌ Fake `CosmosClient` subclass — bypasses the SDK's HTTP serialization and request pipeline
- ❌ No fault injection or query logging

### When to Use

- Code that depends on `CosmosClient` (directly or through a factory interface)
- Multi-container scenarios where different parts of the app access different containers
- When you need `client.GetDatabase()` or `client.GetContainer()` to work

---

## `CosmosClient` + `FakeCosmosHandler` (High Fidelity)

**Best for:** Maximum confidence; zero production code changes; testing retry/throttle handling; LINQ-to-SQL translation fidelity. This is what `UseInMemoryCosmosDB()` uses internally.

### How It Works

`FakeCosmosHandler` is a custom `HttpMessageHandler` injected into a real `CosmosClient`. The SDK builds and serializes HTTP requests exactly as in production; `FakeCosmosHandler` intercepts them before they leave the process and serves responses from an `InMemoryContainer`. **Your production code stays completely untouched** — including `.ToFeedIterator()` and all LINQ operations.

> 💡 **`UseInMemoryCosmosDB()` does this for you.** The DI extension method creates `InMemoryContainer`s, wraps them in `FakeCosmosHandler`s, and wires everything into a real `CosmosClient` automatically. The manual setup below is only needed for direct instantiation (no DI) or when you need fine-grained control over the handler.

### Step-by-Step

**1. Create the in-memory container and handler:**

```csharp
var inMemoryContainer = new InMemoryContainer("orders", "/customerId");
var handler = new FakeCosmosHandler(inMemoryContainer);
```

**2. Create a `CosmosClient` with the handler:**

```csharp
var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        HttpClientFactory = () => new HttpClient(handler)
    });
```

> The connection string doesn't matter — all requests go to the handler, never to the network. But it must be syntactically valid.

> ⚠️ **Serialization matters.** If your C# models use PascalCase properties (e.g. `Id`), you must configure the `CosmosClient` with a camelCase serializer — otherwise operations will fail with `"Item must have an 'id' property"`. See [Troubleshooting](Troubleshooting#item-must-have-an-id-property) for details and a ready-to-use serializer snippet.

**3. Use the client exactly as production code does:**

```csharp
var container = client.GetContainer("my-database", "orders");

// CRUD operations — work as-is
await container.CreateItemAsync(
    new Order { Id = "1", CustomerId = "cust-1", Status = "active" },
    new PartitionKey("cust-1"));

var response = await container.ReadItemAsync<Order>("1", new PartitionKey("cust-1"));

// SQL queries — work as-is
var iterator = container.GetItemQueryIterator<Order>(
    "SELECT * FROM c WHERE c.status = 'active'");

// LINQ — works as-is, including .ToFeedIterator()
var linqIterator = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();  // ← works unchanged!
```

All CRUD operations (create, read, upsert, replace, delete, patch), SQL queries, and LINQ queries work through the same `Container` reference — no need for a separate seeding step.

**4. (Optional) Seed test data through the backing container:**

If you prefer, you can still seed data directly through the `InMemoryContainer`:

```csharp
await inMemoryContainer.CreateItemAsync(
    new Order { Id = "1", CustomerId = "cust-1", Status = "active" },
    new PartitionKey("cust-1"));
```

### Multi-Container Routing

Use `FakeCosmosHandler.CreateRouter()` when your production code accesses multiple containers:

```csharp
var orders = new InMemoryContainer("orders", "/customerId");
var customers = new InMemoryContainer("customers", "/id");

var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
{
    ["orders"] = new FakeCosmosHandler(orders),
    ["customers"] = new FakeCosmosHandler(customers)
});

var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        HttpClientFactory = () => new HttpClient(router)
    });

// Both containers accessible through the same client
var ordersContainer = client.GetContainer("db", "orders");
var customersContainer = client.GetContainer("db", "customers");
```

> 💡 For a DI-integrated version of multi-container routing, see [Dependency Injection — Pattern 6: Custom Factory Interface](Dependency-Injection#pattern-6-custom-factory-interface--fakecosmoshandler-zero-production-changes).

### `CreateAndInitializeAsync`

If your production code uses the static factory `CosmosClient.CreateAndInitializeAsync(...)`, the same `HttpClientFactory` approach works — no production code changes needed:

```csharp
var inMemoryContainer = new InMemoryContainer("orders", "/customerId");
var handler = new FakeCosmosHandler(inMemoryContainer);

var containers = new List<(string, string)> { ("myDb", "orders") };

var client = await CosmosClient.CreateAndInitializeAsync(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    containers,
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        LimitToEndpoint = true,
        HttpClientFactory = () => new HttpClient(handler)
    });

// client is a real CosmosClient backed by in-memory data
var container = client.GetContainer("myDb", "orders");
```

For multiple containers, combine with `CreateRouter()`:

```csharp
var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
{
    ["orders"] = new FakeCosmosHandler(ordersContainer),
    ["customers"] = new FakeCosmosHandler(customersContainer)
});

var client = await CosmosClient.CreateAndInitializeAsync(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new List<(string, string)> { ("db", "orders"), ("db", "customers") },
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        LimitToEndpoint = true,
        HttpClientFactory = () => new HttpClient(router)
    });
```

### Fault Injection

Simulate transient failures, throttling, and timeouts:

```csharp
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

### Handler Options

```csharp
var options = new FakeCosmosHandlerOptions
{
    CacheTtl = TimeSpan.FromMinutes(5),    // Query result cache TTL
    CacheMaxEntries = 100,                  // Max cached result sets
    PartitionKeyRangeCount = 4              // Simulated partition ranges
};

var handler = new FakeCosmosHandler(container, options);
```

### SDK Compatibility Verification

Detect breaking changes in the Cosmos SDK before they cause silent data corruption:

```csharp
// Call once during test suite setup (e.g. in a collection fixture)
await FakeCosmosHandler.VerifySdkCompatibilityAsync();
```

### Query and Request Logging

```csharp
// After running tests
foreach (var request in handler.RequestLog)
    Console.WriteLine(request);  // "POST /dbs/db/colls/orders/docs"

foreach (var query in handler.QueryLog)
    Console.WriteLine(query);    // "SELECT * FROM c WHERE c.status = 'active'"
```

### Pros

- ✅ **Zero production code changes** — no `.ToFeedIteratorOverridable()`, no extra NuGet packages
- ✅ Highest fidelity — real `CosmosClient` exercises the full SDK HTTP pipeline
- ✅ LINQ-to-SQL translation tested — the SDK translates your LINQ into Cosmos SQL, which `CosmosSqlParser` then executes
- ✅ Fault injection for testing retry/throttle handling
- ✅ Query and request logging
- ✅ Realistic pagination with continuation tokens
- ✅ Multi-container routing via `CreateRouter()`
- ✅ Custom `DelegatingHandler` wrapping via `HttpMessageHandlerWrapper` (e.g. for test diagram generation, request logging, metrics)

### Cons

- ❌ More setup code (connection string, `CosmosClientOptions`, handler wiring)
- ❌ Slightly slower (HTTP pipeline overhead, though still in-process)
- ❌ Requires `ConnectionMode.Gateway` (Cosmos's direct mode bypasses `HttpMessageHandler`)

### When to Use

- Maximum confidence — full SDK pipeline exercised
- Testing retry/fallback/throttle policies via fault injection
- When you can't modify production code at all
- When you need LINQ-to-SQL translation fidelity

---

## Combining Approaches

The two decisions are **orthogonal** — pick one from each axis. A common pattern:

1. **Integration tests** → DI Extensions + `UseInMemoryCosmosDB()` (highest fidelity, replaces `CosmosClient` + `Container`s in one line, uses `FakeCosmosHandler` internally)
2. **Unit tests** → Direct `InMemoryContainer` (no DI, simplest setup)
3. **Acceptance tests** → Direct + `FakeCosmosHandler` (full SDK fidelity + fault injection + zero production code changes, manual wiring)

## Decision Flowchart

```
1. How do you wire it up?
   ├── System uses DI? → DI Extensions (UseInMemoryCosmosDB / UseInMemoryCosmosContainers)
   └── No DI / unit test? → Direct Instantiation

2. What do you replace?
   ├── Code depends on Container directly?
   │   └── InMemoryContainer — simplest setup, one container per instance
   ├── Need typed CosmosClient subclasses or direct InMemoryCosmosClient?
   │   └── InMemoryCosmosClient + InMemoryContainer — multi-container, lazy creation
   ├── Need manual fault injection, request logging, or direct handler control?
   │   └── CosmosClient + FakeCosmosHandler — real CosmosClient with intercepted HTTP pipeline
   └── Default (recommended)?
       └── UseInMemoryCosmosDB() — uses FakeCosmosHandler internally, one-line setup
           (fault injection available via OnHandlerCreated callback)
```
