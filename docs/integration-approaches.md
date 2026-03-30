# Integration Approaches

There are three ways to integrate CosmosDB.InMemoryEmulator into your tests. Each has different trade-offs around production code changes, fidelity, and setup complexity.

## At a Glance

| | DI Extensions (Recommended) | Direct `InMemoryContainer` | `FakeCosmosHandler` |
|---|---|---|---|
| **Production code changes** | None | One token per LINQ call site¹ | **None** |
| **SQL queries** | `CosmosSqlParser` | `CosmosSqlParser` | `CosmosSqlParser` (same engine) |
| **LINQ queries** | LINQ-to-Objects | LINQ-to-Objects | SDK translates LINQ → SQL → `CosmosSqlParser`² |
| **Setup complexity** | **Minimal** | Minimal | Moderate |
| **Fault injection** | No | No | Yes (429s, 503s, timeouts) |
| **Pagination fidelity** | Basic | Basic | Realistic continuation tokens |
| **Query logging** | No | No | Yes |
| **SDK fidelity** | Medium | Lower (bypasses SDK pipeline) | **Highest** (exercises full SDK pipeline) |
| **Best for** | **Most projects** | Unit tests without DI | Acceptance tests, fault injection |

¹ `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`. Not needed if you don't use LINQ `.ToFeedIterator()`.

² All approaches run entirely in-process — nothing goes over the network. With `FakeCosmosHandler`, the SDK's HTTP pipeline executes but the handler intercepts requests before they leave the process.

---

## DI Extensions (Recommended)

**Start here.** Covers the vast majority of test scenarios with one line of setup. Full guide: [Dependency Injection](dependency-injection.md).

### Quick Example

```csharp
// In your WebApplicationFactory or test setup
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB();
});
```

Existing `CosmosClient` and `Container` registrations are automatically replaced with in-memory equivalents. See the [DI guide](dependency-injection.md) for zero-config auto-detect, explicit container configuration, typed clients, and more.

### Pros

- ✅ One-liner setup
- ✅ Zero-config auto-detect of existing container registrations
- ✅ No production code changes (except `.ToFeedIteratorOverridable()` for LINQ)
- ✅ Automatically matches existing service lifetimes

### Cons

- ❌ Requires DI in the system under test
- ❌ For LINQ `.ToFeedIterator()` support, needs `.ToFeedIteratorOverridable()` in production code

### When to Use

- Integration tests with `WebApplicationFactory`
- Any system that uses DI for Cosmos registrations
- **Default choice for most projects**

---

## Direct `InMemoryContainer`

**Best for:** Unit tests, simple component tests, testing repository classes directly.

### How It Works

You create an `InMemoryContainer` and pass it directly to the code under test (or to a repository that depends on `Container`).

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
- ✅ Fastest execution — no HTTP pipeline overhead
- ✅ Works with any test framework
- ✅ No `CosmosClient` required

### Cons

- ❌ Requires a one-token change per LINQ `.ToFeedIterator()` call site (if using LINQ)
- ❌ Requires the `ProductionExtensions` NuGet package in the production project (if using LINQ)
- ❌ Bypasses the Cosmos SDK's HTTP pipeline — lower fidelity
- ❌ No fault injection
- ❌ No query logging

### When to Use

- Unit tests for individual repositories or services
- Projects that don't use LINQ `.ToFeedIterator()` at all
- When you want the absolute simplest setup

---

## `FakeCosmosHandler` (High Fidelity)

**Best for:** Acceptance tests, CI pipelines where you need maximum confidence, testing retry policies.

### How It Works

`FakeCosmosHandler` is a custom `HttpMessageHandler` that intercepts all HTTP requests from the Cosmos SDK. The SDK thinks it's talking to a real Cosmos DB endpoint, but all operations are served from an `InMemoryContainer`. **Your production code stays completely untouched** — including `.ToFeedIterator()` and all LINQ operations.

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

**3. Use the client exactly as production code does:**

```csharp
var container = client.GetContainer("my-database", "orders");

// SQL queries — works as-is
var iterator = container.GetItemQueryIterator<Order>(
    "SELECT * FROM c WHERE c.status = 'active'");

// LINQ — works as-is, including .ToFeedIterator()
var linqIterator = container
    .GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();  // ← works unchanged!
```

**4. Seed test data through the in-memory container:**

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
- ✅ Highest fidelity — exercises the full Cosmos SDK HTTP pipeline
- ✅ Fault injection for testing retry policies
- ✅ Query and request logging
- ✅ Realistic pagination with continuation tokens
- ✅ Multi-container routing

### Cons

- ❌ More setup code (connection string, `CosmosClientOptions`, handler wiring)
- ❌ Slightly slower (HTTP pipeline overhead, though still in-process)
- ❌ Requires `ConnectionMode.Gateway` (Cosmos's direct mode bypasses `HttpMessageHandler`)

### When to Use

- Component tests and acceptance tests
- Tests for retry/fallback policies
- CI pipelines where you want maximum confidence
- When you can't modify production code at all

---

---

## Combining Approaches

Approaches are **complementary, not mutually exclusive**. A common pattern:

1. **Integration tests** → DI Extensions (covers most scenarios)
2. **Unit tests** → Direct `InMemoryContainer` (no DI overhead)
3. **Acceptance tests** → `FakeCosmosHandler` (full fidelity + fault injection)

## Decision Flowchart

```
Does the system under test use DI?
├── Yes → DI Extensions (UseInMemoryCosmosDB) ← start here
└── No → Direct InMemoryContainer

Do you also need fault injection / query logging?
├── Yes → FakeCosmosHandler
└── No → stick with the above

Does production code use .ToFeedIterator() and you can't change it?
└── Yes → FakeCosmosHandler (no production code changes needed)
```
