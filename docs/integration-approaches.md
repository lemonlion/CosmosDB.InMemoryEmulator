# Integration Approaches

There are three ways to integrate CosmosDB.InMemoryEmulator into your tests. Each has different trade-offs around production code changes, fidelity, and setup complexity.

## At a Glance

| | Approach 1: Direct `InMemoryContainer` | Approach 2: `FakeCosmosHandler` | Approach 3: DI Extensions |
|---|---|---|---|
| **Production code changes** | One token per LINQ call site¹ | **None** | None |
| **Query execution** | LINQ-to-Objects | Full SQL parser via HTTP | In-memory SQL parser |
| **Setup complexity** | Minimal | Moderate | Minimal |
| **Fault injection** | No | Yes (429s, 503s, timeouts) | No |
| **Pagination fidelity** | Basic | Realistic continuation tokens | Basic |
| **Query logging** | No | Yes | No |
| **SDK fidelity** | Lower (bypasses SDK pipeline) | **Highest** (exercises full SDK HTTP pipeline) | Medium |
| **Best for** | Unit tests | Component/acceptance tests, CI | Integration tests with DI |

¹ `.ToFeedIterator()` → `.ToFeedIteratorOverridable()`. Not needed if you don't use LINQ `.ToFeedIterator()`.

---

## Approach 1: Direct `InMemoryContainer` Usage

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

## Approach 2: `FakeCosmosHandler` (Recommended for High Fidelity)

**Best for:** Component tests, acceptance tests, CI pipelines, testing retry policies.

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

## Approach 3: DI Extension Methods

**Best for:** Integration tests using `WebApplicationFactory` or any DI-based test host.

### How It Works

Extension methods on `IServiceCollection` replace existing `CosmosClient` and/or `Container` registrations with in-memory equivalents. Call them from `ConfigureTestServices` in your `WebApplicationFactory` or equivalent test setup.

### Full guide: [Dependency Injection](dependency-injection.md)

### Quick Example

```csharp
// In your WebApplicationFactory or test setup
services.UseInMemoryCosmosDB(options => options
    .AddContainer("orders", "/customerId")
    .AddContainer("customers", "/id"));
```

### Pros

- ✅ No production code changes (except services needing `.ToFeedIteratorOverridable()` for LINQ)
- ✅ One-liner setup in `ConfigureTestServices`
- ✅ Automatically matches existing service lifetimes (singleton/scoped/transient)
- ✅ Configuration callbacks for seeding data

### Cons

- ❌ Requires DI in the system under test
- ❌ For LINQ `.ToFeedIterator()` support, needs `.ToFeedIteratorOverridable()` in production code

### When to Use

- Integration tests with `WebApplicationFactory`
- Systems that already use DI for Cosmos

---

## Combining Approaches

Approaches are **complementary, not mutually exclusive**. A common pattern:

1. **Unit tests** → Approach 1 (direct `InMemoryContainer`)
2. **Integration tests** → Approach 3 (DI extensions)
3. **Acceptance tests** → Approach 2 (`FakeCosmosHandler` for full fidelity + fault injection)

## Decision Flowchart

```
Do you need fault injection / query logging?
├── Yes → Approach 2 (FakeCosmosHandler)
└── No
    ├── Does the system under test use DI?
    │   ├── Yes → Approach 3 (DI Extensions)
    │   └── No → Approach 1 (Direct InMemoryContainer)
    └── Does production code use .ToFeedIterator()?
        ├── Yes, and you can't change it → Approach 2 (FakeCosmosHandler)
        └── Yes, and you can change it → Approach 1 with .ToFeedIteratorOverridable()
```
