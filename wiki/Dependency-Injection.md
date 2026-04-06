# Dependency Injection

Step-by-step guide for integrating CosmosDB.InMemoryEmulator with dependency injection in your test projects. For installation, see [Getting Started](Getting-Started). For help choosing between the three integration layers (`InMemoryContainer`, `InMemoryCosmosClient`, `FakeCosmosHandler`), see [Integration Approaches](Integration-Approaches).

## Overview

The emulator provides three DI extension methods on `IServiceCollection`, plus documented patterns for custom factory interfaces:

| Pattern | Method / Approach | Purpose |
|---------|-------------------|---------|
| 1 | `UseInMemoryCosmosDB()` | Client + Container — the most common setup |
| 2 | `UseInMemoryCosmosDB<TClient>()` | Typed `CosmosClient` subclass |
| 3 | `UseInMemoryCosmosDB()` | Client only — repos call `GetContainer()` directly |
| 4 | `UseInMemoryCosmosContainers()` | Container only — no `CosmosClient` replacement |
| 5 | Manual — `InMemoryCosmosClient` | Custom factory interface |
| 6 | Manual — `FakeCosmosHandler` | Custom factory interface, zero production changes |

Patterns 1–4 are one-liner DI calls from `ConfigureTestServices`. Patterns 5–6 are for projects where Cosmos access is behind a custom factory interface — common in enterprise codebases with internal frameworks.

---

## Pattern 1: Singleton Client + Singleton Container

**The most common pattern.** Production code registers one `CosmosClient` and one or more `Container` instances.

### Production Code (unchanged)

```csharp
// In Startup.cs / Program.cs
services.AddSingleton<CosmosClient>(sp =>
    new CosmosClient(configuration["CosmosDb:ConnectionString"]));

services.AddSingleton<Container>(sp =>
{
    var client = sp.GetRequiredService<CosmosClient>();
    return client.GetContainer("MyDatabase", "orders");
});
```

### Test Setup — Zero-Config (Recommended)

```csharp
public class MyAppFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureTestServices(services =>
        {
            services.UseInMemoryCosmosDB();
        });
    }
}
```

That's it! `UseInMemoryCosmosDB()` creates an `InMemoryContainer` and wires it through `FakeCosmosHandler` into a real `CosmosClient`. Your existing `Container` factory registrations are **preserved** — they resolve against the new `CosmosClient`, which routes all requests through the in-memory handler.

### What Happens (Zero-Config)

1. The existing `CosmosClient` registration is removed
2. An `InMemoryContainer` is created and wrapped in `FakeCosmosHandler`
3. A real `CosmosClient` is created with `ConnectionMode.Gateway` and the handler intercepting all HTTP traffic
4. The `CosmosClient` is registered as a singleton
5. Existing `Container` factory registrations are **kept** — they call `client.GetContainer()` on the real client, which routes through `FakeCosmosHandler`

> **Note:** The default in-memory container uses `/id` as the partition key path. This works for most tests since production code passes explicit `PartitionKey` values. If your tests rely on partition-key-path-based extraction (e.g. `PartitionKey.None`), use the explicit form below.

### Test Setup — Explicit Containers

Use `AddContainer()` when you need to control the partition key path:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB(options => options
        .AddContainer("orders", "/customerId"));
});
```

### What Happens (Explicit)

1. The existing `CosmosClient` registration is removed
2. The existing `Container` registration(s) are removed
3. `InMemoryContainer` instances are created for each `AddContainer()` call and wrapped in `FakeCosmosHandler`
4. A real `CosmosClient` is registered (singleton), backed by the handler
5. `Container` instances are registered (via `client.GetContainer()`), preserving the original lifetime

### Multiple Containers

```csharp
services.UseInMemoryCosmosDB(options => options
    .AddContainer("orders", "/customerId")
    .AddContainer("customers", "/id")
    .AddContainer("products", "/categoryId"));
```

Each container is registered as a separate `Container` service descriptor. If your production code resolves multiple `Container` instances, they'll each get a distinct in-memory container.

> **Important — explicit mode is all-or-nothing.** As soon as you call `AddContainer()`, *all* existing `Container` registrations are removed and only the containers you explicitly add are registered. If your production code registers three containers but you only call `AddContainer()` for one of them, the other two will not be available via DI. Declare all containers your code depends on.

---

## Pattern 2: Typed CosmosClient Subclasses

**Used when production code has multiple typed `CosmosClient` subclasses** — each connecting to a different Cosmos DB account or database. Common in multi-tenant or domain-segmented architectures.

### Production Code (unchanged)

```csharp
// Production typed clients
public class EmployeeCosmosClient : CosmosClient
{
    public EmployeeCosmosClient(string connectionString) : base(connectionString) { }
}

public class CustomerCosmosClient : CosmosClient
{
    public CustomerCosmosClient(string connectionString) : base(connectionString) { }
}

// Registration
services.AddSingleton(new EmployeeCosmosClient(config["Biometric:ConnectionString"]));
services.AddSingleton(new CustomerCosmosClient(config["OOB:ConnectionString"]));

// Repos resolve the typed client
public class BiometricRepository
{
    private readonly EmployeeCosmosClient _client;
    public BiometricRepository(EmployeeCosmosClient client) => _client = client;
}
```

### Test Project Setup

**Step 1:** Create test-only shadow types that inherit from `InMemoryCosmosClient` instead of `CosmosClient`:

```csharp
// In your test project — shadows the production type
namespace MyApp.Cosmos;  // Same namespace as production

public class EmployeeCosmosClient : InMemoryCosmosClient { }
public class CustomerCosmosClient : InMemoryCosmosClient { }
```

> **Important:** These must be in the same namespace as the production types, or use `using` aliases, so that DI resolves the test type for repos that depend on the typed client.

**Step 2:** Replace registrations in test setup:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB<EmployeeCosmosClient>(options => options
        .AddContainer("employee-data", "/employeeId"));

    services.UseInMemoryCosmosDB<CustomerCosmosClient>(options => options
        .AddContainer("customer-stuff", "/customerId"));
});
```

### What Happens

1. The existing `EmployeeCosmosClient` registration is removed
2. A new `EmployeeCosmosClient` (which is `InMemoryCosmosClient`) is registered, preserving lifetime
3. No `Container` or base `CosmosClient` is registered — Pattern 2 repos call `client.GetContainer()` directly
4. Repos that depend on `EmployeeCosmosClient` resolve the in-memory version

### Why Shadow Types Are Needed

The generic method requires `TClient : InMemoryCosmosClient, new()`. This is because:

- `CosmosClient` has `internal virtual` methods that prevent runtime proxying (NSubstitute/Castle.Core)
- .NET DI type-checks at resolution time — you can't register `InMemoryCosmosClient` as `EmployeeCosmosClient`

The one-liner shadow type is the only approach that works. It goes in the **test project only** — production code is never changed.

> **Note:** Pattern 2 uses [`InMemoryCosmosClient`](API-Reference) directly (not `FakeCosmosHandler`), so LINQ `.ToFeedIterator()` requires `.ToFeedIteratorOverridable()`. See the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for details. If your repos only use SQL queries or point reads, this doesn't apply.

---

## Pattern 3: Singleton Client, Repos Call `GetContainer()`

**Production code registers only `CosmosClient`, and each repository calls `client.GetContainer()` directly.**

### Production Code (unchanged)

```csharp
services.AddSingleton<CosmosClient>(sp =>
    new CosmosClient(configuration["CosmosDb:ConnectionString"]));

public class OrderRepository
{
    private readonly Container _container;

    public OrderRepository(CosmosClient client)
    {
        _container = client.GetContainer("MyDb", "orders");
    }
}
```

### Test Setup

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB(options => options
        .AddContainer("orders", "/customerId")
        .AddContainer("customers", "/id"));
});
```

When your repositories call `client.GetContainer()`, the real `CosmosClient` returns a standard `Container` proxy. All HTTP requests from that proxy are intercepted by `FakeCosmosHandler`, which routes them to the pre-configured `InMemoryContainer` with the correct partition key path. You must declare every container your repositories use via `AddContainer()`.

> **Note:** Since `UseInMemoryCosmosDB()` uses `FakeCosmosHandler` internally, `.ToFeedIterator()` works without any production code changes. The SDK's LINQ-to-SQL translation is exercised, giving you higher fidelity than using `InMemoryContainer` or `InMemoryCosmosClient` alone. See [Integration Approaches](Integration-Approaches) for a detailed comparison.

---

## Pattern 4: Container-Only Replacement

**Use when you only want to replace `Container` registrations but keep the real `CosmosClient`** (or don't have one registered). This registers raw `InMemoryContainer` instances — not backed by `FakeCosmosHandler`.

### Production Code (unchanged)

```csharp
services.AddSingleton<Container>(sp =>
{
    var client = sp.GetRequiredService<CosmosClient>();
    return client.GetContainer("MyDatabase", "orders");
});

public class OrderRepository(Container container)
{
    public async Task<Order> GetOrder(string id, string partitionKey)
        => (await container.ReadItemAsync<Order>(id, new PartitionKey(partitionKey))).Resource;
}
```

### Test Setup

```csharp
services.UseInMemoryCosmosContainers(options => options
    .AddContainer("orders", "/customerId"));
```

### What Happens

1. Existing `Container` registrations are removed
2. New `InMemoryContainer` instances are registered, preserving lifetime
3. `CosmosClient` is NOT touched
4. `InMemoryFeedIteratorSetup.Register()` is called (for `.ToFeedIteratorOverridable()` support)

> **Note:** Since this pattern does not use `FakeCosmosHandler`, LINQ chains ending in `.ToFeedIterator()` require changing to `.ToFeedIteratorOverridable()`. See the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for details on both approaches.

---

## Pattern 5: Custom Factory Interface — Direct (`InMemoryCosmosClient`)

**Used when production code resolves containers through a custom factory interface** (e.g. `IDatabaseClientFactory`) rather than registering `CosmosClient` or `Container` directly in DI. Common in enterprise codebases with internal frameworks. See [Integration Approaches](Integration-Approaches) for when to choose this over Pattern 6.

### Production Code (unchanged)

```csharp
// Framework-provided interface (e.g. from an internal NuGet package)
public interface IDatabaseClientFactory
{
    Container GetContainer(string containerName);
}

// Repositories depend on the factory
public class TransactionsRepository(IDatabaseClientFactory databaseClientFactory)
{
    public async Task<ItemResponse<Transaction>> InsertTransaction(Transaction transaction)
    {
        var container = databaseClientFactory.GetContainer("transactions");
        return await container.CreateItemAsync(transaction, new PartitionKey(transaction.MerchantId));
    }
}
```

### Test Setup

Implement the factory interface using `InMemoryCosmosClient`:

```csharp
public class InMemoryDatabaseClientFactory : IDatabaseClientFactory
{
    private readonly InMemoryCosmosClient _client = new();

    public Container GetContainer(string containerName)
        => _client.GetContainer("test-db", containerName);
}
```

Register it in your test DI:

```csharp
builder.ConfigureTestServices(services =>
{
    var factory = new InMemoryDatabaseClientFactory();
    services.AddSingleton<IDatabaseClientFactory>(factory);

    InMemoryFeedIteratorSetup.Register();
});
```

### What Happens

1. Existing `IDatabaseClientFactory` registration is replaced by the in-memory factory
2. Each `GetContainer()` call returns an `InMemoryContainer` (auto-created by `InMemoryCosmosClient`)
3. CRUD operations, SQL queries via `GetItemQueryIterator`, and `GetItemLinqQueryable` all work
4. LINQ `.ToFeedIterator()` requires changing to `.ToFeedIteratorOverridable()` (see note below)

### LINQ `.ToFeedIterator()` Consideration

If your production code uses LINQ chains ending in `.ToFeedIterator()`, you have two choices:

1. **Change to `.ToFeedIteratorOverridable()`** — one-token change per call site. Add the `CosmosDB.InMemoryEmulator.ProductionExtensions` NuGet to your production project and call `InMemoryFeedIteratorSetup.Register()` in test setup. See the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for a detailed walkthrough.
2. **Use Pattern 6 instead** — zero production code changes. See below.

If your production code only uses SQL queries (`GetItemQueryIterator`) or point reads (`ReadItemAsync`), no LINQ changes are needed — Pattern 5 works as-is.

### Pros

- ✅ Simple — minimal setup, no HTTP handler wiring
- ✅ Fast — no HTTP pipeline overhead
- ✅ Auto-creates containers on demand
- ✅ Works with any factory interface shape

### Cons

- ❌ LINQ `.ToFeedIterator()` requires a one-token production code change
- ❌ Lower fidelity — bypasses the SDK's HTTP pipeline (SQL queries still use the same [query engine](SQL-Queries))
- ❌ No [fault injection](API-Reference) or query logging

---

## Pattern 6: Custom Factory Interface — `FakeCosmosHandler` (Zero Production Changes)

**Same scenario as Pattern 5, but uses `FakeCosmosHandler`** so that production code stays completely untouched — including `.ToFeedIterator()`, all LINQ operations, and SDK-level behaviours.

### Test Setup

```csharp
public class CosmosDbFixture : IDatabaseClientFactory, IDisposable
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();
    private readonly CosmosClient _client;

    private static readonly Dictionary<string, string> PartitionKeys = new()
    {
        ["transactions"] = "/merchantId",
        ["idempotency-locks"] = "/endpoint",
        ["orders"] = "/customerId",
        // Add all your containers and their partition key paths
    };

    public CosmosDbFixture()
    {
        var handlers = new Dictionary<string, FakeCosmosHandler>();

        foreach (var (name, partitionKeyPath) in PartitionKeys)
        {
            var container = new InMemoryContainer(name, partitionKeyPath);
            _containers[name] = container;
            handlers[name] = new FakeCosmosHandler(container);
        }

        var router = FakeCosmosHandler.CreateRouter(handlers);

        _client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(router)
            });
    }

    public Container GetContainer(string containerName)
        => _client.GetContainer("test-db", containerName);

    /// <summary>Direct access to the underlying InMemoryContainer for seeding test data.</summary>
    public InMemoryContainer GetInMemoryContainer(string containerName)
        => _containers[containerName];

    public void Dispose() => _client.Dispose();
}
```

> ⚠️ **Serialization: camelCase `id` required.** When using `FakeCosmosHandler`, the Cosmos SDK serializes your C# objects to JSON before sending them over HTTP. If your models use PascalCase `Id`, the JSON will contain `"Id"` instead of `"id"`, and upserts/creates will fail with _"Item must have an 'id' property"_. Fix this by configuring the serializer:
>
> ```csharp
> new CosmosClientOptions
> {
>     ConnectionMode = ConnectionMode.Gateway,
>     HttpClientFactory = () => new HttpClient(router),
>     Serializer = new CosmosJsonDotNetSerializer(
>         new JsonSerializerSettings
>         {
>             ContractResolver = new CamelCasePropertyNamesContractResolver()
>         })
> }
> ```
>
> This applies to Pattern 6 and any manual `FakeCosmosHandler` setup where you create the `CosmosClient` yourself. For Patterns 1–3 (the DI extension methods), the `CosmosClient` is created internally with the SDK's default serializer — ensure your models use lowercase `id` via attributes (e.g. `[JsonProperty("id")]`) or naming conventions. See [Troubleshooting](Troubleshooting) for more common issues.

Register in test DI:

```csharp
builder.ConfigureTestServices(services =>
{
    var fixture = new CosmosDbFixture();
    services.AddSingleton<IDatabaseClientFactory>(fixture);
});
```

### What Happens

1. A real `CosmosClient` is created, but wired to `FakeCosmosHandler` — all HTTP requests are intercepted in-process
2. The SDK's full pipeline executes (LINQ → SQL translation, partition key routing, continuation tokens)
3. `.ToFeedIterator()` works unchanged — the SDK translates LINQ to SQL, sends it over HTTP, and the handler executes it via the [SQL query engine](SQL-Queries)
4. SQL queries, CRUD, patches, batches — everything goes through the real SDK pipeline

### Seeding Test Data

Seed data through the `InMemoryContainer` directly (not through the `CosmosClient`):

```csharp
var container = fixture.GetInMemoryContainer("transactions");
await container.CreateItemAsync(
    new Transaction { Id = "tx-1", MerchantId = "m-1" },
    new PartitionKey("m-1"));
```

### Pros

- ✅ **Zero production code changes** — `.ToFeedIterator()` stays as-is, no `ProductionExtensions` package needed
- ✅ Highest fidelity — exercises the full Cosmos SDK HTTP pipeline
- ✅ [Fault injection](API-Reference) available via `handler.FaultInjector`
- ✅ Query and request logging available
- ✅ Works with any factory interface shape

### Cons

- ❌ More setup code (connection string, handler wiring, container map)
- ❌ Slightly slower than Pattern 5 (HTTP pipeline overhead, though still in-process)
- ❌ Requires `ConnectionMode.Gateway`
- ❌ Containers must be pre-declared (not auto-created on demand)

> **Related:** For test sequence diagram tracking with Pattern 6, see the [TestTrackingDiagrams CosmosDB Extension](https://github.com/lemonlion/TestTrackingDiagrams/wiki/Integration-CosmosDB-Extension) integration guide. It shows how to wrap the router with a tracking handler via `WithHttpMessageHandlerWrapper` to capture Cosmos operations in your test diagrams.

---

## Choosing Between Pattern 5 and Pattern 6

| | Pattern 5 (Direct) | Pattern 6 (FakeCosmosHandler) |
|---|---|---|
| **Production code changes** | One-token per LINQ `.ToFeedIterator()` call | **None** |
| **LINQ fidelity** | LINQ-to-Objects | SDK translates LINQ → SQL (higher fidelity) |
| **SQL query fidelity** | [CosmosSqlParser](SQL-Queries) | [CosmosSqlParser](SQL-Queries) (same engine) |
| **Setup complexity** | Minimal | Moderate |
| **Fault injection** | No | Yes |
| **Best for** | Projects with no LINQ, or willing to use `.ToFeedIteratorOverridable()` | Projects that want zero changes, or need highest fidelity |

For a broader comparison of all three integration layers, see [Integration Approaches](Integration-Approaches).

---

## Configuration Options

### `InMemoryCosmosOptions` (for `UseInMemoryCosmosDB` and `UseInMemoryCosmosDB<TClient>`)

```csharp
services.UseInMemoryCosmosDB(options =>
{
    // Add containers with custom partition key paths
    options.AddContainer("orders", "/customerId");
    options.AddContainer("customers", "/id", databaseName: "customer-db");

    // Override the default database name (defaults to "in-memory-db")
    options.DatabaseName = "my-database";

    // Callback after client creation (e.g. capture reference)
    options.OnClientCreated = client =>
    {
        // client is a real CosmosClient backed by FakeCosmosHandler
    };

    // Callback for each FakeCosmosHandler after creation (e.g. seed data, configure fault injection)
    options.OnHandlerCreated = (containerName, handler) =>
    {
        var backingContainer = handler.BackingContainer;
        backingContainer.CreateItemAsync(new { id = "seed-1", customerId = "c1" },
            new PartitionKey("c1")).Wait();
    };

    // Register InMemoryFeedIteratorSetup (default: true).
    // Only relevant for UseInMemoryCosmosDB<TClient>(); the non-generic overload
    // uses FakeCosmosHandler which handles .ToFeedIterator() natively.
    options.RegisterFeedIteratorSetup = true;

    // Wrap the FakeCosmosHandler with a custom DelegatingHandler (e.g. for logging, tracking).
    // The function receives the handler/router and must return the outermost handler.
    // Only applies to UseInMemoryCosmosDB() (not the generic <TClient> overload).
    // Added in v2.0.5.
    options.WithHttpMessageHandlerWrapper(fakeHandler =>
        new MyLoggingHandler(fakeHandler));
});
```

### `InMemoryContainerOptions` (for `UseInMemoryCosmosContainers`)

```csharp
services.UseInMemoryCosmosContainers(options =>
{
    options.AddContainer("orders", "/customerId");

    // Disable automatic feed iterator setup
    options.RegisterFeedIteratorSetup = false;

    // Callback for each container after creation
    options.OnContainerCreated = container =>
    {
        container.DefaultTimeToLive = 3600; // 1 hour TTL
    };
});
```

### `ContainerConfig` Record

```csharp
public record ContainerConfig(
    string ContainerName,
    string PartitionKeyPath = "/id",
    string? DatabaseName = null);
```

See the [API Reference](API-Reference) for full property details on all classes.

---

## Service Lifetime Behaviour

All three DI methods **preserve the lifetime of existing registrations**:

- If production registered `Container` as **scoped**, the in-memory replacement is also scoped
- If production registered `CosmosClient` as **singleton**, the in-memory replacement is also singleton
- If no existing registration is found, **singleton** is used as the default

This means your test DI graph has the same lifetime characteristics as production.

---

## Complete WebApplicationFactory Example

```csharp
public class OrderApiTests : IClassFixture<OrderApiFactory>
{
    private readonly HttpClient _client;

    public OrderApiTests(OrderApiFactory factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task GetOrder_ReturnsOrder()
    {
        // Arrange — seed via API or direct container access
        var response = await _client.PostAsJsonAsync("/api/orders",
            new { customerId = "cust-1", product = "Widget" });
        response.EnsureSuccessStatusCode();

        // Act
        var order = await _client.GetFromJsonAsync<Order>("/api/orders/cust-1");

        // Assert
        order.Should().NotBeNull();
        order!.Product.Should().Be("Widget");
    }
}

public class OrderApiFactory : WebApplicationFactory<Program>
{
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureTestServices(services =>
        {
            services.UseInMemoryCosmosDB(options => options
                .AddContainer("orders", "/customerId"));
        });
    }
}
```

For simpler unit tests that don't need DI or `WebApplicationFactory`, see [Unit Testing](Unit-Testing).

---

## Troubleshooting

### "ToFeedIterator is only supported on Cosmos LINQ query operations"

Your production code uses `.ToFeedIterator()` which requires the real Cosmos SDK LINQ provider. Two fixes:

1. **Recommended:** Use `UseInMemoryCosmosDB()` — it uses `FakeCosmosHandler` internally, so `.ToFeedIterator()` works unchanged
2. If using `UseInMemoryCosmosContainers()` or direct `InMemoryContainer`: change `.ToFeedIterator()` to `.ToFeedIteratorOverridable()` in production code and add `CosmosDB.InMemoryEmulator.ProductionExtensions` to your production project

See the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for a detailed comparison of both approaches.

### Container not found / empty results

Ensure the container names and partition key paths in your test setup match what production code expects:

```csharp
// If production does: client.GetContainer("MyDb", "orders")
// Then test setup needs:
options.AddContainer("orders", "/customerId");  // container name must match
```

### Multiple Container instances resolve to the same container

Each `AddContainer()` call creates a separate `InMemoryContainer` instance. If your production code resolves `Container` from DI, it gets the first one registered. If you need multiple containers, consider Pattern 3 (repos call `client.GetContainer()`) instead of resolving multiple `Container` instances from DI.

For more troubleshooting tips, see the [Troubleshooting](Troubleshooting) page.
