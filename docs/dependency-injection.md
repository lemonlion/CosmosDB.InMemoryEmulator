# Dependency Injection Guide

Step-by-step guide for integrating CosmosDB.InMemoryEmulator with dependency injection in your test projects.

## Overview

The emulator provides three DI extension methods on `IServiceCollection`:

| Method | Purpose |
|--------|---------|
| `UseInMemoryCosmosDB()` | Replaces `CosmosClient` + `Container` registrations |
| `UseInMemoryCosmosContainers()` | Replaces only `Container` registrations |
| `UseInMemoryCosmosDB<TClient>()` | Replaces a typed `CosmosClient` subclass registration |

All three are designed to be called from `ConfigureTestServices` inside a `WebApplicationFactory` or equivalent test host setup.

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

That's it! The existing `Container` factory registrations are **preserved** — they'll resolve against the new `InMemoryCosmosClient` and call `GetContainer()` just like production code does, automatically creating `InMemoryContainer` instances.

### What Happens (Zero-Config)

1. The existing `CosmosClient` registration is removed
2. A new `InMemoryCosmosClient` is registered as `CosmosClient` (singleton)
3. Existing `Container` factory registrations are **kept**
4. When `Container` is resolved, the production factory calls `client.GetContainer("MyDatabase", "orders")` on the in-memory client, which creates an `InMemoryContainer` automatically
5. `InMemoryFeedIteratorSetup.Register()` is called so `.ToFeedIteratorOverridable()` works

> **Note:** Auto-detected containers default to `/id` as the partition key path. This works for most tests since production code passes explicit `PartitionKey` values. If your tests rely on partition-key-path-based extraction (e.g. `PartitionKey.None`), use the explicit form below.

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
3. A new `InMemoryCosmosClient` is registered as `CosmosClient` (singleton)
4. A new `InMemoryContainer` is registered as `Container`, preserving the original lifetime
5. `InMemoryFeedIteratorSetup.Register()` is called so `.ToFeedIteratorOverridable()` works

### Multiple Containers

```csharp
services.UseInMemoryCosmosDB(options => options
    .AddContainer("orders", "/customerId")
    .AddContainer("customers", "/id")
    .AddContainer("products", "/categoryId"));
```

Each container is registered as a separate `Container` service descriptor. If your production code resolves multiple `Container` instances, they'll each get a distinct in-memory container.

---

## Pattern 2: Typed CosmosClient Subclasses

**Used when production code has multiple typed `CosmosClient` subclasses** — each connecting to a different Cosmos DB account or database. Common in multi-tenant or domain-segmented architectures.

### Production Code (unchanged)

```csharp
// Production typed clients
public class BiometricCosmosClient : CosmosClient
{
    public BiometricCosmosClient(string connectionString) : base(connectionString) { }
}

public class OOBCosmosClient : CosmosClient
{
    public OOBCosmosClient(string connectionString) : base(connectionString) { }
}

// Registration
services.AddSingleton(new BiometricCosmosClient(config["Biometric:ConnectionString"]));
services.AddSingleton(new OOBCosmosClient(config["OOB:ConnectionString"]));

// Repos resolve the typed client
public class BiometricRepository
{
    private readonly BiometricCosmosClient _client;
    public BiometricRepository(BiometricCosmosClient client) => _client = client;
}
```

### Test Project Setup

**Step 1:** Create test-only shadow types that inherit from `InMemoryCosmosClient` instead of `CosmosClient`:

```csharp
// In your test project — shadows the production type
namespace MyApp.Cosmos;  // Same namespace as production

public class BiometricCosmosClient : InMemoryCosmosClient { }
public class OOBCosmosClient : InMemoryCosmosClient { }
```

> **Important:** These must be in the same namespace as the production types, or use `using` aliases, so that DI resolves the test type for repos that depend on the typed client.

**Step 2:** Replace registrations in test setup:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB<BiometricCosmosClient>(options => options
        .AddContainer("biometric-data", "/userId"));

    services.UseInMemoryCosmosDB<OOBCosmosClient>(options => options
        .AddContainer("oob-messages", "/channelId"));
});
```

### What Happens

1. The existing `BiometricCosmosClient` registration is removed
2. A new `BiometricCosmosClient` (which is `InMemoryCosmosClient`) is registered, preserving lifetime
3. No `Container` or base `CosmosClient` is registered — Pattern 2 repos call `client.GetContainer()` directly
4. Repos that depend on `BiometricCosmosClient` resolve the in-memory version

### Why Shadow Types Are Needed

The generic method requires `TClient : InMemoryCosmosClient, new()`. This is because:

- `CosmosClient` has `internal virtual` methods that prevent runtime proxying (NSubstitute/Castle.Core)
- .NET DI type-checks at resolution time — you can't register `InMemoryCosmosClient` as `BiometricCosmosClient`

The one-liner shadow type is the only approach that works. It goes in the **test project only** — production code is never changed.

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

The `InMemoryCosmosClient` creates containers lazily, so `GetContainer()` in your repositories will return the pre-configured `InMemoryContainer` with the correct partition key path.

---

## Pattern 4: Container-Only Replacement

**Use when you only want to replace `Container` registrations but keep the real `CosmosClient`** (or don't have one registered).

### Test Setup

```csharp
services.UseInMemoryCosmosContainers(options => options
    .AddContainer("orders", "/customerId"));
```

### What Happens

1. Existing `Container` registrations are removed
2. New `InMemoryContainer` instances are registered, preserving lifetime
3. `CosmosClient` is NOT touched
4. `InMemoryFeedIteratorSetup.Register()` is called

---

## Configuration Options

### `InMemoryCosmosOptions` (for `UseInMemoryCosmosDB`)

```csharp
services.UseInMemoryCosmosDB(options =>
{
    // Add containers with custom partition key paths
    options.AddContainer("orders", "/customerId");
    options.AddContainer("customers", "/id", databaseName: "customer-db");

    // Override the default database name (defaults to "in-memory-db")
    options.DatabaseName = "my-database";

    // Disable automatic InMemoryFeedIteratorSetup.Register()
    options.RegisterFeedIteratorSetup = false;

    // Callback after client creation (e.g. seed data)
    options.OnClientCreated = client =>
    {
        var container = (InMemoryContainer)client.GetContainer("my-database", "orders");
        container.CreateItemAsync(new { id = "seed-1", customerId = "c1" },
            new PartitionKey("c1")).Wait();
    };
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

---

## Troubleshooting

### "ToFeedIterator is only supported on Cosmos LINQ query operations"

Your production code uses `.ToFeedIterator()` which requires the real Cosmos SDK LINQ provider. Two fixes:

1. **Recommended:** Switch to [Approach 2 (FakeCosmosHandler)](integration-approaches.md#approach-2-fakecosmoshandler-recommended-for-high-fidelity) — no code changes needed
2. Change `.ToFeedIterator()` to `.ToFeedIteratorOverridable()` in production code and add `CosmosDB.InMemoryEmulator.ProductionExtensions` to your production project

### Container not found / empty results

Ensure the container names and partition key paths in your test setup match what production code expects:

```csharp
// If production does: client.GetContainer("MyDb", "orders")
// Then test setup needs:
options.AddContainer("orders", "/customerId");  // container name must match
```

### Multiple Container instances resolve to the same container

Each `AddContainer()` call creates a separate `InMemoryContainer` instance. If your production code resolves `Container` from DI, it gets the first one registered. If you need multiple containers, consider using `InMemoryCosmosClient` directly and having repos call `client.GetContainer()`.
