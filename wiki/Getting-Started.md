## Installation

Add the emulator package to your **test project**:

```
dotnet add package CosmosDB.InMemoryEmulator
```

### Requirements

| Requirement | Version |
|-------------|---------|
| .NET | 8.0+ |
| Microsoft.Azure.Cosmos | 3.58.0+ |

## Quick Start — Integration Tests with DI

The recommended approach uses `UseInMemoryCosmosDB()` to replace your real Cosmos registrations with in-memory equivalents. **No production code changes required.**

### Step 1: Your production code (unchanged)

```csharp
// Program.cs / Startup.cs
services.AddSingleton<CosmosClient>(sp =>
    new CosmosClient(configuration["CosmosDb:ConnectionString"]));

services.AddSingleton<Container>(sp =>
{
    var client = sp.GetRequiredService<CosmosClient>();
    return client.GetContainer("MyDatabase", "orders");
});
```

```csharp
// OrderRepository.cs — depends on Container via DI
public class OrderRepository
{
    private readonly Container _container;
    public OrderRepository(Container container) => _container = container;

    public async Task<Order> CreateAsync(Order order)
    {
        var response = await _container.CreateItemAsync(order, new PartitionKey(order.CustomerId));
        return response.Resource;
    }

    public async Task<Order?> GetAsync(string id, string customerId)
    {
        try
        {
            var response = await _container.ReadItemAsync<Order>(id, new PartitionKey(customerId));
            return response.Resource;
        }
        catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
        {
            return null;
        }
    }
}
```

### Step 2: Replace Cosmos with in-memory in tests

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;

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

That single line replaces the real `CosmosClient` with a real `CosmosClient` backed by [`FakeCosmosHandler`](Integration-Approaches#cosmosclient--fakecosmoshandler-high-fidelity) — all HTTP requests are intercepted in-process. Your existing `Container` factory registrations are **preserved** — they resolve against the new client and route through in-memory storage automatically.

> ✅ **LINQ `.ToFeedIterator()` just works.** Because `UseInMemoryCosmosDB()` uses `FakeCosmosHandler` internally, the SDK’s own LINQ provider translates your LINQ expressions into Cosmos DB SQL. No production code changes or extra packages needed.

> ⚠️ **Serialization matters.** The Cosmos SDK serializes your C# objects using Newtonsoft.Json by default. If your model properties use PascalCase (e.g. `Id`), you need `[JsonProperty("id")]` attributes from `Newtonsoft.Json` — otherwise creates/upserts will fail with _"Item must have an 'id' property"_. See [Troubleshooting](Troubleshooting#item-must-have-an-id-property) for details.

### Step 3: Write a test

```csharp
public class OrderTests : IClassFixture<MyAppFactory>
{
    private readonly HttpClient _client;

    public OrderTests(MyAppFactory factory) => _client = factory.CreateClient();

    [Fact]
    public async Task CreateOrder_ReturnsCreatedOrder()
    {
        var order = new { customerId = "cust-1", product = "Widget", price = 9.99 };

        var response = await _client.PostAsJsonAsync("/api/orders", order);

        response.StatusCode.Should().Be(HttpStatusCode.Created);
    }
}
```

### Specifying partition key paths

By default, auto-detected containers use `/id` as the partition key path. If your tests need a specific path:

```csharp
services.UseInMemoryCosmosDB(options => options
    .AddContainer("orders", "/customerId"));
```

See the full [Dependency Injection guide](Dependency-Injection) for multiple containers, typed clients, custom factory interfaces, and more. For a comparison of all integration approaches, see [Integration Approaches](Integration-Approaches).

## Quick Start — Unit Tests (No DI)

For unit tests where you pass a `Container` directly to the class under test:

```csharp
public class OrderRepositoryTests
{
    [Fact]
    public async Task CreateAsync_StoresOrder()
    {
        // Arrange
        var container = new InMemoryContainer("orders", "/customerId");
        var repo = new OrderRepository(container);
        var order = new Order { Id = "1", CustomerId = "cust-1", Product = "Widget" };

        // Act
        await repo.CreateAsync(order);

        // Assert
        var stored = await repo.GetAsync("1", "cust-1");
        stored.Should().NotBeNull();
        stored!.Product.Should().Be("Widget");
    }
}
```

See the full [Unit Testing guide](Unit-Testing) for CRUD, queries, and more examples.

> **Note:** When using `InMemoryContainer` directly, LINQ `.ToFeedIterator()` requires changing to `.ToFeedIteratorOverridable()` in production code. If you want LINQ to work unchanged, use `UseInMemoryCosmosDB()` instead — see [Feed Iterator Usage](Feed-Iterator-Usage-Guide) for details.

## Next Steps

- **[Integration Approaches](Integration-Approaches)** — compare DI extensions, direct usage, and `FakeCosmosHandler`; decision flowchart
- **[Dependency Injection](Dependency-Injection)** — full DI patterns (typed clients, custom factories, multi-container)
- **[Unit Testing](Unit-Testing)** — direct `InMemoryContainer` usage without DI
- **[Feed Iterator Usage](Feed-Iterator-Usage-Guide)** — how `.ToFeedIterator()` and `.ToFeedIteratorOverridable()` work
- **[SQL Queries](SQL-Queries)** — full reference for the built-in query engine (120+ functions)
- **[Features](Features)** — patch, batches, change feed, ETags, TTL, stored procs, state persistence, PITR
- **[API Reference](API-Reference)** — complete class and method reference
- **[Known Limitations](Known-Limitations)** — behavioural differences from real Cosmos DB
- **[Troubleshooting](Troubleshooting)** — common errors and how to fix them
