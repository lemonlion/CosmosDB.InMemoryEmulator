# Seeding Data

Guide to populating the in-memory emulator with test data. Covers all DI patterns, direct instantiation,
shared fixtures, bulk seeding, state persistence, and automatic state persistence between test runs.

## Overview

There are five main approaches to seeding data, each suited to different situations:

| Approach | When to use |
|---|---|
| [DI callbacks](#seeding-via-di-callbacks) (`OnHandlerCreated` / `OnContainerCreated`) | Seed during service registration — data is ready before the first test runs |
| [SDK methods](#seeding-via-sdk-methods) (`CreateItemAsync` / `UpsertItemAsync`) | Seed through the real SDK pipeline — highest fidelity, per-test data |
| [State persistence](#seeding-from-a-file-or-snapshot) (`ImportState` / `ImportStateFromFile`) | Restore a pre-built dataset from JSON — fast, repeatable, shareable |
| [Direct container access](#seeding-with-direct-container-access) (resolve from DI or capture reference) | Seed after the host is built but before tests run |
| [Automatic persistence](#persisting-state-between-test-runs) (`StatePersistenceDirectory`) | Automatically save/restore container state between test runs — zero manual export/import |

All approaches work with every [DI pattern](Dependency-Injection) and with [direct instantiation](Unit-Testing).

---

## Seeding via DI Callbacks

The DI extension methods expose callbacks that fire during service registration. These are the simplest
way to seed data when using `ConfigureTestServices`.

### `UseInMemoryCosmosDB` — `OnHandlerCreated`

`OnHandlerCreated` gives you the `FakeCosmosHandler` for each container. Access the backing
`InMemoryContainer` via `handler.BackingContainer` and seed using standard SDK methods:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.OnHandlerCreated = (containerName, handler) =>
        {
            var backing = handler.BackingContainer;
            backing.CreateItemAsync(
                new { id = "order-1", customerId = "cust-1", total = 99.99 },
                new PartitionKey("cust-1")).Wait();

            backing.CreateItemAsync(
                new { id = "order-2", customerId = "cust-2", total = 149.50 },
                new PartitionKey("cust-2")).Wait();
        };
    });
});
```

> **Note:** The callback runs synchronously during DI registration, so use `.Wait()` or `.GetAwaiter().GetResult()` for async methods. This is safe because the in-memory container never blocks.

### `UseInMemoryCosmosContainers` — `OnContainerCreated`

`OnContainerCreated` gives you the `InMemoryContainer` directly:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosContainers(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.OnContainerCreated = container =>
        {
            container.CreateItemAsync(
                new { id = "order-1", customerId = "cust-1", total = 99.99 },
                new PartitionKey("cust-1")).Wait();
        };
    });
});
```

### `UseInMemoryCosmosDB<TClient>` — `OnClientCreated`

For [typed client patterns](Dependency-Injection#pattern-2-typed-cosmosclient-subclasses),
seed through the client after it's created:

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB<EmployeeCosmosClient>(options =>
    {
        options.AddContainer("employees", "/departmentId");
        options.OnClientCreated = client =>
        {
            var container = client.GetContainer("in-memory-db", "employees");
            container.CreateItemAsync(
                new { id = "emp-1", departmentId = "eng", name = "Alice" },
                new PartitionKey("eng")).Wait();
        };
    });
});
```

### Seeding Multiple Containers

When your app uses multiple containers, seed each one in the `OnHandlerCreated` callback —
it fires once per container:

```csharp
services.UseInMemoryCosmosDB(options =>
{
    options.AddContainer("orders", "/customerId");
    options.AddContainer("customers", "/id");
    options.AddContainer("products", "/categoryId");

    options.OnHandlerCreated = (containerName, handler) =>
    {
        var backing = handler.BackingContainer;
        switch (containerName)
        {
            case "orders":
                backing.CreateItemAsync(
                    new { id = "order-1", customerId = "cust-1", total = 42.00 },
                    new PartitionKey("cust-1")).Wait();
                break;

            case "customers":
                backing.CreateItemAsync(
                    new { id = "cust-1", name = "Alice", tier = "gold" },
                    new PartitionKey("cust-1")).Wait();
                break;

            case "products":
                backing.CreateItemAsync(
                    new { id = "prod-1", categoryId = "widgets", name = "Deluxe Widget" },
                    new PartitionKey("widgets")).Wait();
                break;
        }
    };
});
```

---

## Seeding via SDK Methods

Seed through the standard `Container` SDK methods for highest fidelity — your seed data goes
through the same pipeline as production code.

### Per-Test Seeding (WebApplicationFactory)

Resolve the `Container` from DI after building the host, and seed before making requests:

```csharp
public class OrderApiTests : IClassFixture<OrderApiFactory>
{
    private readonly OrderApiFactory _factory;
    private readonly HttpClient _client;

    public OrderApiTests(OrderApiFactory factory)
    {
        _factory = factory;
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task GetOrder_ReturnsSeededOrder()
    {
        // Seed via SDK
        var container = _factory.Services.GetRequiredService<Container>();
        await container.CreateItemAsync(
            new Order { Id = "order-1", CustomerId = "cust-1", Product = "Widget" },
            new PartitionKey("cust-1"));

        // Act
        var response = await _client.GetAsync("/api/orders/order-1?pk=cust-1");

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var order = await response.Content.ReadFromJsonAsync<Order>();
        order!.Product.Should().Be("Widget");
    }
}
```

### Per-Test Seeding (Direct Instantiation)

For unit tests without DI, seed the `InMemoryContainer` or `InMemoryCosmosClient` directly:

```csharp
public class OrderRepositoryTests
{
    private readonly InMemoryContainer _container = new("orders", "/customerId");

    [Fact]
    public async Task GetOrder_ReturnsItem()
    {
        // Seed
        await _container.CreateItemAsync(
            new Order { Id = "order-1", CustomerId = "cust-1", Product = "Widget" },
            new PartitionKey("cust-1"));

        // Act
        var repo = new OrderRepository(_container);
        var order = await repo.GetOrder("order-1", "cust-1");

        // Assert
        order.Product.Should().Be("Widget");
    }
}
```

### Helper Method Pattern

Extract a reusable `SeedAsync` method to keep tests clean:

```csharp
public class OrderTests
{
    private readonly InMemoryContainer _container = new("orders", "/customerId");

    private async Task SeedAsync(params Order[] orders)
    {
        foreach (var order in orders)
            await _container.CreateItemAsync(order, new PartitionKey(order.CustomerId));
    }

    [Fact]
    public async Task Query_ReturnsMatchingOrders()
    {
        await SeedAsync(
            new Order { Id = "1", CustomerId = "cust-1", Product = "Widget", Total = 10 },
            new Order { Id = "2", CustomerId = "cust-1", Product = "Gadget", Total = 25 },
            new Order { Id = "3", CustomerId = "cust-2", Product = "Widget", Total = 15 });

        var results = await _container.GetItemQueryIterator<Order>(
            new QueryDefinition("SELECT * FROM c WHERE c.customerId = @pk")
                .WithParameter("@pk", "cust-1"))
            .ReadNextAsync();

        results.Should().HaveCount(2);
    }
}
```

### Bulk Seeding via `UpsertItemAsync`

Use `UpsertItemAsync` when you need idempotent seeding — safe to run multiple times without
conflict errors:

```csharp
private async Task SeedAsync(Container container, IEnumerable<Order> orders)
{
    foreach (var order in orders)
        await container.UpsertItemAsync(order, new PartitionKey(order.CustomerId));
}
```

### Parallel Bulk Seeding

For large datasets, seed in parallel with `Task.WhenAll`:

```csharp
private async Task SeedManyAsync(Container container, int count)
{
    var tasks = Enumerable.Range(1, count).Select(i =>
    {
        var order = new Order
        {
            Id = $"order-{i}",
            CustomerId = $"cust-{i % 20}",
            Product = $"Product-{i}",
            Total = i * 1.5m
        };
        return container.UpsertItemAsync(order, new PartitionKey(order.CustomerId));
    });

    await Task.WhenAll(tasks);
}
```

---

## Seeding from a File or Snapshot

Use state persistence to restore a pre-built dataset. This is ideal for large, stable seed
datasets that don't change between tests, or for sharing data between repositories.

### Export a Snapshot

Build the dataset once and export it to a JSON file:

```csharp
var container = new InMemoryContainer("orders", "/customerId");

// Build your data
await container.CreateItemAsync(new { id = "1", customerId = "c1", product = "Widget" }, new PartitionKey("c1"));
await container.CreateItemAsync(new { id = "2", customerId = "c2", product = "Gadget" }, new PartitionKey("c2"));
// ... more items

// Export to file
container.ExportStateToFile("TestData/orders-seed.json");
```

### Import at Test Time

Restore from a file during setup:

```csharp
// Direct instantiation
var container = new InMemoryContainer("orders", "/customerId");
container.ImportStateFromFile("TestData/orders-seed.json");

// Via DI callback
services.UseInMemoryCosmosContainers(options =>
{
    options.AddContainer("orders", "/customerId");
    options.OnContainerCreated = container =>
    {
        container.ImportStateFromFile("TestData/orders-seed.json");
    };
});

// Via OnHandlerCreated
services.UseInMemoryCosmosDB(options =>
{
    options.AddContainer("orders", "/customerId");
    options.OnHandlerCreated = (name, handler) =>
    {
        handler.BackingContainer.ImportStateFromFile("TestData/orders-seed.json");
    };
});
```

### Import from a JSON String

If the seed data is embedded or generated programmatically:

```csharp
var json = """
{
  "items": [
    { "id": "1", "customerId": "c1", "product": "Widget" },
    { "id": "2", "customerId": "c2", "product": "Gadget" }
  ]
}
""";

container.ImportState(json);
```

### Sharing Snapshots Between Test Suites

Check seed files into source control so they're available everywhere:

```
tests/
  TestData/
    orders-seed.json
    customers-seed.json
    products-seed.json
  MyApp.Tests/
    OrderTests.cs
```

Set the files as **Content / Copy if newer** in your `.csproj`:

```xml
<ItemGroup>
  <Content Include="TestData\*.json" CopyToOutputDirectory="PreserveNewest" />
</ItemGroup>
```

---

## Seeding with Direct Container Access

When you need to seed after the host is built but before tests run, capture a reference to the
`InMemoryContainer` or `FakeCosmosHandler` and seed directly.

### Capture via `OnHandlerCreated`

```csharp
public class OrderApiFactory : WebApplicationFactory<Program>
{
    public FakeCosmosHandler? OrdersHandler { get; private set; }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureTestServices(services =>
        {
            services.UseInMemoryCosmosDB(options =>
            {
                options.AddContainer("orders", "/customerId");
                options.OnHandlerCreated = (name, handler) =>
                {
                    if (name == "orders")
                        OrdersHandler = handler;
                };
            });
        });
    }
}

public class OrderApiTests : IClassFixture<OrderApiFactory>
{
    private readonly OrderApiFactory _factory;

    public OrderApiTests(OrderApiFactory factory) => _factory = factory;

    [Fact]
    public async Task GetOrder_WithSeededData()
    {
        // Seed directly into the backing container
        var backing = _factory.OrdersHandler!.BackingContainer;
        await backing.CreateItemAsync(
            new { id = "order-1", customerId = "cust-1", total = 99.99 },
            new PartitionKey("cust-1"));

        var client = _factory.CreateClient();
        var response = await client.GetAsync("/api/orders/order-1");
        response.StatusCode.Should().Be(HttpStatusCode.OK);
    }
}
```

### Capture via `OnContainerCreated`

```csharp
public class OrderApiFactory : WebApplicationFactory<Program>
{
    public InMemoryContainer? OrdersContainer { get; private set; }

    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.ConfigureTestServices(services =>
        {
            services.UseInMemoryCosmosContainers(options =>
            {
                options.AddContainer("orders", "/customerId");
                options.OnContainerCreated = container =>
                {
                    OrdersContainer = container;
                };
            });
        });
    }
}
```

### Custom Factory Interface (Pattern 6)

When using a [custom factory interface](Dependency-Injection#pattern-6-custom-factory-interface--fakecosmoshandler-zero-production-changes),
expose the `InMemoryContainer` for seeding:

```csharp
public class CosmosDbFixture : IDatabaseClientFactory, IDisposable
{
    private readonly ConcurrentDictionary<string, InMemoryContainer> _containers = new();
    private readonly CosmosClient _client;

    private static readonly Dictionary<string, string> PartitionKeys = new()
    {
        ["orders"] = "/customerId",
        ["customers"] = "/id",
    };

    public CosmosDbFixture()
    {
        var handlers = new Dictionary<string, FakeCosmosHandler>();
        foreach (var (name, pkPath) in PartitionKeys)
        {
            var container = new InMemoryContainer(name, pkPath);
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

    /// <summary>Direct access for seeding test data.</summary>
    public InMemoryContainer GetInMemoryContainer(string containerName)
        => _containers[containerName];

    public void Dispose() => _client.Dispose();
}

// Seed in tests:
var orders = fixture.GetInMemoryContainer("orders");
await orders.CreateItemAsync(
    new Order { Id = "1", CustomerId = "cust-1" },
    new PartitionKey("cust-1"));
```

---

## Persisting State Between Test Runs

The emulator can automatically save and restore container state between test runs. This is useful
when you want data created during one test run to be available in the next, without manually calling
`ExportState`/`ImportState`.

### How It Works

Set `StatePersistenceDirectory` on the DI options. On startup, each container checks for an existing
state file and loads it. On disposal, each container saves its current state back to the file.

| Step | What happens |
|---|---|
| **Startup** | If a state file exists for the container, `ImportState` loads it automatically |
| **First run** | No file exists — container starts empty (no error) |
| **Disposal** | Container saves its state via `ExportState` to the file |
| **Next run** | Container loads the previously saved state |

### Via `UseInMemoryCosmosDB` (Pattern 1)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

Files are named `{DatabaseName}_{ContainerName}.json` — e.g. `test-state/in-memory-db_orders.json`.

### Via `UseInMemoryCosmosContainers` (Pattern 3)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosContainers(options =>
    {
        options.AddContainer("orders", "/customerId");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

Files are named `{ContainerName}.json` — e.g. `test-state/orders.json`.

### Via `UseInMemoryCosmosDB<TClient>` (Pattern 2)

```csharp
builder.ConfigureTestServices(services =>
{
    services.UseInMemoryCosmosDB<EmployeeCosmosClient>(options =>
    {
        options.AddContainer("employees", "/departmentId");
        options.StatePersistenceDirectory = "./test-state";
    });
});
```

### Direct Instantiation (No DI)

For unit tests without DI, use `StateFilePath` and `LoadPersistedState()` directly:

```csharp
public class OrderRepositoryTests : IDisposable
{
    private readonly InMemoryContainer _container;

    public OrderRepositoryTests()
    {
        _container = new InMemoryContainer("orders", "/customerId");
        _container.StateFilePath = "./test-state/orders.json";
        _container.LoadPersistedState(); // Loads existing state, or no-op if file doesn't exist
    }

    [Fact]
    public async Task CreateOrder_PersistsBetweenRuns()
    {
        await _container.CreateItemAsync(
            new Order { Id = "order-1", CustomerId = "cust-1" },
            new PartitionKey("cust-1"));
    }

    public void Dispose()
    {
        _container.Dispose(); // Saves state to file
    }
}
```

### File Naming Convention

| DI Extension | File name pattern | Example |
|---|---|---|
| `UseInMemoryCosmosDB` | `{DatabaseName}_{ContainerName}.json` | `in-memory-db_orders.json` |
| `UseInMemoryCosmosDB<T>` | `{DatabaseName}_{ContainerName}.json` | `in-memory-db_employees.json` |
| `UseInMemoryCosmosContainers` | `{ContainerName}.json` | `orders.json` |
| Direct (`StateFilePath`) | Whatever you set | `./my-state/orders.json` |

### Behaviour Details

- **First run:** No state file → container starts empty. State is saved on disposal.
- **Subsequent runs:** State file is loaded on creation. Any changes are saved on disposal.
- **Directory creation:** The directory is created automatically if it doesn't exist (on save).
- **ETags and timestamps:** `ImportState` generates new ETags and timestamps on load. This matches
  the existing `ImportState` behaviour — items get fresh system properties each run.
- **Change feed:** Not persisted. The change feed starts fresh each run.
- **Multiple containers:** Each container gets its own file. Multiple containers in the same
  persistence directory coexist without conflict.

### Adding State Files to `.gitignore`

If you don't want state files committed to source control:

```gitignore
# In-memory emulator persisted state
test-state/
```

If you _do_ want to commit them (e.g. as seed data that evolves over time), that also works — the
files are standard JSON in the same `{"items":[...]}` format used by `ExportState`.

---

## Resetting Data Between Tests

The `InMemoryContainer` is shared across tests within the same factory or fixture. To avoid
test pollution, clear data between tests.

### `ClearItems()`

Removes all items, ETags, timestamps, and change feed entries:

```csharp
public class OrderTests : IAsyncLifetime
{
    private readonly InMemoryContainer _container = new("orders", "/customerId");

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _container.ClearItems();
        return ValueTask.CompletedTask;
    }
}
```

### Clear via Captured Reference (WebApplicationFactory)

```csharp
public class OrderApiTests : IClassFixture<OrderApiFactory>, IAsyncLifetime
{
    private readonly OrderApiFactory _factory;

    public OrderApiTests(OrderApiFactory factory) => _factory = factory;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync()
    {
        _factory.OrdersHandler?.BackingContainer.ClearItems();
        return ValueTask.CompletedTask;
    }
}
```

### Isolated Tests — New Container Per Test

If test isolation matters more than speed, create a fresh container for each test:

```csharp
[Fact]
public async Task EachTest_GetsCleanState()
{
    var container = new InMemoryContainer("orders", "/customerId");

    await container.CreateItemAsync(
        new { id = "1", customerId = "c1" },
        new PartitionKey("c1"));

    container.ItemCount.Should().Be(1);
}
```

---

## Choosing an Approach

| Scenario | Recommended approach |
|---|---|
| Integration tests with `WebApplicationFactory` | [DI callbacks](#seeding-via-di-callbacks) for shared data, [SDK methods](#seeding-via-sdk-methods) for per-test data |
| Unit tests with `InMemoryContainer` | [SDK methods](#seeding-via-sdk-methods) with a `SeedAsync` helper |
| Large, stable reference datasets | [State persistence](#seeding-from-a-file-or-snapshot) with `ImportStateFromFile` |
| Data that evolves across test runs | [Automatic persistence](#persisting-state-between-test-runs) with `StatePersistenceDirectory` |
| Performance / load tests | [Parallel bulk seeding](#parallel-bulk-seeding) or [state persistence](#seeding-from-a-file-or-snapshot) |
| Custom factory interface (Pattern 5/6) | [Direct container access](#custom-factory-interface-pattern-6) via exposed `GetInMemoryContainer()` |
| Test isolation required | [New container per test](#isolated-tests--new-container-per-test) or [ClearItems()](#clearitems) in teardown |
| Seeding across multiple containers | [Multiple container callbacks](#seeding-multiple-containers) |

---

## See Also

- [Dependency Injection](Dependency-Injection) — full DI setup guide for all six patterns
- [Unit Testing](Unit-Testing) — using `InMemoryContainer` and `InMemoryCosmosClient` directly
- [Integration Approaches](Integration-Approaches) — choosing between the three integration layers
- [API Reference](API-Reference) — `InMemoryContainer`, `FakeCosmosHandler`, `InMemoryCosmosClient`
- [Features](Features) — state persistence, change feed, TTL, and more
