# Unit Testing with InMemoryContainer

For unit tests that don't use dependency injection, create an `InMemoryContainer` directly and pass it to the code under test. Since `InMemoryContainer` extends the SDK's abstract `Container` class, any code that depends on `Container` works without changes.

## Basic Setup

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

var container = new InMemoryContainer("my-container", "/partitionKey");
```

Both arguments are optional (defaulting to `"in-memory-container"` and `"/id"`), but in practice you'll want to specify them to match your production container. The container is ready to use immediately — no database or client needed.

For [composite (hierarchical) partition keys](Features#composite-hierarchical-partition-keys):

```csharp
var container = new InMemoryContainer("my-container", new[] { "/tenantId", "/userId" });
```

Or for advanced settings like [unique key policies](Features#unique-key-policies), pass a `ContainerProperties` instance:

```csharp
var props = new ContainerProperties("my-container", "/partitionKey")
{
    UniqueKeyPolicy = new UniqueKeyPolicy
    {
        UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
    }
};
var container = new InMemoryContainer(props);
```

See [API Reference — InMemoryContainer](API-Reference#inmemorycontainer) for the full constructor and property reference.

## CRUD Operations

```csharp
[Fact]
public async Task CreateAndRead_RoundTrips()
{
    var container = new InMemoryContainer("orders", "/customerId");

    // Create
    var order = new { id = "1", customerId = "cust-1", product = "Widget", price = 9.99 };
    var createResponse = await container.CreateItemAsync(order, new PartitionKey("cust-1"));
    Assert.Equal(HttpStatusCode.Created, createResponse.StatusCode);

    // Read
    var readResponse = await container.ReadItemAsync<dynamic>("1", new PartitionKey("cust-1"));
    Assert.Equal("Widget", (string)readResponse.Resource.product);

    // Upsert
    var updated = new { id = "1", customerId = "cust-1", product = "Widget", price = 12.99 };
    await container.UpsertItemAsync(updated, new PartitionKey("cust-1"));

    // Delete
    await container.DeleteItemAsync<dynamic>("1", new PartitionKey("cust-1"));
}
```

## SQL Queries

```csharp
[Fact]
public async Task Query_FiltersCorrectly()
{
    var container = new InMemoryContainer("products", "/category");

    await container.CreateItemAsync(
        new { id = "1", category = "electronics", name = "Laptop", price = 999 },
        new PartitionKey("electronics"));
    await container.CreateItemAsync(
        new { id = "2", category = "electronics", name = "Mouse", price = 25 },
        new PartitionKey("electronics"));
    await container.CreateItemAsync(
        new { id = "3", category = "books", name = "C# in Depth", price = 40 },
        new PartitionKey("books"));

    var query = new QueryDefinition("SELECT * FROM c WHERE c.price > @min")
        .WithParameter("@min", 30);

    var iterator = container.GetItemQueryIterator<dynamic>(query);
    var results = new List<dynamic>();
    while (iterator.HasMoreResults)
    {
        var page = await iterator.ReadNextAsync();
        results.AddRange(page);
    }

    Assert.Equal(2, results.Count);
}
```

See [SQL Queries](SQL-Queries) for the full reference — 120+ built-in functions, JOINs, GROUP BY, aggregates, and more.

## LINQ Queries

`InMemoryContainer` supports `GetItemLinqQueryable<T>()`, which returns a LINQ-to-Objects queryable:

```csharp
[Fact]
public async Task Linq_FiltersAndProjects()
{
    var container = new InMemoryContainer("orders", "/customerId");

    await container.CreateItemAsync(
        new Order { Id = "1", CustomerId = "cust-1", Product = "Widget", Price = 9.99m },
        new PartitionKey("cust-1"));
    await container.CreateItemAsync(
        new Order { Id = "2", CustomerId = "cust-1", Product = "Gadget", Price = 49.99m },
        new PartitionKey("cust-1"));

    var expensive = container.GetItemLinqQueryable<Order>(true)
        .Where(o => o.Price > 20)
        .ToList();

    Assert.Single(expensive);
    Assert.Equal("Gadget", expensive[0].Product);
}
```

> **Note:** LINQ queries execute as LINQ-to-Objects here — C# delegates run against in-memory data, bypassing the SDK's LINQ-to-SQL translation. If you need to test that translation, use the [`FakeCosmosHandler` approach](Integration-Approaches#cosmosclient--fakecosmoshandler-high-fidelity) instead. See also [LINQ Support](Features#linq-support) and [Known Limitations: LINQ Queryable Options Ignored](Known-Limitations#11-linq-queryable-options-ignored).

## Testing a Repository

The most common pattern — your repository depends on `Container`, and you pass in an `InMemoryContainer`:

```csharp
// Production code (unchanged)
public class OrderRepository
{
    private readonly Container _container;
    public OrderRepository(Container container) => _container = container;

    public async Task<Order> CreateAsync(Order order)
    {
        var response = await _container.CreateItemAsync(order, new PartitionKey(order.CustomerId));
        return response.Resource;
    }

    public async Task<List<Order>> GetByCustomerAsync(string customerId)
    {
        var query = new QueryDefinition("SELECT * FROM c WHERE c.customerId = @id")
            .WithParameter("@id", customerId);

        var iterator = _container.GetItemQueryIterator<Order>(query);
        var results = new List<Order>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }
}

// Test
public class OrderRepositoryTests
{
    [Fact]
    public async Task GetByCustomer_ReturnsOnlyMatchingOrders()
    {
        var container = new InMemoryContainer("orders", "/customerId");
        var repo = new OrderRepository(container);

        await repo.CreateAsync(new Order { Id = "1", CustomerId = "cust-1", Product = "Widget" });
        await repo.CreateAsync(new Order { Id = "2", CustomerId = "cust-2", Product = "Gadget" });
        await repo.CreateAsync(new Order { Id = "3", CustomerId = "cust-1", Product = "Sprocket" });

        var results = await repo.GetByCustomerAsync("cust-1");

        Assert.Equal(2, results.Count);
        Assert.All(results, o => Assert.Equal("cust-1", o.CustomerId));
    }
}
```

## Patch Operations

[Patch operations](Features#patch-operations) let you update individual properties without replacing the entire document:

```csharp
[Fact]
public async Task Patch_UpdatesSingleProperty()
{
    var container = new InMemoryContainer("orders", "/customerId");

    await container.CreateItemAsync(
        new { id = "1", customerId = "cust-1", status = "pending" },
        new PartitionKey("cust-1"));

    await container.PatchItemAsync<dynamic>("1", new PartitionKey("cust-1"),
        new[] { PatchOperation.Set("/status", "shipped") });

    var response = await container.ReadItemAsync<dynamic>("1", new PartitionKey("cust-1"));
    Assert.Equal("shipped", (string)response.Resource.status);
}
```

## ETag & Optimistic Concurrency

Every write returns an ETag that you can use for [optimistic concurrency control](Features#etag--optimistic-concurrency):

```csharp
[Fact]
public async Task Replace_WithETag_DetectsConflict()
{
    var container = new InMemoryContainer("orders", "/customerId");

    var created = await container.CreateItemAsync(
        new { id = "1", customerId = "cust-1", status = "pending" },
        new PartitionKey("cust-1"));
    var etag = created.ETag;

    // First replace succeeds
    await container.ReplaceItemAsync(
        new { id = "1", customerId = "cust-1", status = "shipped" }, "1",
        new PartitionKey("cust-1"),
        new ItemRequestOptions { IfMatchEtag = etag });

    // Second replace with the stale ETag fails
    var ex = await Assert.ThrowsAsync<CosmosException>(() =>
        container.ReplaceItemAsync(
            new { id = "1", customerId = "cust-1", status = "cancelled" }, "1",
            new PartitionKey("cust-1"),
            new ItemRequestOptions { IfMatchEtag = etag }));

    Assert.Equal(HttpStatusCode.PreconditionFailed, ex.StatusCode);
}
```

## Container Configuration

Configure container-level settings via properties after construction, or by passing `ContainerProperties` to the constructor.

### TTL

Set `DefaultTimeToLive` (in seconds) to enable automatic [item expiration](Features#ttl--expiration):

```csharp
var container = new InMemoryContainer("orders", "/customerId");
container.DefaultTimeToLive = 3600; // items expire after 1 hour
```

Or via `ContainerProperties`:

```csharp
var container = new InMemoryContainer(
    new ContainerProperties("orders", "/customerId") { DefaultTimeToLive = 3600 });
```

### FeedRange Count

Set `FeedRangeCount` to simulate [multiple physical partitions](Features#feedrange-support) for testing FeedRange-scoped queries and change feed iterators:

```csharp
var container = new InMemoryContainer("events", "/tenantId");
container.FeedRangeCount = 4;
```

See [API Reference — InMemoryContainer](API-Reference#inmemorycontainer) for all settable properties.

## Test Isolation

Each `InMemoryContainer` instance starts empty, so creating a new instance per test provides clean isolation automatically. If you share a container across tests (e.g. via a fixture), call `ClearItems()` to reset state:

```csharp
container.ClearItems(); // removes all documents
```

For snapshot-based testing, you can export and import the full container state as JSON:

```csharp
var snapshot = container.ExportState();

// ... run operations that modify data ...

container.ClearItems();
container.ImportState(snapshot); // restore to the saved state
```

## When to Use DI Instead

If your system under test uses dependency injection, consider the [DI approach](Getting-Started#quick-start--integration-tests-with-di) instead — it requires zero setup in individual tests and automatically replaces all Cosmos registrations. See [Dependency Injection](Dependency-Injection) for all supported patterns.

For maximum SDK fidelity (testing the full HTTP pipeline, LINQ-to-SQL translation, fault injection), see [`FakeCosmosHandler`](Integration-Approaches#cosmosclient--fakecosmoshandler-high-fidelity).

If something isn't working as expected, check [Troubleshooting](Troubleshooting) and [Known Limitations](Known-Limitations).
