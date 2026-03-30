# Getting Started

## Installation

### Test project only (most common)

Add the main emulator package to your **test project**:

```
dotnet add package CosmosDB.InMemoryEmulator
```

### If using `.ToFeedIteratorOverridable()` in production code

If your production code uses LINQ with `.ToFeedIterator()` and you want to swap it for the testable `.ToFeedIteratorOverridable()`, add the lightweight production extensions package to your **production project**:

```
dotnet add package CosmosDB.InMemoryEmulator.ProductionExtensions
```

This package contains only the `ToFeedIteratorOverridable()` extension method — no test infrastructure, no NSubstitute, no Superpower parser. It has a single dependency on `Microsoft.Azure.Cosmos`.

> **Note:** If you use the `FakeCosmosHandler` approach (recommended), you don't need this package at all — production code stays completely untouched.

### Requirements

| Requirement | Version |
|-------------|---------|
| .NET | 8.0+ |
| Microsoft.Azure.Cosmos | 3.57.1+ |

## Quick Start — 5 Minutes to Your First Test

### Step 1: Create a container

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

var container = new InMemoryContainer("my-container", "/partitionKey");
```

### Step 2: Perform CRUD operations

```csharp
// Create
var item = new { id = "1", partitionKey = "pk1", name = "Alice", age = 30 };
var response = await container.CreateItemAsync(item, new PartitionKey("pk1"));
// response.StatusCode == HttpStatusCode.Created

// Read
var read = await container.ReadItemAsync<dynamic>("1", new PartitionKey("pk1"));
// read.Resource.name == "Alice"

// Update
var updated = new { id = "1", partitionKey = "pk1", name = "Alice", age = 31 };
await container.UpsertItemAsync(updated, new PartitionKey("pk1"));

// Delete
await container.DeleteItemAsync<dynamic>("1", new PartitionKey("pk1"));
```

### Step 3: Query with SQL

```csharp
var query = new QueryDefinition("SELECT * FROM c WHERE c.age > @age")
    .WithParameter("@age", 25);

var iterator = container.GetItemQueryIterator<dynamic>(query);
while (iterator.HasMoreResults)
{
    var page = await iterator.ReadNextAsync();
    foreach (var doc in page)
        Console.WriteLine(doc);
}
```

### Step 4: Use in a real test

```csharp
public class OrderServiceTests
{
    [Fact]
    public async Task PlaceOrder_SavesOrderToContainer()
    {
        // Arrange
        var container = new InMemoryContainer("orders", "/customerId");
        var service = new OrderService(container);

        // Act
        await service.PlaceOrderAsync("cust-1", "Widget", 9.99m);

        // Assert
        var iterator = container.GetItemQueryIterator<Order>(
            "SELECT * FROM c WHERE c.customerId = 'cust-1'");
        var page = await iterator.ReadNextAsync();
        page.Should().ContainSingle()
            .Which.ProductName.Should().Be("Widget");
    }
}
```

## Next Steps

- [Choose an integration approach](integration-approaches.md) — understand the trade-offs between direct usage, FakeCosmosHandler, and DI extensions
- [SQL query support](sql-queries.md) — full reference for the built-in query engine
- [Dependency injection guide](dependency-injection.md) — step-by-step DI integration
- [API reference](api-reference.md) — complete class and method reference
