# CosmosDB.InMemoryEmulator

A high-fidelity, in-memory implementation of the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

**Drop-in replacement for `Microsoft.Azure.Cosmos.Container`** — no network, no emulator process, no Docker, no Azure subscription required.

## Core Types

| Type | Purpose |
|------|---------|
| `InMemoryContainer` | Drop-in `Container` replacement with full CRUD, queries, patch, batches, change feed |
| `FakeCosmosHandler` | HTTP-level interceptor — zero production code changes, full SDK fidelity |
| `InMemoryCosmosClient` | Drop-in `CosmosClient` replacement with database/container management |

## Quick Start

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

// Direct usage
var container = new InMemoryContainer("my-container", "/partitionKey");

await container.CreateItemAsync(
    new { id = "1", partitionKey = "pk1", name = "Alice" },
    new PartitionKey("pk1"));

var iterator = container.GetItemQueryIterator<dynamic>(
    "SELECT * FROM c WHERE c.name = 'Alice'");
```

## FakeCosmosHandler (Zero Code Changes)

```csharp
var inMemoryContainer = new InMemoryContainer("orders", "/customerId");
var handler = new FakeCosmosHandler(inMemoryContainer);

var client = new CosmosClient(
    "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
    new CosmosClientOptions
    {
        ConnectionMode = ConnectionMode.Gateway,
        HttpClientFactory = () => new HttpClient(handler)
    });

var container = client.GetContainer("db", "orders");

// All operations work through the same Container reference:
await container.CreateItemAsync(order, new PartitionKey(order.CustomerId));
var query = container.GetItemLinqQueryable<Order>()
    .Where(o => o.Status == "active")
    .ToFeedIterator();
```

## Features

Full CRUD, 100+ SQL functions, LINQ, transactional batches, change feed, patch operations, ETag concurrency, partition keys, TTL, fault injection, DI integration, state persistence, unique key policies.

## Documentation

Full documentation: [GitHub Wiki](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki)
