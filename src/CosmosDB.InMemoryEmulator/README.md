# CosmosDB.InMemoryEmulator

A high-fidelity, in-memory implementation of the Azure Cosmos DB SDK for .NET — purpose-built for fast, reliable component and integration testing.

**Drop-in replacements for `CosmosClient`, `Database`, and `Container`** — full CRUD, SQL queries, LINQ, patch, batches, change feed, and more. No network, no emulator process, no Docker, no Azure subscription required.

## Core Types

| Type | Purpose |
|------|---------|
| `InMemoryCosmos` | **Recommended entry point** — one-liner setup with full SDK fidelity, test setup, and fault injection |
| `FakeCosmosHandler` | HTTP-level interceptor — zero production code changes, full SDK fidelity |
| `InMemoryContainer` | Backing in-memory storage (used internally; direct usage deprecated in 4.0) |
| `InMemoryCosmosClient` | Deprecated in 4.0 — use `InMemoryCosmos.Create()` instead |

## Quick Start

```csharp
using CosmosDB.InMemoryEmulator;
using Microsoft.Azure.Cosmos;

// Recommended: InMemoryCosmos — one-liner, full SDK fidelity
using var cosmos = InMemoryCosmos.Create("my-container", "/partitionKey");

await cosmos.Container.CreateItemAsync(
    new { id = "1", partitionKey = "pk1", name = "Alice" },
    new PartitionKey("pk1"));

var iterator = cosmos.Container.GetItemQueryIterator<dynamic>(
    "SELECT * FROM c WHERE c.name = 'Alice'");
```

## Multi-Container

```csharp
using var cosmos = InMemoryCosmos.Builder()
    .AddContainer("orders", "/customerId")
    .AddContainer("products", "/categoryId")
    .Build();

var orders = cosmos.Containers["orders"];
var products = cosmos.Containers["products"];
```

## Test Setup & Fault Injection

```csharp
// Register UDFs, stored procedures, seed data
cosmos.SetupContainer("orders").RegisterUdf("myUdf", args => args[0]);

// Inject faults: 429 throttling, 503 errors, timeouts
cosmos.Handler.FaultInjector = req => new HttpResponseMessage((HttpStatusCode)429);
```

## Features

Full CRUD, 125+ SQL functions, LINQ, bulk operations, transactional batches, change feed, patch operations, ETag concurrency, partition keys, TTL, fault injection, DI integration, state persistence, point-in-time restore, unique key policies, computed properties.

## Documentation

Full documentation: [GitHub Wiki](https://github.com/lemonlion/CosmosDB.InMemoryEmulator/wiki)
