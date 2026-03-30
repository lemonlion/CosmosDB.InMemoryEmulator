# API Reference

Complete reference for all public classes and methods in the CosmosDB.InMemoryEmulator packages.

## Package: CosmosDB.InMemoryEmulator

### `InMemoryContainer`

In-memory implementation of `Container`. The primary class for most testing scenarios.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.Container`

#### Constructors

```csharp
// Single partition key
InMemoryContainer(string id = "in-memory-container", string partitionKeyPath = "/id")

// Composite partition keys
InMemoryContainer(string id, IReadOnlyList<string> partitionKeyPaths)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Id` | `string` | Container identifier |
| `Database` | `Database` | Stubbed Database instance |
| `Scripts` | `Scripts` | For stored procedure/UDF execution |
| `DefaultTimeToLive` | `int?` | Container-level TTL in seconds |
| `PartitionKeyPaths` | `IReadOnlyList<string>` | Partition key path(s) |
| `ItemCount` | `int` | Number of items currently stored |
| `IndexingPolicy` | `IndexingPolicy` | Accepted but does not affect query behaviour |

#### CRUD Methods (Typed)

| Method | Returns |
|--------|---------|
| `CreateItemAsync<T>(T item, PartitionKey?, ItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |
| `ReadItemAsync<T>(string id, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |
| `UpsertItemAsync<T>(T item, PartitionKey?, ItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |
| `ReplaceItemAsync<T>(T item, string id, PartitionKey?, ItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |
| `DeleteItemAsync<T>(string id, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |
| `PatchItemAsync<T>(string id, PartitionKey, IReadOnlyList<PatchOperation>, PatchItemRequestOptions?, CancellationToken)` | `ItemResponse<T>` |

#### CRUD Methods (Stream)

| Method | Returns |
|--------|---------|
| `CreateItemStreamAsync(Stream, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `ReadItemStreamAsync(string id, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `UpsertItemStreamAsync(Stream, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `ReplaceItemStreamAsync(Stream, string id, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `DeleteItemStreamAsync(string id, PartitionKey, ItemRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `PatchItemStreamAsync(string id, PartitionKey, IReadOnlyList<PatchOperation>, PatchItemRequestOptions?, CancellationToken)` | `ResponseMessage` |

#### Query Methods

| Method | Returns |
|--------|---------|
| `GetItemQueryIterator<T>(QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator<T>` |
| `GetItemQueryIterator<T>(string queryText, string?, QueryRequestOptions?)` | `FeedIterator<T>` |
| `GetItemQueryStreamIterator(QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator` |
| `GetItemQueryStreamIterator(string queryText, string?, QueryRequestOptions?)` | `FeedIterator` |
| `GetItemLinqQueryable<T>(bool?, string?, QueryRequestOptions?, CosmosLinqSerializerOptions?)` | `IOrderedQueryable<T>` |

#### ReadMany

| Method | Returns |
|--------|---------|
| `ReadManyItemsAsync<T>(IReadOnlyList<(string, PartitionKey)>, ReadManyRequestOptions?, CancellationToken)` | `FeedResponse<T>` |
| `ReadManyItemsStreamAsync(IReadOnlyList<(string, PartitionKey)>, ReadManyRequestOptions?, CancellationToken)` | `ResponseMessage` |

#### Change Feed

| Method | Returns |
|--------|---------|
| `GetChangeFeedIterator<T>(ChangeFeedStartFrom, ChangeFeedMode, ChangeFeedRequestOptions?)` | `FeedIterator<T>` |
| `GetChangeFeedIterator<T>(long checkpoint)` | `FeedIterator<T>` |
| `GetChangeFeedStreamIterator(ChangeFeedStartFrom, ChangeFeedMode, ChangeFeedRequestOptions?)` | `FeedIterator` |
| `GetChangeFeedProcessorBuilder<T>(string, ChangesHandler<T>)` | `ChangeFeedProcessorBuilder` |
| `GetChangeFeedProcessorBuilder<T>(string, ChangeFeedHandler<T>)` | `ChangeFeedProcessorBuilder` |
| `GetChangeFeedCheckpoint()` | `long` |

#### Transactional Batch

| Method | Returns |
|--------|---------|
| `CreateTransactionalBatch(PartitionKey)` | `TransactionalBatch` |

#### Test Infrastructure

| Method | Returns | Description |
|--------|---------|-------------|
| `ClearItems()` | `void` | Remove all data |
| `ExportState()` | `string` | Export state as JSON |
| `ImportState(string json)` | `void` | Import state from JSON |
| `ExportStateToFile(string filePath)` | `void` | Export to file |
| `ImportStateFromFile(string filePath)` | `void` | Import from file |
| `RegisterUdf(string name, Func<object[], object>)` | `void` | Register UDF for SQL |
| `RegisterStoredProcedure(string id, Func<PartitionKey, dynamic[], string>)` | `void` | Register sproc |
| `DeregisterStoredProcedure(string id)` | `void` | Remove sproc |

#### Container Management

| Method | Returns |
|--------|---------|
| `ReadContainerAsync(ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `ReplaceContainerAsync(ContainerProperties, ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `DeleteContainerAsync(ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `DeleteAllItemsByPartitionKeyStreamAsync(PartitionKey, RequestOptions?, CancellationToken)` | `ResponseMessage` |
| `GetFeedRangesAsync(CancellationToken)` | `IReadOnlyList<FeedRange>` |

---

### `InMemoryCosmosClient`

In-memory implementation of `CosmosClient`. Manages databases and containers.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.CosmosClient`  
**Sealed:** No (can be subclassed for Pattern 2 typed clients)

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Endpoint` | `Uri` | Always `https://localhost:8081/` |
| `ClientOptions` | `CosmosClientOptions` | Default options |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `GetDatabase(string id)` | `Database` | Get or create database (lazy) |
| `GetContainer(string dbId, string containerId)` | `Container` | Get or create container (lazy) |
| `CreateDatabaseAsync(string id, int?, ...)` | `DatabaseResponse` | Create database (409 if exists) |
| `CreateDatabaseIfNotExistsAsync(string id, int?, ...)` | `DatabaseResponse` | Create or get database |
| `GetDatabaseQueryIterator<T>(...)` | `FeedIterator<T>` | List all databases |
| `ReadAccountAsync()` | `AccountProperties` | Returns synthetic account info |

---

### `InMemoryDatabase`

In-memory implementation of `Database`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.Database`

#### Constructors

```csharp
InMemoryDatabase(string id)
InMemoryDatabase(string id, InMemoryCosmosClient client)
```

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `GetContainer(string id)` | `Container` | Get or create container (lazy) |
| `CreateContainerAsync(string id, string pkPath, ...)` | `ContainerResponse` | Create (409 if exists) |
| `CreateContainerIfNotExistsAsync(string id, string pkPath, ...)` | `ContainerResponse` | Create or get |
| `ReadAsync(...)` | `DatabaseResponse` | Read database properties |
| `DeleteAsync(...)` | `DatabaseResponse` | Delete database and all containers |

---

### `FakeCosmosHandler`

HTTP message handler that intercepts Cosmos SDK requests and serves them from an `InMemoryContainer`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `System.Net.Http.HttpMessageHandler`

#### Constructors

```csharp
FakeCosmosHandler(InMemoryContainer container)
FakeCosmosHandler(InMemoryContainer container, FakeCosmosHandlerOptions options)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `RequestLog` | `IReadOnlyCollection<string>` | Recorded HTTP requests |
| `QueryLog` | `IReadOnlyCollection<string>` | Recorded SQL queries |
| `FaultInjector` | `Func<HttpRequestMessage, HttpResponseMessage?>?` | Fault injection delegate |
| `FaultInjectorIncludesMetadata` | `bool` | Whether faults affect metadata routes |

#### Static Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `CreateRouter(IReadOnlyDictionary<string, FakeCosmosHandler>)` | `HttpMessageHandler` | Multi-container router |
| `VerifySdkCompatibilityAsync()` | `Task` | SDK compatibility self-test |

---

### `FakeCosmosHandlerOptions`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `CacheTtl` | `TimeSpan` | 5 min | Query result cache TTL |
| `CacheMaxEntries` | `int` | 100 | Max cached query results |
| `PartitionKeyRangeCount` | `int` | 1 | Number of simulated partition ranges |

---

### `InMemoryFeedIterator<T>`

In-memory implementation of `FeedIterator<T>`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.FeedIterator<T>`

#### Constructors

```csharp
InMemoryFeedIterator(IReadOnlyList<T> items, int? maxItemCount = null, int initialOffset = 0)
InMemoryFeedIterator(IEnumerable<T> source, int? maxItemCount = null)
InMemoryFeedIterator(Func<IReadOnlyList<T>> factory, int? maxItemCount = null)
```

---

### `InMemoryTransactionalBatch`

In-memory implementation of `TransactionalBatch`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.TransactionalBatch`

#### Constructor

```csharp
InMemoryTransactionalBatch(InMemoryContainer container, PartitionKey partitionKey)
```

---

### `InMemoryFeedIteratorSetup`

Wires up `ToFeedIteratorOverridable()` to return `InMemoryFeedIterator<T>`.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Description |
|--------|-------------|
| `Register()` | Enable in-memory feed iterator interception |
| `Deregister()` | Disable interception, revert to SDK behaviour |

---

### `ServiceCollectionExtensions`

DI extension methods for `IServiceCollection`.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Description |
|--------|-------------|
| `UseInMemoryCosmosDB(Action<InMemoryCosmosOptions>?)` | Replace `CosmosClient` + `Container` |
| `UseInMemoryCosmosContainers(Action<InMemoryContainerOptions>?)` | Replace `Container` only |
| `UseInMemoryCosmosDB<TClient>(Action<InMemoryCosmosOptions>?)` | Replace typed client subclass |

---

### `InMemoryCosmosOptions`

Options for `UseInMemoryCosmosDB()`.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Containers` | `List<ContainerConfig>` | Empty | Container configurations |
| `DatabaseName` | `string?` | `"in-memory-db"` | Default database name |
| `RegisterFeedIteratorSetup` | `bool` | `true` | Auto-register feed iterator factory |
| `OnClientCreated` | `Action<InMemoryCosmosClient>?` | null | Post-creation callback |

### `InMemoryContainerOptions`

Options for `UseInMemoryCosmosContainers()`.

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Containers` | `List<ContainerConfig>` | Empty | Container configurations |
| `RegisterFeedIteratorSetup` | `bool` | `true` | Auto-register feed iterator factory |
| `OnContainerCreated` | `Action<InMemoryContainer>?` | null | Per-container callback |

### `ContainerConfig`

```csharp
public record ContainerConfig(
    string ContainerName,
    string PartitionKeyPath = "/id",
    string? DatabaseName = null);
```

---

### `CosmosSqlParser`

Cosmos DB SQL parser (internal details exposed for advanced use).

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Returns | Description |
|--------|---------|-------------|
| `Parse(string sql)` | `CosmosSqlQuery` | Parse SQL (throws on failure) |
| `TryParse(string sql, out CosmosSqlQuery)` | `bool` | Parse SQL (returns false on failure) |

---

## Package: CosmosDB.InMemoryEmulator.ProductionExtensions

### `CosmosOverridableFeedIteratorExtensions`

**Namespace:** `CosmosDB.InMemoryEmulator.ProductionExtensions`

| Member | Type | Description |
|--------|------|-------------|
| `ToFeedIteratorOverridable<T>(this IQueryable<T>)` | Extension method | Drop-in replacement for `.ToFeedIterator()` |
| `FeedIteratorFactory` | `Func<object, object>?` | Per-async-flow factory (AsyncLocal) |
| `StaticFallbackFactory` | `Func<object, object>?` | Global fallback for `new Thread()` scenarios |

The extension method checks factories in this order:
1. `FeedIteratorFactory` (AsyncLocal, per-async-flow)
2. `StaticFallbackFactory` (static, global fallback)
3. `.ToFeedIterator()` (real Cosmos SDK — production path)
