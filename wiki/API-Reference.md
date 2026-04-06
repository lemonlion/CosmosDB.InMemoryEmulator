Complete reference for all public classes and methods in the CosmosDB.InMemoryEmulator packages.

> **See also:** [Getting Started](Getting-Started) · [Integration Approaches](Integration-Approaches) · [Dependency Injection](Dependency-Injection) · [SQL Queries](SQL-Queries) · [Known Limitations](Known-Limitations)

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

// With container properties (unique key policies, etc.)
InMemoryContainer(ContainerProperties containerProperties)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Id` | `string` | Container identifier |
| `Database` | `Database` | Stubbed Database instance |
| `Conflicts` | `Conflicts` | Stubbed Conflicts instance |
| `Scripts` | `Scripts` | For stored procedure/UDF execution |
| `DefaultTimeToLive` | `int?` | Container-level TTL in seconds |
| `PartitionKeyPaths` | `IReadOnlyList<string>` | Partition key path(s) |
| `FeedRangeCount` | `int` | Number of feed ranges (default 1). Set > 1 to simulate multiple physical partitions |
| `ItemCount` | `int` | Number of items currently stored |
| `IndexingPolicy` | `IndexingPolicy` | Accepted but does not affect query behaviour |
| `JsTriggerEngine` | `IJsTriggerEngine` | JavaScript trigger engine. Set via `.UseJsTriggers()` from the [JsTriggers package](#package-cosmosdbinmemoryemulatorjstriggers), or assign a custom implementation |
| `StateFilePath` | `string` | Path for automatic state persistence. Set before calling `LoadPersistedState()` / `Dispose()`. See [State Persistence](State-Persistence) |

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

See [SQL Queries](SQL-Queries) for supported SQL syntax.

| Method | Returns |
|--------|---------|
| `GetItemQueryIterator<T>(QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator<T>` |
| `GetItemQueryIterator<T>(string queryText, string?, QueryRequestOptions?)` | `FeedIterator<T>` |
| `GetItemQueryIterator<T>(FeedRange, QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator<T>` |
| `GetItemQueryStreamIterator(QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator` |
| `GetItemQueryStreamIterator(string queryText, string?, QueryRequestOptions?)` | `FeedIterator` |
| `GetItemQueryStreamIterator(FeedRange, QueryDefinition, string?, QueryRequestOptions?)` | `FeedIterator` |
| `GetItemLinqQueryable<T>(bool?, string?, QueryRequestOptions?, CosmosLinqSerializerOptions?)` | `IOrderedQueryable<T>` |

> **Tip:** For LINQ `.ToFeedIterator()` support in tests, see [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide).

#### ReadMany

| Method | Returns |
|--------|---------|
| `ReadManyItemsAsync<T>(IReadOnlyList<(string, PartitionKey)>, ReadManyRequestOptions?, CancellationToken)` | `FeedResponse<T>` |
| `ReadManyItemsStreamAsync(IReadOnlyList<(string, PartitionKey)>, ReadManyRequestOptions?, CancellationToken)` | `ResponseMessage` |

#### Change Feed

| Method | Returns | Description |
|--------|---------|-------------|
| `GetChangeFeedIterator<T>(ChangeFeedStartFrom, ChangeFeedMode, ChangeFeedRequestOptions?)` | `FeedIterator<T>` | SDK-style iterator. Supports `Incremental` (latest version, deletes filtered) and `FullFidelity` modes |
| `GetChangeFeedIterator<T>(long checkpoint)` | `FeedIterator<T>` | Custom helper — all versions + delete tombstones from given checkpoint position |
| `GetChangeFeedStreamIterator(ChangeFeedStartFrom, ChangeFeedMode, ChangeFeedRequestOptions?)` | `FeedIterator` | Stream variant of SDK-style iterator |
| `GetChangeFeedProcessorBuilder<T>(string, ChangesHandler<T>)` | `ChangeFeedProcessorBuilder` | Legacy handler signature |
| `GetChangeFeedProcessorBuilder<T>(string, ChangeFeedHandler<T>)` | `ChangeFeedProcessorBuilder` | Context-aware handler signature |
| `GetChangeFeedProcessorBuilder(string, ChangeFeedStreamHandler)` | `ChangeFeedProcessorBuilder` | Stream handler signature |
| `GetChangeFeedProcessorBuilderWithManualCheckpoint<T>(string, ChangeFeedHandlerWithManualCheckpoint<T>)` | `ChangeFeedProcessorBuilder` | Manual checkpoint — advances only when `checkpointAsync` is called |
| `GetChangeFeedProcessorBuilderWithManualCheckpoint(string, ChangeFeedStreamHandlerWithManualCheckpoint)` | `ChangeFeedProcessorBuilder` | Stream manual checkpoint — advances only when `checkpointAsync` is called |
| `GetChangeFeedEstimator(string, Container)` | `ChangeFeedEstimator` | Returns a stubbed estimator |
| `GetChangeFeedEstimatorBuilder(string, ChangesEstimationHandler, TimeSpan?)` | `ChangeFeedProcessorBuilder` | Returns a no-op processor builder |
| `GetChangeFeedCheckpoint()` | `long` | Current position in change feed (advances on create, upsert, replace, patch, and delete) |

#### Transactional Batch

| Method | Returns |
|--------|---------|
| `CreateTransactionalBatch(PartitionKey)` | `TransactionalBatch` |

#### Container Management

| Method | Returns |
|--------|---------|
| `ReadContainerAsync(ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `ReadContainerStreamAsync(ContainerRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `ReplaceContainerAsync(ContainerProperties, ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `ReplaceContainerStreamAsync(ContainerProperties, ContainerRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `DeleteContainerAsync(ContainerRequestOptions?, CancellationToken)` | `ContainerResponse` |
| `DeleteContainerStreamAsync(ContainerRequestOptions?, CancellationToken)` | `ResponseMessage` |
| `DeleteAllItemsByPartitionKeyStreamAsync(PartitionKey, RequestOptions?, CancellationToken)` | `ResponseMessage` |
| `GetFeedRangesAsync(CancellationToken)` | `Task<IReadOnlyList<FeedRange>>` |

#### Throughput (Stubbed)

| Method | Returns |
|--------|---------|
| `ReadThroughputAsync(CancellationToken)` | `Task<int?>` (returns 400) |
| `ReadThroughputAsync(RequestOptions, CancellationToken)` | `ThroughputResponse` |
| `ReplaceThroughputAsync(int, RequestOptions?, CancellationToken)` | `ThroughputResponse` |
| `ReplaceThroughputAsync(ThroughputProperties, RequestOptions?, CancellationToken)` | `ThroughputResponse` |

#### Test Infrastructure

| Method | Returns | Description |
|--------|---------|-------------|
| `ClearItems()` | `void` | Remove all data |
| `ExportState()` | `string` | Export state as JSON |
| `ImportState(string json)` | `void` | Import state from JSON |
| `ExportStateToFile(string filePath)` | `void` | Export to file |
| `ImportStateFromFile(string filePath)` | `void` | Import from file |
| `RestoreToPointInTime(DateTimeOffset pointInTime)` | `void` | Restore container to state at given timestamp |
| `RegisterUdf(string name, Func<object[], object>)` | `void` | Register UDF for SQL |
| `RegisterStoredProcedure(string id, Func<PartitionKey, dynamic[], string>)` | `void` | Register sproc |
| `DeregisterStoredProcedure(string id)` | `void` | Remove sproc |
| `RegisterTrigger(string name, TriggerType, TriggerOperation, Func<JObject, JObject> preHandler)` | `void` | Register pre-trigger |
| `RegisterTrigger(string name, TriggerType, TriggerOperation, Action<JObject> postHandler)` | `void` | Register post-trigger |
| `DeregisterTrigger(string name)` | `void` | Remove trigger |
| `LoadPersistedState()` | `void` | Load state from `StateFilePath` if the file exists (no-op if missing). See [State Persistence](State-Persistence) |
| `Dispose()` | `void` | If `StateFilePath` is set, exports state to that file before cleanup. Implements `IDisposable` |

> See [Unit Testing](Unit-Testing) for patterns using these methods.

---

### `InMemoryCosmosClient`

In-memory implementation of `CosmosClient`. Manages databases and containers.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.CosmosClient`  
**Sealed:** No (can be subclassed for typed-client patterns — see [Integration Approaches](Integration-Approaches))

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Endpoint` | `Uri` | Always `https://localhost:8081/` |
| `ClientOptions` | `CosmosClientOptions` | Default options |
| `ResponseFactory` | `CosmosResponseFactory` | Stubbed response factory |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `GetDatabase(string id)` | `Database` | Get or create database (lazy) |
| `GetContainer(string dbId, string containerId)` | `Container` | Get or create container (lazy) |
| `CreateDatabaseAsync(string id, int?, ...)` | `DatabaseResponse` | Create database (409 if exists) |
| `CreateDatabaseAsync(string id, ThroughputProperties?, ...)` | `DatabaseResponse` | Create with throughput properties |
| `CreateDatabaseIfNotExistsAsync(string id, int?, ...)` | `DatabaseResponse` | Create or get database |
| `CreateDatabaseIfNotExistsAsync(string id, ThroughputProperties?, ...)` | `DatabaseResponse` | Create or get with throughput properties |
| `CreateDatabaseStreamAsync(DatabaseProperties, int?, ...)` | `ResponseMessage` | Stream variant |
| `GetDatabaseQueryIterator<T>(string?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List all databases |
| `GetDatabaseQueryIterator<T>(QueryDefinition?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List databases by query |
| `GetDatabaseQueryStreamIterator(string?, string?, QueryRequestOptions?)` | `FeedIterator` | Stream variant |
| `GetDatabaseQueryStreamIterator(QueryDefinition?, string?, QueryRequestOptions?)` | `FeedIterator` | Stream variant |
| `ReadAccountAsync()` | `AccountProperties` | Returns synthetic account info |

---

### `InMemoryDatabase`

In-memory implementation of `Database`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.Database`  
**Sealed:** Yes

#### Constructors

```csharp
InMemoryDatabase(string id)
InMemoryDatabase(string id, InMemoryCosmosClient client)
```

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Id` | `string` | Database identifier |
| `Client` | `CosmosClient` | Parent client (stubbed if not provided) |

#### Container Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `GetContainer(string id)` | `Container` | Get or create container (lazy) |
| `CreateContainerAsync(string id, string pkPath, ...)` | `ContainerResponse` | Create (409 if exists) |
| `CreateContainerAsync(ContainerProperties, int?, ...)` | `ContainerResponse` | Create with properties |
| `CreateContainerAsync(ContainerProperties, ThroughputProperties?, ...)` | `ContainerResponse` | Create with throughput |
| `CreateContainerIfNotExistsAsync(string id, string pkPath, ...)` | `ContainerResponse` | Create or get |
| `CreateContainerIfNotExistsAsync(ContainerProperties, int?, ...)` | `ContainerResponse` | Create or get with properties |
| `CreateContainerIfNotExistsAsync(ContainerProperties, ThroughputProperties?, ...)` | `ContainerResponse` | Create or get with throughput |
| `CreateContainerStreamAsync(ContainerProperties, int?, ...)` | `ResponseMessage` | Stream variant |
| `CreateContainerStreamAsync(ContainerProperties, ThroughputProperties?, ...)` | `ResponseMessage` | Stream variant with throughput |
| `GetContainerQueryIterator<T>(string?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List containers |
| `GetContainerQueryIterator<T>(QueryDefinition?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List containers by query |
| `GetContainerQueryStreamIterator(string?, string?, QueryRequestOptions?)` | `FeedIterator` | Stream variant |
| `GetContainerQueryStreamIterator(QueryDefinition?, string?, QueryRequestOptions?)` | `FeedIterator` | Stream variant |
| `DefineContainer(string name, string pkPath)` | `ContainerBuilder` | Fluent container builder |

#### Database Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `ReadAsync(...)` | `DatabaseResponse` | Read database properties |
| `ReadStreamAsync(...)` | `ResponseMessage` | Stream variant |
| `DeleteAsync(...)` | `DatabaseResponse` | Delete database and all containers |
| `DeleteStreamAsync(...)` | `ResponseMessage` | Stream variant |

#### Throughput (Stubbed)

| Method | Returns |
|--------|---------|
| `ReadThroughputAsync(CancellationToken)` | `Task<int?>` |
| `ReadThroughputAsync(RequestOptions, CancellationToken)` | `ThroughputResponse` |
| `ReplaceThroughputAsync(int, RequestOptions?, CancellationToken)` | `ThroughputResponse` |
| `ReplaceThroughputAsync(ThroughputProperties, RequestOptions?, CancellationToken)` | `ThroughputResponse` |

#### User Management

| Method | Returns | Description |
|--------|---------|-------------|
| `GetUser(string id)` | `User` | Get or create user (lazy) |
| `CreateUserAsync(string id, ...)` | `UserResponse` | Create user (409 if exists) |
| `UpsertUserAsync(string id, ...)` | `UserResponse` | Create or replace user |
| `GetUserQueryIterator<T>(string?, string?, QueryRequestOptions?)` | `Task<FeedIterator<T>>` | List users |
| `GetUserQueryIterator<T>(QueryDefinition?, string?, QueryRequestOptions?)` | `Task<FeedIterator<T>>` | List users by query |

---

### `InMemoryUser`

In-memory implementation of `User`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.User`  
**Sealed:** Yes

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Id` | `string` | User identifier |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `ReadAsync(...)` | `UserResponse` | Read user properties |
| `ReplaceAsync(UserProperties, ...)` | `UserResponse` | Replace user properties |
| `DeleteAsync(...)` | `UserResponse` | Delete user |
| `GetPermission(string id)` | `Permission` | Get permission by id |
| `CreatePermissionAsync(PermissionProperties, int?, ...)` | `PermissionResponse` | Create permission (409 if exists) |
| `UpsertPermissionAsync(PermissionProperties, int?, ...)` | `PermissionResponse` | Create or replace permission |
| `GetPermissionQueryIterator<T>(string?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List permissions |
| `GetPermissionQueryIterator<T>(QueryDefinition?, string?, QueryRequestOptions?)` | `FeedIterator<T>` | List permissions by query |

---

### `InMemoryPermission`

In-memory implementation of `Permission`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.Permission`  
**Sealed:** Yes

#### Properties

| Property | Type | Description |
|----------|------|-------------|
| `Id` | `string` | Permission identifier |

#### Methods

| Method | Returns | Description |
|--------|---------|-------------|
| `ReadAsync(int?, ...)` | `PermissionResponse` | Read permission properties |
| `ReplaceAsync(PermissionProperties, int?, ...)` | `PermissionResponse` | Replace permission properties |
| `DeleteAsync(...)` | `PermissionResponse` | Delete permission |

---

### `FakeCosmosHandler`

HTTP message handler that intercepts Cosmos SDK requests and serves them from an `InMemoryContainer`. Use this for [Pattern 3 (HttpMessageHandler)](Integration-Approaches) integration.

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
| `BackingContainer` | `InMemoryContainer` | The underlying in-memory container |
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

Configuration for `FakeCosmosHandler`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Sealed:** Yes

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

In-memory implementation of `TransactionalBatch`. Supports `CreateItem`, `UpsertItem`, `ReplaceItem`, `DeleteItem`, `ReadItem`, `PatchItem`, and their stream variants. All operations execute atomically via `ExecuteAsync`.

**Namespace:** `CosmosDB.InMemoryEmulator`  
**Inherits:** `Microsoft.Azure.Cosmos.TransactionalBatch`

#### Constructor

```csharp
InMemoryTransactionalBatch(InMemoryContainer container, PartitionKey partitionKey)
```

---

### `InMemoryFeedIteratorSetup`

Wires up `ToFeedIteratorOverridable()` to return `InMemoryFeedIterator<T>`. See [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for details.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Description |
|--------|-------------|
| `Register()` | Enable in-memory feed iterator interception |
| `Deregister()` | Disable interception, revert to SDK behaviour |

---

### `IJsTriggerEngine`

Interface for executing JavaScript trigger bodies. Implemented by `JintTriggerEngine` in the [JsTriggers package](#package-cosmosdbinmemoryemulatorjstriggers), or provide a custom implementation.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Returns | Description |
|--------|---------|-------------|
| `ExecutePreTrigger(string jsBody, JObject document)` | `JObject` | Execute pre-trigger, returns (possibly modified) document |
| `ExecutePostTrigger(string jsBody, JObject document)` | `void` | Execute post-trigger with read-only access |

---

### `ServiceCollectionExtensions`

DI extension methods for `IServiceCollection`. See [Dependency Injection](Dependency-Injection) for usage examples.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Method | Description |
|--------|-------------|
| `UseInMemoryCosmosDB(Action<InMemoryCosmosOptions>?)` | Replace `CosmosClient` + `Container` registrations using `FakeCosmosHandler` |
| `UseInMemoryCosmosContainers(Action<InMemoryContainerOptions>?)` | Replace `Container` registrations only |
| `UseInMemoryCosmosDB<TClient>(Action<InMemoryCosmosOptions>?)` | Replace typed `CosmosClient` subclass + `Container` registrations |

---

### `InMemoryCosmosOptions`

Options for `UseInMemoryCosmosDB()` and `UseInMemoryCosmosDB<TClient>()`.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Containers` | `List<ContainerConfig>` | Empty | Container configurations |
| `DatabaseName` | `string?` | `"in-memory-db"` | Default database name |
| `RegisterFeedIteratorSetup` | `bool` | `true` | Auto-register feed iterator factory (only used by `UseInMemoryCosmosDB<TClient>()`; the non-generic method uses `FakeCosmosHandler` which handles `.ToFeedIterator()` natively) |
| `OnClientCreated` | `Action<CosmosClient>?` | null | Post-creation callback (receives real `CosmosClient` backed by `FakeCosmosHandler`, or `InMemoryCosmosClient` subclass for the generic method) |
| `OnHandlerCreated` | `Action<string, FakeCosmosHandler>?` | null | Per-container handler callback — container name + handler (only used by `UseInMemoryCosmosDB()`) |
| `HttpMessageHandlerWrapper` | `Func<HttpMessageHandler, HttpMessageHandler>?` | null | Wraps the `FakeCosmosHandler`/router before it’s passed to `HttpClientFactory`. Use to insert a `DelegatingHandler` for logging, tracking, etc. Only applies to `UseInMemoryCosmosDB()` (not the generic `<TClient>` overload). || `StatePersistenceDirectory` | `string?` | null | Directory for automatic state persistence. Container state is loaded on creation and saved on disposal. Files are named `{DatabaseName}_{ContainerName}.json`. See [State Persistence](State-Persistence) |
| Method | Returns | Description |
|--------|---------|-------------|
| `AddContainer(string containerName, string partitionKeyPath = "/id", string? databaseName = null)` | `InMemoryCosmosOptions` | Fluent helper to add a container configuration |
| `WithHttpMessageHandlerWrapper(Func<HttpMessageHandler, HttpMessageHandler> wrapper)` | `InMemoryCosmosOptions` | Fluent helper to set `HttpMessageHandlerWrapper` |

### `InMemoryContainerOptions`

Options for `UseInMemoryCosmosContainers()`.

**Namespace:** `CosmosDB.InMemoryEmulator`

| Property | Type | Default | Description |
|----------|------|---------|-------------|
| `Containers` | `List<ContainerConfig>` | Empty | Container configurations |
| `RegisterFeedIteratorSetup` | `bool` | `true` | Auto-register feed iterator factory |
| `OnContainerCreated` | `Action<InMemoryContainer>?` | null | Per-container callback |
| `StatePersistenceDirectory` | `string?` | null | Directory for automatic state persistence. Container state is loaded on creation and saved on disposal. Files are named `{ContainerName}.json`. See [State Persistence](State-Persistence) |

| Method | Returns | Description |
|--------|---------|-------------|
| `AddContainer(string containerName = "in-memory-container", string partitionKeyPath = "/id")` | `InMemoryContainerOptions` | Fluent helper to add a container configuration |

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

Drop-in replacement for `.ToFeedIterator()` that can be intercepted in tests. See [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for full usage.

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

---

## Package: CosmosDB.InMemoryEmulator.JsTriggers

Optional package that provides a Jint-based JavaScript engine for executing real Cosmos DB trigger scripts in tests.

### `JintTriggerEngine`

JavaScript trigger engine built on [Jint](https://github.com/sebastienros/jint).

**Namespace:** `CosmosDB.InMemoryEmulator.JsTriggers`  
**Implements:** `IJsTriggerEngine`

| Method | Returns | Description |
|--------|---------|-------------|
| `ExecutePreTrigger(string jsBody, JObject document)` | `JObject` | Execute pre-trigger JavaScript, returns modified document |
| `ExecutePostTrigger(string jsBody, JObject document)` | `void` | Execute post-trigger JavaScript |

### `JsTriggerExtensions`

Extension method to wire up the Jint engine.

**Namespace:** `CosmosDB.InMemoryEmulator.JsTriggers`

| Method | Returns | Description |
|--------|---------|-------------|
| `UseJsTriggers(this InMemoryContainer)` | `InMemoryContainer` | Sets `JsTriggerEngine` to a `JintTriggerEngine` instance |
