Detailed reference for all emulator features beyond basic CRUD and queries.

For installation and quick-start examples, see [Getting Started](Getting-Started). For the full SQL query engine (120+ built-in functions), see [SQL Queries](SQL-Queries). For behavioural differences from real Cosmos DB, see [Known Limitations](Known-Limitations). For a side-by-side comparison with the official Microsoft emulator, see [Feature Comparison](Feature-Comparison-With-Alternatives).

## Patch Operations

Supports all five Cosmos DB patch operation types with deep nested path support.

### Atomicity

Patch operations are atomic and executed sequentially, matching real Cosmos DB. If any operation in a batch fails, none of the operations are applied — the item remains unchanged.

```csharp
// If the increment fails (e.g., /name is a string), the set is also rolled back
await container.PatchItemAsync<MyDocument>("id", new PartitionKey("pk"),
    [
        PatchOperation.Set("/name", "Changed"),
        PatchOperation.Increment("/name", 1),  // Fails — name is now a string
    ]);
// Item is unchanged — atomicity preserved
```

### Operation Types

| Operation | Description | Example |
|-----------|-------------|---------|
| `Set` | Set a property value (creates if missing) | `PatchOperation.Set("/status", "active")` |
| `Replace` | Replace a property value (must exist) | `PatchOperation.Replace("/name", "New Name")` |
| `Add` | Add to array or set a new property | `PatchOperation.Add("/tags/-", "new-tag")` |
| `Remove` | Remove a property | `PatchOperation.Remove("/tempField")` |
| `Increment` | Increment a numeric value | `PatchOperation.Increment("/version", 1)` |

### Examples

```csharp
// Multiple operations in one call
var patchOps = new List<PatchOperation>
{
    PatchOperation.Set("/status", "active"),
    PatchOperation.Replace("/name", "Updated Name"),
    PatchOperation.Add("/tags/-", "new-tag"),        // Append to array
    PatchOperation.Remove("/tempField"),
    PatchOperation.Increment("/version", 1)
};

var response = await container.PatchItemAsync<MyDocument>(
    "doc-id", new PartitionKey("pk"), patchOps);
```

### Deep Nested Paths

```csharp
// Set a deeply nested property — intermediate objects are created automatically
PatchOperation.Set("/address/city/zipCode", "12345")
```

### Conditional Patching

```csharp
// ETag-based: only patch if the document hasn't been modified
await container.PatchItemAsync<MyDocument>(
    "id", new PartitionKey("pk"),
    [PatchOperation.Set("/status", "done")],
    new PatchItemRequestOptions { IfMatchEtag = etag });

// Filter predicate: only patch if a condition is met
await container.PatchItemAsync<MyDocument>(
    "id", new PartitionKey("pk"),
    [PatchOperation.Set("/name", "Patched")],
    new PatchItemRequestOptions
    {
        FilterPredicate = "FROM c WHERE c.isActive = true"
    });
```

### Validation (v2.0.53+)

Patch operations targeting `/id` or the container's partition key path are rejected with `CosmosException(HttpStatusCode.BadRequest)`, matching real Cosmos DB.

**Computed property protection (v2.0.54+):** Patch operations targeting computed property paths are also rejected with 400 BadRequest.

### Stream Variant

```csharp
var response = await container.PatchItemStreamAsync(
    "id", new PartitionKey("pk"), patchOps);
// Returns ResponseMessage with StatusCode instead of throwing
```

---

## System Metadata Properties

All stored items are automatically enriched with system metadata properties, matching real Cosmos DB behaviour:

| Property | Type | Description |
|----------|------|-------------|
| `_ts` | `long` | Unix epoch timestamp (seconds) of the last write |
| `_etag` | `string` | Entity tag for optimistic concurrency |
| `_rid` | `string` | Synthetic resource ID (v2.0.57+) |
| `_self` | `string` | Synthetic self-link (v2.0.57+) |
| `_attachments` | `string` | Synthetic attachments link (v2.0.57+) |

These properties are updated on every Create, Upsert, Replace, and Patch operation, and are available when reading items back:

```csharp
// Read back system properties via JObject
var response = await container.ReadItemAsync<JObject>("id", new PartitionKey("pk"));
long timestamp = response.Resource["_ts"].Value<long>();
string etag = response.Resource["_etag"].ToString();

// Or map to your own type
public class MyDocument
{
    [JsonProperty("id")] public string Id { get; set; }
    [JsonProperty("_ts")] public long Timestamp { get; set; }
    [JsonProperty("_etag")] public string ETag { get; set; }
}
```

---

## Transactional Batches

Atomic multi-operation execution with automatic rollback on failure.

### Basic Usage

```csharp
var batch = container.CreateTransactionalBatch(new PartitionKey("pk"))
    .CreateItem(new { id = "1", partitionKey = "pk", name = "Alice" })
    .CreateItem(new { id = "2", partitionKey = "pk", name = "Bob" })
    .ReplaceItem("3", updatedItem)
    .PatchItem("4", new[] { PatchOperation.Set("/status", "done") })
    .DeleteItem("5");

using var response = await batch.ExecuteAsync();
if (response.IsSuccessStatusCode)
{
    // All operations succeeded atomically
    var alice = response.GetOperationResultAtIndex<dynamic>(0);
}
else
{
    // All operations rolled back — nothing was persisted
}
```

### Supported Operations

- `CreateItem<T>` / `CreateItemStream`
- `UpsertItem<T>` / `UpsertItemStream`
- `ReplaceItem<T>` / `ReplaceItemStream`
- `DeleteItem`
- `ReadItem`
- `PatchItem`

### Limits

- Maximum 100 operations per batch
- Maximum 2 MB total payload size
- All operations must target the same partition key

### Rollback Behaviour

When any operation fails:

1. All preceding operations are rolled back (data restored to pre-batch state)
2. The failed operation gets its actual error status code (e.g. 404, 409)
3. All preceding operations get `424 FailedDependency`
4. All subsequent operations also get `424 FailedDependency`

---


---

## Bulk Operations

Supports the `AllowBulkExecution = true` pattern where many concurrent point operations are fired via `Task.WhenAll`. The SDK internally batches these into efficient service calls grouped by partition key.

### Usage

```csharp
// Set AllowBulkExecution on the client (works with both InMemoryCosmosClient and FakeCosmosHandler)
var client = new InMemoryCosmosClient();
client.ClientOptions.AllowBulkExecution = true;

var container = client.GetDatabase("db").GetContainer("container");

// Fire hundreds of concurrent operations — the emulator handles them correctly
var tasks = items.Select(item =>
    container.CreateItemAsync(item, new PartitionKey(item.PartitionKey)));

await Task.WhenAll(tasks);
```

### Supported Operations

All point operations work correctly under bulk concurrency:

- `CreateItemAsync` / `CreateItemStreamAsync`
- `UpsertItemAsync` / `UpsertItemStreamAsync`
- `ReplaceItemAsync` / `ReplaceItemStreamAsync`
- `DeleteItemAsync` / `DeleteItemStreamAsync`
- `ReadItemAsync` / `ReadItemStreamAsync`
- `PatchItemAsync` / `PatchItemStreamAsync`

### Concurrency Guarantees

- **Thread-safe storage** — backed by `ConcurrentDictionary`, all concurrent reads and writes are safe
- **Unique key enforcement** — unique key policies are enforced atomically even under concurrent writes (no TOCTOU race conditions)
- **ETag generation** — each write generates a unique ETag
- **Change feed recording** — all concurrent writes are captured in the change feed
- **Conflict detection** — duplicate `CreateItemAsync` calls with the same ID correctly return `409 Conflict`
- **Conditional operations** — `IfMatchEtag`, `IfNoneMatchEtag` (including wildcard `*`) work correctly under concurrency
- **EnableContentResponseOnWrite** — suppresses response body for all write operations (Create, Upsert, Replace, Patch)
- **CancellationToken** — already-cancelled tokens throw `OperationCanceledException` immediately
- **Hierarchical partition keys** — bulk operations work with composite partition keys via `PartitionKeyBuilder`
- **Filter predicate patches** — `PatchItemRequestOptions.FilterPredicate` correctly filters under bulk concurrency
- **TTL interaction** — items with per-item or container-level TTL expire correctly after bulk creation
- **Stream error handling** — stream methods return `ResponseMessage` with error status codes (404, 409, 412) instead of throwing
- **Document size limits** — 2 MB limit enforced; oversized documents reject with HTTP 413
- **DeleteAllItemsByPartitionKey** — works correctly concurrent with bulk operations on different partitions
- **Hierarchical PK concurrency** — concurrent writes with composite partition keys (`/tenantId` + `/userId`) are fully thread-safe
- **State persistence under concurrency** — `ExportState()` produces valid JSON even during concurrent writes
- **Batch isolation** — transactional batches execute atomically (all-or-nothing) with snapshot rollback on failure. See [Known Limitations](Known-Limitations) for concurrent reader visibility during batch execution.

### Divergent Behaviour

In real Cosmos DB, `AllowBulkExecution` causes the SDK to group concurrent operations into batched service calls for throughput optimisation. The in-memory emulator executes each operation individually — there is no batching layer. This has no functional impact; all operations produce identical results. The only difference is performance characteristics (the emulator does not simulate RU consumption or throughput limits).
## Change Feed

### Change Feed Iterators

**Checkpoint-based (recommended for all versions + deletes):**

The checkpoint-based iterator returns **all versions** of every change, including **delete tombstones**. This is the equivalent of Cosmos DB's `AllVersionsAndDeletes` mode.

```csharp
long checkpoint = container.GetChangeFeedCheckpoint();

await container.CreateItemAsync(item1, new PartitionKey("pk"));
await container.UpsertItemAsync(item1Updated, new PartitionKey("pk"));
await container.DeleteItemAsync<MyDocument>("1", new PartitionKey("pk"));

var iterator = container.GetChangeFeedIterator<JObject>(checkpoint);
while (iterator.HasMoreResults)
{
    var changes = await iterator.ReadNextAsync();
    foreach (var change in changes)
    {
        if (change["_deleted"]?.Value<bool>() == true)
            Console.WriteLine($"Deleted: {change["id"]}");
        else
            Console.WriteLine($"Changed: {change["id"]}");
    }
}
```

Delete tombstones contain `{ "id": "...", "<partitionKeyField>": "...", "_deleted": true, "_ts": ... }`.

**SDK-style start positions (Incremental / Latest Version):**

The SDK-style iterators with `ChangeFeedMode.Incremental` return only the **latest version** of each item. Items whose last change is a delete are filtered out — matching real Cosmos DB Latest Version mode behaviour.

```csharp
// All changes since container creation (latest version per item)
var iter1 = container.GetChangeFeedIterator<MyDoc>(
    ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);

// Only changes after this point
var iter2 = container.GetChangeFeedIterator<MyDoc>(
    ChangeFeedStartFrom.Now(), ChangeFeedMode.Incremental);

// Changes after a specific timestamp
var iter3 = container.GetChangeFeedIterator<MyDoc>(
    ChangeFeedStartFrom.Time(DateTime.UtcNow.AddMinutes(-5)),
    ChangeFeedMode.Incremental);
```

### Change Feed Processor

```csharp
var processor = container
    .GetChangeFeedProcessorBuilder<MyDocument>(
        "myProcessor",
        async (context, changes, ct) =>
        {
            foreach (var doc in changes)
                Console.WriteLine($"Changed: {doc.Id}");
        })
    .Build();

await processor.StartAsync();
// ... changes will be delivered to the handler via polling ...
await processor.StopAsync();
```

The processor polls the in-memory change feed every 50ms.

### Manual Checkpoint Processor

```csharp
var processor = container
    .GetChangeFeedProcessorBuilderWithManualCheckpoint<MyDocument>(
        "myProcessor",
        async (context, changes, checkpointAsync, ct) =>
        {
            foreach (var doc in changes)
                Console.WriteLine($"Changed: {doc.Id}");
            // Checkpoint only advances when you call checkpointAsync.
            // If you don't call it, the same changes are redelivered on next poll.
            await checkpointAsync();
        })
    .Build();

await processor.StartAsync();
// ... changes will be delivered to the handler via polling ...
await processor.StopAsync();
```

Both typed (`ChangeFeedHandlerWithManualCheckpoint<T>`) and stream (`ChangeFeedStreamHandlerWithManualCheckpoint`) handlers are supported. If `checkpointAsync` is not called, the same batch of changes is redelivered on the next poll cycle, matching real Cosmos DB manual checkpoint semantics.

> **Limitation:** The stream change handler variant (`GetChangeFeedProcessorBuilder` with a `Stream` handler) may fail with `WithLeaseContainer()` because the SDK casts the lease `Container` to the internal `ContainerInternal` class. Use `WithInMemoryLeaseContainer()` or the typed handler instead. See [Known Limitations](Known-Limitations) for details.

### Stream Change Feed

```csharp
var iterator = container.GetChangeFeedStreamIterator(
    ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
```

### Delete Tombstones

When an item is deleted, a tombstone entry is recorded in the change feed:

```json
{ "id": "item-1", "partitionKey": "pk1", "_deleted": true, "_ts": 1743379200 }
```

- **Checkpoint-based iterator**: Returns tombstones alongside creates/updates — use this to process deletes.
- **Incremental mode** (`ChangeFeedMode.Incremental`): Filters out items whose last change is a delete, matching real Cosmos DB Latest Version behaviour.
- **Change feed processors**: Use the checkpoint-based iterator internally, so tombstones are delivered to the handler.

### Additional Change Feed Behaviours

- **Failed writes are not recorded**: Operations that throw (`CosmosException`) — such as unique key violations, ETag conflicts, or failed conditional patches — do not produce a change feed entry
- **`ClearItems()` resets the change feed**: Calling `ClearItems()` removes all entries from the change feed and resets the checkpoint to 0
- **PITR preserves the change feed log**: `RestoreToPointInTime()` replays the change feed to reconstruct container state, but does not delete change feed entries
- **Handler exceptions cause redelivery**: If a processor handler throws, the checkpoint is not advanced and the same batch is redelivered on the next poll cycle
- **All processor types deliver all versions**: Unlike real Cosmos DB's latest-version processor, the in-memory processor delivers intermediate versions. Handlers should be designed for idempotent processing
- **TTL eviction is silent**: Items expired by TTL are lazily evicted without producing a change feed tombstone
- **Change feed items include `_ts` and `_etag`**: System properties are enriched before recording in the change feed
- **FeedRange scoping works**: Both typed and stream change feed iterators support `FeedRange` filtering via `ChangeFeedStartFrom.Beginning(range)`, `ChangeFeedStartFrom.Time(date, range)`, etc.

---

## ETag & Optimistic Concurrency

### Conditional Replace

```csharp
var response = await container.ReadItemAsync<MyDoc>("id", new PartitionKey("pk"));
var etag = response.ETag;

// Only replace if the document hasn't been modified
try
{
    await container.ReplaceItemAsync(updatedDoc, "id", new PartitionKey("pk"),
        new ItemRequestOptions { IfMatchEtag = etag });
}
catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.PreconditionFailed)
{
    // Another write happened — retry with fresh data
}
```

### Conditional Read (Not Modified)

```csharp
try
{
    await container.ReadItemAsync<MyDoc>("id", new PartitionKey("pk"),
        new ItemRequestOptions { IfNoneMatchEtag = etag });
}
catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotModified)
{
    // Document hasn't changed — use cached version
}
```

### Wildcard ETag

```csharp
// IfMatch with "*" matches any existing document
new ItemRequestOptions { IfMatchEtag = "*" }

// IfNoneMatch with "*" means "only if document doesn't exist"
new ItemRequestOptions { IfNoneMatchEtag = "*" }
```

---

## TTL / Expiration

```csharp
// Container-level default (seconds)
container.DefaultTimeToLive = 3600; // items expire after 1 hour

// Enable TTL without a default expiry (only items with per-item _ttl expire)
container.DefaultTimeToLive = -1;

// Per-item override via _ttl property in JSON
var item = new { id = "1", partitionKey = "pk", _ttl = 7200, data = "..." };
await container.CreateItemAsync(item, new PartitionKey("pk"));

// Per-item _ttl = -1 means "never expire" even if container has a default TTL
var immortal = new { id = "2", partitionKey = "pk", _ttl = -1, data = "..." };
await container.CreateItemAsync(immortal, new PartitionKey("pk"));

// Disable TTL entirely (per-item _ttl is ignored)
container.DefaultTimeToLive = null;
```

**Semantics** (matches real Cosmos DB):

| Container `DefaultTimeToLive` | Item `_ttl` | Behaviour |
|-------------------------------|-------------|-----------|
| `null` | any | Never expires (TTL disabled; per-item `_ttl` ignored) |
| `-1` | missing | Never expires |
| `-1` | `-1` | Never expires |
| `-1` | `N` | Expires after `N` seconds |
| `N` | missing | Expires after `N` seconds |
| `N` | `-1` | Never expires (item overrides container) |
| `N` | `M` | Expires after `M` seconds (item overrides container) |

**Validation (v2.0.53+):**
- Setting `DefaultTimeToLive = 0` throws `CosmosException(HttpStatusCode.BadRequest)` — real Cosmos DB rejects zero as a TTL value.
- Creating/upserting an item with `_ttl = 0` throws `CosmosException(HttpStatusCode.BadRequest)`.

**Eviction behaviour:**
- Items are lazily evicted — expired items are removed the next time they are accessed via read, query, replace, patch, delete, or ReadMany.
- Upsert and Create on an expired item succeed (the expired item is evicted first).
- Replace/Upsert/Patch reset the TTL clock.
- TTL-triggered evictions do **not** produce change feed events (unlike real Cosmos DB in AllVersionsAndDeletes mode).
- `ReplaceContainerAsync` updates the active TTL setting.

See [Known Limitations](Known-Limitations) for details on lazy eviction.

---

## Stored Procedures

Register C# functions as stored procedure handlers:

```csharp
container.RegisterStoredProcedure("bulkDelete", (pk, args) =>
{
    // pk is the PartitionKey the sproc was called with
    // args are the dynamic arguments passed to ExecuteStoredProcedureAsync
    return JsonConvert.SerializeObject(new { deleted = 42 });
});

var response = await container.Scripts.ExecuteStoredProcedureAsync<string>(
    "bulkDelete", new PartitionKey("pk"), new dynamic[] { "arg1" });

// Deregister when done
container.DeregisterStoredProcedure("bulkDelete");
```

**Validation (v2.0.53+):** Executing a stored procedure that has not been registered (via `RegisterStoredProcedure` or `CreateStoredProcedureAsync`) returns `CosmosException(HttpStatusCode.NotFound)`, matching real Cosmos DB.

---

## Triggers

Register C# pre-triggers and post-triggers that fire during create, upsert, and replace operations. Trigger invocation is controlled via `ItemRequestOptions.PreTriggers` / `PostTriggers`, matching the real SDK API.

### Pre-Triggers

Pre-triggers receive the incoming document as a `JObject` and return a (possibly modified) document:

```csharp
container.RegisterTrigger("addTimestamp", TriggerType.Pre, TriggerOperation.Create,
    preHandler: doc =>
    {
        doc["createdAt"] = DateTime.UtcNow.ToString("o");
        return doc;
    });

// Fire the trigger by specifying it in ItemRequestOptions
await container.CreateItemAsync(
    new { id = "1", pk = "a" },
    new PartitionKey("a"),
    new ItemRequestOptions { PreTriggers = new List<string> { "addTimestamp" } });
```

### Post-Triggers

Post-triggers receive the committed document. If a post-trigger throws, the write is rolled back:

```csharp
container.RegisterTrigger("audit", TriggerType.Post, TriggerOperation.Create,
    postHandler: doc =>
    {
        // Perform side-effects (logging, auditing, etc.)
        Console.WriteLine($"Created: {doc["id"]}");
    });

await container.CreateItemAsync(
    new { id = "1", pk = "a" },
    new PartitionKey("a"),
    new ItemRequestOptions { PostTriggers = new List<string> { "audit" } });
```

### TriggerOperation Filtering

Triggers only fire when the operation matches. Use `TriggerOperation.All` to fire on any write:

```csharp
container.RegisterTrigger("validateAll", TriggerType.Pre, TriggerOperation.All,
    preHandler: doc => { /* runs on create, upsert, and replace */ return doc; });
```

### Deregistering Triggers

```csharp
container.DeregisterTrigger("addTimestamp");
```

### JavaScript Trigger Bodies (Optional Package)

Install the `CosmosDB.InMemoryEmulator.JsTriggers` NuGet package to execute real Cosmos DB trigger JavaScript bodies. This uses the [Jint](https://github.com/sebastienros/jint) interpreter and supports the standard server-side API: `getContext()`, `getRequest()`/`getResponse()`, `getBody()`, `setBody()`.

```csharp
using CosmosDB.InMemoryEmulator.JsTriggers;

// Enable JS trigger interpretation on a container
container.UseJsTriggers();

// Register a trigger with a JavaScript body via CreateTriggerAsync
await container.Scripts.CreateTriggerAsync(new TriggerProperties
{
    Id = "addTimestamp",
    TriggerType = TriggerType.Pre,
    TriggerOperation = TriggerOperation.Create,
    Body = @"
        function addTimestamp() {
            var context = getContext();
            var request = context.getRequest();
            var doc = request.getBody();
            doc['timestamp'] = new Date().toISOString();
            request.setBody(doc);
        }"
});

// Fire it the same way as C# triggers
await container.CreateItemAsync(
    new { id = "1", pk = "a" },
    new PartitionKey("a"),
    new ItemRequestOptions { PreTriggers = new List<string> { "addTimestamp" } });
```

**Priority:** If both a C# handler (`RegisterTrigger`) and a JS body (`CreateTriggerAsync`) exist for the same trigger name, the C# handler takes priority.

---

## State Persistence

Export and import container state for test data seeding, snapshots, or debugging. For the full guide including automatic persistence between test runs and point-in-time restore, see **[State Persistence](State-Persistence)**.

```csharp
// Export to string
string json = container.ExportState();

// Export to file
container.ExportStateToFile("test-data/snapshot.json");

// Import from string (replaces all data)
container.ImportState(json);

// Import from file
container.ImportStateFromFile("test-data/snapshot.json");

// Automatic persistence between test runs (v2.0.70+)
services.UseInMemoryCosmosDB(options =>
{
    options.AddContainer("orders", "/customerId");
    options.StatePersistenceDirectory = "./test-state"; // auto-save on dispose, auto-load on create
});

// Utilities
int count = container.ItemCount;
container.ClearItems(); // Remove everything
```

The exported format is:

```json
{
  "items": [
    { "id": "1", "partitionKey": "pk1", "name": "Alice", ... },
    { "id": "2", "partitionKey": "pk2", "name": "Bob", ... }
  ]
}
```

### Point-in-Time Restore

Restore a container to its state at any previous point in time. This replays the internal change feed up to the specified timestamp, reconstructing the exact state of every item:

```csharp
// Create and modify data over time
await container.CreateItemAsync(item1, new PartitionKey("pk"));
var restorePoint = DateTimeOffset.UtcNow;

await container.CreateItemAsync(item2, new PartitionKey("pk"));
await container.DeleteItemAsync<MyDoc>("item1", new PartitionKey("pk"));

// Roll back to the restore point — item1 reappears, item2 is gone
container.RestoreToPointInTime(restorePoint);
```

This is useful for:
- **Debugging** — inspect container state at a specific moment
- **Test isolation** — restore to a known baseline between test cases
- **Simulating Cosmos DB continuous backup** — test PITR-dependent recovery logic

> **Note:** The restore replays the change feed, so it only works for changes made during the lifetime of the container instance. The change feed is not persisted across `ExportState` / `ImportState` calls.

---

## Partition Keys

### Single Partition Key

```csharp
var container = new InMemoryContainer("orders", "/customerId");
```

### Composite (Hierarchical) Partition Keys

```csharp
var container = new InMemoryContainer("orders", new[] { "/tenantId", "/customerId" });
```

### PartitionKey.None

When `PartitionKey.None` is used, the partition key value is extracted from the document body:

```csharp
await container.CreateItemAsync(item, PartitionKey.None);
// PK value is extracted from item.partitionKey (or whatever the PK path points to)
```

> **Note:** The emulator treats `PartitionKey.None` and `PartitionKey.Null` identically. See [Known Limitations](Known-Limitations#7-partitionkeynone-vs-partitionkeynull-treated-identically) for details.

---

## FeedRange Support

Simulate multiple physical partitions and scope queries / change feeds to specific feed ranges.

### FeedRange Count

By default, `GetFeedRangesAsync()` returns a single `FeedRange` covering the entire hash space. Set `FeedRangeCount` to simulate multiple partitions:

```csharp
var container = new InMemoryContainer("orders", "/customerId") { FeedRangeCount = 4 };
var ranges = await container.GetFeedRangesAsync();
// Returns 4 contiguous FeedRangeEpk ranges covering [min, max) of the hash space
```

### FeedRange-Scoped Queries

Pass a `FeedRange` to `GetItemQueryIterator` or `GetItemQueryStreamIterator` to scope results to a partition range:

```csharp
var ranges = await container.GetFeedRangesAsync();
foreach (var range in ranges)
{
    var iterator = container.GetItemQueryIterator<MyDoc>(
        range, new QueryDefinition("SELECT * FROM c WHERE c.status = 'active'"));
    while (iterator.HasMoreResults)
    {
        var page = await iterator.ReadNextAsync();
        // Items scoped to this range only
    }
}
```

### FeedRange-Scoped Change Feed

Pass a `FeedRange` to `ChangeFeedStartFrom.Beginning()` or `ChangeFeedStartFrom.Now()` to scope the change feed:

```csharp
var ranges = await container.GetFeedRangesAsync();
foreach (var range in ranges)
{
    var iterator = container.GetChangeFeedIterator<MyDoc>(
        ChangeFeedStartFrom.Beginning(range),
        ChangeFeedMode.Incremental);
    while (iterator.HasMoreResults)
    {
        var page = await iterator.ReadNextAsync();
        // Changes scoped to this range only
    }
}
```

### Partition Distribution

Items are assigned to ranges using MurmurHash3 on the partition key value. The same algorithm is used by `FakeCosmosHandler` (via `PartitionKeyRangeCount`), so both approaches produce consistent partition assignments.

Tested edge cases include:
- Odd feed range counts (3, 5, 7) with contiguous boundaries
- Composite and numeric partition keys
- Null/missing partition key values (`PartitionKey.None`)
- Malformed FeedRange JSON (graceful fallback)
- `ChangeFeedStartFrom.Time` with FeedRange filtering
- Pagination with FeedRange scoping

---

## ReadMany

Batch-read multiple items by their (id, partitionKey) pairs:

```csharp
var items = new List<(string, PartitionKey)>
{
    ("1", new PartitionKey("pk1")),
    ("2", new PartitionKey("pk1")),
    ("3", new PartitionKey("pk2")),  // doesn't exist — silently omitted
};

var response = await container.ReadManyItemsAsync<MyDoc>(items);
// response.Resource contains only the found items
```

A stream variant (`ReadManyItemsStreamAsync`) is also available, returning a `ResponseMessage` instead of deserialized objects.

---

## Stream API

All CRUD operations have stream variants that return `ResponseMessage` instead of throwing exceptions:

```csharp
var response = await container.ReadItemStreamAsync("id", new PartitionKey("pk"));
if (response.StatusCode == HttpStatusCode.NotFound)
{
    // Handle missing item without exception overhead
}
```

Stream methods: `CreateItemStreamAsync`, `ReadItemStreamAsync`, `UpsertItemStreamAsync`, `ReplaceItemStreamAsync`, `DeleteItemStreamAsync`, `PatchItemStreamAsync`.

Stream query iterators (`GetItemQueryStreamIterator`) support continuation tokens and `MaxItemCount` pagination, matching the typed iterator behaviour.

---

## Unique Key Policies

Unique key constraints are enforced across all write operations:

```csharp
var properties = new ContainerProperties("users", "/pk")
{
    UniqueKeyPolicy = new UniqueKeyPolicy
    {
        UniqueKeys = { new UniqueKey { Paths = { "/email" } } }
    }
};
var container = new InMemoryContainer(properties);

// First insert succeeds
await container.CreateItemAsync(
    new { id = "1", pk = "a", email = "alice@example.com" },
    new PartitionKey("a"));

// Duplicate email → CosmosException 409 Conflict
await container.CreateItemAsync(
    new { id = "2", pk = "a", email = "alice@example.com" },
    new PartitionKey("a")); // Throws!
```

Unique keys are validated on:
- `CreateItemAsync` / `CreateItemStreamAsync`
- `UpsertItemAsync` / `UpsertItemStreamAsync`
- `ReplaceItemAsync` / `ReplaceItemStreamAsync`
- `PatchItemAsync`

---

## Container Management

Full container lifecycle support: create, read, replace, delete — typed and stream variants.

```csharp
// Read container properties
var response = await container.ReadContainerAsync();

// Replace container properties (Id must match)
await container.ReplaceContainerAsync(new ContainerProperties("my-container", "/pk") { DefaultTimeToLive = 3600 });

// Delete container
await container.DeleteContainerAsync();

// Delete all items in a partition
await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));

// Throughput — persisted and round-tripped
await container.ReplaceThroughputAsync(1000);
var throughput = await container.ReadThroughputAsync(); // → 1000
```

**Container management features:**
- **Throughput persistence** — `ReplaceThroughputAsync` stores the value; `ReadThroughputAsync` returns it (defaults to 400 RU/s). Works on both containers and databases.
- **Id validation on Replace** — `ReplaceContainerAsync` and `ReplaceContainerStreamAsync` reject properties with mismatched container Ids (throws `CosmosException` with 400 BadRequest / returns 400)
- **All creation overloads** — `CreateContainerAsync`, `CreateContainerIfNotExistsAsync`, `CreateContainerStreamAsync` with optional `ThroughputProperties`
- **Container query iterators** — `GetContainerQueryIterator<T>` and `GetContainerQueryStreamIterator` with `QueryDefinition` or query text
- **Special character Ids** — Container Ids with spaces, dots, underscores, parentheses, and Unicode characters are fully supported
- **ClearItems()** — Public method to reset a container's items, ETags, timestamps, and change feed without deleting the container
- **Idempotent delete** — `DeleteContainerAsync` can be called multiple times without error
- **Delete cascade (v2.0.53+)** — `DeleteContainerAsync` clears stored procedures, UDFs, and triggers alongside items/ETags/timestamps/change feed
- **Replace validation (v2.0.53+)** — `ReplaceContainerAsync` rejects partition key path changes with 400 BadRequest, and preserves `UniqueKeyPolicy` if omitted from replacement properties
- **Dispose tracking (v2.0.53+)** — `InMemoryCosmosClient` tracks disposal state; calling `CreateDatabaseAsync`, `CreateDatabaseIfNotExistsAsync`, `GetDatabase`, or `GetContainer` after `Dispose()` throws `ObjectDisposedException`
- **Database property (v2.0.54+)** — `Container.Database` returns a cached instance with the correct parent database Id. When created via `InMemoryDatabase`, `container.Database.Id` returns the database name
- **Pre-trigger size validation (v2.0.55+)** — Document size is re-validated after pre-trigger execution. Pre-triggers that inflate documents past 2MB are rejected with 413, matching real Cosmos DB
- **Stream ErrorMessage (v2.0.55+)** — `ResponseMessage.ErrorMessage` is now set for all error status codes (≥400), providing human-readable error descriptions

**Note:** `Database.GetContainer("name")` auto-creates the container for test convenience (real SDK returns a proxy where the first data operation throws 404). Database and container query iterators return all items in a single page — `MaxItemCount` paging and `WHERE` clause filtering are not supported for metadata queries.

---

## LINQ Support

```csharp
var queryable = container.GetItemLinqQueryable<MyDoc>(allowSynchronousQueryExecution: true);

// Standard LINQ operators
var results = queryable
    .Where(d => d.Status == "active")
    .OrderBy(d => d.Name)
    .Take(10)
    .ToList();

// With partition key filter
var results2 = container
    .GetItemLinqQueryable<MyDoc>(true,
        requestOptions: new QueryRequestOptions { PartitionKey = new PartitionKey("pk1") })
    .Where(d => d.Age > 30)
    .ToList();
```

For `.ToFeedIterator()` support in tests, see the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) and the [Integration Approaches](Integration-Approaches) guide.

---

## Full-Text Search (Approximate)

All four full-text search functions are implemented with case-insensitive substring matching as an approximation of the real Cosmos DB NLP-based full-text indexing engine.

> **Note:** Real Cosmos DB requires a full-text indexing policy on the container. The emulator skips this validation — queries work on any container without configuration.

### FULLTEXTCONTAINS

Returns `true` if the field contains the search term (case-insensitive):

```sql
SELECT * FROM c WHERE FULLTEXTCONTAINS(c.description, 'database')
```

### FULLTEXTCONTAINSALL

Returns `true` if the field contains **all** of the search terms:

```sql
SELECT * FROM c WHERE FULLTEXTCONTAINSALL(c.text, 'cosmos', 'database', 'azure')
```

### FULLTEXTCONTAINSANY

Returns `true` if the field contains **any** of the search terms:

```sql
SELECT * FROM c WHERE FULLTEXTCONTAINSANY(c.text, 'cosmos', 'sql', 'mongo')
```

### FULLTEXTSCORE + ORDER BY RANK

`FULLTEXTSCORE` returns a relevance score for the given search terms. Use with `ORDER BY RANK` to sort results by relevance (highest score first):

```sql
-- Sort by relevance
SELECT * FROM c ORDER BY RANK FullTextScore(c.text, ['database', 'cosmos'])

-- Combine with filtering and projection
SELECT c.id, FullTextScore(c.text, ['database', 'cosmos']) AS score
FROM c
WHERE c.category = 'docs'
ORDER BY RANK FullTextScore(c.text, ['database', 'cosmos'])
```

### Behavioural Differences

| Aspect | Real Cosmos DB | InMemoryEmulator |
|--------|---------------|-----------------|
| Indexing policy | Required (FullTextPolicy + FullTextIndexes) | Not required |
| Text analysis | Tokenization, stemming, stop-word removal | Case-insensitive substring matching |
| Scoring algorithm | BM25 (TF-IDF + length normalization) | Naive term-frequency count |
| Stemming | \"running\" matches \"runs\", \"ran\" | Literal matching only |

For the complete list of full-text search behavioural differences, see [Known Limitations](Known-Limitations#13-full-text-search-uses-naive-matching-not-bm25).

---

## Vector Search

`VECTORDISTANCE` computes the distance between two vectors using cosine similarity, dot product, or Euclidean distance. Works in `SELECT`, `WHERE`, and `ORDER BY` clauses.

```sql
-- Find the 10 most similar items by cosine similarity
SELECT TOP 10 c.id, VectorDistance(c.embedding, [0.1, 0.2, 0.3]) AS score
FROM c
ORDER BY VectorDistance(c.embedding, [0.1, 0.2, 0.3])

-- Filter by similarity threshold
SELECT * FROM c
WHERE VectorDistance(c.embedding, [0.1, 0.2, 0.3], false, {distanceFunction: 'cosine'}) < 0.5
```

### Supported Distance Functions

| Distance Function | Description |
|-------------------|-------------|
| `cosine` | Cosine similarity (default) — `dot(a,b) / (|a| × |b|)` |
| `dotproduct` | Dot product — higher values indicate greater similarity |
| `euclidean` | Euclidean (L2) distance |

### Behavioural Differences

| Aspect | Real Cosmos DB | InMemoryEmulator |
|--------|---------------|------------------|
| Vector index policy | Required for approximate nearest neighbour (ANN) | Not required — always brute-force exact computation |
| Zero-magnitude vectors | Returns no score (null-like) | Returns `null` for cosine (undefined); dot product returns 0 |
| Dimensionality limits | flat: 505, quantizedFlat/diskANN: 4096 | No limit (tested up to 2000) |
| Additional options | `dataType`, `searchListSizeMultiplier`, `filterPriority` affect index behaviour | Accepted but ignored |
| Index types | flat, quantizedFlat, diskANN | Not applicable |
| Performance | ANN indexing for sub-linear search | Linear scan (suitable for test data volumes) |
| Unknown distance function | Rejected with error | ~~Silently fell back to cosine~~ **Fixed in v2.0.53:** now throws `CosmosException(BadRequest)` |
| Extra args (>4) | Rejected at query compilation | ~~Silently ignored~~ **Fixed in v2.0.53:** now rejects with `CosmosException(BadRequest)` |
| Infinity/NaN overflow | Unlikely (bounded dimensions) | Returns `null` for non-finite results |

### Verified Integrations

- **CRUD interaction:** VectorDistance queries reflect upserts, patches, and deletes immediately
- **Change feed:** Vector field updates appear in `GetChangeFeedIterator` results
- **Stream API:** `GetItemQueryStreamIterator` returns valid JSON with vector scores
- **Cross-partition:** Works with both single and hierarchical partition keys
- **Arithmetic expressions:** `VectorDistance(...) * 100`, `VectorDistance(...) + 5`, `ABS(VectorDistance(...))`, `IIF(VectorDistance(...) > threshold, ...)` all work
- **Concurrent reads:** Thread-safe for parallel vector queries

For the full list of supported SQL functions, see [SQL Queries](SQL-Queries).

---

## Geospatial Functions

All six Cosmos DB geospatial functions are implemented with real geometric calculations:

| Function | Implementation |
|----------|---------------|
| `ST_DISTANCE(a, b)` | Haversine formula (metres) |
| `ST_WITHIN(point, region)` | Point-in-polygon (ray casting) and point-in-circle (haversine radius) |
| `ST_INTERSECTS(geo1, geo2)` | Point-point, point-polygon, and polygon-polygon overlap detection |
| `ST_ISVALID(geojson)` | Full GeoJSON structure validation (Point, Polygon, LineString, MultiPoint) |
| `ST_ISVALIDDETAILED(geojson)` | Returns `{ valid, reason }` with specific validation error messages |
| `ST_AREA(polygon)` | Spherical excess formula |

```sql
-- Find locations within 5 km
SELECT * FROM c
WHERE ST_DISTANCE(c.location, {'type': 'Point', 'coordinates': [-122.4, 37.8]}) < 5000

-- Check if a point is within a polygon
SELECT * FROM c
WHERE ST_WITHIN(c.location, {
    'type': 'Polygon',
    'coordinates': [[[-122.5, 37.7], [-122.3, 37.7], [-122.3, 37.9], [-122.5, 37.9], [-122.5, 37.7]]]
})
```

> **Note:** Results are geometrically correct but may differ slightly from Cosmos DB's exact values due to different calculation methods. See [Known Limitations](Known-Limitations#geospatial-functions) for details.

---

## Document Size Limits

The emulator enforces the same 2 MB maximum document size as real Cosmos DB. Documents exceeding this limit are rejected with a `RequestEntityTooLarge` (413) status code on create, upsert, replace, and patch operations.

Transactional batches also enforce the 2 MB total payload size limit across all operations in the batch.

---

## Users & Permissions

In-memory stub implementation of Cosmos DB's user and permission management. CRUD operations work and return realistic responses with synthetic metadata. No actual authorization is enforced — all data operations succeed regardless of permission settings.

### Database-Level User Operations

```csharp
var db = client.GetDatabase("myDb");

// Create a user
var response = await db.CreateUserAsync("user1");
// response.StatusCode == HttpStatusCode.Created

// Upsert (create or update)
await db.UpsertUserAsync("user1"); // 200 OK if exists, 201 Created if new

// Get user proxy
var user = db.GetUser("user1");

// Query all users
var iterator = db.GetUserQueryIterator<UserProperties>();
```

### User Instance Operations

```csharp
var user = db.GetUser("user1");

// Read
var readResponse = await user.ReadAsync();
// readResponse.Resource.Id == "user1"

// Replace
await user.ReplaceAsync(new UserProperties("user1-renamed"));

// Delete
await user.DeleteAsync(); // 204 NoContent, removed from database
```

### Permission Operations

```csharp
var user = db.GetUser("user1");
var container = db.GetContainer("myContainer");

// Create permission
var permProps = new PermissionProperties("perm1", PermissionMode.All, container);
var response = await user.CreatePermissionAsync(permProps);
// response.Resource.Token contains a synthetic token string

// Upsert permission
await user.UpsertPermissionAsync(new PermissionProperties("perm1", PermissionMode.Read, container));

// Read permission
var perm = user.GetPermission("perm1");
var readResponse = await perm.ReadAsync();

// Replace permission
await perm.ReplaceAsync(new PermissionProperties("perm1", PermissionMode.All, container));

// Delete permission
await perm.DeleteAsync();

// Query all permissions for a user
var iterator = user.GetPermissionQueryIterator<PermissionProperties>();
```

### Behavioural Differences

| Aspect | Real Cosmos DB | InMemoryEmulator |
|--------|---------------|-----------------|
| Permission tokens | Cryptographic, time-limited resource tokens | Synthetic placeholders (`type=resource&ver=1&sig=stub_{id}`) |
| Token expiry | Enforced (`tokenExpiryInSeconds`) | Accepted but not enforced |
| Authorization | Operations scoped by permission mode | No authorization — all operations succeed |
| Resource scoping | Tokens grant access to specific containers/items | No scoping — permissions are metadata only |

For the full list of behavioural differences, see [Known Limitations](Known-Limitations).

## Computed Properties

Computed properties are virtual top-level properties defined on a container with a `Name` and a `Query`. They are not persisted on documents — instead, queries can reference them like regular properties in `SELECT`, `WHERE`, `ORDER BY`, and `GROUP BY`.

### Defining Computed Properties

```csharp
var props = new ContainerProperties("myContainer", "/pk")
{
    ComputedProperties = new Collection<ComputedProperty>
    {
        new ComputedProperty
        {
            Name = "cp_lowerName",
            Query = "SELECT VALUE LOWER(c.name) FROM c"
        },
        new ComputedProperty
        {
            Name = "cp_discountedPrice",
            Query = "SELECT VALUE c.price * 0.8 FROM c"
        }
    }
};
var container = new InMemoryContainer(props);
```

### Using in Queries

```csharp
// SELECT projection
var q1 = "SELECT c.cp_lowerName FROM c";

// WHERE filtering
var q2 = "SELECT c.id FROM c WHERE c.cp_lowerName = 'alice'";

// ORDER BY
var q3 = "SELECT c.id FROM c ORDER BY c.cp_lowerName ASC";

// GROUP BY
var q4 = "SELECT c.cp_lowerName, COUNT(1) AS cnt FROM c GROUP BY c.cp_lowerName";
```

### Updating Computed Properties

```csharp
var readResp = await container.ReadContainerAsync();
var updatedProps = readResp.Resource;
updatedProps.ComputedProperties = new Collection<ComputedProperty>
{
    new ComputedProperty { Name = "cp_upperName", Query = "SELECT VALUE UPPER(c.name) FROM c" }
};
await container.ReplaceContainerAsync(updatedProps);
```

### Behavioural Notes

- `SELECT *` does **not** include computed properties (matching real Cosmos DB)
- Computed properties are evaluated at query time using the emulator's SQL expression engine
- All 120+ built-in SQL functions are available in computed property queries
- Zero performance overhead for containers without computed properties
- Point reads (`ReadItemAsync`, `ReadItemStreamAsync`) do not include computed properties
- `ReadManyItemsAsync` / `ReadManyItemsStreamAsync` do not include computed properties
- Change feed results do not include computed properties
- Transactional batch reads do not include computed properties
- Works with: `SELECT`, `WHERE`, `ORDER BY`, `GROUP BY`, `DISTINCT`, `TOP`, `OFFSET`/`LIMIT`, `VALUE`, aggregates, aliases, boolean logic, arithmetic, `IN`, `BETWEEN`, parameterized queries
- Expression types: string functions, math functions, nested property access, type-check functions, array functions, IIF conditionals, coalesce (`??`), CONCAT, StringSplit, TimestampToDateTime, ToString
- CPs re-evaluate correctly after document updates (upsert, replace)
- `ReplaceContainerAsync` / `ReplaceContainerStreamAsync` invalidate CP cache — old CPs removed, new CPs active
- Cross-partition queries work correctly with CPs
- Multiple CPs can be used together in a single query (SELECT, WHERE, ORDER BY)
- Non-`c` FROM aliases (e.g. `FROM root`) are supported in CP definitions
- Creating containers with CPs via `InMemoryDatabase.CreateContainerAsync` is supported
- LINQ queries (`GetItemLinqQueryable`) do **not** include computed properties (known limitation — LINQ bypasses the SQL engine)
- See [Known Limitations](Known-Limitations) for undefined propagation, validation gaps, and SELECT c.* issues

### Validation (v2.0.53–v2.0.54)

- Maximum 20 computed properties per container (matching real Cosmos DB)
- Reserved system property names rejected: `id`, `_rid`, `_ts`, `_etag`, `_self`, `_attachments`
- **SELECT VALUE required (v2.0.54+):** CP queries must use `SELECT VALUE` syntax
- **Prohibited clauses (v2.0.54+):** WHERE, ORDER BY, GROUP BY, TOP, DISTINCT, OFFSET, LIMIT, JOIN rejected in CP queries
- **Cross-CP references (v2.0.54+):** A computed property cannot reference another computed property
- Validation runs on both `InMemoryContainer` construction and `ReplaceContainerAsync`