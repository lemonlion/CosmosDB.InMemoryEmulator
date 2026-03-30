# Features Guide

Detailed reference for all emulator features beyond basic CRUD and queries.

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

## Change Feed

### Change Feed Iterators

**Checkpoint-based (recommended):**

```csharp
long checkpoint = container.GetChangeFeedCheckpoint();

await container.CreateItemAsync(item1, new PartitionKey("pk"));
await container.CreateItemAsync(item2, new PartitionKey("pk"));

var iterator = container.GetChangeFeedIterator<MyDocument>(checkpoint);
while (iterator.HasMoreResults)
{
    var changes = await iterator.ReadNextAsync();
    foreach (var change in changes)
        Console.WriteLine(change.Id);
}
```

**SDK-style start positions:**

```csharp
// All changes since container creation
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

### Stream Change Feed

```csharp
var iterator = container.GetChangeFeedStreamIterator(
    ChangeFeedStartFrom.Beginning(), ChangeFeedMode.Incremental);
```

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
container.DefaultTimeToLive = 3600; // 1 hour

// Per-item override via _ttl property in JSON
var item = new { id = "1", partitionKey = "pk", _ttl = 7200, data = "..." };
await container.CreateItemAsync(item, new PartitionKey("pk"));

// Disable TTL
container.DefaultTimeToLive = null;
```

> **Important:** Items are lazily evicted — expired items are removed the next time they are accessed (read, query, or enumerate). There is no background eviction thread.

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

---

## State Persistence

Export and import container state for test data seeding, snapshots, or debugging:

```csharp
// Export to string
string json = container.ExportState();

// Export to file
container.ExportStateToFile("test-data/snapshot.json");

// Import from string (replaces all data)
container.ImportState(json);

// Import from file
container.ImportStateFromFile("test-data/snapshot.json");

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

---

## Container Management

```csharp
// Read container properties
var response = await container.ReadContainerAsync();

// Replace container properties
await container.ReplaceContainerAsync(new ContainerProperties("my-container", "/newPk"));

// Delete container
await container.DeleteContainerAsync();

// Delete all items in a partition
await container.DeleteAllItemsByPartitionKeyStreamAsync(new PartitionKey("pk1"));
```

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

For `.ToFeedIterator()` support in tests, see the [Integration Approaches](integration-approaches.md) guide.
