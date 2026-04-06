# Troubleshooting

Common errors and how to fix them. For behavioural differences between the emulator and real Azure Cosmos DB, see [Known Limitations](Known-Limitations).

---

## "Item must have an 'id' property"

**Symptom:** `CreateItemAsync`, `UpsertItemAsync`, or their stream variants throw:

> Item must have an 'id' property (case-sensitive, lowercase). If your C# model uses PascalCase 'Id', ensure CosmosClientOptions.Serializer is configured with camelCase naming (e.g. CosmosJsonDotNetSerializer with CamelCasePropertyNamesContractResolver).

**Cause:** The Cosmos SDK serializes your C# object to JSON before sending it. If your model has `public string Id { get; set; }` (PascalCase), the default serializer produces `{ "Id": "..." }` — but Cosmos requires lowercase `"id"`.

**Fix (recommended):** Add `[JsonProperty("id")]` on the property:

```csharp
using Newtonsoft.Json;

[JsonProperty("id")]
public string Id { get; set; }
```

This is the simplest approach and works with all [integration approaches](Integration-Approaches).

**Alternative fix:** Configure `CosmosClientOptions.Serializer` with a camelCase contract resolver. Note that `CosmosJsonDotNetSerializer` is not part of the public SDK — you need to supply your own `CosmosSerializer` implementation, or copy the one from the [Azure Cosmos DB samples](https://github.com/Azure/azure-cosmos-dotnet-v3/blob/master/Microsoft.Azure.Cosmos.Samples/Usage/SystemTextJson/CosmosSystemTextJsonSerializer.cs):

```csharp
var client = new CosmosClient(connectionString, new CosmosClientOptions
{
    Serializer = new MyCustomCosmosSerializer(new JsonSerializerSettings
    {
        ContractResolver = new CamelCasePropertyNamesContractResolver()
    })
});
```

> This error applies equally to `InMemoryContainer`, `FakeCosmosHandler`, and the real Azure Cosmos DB service. See [Getting Started — serialization note](Getting-Started) and [Dependency Injection — serialization](Dependency-Injection) for more context.

---

## NullReferenceException in HandleUpsertAsync / HandleCreateAsync

**Symptom:** `NullReferenceException` when calling CRUD operations through `FakeCosmosHandler`.

**Cause:** Usually the same root cause as above — the serialized JSON doesn't contain `"id"` (lowercase), so partition key extraction or document indexing fails.

**Fix:** Apply the `[JsonProperty("id")]` fix or camelCase serializer described [above](#item-must-have-an-id-property).

---

## Query returns empty results

**Symptom:** `GetItemQueryIterator` or LINQ queries return no items, even though items were inserted.

**Possible causes:**

1. **Partition key path mismatch.** The `InMemoryContainer` was created with a different partition key path than your production container. For example, your production container uses `/customerId` but the test uses the default `/id`.

2. **Cross-partition query not enabled.** By default, queries target a single partition. If you pass a `PartitionKey` in `QueryRequestOptions`, only items in that partition are returned. Omit the partition key to run a cross-partition query.

3. **Serialization mismatch.** Items were inserted with one casing but queried with another. Ensure serializer configuration is consistent across all operations.

**Fix:** Verify the partition key path matches your production configuration:

```csharp
// Must match your Azure Cosmos container's partition key path
var container = new InMemoryContainer("orders", "/customerId");
```

For a full list of supported query syntax, see [SQL Queries](SQL-Queries).

---

## "Container 'xxx' is not registered with CreateRouter()"

**Symptom:** `InvalidOperationException` when using `FakeCosmosHandler.CreateRouter()`:

> Container 'xxx' is not registered with CreateRouter(). Registered containers: a, b, c. Add it to the dictionary passed to FakeCosmosHandler.CreateRouter().

**Cause:** Your production code calls `client.GetContainer("db", "xxx")`, but `"xxx"` is not in the dictionary passed to `CreateRouter()`.

**Fix:** Add the missing container to the router dictionary:

```csharp
var router = FakeCosmosHandler.CreateRouter(new Dictionary<string, FakeCosmosHandler>
{
    ["orders"] = new FakeCosmosHandler(ordersContainer),
    ["customers"] = new FakeCosmosHandler(customersContainer),
    ["xxx"] = new FakeCosmosHandler(xxxContainer)  // ← add this
});
```

See [Integration Approaches — multi-container routing](Integration-Approaches#multi-container-routing) for the full pattern.

---

## Change feed processor doesn't fire

**Symptom:** `InMemoryChangeFeedProcessor` doesn't invoke the delegate after inserting items.

**Cause:** The processor must be started explicitly and polls on a 50 ms interval internally. If your test completes before the poll cycle runs, the delegate won't fire.

**Fix:** Ensure you call `StartAsync()` and allow time for the poll cycle:

```csharp
await processor.StartAsync();
// ... insert items ...
await Task.Delay(500); // allow poll cycle to complete
await processor.StopAsync();
```

> The in-memory change feed has several [known behavioural differences](Known-Limitations#behavioral-differences) from real Cosmos DB, including FeedRange scoping and the stream handler variant. See [Features — Change Feed](Features) for supported functionality.

---

## "Failed to parse Cosmos SQL query"

**Symptom:** `NotSupportedException` when running a SQL query:

> Failed to parse Cosmos SQL query: SELECT ...

**Cause:** The built-in SQL parser doesn't support the query syntax you're using. While it covers 120+ functions and most common patterns, some advanced or rarely-used Cosmos SQL syntax may not yet be implemented.

**Fix:** Check the [SQL Queries](SQL-Queries) page for the full list of supported clauses, operators, and functions. If your query uses unsupported syntax, consider:

1. Filing an issue to request support.
2. Rewriting the query using supported syntax.
3. Falling back to LINQ-based queries if applicable.

---

## TTL items not disappearing immediately

**Symptom:** Items with a TTL (time-to-live) are still returned by queries after they should have expired.

**Cause:** The emulator uses **lazy TTL eviction** — expired items are removed during reads and queries, not proactively on a background timer. This differs from real Cosmos DB, which evicts items in the background.

**Fix:** This is expected behaviour. Expired items are filtered out of query results and point reads. If you need to verify TTL expiry in tests, insert the item, advance past the TTL duration with `Task.Delay`, and then confirm the item is no longer returned. See [Known Limitations](Known-Limitations) for details.

---

## .ToFeedIterator() throws ArgumentOutOfRangeException

**Symptom:** Calling `.ToFeedIterator()` on a LINQ query throws:

> ArgumentOutOfRangeException: ToFeedIterator is only supported on Cosmos LINQ query operations

**Cause:** `InMemoryContainer.GetItemLinqQueryable<T>()` returns a standard LINQ-to-Objects queryable. The SDK's `.ToFeedIterator()` only works on queryables created by its own `CosmosLinqQueryProvider`.

**Fix:** Either:

1. **Use `FakeCosmosHandler`** — production code stays untouched, `.ToFeedIterator()` works as-is because the SDK's own LINQ provider is used. This is the [highest-fidelity integration approach](Integration-Approaches#cosmosclient--fakecosmoshandler-high-fidelity).
2. **Use `.ToFeedIteratorOverridable()`** — a one-token change per call site. Add the `CosmosDB.InMemoryEmulator.ProductionExtensions` NuGet and call `InMemoryFeedIteratorSetup.Register()` in test setup.

See the [Feed Iterator Usage Guide](Feed-Iterator-Usage-Guide) for a detailed comparison of both approaches.

---

## SDK compatibility check failed

**Symptom:** `InvalidOperationException` during test setup mentioning "SDK compatibility check failed":

> SDK compatibility check failed (v3.x.x): expected 3 items from basic query but got 0. The Cosmos SDK may have changed its internal HTTP contract.

**Cause:** `FakeCosmosHandler.VerifySdkCompatibilityAsync()` runs a suite of checks to verify that the installed Cosmos SDK version is compatible with the handler's HTTP interception layer. A failure means the SDK version you're using has changed its internal HTTP contract in a way that `FakeCosmosHandler` doesn't yet support.

**Fix:**

1. Check you're using a supported SDK version (3.58.0+).
2. Update to the latest version of `CosmosDB.InMemoryEmulator`.
3. If the issue persists, file an issue — the handler may need updating for new SDK internals.

---

## Still stuck?

- Review the [Known Limitations](Known-Limitations) page for documented behavioural differences.
- Check the [API Reference](API-Reference) for method signatures and available options.
- Consult [Integration Approaches](Integration-Approaches) to ensure you're using the right approach for your scenario.
- File an issue on the [GitHub repository](https://github.com/jamesfera/CosmosDB.InMemoryEmulator/issues) with a minimal repro.
