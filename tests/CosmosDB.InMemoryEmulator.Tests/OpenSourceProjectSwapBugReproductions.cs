using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Net;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Minimal bug reproductions discovered by swapping real Cosmos DB emulator
/// with InMemoryEmulator in 5 open-source projects:
/// - ASOS/SimpleEventStore
/// - Azure/Microsoft.Extensions.Caching.Cosmos
/// - MassTransit/MassTransit
/// - Avanade/Beef
/// - Particular/NServiceBus.Persistence.CosmosDB
/// </summary>
public class OpenSourceProjectSwapBugReproductions
{
    // =========================================================================
    // Bug 1: /_etag/? not auto-added to IndexingPolicy ExcludedPaths
    // Found in: ASOS/SimpleEventStore
    // Real Cosmos: Automatically adds /_etag/? to excluded paths on container creation
    // InMemoryEmulator: Does not add it
    // =========================================================================
    [Fact]
    public async Task ContainerProperties_ShouldAutoAddEtagToExcludedPaths()
    {
        var client = new InMemoryCosmosClient();
        var database = (await client.CreateDatabaseIfNotExistsAsync("etag-index-db")).Database;

        var containerProperties = new ContainerProperties("test-container", "/partitionKey")
        {
            IndexingPolicy = new IndexingPolicy
            {
                ExcludedPaths = { new ExcludedPath { Path = "/body/*" }, new ExcludedPath { Path = "/metaData/*" } }
            }
        };
        await database.CreateContainerIfNotExistsAsync(containerProperties);

        var container = database.GetContainer("test-container");
        var response = await container.ReadContainerAsync();
        var excludedPaths = response.Resource.IndexingPolicy.ExcludedPaths.Select(p => p.Path).ToList();

        // Real Cosmos auto-adds /_etag/? to excluded paths
        excludedPaths.Should().Contain("/_etag/?",
            "Cosmos DB auto-adds /_etag/? to ExcludedPaths when a custom IndexingPolicy is provided");
    }

    // =========================================================================
    // Bug 2: CancellationToken not honored on in-memory query operations
    // Found in: ASOS/SimpleEventStore
    // Real Cosmos: Throws OperationCanceledException when token is already cancelled
    // InMemoryEmulator: Completes synchronously, ignoring the token
    // =========================================================================
    [Fact]
    public async Task QueryIterator_ShouldThrowWhenCancellationTokenIsCancelled()
    {
        var container = new InMemoryContainer("cancel-test", "/partitionKey");

        // Create some items
        for (var i = 0; i < 10; i++)
            await container.CreateItemAsync(
                new TestDocument { Id = $"item-{i}", PartitionKey = "pk1", Name = $"Item{i}" },
                new PartitionKey("pk1"));

        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancel the token

        var query = new QueryDefinition("SELECT * FROM c");
        var iterator = container.GetItemQueryIterator<TestDocument>(query);

        // Real Cosmos throws OperationCanceledException when given a cancelled token
        var act = async () =>
        {
            while (iterator.HasMoreResults)
                await iterator.ReadNextAsync(cts.Token);
        };

        await act.Should().ThrowAsync<OperationCanceledException>(
            "A pre-cancelled CancellationToken should cause OperationCanceledException");
    }

    // =========================================================================
    // Bug 3: TTL-based document expiration doesn't filter expired items on read
    // Found in: Particular/NServiceBus.Persistence.CosmosDB
    // Real Cosmos: Documents with expired TTL return 404 on point reads
    // InMemoryEmulator: Known limitation (lazy eviction), but even on read
    //   the item should be filtered out / return 404 after TTL expires
    // =========================================================================
    [Fact]
    public async Task ItemWithTTL_ShouldReturn404AfterExpiry()
    {
        var containerProps = new ContainerProperties("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = -1 // Container TTL enabled, per-item TTL controls
        };
        var container = new InMemoryContainer(containerProps);

        // Create item with 1-second TTL
        var item = new TtlDocument { Id = "expiring-item", PartitionKey = "pk1", Ttl = 1, Data = "test" };
        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        // Verify it exists
        var readBefore = await container.ReadItemAsync<TtlDocument>("expiring-item", new PartitionKey("pk1"));
        readBefore.StatusCode.Should().Be(HttpStatusCode.OK);

        // Wait for TTL to expire
        await Task.Delay(2000);

        // Point read should return 404 for expired items
        var act = async () => await container.ReadItemAsync<TtlDocument>("expiring-item", new PartitionKey("pk1"));
        var exception = (await act.Should().ThrowAsync<CosmosException>()).Which;
        exception.StatusCode.Should().Be(HttpStatusCode.NotFound,
            "Item with expired TTL should return 404 on point read after expiry");
    }

    // =========================================================================
    // Bug 4: FakeCosmosHandler does not support database/container management routes
    // Found in: ALL projects (SimpleEventStore, Caching.Cosmos, MassTransit, Beef, NServiceBus)
    // Real Cosmos: CreateDatabaseIfNotExistsAsync works via HTTP
    // FakeCosmosHandler: Throws "unrecognised route POST /dbs"
    // This forced all 5 projects to use InMemoryCosmosClient instead of
    // the recommended FakeCosmosHandler approach
    // =========================================================================
    [Fact]
    public async Task FakeCosmosHandler_ShouldSupportDatabaseCreation()
    {
        var inMemoryContainer = new InMemoryContainer("test-container", "/partitionKey");
        var handler = new FakeCosmosHandler(inMemoryContainer);

        var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                HttpClientFactory = () => new HttpClient(handler)
            });

        // This is needed by all real-world projects before they can use containers.
        // Currently throws: "FakeCosmosHandler: unrecognised route POST /dbs"
        var act = async () => await client.CreateDatabaseIfNotExistsAsync("test-db");
        await act.Should().NotThrowAsync(
            "FakeCosmosHandler should support CreateDatabaseIfNotExistsAsync since nearly all real projects need it");
    }
}

public class TtlDocument
{
    [JsonProperty("id")]
    public string Id { get; set; } = default!;

    [JsonProperty("partitionKey")]
    public string PartitionKey { get; set; } = default!;

    [JsonProperty("ttl")]
    public int Ttl { get; set; }

    [JsonProperty("data")]
    public string Data { get; set; } = default!;
}
