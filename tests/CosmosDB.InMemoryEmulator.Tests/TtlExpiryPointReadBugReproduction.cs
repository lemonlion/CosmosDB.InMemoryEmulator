using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using System.Net;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Bug: Items with expired TTL are not filtered out on point reads.
///
/// Found while migrating Particular/NServiceBus.Persistence.CosmosDB to InMemoryEmulator.
/// See: https://github.com/McNultyyy/NServiceBus.Persistence.CosmosDB/tree/use-inmemory-emulator
///
/// Real Cosmos DB returns 404 NotFound when reading an item whose per-item TTL has
/// expired. The InMemoryEmulator's lazy eviction filters expired items from queries
/// but does not filter them from point reads (ReadItemAsync), so the item is still
/// returned after its TTL has passed.
/// </summary>
public class TtlExpiryPointReadBugReproduction
{
    [Fact]
    public async Task ItemWithTTL_ShouldReturn404AfterExpiry()
    {
        var containerProps = new ContainerProperties("ttl-test", "/partitionKey")
        {
            DefaultTimeToLive = -1 // Container TTL enabled, per-item TTL controls
        };
        var container = new InMemoryContainer(containerProps);

        // Create item with 1-second TTL
        var item = new TtlTestDocument { Id = "expiring-item", PartitionKey = "pk1", Ttl = 1, Data = "test" };
        await container.CreateItemAsync(item, new PartitionKey("pk1"));

        // Verify it exists
        var readBefore = await container.ReadItemAsync<TtlTestDocument>("expiring-item", new PartitionKey("pk1"));
        readBefore.StatusCode.Should().Be(HttpStatusCode.OK);

        // Wait for TTL to expire
        await Task.Delay(2000);

        // Point read should return 404 for expired items
        var act = async () => await container.ReadItemAsync<TtlTestDocument>("expiring-item", new PartitionKey("pk1"));
        var exception = (await act.Should().ThrowAsync<CosmosException>()).Which;
        exception.StatusCode.Should().Be(HttpStatusCode.NotFound,
            "Item with expired TTL should return 404 on point read after expiry");
    }
}

public class TtlTestDocument
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
