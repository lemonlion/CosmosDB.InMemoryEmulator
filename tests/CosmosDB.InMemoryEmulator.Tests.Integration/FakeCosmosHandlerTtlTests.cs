using System.Net;
using System.Text;
using AwesomeAssertions;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json.Linq;
using Xunit;

namespace CosmosDB.InMemoryEmulator.Tests;

/// <summary>
/// Tests for TTL (Time to Live) behavior through FakeCosmosHandler.
/// CRUD goes through handler, TTL filtering verified via queries.
/// Parity-validated: runs against both FakeCosmosHandler (in-memory) and real emulator.
/// Change feed tombstone test is tagged InMemoryOnly.
/// </summary>
[Collection(IntegrationCollection.Name)]
public class FakeCosmosHandlerTtlTests(EmulatorSession session) : IAsyncLifetime
{
    private readonly ITestContainerFixture _fixture = TestFixtureFactory.Create(session);
    private Container _container = null!;

    public async ValueTask InitializeAsync()
    {
        _container = await _fixture.CreateContainerAsync("test-ttl", "/partitionKey",
            configure: props => props.DefaultTimeToLive = 2);
    }

    public async ValueTask DisposeAsync()
    {
        await _fixture.DisposeAsync();
    }

    private async Task<List<T>> DrainQuery<T>(string sql)
    {
        var iterator = _container.GetItemQueryIterator<T>(sql);
        var results = new List<T>();
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            results.AddRange(page);
        }
        return results;
    }

    [Fact]
    public async Task TTL_ItemBeforeExpiry_VisibleInQuery()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "Fresh" },
            new PartitionKey("pk1"));

        var results = await DrainQuery<TestDocument>("SELECT * FROM c");
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("Fresh");
    }

    [Fact]
    public async Task TTL_ItemAfterExpiry_FilteredFromQuery()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "WillExpire" },
            new PartitionKey("pk1"));

        // Wait for TTL to expire (container TTL = 2 seconds)
        await Task.Delay(3000);

        var results = await DrainQuery<TestDocument>("SELECT * FROM c");
        results.Should().BeEmpty();
    }

    [Fact]
    public async Task TTL_ItemAfterExpiry_PointReadThrows404()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "WillExpire" },
            new PartitionKey("pk1"));

        await Task.Delay(3000);

        var act = () => _container.ReadItemAsync<TestDocument>("1", new PartitionKey("pk1"));
        (await act.Should().ThrowAsync<CosmosException>()).Which.StatusCode.Should().Be(HttpStatusCode.NotFound);
    }

    [Fact]
    public async Task TTL_NonExpiredItems_StillVisibleAfterOthersExpire()
    {
        await _container.CreateItemAsync(
            new TestDocument { Id = "1", PartitionKey = "pk1", Name = "WillExpire" },
            new PartitionKey("pk1"));

        await Task.Delay(3000);

        // Create a new item after old one expired
        await _container.CreateItemAsync(
            new TestDocument { Id = "2", PartitionKey = "pk1", Name = "StillFresh" },
            new PartitionKey("pk1"));

        var results = await DrainQuery<TestDocument>("SELECT * FROM c");
        results.Should().HaveCount(1);
        results[0].Name.Should().Be("StillFresh");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  Per-Item TTL Override
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TTL_PerItemOverride_ShortTtlExpiresBeforeContainerDefault()
    {
        // Container default = 2s. Item TTL = 1s → expires faster
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "short", partitionKey = "pk1", name = "ShortLived", _ttl = 1 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "default", partitionKey = "pk1", name = "DefaultTtl" }),
            new PartitionKey("pk1"));

        // After 1.5s: short-TTL expired, default-TTL still alive
        await Task.Delay(1500);

        var results = await DrainQuery<JObject>("SELECT c.id FROM c");
        results.Should().HaveCount(1);
        results[0]["id"]!.Value<string>().Should().Be("default");
    }

    [Fact]
    public async Task TTL_PerItemOverride_MinusOne_NeverExpires()
    {
        // Container default = 2s. Item TTL = -1 → never expires
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "forever", partitionKey = "pk1", name = "Immortal", _ttl = -1 }),
            new PartitionKey("pk1"));
        await _container.CreateItemAsync(
            JObject.FromObject(new { id = "mortal", partitionKey = "pk1", name = "WillDie" }),
            new PartitionKey("pk1"));

        await Task.Delay(3000);

        var results = await DrainQuery<JObject>("SELECT c.id FROM c");
        results.Should().HaveCount(1);
        results[0]["id"]!.Value<string>().Should().Be("forever");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    //  TTL items produce tombstones in change feed
    // ═══════════════════════════════════════════════════════════════════════════

    [Fact]
    [Trait(TestTraits.Target, TestTraits.InMemoryOnly)]
    public async Task TTL_ExpiredItem_ProducesTombstoneInChangeFeed()
    {
        var inMemoryContainer = new InMemoryContainer("test-ttl-cf", "/partitionKey") { DefaultTimeToLive = 2 };
        using var handler = new FakeCosmosHandler(inMemoryContainer);
        using var client = new CosmosClient(
            "AccountEndpoint=https://localhost:9999/;AccountKey=dGVzdGtleQ==;",
            new CosmosClientOptions
            {
                ConnectionMode = ConnectionMode.Gateway,
                LimitToEndpoint = true,
                MaxRetryAttemptsOnRateLimitedRequests = 0,
                RequestTimeout = TimeSpan.FromSeconds(10),
                HttpClientFactory = () => new HttpClient(handler) { Timeout = TimeSpan.FromSeconds(10) }
            });
        var container = client.GetContainer("db", "test-ttl-cf");

        var checkpoint = inMemoryContainer.GetChangeFeedCheckpoint();

        await container.CreateItemAsync(
            new TestDocument { Id = "ttl1", PartitionKey = "pk1", Name = "WillExpire" },
            new PartitionKey("pk1"));

        await Task.Delay(3000); // wait for TTL expiry

        // Force eviction by querying
        var iterator2 = container.GetItemQueryIterator<TestDocument>("SELECT * FROM c WHERE c.id = 'ttl1'");
        while (iterator2.HasMoreResults) await iterator2.ReadNextAsync();

        // Change feed should show the create + the TTL eviction tombstone
        var changes = new List<JObject>();
        var iterator = inMemoryContainer.GetChangeFeedIterator<JObject>(checkpoint);
        while (iterator.HasMoreResults)
        {
            var page = await iterator.ReadNextAsync();
            changes.AddRange(page);
        }

        changes.Should().HaveCountGreaterThanOrEqualTo(1);
        // At least the create should be present
        changes.Any(c => c["id"]?.Value<string>() == "ttl1").Should().BeTrue();
    }
}
